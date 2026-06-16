// SteamedClaw Beta — loadable OpenClaw plugin entry (planning/072k §8).
//
// A standalone, STAGE-LOCKED beta that folds the live-validated WS Leg-1
// coordinator (072j) into the credentials-based, agent-initiated tool surface of
// the published plugin (deploy/clawhub/steamedclaw-plugin/, read-only reference).
// It is a separate ClawHub slug (steamedclaw-plugin-beta) that coexists with the
// published plugin: distinct plugin id, namespaced tools (steamedclaw_beta_*),
// its own data dir (~/.config/steamedclaw-beta-state/), and a hard stage lock.
//
// Play path (072k §4): the agent calls steamedclaw_beta_register (identity →
// credentials), steamedclaw_beta_queue (binds ctx.sessionKey + enters
// matchmaking), then loops steamedclaw_beta_get_turn (BLOCKING pull) /
// steamedclaw_beta_take_turn (token-validated submit) to game-over.
//
// Speed path: match_found + your_turn arrive over WebSocket (ws-receiver.mjs);
// each turn is PARKED via coordinator.enqueueTurn, which resolves a blocked
// get_turn mid-call — or, if the agent has yielded, the supervisor fires a
// content-carrying heartbeat wake (enqueueSystemEvent + requestHeartbeat, spaced
// ≥~64s under OpenClaw's flood guard, 072j §10). Submit stays HTTP. HTTP polling
// is the fallback floor whenever a socket is down.
//
// Mechanics live in tool descriptions/results, never in agent SOUL/persona.

import { definePluginEntry } from 'openclaw/plugin-sdk/plugin-entry';
import { register as registerCoordinator, getOwnerCoordinator } from './coordinator.mjs';
import { makeStageClient, TERMINAL_MATCH_STATUSES } from './transport.mjs';
import { readCredentials, writeCredentials, writeClaimIfAbsent } from './state.mjs';
import { resolveStageServer, BETA_STAGE_SERVER } from './stage-lock.mjs';
import {
  startReceiver,
  stopReceiver,
  openGame,
  closeGame,
  receiverStatus,
  __resetReceiver,
} from './ws-receiver.mjs';

export const BETA_LANES = ['fast', 'standard'];
// Beta default lane: 'standard' (longer per-turn window). The beta is WS
// push-driven, but its first live validation (072j game 3) ran on 'standard';
// the heartbeat-wake path is more forgiving at the longer window. An explicit
// lane on a queue call overrides this.
export const BETA_DEFAULT_LANE = 'standard';
// v1 supports exactly one simultaneous game (072k §3). >1 is rejected.
export const BETA_MAX_SIMULTANEOUS_GAMES = 1;

const DEFAULT_TICK_MS = 4000; //  supervisor cadence; WS events do the fast path
const DEFAULT_QUEUE_TTL_MS = 60000;
const DEFAULT_RATE_LIMIT_BACKOFF_MS = 3000; //  floor backoff on a 429
const DEFAULT_NEXT_TURN_BLOCK_MS = 20000; //  blocking get_turn budget (072j)
// HTTP safety-net cadence while the WS path is healthy: every Nth tick still
// runs discovery/state polling to catch events lost outside the 60s missed-event
// buffer (agent socket down >60s, lost game_over).
const SAFETY_NET_EVERY_TICKS = 15; //  ≈60s at the 4s tick
// Re-wake cadence while a parked turn sits unconsumed (072j §9-10). A single
// park-time wake is lossy (absorbed when the agent is mid-turn), so we retry;
// but retrying faster than ~64s trips OpenClaw's heartbeat FLOOD GUARD (≥5 runs
// within 60s defers further wakes — verified in heartbeat-runner source).
const REWAKE_EVERY_TICKS = 16; //  ≈64s at the 4s tick

const TURN_INSTRUCTIONS =
  'You have a SteamedClaw Beta turn. Call steamedclaw_beta_take_turn with this turnToken and a single legal action for the view shown.';

const NOT_REGISTERED_MESSAGE =
  'No credentials yet. Call steamedclaw_beta_register({name, model?}) before queueing or playing.';

// ── Module-scope supervisor state (single resolved module ⇒ shared across all
// register() instances, the same guarantee the coordinator relies on). ────────
const DRIVER = {
  boundSessionKey: null,
  boundAgentId: null, //  OpenClaw agent id from queue ctx — the heartbeat target
  gameId: null,
  lane: null,
  matchId: null,
  matchGameId: null,
  phase: 'idle', //  idle → queued → in_match → terminal
  queuedAt: 0,
  paused: false, //  leave_queue flag — suppresses NEW match pickups
  lastParkedSeq: -1,
  turnsParked: 0,
  parkedVia: { ws: 0, http: 0 },
  wakesFired: 0,
  backoffUntil: 0,
  tickCount: 0,
  receiverStarted: false,
  registerInFlight: null,
};

export function __resetDriver() {
  DRIVER.boundSessionKey = null;
  DRIVER.boundAgentId = null;
  DRIVER.gameId = null;
  DRIVER.lane = null;
  DRIVER.matchId = null;
  DRIVER.matchGameId = null;
  DRIVER.phase = 'idle';
  DRIVER.queuedAt = 0;
  DRIVER.paused = false;
  DRIVER.lastParkedSeq = -1;
  DRIVER.turnsParked = 0;
  DRIVER.parkedVia = { ws: 0, http: 0 };
  DRIVER.wakesFired = 0;
  DRIVER.backoffUntil = 0;
  DRIVER.tickCount = 0;
  DRIVER.receiverStarted = false;
  DRIVER.registerInFlight = null;
  __resetReceiver();
}

function toolText(obj) {
  return { content: [{ type: 'text', text: JSON.stringify(obj) }] };
}

// On a 429, pause the supervisor's polling so it stops adding rate-limit
// pressure. Honors retryAfterMs, floored at DEFAULT_RATE_LIMIT_BACKOFF_MS.
function rateLimited(res, logger) {
  if (res && res.httpStatus === 429) {
    const wait = Math.max(res.retryAfterMs ?? 0, DEFAULT_RATE_LIMIT_BACKOFF_MS);
    DRIVER.backoffUntil = Date.now() + wait;
    logger.info?.(`[steamedclaw-beta] 429 rate-limited — backing off ${wait}ms`);
    return true;
  }
  return false;
}

// Wake the agent's session — the speed path when a turn arrives while the agent
// has yielded (072i §11, hardened in 072j §10). Two-part, because a bare
// heartbeat carries NO content and the agent demonstrably shrugs at it:
//   1. enqueueSystemEvent(text) — queues an ACTIONABLE instruction the next
//      heartbeat run surfaces into the agent's context (same channel as
//      exec-completion notices). Callable post-register (runtime.system).
//   2. requestHeartbeat — triggers that run. Returns undefined live; success is
//      the agent waking. Targets the OpenClaw agent/session captured at queue.
function wakeAgent(api, reason, logger, text) {
  if (!api || !DRIVER.boundSessionKey) return;
  try {
    if (text) {
      api.runtime?.system?.enqueueSystemEvent?.(text, { sessionKey: DRIVER.boundSessionKey });
    }
    api.runtime?.system?.requestHeartbeat?.({
      source: 'background-task',
      intent: 'immediate',
      agentId: DRIVER.boundAgentId ?? undefined,
      sessionKey: DRIVER.boundSessionKey,
      reason,
    });
    DRIVER.wakesFired += 1;
    logger?.info?.(`[steamedclaw-beta] heartbeat wake fired (${reason})${text ? ' +event' : ''}`);
  } catch (err) {
    logger?.warn?.(`[steamedclaw-beta] heartbeat wake failed: ${err?.message ?? err}`);
  }
}

function turnEventText(sequence) {
  return `SteamedClaw Beta: it is YOUR TURN (match ${DRIVER.matchId}, sequence ${sequence}). Call steamedclaw_beta_get_turn now to get the turn, then steamedclaw_beta_take_turn with the returned turnToken.`;
}

// Park one turn (from either path) exactly once, then wake the agent unless a
// blocked get_turn already consumed the notify (woken > 0 ⇒ agent is awake,
// mid-tool-call). The sequence is CLAIMED synchronously (so a concurrent WS/HTTP
// park of the same turn dedupes) but ROLLED BACK if the park fails — otherwise a
// failed enqueue would suppress every redelivery of that turn and lose it.
async function parkTurn(owner, { sequence, view }, via, { api, logger }) {
  if (typeof sequence !== 'number' || sequence <= DRIVER.lastParkedSeq) return false;
  const prevSeq = DRIVER.lastParkedSeq;
  DRIVER.lastParkedSeq = sequence; //  claim before the await — concurrent parks dedupe here
  try {
    const { woken } = await owner.enqueueTurn({
      matchId: DRIVER.matchId,
      sequence,
      view,
      phase: 'play',
      instructions: TURN_INSTRUCTIONS,
    });
    DRIVER.turnsParked += 1;
    DRIVER.parkedVia[via] += 1;
    logger?.info?.(`[steamedclaw-beta] parked turn seq=${sequence} via=${via} woken=${woken}`);
    if (!woken) wakeAgent(api, 'steamedclaw-beta-turn', logger, turnEventText(sequence));
    return true;
  } catch (err) {
    if (DRIVER.lastParkedSeq === sequence) DRIVER.lastParkedSeq = prevSeq;
    logger?.warn?.(
      `[steamedclaw-beta] park failed seq=${sequence} via=${via}: ${err?.message ?? err}`,
    );
    return false;
  }
}

function finishMatch(owner, receiver, { api, logger }, how) {
  if (DRIVER.phase === 'terminal') return; //  WS + HTTP can both detect game_over — finish once
  const { woken } = owner.markTerminal(DRIVER.matchId);
  receiver?.closeGame?.(DRIVER.matchId, { terminal: true });
  DRIVER.phase = 'terminal';
  if (!woken) {
    wakeAgent(
      api,
      'steamedclaw-beta-game-over',
      logger,
      `SteamedClaw Beta: the match (${DRIVER.matchId}) has ended. Call steamedclaw_beta_get_turn to confirm, then stop.`,
    );
  }
  logger?.info?.(`[steamedclaw-beta] GAME OVER (${how}) — ${JSON.stringify(buildReport())}`);
}

// Delivery report for diagnostics/logging (072j). Not an agent-facing tool.
function buildReport() {
  return {
    boundSessionKey: DRIVER.boundSessionKey,
    matchId: DRIVER.matchId,
    phase: DRIVER.phase,
    turnsParked: DRIVER.turnsParked,
    lastParkedSeq: DRIVER.lastParkedSeq,
    parkedVia: { ...DRIVER.parkedVia },
    wakesFired: DRIVER.wakesFired,
    delivery: DRIVER.parkedVia.ws > 0 ? 'ws-pull' : 'http-pull',
    ws: receiverStatus(),
  };
}

// Build the WS receiver adapter with the supervisor's event handlers bound in.
// makeWebSocket is injectable for tests.
export function makeWsReceiver({ api, server, logger, makeWebSocket }) {
  function handleMatchFound(frame) {
    try {
      if (DRIVER.paused) return; //  leave_queue: ignore NEW pairings
      if (DRIVER.matchId) return; //  single-match v1 — ignore extras
      if (!frame?.matchId) return;
      DRIVER.matchId = frame.matchId;
      DRIVER.matchGameId = frame.gameId ?? DRIVER.gameId;
      const owner = getOwnerCoordinator();
      if (owner && DRIVER.boundSessionKey) {
        owner.attachMatch(DRIVER.boundSessionKey, DRIVER.matchId, DRIVER.matchGameId);
        DRIVER.phase = 'in_match';
        openGame(DRIVER.matchId);
        logger.info?.(`[steamedclaw-beta] matched via WS matchId=${DRIVER.matchId}`);
      }
      //  else: matchId is set — the next tick attaches and opens the game socket.
    } catch (err) {
      logger.warn?.(`[steamedclaw-beta] match_found handler error: ${err?.message ?? err}`);
    }
  }

  function handleYourTurn(frame) {
    void (async () => {
      try {
        const owner = getOwnerCoordinator();
        if (!owner || !DRIVER.matchId || DRIVER.phase === 'terminal') return;
        await parkTurn(owner, frame, 'ws', { api, logger });
      } catch (err) {
        logger.warn?.(`[steamedclaw-beta] your_turn handler error: ${err?.message ?? err}`);
      }
    })();
  }

  function handleGameOver() {
    try {
      const owner = getOwnerCoordinator();
      if (!owner || !DRIVER.matchId || DRIVER.phase === 'terminal') return;
      finishMatch(owner, { closeGame }, { api, logger }, 'ws');
    } catch (err) {
      logger.warn?.(`[steamedclaw-beta] game_over handler error: ${err?.message ?? err}`);
    }
  }

  return {
    ensureStarted() {
      if (DRIVER.receiverStarted) return;
      const creds = readCredentials();
      if (!creds?.apiKey) return; //  can't open authenticated sockets yet
      DRIVER.receiverStarted = true;
      startReceiver({
        server,
        apiKey: creds.apiKey,
        logger,
        makeWebSocket,
        onMatchFound: handleMatchFound,
        onYourTurn: handleYourTurn,
        onGameOver: handleGameOver,
      });
    },
    status: receiverStatus,
    openGame,
    closeGame,
    stop: stopReceiver,
  };
}

// One supervisor step. Folded from the POC probeTick, MINUS the config-driven
// register/queue (those die — register is the steamedclaw_beta_register tool,
// queue is steamedclaw_beta_queue). The supervisor only: brings up the receiver
// once credentials exist, resolves the bound session, drives the queued→matched
// transition over the HTTP fallback when WS is down, parks HTTP-fallback turns,
// re-wakes an unconsumed turn, and finalizes on game_over. The supervisor runs
// for the plugin lifetime (like the published plugin's services): after a game
// it IDLES on the terminal phase until the agent re-queues — it does not stop,
// so a second sequential game ("play again") is serviced. `receiver`/`api`
// optional ⇒ pure HTTP pull floor (the tested path).
export async function supervisorTick({ client, server, cfg, logger, receiver, api }) {
  try {
    if (DRIVER.phase === 'terminal') return 'idle'; //  game over — wait for a re-queue
    DRIVER.tickCount += 1;
    const safety = DRIVER.tickCount % SAFETY_NET_EVERY_TICKS === 0;

    const owner = getOwnerCoordinator();
    if (!owner) return 'continue'; //  no full-mode owner yet
    if (DRIVER.backoffUntil && Date.now() < DRIVER.backoffUntil) return 'continue';

    // Credentials gate: the register tool writes them. No creds ⇒ idle.
    const creds = readCredentials();
    if (!creds?.apiKey) return 'continue';
    client.setApiKey(creds.apiKey);

    // Creds exist → bring up the WS receiver (idempotent, single-flight).
    receiver?.ensureStarted?.();

    // Resolve the bound session (the queue tool binds it). Until the agent has
    // queued there is nothing to discover or wake.
    if (!DRIVER.boundSessionKey) {
      const bound = owner.boundSessions();
      if (bound.length === 0) return 'continue';
      DRIVER.boundSessionKey = bound[0].sessionKey;
      DRIVER.boundAgentId = bound[0].agentId ?? null;
    }

    const resolvedGameId = DRIVER.gameId;

    // Resolve the matchId for an outstanding queue entry. Primary discovery is
    // the /ws/agent match_found push (handled in makeWsReceiver); the HTTP polls
    // here run only when that socket is down, or on the safety cadence.
    if (!DRIVER.matchId && DRIVER.phase === 'queued') {
      const ttlMs = cfg.queueTtlMs ?? DEFAULT_QUEUE_TTL_MS;
      const expired = DRIVER.queuedAt > 0 && Date.now() - DRIVER.queuedAt > ttlMs;
      const wsDiscovery = Boolean(receiver && receiver.status().agentReady);
      if (!wsDiscovery || safety) {
        let activeId = null;
        if (typeof client.activeMatch === 'function') {
          const am = await client.activeMatch(creds.agentId, resolvedGameId);
          if (rateLimited(am, logger)) return 'continue';
          if (am.ok && am.matchId) activeId = am.matchId;
        }
        if (activeId) {
          DRIVER.matchId = activeId;
        } else {
          const s = await client.matchmakingStatus(resolvedGameId);
          if (rateLimited(s, logger)) return 'continue';
          if (s.ok && s.status === 'matched' && s.matchId) {
            DRIVER.matchId = s.matchId;
          } else if (s.ok && s.status === 'not_queued') {
            // Server dropped/consumed our entry without a match. Requeue is
            // agent-initiated (072k §3) — wake the agent to re-queue, do not
            // auto-re-POST.
            logger.info?.('[steamedclaw-beta] no longer queued — waking agent to re-queue');
            DRIVER.phase = 'idle';
            DRIVER.queuedAt = 0;
            wakeAgent(
              api,
              'steamedclaw-beta-requeue',
              logger,
              'SteamedClaw Beta: your queue entry expired before a match formed. Call steamedclaw_beta_queue again to keep playing.',
            );
          } else if (expired) {
            logger.info?.('[steamedclaw-beta] queue entry aged out — waking agent to re-queue');
            DRIVER.phase = 'idle';
            DRIVER.queuedAt = 0;
            wakeAgent(
              api,
              'steamedclaw-beta-requeue',
              logger,
              'SteamedClaw Beta: your queue entry aged out without a match. Call steamedclaw_beta_queue again to keep playing.',
            );
          }
        }
      }
    }

    // Attach + open the game socket once matchId is known (covers both the WS
    // match_found that landed before bind, and the queue tool's immediate match).
    if (DRIVER.matchId && DRIVER.phase !== 'in_match' && DRIVER.phase !== 'terminal') {
      owner.attachMatch(
        DRIVER.boundSessionKey,
        DRIVER.matchId,
        DRIVER.matchGameId ?? resolvedGameId,
      );
      DRIVER.phase = 'in_match';
      receiver?.openGame?.(DRIVER.matchId);
      logger.info?.(`[steamedclaw-beta] matched matchId=${DRIVER.matchId}`);
    }
    if (!DRIVER.matchId) return 'continue';

    // Re-wake (072j §9): while a parked turn sits unconsumed, re-fire the wake on
    // a slow cadence (the park-time wake is lost if the agent was mid-turn).
    // Stops on its own: the pull marks the token used, game-over flips the phase.
    if (DRIVER.phase === 'in_match' && api && DRIVER.tickCount % REWAKE_EVERY_TICKS === 0) {
      const cur = owner.nextTurn(DRIVER.boundSessionKey);
      if (cur.status === 'your_turn') {
        wakeAgent(api, 'steamedclaw-beta-turn-rewake', logger, turnEventText(cur.sequence));
      }
    }

    // Turn delivery. Primary: /ws/game your_turn push. Fallback + safety net:
    // poll match state, PARK each new your_turn, finalize on game_over.
    const gameWsReady = Boolean(receiver && receiver.status().gameReady);
    if (gameWsReady && !safety) return 'continue';
    const st = await client.getState(DRIVER.matchId);
    if (rateLimited(st, logger)) return 'continue';
    if (!st.ok) return 'continue';
    if (typeof st.status === 'string' && TERMINAL_MATCH_STATUSES.has(st.status)) {
      finishMatch(owner, receiver, { api, logger }, 'http');
      return 'idle'; //  game over — the supervisor keeps ticking for a re-queue
    }
    if (st.status === 'your_turn') {
      await parkTurn(owner, st, 'http', { api, logger });
    }
    return 'continue';
  } catch (err) {
    logger.warn?.(`[steamedclaw-beta] tick error: ${err?.message ?? err}`);
    return 'continue';
  }
}

function makeSupervisorService(api, client, server, cfg, logger, receiver) {
  let timer = null;
  let stopped = false;
  function stop() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }
  return {
    id: 'steamedclaw-beta-supervisor',
    async start() {
      if (api.registrationMode !== 'full') return;
      stopped = false;
      if (timer) return; //  already ticking — start() can fire twice per boot (072i §11)
      // The supervisor runs for the plugin lifetime (like the published plugin's
      // services). It is NOT stopped after a game — supervisorTick idles on the
      // terminal phase and resumes when the agent re-queues, so play-again works.
      // Only the service lifecycle stop() (plugin unload) clears the interval.
      timer = setInterval(() => {
        if (stopped) return;
        void supervisorTick({ client, server, cfg, logger, receiver, api });
      }, cfg.tickMs ?? DEFAULT_TICK_MS);
    },
    async stop() {
      stopped = true;
      stop();
    },
  };
}

// ── Agent-facing tools (entry side): register, queue, leave_queue, info ───────
// All are factory tools (ctx)=>tool: queue needs ctx.sessionKey/agentId for the
// session bind; the rest are factories for surface consistency. `locked` short-
// circuits every server-touching tool to beta_stage_only when the configured
// server is not stage (072k §8 — the lock is enforced at register AND queue).

function makeRegisterTool({ client, server, locked, lockMsg, logger, receiver }) {
  return () => ({
    name: 'steamedclaw_beta_register',
    description: `Register this agent with the SteamedClaw Beta server (STAGE ONLY — this beta plays only against ${BETA_STAGE_SERVER}). Pass {name, model?} — name is your agent identity (1-64 chars, letters/numbers/hyphens/spaces/underscores, immutable, unique across SteamedClaw); model is optional (your LLM model id for stats). Use your SOUL-defined identity for name. Returns {ok, id?, name?, apiKey?, claimUrl?, verificationCode?, operatorNotice?, error?, message?}. On ok:true surface the operatorNotice in your next message so the operator can claim this agent. On error='already_registered' credentials exist — skip and call steamedclaw_beta_queue. On error='name_taken' pick a different name. On error='beta_stage_only' the operator pointed this beta at a non-stage server — surface the message. ${'After registering, call steamedclaw_beta_queue to play.'}`,
    parameters: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
          description: 'Agent name (1-64 chars; letters/numbers/hyphens/spaces/underscores).',
        },
        model: {
          type: 'string',
          description: 'Optional LLM model identifier (e.g. "claude-opus-4-8").',
        },
      },
      required: ['name'],
      additionalProperties: false,
    },
    async execute(_callId, args) {
      if (locked) return toolText({ ok: false, error: 'beta_stage_only', message: lockMsg });
      const { name, model } = args ?? {};
      if (typeof name !== 'string' || name.length === 0) {
        return toolText({
          ok: false,
          error: 'invalid_name',
          message: 'name is required (1-64 chars).',
        });
      }
      const existing = readCredentials();
      if (existing) {
        receiver?.ensureStarted?.();
        return toolText({
          ok: false,
          error: 'already_registered',
          name: existing.name ?? null,
          message: existing.name
            ? `Already registered as "${existing.name}". Use steamedclaw_beta_queue, steamedclaw_beta_get_turn, etc.`
            : 'Already registered. Use steamedclaw_beta_queue.',
        });
      }
      // Memoize an in-flight register so two parallel calls settle on one POST.
      if (!DRIVER.registerInFlight) {
        DRIVER.registerInFlight = (async () => {
          try {
            const r = await client.register(name, model);
            if (!r.ok) return r;
            try {
              writeCredentials(server, r.id, r.apiKey, name);
            } catch (err) {
              return {
                ok: false,
                error: 'persist_failed',
                serverSideRegistered: true,
                message: `Server registered the agent but local credentials persist failed: ${err?.message ?? err}.`,
              };
            }
            if (r.claimUrl) {
              try {
                writeClaimIfAbsent(r.claimUrl, r.verificationCode);
              } catch (err) {
                logger.warn?.(`[steamedclaw-beta] claim.md write failed: ${err?.message ?? err}`);
              }
            }
            client.setApiKey(r.apiKey);
            receiver?.ensureStarted?.();
            logger.info?.(`[steamedclaw-beta] registered agent "${name}" id=${r.id}`);
            const codeSuffix = r.verificationCode
              ? ` (verification code: ${r.verificationCode})`
              : '';
            const operatorNotice = r.claimUrl
              ? `I registered on SteamedClaw Beta (stage). Link me to your operator account at ${r.claimUrl}${codeSuffix}.`
              : '';
            return {
              ok: true,
              id: r.id,
              name,
              model: typeof model === 'string' && model.length > 0 ? model : null,
              apiKey: r.apiKey,
              claimUrl: r.claimUrl || null,
              verificationCode: r.verificationCode || null,
              operatorNotice,
            };
          } finally {
            DRIVER.registerInFlight = null;
          }
        })();
      }
      const payload = await DRIVER.registerInFlight;
      // Normalize transport error codes the LLM can act on.
      if (payload.ok === false && payload.error === 'name_taken') {
        payload.message =
          payload.message ?? `"${name}" is taken. Pick a different name and call again.`;
      }
      return toolText(payload);
    },
  });
}

function makeQueueTool({ client, server, locked, lockMsg, cfg, logger, receiver, coordinator }) {
  const allowedGames = Array.isArray(cfg.allowedGames) ? cfg.allowedGames : null;
  const maxGames = cfg.maxSimultaneousGames ?? BETA_MAX_SIMULTANEOUS_GAMES;
  return (ctx) => ({
    name: 'steamedclaw_beta_queue',
    description: `Enter SteamedClaw Beta matchmaking for a game (STAGE ONLY). Pass {gameId, lane?} — gameId is a SteamedClaw game id (call steamedclaw_beta_list_games to discover ids); optional lane is "fast" or "standard" (omit for the configured default "${BETA_DEFAULT_LANE}"). This binds your session and holds the queue. Returns {ok, status, matchId?, game?, position?, error?}. On status="matched" a match formed — call steamedclaw_beta_get_turn (it blocks until your turn). On status="queued" no pairing yet — call steamedclaw_beta_get_turn and it will wake/resolve when a match is found; do NOT spam queue. On status="already_queued" you are already in queue. On error="already_in_match" finish the current match first. On error="game_not_allowed" the operator restricted which games this beta may play. On error="beta_stage_only" the server is not stage. On error="not_registered" call steamedclaw_beta_register first. ${'After a match, loop steamedclaw_beta_get_turn / steamedclaw_beta_take_turn to game-over.'}`,
    parameters: {
      type: 'object',
      properties: {
        gameId: {
          type: 'string',
          description: 'Game id to queue for. Call steamedclaw_beta_list_games to discover ids.',
        },
        lane: {
          type: 'string',
          enum: BETA_LANES,
          description: 'Optional lane: "fast" or "standard". Omit for the configured default.',
        },
      },
      required: ['gameId'],
      additionalProperties: false,
    },
    async execute(_callId, args) {
      if (locked) return toolText({ ok: false, error: 'beta_stage_only', message: lockMsg });
      const { gameId, lane } = args ?? {};
      if (typeof gameId !== 'string' || gameId.length === 0) {
        return toolText({ ok: false, error: 'invalid_game', message: 'gameId is required.' });
      }
      if (lane !== undefined && !BETA_LANES.includes(lane)) {
        return toolText({
          ok: false,
          error: 'invalid_lane',
          message: `lane must be one of ${BETA_LANES.join(', ')}`,
        });
      }
      // v1 supports exactly one simultaneous game (072k §3).
      if (maxGames !== BETA_MAX_SIMULTANEOUS_GAMES) {
        return toolText({
          ok: false,
          error: 'max_simultaneous_unsupported',
          message: `SteamedClaw Beta v1 supports maxSimultaneousGames=${BETA_MAX_SIMULTANEOUS_GAMES} only; configured ${maxGames} is not accepted.`,
        });
      }
      if (allowedGames && !allowedGames.includes(gameId)) {
        return toolText({
          ok: false,
          error: 'game_not_allowed',
          message: `The operator restricted this beta to: ${allowedGames.join(', ')}. "${gameId}" is not allowed.`,
        });
      }
      const creds = readCredentials();
      if (!creds)
        return toolText({ ok: false, error: 'not_registered', message: NOT_REGISTERED_MESSAGE });
      // Defense in depth: refuse to queue if the persisted credentials point at a
      // non-stage server (e.g. a hand-edited credentials.md).
      if (!resolveStageServer(creds.server).ok) {
        return toolText({
          ok: false,
          error: 'beta_stage_only',
          message: `Stored credentials point at a non-stage server (${creds.server}); the beta refuses to queue. Re-register against ${BETA_STAGE_SERVER}.`,
        });
      }
      // Slot check at cap=1: an active match occupies the only slot.
      if (DRIVER.matchId && DRIVER.phase !== 'terminal') {
        return toolText({
          ok: false,
          error: 'already_in_match',
          matchId: DRIVER.matchId,
          game: DRIVER.gameId,
        });
      }

      const resolvedLane = lane ?? cfg.defaultLane ?? BETA_DEFAULT_LANE;
      client.setApiKey(creds.apiKey);
      // Fresh queue: the slot check above guaranteed no ACTIVE match, so clear any
      // prior (terminal) match — both the supervisor's per-match state and the
      // coordinator binding — so discovery + match_found pick up the NEW match and
      // get_turn reports no_match (not the finished game). This is what makes a
      // second sequential game ("play again") work.
      DRIVER.matchId = null;
      DRIVER.matchGameId = null;
      DRIVER.lastParkedSeq = -1;
      DRIVER.parkedVia = { ws: 0, http: 0 };
      // Bind the session (072k §3 — queue binds; no separate join tool) and clear
      // any leave_queue pause (symmetric resume). bindSession writes module-scope
      // state, so it does not require ownership — the binding is visible to the
      // owner's supervisor regardless of which instance binds.
      coordinator.bindSession(ctx?.sessionKey, ctx?.agentId);
      coordinator.clearSessionMatch(ctx?.sessionKey); //  drop any finished-match binding
      DRIVER.boundSessionKey = ctx?.sessionKey ?? DRIVER.boundSessionKey;
      DRIVER.boundAgentId = ctx?.agentId ?? DRIVER.boundAgentId;
      DRIVER.gameId = gameId;
      DRIVER.lane = resolvedLane;
      DRIVER.paused = false;
      receiver?.ensureStarted?.();

      let q;
      try {
        q = await client.queue(gameId, resolvedLane);
      } catch (err) {
        return toolText({ ok: false, error: 'queue_failed', message: String(err?.message ?? err) });
      }
      if (!q.ok) return toolText({ ok: false, error: q.error, httpStatus: q.httpStatus });
      if (q.status === 'matched' && q.matchId) {
        DRIVER.matchId = q.matchId;
        DRIVER.matchGameId = gameId;
        DRIVER.phase = 'queued'; //  supervisor's attach block opens the game socket
        return toolText({ ok: true, status: 'matched', matchId: q.matchId, game: gameId });
      }
      if (q.status === 'already_queued') {
        DRIVER.phase = 'queued';
        DRIVER.queuedAt = Date.now();
        return toolText({ ok: true, status: 'already_queued', game: gameId, position: q.position });
      }
      DRIVER.phase = 'queued';
      DRIVER.queuedAt = Date.now();
      return toolText({ ok: true, status: 'queued', game: gameId, position: q.position });
    },
  });
}

function makeLeaveQueueTool() {
  return () => ({
    name: 'steamedclaw_beta_leave_queue',
    description:
      'Pause SteamedClaw Beta matchmaking. After this call the plugin stops picking up NEW match_found pairings — an already-active match still plays out to game_over normally. The pause is in-memory only (a container restart resets to accepting). Resume by calling steamedclaw_beta_queue again — that clears the pause. Returns {ok:true, status:"queue_paused"} (or "already_paused").',
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    async execute() {
      if (DRIVER.paused) return toolText({ ok: true, status: 'already_paused' });
      DRIVER.paused = true;
      return toolText({ ok: true, status: 'queue_paused' });
    },
  });
}

function makeInfoTools({ client, locked, lockMsg }) {
  function requireReady() {
    if (locked) return { ok: false, error: 'beta_stage_only', message: lockMsg };
    const creds = readCredentials();
    if (!creds) return { ok: false, error: 'not_registered', message: NOT_REGISTERED_MESSAGE };
    client.setApiKey(creds.apiKey);
    return null;
  }
  return [
    () => ({
      name: 'steamedclaw_beta_list_games',
      description:
        'List the SteamedClaw game catalog (stage). Returns {ok, games:[{id, name, description, ...}], error?}. Call once after registering to discover gameIds for steamedclaw_beta_queue / steamedclaw_beta_get_rules.',
      parameters: { type: 'object', properties: {}, additionalProperties: false },
      async execute() {
        if (locked) return toolText({ ok: false, error: 'beta_stage_only', message: lockMsg });
        return toolText(await client.listGames());
      },
    }),
    () => ({
      name: 'steamedclaw_beta_get_rules',
      description:
        'Fetch the mechanical rules (action shapes, phases, edge cases) for a SteamedClaw game (stage). Pass {gameId}. Returns {ok, gameId, version, content, error?}. Call once per game before your first turn — without the rules, SteamedClaw-specific games (werewolf-7, liars-dice) will reject every action.',
      parameters: {
        type: 'object',
        properties: { gameId: { type: 'string', description: 'Game id to fetch rules for.' } },
        required: ['gameId'],
        additionalProperties: false,
      },
      async execute(_callId, args) {
        const guard = requireReady();
        if (guard) return toolText(guard);
        return toolText(await client.getRules(args?.gameId));
      },
    }),
    () => ({
      name: 'steamedclaw_beta_get_strategy',
      description:
        'Optional — fetch human-curated strategy hints for a SteamedClaw game (stage). Safe to skip; rules + your turn view are enough to play. Pass {gameId}. Returns {ok, gameId, version, content, error?}.',
      parameters: {
        type: 'object',
        properties: {
          gameId: { type: 'string', description: 'Game id to fetch strategy hints for.' },
        },
        required: ['gameId'],
        additionalProperties: false,
      },
      async execute(_callId, args) {
        const guard = requireReady();
        if (guard) return toolText(guard);
        return toolText(await client.getStrategy(args?.gameId));
      },
    }),
  ];
}

// ── Plugin registration ──────────────────────────────────────────────────────
export function registerBeta(api, opts = {}) {
  const cfg = { ...(api.pluginConfig ?? {}) }; //  local copy — never mutate api.pluginConfig
  const logger = api.logger ?? { info() {}, warn() {}, error() {} };

  // STAGE LOCK (072k §8): resolve + validate the configured server. Default is
  // stage; any non-stage override locks every server-touching tool to a
  // beta_stage_only error. There is no prod default.
  const stage = resolveStageServer(cfg.server);
  const locked = !stage.ok;
  const lockMsg = stage.message ?? '';
  const server = stage.ok ? stage.server : BETA_STAGE_SERVER;
  if (locked) {
    logger.warn?.(`[steamedclaw-beta] STAGE LOCK engaged — configured server rejected: ${lockMsg}`);
  }

  // opts.client lets tests inject a stub transport; production builds the real one.
  const client = opts.client ?? makeStageClient({ server, apiKey: readCredentials()?.apiKey });

  // Coordinator owns get_turn + take_turn + module-scope match state. httpSubmit
  // threaded via opts — NEVER stashed on `api` (Proxy-shadowed; 072i).
  const httpSubmit = ({ matchId, sequence, action }) =>
    client.submitAction(matchId, sequence, action);
  const { coordinator } = registerCoordinator(api, {
    httpSubmit,
    nextTurnBlockMs: cfg.nextTurnBlockMs ?? DEFAULT_NEXT_TURN_BLOCK_MS,
  });

  // WS Leg-1 receiver (072j). wsEnabled:false forces the pure HTTP pull floor.
  // Disabled entirely while locked (no server to talk to).
  const wsEnabled = !locked && cfg.wsEnabled !== false && opts.wsEnabled !== false;
  const receiver = wsEnabled
    ? makeWsReceiver({ api, server, logger, makeWebSocket: opts.makeWebSocket })
    : null;

  api.registerTool(makeRegisterTool({ client, server, locked, lockMsg, logger, receiver }), {
    name: 'steamedclaw_beta_register',
  });
  api.registerTool(
    makeQueueTool({ client, server, locked, lockMsg, cfg, logger, receiver, coordinator }),
    { name: 'steamedclaw_beta_queue' },
  );
  api.registerTool(makeLeaveQueueTool(), { name: 'steamedclaw_beta_leave_queue' });
  const infoFactories = makeInfoTools({ client, locked, lockMsg });
  api.registerTool(infoFactories[0], { name: 'steamedclaw_beta_list_games' });
  api.registerTool(infoFactories[1], { name: 'steamedclaw_beta_get_rules' });
  api.registerTool(infoFactories[2], { name: 'steamedclaw_beta_get_strategy' });

  // Supervisor service — full mode only, and only when not locked (a locked beta
  // does nothing server-side).
  if (api.registrationMode === 'full' && !locked) {
    api.registerService(makeSupervisorService(api, client, server, cfg, logger, receiver));
  }
  return { client, receiver, locked, server };
}

export default definePluginEntry({
  id: 'steamedclaw-plugin-beta',
  name: 'SteamedClaw Beta',
  description:
    'SteamedClaw Beta (stage-locked): a standalone WS Leg-1 plugin. The agent calls steamedclaw_beta_register → steamedclaw_beta_queue → steamedclaw_beta_get_turn (blocking pull) / steamedclaw_beta_take_turn (submit) to play hands-free against stage. Separate slug + data dir from the published plugin; plays ONLY against stage.steamedclaw.com.',
  configSchema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      server: {
        type: 'string',
        description:
          'SteamedClaw server base URL. STAGE-LOCKED: must be https://stage.steamedclaw.com (the default). Any other value disables the beta.',
        default: BETA_STAGE_SERVER,
      },
      defaultLane: {
        type: 'string',
        enum: BETA_LANES,
        default: BETA_DEFAULT_LANE,
        description:
          'Default match lane for queue calls when none is passed. "fast" (low-latency) or "standard" (heartbeat-paced; the beta default).',
      },
      maxSimultaneousGames: {
        type: 'number',
        default: BETA_MAX_SIMULTANEOUS_GAMES,
        description:
          'Concurrent games allowed. v1 accepts 1 only; any other value rejects queue calls.',
      },
      allowedGames: {
        type: 'array',
        items: { type: 'string' },
        description:
          'Optional allowlist of gameIds this beta may queue for. Omit to allow all published games.',
      },
      nextTurnBlockMs: {
        type: 'number',
        description:
          'How long steamedclaw_beta_get_turn blocks for a turn (ms). Default 20000, capped at 25000.',
      },
      wsEnabled: {
        type: 'boolean',
        description:
          'WS Leg-1 receive path (/ws/agent + /ws/game). Default true; false forces the HTTP polling fallback.',
      },
      tickMs: { type: 'number', description: 'Supervisor poll interval (ms). Default 4000.' },
      queueTtlMs: {
        type: 'number',
        description:
          'Max age (ms) of a queue entry before the supervisor wakes the agent to re-queue. Default 60000.',
      },
      serverTurnTimeoutMs: {
        type: 'number',
        description: 'The lane server turn timeout (ms); sets the parked-turn TTL.',
      },
      injectionSafetyMarginMs: {
        type: 'number',
        description: 'Margin subtracted from serverTurnTimeoutMs for the parked-turn TTL.',
      },
    },
  },
  register(api) {
    registerBeta(api);
  },
});
