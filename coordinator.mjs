// Coordinator — module-scope match state + parked-turn token model (072j/072k).
//
// Folded from the live-validated coordinator POC (tools/steamedclaw-coordinator-
// poc/coordinator.mjs). Owns the cross-component protocol state at MODULE scope
// (so every register() instance of this resolved file shares one object — the
// instance-split guarantee the WS receiver also relies on) and the v1 PULL
// delivery contract: each turn is PARKED in module scope with a single-use
// token; the agent fetches it via the BLOCKING steamedclaw_beta_get_turn tool
// and submits via steamedclaw_beta_take_turn (token-validated).
//
// DELIVERY: v1 is PULL + heartbeat WAKE. Coordinator-side context injection
// (enqueueNextTurnInjection) was abandoned (072i blocker: the plugin api Proxy is
// closed after register() and the injection method is not lateCallable), so the
// driver wakes the agent with a content-carrying heartbeat instead (entry.mjs).
//
// Verified SDK shapes used here (memory reference_openclaw_plugin_sdk_shapes):
//   - factory tool = a bare function (ctx) => tool, registered via
//     api.registerTool(fn, { name }); ctx carries the trusted sessionKey/agentId
//   - tool execute(toolCallId, params) — params is the SECOND argument

// Blocking get_turn (072j): hard ceiling on how long the tool may hold its call
// waiting for a turn. The leg1 probe verified execute() survives ≥30s uncut
// (072i §11); we cap below that. The shipped DEFAULT (20s) lives in entry.mjs,
// which threads the config value in.
const MAX_NEXT_TURN_BLOCK_MS = 25000;

// ── Module-scope shared protocol state (REQUIRED at module scope) ────────────
const SHARED = {
  bindings: new Map(), //  sessionKey -> { agentId, gameId, matchId }
  matchToSession: new Map(), //  matchId    -> sessionKey
  tokens: new Map(), //  turnToken  -> { sessionKey, matchId, sequence, viewHash, used, attempts, packet, idempotencyKey }
  currentTokenByMatch: new Map(), //  matchId -> turnToken (newest; older are stale)
  transportByMatch: new Map(), //  matchId -> { isOpen(): bool, submit(frame): Promise<ack> } — dormant Leg-2 seam
  terminalMatches: new Set(), //  matchId set after game_over
  pending: null, //  single shared pending-action slot: { matchId, turnToken }
  owner: null, //  generation of the live full-mode coordinator (fence)
  ownerCoordinator: null, //  the live owner's coordinator object (module-scope handle)
  generation: 0, //  monotonically increasing across start() calls
  tokenSeq: 0, //  monotonic token uniqueness counter (no Math.random)
  turnWaiters: new Map(), //  sessionKey -> Set<resolve> — blocked get_turn calls (072j)
};

// Resolve (and clear) all blocked get_turn waiters for a session. Returns how
// many were woken — the driver skips the heartbeat wake when a waiter consumed
// the turn (the agent is mid-tool-call, already awake).
function notifyTurnWaiters(sessionKey) {
  const set = SHARED.turnWaiters.get(sessionKey);
  if (!set || set.size === 0) return 0;
  SHARED.turnWaiters.delete(sessionKey);
  let woken = 0;
  for (const resolve of set) {
    resolve();
    woken += 1;
  }
  return woken;
}

function notifyAllTurnWaiters() {
  for (const sessionKey of [...SHARED.turnWaiters.keys()]) notifyTurnWaiters(sessionKey);
}

// Park the caller until notifyTurnWaiters(sessionKey) fires or ms elapses.
function waitForTurnNotify(sessionKey, ms) {
  return new Promise((resolve) => {
    let set = SHARED.turnWaiters.get(sessionKey);
    if (!set) {
      set = new Set();
      SHARED.turnWaiters.set(sessionKey, set);
    }
    const entry = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      set.delete(entry);
      if (set.size === 0) SHARED.turnWaiters.delete(sessionKey); //  no empty-set leak
      resolve();
    }, ms);
    timer.unref?.();
    set.add(entry);
  });
}

// Deterministic test reset; also the model for a complete owner-scoped reset on stop().
export function __resetState() {
  notifyAllTurnWaiters(); //  release blocked get_turn calls so tests never hang
  SHARED.bindings.clear();
  SHARED.matchToSession.clear();
  SHARED.tokens.clear();
  SHARED.currentTokenByMatch.clear();
  SHARED.transportByMatch.clear();
  SHARED.terminalMatches.clear();
  SHARED.pending = null;
  SHARED.owner = null;
  SHARED.ownerCoordinator = null;
  SHARED.generation = 0;
  SHARED.tokenSeq = 0;
}

// Test-only view into module scope (assertions only — never used by logic).
export function __peekState() {
  return SHARED;
}

// The live full-mode coordinator instance, readable from any register-instance.
// The supervisor (entry.mjs) may run in a DIFFERENT instance than the one that
// won ownership; routing owner-gated operations through this handle lets it
// attach/enqueue/markTerminal. Null when no full-mode coordinator owns.
export function getOwnerCoordinator() {
  return SHARED.ownerCoordinator;
}

function hashView(view) {
  // Deterministic, dependency-free. Sufficient as a stale-turn fingerprint.
  const s = JSON.stringify(view ?? null);
  let h = 0;
  for (let i = 0; i < s.length; i += 1) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return `${s.length}:${h}`;
}

// ── Coordinator (registerService, full mode only) ──────────────────────────
// Owns transport/lifecycle and is the ONLY component allowed to mutate shared
// protocol state via owner-gated methods (attach/enqueue/markTerminal). Bind +
// the pull/submit reads are NOT owner-gated, so the tools resolve from any
// instance.
function makeCoordinator(api) {
  const logger = api.logger ?? { info() {}, warn() {}, error() {} };

  let myGeneration = 0;
  const isOwner = () => SHARED.owner === myGeneration && myGeneration !== 0;
  const assertOwner = () => {
    if (!isOwner()) {
      throw new Error(`coordinator not owner (mine=${myGeneration} live=${SHARED.owner})`);
    }
  };

  const coordinator = {
    get generation() {
      return myGeneration;
    },
    isOwner,
    sessionForMatch(matchId) {
      return SHARED.matchToSession.get(matchId) ?? null;
    },

    // Bind an agent session to the coordinator. Folded from the POC join tool,
    // but the beta binds at queue time (072k §3 — no separate join tool). A
    // plain module-scope write (readable from any instance) — preserves any
    // existing gameId/matchId for the session.
    bindSession(sessionKey, agentId) {
      if (!sessionKey) return false;
      const existing = SHARED.bindings.get(sessionKey);
      SHARED.bindings.set(sessionKey, {
        agentId: agentId ?? existing?.agentId,
        gameId: existing?.gameId,
        matchId: existing?.matchId,
      });
      return true;
    },

    // Clear a session's current match association (used by the queue tool when
    // the agent starts a NEW match after a previous one ended). Without this the
    // binding's stale matchId would make nextTurn keep reporting the finished
    // game instead of no_match. Not owner-gated (a plain module-scope write).
    clearSessionMatch(sessionKey) {
      const b = SHARED.bindings.get(sessionKey);
      if (b) b.matchId = null;
    },

    // Snapshot of current bindings (module scope, readable from any instance —
    // lets the supervisor see sessions bound by the queue tool).
    boundSessions() {
      return [...SHARED.bindings.entries()].map(([sessionKey, b]) => ({
        sessionKey,
        agentId: b.agentId,
        gameId: b.gameId,
        matchId: b.matchId,
      }));
    },

    // Dormant Leg-2 seam (#457 floor): when a /ws/game submit path exists the
    // coordinator publishes a transport entry so submit even in another instance
    // uses it. v1 submit is HTTP, so this stays empty and submit falls through
    // to httpSubmit — never regressing below the proven HTTP floor.
    registerTransport(matchId, entry) {
      assertOwner();
      SHARED.transportByMatch.set(matchId, entry);
    },
    dropTransport(matchId) {
      SHARED.transportByMatch.delete(matchId);
    },

    attachMatch(sessionKey, matchId, gameId) {
      assertOwner();
      const binding = SHARED.bindings.get(sessionKey);
      if (!binding) throw new Error(`no binding for session ${sessionKey}`);
      binding.matchId = matchId;
      if (gameId) binding.gameId = gameId;
      SHARED.matchToSession.set(matchId, sessionKey);
    },

    // Mint + PARK a turn: new token (older tokens for this match become stale),
    // build the packet, store it in module scope. The agent fetches it via
    // steamedclaw_beta_get_turn (pull).
    async enqueueTurn({
      matchId,
      sequence,
      view,
      phase = 'play',
      allowedActionTypes,
      legalActionSchema,
      instructions,
    }) {
      assertOwner();
      const sessionKey = SHARED.matchToSession.get(matchId);
      if (!sessionKey) throw new Error(`no session bound to match ${matchId}`);
      if (SHARED.terminalMatches.has(matchId)) {
        throw new Error(`match ${matchId} is terminal — refusing to enqueue`);
      }
      const binding = SHARED.bindings.get(sessionKey);

      SHARED.tokenSeq += 1;
      const turnToken = `tok:${matchId}:${sequence}:g${myGeneration}:n${SHARED.tokenSeq}`;
      const idempotencyKey = `steamedclaw-beta:${matchId}:${sequence}`;
      const packet = {
        kind: 'steamedclaw.turn',
        gameId: binding?.gameId,
        matchId,
        sequence,
        turnToken,
        phase,
        view,
        allowedActionTypes,
        legalActionSchema,
        instructions,
      };
      const rec = {
        sessionKey,
        matchId,
        sequence,
        viewHash: hashView(view),
        used: false,
        attempts: 1,
        packet,
        idempotencyKey,
      };
      SHARED.tokens.set(turnToken, rec);
      SHARED.currentTokenByMatch.set(matchId, turnToken);

      // Wake any get_turn call blocked on this session (072j). woken > 0 means
      // the agent is mid-tool-call — the driver then skips the heartbeat wake.
      const woken = notifyTurnWaiters(sessionKey);
      return { turnToken, packet, woken };
    },

    // PULL read (the v1 hard contract). A plain module-scope read — NOT
    // owner-gated, so the get_turn tool resolves it even from a non-owner
    // instance. Statuses: not_joined | no_match (queuing) | waiting (matched, no
    // fresh turn) | your_turn (act now) | game_over.
    nextTurn(sessionKey) {
      const binding = SHARED.bindings.get(sessionKey);
      if (!binding) return { status: 'not_joined' };
      const matchId = binding.matchId;
      if (!matchId) return { status: 'no_match' };
      if (SHARED.terminalMatches.has(matchId)) return { status: 'game_over', matchId };
      const turnToken = SHARED.currentTokenByMatch.get(matchId);
      if (!turnToken) return { status: 'waiting', matchId };
      const rec = SHARED.tokens.get(turnToken);
      if (!rec || rec.used) return { status: 'waiting', matchId };
      return {
        status: 'your_turn',
        matchId,
        turnToken,
        sequence: rec.sequence,
        gameId: rec.packet.gameId,
        phase: rec.packet.phase,
        view: rec.packet.view,
      };
    },

    // Blocking pull read (072j). Same statuses as nextTurn, but on 'waiting' or
    // 'no_match' it parks up to blockMs for enqueueTurn/markTerminal to land — a
    // WS your_turn push resolves it mid-call, so a responsive game plays with no
    // idle polling gap. On timeout it re-reads once and returns whatever the
    // state is ('waiting' is a valid answer; the agent calls again).
    async nextTurnWait(sessionKey, blockMs = 0) {
      const first = coordinator.nextTurn(sessionKey);
      if (first.status !== 'waiting' && first.status !== 'no_match') return first;
      const ms = Math.max(0, Math.min(blockMs ?? 0, MAX_NEXT_TURN_BLOCK_MS));
      if (ms === 0) return first;
      await waitForTurnNotify(sessionKey, ms);
      return coordinator.nextTurn(sessionKey);
    },

    // Token-validated submit (the POC submit_action logic). Returns the same
    // ack shape over WS or HTTP. Rejects stale/replayed/wrong-session/wrong-match
    // tokens. Not owner-gated — runs in whatever instance the host invokes the
    // tool in, reading the shared token store.
    async submitAction(sessionKey, turnToken, action, httpSubmit) {
      if (!turnToken) return { ok: false, error: 'missing_token' };
      const rec = SHARED.tokens.get(turnToken);
      if (!rec) return { ok: false, error: 'unknown_token' };
      if (rec.used) return { ok: false, error: 'replayed_token' };
      if (rec.sessionKey !== sessionKey) return { ok: false, error: 'wrong_session' };
      const boundMatch = SHARED.bindings.get(sessionKey)?.matchId;
      if (rec.matchId !== boundMatch) return { ok: false, error: 'wrong_match' };
      if (SHARED.currentTokenByMatch.get(rec.matchId) !== turnToken) {
        return { ok: false, error: 'stale_token' };
      }
      if (SHARED.terminalMatches.has(rec.matchId)) return { ok: false, error: 'match_terminal' };
      // Single shared pending-action slot (cross-instance): reject concurrent
      // submits against the active match.
      if (SHARED.pending) return { ok: false, error: 'action_in_flight' };
      // Claim the token + slot before any await so a concurrent call in another
      // instance loses the race deterministically.
      rec.used = true;
      SHARED.pending = { matchId: rec.matchId, turnToken };
      try {
        const ack = await submitViaTransport({
          matchId: rec.matchId,
          sequence: rec.sequence,
          action,
          httpSubmit,
        });
        if (ack.status === 'game_over') {
          SHARED.terminalMatches.add(rec.matchId);
          SHARED.currentTokenByMatch.delete(rec.matchId);
          return { ok: true, ...ack };
        }
        // INVARIANT: take_turn never surfaces an actionable turn or a turnToken —
        // get_turn is the SOLE source of actionable turns + tokens (enqueueTurn is
        // the only token-minting site). A fast opponent makes the HTTP submit
        // response carry the NEXT turn's state (status:"your_turn" + view/sequence
        // but NO coordinator-minted token); echoing it produced a tokenless
        // "your_turn" the agent could not act on (replayed_token on the old token,
        // no new token). Return a NEUTRAL ack that routes the agent back to
        // get_turn, which blocks until the next turn is parked WITH a fresh token.
        // This removes the broken shortcut without adding a second token-minting
        // site or any extra calls (the play loop already round-trips get_turn).
        return { ok: true, status: 'submitted', next: 'call steamedclaw_beta_get_turn' };
      } catch (err) {
        // Failed submit: release the token so a re-issued packet can retry.
        rec.used = false;
        return { ok: false, error: 'submit_failed', message: String(err?.message ?? err) };
      } finally {
        SHARED.pending = null;
      }
    },

    markTerminal(matchId) {
      assertOwner();
      SHARED.terminalMatches.add(matchId);
      const tok = SHARED.currentTokenByMatch.get(matchId);
      if (tok) SHARED.tokens.delete(tok);
      SHARED.currentTokenByMatch.delete(matchId);
      // Wake a blocked get_turn so the agent learns game_over immediately.
      const sessionKey = SHARED.matchToSession.get(matchId);
      const woken = sessionKey ? notifyTurnWaiters(sessionKey) : 0;
      return { woken };
    },

    service: {
      id: 'steamedclaw-beta-coordinator',
      async start() {
        SHARED.generation += 1;
        myGeneration = SHARED.generation;
        SHARED.owner = myGeneration; //  newest full-mode service wins ownership
        SHARED.ownerCoordinator = coordinator; //  publish the live owner handle
        logger.info?.(`[steamedclaw-beta] coordinator owner=${myGeneration}`);
      },
      async stop() {
        // Only the live owner performs the owner-scoped reset; a stale/duplicate
        // service stopping must not wipe the live owner's state.
        if (SHARED.owner === myGeneration) {
          notifyAllTurnWaiters(); //  release blocked get_turn calls
          SHARED.bindings.clear();
          SHARED.matchToSession.clear();
          SHARED.tokens.clear();
          SHARED.currentTokenByMatch.clear();
          SHARED.transportByMatch.clear();
          SHARED.terminalMatches.clear();
          SHARED.pending = null;
          SHARED.owner = null;
          SHARED.ownerCoordinator = null;
        }
        myGeneration = 0;
      },
    },
  };
  return coordinator;
}

// ── Transport facade: WS-preferred, HTTP fallback (the #457 correctness floor).
// Reads the module-scope transport registry so any instance resolves the same
// socket the coordinator opened; absent ⇒ HTTP. Identical ack shape either way.
// v1 leaves transportByMatch empty (HTTP submit), so this is HTTP in practice.
async function submitViaTransport({ matchId, sequence, action, httpSubmit }) {
  const entry = SHARED.transportByMatch.get(matchId);
  if (entry && entry.isOpen()) {
    const ack = await entry.submit({ type: 'action', sequence, payload: action });
    return { ...ack, via: 'ws' };
  }
  const ack = await httpSubmit({ matchId, sequence, action });
  return { ...ack, via: 'http' };
}

function toolText(obj) {
  return { content: [{ type: 'text', text: JSON.stringify(obj) }] };
}

// The hard play loop — stated identically in every tool description so the agent
// has one unambiguous contract. Mechanics live in tool descriptions/results,
// never in agent SOUL/persona.
export const PLAY_LOOP =
  'SteamedClaw Beta play loop: call steamedclaw_beta_register once (identity), then steamedclaw_beta_queue to enter matchmaking; then call steamedclaw_beta_get_turn repeatedly until it returns status "your_turn"; then call steamedclaw_beta_take_turn with that turnToken and a legal action; then go back to steamedclaw_beta_get_turn. Repeat until steamedclaw_beta_get_turn returns status "game_over".';

// PULL delivery tool — the v1 hard contract. The agent calls this on each turn.
// With WS Leg-1 (072j) it BLOCKS briefly: a turn pushed over WebSocket resolves
// the call mid-wait, so a responsive game plays without polling gaps.
export function makeGetTurnFactory(coordinator, { nextTurnBlockMs } = {}) {
  const blockMs = Math.max(0, Math.min(nextTurnBlockMs ?? 0, MAX_NEXT_TURN_BLOCK_MS));
  const blockSecs = Math.round(blockMs / 1000);
  return (ctx) => ({
    name: 'steamedclaw_beta_get_turn',
    description: `Fetch your current SteamedClaw Beta turn. This call WAITS up to ~${blockSecs}s for your turn to arrive, then returns status: "not_joined" (you have not queued — call steamedclaw_beta_queue first), "no_match" (still matchmaking — call again), "waiting" (matched, opponent's turn — call again), "your_turn" (act now: pass the returned turnToken to steamedclaw_beta_take_turn), or "game_over" (the match ended — stop). Just call it again whenever it returns no_match or waiting. ${PLAY_LOOP}`,
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    async execute() {
      const sessionKey = ctx?.sessionKey;
      if (!sessionKey) return toolText({ ok: false, error: 'no_session' });
      const r = await coordinator.nextTurnWait(sessionKey, blockMs);
      // Teach the loop EXIT and RE-ENTRY, not just the loop (072j §9): an agent
      // that stops on 'waiting' is woken by a bare heartbeat carrying no context
      // — the result text is the only place to pre-load what that wake means.
      // Mechanics live in tool results, never in agent SOUL/persona.
      if (r.status === 'not_joined') {
        r.guidance = 'You have not queued yet. Call steamedclaw_beta_queue with a gameId first.';
      } else if (r.status === 'waiting' || r.status === 'no_match') {
        r.guidance =
          'Either call steamedclaw_beta_get_turn again, or stop — a heartbeat will wake you when it is your turn. When you wake from a heartbeat, call steamedclaw_beta_get_turn FIRST.';
      } else if (r.status === 'your_turn') {
        r.guidance =
          'Act now: call steamedclaw_beta_take_turn with this turnToken and one legal action, then call steamedclaw_beta_get_turn again.';
      } else if (r.status === 'game_over') {
        r.guidance = 'The match is over — stop calling SteamedClaw Beta tools.';
      }
      return toolText({ ok: true, ...r });
    },
  });
}

export function makeTakeTurnFactory(coordinator, { httpSubmit }) {
  return (ctx) => ({
    name: 'steamedclaw_beta_take_turn',
    description: `Submit your one action for the current SteamedClaw Beta turn. Pass the turnToken that steamedclaw_beta_get_turn returned with status "your_turn", plus your chosen action (the move shape is game-specific, e.g. {type:"move", position:4} for tic-tac-toe). After submitting, call steamedclaw_beta_get_turn again. ${PLAY_LOOP}`,
    parameters: {
      type: 'object',
      properties: {
        turnToken: {
          type: 'string',
          description:
            'The turnToken from the most recent steamedclaw_beta_get_turn "your_turn" result.',
        },
        action: { type: 'object', description: 'Your chosen action for this turn.' },
      },
      required: ['turnToken', 'action'],
      additionalProperties: false,
    },
    async execute(_callId, args) {
      const sessionKey = ctx?.sessionKey;
      if (!sessionKey) return toolText({ ok: false, error: 'no_session' });
      const { turnToken, action } = args ?? {};
      const r = await coordinator.submitAction(sessionKey, turnToken, action, httpSubmit);
      return toolText(r);
    },
  });
}

// ── Plugin registration. entry.mjs wraps this via definePluginEntry; tests call
// register() directly with a mock api. opts.httpSubmit MUST be threaded
// explicitly — NOT stashed on `api`. The live OpenClaw plugin `api` is a Proxy:
// a custom property set on it reads back as a late-callable stub (returns null,
// no HTTP), so a stashed api.httpSubmit silently no-ops while reporting ok:true
// (072i). Passing it through opts keeps the real transport in a closure the
// Proxy can't shadow. Falls back to api.httpSubmit only for the plain test mock.
export function register(api, opts = {}) {
  const coordinator = makeCoordinator(api);
  const httpSubmit = opts.httpSubmit ?? api.httpSubmit;
  const nextTurnBlockMs = opts.nextTurnBlockMs ?? 0;
  // Tools register in ALL modes (the instance split is the whole point). Factories
  // are bare functions; the name is passed via opts so the host (and the tests)
  // can identify the tool before invoking it.
  api.registerTool(makeGetTurnFactory(coordinator, { nextTurnBlockMs }), {
    name: 'steamedclaw_beta_get_turn',
  });
  api.registerTool(makeTakeTurnFactory(coordinator, { httpSubmit }), {
    name: 'steamedclaw_beta_take_turn',
  });
  if (api.registrationMode === 'full') {
    api.registerService(coordinator.service);
  }
  return { coordinator };
}

export default { register };
