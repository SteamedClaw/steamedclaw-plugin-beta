// Stage-only lock for the SteamedClaw Beta plugin (planning/072k §8).
//
// HARD REQUIREMENT (owner-directed): the beta plays ONLY against stage. The
// lock is enforced in CODE at registration AND queue time, not by convention.
// The configured `server` must resolve to EXACTLY the stage host — an exact
// host allowlist, never a substring match — and the default is stage with NO
// prod default. The only way to reach a non-stage server is an explicit
// operator override, which is exactly what this lock refuses (error
// `beta_stage_only`). The lock is a beta-only deviation, removed at GA when the
// slug/names revert (072k §8).

export const BETA_STAGE_SERVER = 'https://stage.steamedclaw.com';
export const BETA_STAGE_HOST = 'stage.steamedclaw.com';

// Resolve + validate the configured server against the stage allowlist.
//
// Returns { ok:true, server } with the normalized origin (protocol//host) when
// the server resolves to stage, or { ok:false, error:'beta_stage_only', message }
// otherwise. An absent/empty server resolves to the stage default — there is NO
// prod default, so the lock cannot be silently bypassed by omitting config.
export function resolveStageServer(server) {
  const raw = server == null || server === '' ? BETA_STAGE_SERVER : String(server);
  let url;
  try {
    url = new URL(raw);
  } catch {
    return {
      ok: false,
      error: 'beta_stage_only',
      message: `SteamedClaw Beta plays only against ${BETA_STAGE_SERVER}; "${raw}" is not a valid URL. Remove the server override — it defaults to stage.`,
    };
  }
  // Exact host allowlist. `url.host` includes any port, so stage on a
  // nonstandard port, a prod URL (steamedclaw.com), or a substring spoof
  // (stage.steamedclaw.com.evil.com) all fail to match.
  if (url.protocol !== 'https:' || url.host !== BETA_STAGE_HOST) {
    return {
      ok: false,
      error: 'beta_stage_only',
      message: `SteamedClaw Beta plays only against ${BETA_STAGE_SERVER}. Configured server "${raw}" is not allowed — the beta cannot register or queue against any other server. Remove the server override (it defaults to stage).`,
    };
  }
  return { ok: true, server: `${url.protocol}//${url.host}` };
}

// Convenience boolean for guards/tests.
export function isStageServer(server) {
  return resolveStageServer(server).ok;
}
