import { readFileSync } from 'node:fs'

/**
 * A mount failure caused by the cloud session lacking the filesystem scopes the
 * relayfile mount needs (e.g. `relayfile:fs:read`). Unlike a stale mount or a
 * transient launch failure, re-launching cannot fix this — the token itself is
 * under-scoped — so callers must treat it as terminal: surface an actionable
 * remediation instead of scheduling an infinite refresh loop, and refuse to
 * spawn/resume agents against the resulting stale mirror.
 */
export class MountAuthScopeError extends Error {
  readonly missingScope?: string
  readonly cause?: unknown

  constructor(message: string, options?: { missingScope?: string; cause?: unknown }) {
    super(message)
    this.name = 'MountAuthScopeError'
    this.missingScope = options?.missingScope
    this.cause = options?.cause
  }
}

export interface MountAuthErrorDetails {
  /** The scope named in the 403, when the server reported one (e.g. `fs:read`). */
  missingScope?: string
  /** The raw diagnostic text the classification matched against. */
  detail: string
}

// The relayfile server reports a scope shortfall as an HTTP 403 whose body names
// the required scope. The mount SDK surfaces it verbatim in log/error text and
// in state.json's `lastError.message`, e.g.:
//   "http 403 forbidden: missing required scope: fs:read"
// A prolonged shortfall also shows up as a stalled bootstrap that never clears.
const MISSING_SCOPE_RE = /missing required scope:\s*([\w:*/-]+)/i
const FORBIDDEN_RE = /\b403\b|forbidden/i
const BOOTSTRAP_STALL_RE = /bootstrap[_ ]stall/i

/**
 * Classify a diagnostic string as a filesystem-scope authorization failure.
 * Returns the matched details, or `undefined` when the text is some other
 * (transient / unrelated) failure.
 */
export function classifyMountAuthError(text: string | undefined | null): MountAuthErrorDetails | undefined {
  if (!text) return undefined
  const scopeMatch = MISSING_SCOPE_RE.exec(text)
  if (scopeMatch) {
    return { missingScope: scopeMatch[1], detail: text }
  }
  // A stalled bootstrap that also reports a forbidden/403 is the same
  // under-scoped session even when the specific scope name is absent.
  if (BOOTSTRAP_STALL_RE.test(text) && FORBIDDEN_RE.test(text)) {
    return { detail: text }
  }
  return undefined
}

/**
 * Read a relayfile mount `state.json` and, if its most recent error is a
 * filesystem-scope authorization failure, return the details. Returns
 * `undefined` when the file is absent, unreadable, or the error is unrelated.
 */
export function readMountAuthErrorFromState(stateFilePath: string): MountAuthErrorDetails | undefined {
  let raw: string
  try {
    raw = readFileSync(stateFilePath, 'utf8')
  } catch {
    return undefined
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return undefined
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined
  const lastError = (parsed as { lastError?: unknown }).lastError
  if (typeof lastError !== 'object' || lastError === null) return undefined
  const { message, kind, code } = lastError as { message?: unknown; kind?: unknown; code?: unknown }
  const fields = [message, kind, code].filter((v): v is string => typeof v === 'string')
  for (const field of fields) {
    const details = classifyMountAuthError(field)
    if (details) return details
  }
  return undefined
}

/**
 * Build the single, actionable remediation message shown when the mount cannot
 * obtain its filesystem scopes. Kept in one place so preflight, the supervisor,
 * and startup all speak with one voice.
 */
export function mountAuthRemediation(details?: MountAuthErrorDetails): string {
  const scope = details?.missingScope ? ` (missing ${details.missingScope})` : ''
  return (
    `[factory] Agent Relay Cloud session lacks the filesystem scope the mount needs${scope}. ` +
    'Re-authenticate with a session that can mint relayfile fs:read/fs:write for this workspace ' +
    '(an organization owner/admin session); a plain `agent-relay cloud login` grants only ' +
    'follow-user/cli:auth and cannot mint fs scopes. Factory will not spawn agents against a ' +
    'read-denied mirror.'
  )
}
