/**
 * Classify errors returned by `FleetClient.release()`.
 *
 * A release that comes back as a Relay 404 `agent_not_found` IS the successful
 * end state release aims for: the agent is already gone from Relay's view.
 * Treating it as a failure re-arms `#abandonStuckDispatch` →
 * `#releaseAndTerminateAgents` at ~1 Hz forever, which is what wedged cloud
 * dispatch: every sweep tick reprinted the same three ghost agents and
 * starved discovery entirely.
 *
 * Two shapes have to satisfy the check because the SDK's `RelayError` and a
 * plain-Error re-throw both reach the caller:
 *
 * 1. `rawCode === 'agent_not_found'` — the canonical field the Relay SDK
 *    stamps onto the error before rethrowing.
 * 2. `statusCode === 404 && code === 'not_found'` — a defensive fallback for
 *    a middleware that re-threw with the HTTP status preserved but dropped
 *    the SDK-specific `rawCode`. Neither field on its own is enough (a 404
 *    on a different route would misclassify).
 *
 * Anything else is a real release failure — a 503 host-unavailable is
 * retryable and must keep going through the failure path; a 5xx from the
 * broker itself is a real fault and must not silently succeed.
 */
export const isAgentAlreadyGoneOnRelease = (error: unknown): boolean => {
  const errAny = error as {
    rawCode?: unknown
    statusCode?: unknown
    code?: unknown
  } | null | undefined
  if (errAny?.rawCode === 'agent_not_found') return true
  if (errAny?.statusCode === 404 && errAny?.code === 'not_found') return true
  return false
}
