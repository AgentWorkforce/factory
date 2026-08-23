/**
 * Per-call deadlines for relayfile operations (#351).
 *
 * `@relayfile/sdk`'s `performRequest` attaches an `AbortSignal` to its `fetch`
 * only when the caller supplies one, and nothing in Factory did — so every
 * relayfile read was a bare `fetch()` with no deadline. On 2026-08-23 one of
 * them stopped returning and the readiness reconcile cycle that issued it never
 * finished: 22 minutes with `consecutiveFailures: 0`, because a hang takes
 * neither the success nor the failure path.
 *
 * The sweep-level deadline (#296) cannot cover this. It is 90 minutes by
 * design — below realistic cold-mirror hydration a slow boot becomes a crash
 * loop — and it rejects the *wait* while `runOnce()` keeps running, so the next
 * cycle coalesces onto the same wedged promise and waits again. A per-call
 * bound can be two orders of magnitude tighter, because a cold sweep's cost is
 * spread across thousands of calls rather than concentrated in one, and its
 * rejection unwinds the sweep — releasing the discovery lease, so the next
 * cycle starts clean.
 */

/** Fallback deadline for one relayfile call. See `config/schema.ts` for why. */
export const DEFAULT_RELAYFILE_OPERATION_TIMEOUT_MS = 5 * 60_000

/**
 * A relayfile call that did not answer inside its budget.
 *
 * The message is built only from code-controlled values — a closed set of
 * operation and phase literals plus one integer — because it is persisted into
 * the operator-facing `readinessReconcile.lastError`. The class name is what
 * reaches the unauthenticated surface, through the `error-class` allowlist.
 */
export class RelayfileOperationTimeoutError extends Error {
  readonly code = 'FACTORY_RELAYFILE_OPERATION_TIMEOUT'

  constructor(
    readonly operation: string,
    readonly timeoutMs: number,
    readonly phase?: string,
  ) {
    super(
      `relayfile ${operation} did not respond within ${timeoutMs}ms` +
      `${phase === undefined ? '' : ` (${phase})`}`,
    )
    this.name = 'RelayfileOperationTimeoutError'
  }
}

/** The budget to apply, or `undefined` when the caller configured none. */
export const relayfileCallBudgetMs = (timeoutMs: number | undefined): number | undefined =>
  timeoutMs !== undefined && Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : undefined

/**
 * The tighter of several budgets, ignoring the ones that impose none.
 *
 * A caller's explicit per-call timeout must *cap* the client-wide budget rather
 * than replace it: a config that tightened `relayfileOperationTimeoutMs` below
 * an explicit argument would otherwise leave the transport running past the
 * orchestrator's backstop, which abandons the wait instead of cancelling the
 * call — the exact behaviour this module exists to avoid.
 */
export const tighterRelayfileBudgetMs = (
  ...timeoutsMs: ReadonlyArray<number | undefined>
): number | undefined => {
  const budgets = timeoutsMs
    .map((timeoutMs) => relayfileCallBudgetMs(timeoutMs))
    .filter((budget): budget is number => budget !== undefined)
  return budgets.length === 0 ? undefined : Math.min(...budgets)
}

/**
 * The same timeout, carrying the phase the orchestrator knows and the transport
 * does not.
 *
 * The transport deadline is meant to win the race, so without this the
 * persisted `lastError` would read `relayfile listTree did not respond within
 * 300000ms` with no way to tell one list or read context from another — which
 * is most of what naming the call was for.
 */
export function relayfileTimeoutWithPhase(error: unknown, phase: string | undefined): unknown {
  if (phase === undefined) return error
  if (!(error instanceof RelayfileOperationTimeoutError) || error.phase !== undefined) return error
  const enriched = new RelayfileOperationTimeoutError(error.operation, error.timeoutMs, phase)
  enriched.cause = error
  return enriched
}

/** True for the abort a `signal` deadline raises, in either transport's shape. */
export function isRelayfileCallAbort(error: unknown): boolean {
  if (error instanceof RelayfileOperationTimeoutError) return true
  if (!(error instanceof Error)) return false
  return error.name === 'TimeoutError' || error.name === 'AbortError'
}

/** A cancellation signal plus the means to release its timer. */
export interface RelayfileCallDeadline {
  /** Undefined when no budget applies, so the call is made exactly as before. */
  readonly signal?: AbortSignal
  /** Releases the timer. Always call it, or a settled call leaves one pending. */
  dispose(): void
}

/**
 * A deadline that cancels the request itself once the budget is spent.
 *
 * Preferred over racing the wait: an abandoned wait leaves the socket open and
 * the SDK's own retry loop running, and a read the SDK has cached as in-flight
 * would hand the *next* cycle the same wedged promise.
 *
 * Deliberately not `AbortSignal.timeout()`, whose timer cannot be cancelled: at
 * a five-minute budget every completed read would leave a five-minute timer and
 * a retained signal behind it, so a busy sweep accumulates thousands. This pair
 * clears the timer the moment the call settles.
 *
 * The abort reason is the named error itself, so a transport that surfaces
 * `signal.reason` (undici's `fetch` does) already reports which call it was.
 */
export function relayfileCallDeadline(
  operation: string,
  timeoutMs: number | undefined,
): RelayfileCallDeadline {
  const budget = relayfileCallBudgetMs(timeoutMs)
  if (budget === undefined) return { dispose: () => undefined }

  const controller = new AbortController()
  const timer = setTimeout(
    () => controller.abort(new RelayfileOperationTimeoutError(operation, budget)),
    budget,
  )
  timer.unref?.()
  return { signal: controller.signal, dispose: () => clearTimeout(timer) }
}

const CALL_TIMED_OUT = Symbol('relayfile-call-timed-out')

type CallOutcome<T> = { ok: true; value: T } | { ok: false; error: unknown }

/**
 * Await `start()`, or give up on it once `timeoutMs` is spent.
 *
 * The backstop behind `relayfileCallSignal`: it covers mount implementations
 * that cannot honour a signal, and operations whose side effect makes real
 * cancellation unsafe. Like `RelayFleetClient.#withinDeadline` (#306/#307) it
 * abandons the *wait*, not the call — so the outcome is folded once, and a late
 * rejection from the abandoned call cannot surface as an unhandled rejection.
 *
 * Takes a thunk rather than a promise so an unusable budget is decided before
 * the request is made.
 */
export async function withRelayfileCallDeadline<T>(
  operation: string,
  phase: string | undefined,
  timeoutMs: number | undefined,
  start: () => Promise<T>,
): Promise<T> {
  const budget = relayfileCallBudgetMs(timeoutMs)
  if (budget === undefined) return await start()

  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const inFlight: Promise<CallOutcome<T>> = start().then(
      (value) => ({ ok: true, value }) as const,
      (error: unknown) => ({ ok: false, error }) as const,
    )
    const outcome = await Promise.race<CallOutcome<T> | typeof CALL_TIMED_OUT>([
      inFlight,
      new Promise<typeof CALL_TIMED_OUT>((resolve) => {
        timer = setTimeout(() => resolve(CALL_TIMED_OUT), budget)
        timer.unref?.()
      }),
    ])
    if (outcome === CALL_TIMED_OUT) throw new RelayfileOperationTimeoutError(operation, budget, phase)
    if (outcome.ok) return outcome.value
    throw outcome.error
  } finally {
    if (timer) clearTimeout(timer)
  }
}
