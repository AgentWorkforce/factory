/**
 * One aggregate deadline for one discovery sweep.
 *
 * ## Why a per-call bound is not enough
 *
 * Three separate unbounded dependency calls have wedged this sweep in a single
 * day, each one real, each one bounded, and each time the wedge came back one
 * layer down:
 *
 *   - the FACTORY_STATE Durable Object calls (factory-cloud#78),
 *   - the relayfile change-feed tail reads (#368, shipped in 0.1.75),
 *   - the retry of the now-bounded call, which is itself unbounded.
 *
 * The pattern is structural, not incidental. A per-call deadline bounds ONE
 * await; a sweep is thousands of awaits plus every retry loop between them, so
 * "every call is bounded" never adds up to "the sweep is bounded". The
 * deployed 0.1.75 proves it: `readinessReconcile.state` reached `retrying`,
 * which means #368's deadline genuinely fires, and `inFlightMs` still climbed
 * with wall clock to 31 minutes on a 60-second interval.
 *
 * This module bounds the total instead. Time is charged against ONE timer for
 * the whole sweep, so it does not matter which await is slow, how many there
 * are, or how many times the sweep retries one of them: the sum cannot exceed
 * the budget. The next unbounded call is then a degraded sweep rather than the
 * end of dispatch.
 *
 * ## What it can and cannot interrupt
 *
 * `run()` is a race, not a cancellation — the same honest limitation #368
 * documented for `withRelayfileCallDeadline`, and stated here for the same
 * reason: the next lane over has to be able to reason about what is left.
 *
 * It CAN abandon an in-flight await. Expiry rejects the sweep from inside
 * `#runOnceWithDiscoveryFence`, so the sweep's own teardown runs: the lease is
 * released and `#runOnceInFlight` is cleared, which is what lets the next cycle
 * start a fresh sweep instead of coalescing onto the wedged one. That is the
 * whole difference from the #296 sweep deadline, which rejected the *caller's
 * wait* and left `runOnce()` running for every later cycle to coalesce onto.
 *
 * It CANNOT stop the abandoned work. The socket stays open, the SDK's own
 * retry loop keeps running, and any side effect already in flight still lands.
 * Two things narrow that:
 *
 *   - `signal` aborts at expiry, so anything downstream that honours an
 *     `AbortSignal` is really cancelled. Nothing in the sweep consumes it yet
 *     (the relayfile client mints its own per-call signal, and that file is
 *     owned by another lane this week) — it is exported so the wiring is a
 *     one-line change rather than a redesign.
 *   - `assertNotExpired()` is a between-await check, which is worth exactly
 *     what #368 said it was worth — nothing against a call that never returns
 *     — but it does make an abandoned pass unwind at its next loop iteration
 *     if it ever regains control, instead of running to completion beside the
 *     sweep that replaced it.
 */

/** The phases a sweep can be abandoned in. A closed set: see the error below. */
export type DiscoverySweepPhase =
  | 'fleet-control-plane-probe'
  | 'discovery-lease-claim'
  | 'discovery-backoff-wait'
  | 'discovery-session'
  | 'run-once'
  | 'discovery-checkpoint'
  | 'discovery-renewal-stop'
  | 'discovery-commit'

/**
 * A sweep that did not finish inside its aggregate budget.
 *
 * Built only from code-controlled values — one integer and one literal from the
 * closed set above — because it is persisted verbatim into the operator-facing
 * `readinessReconcile.lastError`, and the class name reaches the
 * unauthenticated health surface through the `error-class` allowlist.
 *
 * The phase is the diagnostic that matters: it names the await the sweep was
 * abandoned on, which is the one thing no per-call bound could report once the
 * sweep was already wedged.
 */
export class DiscoverySweepBudgetExceededError extends Error {
  readonly code = 'FACTORY_DISCOVERY_SWEEP_BUDGET_EXCEEDED'

  constructor(
    readonly budgetMs: number,
    readonly phase: DiscoverySweepPhase,
  ) {
    super(`discovery sweep exceeded its ${budgetMs}ms budget while waiting on ${phase}`)
    this.name = 'DiscoverySweepBudgetExceededError'
  }
}

/** The budget to apply, or `undefined` when the caller configured none. */
export const discoverySweepBudgetMs = (timeoutMs: number | undefined): number | undefined =>
  timeoutMs !== undefined && Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : undefined

export interface DiscoverySweepBudget {
  /** The applied budget, or `undefined` when the sweep is deliberately unbounded. */
  readonly budgetMs?: number
  /** Aborted the moment the budget is spent. Reason is the error below. */
  readonly signal: AbortSignal
  /** True once the budget is spent, whether or not anything is awaiting. */
  expired(): boolean
  /** Await `start()`, or abandon that wait once the sweep's budget is spent. */
  run<T>(phase: DiscoverySweepPhase, start: () => Promise<T>): Promise<T>
  /** Throw if the budget is already spent. A between-await check; see above. */
  assertNotExpired(phase: DiscoverySweepPhase): void
  /** Releases the timer. Always call it, or a settled sweep leaves one pending. */
  dispose(): void
}

const BUDGET_EXPIRED = Symbol('discovery-sweep-budget-expired')

type SweepOutcome<T> = { ok: true; value: T } | { ok: false; error: unknown }

/**
 * Start the clock for one sweep.
 *
 * Deliberately not `AbortSignal.timeout()`: its timer cannot be cancelled, so a
 * sweep that finishes in four seconds would leave a ninety-minute timer and a
 * retained signal behind it. `dispose()` clears the timer the moment the sweep
 * settles.
 *
 * One timer and one expiry promise are shared by every `run()` on this budget.
 * That sharing is the aggregate property: a sweep does not get a fresh budget
 * per await, so elapsed time accumulates across phases and across retries.
 */
export function startDiscoverySweepBudget(timeoutMs: number | undefined): DiscoverySweepBudget {
  const budgetMs = discoverySweepBudgetMs(timeoutMs)
  const controller = new AbortController()
  let expired = false
  let timer: ReturnType<typeof setTimeout> | undefined
  let expire: () => void = () => undefined
  // Never rejects, so an unawaited race arm cannot surface as an unhandled
  // rejection when the sweep finishes first.
  const expiry = new Promise<typeof BUDGET_EXPIRED>((resolve) => {
    expire = () => resolve(BUDGET_EXPIRED)
  })

  if (budgetMs !== undefined) {
    timer = setTimeout(() => {
      expired = true
      // `run()` supplies the phase on the throw; the abort reason cannot know
      // which await was in flight, so it names the sweep as a whole.
      controller.abort(new DiscoverySweepBudgetExceededError(budgetMs, 'run-once'))
      expire()
    }, budgetMs)
    // Deliberately NOT unref'd, unlike the per-call deadline in
    // `relayfile-operation-timeout.ts`. That one is created thousands of times
    // per sweep and can afford to lose a race with process exit; this one is
    // the guarantee. Under a one-shot `runOnce()` whose only pending work is a
    // promise nothing else references, an unref'd timer lets Node exit before
    // the budget fires — the command would return without ever reporting the
    // wedge or releasing the lease. It lives for at most one budget and
    // `dispose()` clears it from a `finally` on every path.
  }

  const assertNotExpired = (phase: DiscoverySweepPhase): void => {
    if (expired && budgetMs !== undefined) throw new DiscoverySweepBudgetExceededError(budgetMs, phase)
  }

  return {
    ...(budgetMs === undefined ? {} : { budgetMs }),
    signal: controller.signal,
    expired: () => expired,
    assertNotExpired,
    async run<T>(phase: DiscoverySweepPhase, start: () => Promise<T>): Promise<T> {
      if (budgetMs === undefined) return await start()
      // Decided before the call is made, so an already-spent budget cannot
      // start new work against the dependency it is abandoning.
      assertNotExpired(phase)
      // The outcome is folded once. A late rejection from the abandoned call
      // then has a handler attached and cannot crash the process.
      const inFlight: Promise<SweepOutcome<T>> = start().then(
        (value) => ({ ok: true, value }) as const,
        (error: unknown) => ({ ok: false, error }) as const,
      )
      const outcome = await Promise.race<SweepOutcome<T> | typeof BUDGET_EXPIRED>([inFlight, expiry])
      if (outcome === BUDGET_EXPIRED) throw new DiscoverySweepBudgetExceededError(budgetMs, phase)
      if (outcome.ok) return outcome.value
      throw outcome.error
    },
    dispose: () => {
      if (timer) clearTimeout(timer)
    },
  }
}

/**
 * How long one sweep-teardown step may take.
 *
 * Teardown cannot run under the aggregate budget: by the time it runs the
 * budget is spent by construction, so every step would reject and the lease
 * would never be released — which is the half of the fix that makes the next
 * cycle clean. It gets its own, much shorter, deadline instead, because an
 * unbounded release call would re-create the very wedge this module exists to
 * end, one layer further down.
 */
export const DISCOVERY_SWEEP_TEARDOWN_TIMEOUT_MS = 30_000

const TEARDOWN_TIMED_OUT = Symbol('discovery-sweep-teardown-timed-out')

/**
 * Run one teardown step under its own deadline.
 *
 * Returns `undefined` if the step did not finish in time; the caller decides
 * what an abandoned teardown means. Errors propagate exactly as they did
 * before this wrapper existed — a teardown deadline is not a licence to start
 * swallowing failures that used to surface.
 *
 * An abandoned lease release is survivable and is why this returns rather than
 * throws: the durable lease carries its own expiry, so it is reclaimed as an
 * orphan by a later sweep instead of being held forever.
 */
export async function withSweepTeardownDeadline<T>(
  timeoutMs: number,
  step: () => Promise<T>,
): Promise<T | undefined> {
  const budget = discoverySweepBudgetMs(timeoutMs)
  if (budget === undefined) return await step()

  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const inFlight: Promise<SweepOutcome<T>> = step().then(
      (value) => ({ ok: true, value }) as const,
      (error: unknown) => ({ ok: false, error }) as const,
    )
    const outcome = await Promise.race<SweepOutcome<T> | typeof TEARDOWN_TIMED_OUT>([
      inFlight,
      new Promise<typeof TEARDOWN_TIMED_OUT>((resolve) => {
        // Referenced, for the same reason as the budget timer above: this is
        // the deadline that guarantees the lease is handed back.
        timer = setTimeout(() => resolve(TEARDOWN_TIMED_OUT), budget)
      }),
    ])
    if (outcome === TEARDOWN_TIMED_OUT) return undefined
    if (outcome.ok) return outcome.value
    throw outcome.error
  } finally {
    if (timer) clearTimeout(timer)
  }
}
