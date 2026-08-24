/**
 * Why a dispatch *attempt* failed, as a closed set (#355).
 *
 * `skipReasons` (#358) answers "why did the sweep decline this work unit", and
 * on the live container it answers it with `dispatch-failed: 5` — every sweep,
 * with the control-plane breaker closed, the fleet agent online and
 * `readinessReconcile` healthy. That bucket is a *count*. It says the sweep got
 * all the way to dispatching and dispatch threw; it does not say what threw,
 * and the message that would say so goes to the daemon's stdout, which does not
 * reach the deployed container's operator.
 *
 * So this is the second level of the same breakdown: `dispatchFailures` is the
 * total (the same number `skipReasons['dispatch-failed']` carries), and
 * `dispatchFailureReasons` splits it by cause. Same discipline as #358 —
 * counts only, keys from this vocabulary rather than from the record, unknown
 * codes folded into `other` rather than dropped so the parts still sum.
 *
 * As in #358 the code is recorded at the skip site alongside the free text,
 * never derived from it: `perItemDispatchFailureCode` classifies a thrown value
 * by type, and the two named-by-class-name entries below are the explicit
 * exception, documented there.
 *
 * The three `unclassified-*` codes are not a failure of this vocabulary — they
 * are its most useful answer when nothing named matches, because the *phase*
 * that threw already names the owner. A pass that reaches dispatch and fails is
 * a fleet problem; one that fails in triage never got near the fleet; one that
 * fails in the gates ahead of triage failed reading durable state. Those are
 * three different people's bugs, and separating them costs one variable.
 */
export const FACTORY_DISPATCH_FAILURE_REASON_CODES = [
  /** Relayfile shed an operation this dispatch needed (#297). Clears on its own. */
  'relayfile-overloaded',
  /** Live provider state moved under the dispatch; the unit returns to the queue. */
  'live-state-changed',
  /** The never-placed deadline released the dispatch mid-spawn (#303). Self-healing. */
  'late-placement-released',
  /** The durable lifecycle record is terminal: this unit will not be retried. */
  'lifecycle-terminal',
  /** Another publisher holds the durable lifecycle lease. Clears on release. */
  'lifecycle-owned-elsewhere',
  /** The fleet control-plane circuit refused admission. Dispatch is globally paused. */
  'control-plane-open',
  /** Something in the attempt hit its own deadline — see the note on class names. */
  'timed-out',
  /** The fleet broker never acknowledged the spawn within its ack timeout. */
  'spawn-ack-timeout',
  /** The fleet client holds a read-only identity and cannot spawn at all. */
  'fleet-identity-read-only',
  /** Registering the spawned agent with the broker failed. */
  'agent-registration-failed',
  /** Threw in the gates ahead of triage: scope, orphan recovery, durable reads. */
  'unclassified-gate',
  /** Threw inside triage, before the fleet was ever asked for anything. */
  'unclassified-triage',
  /** Threw inside dispatch itself, and no code above matched it. */
  'unclassified-dispatch',
  /** Recorded by a producer this vocabulary does not know. */
  'other',
] as const

export type FactoryDispatchFailureReasonCode = typeof FACTORY_DISPATCH_FAILURE_REASON_CODES[number]

/**
 * Codes recognised by allowlisted error *class name* rather than by `instanceof`.
 *
 * The rule everywhere else is to classify by type, because a message is one
 * rename away from collapsing a bucket. These five are the exception, for one
 * reason: their classes live in `src/fleet/relay-fleet-client.ts`, and
 * `factory.ts` does not import that module. Adding the edge to get five
 * `instanceof` checks would pull the relay SDK into the orchestrator's module
 * graph — a real cost — to buy precision over a *class name*, which is
 * code-controlled and already the identifier `telemetryErrorClass` publishes
 * everywhere else in this codebase. A rename here degrades one bucket to
 * `unclassified-dispatch`; it cannot leak and it cannot break the sum.
 *
 * `TimeoutError` is deliberately generic: `FleetControlPlaneTimeoutError` sets
 * that name on purpose, and so do `AbortSignal.timeout` and several SDK paths.
 * Mapping it to the control plane specifically would be a guess; mapping it to
 * "something in this attempt hit a deadline" is exactly true, and with 34-minute
 * sweeps against 60-second attempts it is the answer worth being able to see.
 */
const DISPATCH_FAILURE_ERROR_CLASS_CODES: Readonly<Record<string, FactoryDispatchFailureReasonCode>> = {
  RelaySpawnAckTimeoutError: 'spawn-ack-timeout',
  ReadOnlyFleetIdentityError: 'fleet-identity-read-only',
  FactoryAgentRegistrationError: 'agent-registration-failed',
  TimeoutError: 'timed-out',
  AbortError: 'timed-out',
}

/** The code an allowlisted error class name names, if this vocabulary knows one. */
export const factoryDispatchFailureReasonCodeForErrorClass = (
  errorClass: string,
): FactoryDispatchFailureReasonCode | undefined =>
  Object.hasOwn(DISPATCH_FAILURE_ERROR_CLASS_CODES, errorClass)
    ? DISPATCH_FAILURE_ERROR_CLASS_CODES[errorClass]
    : undefined

/**
 * Coerce an arbitrary value onto the vocabulary.
 *
 * Unrecognised codes collapse to `other` rather than being dropped, for the
 * same reason #358 gives: a failure that vanished from the breakdown would make
 * the parts stop summing to `dispatchFailures`, and a reader comparing the two
 * would conclude the counter was broken rather than that the producer was newer.
 */
export const factoryDispatchFailureReasonCode = (value: unknown): FactoryDispatchFailureReasonCode =>
  typeof value === 'string' &&
  (FACTORY_DISPATCH_FAILURE_REASON_CODES as readonly string[]).includes(value)
    ? value as FactoryDispatchFailureReasonCode
    : 'other'

/**
 * The per-cause breakdown of one sweep's failed dispatch attempts, counts only.
 *
 * Counts exactly the entries the sweep recorded as `dispatch-failed`, so the
 * parts sum to `dispatchFailures` and to `skipReasons['dispatch-failed']` by
 * construction. An entry whose producer recorded no `failureCode` folds into
 * `other` for the same reason an unknown one does.
 *
 * Zero-count codes are omitted — the vocabulary is fixed and published, so an
 * absent key reads as zero unambiguously. The total is carried separately by
 * `dispatchFailures`, which is written whenever a sweep completes, so "ran and
 * every dispatch succeeded" stays distinguishable from "never ran".
 */
export function factoryDispatchFailureReasonCounts(
  skipped: Iterable<{ code?: unknown; failureCode?: unknown }> | undefined,
): Partial<Record<FactoryDispatchFailureReasonCode, number>> {
  const counts: Partial<Record<FactoryDispatchFailureReasonCode, number>> = {}
  if (!skipped) return counts
  for (const entry of skipped) {
    if (entry === null || typeof entry !== 'object' || entry.code !== 'dispatch-failed') continue
    const code = factoryDispatchFailureReasonCode(entry.failureCode)
    counts[code] = (counts[code] ?? 0) + 1
  }
  return counts
}
