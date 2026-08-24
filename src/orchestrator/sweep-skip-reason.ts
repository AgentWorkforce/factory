/**
 * Why a discovery sweep declined to dispatch a work unit, as a closed set (#355).
 *
 * `IterationReport.skipped[].reason` is free text assembled at the skip site:
 * it names issue keys, dependency blockers and provider conditions, so it
 * cannot cross onto the unauthenticated health surface. But the *count* of
 * skips, split by cause, is the measurement that separates the two bugs #355
 * is stuck between — a sweep that saw seven eligible issues and rejected them
 * has a cause, and the cause is what says who owns the fix.
 *
 * So the code is recorded at the skip site alongside the text, never derived
 * from it: matching on a message is a rename away from silently collapsing
 * every bucket into `other`, and this vocabulary is the thing an operator
 * reads when nothing else is available.
 *
 * Codes are the *structural* reason the sweep stopped, deliberately finer
 * than the four `state`/`reason` buckets an operator can already see:
 * `dispatch-terminal` and `dispatch-retry-limit` mean the work unit is
 * permanently declined and needs a human, while `dispatch-backoff` and
 * `already-tracked` clear on their own.
 */
export const FACTORY_SWEEP_SKIP_REASON_CODES = [
  /** Relayfile shed this work unit's read; the sweep continued without it. */
  'read-failed',
  /** Durable dispatch state says terminal — this issue will never be retried. */
  'dispatch-terminal',
  /** Durable dispatch state says a dispatch is already running for it. */
  'dispatch-in-flight',
  /** Durable dispatch state is in error cooldown. */
  'dispatch-backoff',
  /** Durable dispatch state exhausted `dispatch.maxAttempts`. */
  'dispatch-retry-limit',
  /** The in-memory batch already holds it in flight or queued. */
  'already-tracked',
  /** Live provider state is not ready-for-agent, and no orphan was recovered. */
  'not-ready',
  /** Outside the configured factory scope (label / title prefix / repo route). */
  'out-of-scope',
  /** In scope but not a dispatchable reconciled issue. */
  'not-dispatchable',
  /** Dispatched into a dependency park. */
  'parked-dependency',
  /** Dispatched into a park because its dependencies form a cycle. */
  'dependency-cycle',
  /** Dispatch returned without agents: queued on capacity, or escalated to a human. */
  'queued-or-escalated',
  /** Dispatch threw; the sweep kept the rest of the pass (#292). */
  'dispatch-failed',
  /** Recorded by a producer this vocabulary does not know. */
  'other',
] as const

export type FactorySweepSkipReasonCode = typeof FACTORY_SWEEP_SKIP_REASON_CODES[number]

/**
 * Coerce an arbitrary value onto the vocabulary.
 *
 * Unrecognised codes collapse to `other` rather than being dropped: a skip
 * that vanished from the breakdown would make the parts stop summing to
 * `skipped`, and a reader comparing the two would conclude the counter was
 * broken rather than that the producer was newer.
 */
export const factorySweepSkipReasonCode = (value: unknown): FactorySweepSkipReasonCode =>
  typeof value === 'string' && (FACTORY_SWEEP_SKIP_REASON_CODES as readonly string[]).includes(value)
    ? value as FactorySweepSkipReasonCode
    : 'other'

/**
 * The per-cause breakdown, as counts only.
 *
 * Zero-count codes are omitted — the vocabulary is fixed and published, so an
 * absent key reads as zero unambiguously, and emitting fourteen zeroes on
 * every heartbeat would bury the one or two that are non-zero. The total is
 * carried separately by `skipped`, which is always present, so "ran and
 * skipped nothing" stays distinguishable from "never ran".
 */
export function factorySweepSkipReasonCounts(
  skipped: Iterable<{ code?: unknown }> | undefined,
): Partial<Record<FactorySweepSkipReasonCode, number>> {
  const counts: Partial<Record<FactorySweepSkipReasonCode, number>> = {}
  if (!skipped) return counts
  for (const entry of skipped) {
    const code = factorySweepSkipReasonCode(
      entry !== null && typeof entry === 'object' ? entry.code : undefined,
    )
    counts[code] = (counts[code] ?? 0) + 1
  }
  return counts
}
