import { MountAuthScopeError } from '../mount/mount-auth-error'
import { isLiveDispatchStateChangedError } from '../orchestrator'
import type { DispatchResult, IterationReport } from '../types'

/**
 * Process exit codes for the `factory` CLI.
 *
 * The contract is deliberately narrow: `OK` means *the requested action was
 * performed*. Every other value means it was not. A supervising loop, cron
 * entry, or CI step that only reads `$?` must be able to tell a refusal from a
 * completed run — printing a good diagnostic to stderr does not help a caller
 * that never reads stderr.
 *
 * `REFUSED` and `RETRYABLE` split the refusals along the one axis a caller can
 * act on: whether repeating the identical command could ever succeed.
 */
export const FACTORY_EXIT = {
  /** The requested action was performed. */
  OK: 0,
  /** The command failed. The default for a thrown error with no finer classification. */
  FAILED: 1,
  /**
   * Deliberately refused. Repeating the identical command will refuse again
   * until an input, a config value, or a credential changes.
   */
  REFUSED: 2,
  /**
   * Not performed this time, but the same command may succeed later — a lost
   * writeback race, or a capacity/dependency hold that clears on its own.
   */
  RETRYABLE: 3,
} as const

export type FactoryExitCode = (typeof FACTORY_EXIT)[keyof typeof FACTORY_EXIT]

/**
 * Classify a thrown command failure.
 *
 * A mount scope shortfall is terminal until the operator re-authenticates with
 * a session that can mint the filesystem scopes, so it is `REFUSED`. A live
 * dispatch state change means another writer won the race for the same issue;
 * the work unit is still there and the next attempt may win, so it is
 * `RETRYABLE`. Everything else is an unclassified failure.
 */
export function exitCodeForError(error: unknown): FactoryExitCode {
  if (error instanceof MountAuthScopeError) return FACTORY_EXIT.REFUSED
  if (isLiveDispatchStateChangedError(error)) return FACTORY_EXIT.RETRYABLE
  return FACTORY_EXIT.FAILED
}

/**
 * Classify the outcome of a single-issue `factory dispatch`.
 *
 * A dry run's requested action is the dry run itself, so it is always `OK`. For
 * a real dispatch the only success is at least one spawned agent: a hold, a
 * queue, or an escalation all leave the issue exactly where it was.
 */
export function exitCodeForDispatchResult(result: DispatchResult): FactoryExitCode {
  if (result.dryRun) return FACTORY_EXIT.OK
  if (result.agents.length > 0) return FACTORY_EXIT.OK
  // A capacity or dependency hold clears without anyone changing the request.
  // A dependency *cycle* does not — it needs a human to break the cycle.
  if (result.hold?.kind === 'capacity' || result.hold?.kind === 'dependency') return FACTORY_EXIT.RETRYABLE
  return FACTORY_EXIT.REFUSED
}

/**
 * Classify the outcome of a `factory run-once` / `factory loop` iteration.
 *
 * `skipped` entries are ordinary: a sweep that examines issues and dispatches
 * none of them still did what it was asked. A recorded `error` is a failed
 * cycle, and a deferred discovery means the sweep never ran at all.
 */
export function exitCodeForIterationReport(report: IterationReport): FactoryExitCode {
  if (report.error) return FACTORY_EXIT.FAILED
  if (report.discoveryDeferred) return FACTORY_EXIT.RETRYABLE
  return FACTORY_EXIT.OK
}

/**
 * Collapse a bounded loop's iteration reports into one exit code.
 *
 * A failed iteration means the loop did not complete cleanly and a caller
 * reading only `$?` has to see that. A *deferred* iteration does not: the loop
 * was asked to run several sweeps, and another owner holding the sweep for one
 * of them is ordinary contention, not a refusal. Escalating that would make a
 * healthy loop exit non-zero — the same defect as exiting 0 on a refusal, just
 * pointed the other way.
 */
export function exitCodeForLoopReports(reports: readonly IterationReport[]): FactoryExitCode {
  return reports.some((report) => report.error) ? FACTORY_EXIT.FAILED : FACTORY_EXIT.OK
}
