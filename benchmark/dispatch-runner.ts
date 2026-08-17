import type { BenchmarkMode, BenchmarkTask } from './schema'

export interface DispatchOutcome {
  runId: string
  /** The PR head branch the implementer(s) pushed, once dispatch completes. */
  prBranch: string
}

/**
 * The real-world side of running one matrix cell: ensure a GitHub issue
 * exists for the task, dispatch Factory at the forced mode, wait for a PR,
 * check it out, and run the task's verify command against it. Kept as an
 * interface (not a concrete class) so orchestrate.ts's matrix-walking logic
 * is unit-testable against a fake, the same separation this repo already
 * uses for FleetClient/GithubRead/etc. in src/ports.
 */
export interface DispatchRunner {
  /** Creates (or reuses) the GitHub issue and dispatches Factory at the given mode. Returns once a PR exists. */
  dispatch(task: BenchmarkTask, mode: BenchmarkMode): Promise<DispatchOutcome>
  /** Checks out `prBranch` in `task.targetRepo` and runs `task.verify.command` there. */
  verify(task: BenchmarkTask, outcome: DispatchOutcome): Promise<{ passed: boolean; notes?: string }>
}
