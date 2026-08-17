import type { DispatchRunner } from './dispatch-runner'
import type { MatrixCell } from './matrix'
import type { BenchmarkResult } from './schema'

export interface OrchestrateDeps {
  runner: DispatchRunner
  appendResult: (result: BenchmarkResult) => Promise<void>
  log: (message: string) => void
  now: () => string
  /**
   * Monotonic clock in milliseconds. Injected so tests can drive it
   * deterministically; defaults to `performance.now()` at call sites.
   */
  monotonicMs?: () => number
}

/**
 * Walks the matrix sequentially (one cell at a time — real dispatches spend
 * real money and share a fleet, so this deliberately does not parallelize)
 * recording one result per cell as soon as it finishes, so a crash partway
 * through only loses the in-flight cell, not the whole run.
 */
export async function runMatrix(cells: MatrixCell[], deps: OrchestrateDeps): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []
  for (const [index, cell] of cells.entries()) {
    deps.log(`[${index + 1}/${cells.length}] ${cell.task.id} x ${cell.mode} (repeat ${cell.repeat})`)
    const result = await runCell(cell, deps)
    await deps.appendResult(result)
    results.push(result)
  }
  return results
}

async function runCell(cell: MatrixCell, deps: OrchestrateDeps): Promise<BenchmarkResult> {
  const monotonic = deps.monotonicMs ?? (() => performance.now())
  const startedAtMs = monotonic()
  try {
    const outcome = await deps.runner.dispatch(cell.task, cell.mode)
    const verdict = await deps.runner.verify(cell.task, outcome)
    return {
      taskId: cell.task.id,
      mode: cell.mode,
      repeat: cell.repeat,
      runId: outcome.runId,
      passed: verdict.passed,
      notes: verdict.notes,
      timestamp: deps.now(),
      durationMs: Math.max(0, Math.round(monotonic() - startedAtMs)),
    }
  } catch (error) {
    // A dispatch/verify failure is itself a real, reportable data point (the
    // mode failed this task) — never let one cell's exception abort the run.
    return {
      taskId: cell.task.id,
      mode: cell.mode,
      repeat: cell.repeat,
      runId: `error:${cell.task.id}:${cell.mode}:${cell.repeat}`,
      passed: false,
      notes: `runner error: ${error instanceof Error ? error.message : String(error)}`,
      timestamp: deps.now(),
      durationMs: Math.max(0, Math.round(monotonic() - startedAtMs)),
    }
  }
}
