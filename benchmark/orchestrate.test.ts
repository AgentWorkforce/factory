import { describe, expect, it, vi } from 'vitest'

import type { DispatchRunner } from './dispatch-runner'
import { runMatrix } from './orchestrate'
import type { BenchmarkResult, BenchmarkTask } from './schema'

function task(id: string): BenchmarkTask {
  return {
    id,
    title: `Task ${id}`,
    issueBody: 'Do the thing.',
    targetRepo: 'AgentWorkforce/factory-benchmark-fixtures',
    baseRef: 'main',
    difficulty: 'single-file',
    verify: { command: 'npm test', timeoutMs: 300_000 },
    source: 'authored',
  }
}

function deps(runner: DispatchRunner) {
  const appended: BenchmarkResult[] = []
  const logged: string[] = []
  return {
    opts: {
      runner,
      appendResult: async (result: BenchmarkResult) => { appended.push(result) },
      log: (message: string) => { logged.push(message) },
      now: () => 'fixed-timestamp',
    },
    appended,
    logged,
  }
}

describe('runMatrix', () => {
  it('dispatches, verifies, and appends one result per cell in order', async () => {
    const runner: DispatchRunner = {
      dispatch: vi.fn(async (t, mode) => ({ runId: `${t.id}-${mode}`, prBranch: `factory/${t.id}` })),
      verify: vi.fn(async () => ({ passed: true })),
    }
    const { opts, appended } = deps(runner)

    const results = await runMatrix([
      { task: task('a'), mode: 'single', repeat: 0 },
      { task: task('a'), mode: 'team', repeat: 0 },
    ], opts)

    expect(results).toEqual([
      { taskId: 'a', mode: 'single', repeat: 0, runId: 'a-single', passed: true, notes: undefined, timestamp: 'fixed-timestamp' },
      { taskId: 'a', mode: 'team', repeat: 0, runId: 'a-team', passed: true, notes: undefined, timestamp: 'fixed-timestamp' },
    ])
    expect(appended).toEqual(results)
    expect(runner.dispatch).toHaveBeenCalledTimes(2)
  })

  it('records a failed result and keeps going when one cell throws, instead of aborting the run', async () => {
    const runner: DispatchRunner = {
      dispatch: vi.fn()
        .mockRejectedValueOnce(new Error('fleet spawn failed'))
        .mockResolvedValueOnce({ runId: 'a-team', prBranch: 'factory/a' }),
      verify: vi.fn(async () => ({ passed: true })),
    }
    const { opts } = deps(runner)

    const results = await runMatrix([
      { task: task('a'), mode: 'single', repeat: 0 },
      { task: task('a'), mode: 'team', repeat: 0 },
    ], opts)

    expect(results[0]).toMatchObject({ mode: 'single', passed: false, notes: expect.stringContaining('fleet spawn failed') })
    expect(results[1]).toMatchObject({ mode: 'team', passed: true })
    expect(runner.dispatch).toHaveBeenCalledTimes(2) // second cell still ran after the first threw
  })

  it('processes cells strictly sequentially, never overlapping two dispatches', async () => {
    const order: string[] = []
    const runner: DispatchRunner = {
      dispatch: vi.fn(async (t) => {
        order.push(`start:${t.id}`)
        await new Promise((resolve) => setTimeout(resolve, 5))
        order.push(`end:${t.id}`)
        return { runId: t.id, prBranch: `factory/${t.id}` }
      }),
      verify: vi.fn(async () => ({ passed: true })),
    }
    const { opts } = deps(runner)

    await runMatrix([
      { task: task('a'), mode: 'single', repeat: 0 },
      { task: task('b'), mode: 'single', repeat: 0 },
    ], opts)

    expect(order).toEqual(['start:a', 'end:a', 'start:b', 'end:b'])
  })
})
