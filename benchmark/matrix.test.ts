import { describe, expect, it } from 'vitest'

import { buildMatrix } from './matrix'
import type { BenchmarkTask } from './schema'

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

describe('buildMatrix', () => {
  it('produces every task x mode x repeat combination by default', () => {
    const cells = buildMatrix([task('a'), task('b')], ['single', 'team'], [], { repeats: 2 })

    expect(cells).toHaveLength(8)
    expect(cells.filter((cell) => cell.task.id === 'a' && cell.mode === 'single')).toHaveLength(2)
  })

  it('skips cells that already have a recorded result (resumable)', () => {
    const cells = buildMatrix(
      [task('a')],
      ['single', 'team', 'swarm'],
      [
        { taskId: 'a', mode: 'single', repeat: 0 },
        { taskId: 'a', mode: 'team', repeat: 0 },
      ],
      { repeats: 1 },
    )

    expect(cells).toEqual([{ task: task('a'), mode: 'swarm', repeat: 0 }])
  })

  it('only re-runs the repeats not yet recorded, per mode', () => {
    const cells = buildMatrix(
      [task('a')],
      ['single'],
      [{ taskId: 'a', mode: 'single', repeat: 0 }],
      { repeats: 3 },
    )

    expect(cells.map((cell) => cell.repeat)).toEqual([1, 2])
  })

  it('narrows to --only-task and --only-mode without touching other tasks', () => {
    const cells = buildMatrix(
      [task('a'), task('b')],
      ['single', 'team', 'swarm'],
      [],
      { onlyTaskIds: ['b'], onlyModes: ['swarm'], repeats: 1 },
    )

    expect(cells).toEqual([{ task: task('b'), mode: 'swarm', repeat: 0 }])
  })

  it('rejects a non-positive repeats value instead of silently producing an empty matrix', () => {
    expect(() => buildMatrix([task('a')], ['single'], [], { repeats: 0 })).toThrow(/repeats must be >= 1/)
  })
})
