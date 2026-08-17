import { describe, expect, it } from 'vitest'

import { buildReportRows, renderMarkdownReport } from './report'
import type { BenchmarkTask } from './schema'

const task: BenchmarkTask = {
  id: 'rename-error-type',
  title: 'Rename NetworkError to TransportError',
  issueBody: 'Do the thing.',
  targetRepo: 'AgentWorkforce/factory-benchmark-fixtures',
  baseRef: 'main',
  difficulty: 'single-file',
  verify: { command: 'npm test', timeoutMs: 300_000 },
  source: 'authored',
}

describe('buildReportRows', () => {
  it('computes success rate and sample size per task x mode', () => {
    const rows = buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't' },
      { taskId: 'rename-error-type', mode: 'single', repeat: 1, runId: 'r2', passed: false, timestamp: 't' },
      { taskId: 'rename-error-type', mode: 'team', repeat: 0, runId: 'r3', passed: true, timestamp: 't' },
    ])

    expect(rows).toEqual([
      expect.objectContaining({ taskId: 'rename-error-type', mode: 'single', sampleSize: 2, successRate: 0.5 }),
      expect.objectContaining({ taskId: 'rename-error-type', mode: 'team', sampleSize: 1, successRate: 1 }),
    ])
  })

  it('joins cost/duration samples by runId and averages them', () => {
    const rows = buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't' },
      { taskId: 'rename-error-type', mode: 'single', repeat: 1, runId: 'r2', passed: true, timestamp: 't' },
    ], [
      { runId: 'r1', usd: 1.0, durationMs: 60_000 },
      { runId: 'r2', usd: 3.0, durationMs: 180_000 },
    ])

    expect(rows[0]?.meanCostUsd).toBe(2.0)
    expect(rows[0]?.meanWallClockMs).toBe(120_000)
  })

  it('leaves cost fields undefined when no cost sample matches a run, rather than defaulting to 0', () => {
    const rows = buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't' },
    ])

    expect(rows[0]?.meanCostUsd).toBeUndefined()
    expect(rows[0]?.meanWallClockMs).toBeUndefined()
  })

  // New shape: durationMs and costUsd are recorded directly on the result by
  // orchestrate.runMatrix, so the report never needs an out-of-band samples
  // file to fill in the headline wall-clock column.
  it('reads durationMs and costUsd off the result row directly', () => {
    const rows = buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't', durationMs: 60_000, costUsd: 1 },
      { taskId: 'rename-error-type', mode: 'single', repeat: 1, runId: 'r2', passed: true, timestamp: 't', durationMs: 180_000, costUsd: 3 },
    ])

    expect(rows[0]?.meanWallClockMs).toBe(120_000)
    expect(rows[0]?.meanCostUsd).toBe(2)
  })

  it('averages only the rows that carry duration/cost, treating missing values as absent rather than 0', () => {
    const rows = buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't', durationMs: 120_000 },
      { taskId: 'rename-error-type', mode: 'single', repeat: 1, runId: 'r2', passed: false, timestamp: 't' }, // no durationMs
    ])

    // Only the sampled row counts toward the mean — no silent 0 bringing it down.
    expect(rows[0]?.meanWallClockMs).toBe(120_000)
    expect(rows[0]?.meanCostUsd).toBeUndefined()
  })

  it('drops results for tasks no longer in the corpus instead of throwing', () => {
    const rows = buildReportRows([task], [
      { taskId: 'deleted-task', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't' },
    ])

    expect(rows).toEqual([])
  })
})

describe('renderMarkdownReport', () => {
  it('groups rows under a heading per difficulty tier', () => {
    const markdown = renderMarkdownReport(buildReportRows([task], [
      { taskId: 'rename-error-type', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't' },
    ]))

    expect(markdown).toContain('## single-file')
    expect(markdown).toContain('rename-error-type')
    expect(markdown).toContain('100%')
    expect(markdown).not.toContain('## multi-file')
  })

  it('reports no results without crashing on an empty matrix', () => {
    expect(renderMarkdownReport([])).toContain('No results recorded yet.')
  })
})
