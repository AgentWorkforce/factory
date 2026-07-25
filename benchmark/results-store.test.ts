import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { appendResult, loadResults } from './results-store'

let dir: string

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'factory-benchmark-results-'))
})

afterEach(async () => {
  await rm(dir, { recursive: true, force: true })
})

describe('results-store', () => {
  it('returns an empty list when the results file does not exist yet', async () => {
    await expect(loadResults(join(dir, 'results.jsonl'))).resolves.toEqual([])
  })

  it('round-trips appended results as JSONL, one per line', async () => {
    const path = join(dir, 'results.jsonl')
    await appendResult(path, { taskId: 'a', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't1' })
    await appendResult(path, { taskId: 'a', mode: 'team', repeat: 0, runId: 'r2', passed: false, timestamp: 't2', notes: 'flaked' })

    const results = await loadResults(path)

    expect(results).toEqual([
      { taskId: 'a', mode: 'single', repeat: 0, runId: 'r1', passed: true, timestamp: 't1' },
      { taskId: 'a', mode: 'team', repeat: 0, runId: 'r2', passed: false, timestamp: 't2', notes: 'flaked' },
    ])
  })

  it('rejects a malformed result instead of silently writing bad data', async () => {
    const path = join(dir, 'results.jsonl')
    const malformed = { taskId: 'a', mode: 'not-a-mode', repeat: 0, runId: 'r1', passed: true, timestamp: 't1' } as unknown as Parameters<typeof appendResult>[1]
    await expect(appendResult(path, malformed)).rejects.toThrow()
    await expect(loadResults(path)).resolves.toEqual([])
  })
})
