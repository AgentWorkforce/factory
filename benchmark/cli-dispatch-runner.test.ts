import { describe, expect, it } from 'vitest'

import { createCliDispatchRunner } from './cli-dispatch-runner'
import type { BenchmarkTask } from './schema'

// The runner is deliberately real-IO; the guards below are pure input
// validation that runs BEFORE any child_process call, so they are safe (and
// worth) unit-testing on their own. They exist to prevent the harness from
// producing plausibly-shaped-but-wrong benchmark numbers when Factory cannot
// honor the requested cell — see the guards' comments in cli-dispatch-runner.ts.

function task(overrides: Partial<BenchmarkTask> = {}): BenchmarkTask {
  return {
    id: 'ex',
    title: 'Example',
    issueBody: 'Do the thing.',
    targetRepo: 'AgentWorkforce/factory-benchmark-fixtures',
    baseRef: 'main',
    difficulty: 'single-file',
    verify: { command: 'npm test', timeoutMs: 300_000 },
    source: 'authored',
    ...overrides,
  }
}

describe('createCliDispatchRunner guards', () => {
  it('refuses team mode for a single-repository task (Factory team scope needs multiple repo routes)', async () => {
    const runner = createCliDispatchRunner({ factoryConfigPath: '/tmp/nowhere.json' })
    await expect(runner.dispatch(task(), 'team')).rejects.toThrow(
      /Cannot dispatch team mode for single-repository task ex/,
    )
    // If the guard failed to fire, `gh issue create` would run for real —
    // proving the guard is the boundary, not the network.
  })

  it('refuses a non-default baseRef because Factory always cuts from the repo default branch', async () => {
    const runner = createCliDispatchRunner({ factoryConfigPath: '/tmp/nowhere.json' })
    await expect(runner.dispatch(task({ baseRef: 'abc123def456' }), 'single')).rejects.toThrow(
      /Cannot dispatch task ex with baseRef=abc123def456/,
    )
  })

})
