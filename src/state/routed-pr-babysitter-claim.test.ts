import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import type { StateStore } from '../ports/state'
import { FileStateStore } from './file-state-store'
import { InMemoryStateStore } from './in-memory-state-store'

const seed = {
  repo: 'AgentWorkforce/pear',
  prNumber: 7,
  revision: 'rev-1',
  source: 'routed-open-prs' as const,
}

const verifyClaims = (label: string, create: () => Promise<StateStore> | StateStore) => {
  it(`${label} atomically gives one owner the PR work unit`, async () => {
    const store = await create()
    const results = await Promise.all([
      store.claimRoutedPrBabysitter('workspace', 'agentworkforce/pear#7', seed, 'owner-a', 1_000, 60_000, 5),
      store.claimRoutedPrBabysitter('workspace', 'agentworkforce/pear#7', seed, 'owner-b', 1_000, 60_000, 5),
    ])
    expect(results.map((result) => result.outcome).sort()).toEqual(['already-running', 'claimed'])
  })

  it(`${label} enforces global capacity and re-admits only changed completed work`, async () => {
    const store = await create()
    const first = await store.claimRoutedPrBabysitter('workspace', 'agentworkforce/pear#7', seed, 'owner-a', 1_000, 60_000, 1)
    expect(first.outcome).toBe('claimed')
    expect(await store.markRoutedPrBabysitterRunning('workspace', 'agentworkforce/pear#7', 'owner-a', 'agent-a', 1_001)).toBe(true)
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#8',
      { ...seed, prNumber: 8 },
      'owner-a',
      1_002,
      60_000,
      1,
    )).outcome).toBe('capacity')
    expect(await store.completeRoutedPrBabysitter('workspace', 'agentworkforce/pear#7', 'agent-a', 1_003)).toBe(true)
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-b',
      1_004,
      60_000,
      1,
    )).outcome).toBe('unchanged')
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      { ...seed, revision: 'rev-2' },
      'owner-b',
      1_005,
      60_000,
      1,
    )).outcome).toBe('claimed')
  })
}

describe('routed PR babysitter claims', () => {
  verifyClaims('memory state', () => new InMemoryStateStore({ batchSize: 5 }))
  verifyClaims('file state', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-routed-pr-claim-'))
    return new FileStateStore({ batchSize: 5, watchStatePath: join(root, 'state.json') })
  })
})
