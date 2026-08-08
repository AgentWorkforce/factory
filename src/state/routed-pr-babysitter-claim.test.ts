import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  ROUTED_PR_BABYSITTER_COMPLETED_RETENTION_MS,
  type StateStore,
} from '../ports/state'
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
    expect(await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      first.claim!.claimId,
      'agent-a',
      1_001,
    )).toBe(true)
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#8',
      { ...seed, prNumber: 8 },
      'owner-a',
      1_002,
      60_000,
      1,
    )).outcome).toBe('capacity')
    expect(await store.completeRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      first.claim!.claimId,
      'agent-a',
      1_003,
    )).toBe(true)
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

  it(`${label} treats an expired same-owner claim as a fresh admission`, async () => {
    const store = await create()
    await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-a',
      1_000,
      1_000,
      1,
    )
    await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#8',
      { ...seed, prNumber: 8 },
      'owner-b',
      2_001,
      60_000,
      1,
    )
    expect((await store.listRoutedPrBabysitterClaims('workspace'))
      .map(([identity]) => identity)).toEqual(['agentworkforce/pear#8'])
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-a',
      2_002,
      60_000,
      1,
    )).outcome).toBe('capacity')
  })

  it(`${label} does not merge distinct issue-created work units owned by one process`, async () => {
    const store = await create()
    const issueSeed = { ...seed, source: 'issue-created' as const }
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      issueSeed,
      'dispatch-owner',
      1_000,
      60_000,
      5,
    )).outcome).toBe('claimed')
    expect((await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      { ...issueSeed, revision: 'different-issue-lifecycle' },
      'dispatch-owner',
      1_001,
      60_000,
      5,
    )).outcome).toBe('already-running')
  })

  it(`${label} fences delayed transitions from an expired claim generation`, async () => {
    const store = await create()
    const expired = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-a',
      1_000,
      1_000,
      5,
    )
    const current = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      { ...seed, revision: 'rev-2' },
      'owner-a',
      2_001,
      60_000,
      5,
    )
    expect(current.outcome).toBe('claimed')
    expect(await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      expired.claim!.claimId,
      'stale-agent',
      2_002,
    )).toBe(false)
    expect(await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      current.claim!.claimId,
      'current-agent',
      2_002,
    )).toBe(true)
    expect(await store.completeRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      expired.claim!.claimId,
      'current-agent',
      2_003,
    )).toBe(false)
  })

  it(`${label} atomically adopts an expired restored running claim`, async () => {
    const store = await create()
    const original = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'old-owner',
      1_000,
      60_000,
      5,
    )
    await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'old-owner',
      original.claim!.claimId,
      'agent-a',
      1_001,
    )
    const adopted = await store.adoptRoutedPrBabysitterClaim(
      'workspace',
      'agentworkforce/pear#7',
      'agent-a',
      'new-owner',
      61_001,
      60_000,
    )
    expect(adopted).toMatchObject({ owner: 'new-owner' })
    expect((await store.listRoutedPrBabysitterClaims('workspace'))[0]?.[1]).toMatchObject({
      owner: 'new-owner',
      leaseUntilMs: 121_001,
      agentName: 'agent-a',
      status: 'running',
    })
    expect(await store.releaseRoutedPrBabysitterClaim(
      'workspace',
      'agentworkforce/pear#7',
      'old-owner',
      original.claim!.claimId,
    )).toBe(false)
  })

  it(`${label} refuses to adopt another owner's live running lease`, async () => {
    const store = await create()
    const original = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'old-owner',
      1_000,
      60_000,
      5,
    )
    await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'old-owner',
      original.claim!.claimId,
      'agent-a',
      1_001,
    )

    expect(await store.adoptRoutedPrBabysitterClaim(
      'workspace',
      'agentworkforce/pear#7',
      'agent-a',
      'new-owner',
      2_000,
      60_000,
    )).toBeUndefined()
    expect((await store.listRoutedPrBabysitterClaims('workspace'))[0]?.[1]).toMatchObject({
      owner: 'old-owner',
      leaseUntilMs: 61_000,
    })
  })

  it(`${label} lets the current owner renew a live running lease`, async () => {
    const store = await create()
    const original = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-a',
      1_000,
      60_000,
      5,
    )
    await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      original.claim!.claimId,
      'agent-a',
      1_001,
    )

    expect(await store.adoptRoutedPrBabysitterClaim(
      'workspace',
      'agentworkforce/pear#7',
      'agent-a',
      'owner-a',
      2_000,
      60_000,
    )).toMatchObject({ owner: 'owner-a', claimId: original.claim!.claimId })
    expect((await store.listRoutedPrBabysitterClaims('workspace'))[0]?.[1]).toMatchObject({
      owner: 'owner-a',
      leaseUntilMs: 62_000,
    })
  })

  it(`${label} prunes completed claims after bounded retention`, async () => {
    const store = await create()
    const original = await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      seed,
      'owner-a',
      1_000,
      60_000,
      5,
    )
    await store.markRoutedPrBabysitterRunning(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      original.claim!.claimId,
      'agent-a',
      1_001,
    )
    await store.completeRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#7',
      'owner-a',
      original.claim!.claimId,
      'agent-a',
      1_002,
    )
    await store.claimRoutedPrBabysitter(
      'workspace',
      'agentworkforce/pear#8',
      { ...seed, prNumber: 8 },
      'owner-b',
      1_002 + ROUTED_PR_BABYSITTER_COMPLETED_RETENTION_MS + 1,
      60_000,
      5,
    )
    expect((await store.listRoutedPrBabysitterClaims('workspace'))
      .map(([identity]) => identity)).toEqual(['agentworkforce/pear#8'])
  })
}

describe('routed PR babysitter claims', () => {
  verifyClaims('memory state', () => new InMemoryStateStore({ batchSize: 5 }))
  verifyClaims('file state', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-routed-pr-claim-'))
    return new FileStateStore({ batchSize: 5, watchStatePath: join(root, 'state.json') })
  })
})
