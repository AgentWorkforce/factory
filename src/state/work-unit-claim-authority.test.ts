import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import { dispatchIssueIdentity, workUnitOriginFromRaw } from '../dispatch/work-unit-identity'
import type { DispatchLifecycle } from '../ports/state'
import { HeuristicTriage } from '../triage/heuristic'
import type { IssueRef, LinearIssue, TriageContext } from '../types'
import { FileStateStore } from './file-state-store'
import { legacyCompositeLifecycleKey, prunableMigrationAliases } from './work-unit-lifecycle-migration'
import { InMemoryStateStore } from './in-memory-state-store'

/**
 * AR-448: one GitHub issue reached Factory twice — once GitHub-native, once
 * through its own Linear mirror — and dispatched twice, because the claim key
 * carried the surface path instead of the work unit.
 *
 * The mirror is the discriminating half. Two arbitrary surfaces do not
 * reproduce the incident; a GitHub issue and the Linear row that mirrors it do.
 */
const GITHUB_NATIVE: IssueRef = {
  key: '448',
  uuid: 'AgentWorkforce/factory#448',
  path: '/github/repos/AgentWorkforce/factory/issues/by-id/448.json',
}

/**
 * The same underlying issue as GITHUB_NATIVE, surfaced as a `[factory]` Linear
 * mirror. Its Relayfile sense path and provider uuid are Linear's; its declared
 * origin is the GitHub issue it mirrors.
 */
const LINEAR_MIRROR: IssueRef = {
  key: 'AR-448',
  uuid: '5f1c6d6e-4a0b-4c7d-9c1e-2b8a7d3f0e11',
  path: '/linear/issues/AR-448__5f1c6d6e-4a0b-4c7d-9c1e-2b8a7d3f0e11.json',
  origin: { provider: 'github', owner: 'AgentWorkforce', repo: 'factory', number: 448 },
} as IssueRef

/** A genuinely different work unit — never allowed to collapse into the pair above. */
const DISTINCT_UNIT: IssueRef = {
  key: '449',
  uuid: 'AgentWorkforce/factory#449',
  path: '/github/repos/AgentWorkforce/factory/issues/by-id/449.json',
}

const seedFor = (issue: IssueRef, runId: string): DispatchLifecycle => ({
  runId,
  issue: { ...issue },
  decision: {
    issue: { ...issue },
    routes: [{ repo: 'AgentWorkforce/factory', clonePath: '/work/factory', rationale: 'test' }],
    scope: 'single',
    implementers: [],
    reviewer: {
      name: `ar-${issue.key}-review`,
      role: 'reviewer',
      capability: 'spawn:claude',
      task: 'review',
      repo: 'AgentWorkforce/factory',
    },
    thin: false,
    confidence: 'high',
    rationale: 'test',
  },
  dryRun: false,
  phase: 'dispatching',
  agents: [],
  invocationIds: [],
  updatedAtMs: 1_000,
})

const stores = async (root: string): Promise<Array<[string, InMemoryStateStore | FileStateStore]>> => [
  ['memory', new InMemoryStateStore({ batchSize: 4 })],
  ['file', new FileStateStore({ batchSize: 4, watchStatePath: join(root, 'state.json') })],
]

/**
 * The raw provider payload a `[factory]` Linear mirror of a GitHub issue
 * carries — the shape `safety/factory-scope.ts` already reads to recognise one.
 */
const MIRROR_RAW = {
  provider: 'linear',
  payload: {
    source: {
      provider: 'github',
      owner: 'AgentWorkforce',
      repo: 'factory',
      number: 448,
      url: 'https://github.com/AgentWorkforce/factory/issues/448',
    },
  },
}

const mirrorLinearIssue = (): LinearIssue => ({
  uuid: LINEAR_MIRROR.uuid,
  key: LINEAR_MIRROR.key,
  title: '[factory] duplicate dispatch when one unit arrives twice',
  description: 'Mirror of the GitHub issue. '.repeat(12),
  stateId: 'ready',
  state: { name: 'Ready for Agent' },
  labels: ['factory'],
  team: 'agent-relay',
  path: LINEAR_MIRROR.path,
  raw: MIRROR_RAW,
})

const triageContext = (): TriageContext => ({
  config: FactoryConfigSchema.parse({
    workspaceId: 'ws_211',
    repos: {
      byLabel: { factory: 'AgentWorkforce/factory' },
      clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
      default: 'AgentWorkforce/factory',
    },
  }),
  repoMap: [],
})

describe('work-unit claim authority', () => {
  it('resolves a GitHub issue and its own Linear mirror to one work-unit identity', () => {
    expect(dispatchIssueIdentity(LINEAR_MIRROR)).toBe(dispatchIssueIdentity(GITHUB_NATIVE))
  })

  /**
   * The identity function reading `origin` is only half of it. If nothing on
   * the real path ever WRITES `origin`, every mirror still falls back to
   * `linear:<uuid>` in production while the tests above stay green. This drives
   * the construction path that actually produces the IssueRef a claim is keyed
   * on.
   */
  it('reads a declared GitHub origin out of a mirror payload', () => {
    expect(workUnitOriginFromRaw(mirrorLinearIssue().raw)).toEqual({
      provider: 'github',
      owner: 'AgentWorkforce',
      repo: 'factory',
      number: 448,
    })
    expect(workUnitOriginFromRaw({ provider: 'linear', payload: {} })).toBeUndefined()
  })

  it('populates the origin on the real construction path, not just in fixtures', async () => {
    // The IssueRef a dispatch claim is keyed on is the one triage produces.
    const decision = await new HeuristicTriage().triage(mirrorLinearIssue(), triageContext())

    expect(decision.issue.origin, 'triage must retain the mirror origin').toEqual({
      provider: 'github',
      owner: 'AgentWorkforce',
      repo: 'factory',
      number: 448,
    })
    expect(dispatchIssueIdentity(decision.issue)).toBe('github:agentworkforce/factory#448')
    expect(dispatchIssueIdentity(decision.issue)).toBe(dispatchIssueIdentity(GITHUB_NATIVE))
  })

  it('gives a triage-produced mirror and a GitHub-native arrival ONE claim', async () => {
    const store = new InMemoryStateStore({ batchSize: 4 })
    const decision = await new HeuristicTriage().triage(mirrorLinearIssue(), triageContext())

    const native = await store.claimDispatchLifecycle(
      'workspace-construction',
      dispatchIssueIdentity(GITHUB_NATIVE),
      seedFor(GITHUB_NATIVE, 'run-native'),
      'dispatcher-a',
      1_000,
      5_000,
    )
    const mirror = await store.claimDispatchLifecycle(
      'workspace-construction',
      dispatchIssueIdentity(decision.issue),
      seedFor(decision.issue, 'run-mirror'),
      'dispatcher-b',
      1_001,
      5_000,
    )

    expect(native).toMatchObject({ acquired: true, created: true })
    expect(mirror).toMatchObject({ acquired: false, created: false })
    expect(await store.listDispatchLifecycles('workspace-construction')).toHaveLength(1)
  })

  /** A native Linear issue has no upstream origin and keeps its own identity. */
  it('leaves a native Linear issue on its own provider identity', async () => {
    const native = mirrorLinearIssue()
    native.raw = { provider: 'linear', payload: {} }

    const decision = await new HeuristicTriage().triage(native, triageContext())

    expect(decision.issue.origin).toBeUndefined()
    expect(dispatchIssueIdentity(decision.issue)).toBe(`linear:${LINEAR_MIRROR.uuid}`)
  })

  it('gives a GitHub issue and its own Linear mirror ONE claim and ONE dispatch', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-work-unit-claim-'))
    try {
      for (const [name, store] of await stores(root)) {
        const workspace = `workspace-${name}`

        const native = await store.claimDispatchLifecycle(
          workspace,
          dispatchIssueIdentity(GITHUB_NATIVE),
          seedFor(GITHUB_NATIVE, 'run-native'),
          'dispatcher-a',
          1_000,
          5_000,
        )
        expect(native, `${name}: GitHub-native claim`).toMatchObject({ acquired: true, created: true })

        // Same work unit, other surface, while the first lease is live.
        const mirror = await store.claimDispatchLifecycle(
          workspace,
          dispatchIssueIdentity(LINEAR_MIRROR),
          seedFor(LINEAR_MIRROR, 'run-mirror'),
          'dispatcher-b',
          1_001,
          5_000,
        )

        expect(mirror.created, `${name}: mirror must not create a second claim`).toBe(false)
        expect(mirror.acquired, `${name}: mirror must be refused while the unit is claimed`).toBe(false)
        expect(await store.listDispatchLifecycles(workspace), `${name}: one row per work unit`).toHaveLength(1)
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps two genuinely distinct work units distinct', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-work-unit-distinct-'))
    try {
      for (const [name, store] of await stores(root)) {
        const workspace = `workspace-${name}`

        const first = await store.claimDispatchLifecycle(
          workspace,
          dispatchIssueIdentity(GITHUB_NATIVE),
          seedFor(GITHUB_NATIVE, 'run-448'),
          'dispatcher-a',
          1_000,
          5_000,
        )
        const second = await store.claimDispatchLifecycle(
          workspace,
          dispatchIssueIdentity(DISTINCT_UNIT),
          seedFor(DISTINCT_UNIT, 'run-449'),
          'dispatcher-b',
          1_001,
          5_000,
        )

        expect(first, `${name}: #448`).toMatchObject({ acquired: true, created: true })
        expect(second, `${name}: #449`).toMatchObject({ acquired: true, created: true })
        expect(await store.listDispatchLifecycles(workspace), `${name}: two units, two rows`).toHaveLength(2)
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('lets a retry of the same work unit recognise its own claim instead of creating a second', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-work-unit-retry-'))
    try {
      for (const [name, store] of await stores(root)) {
        const workspace = `workspace-${name}`
        const key = dispatchIssueIdentity(LINEAR_MIRROR)

        const first = await store.claimDispatchLifecycle(
          workspace, key, seedFor(LINEAR_MIRROR, 'run-mirror'), 'dispatcher-a', 1_000, 5_000,
        )
        const retry = await store.claimDispatchLifecycle(
          workspace, key, seedFor(LINEAR_MIRROR, 'run-mirror'), 'dispatcher-a', 1_500, 5_000,
        )

        expect(first, `${name}: first claim`).toMatchObject({ acquired: true, created: true })
        expect(retry, `${name}: retry reclaims its own row`).toMatchObject({ acquired: true, created: false })
        expect(retry.lease?.epoch, `${name}: same owner keeps its fencing epoch`).toBe(first.lease?.epoch)
        expect(retry.lifecycle.runId, `${name}: retry keeps the original run`).toBe(first.lifecycle.runId)
        expect(await store.listDispatchLifecycles(workspace), `${name}: still one row`).toHaveLength(1)
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('adopts a live in-flight lease persisted under the old composite key, across a restart', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-work-unit-rekey-'))
    try {
      const watchStatePath = join(root, 'state.json')
      const legacyKey = legacyCompositeLifecycleKey(GITHUB_NATIVE)
      const canonicalKey = dispatchIssueIdentity(GITHUB_NATIVE)
      expect(legacyKey).not.toBe(canonicalKey)

      // A pre-deploy process claimed under the composite key and is still holding it.
      const before = new FileStateStore({ batchSize: 4, watchStatePath })
      const original = await before.claimDispatchLifecycle(
        'workspace-1', legacyKey, seedFor(GITHUB_NATIVE, 'run-legacy'), 'dispatcher-a', 1_000, 60_000,
      )
      expect(original).toMatchObject({ acquired: true, created: true })

      // The deployed build derives the canonical key and must find that row,
      // not create a second claim beside it.
      const after = new FileStateStore({ batchSize: 4, watchStatePath })
      const reclaimed = await after.claimDispatchLifecycle(
        'workspace-1', canonicalKey, seedFor(GITHUB_NATIVE, 'run-new'), 'dispatcher-a', 2_000, 60_000,
      )

      expect(reclaimed.created, 'the rekeyed row is not a new claim').toBe(false)
      expect(reclaimed.lifecycle.runId, 'the original run survives the rekey').toBe('run-legacy')
      expect(await after.getDispatchLifecycle('workspace-1', legacyKey)).toBeUndefined()
      expect(await after.listDispatchLifecycles('workspace-1')).toHaveLength(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses to choose when two keys for one work unit both hold a live lease', async () => {
    const store = new InMemoryStateStore({ batchSize: 4 })
    const legacyKey = legacyCompositeLifecycleKey(GITHUB_NATIVE)
    const surfaceKey = `${GITHUB_NATIVE.key}:${GITHUB_NATIVE.uuid}:/github/repos/AgentWorkforce/factory/issues/448__slug/meta.json`
    const canonicalKey = dispatchIssueIdentity(GITHUB_NATIVE)

    // Two dispatchers each took a lease under a different legacy spelling.
    await store.claimDispatchLifecycle(
      'ws', legacyKey, seedFor(GITHUB_NATIVE, 'run-a'), 'dispatcher-a', 1_000, 60_000,
    )
    const slugged: IssueRef = {
      ...GITHUB_NATIVE,
      path: '/github/repos/AgentWorkforce/factory/issues/448__slug/meta.json',
    }
    await store.claimDispatchLifecycle(
      'ws', surfaceKey, seedFor(slugged, 'run-b'), 'dispatcher-b', 1_000, 60_000,
    )

    await expect(store.claimDispatchLifecycle(
      'ws', canonicalKey, seedFor(GITHUB_NATIVE, 'run-c'), 'dispatcher-c', 1_500, 60_000,
    )).rejects.toThrow(/hold a live lease/u)

    // Fail closed: neither live row was touched and no new claim exists.
    expect(await store.getDispatchLifecycle('ws', canonicalKey)).toBeUndefined()
    expect(await store.getDispatchLifecycle('ws', legacyKey)).toMatchObject({ runId: 'run-a' })
    expect(await store.getDispatchLifecycle('ws', surfaceKey)).toMatchObject({ runId: 'run-b' })
  })

  it('retains a losing alias as typed audit evidence that cannot be dispatched', async () => {
    const store = new InMemoryStateStore({ batchSize: 4 })
    const legacyKey = legacyCompositeLifecycleKey(GITHUB_NATIVE)
    const canonicalKey = dispatchIssueIdentity(GITHUB_NATIVE)
    const slugged: IssueRef = {
      ...GITHUB_NATIVE,
      path: '/github/repos/AgentWorkforce/factory/issues/448__slug/meta.json',
    }
    const sluggedKey = legacyCompositeLifecycleKey(slugged)

    // Two legacy rows, only one of them still leased.
    const winner = await store.claimDispatchLifecycle(
      'ws', legacyKey, seedFor(GITHUB_NATIVE, 'run-live'), 'dispatcher-a', 1_000, 60_000,
    )
    await store.claimDispatchLifecycle(
      'ws', sluggedKey, seedFor(slugged, 'run-stale'), 'dispatcher-b', 1_000, 1,
    )
    expect(winner).toMatchObject({ acquired: true })

    await store.claimDispatchLifecycle(
      'ws', canonicalKey, seedFor(GITHUB_NATIVE, 'run-new'), 'dispatcher-a', 5_000, 60_000,
    )

    const alias = await store.getDispatchLifecycle('ws', sluggedKey)
    expect(alias?.migrationAliasOf, 'the loser is typed as a migration alias').toBe(canonicalKey)
    expect(alias?.lease, 'an alias holds no lease').toBeUndefined()
    // Audit-only: it must never be handed back as adoptable work.
    expect((await store.listDispatchLifecycles('ws')).map(([key]) => key)).toEqual([canonicalKey])
  })

  it('bounds alias retention and drops every alias once the canonical row is terminal', () => {
    const canonicalKey = 'github:agentworkforce/factory#448'
    const alias = (key: string, updatedAtMs: number, lease?: { leaseUntilMs: number }) => [
      key,
      {
        ...seedFor(GITHUB_NATIVE, `run-${key}`),
        migrationAliasOf: canonicalKey,
        updatedAtMs,
        ...(lease ? { lease: { owner: 'o', epoch: 1, ...lease } } : {}),
      },
    ] as [string, DispatchLifecycle]

    const live = { ...seedFor(GITHUB_NATIVE, 'run-canonical'), phase: 'running' as const }
    const entries: Array<[string, DispatchLifecycle]> = [
      [canonicalKey, live],
      alias('a1', 1_000),
      alias('a2', 2_000),
      alias('a3', 3_000),
      alias('leased', 500, { leaseUntilMs: 999_999 }),
    ]

    // Cap of 2 per canonical drops the oldest; the leased row is never eligible.
    const pruned = prunableMigrationAliases(entries, 10_000)
    expect(pruned).not.toContain('leased')
    expect(pruned).toContain('a1')
    expect(entries.filter(([key]) => !pruned.includes(key) && key !== canonicalKey)).toHaveLength(3)

    // Age cap.
    expect(prunableMigrationAliases(entries, 1_000 + 8 * 24 * 60 * 60 * 1_000)).toContain('a3')

    // Terminal canonical returns the alias count to zero, leased row aside.
    const terminal: Array<[string, DispatchLifecycle]> = [
      [canonicalKey, { ...live, phase: 'complete' }],
      alias('a1', 1_000),
      alias('a2', 2_000),
    ]
    expect(prunableMigrationAliases(terminal, 10_000).sort()).toEqual(['a1', 'a2'])
  })
})
