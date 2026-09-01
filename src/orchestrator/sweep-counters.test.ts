import { describe, expect, it, vi } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  FactoryConfigSchema,
  createFactory,
  publicHealthFromHeartbeat,
  type FactoryConfig,
  type LinearIssue,
  type TriageDecision,
  type TriageEngine,
} from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { InMemoryStateStore } from '../state/in-memory-state-store'
import type { DiscoverySweepClaim } from '../ports/state'
import { normalizePublicHealth } from './public-health'
import type { ChangeEvent } from '../ports'
import type { FactoryPublicReadinessReconcileHealth, FactoryReadinessReconcileStatus } from '../types'

/**
 * The sweep counters, end to end (#355).
 *
 * A healthy sub-second sweep declined seven eligible issues with a free
 * dispatch slot and every published subsystem read green. `candidates` splits
 * that in half: non-zero means the sweep saw them and rejected them, zero
 * means it never pulled them, and those are two different bugs with two
 * different owners.
 *
 * Every assertion below drives the real writer — a live daemon running a real
 * sweep over a real mount — rather than hand-setting the field on a status
 * fixture. A fixture would prove the projection copies a number it was handed;
 * only the sweep proves the number is the sweep's.
 */

const ready = '11111111-1111-4111-8111-111111111111'
const implementing = '22222222-2222-4222-8222-222222222222'
const done = '33333333-3333-4333-8333-333333333333'
const planning = '44444444-4444-4444-8444-444444444444'

const config = (overrides: Record<string, unknown> = {}): FactoryConfig => FactoryConfigSchema.parse({
  workspaceId: 'factory-sweep-counters',
  repos: {
    byLabel: { pear: 'AgentWorkforce/pear' },
    clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
    default: 'AgentWorkforce/pear',
  },
  triage: { maxImplementers: 4 },
  batchSize: 4,
  stateIds: { readyForAgent: ready, agentImplementing: implementing, done, inPlanning: planning },
  verification: { enabled: false },
  ...overrides,
})

const issuePath = (n: number) => `/linear/issues/AR-${n}__uuid-${n}.json`

const issueFile = (n: number, title = `[factory-e2e] Fix factory issue ${n}`) => ({
  provider: 'linear',
  objectType: 'issue',
  objectId: `uuid-${n}`,
  payload: {
    id: `uuid-${n}`,
    identifier: `AR-${n}`,
    title,
    description: 'Implement the requested fix in src/orchestrator/factory.ts and verify it with tests.',
    stateId: ready,
    url: `https://linear.app/agent-relay/issue/AR-${n}/factory-issue-${n}`,
    labels: [{ name: 'pear' }],
    labelIds: ['label-id-not-used-by-parser'],
    team: { key: 'AR', name: 'Agent Relay' },
    project: { name: 'Factory' },
    state: { id: ready, name: 'Ready for Agent' },
  },
})

class StaticTriage implements TriageEngine {
  async triage(issue: LinearIssue): Promise<TriageDecision> {
    const number = issue.key.match(/\d+/)?.[0] ?? '0'
    return {
      issue: { uuid: issue.uuid, key: issue.key, path: issue.path },
      routes: [{ repo: 'AgentWorkforce/pear', clonePath: '/work/pear', rationale: 'test route' }],
      scope: 'single',
      implementers: [{
        name: `ar-${number}-impl`,
        role: 'implementer',
        capability: 'spawn:codex',
        model: 'codex',
        task: `Implement ${issue.key}`,
        repo: 'AgentWorkforce/pear',
        clonePath: '/work/pear',
        node: 'self',
      }],
      reviewer: {
        name: `ar-${number}-review`,
        role: 'reviewer',
        capability: 'spawn:claude',
        model: 'claude',
        task: `Review ${issue.key}`,
        repo: 'AgentWorkforce/pear',
        clonePath: '/work/pear',
        node: 'self',
      },
      thin: false,
      confidence: 'high',
      rationale: 'static test decision',
    }
  }
}

class RemoteLifecycleFleetClient extends FakeFleetClient {
  override readonly placementLocality = 'remote' as const
  override readonly lifecycleActionName = 'factory.lifecycle'
}

/**
 * Fails GitHub issue enumeration the way an ordinary backend outage does.
 *
 * Deliberately neither a 429 nor an operation timeout: `isPassWideRelayfileFault`
 * must be false for this error, because that is the branch `#githubIssuePaths`
 * absorbs instead of rethrowing, and the absorbed branch is the #406 shape.
 */
class ToggleableGithubIssueMount extends FakeMountClient {
  failing = false

  override async listTree(prefix: string): Promise<string[]> {
    if (this.failing && prefix.startsWith('/github/repos/')) {
      throw new Error('github issue tree unavailable: 500 Internal Server Error')
    }
    return super.listTree(prefix)
  }
}

class CountingListTreeMount extends FakeMountClient {
  readonly listTreePrefixes: string[] = []

  override async listTree(prefix: string): Promise<string[]> {
    this.listTreePrefixes.push(prefix)
    return super.listTree(prefix)
  }
}

const changeEvent = (path: string): ChangeEvent => ({
  id: 'silent-cache-watermark',
  workspace: 'factory-sweep-counters',
  type: 'relayfile.changed',
  occurredAt: new Date().toISOString(),
  resource: { path, kind: 'file', id: path, provider: 'github' },
  summary: {},
  expand: async () => ({ level: 'summary', path, summary: {} }),
}) as unknown as ChangeEvent

/**
 * Run one live daemon over `files` and return the readiness record its own
 * startup sweep produced.
 *
 * The startup backfill is a full discovery pass and settles before `start()`
 * resolves, so this needs no timer advance and no polling — which is what
 * keeps these assertions off the nondeterminism in #342.
 */
async function sweepReadiness(
  files: Record<string, unknown>,
  overrides: Record<string, unknown> = {},
): Promise<{ status: FactoryReadinessReconcileStatus; spawns: string[] }> {
  const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
  const mount = new FakeMountClient(files)
  const fleet = new FakeFleetClient()
  const factory = createFactory(
    config({
      loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') },
      ...overrides,
    }),
    { mount, fleet, triage: new StaticTriage(), logger: process.env.SWEEP_DEBUG ? console : {} },
  )
  try {
    await factory.start({
      mode: 'live',
      // Long enough that no periodic pass can race the assertion: the startup
      // backfill is the sweep under test, and a second one landing mid-read
      // would make the numbers describe a pass the test never set up.
      liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 600_000 },
    })
    const status = factory.status().readinessReconcile
    if (!status) throw new Error('live daemon published no readinessReconcile status')
    return { status, spawns: fleet.spawns.map((spawn) => spawn.name) }
  } finally {
    await factory.stop()
    await rm(root, { recursive: true, force: true })
  }
}

/** The public record a container would serve for this readiness status. */
const published = (status: FactoryReadinessReconcileStatus): FactoryPublicReadinessReconcileHealth => {
  const health = publicHealthFromHeartbeat({
    pid: 1,
    status: 'running',
    iteration: 1,
    maxIterations: 1,
    updatedAt: new Date(1_700_000_000_000).toISOString(),
    updatedAtMs: 1_700_000_000_000,
    readinessReconcile: status,
  }, { nowMs: 1_700_000_000_000 })
  if (!health.readinessReconcile) throw new Error('public health carried no readinessReconcile block')
  return health.readinessReconcile
}

/** The must-fire expectation, as a function so the control can aim it elsewhere. */
const expectSawAndDispatched = (readiness: FactoryPublicReadinessReconcileHealth): void => {
  expect(readiness.candidates).toBeGreaterThan(0)
  expect(readiness.dispatched).toBeGreaterThan(0)
}

/** The must-not-fire expectation: a real zero, never an absence. */
const expectSweptNothing = (readiness: FactoryPublicReadinessReconcileHealth): void => {
  expect(readiness.candidates).toBe(0)
  expect(readiness.dispatched).toBe(0)
  expect(readiness.skipped).toBe(0)
}

describe('readiness sweep counters (#355)', () => {
  it('distinguishes index-backed empty discovery from a tree that served non-candidate content', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-github-empty-kinds-'))
    const create = (mount: FakeMountClient, workspaceId: string) => createFactory(config({
      workspaceId,
      issueSource: 'github',
      safety: { requireLabel: 'factory', requireTitlePrefix: '[factory]' },
      loop: { registryPath: join(root, `${workspaceId}.json`) },
    }), {
      mount,
      fleet: new RemoteLifecycleFleetClient(),
      triage: new StaticTriage(),
      logger: {},
    })

    try {
      const indexFactory = create(new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/_index.json': [],
      }), 'index-empty')
      const indexEmpty = await indexFactory.runOnce()
      expect(indexEmpty.pulled).toEqual([])
      expect(indexEmpty.treeReads).toBe(0)
      expect(indexEmpty.emptyTreeReads).toBe(0)
      expect(indexEmpty.discoverySources).toEqual({
        configuredRepos: 1,
        indexBackedRepos: 1,
        indexBackedEmptyRepos: 1,
        cacheBackedRepos: 0,
        cacheBackedEmptyRepos: 0,
        treeBackedRepos: 0,
        treeBackedEmptyRepos: 0,
        paths: 0,
      })
      await indexFactory.stop()

      const contentFactory = create(new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/README.json': { note: 'no ready issues' },
      }), 'content-no-candidates')
      const genuinelyEmpty = await contentFactory.runOnce()
      expect(genuinelyEmpty.pulled).toEqual([])
      expect(genuinelyEmpty.treeReads).toBe(2)
      expect(genuinelyEmpty.emptyTreeReads).toBe(1)
      expect(genuinelyEmpty.discoverySources).toEqual({
        configuredRepos: 1,
        indexBackedRepos: 0,
        indexBackedEmptyRepos: 0,
        cacheBackedRepos: 0,
        cacheBackedEmptyRepos: 0,
        treeBackedRepos: 1,
        treeBackedEmptyRepos: 0,
        paths: 1,
      })
      await contentFactory.stop()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('alarms once after three cache-backed zero-candidate sweeps that issue zero tree reads', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-github-silent-cache-'))
    const mount = new CountingListTreeMount()
    mount.emit(changeEvent('/factory/observability/mount-health/current.json'))
    const stateStore = new InMemoryStateStore({ batchSize: 2 })
    const create = (logger: { warn?: ReturnType<typeof vi.fn> } = {}) => createFactory(config({
      issueSource: 'github',
      safety: { requireLabel: 'factory', requireTitlePrefix: '[factory]' },
      loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') },
    }), {
      mount,
      fleet: new RemoteLifecycleFleetClient(),
      stateStore,
      triage: new StaticTriage(),
      logger,
    })

    let seedFactory: ReturnType<typeof create> | undefined
    let factory: ReturnType<typeof create> | undefined
    // `stop()` has no early-return guard, so an unconditional second call
    // would make every passing run pay a full extra shutdown on both
    // factories. These flags keep the guarded net for the failure path only.
    let seedStopped = false
    let stopped = false
    try {
      seedFactory = create()
      const seed = await seedFactory.runOnce()
      expect(seed.discoverySources).toMatchObject({
        configuredRepos: 1,
        treeBackedRepos: 1,
        treeBackedEmptyRepos: 1,
        paths: 0,
      })
      expect(seed.treeReads).toBe(2)
      expect(seed.emptyTreeReads).toBe(2)
      await seedFactory.stop()
      seedStopped = true
      const readsAfterSeed = mount.listTreePrefixes.length

      const warn = vi.fn()
      factory = create({ warn })
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 25 },
      })

      await vi.waitFor(() => {
        expect(factory.status().readinessReconcile).toMatchObject({
          zeroCandidateAlarmActive: true,
        })
      }, { timeout: 5_000 })
      const snapshot = factory.status()
      expect(snapshot.readinessReconcile).toMatchObject({
          candidates: 0,
          treeReads: 0,
          emptyTreeReads: 0,
          configuredRepos: 1,
          indexBackedRepos: 0,
          indexBackedEmptyRepos: 0,
          cacheBackedRepos: 1,
          cacheBackedEmptyRepos: 1,
          treeBackedRepos: 0,
          treeBackedEmptyRepos: 0,
          discoveryPaths: 0,
          zeroCandidateAlarmThreshold: 3,
          zeroCandidateAlarmActive: true,
      })
      const emptySweeps = snapshot.readinessReconcile?.zeroCandidateSweeps
      expect(emptySweeps).toBeGreaterThanOrEqual(3)
      expect(snapshot.counters.readinessPersistentZeroCandidateSignals).toBe(1)
      expect(snapshot.counters.readinessZeroCandidateSweeps).toBe(emptySweeps)
      expect(snapshot.counters.readinessZeroTreeReadSweeps).toBe(emptySweeps)
      expect(snapshot.counters.githubIssueDiscoveryCacheEmptyRepos).toBe(emptySweeps)
      expect(snapshot.counters.githubIssueIndexEmptyRepos).toBeUndefined()
      expect(snapshot.counters.githubIssueTreeEmptyRepos).toBeUndefined()

      // The exact #402 failure shape: both durable repository roots are
      // present-but-empty, so fallback uses cache, yields no paths, and issues
      // no listTree. The fleet is explicitly remote, matching production.
      expect(mount.listTreePrefixes).toHaveLength(readsAfterSeed)
      expect(warn.mock.calls.filter(([message]) =>
        message === '[factory] discovery has persistently produced zero candidates')).toHaveLength(1)
      expect(warn.mock.calls.find(([message]) =>
        message === '[factory] discovery has persistently produced zero candidates')?.[1]).toMatchObject({
        consecutiveSweeps: 3,
        threshold: 3,
        configuredRepos: 1,
        cacheBackedRepos: 1,
        cacheBackedEmptyRepos: 1,
        discoveryPaths: 0,
        treeReads: 0,
        emptyTreeReads: 0,
      })

      await vi.waitFor(() => {
        expect(factory.status().readinessReconcile?.zeroCandidateSweeps).toBeGreaterThan(3)
      }, { timeout: 5_000 })
      expect(warn.mock.calls.filter(([message]) =>
        message === '[factory] discovery has persistently produced zero candidates')).toHaveLength(1)
      await factory.stop()
      stopped = true
    } finally {
      // Guarded, and in the `finally`: every assertion above runs against a
      // started daemon holding a 25ms reconcile timer, so a failing one left
      // that timer running for the rest of the worker's life while this block
      // removed the temp dir out from under it. Swallowing is deliberate --
      // a throwing teardown replaces the real assertion failure in the report
      // with its own stack. The happy-path `stop()` above stays unguarded, so
      // a genuine shutdown fault still fails this test; this runs only when
      // the try did not get that far.
      if (!seedStopped) await seedFactory?.stop().catch(() => {})
      if (!stopped) await factory?.stop().catch(() => {})
      await rm(root, { recursive: true, force: true })
    }
  }, 15_000)

  /**
   * #406: a failed enumeration is not an empty one.
   *
   * `#githubIssuePaths` absorbs an ordinary backend error -- it rethrows only
   * a pass-wide relayfile fault -- and returns no paths, after the per-repo
   * `configuredRepos` counter has already been incremented. That settles the
   * sweep in exactly the shape the zero-candidate alarm watches for. The pair
   * below differs in one variable, `mount.failing`, so the guard is shown to
   * be scoped to the failure rather than disabling the alarm.
   */
  const discoveryFailureHarness = async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-github-discovery-failure-'))
    const mount = new ToggleableGithubIssueMount()
    mount.emit(changeEvent('/factory/observability/mount-health/current.json'))
    const stateStore = new InMemoryStateStore({ batchSize: 2 })
    const warn = vi.fn()
    const create = () => createFactory(config({
      issueSource: 'github',
      safety: { requireLabel: 'factory', requireTitlePrefix: '[factory]' },
      loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') },
    }), {
      mount,
      fleet: new RemoteLifecycleFleetClient(),
      stateStore,
      triage: new StaticTriage(),
      logger: { warn },
    })
    const alarms = () => warn.mock.calls.filter(([message]) =>
      message === '[factory] discovery has persistently produced zero candidates')
    return { root, mount, create, warn, alarms }
  }

  it('does not count a failed GitHub enumeration as a zero-candidate sweep (#406)', async () => {
    const { root, mount, create, alarms } = await discoveryFailureHarness()
    mount.failing = true
    let factory: ReturnType<typeof create> | undefined
    let stopped = false
    try {
      factory = create()
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 25 },
      })

      // Four absorbed failures: one more than the alarm threshold, so a sweep
      // miscounted as empty would already have raised it.
      await vi.waitFor(() => {
        expect(factory!.status().counters.githubIssueListFailures ?? 0).toBeGreaterThanOrEqual(4)
      }, { timeout: 5_000 })

      const snapshot = factory.status()
      expect(snapshot.counters.githubIssueListFailures).toBeGreaterThanOrEqual(4)
      // The defect first, so this test fails on the behaviour it exists to
      // pin rather than on the marker that implements it: a failed pass is
      // never laundered into a zero-candidate measurement, and so can never
      // raise the persistent-empty-discovery alarm.
      expect(alarms()).toHaveLength(0)
      expect(snapshot.counters.readinessPersistentZeroCandidateSignals).toBeUndefined()
      expect(snapshot.counters.readinessZeroCandidateSweeps).toBeUndefined()
      expect(snapshot.readinessReconcile?.zeroCandidateAlarmActive).toBeFalsy()
      expect(snapshot.readinessReconcile?.zeroCandidateSweeps ?? 0).toBe(0)
      // Then the marker that preserves it as a failure.
      expect(snapshot.readinessReconcile?.discoveryFailed).toBe('issue-listing-failed')
      await factory.stop()
      stopped = true
    } finally {
      if (!stopped) await factory?.stop().catch(() => {})
      await rm(root, { recursive: true, force: true })
    }
  }, 15_000)

  it('still alarms when the same harness enumerates successfully and finds nothing (#406 control)', async () => {
    const { root, mount, create, alarms } = await discoveryFailureHarness()
    // The single variable: discovery now succeeds and genuinely finds nothing.
    mount.failing = false
    let factory: ReturnType<typeof create> | undefined
    let stopped = false
    try {
      factory = create()
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 25 },
      })

      await vi.waitFor(() => {
        expect(factory!.status().readinessReconcile).toMatchObject({
          zeroCandidateAlarmActive: true,
        })
      }, { timeout: 5_000 })

      const snapshot = factory.status()
      expect(snapshot.counters.githubIssueListFailures).toBeUndefined()
      expect(snapshot.readinessReconcile?.discoveryFailed).toBeUndefined()
      expect(snapshot.readinessReconcile?.zeroCandidateSweeps ?? 0).toBeGreaterThanOrEqual(3)
      expect(snapshot.counters.readinessPersistentZeroCandidateSignals).toBe(1)
      expect(alarms()).toHaveLength(1)
      await factory.stop()
      stopped = true
    } finally {
      if (!stopped) await factory?.stop().catch(() => {})
      await rm(root, { recursive: true, force: true })
    }
  }, 15_000)

  it('does not mark the sweep failed when only the GitHub mirror ingest failed (#406 review)', async () => {
    // cubic, P2. `#githubIssuePaths` serves two roles. On a LINEAR-source
    // workspace it is reached from `#ingestGithubIssues`, which only hydrates
    // the GitHub mirror -- the sink enumeration is the Linear tree and can
    // still return candidates. Marking the sweep failed from the mirror path
    // stranded a sweep that DID find work: `#recordReadinessSweepOutcome`
    // returned early, so its counts were never recorded and the
    // zero-candidate streak was never reset.
    const root = await mkdtemp(join(tmpdir(), 'factory-mirror-ingest-failure-'))
    // Linear source (the config default), so the mirror ingest runs and fails
    // while the Linear tree below still serves a ready issue.
    const mount = new ToggleableGithubIssueMount({ [issuePath(971)]: issueFile(971) })
    mount.failing = true
    let factory: ReturnType<typeof createFactory> | undefined
    let stopped = false
    try {
      factory = createFactory(
        config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
        { mount, fleet: new FakeFleetClient(), stateStore: new InMemoryStateStore({ batchSize: 4 }), triage: new StaticTriage(), logger: {} },
      )
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
      })

      // The sweep found and dispatched real work, so it MUST be recorded.
      await vi.waitFor(() => {
        expect(factory!.status().readinessReconcile).toMatchObject({
          candidates: 1,
          dispatched: 1,
        })
      }, { timeout: 5_000 })

      const status = factory.status().readinessReconcile
      // The mirror failure is not this sweep's enumeration failing.
      expect(status?.discoveryFailed).toBeUndefined()
      expect(status?.zeroCandidateSweeps ?? 0).toBe(0)
      expect(status?.zeroCandidateAlarmActive).toBeFalsy()
      await factory.stop()
      stopped = true
    } finally {
      if (!stopped) await factory?.stop().catch(() => {})
      await rm(root, { recursive: true, force: true })
    }
  }, 15_000)

  it('retires the discovery-failure marker when the next pass defers (#406 review)', async () => {
    // CodeRabbit, minor. Both markers describe THE most recent pass, so they
    // must never be publishable together: a sweep cannot simultaneously have
    // failed to enumerate and have deferred to another owner's lease.
    class DeferrableStateStore extends InMemoryStateStore {
      deferClaims = false

      override async claimDiscoverySweep(
        workspaceId: string,
        owner: string,
        nowMs: number,
        leaseMs: number,
      ): Promise<DiscoverySweepClaim> {
        const claim = await super.claimDiscoverySweep(
          workspaceId,
          this.deferClaims ? 'another-process' : owner,
          nowMs,
          leaseMs,
        )
        return this.deferClaims ? { ...claim, acquired: false, lease: undefined } : claim
      }
    }

    const root = await mkdtemp(join(tmpdir(), 'factory-discovery-failure-then-defer-'))
    const mount = new ToggleableGithubIssueMount()
    mount.failing = true
    const stateStore = new DeferrableStateStore({ batchSize: 2 })
    let factory: ReturnType<typeof createFactory> | undefined
    let stopped = false
    try {
      factory = createFactory(config({
        issueSource: 'github',
        safety: { requireLabel: 'factory', requireTitlePrefix: '[factory]' },
        loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') },
      }), {
        mount,
        fleet: new RemoteLifecycleFleetClient(),
        stateStore,
        triage: new StaticTriage(),
        logger: {},
      })
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 25 },
      })

      // A failing enumeration first, so the marker is genuinely set.
      await vi.waitFor(() => {
        expect(factory!.status().readinessReconcile?.discoveryFailed).toBe('issue-listing-failed')
      }, { timeout: 5_000 })

      stateStore.deferClaims = true

      await vi.waitFor(() => {
        expect(factory!.status().readinessReconcile?.discoveryDeferred).toBe('sweep-in-flight')
      }, { timeout: 5_000 })

      const status = factory.status().readinessReconcile
      expect(status?.discoveryDeferred).toBe('sweep-in-flight')
      // The point of the test: the older pass's failure is retired, not
      // published alongside the deferral it did not cause.
      expect(status?.discoveryFailed).toBeUndefined()
      await factory.stop()
      stopped = true
    } finally {
      if (!stopped) await factory?.stop().catch(() => {})
      await rm(root, { recursive: true, force: true })
    }
  }, 15_000)

  it('publishes a non-zero candidate count for a sweep that found and dispatched ready work', async () => {
    const { status, spawns } = await sweepReadiness({
      [issuePath(901)]: issueFile(901),
      [issuePath(902)]: issueFile(902),
    })

    expect(spawns).toEqual(['ar-901-impl-pear', 'ar-901-review', 'ar-902-impl-pear', 'ar-902-review'])
    expect(status).toMatchObject({ candidates: 2, dispatched: 2, skipped: 0 })
    expect(published(status)).toMatchObject({ candidates: 2, dispatched: 2, skipped: 0 })
    // On a pass that enumerated, the two stamps describe the same instant.
    expect(status.lastEnumeratedAtMs).toBe(status.lastCompletedAtMs)
    expect(published(status).lastEnumeratedAtMs).toBe(status.lastCompletedAtMs)
  })

  it('publishes zero — not undefined — for a sweep that completed and found nothing', async () => {
    const { status, spawns } = await sweepReadiness({})

    expect(spawns).toEqual([])
    expect(status).toMatchObject({ candidates: 0, dispatched: 0, skipped: 0 })
    const readiness = published(status)
    expectSweptNothing(readiness)
    // `toMatchObject` is satisfied by an absent key holding `undefined`, and
    // absent-vs-zero is the entire distinction this field exists to carry.
    expect(Object.hasOwn(readiness, 'candidates')).toBe(true)
    expect(Object.hasOwn(readiness, 'dispatched')).toBe(true)
    expect(Object.hasOwn(readiness, 'skipped')).toBe(true)
    // And it survives the wire: a reader re-normalising a served record must
    // not turn the published zero back into an absence.
    const roundTripped = normalizePublicHealth(JSON.parse(JSON.stringify({
      schemaVersion: 1,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: readiness,
    })))
    expect(roundTripped?.readinessReconcile).toMatchObject({ candidates: 0, dispatched: 0, skipped: 0 })
    expect(Object.hasOwn(roundTripped?.readinessReconcile ?? {}, 'candidates')).toBe(true)
  })

  it('CONTROL: the two expectations are not interchangeable, so a swap would fail the suite', async () => {
    // The pair above is only evidence if each assertion can tell the two
    // sweeps apart. If `candidates` were hard-wired — to a constant, to
    // `pulled.length` of the wrong pass, or to zero — one of these four would
    // stop throwing, and the must-fire/must-not-fire pair would pass for a
    // reason that has nothing to do with what the sweep saw.
    const found = published((await sweepReadiness({
      [issuePath(903)]: issueFile(903),
    })).status)
    const empty = published((await sweepReadiness({})).status)

    expect(() => expectSweptNothing(found)).toThrow()
    expect(() => expectSawAndDispatched(empty)).toThrow()
    expect(() => expectSawAndDispatched(found)).not.toThrow()
    expect(() => expectSweptNothing(empty)).not.toThrow()
  })

  it('leaves the counters absent until a sweep completes, so "never ran" stays readable', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      { mount: new FakeMountClient(), fleet: new FakeFleetClient(), triage: new StaticTriage(), logger: {} },
    )
    try {
      const status = factory.status().readinessReconcile
      expect(status?.state).toBe('not-running')
      expect(status && Object.hasOwn(status, 'candidates')).toBe(false)
      expect(status && Object.hasOwn(status, 'lastEnumeratedAtMs')).toBe(false)
      // The projection must not invent a zero for it either — that would make
      // an instance that has never swept indistinguishable from one that swept
      // and found nothing, which is the ambiguity #355 was stuck on.
      expect(status && Object.hasOwn(published(status), 'candidates')).toBe(false)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('splits `skipped` by a bounded reason code when the sweep saw work and rejected it', async () => {
    // The exact shape #355 suspects: the sweep pulls every ready issue and the
    // eligibility gate declines some of them. `candidates` alone says "it saw
    // them"; the code says which gate, which is what names the owner.
    const { status, spawns } = await sweepReadiness({
      [issuePath(911)]: issueFile(911),
      [issuePath(912)]: issueFile(912, 'Ordinary product issue, not in factory scope'),
    })

    expect(spawns).toEqual(['ar-911-impl-pear', 'ar-911-review'])
    expect(status).toMatchObject({
      candidates: 2,
      dispatched: 1,
      skipped: 1,
      skipReasons: { 'out-of-scope': 1 },
    })
    expect(published(status).skipReasons).toEqual({ 'out-of-scope': 1 })
  })

  it('records the PERIODIC sweep, not just the startup backfill', async () => {
    // Everything above drives the startup backfill, which is one of two call
    // sites. The deployed outage is the *periodic* loop: on the live container
    // it had been the only sweep running for hours. A recorder wired to the
    // backfill alone would satisfy every other test in this file and publish
    // nothing for the pass that matters.
    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const mount = new FakeMountClient()
    const fleet = new FakeFleetClient()
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      { mount, fleet, triage: new StaticTriage(), logger: {} },
    )
    try {
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
      })
      // The backfill swept an empty mount, so anything non-zero from here can
      // only have come from a later pass.
      expect(factory.status().readinessReconcile).toMatchObject({ candidates: 0, dispatched: 0 })

      mount.files.set(issuePath(931), { content: issueFile(931) })

      await vi.waitFor(() => {
        expect(factory.status().counters.readinessReconcileSweeps).toBeGreaterThanOrEqual(1)
        expect(factory.status().readinessReconcile).toMatchObject({
          candidates: 1,
          dispatched: 1,
          skipped: 0,
        })
      }, { timeout: 5_000 })
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('names a sweep that deferred to another owner, so its zero is not read as an empty provider', async () => {
    // The nastiest reading of `candidates: 0`. A sweep that never claimed the
    // discovery lease returns an empty report immediately and completes
    // *healthy* in milliseconds — indistinguishable, on counts alone, from one
    // that queried every routed repo and legitimately found no ready work.
    class LeaseHeldElsewhereStateStore extends InMemoryStateStore {
      override async claimDiscoverySweep(
        workspaceId: string,
        owner: string,
        nowMs: number,
        leaseMs: number,
      ): Promise<DiscoverySweepClaim> {
        const claim = await super.claimDiscoverySweep(workspaceId, 'another-process', nowMs, leaseMs)
        return { ...claim, acquired: false, lease: undefined }
      }
    }

    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const stateStore = new LeaseHeldElsewhereStateStore({ batchSize: 4 })
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      {
        mount: new FakeMountClient({ [issuePath(941)]: issueFile(941) }),
        fleet: new FakeFleetClient(),
        stateStore,
        triage: new StaticTriage(),
        logger: {},
      },
    )
    try {
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 600_000 },
      })
      const status = factory.status().readinessReconcile
      // Nothing has enumerated, so there are NO counts to publish — and the
      // marker still has to say why. A `0` here would claim a sweep queried the
      // provider and found nothing, which is the opposite diagnosis.
      expect(status).toMatchObject({ state: 'healthy', discoveryDeferred: 'sweep-in-flight' })
      expect(status && Object.hasOwn(status, 'candidates')).toBe(false)
      const readiness = published(status!)
      expect(readiness.discoveryDeferred).toBe('sweep-in-flight')
      expect(Object.hasOwn(readiness, 'candidates')).toBe(false)
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('a deferred pass does not overwrite the last enumerating sweep it followed', async () => {
    // #358 review, CodeRabbit (Major). A deferred pass settles healthy in
    // milliseconds having read nothing. Folding its zeroes into the snapshot
    // erased the last real measurement — and where another process holds the
    // lease for any length of time, EVERY pass would publish `candidates: 0`
    // and the numbers this change exists to provide would be unrecoverable.
    class DeferrableStateStore extends InMemoryStateStore {
      deferClaims = false

      override async claimDiscoverySweep(
        workspaceId: string,
        owner: string,
        nowMs: number,
        leaseMs: number,
      ): Promise<DiscoverySweepClaim> {
        const claim = await super.claimDiscoverySweep(
          workspaceId,
          this.deferClaims ? 'another-process' : owner,
          nowMs,
          leaseMs,
        )
        return this.deferClaims ? { ...claim, acquired: false, lease: undefined } : claim
      }
    }

    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const mount = new FakeMountClient({ [issuePath(961)]: issueFile(961) })
    const stateStore = new DeferrableStateStore({ batchSize: 4 })
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      { mount, fleet: new FakeFleetClient(), stateStore, triage: new StaticTriage(), logger: {} },
    )
    try {
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
      })
      // A real enumeration first.
      await vi.waitFor(() => expect(factory.status().readinessReconcile).toMatchObject({
        candidates: 1,
        dispatched: 1,
        skipped: 0,
      }), { timeout: 5_000 })

      stateStore.deferClaims = true

      // Then deferred passes, for long enough that several land.
      await vi.waitFor(() => {
        const deferred = factory.status().readinessReconcile
        expect(deferred?.discoveryDeferred).toBe('sweep-in-flight')
        expect(deferred?.lastEnumeratedAtMs).toBeDefined()
        expect(deferred?.lastCompletedAtMs).toBeGreaterThan(deferred?.lastEnumeratedAtMs ?? 0)
      }, { timeout: 5_000 })

      const status = factory.status().readinessReconcile
      // Capture the enumerating baseline only after deferral is observable. A
      // 50ms pass can land between the first wait and flipping `deferClaims`,
      // so a pre-toggle completion timestamp is inherently racy.
      const completedAtMs = status?.lastEnumeratedAtMs
      expect(completedAtMs).toBeDefined()
      // The measurement survives, and the marker says it is from an earlier pass.
      expect(status).toMatchObject({
        state: 'healthy',
        candidates: 1,
        dispatched: 1,
        skipped: 0,
        discoveryDeferred: 'sweep-in-flight',
      })
      // `lastCompletedAtMs` DOES move for a deferred pass, deliberately: the
      // #295/#296 stall derivation reads it against `lastStartedAtMs`, so
      // freezing it would report a daemon that is correctly deferring as hung
      // after ten intervals.
      expect(status?.lastCompletedAtMs).toBeGreaterThan(completedAtMs ?? 0)
      // ...which is exactly why the counts need their own stamp (#359 review).
      // It stays pinned to the pass that enumerated, so the gap between the
      // two IS the staleness of the retained measurement — without it, old
      // counts sat beside an ever-fresh completion time and a reader could not
      // tell a measurement one interval old from one four days old.
      expect(status?.lastEnumeratedAtMs).toBe(completedAtMs)
      expect(status?.lastEnumeratedAtMs).toBeLessThan(status?.lastCompletedAtMs ?? 0)
      expect(published(status!)).toMatchObject({
        candidates: 1,
        dispatched: 1,
        discoveryDeferred: 'sweep-in-flight',
        lastEnumeratedAtMs: completedAtMs,
      })

      // And it holds across FURTHER deferrals rather than creeping forward.
      const pinned = status?.lastEnumeratedAtMs
      await vi.waitFor(() => {
        expect(factory.status().readinessReconcile?.lastCompletedAtMs)
          .toBeGreaterThan(status?.lastCompletedAtMs ?? 0)
      }, { timeout: 5_000 })
      expect(factory.status().readinessReconcile?.lastEnumeratedAtMs).toBe(pinned)
      expect(factory.status().readinessReconcile?.candidates).toBe(1)
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('logs THIS pass\'s breakdown on a deferred completion, never the retained one', async () => {
    // #359 review, codex P2. The completion log is what a local operator reads,
    // and it drew `skipReasons` from the retained snapshot while drawing the
    // counts from the current report. On a deferred pass that printed
    // `skipped: 0` beside a non-empty breakdown — a line contradicting its own
    // arithmetic, on the surface meant to explain the arithmetic.
    class DeferrableStateStore extends InMemoryStateStore {
      deferClaims = false

      override async claimDiscoverySweep(
        workspaceId: string,
        owner: string,
        nowMs: number,
        leaseMs: number,
      ): Promise<DiscoverySweepClaim> {
        const claim = await super.claimDiscoverySweep(
          workspaceId,
          this.deferClaims ? 'another-process' : owner,
          nowMs,
          leaseMs,
        )
        return this.deferClaims ? { ...claim, acquired: false, lease: undefined } : claim
      }
    }

    const completions: Array<Record<string, unknown>> = []
    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const stateStore = new DeferrableStateStore({ batchSize: 4 })
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      {
        mount: new FakeMountClient({
          [issuePath(971)]: issueFile(971),
          [issuePath(972)]: issueFile(972, 'Ordinary product issue, not in factory scope'),
        }),
        fleet: new FakeFleetClient(),
        stateStore,
        triage: new StaticTriage(),
        logger: {
          info: (message: string, detail?: unknown) => {
            if (message === '[factory] periodic readiness reconciliation completed') {
              completions.push(detail as Record<string, unknown>)
            }
          },
        },
      },
    )
    try {
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
      })
      // One real pass that skips something, so a retained breakdown exists.
      // (The startup backfill dispatches 971 and skips 972; a periodic pass
      // then skips both — 971 as `already-tracked` — so assert on the shape,
      // not on an exact count.)
      await vi.waitFor(() => {
        expect(completions.some((entry) => (entry.skipped as number) > 0)).toBe(true)
      }, { timeout: 5_000 })
      const enumerated = completions.find((entry) => (entry.skipped as number) > 0)
      expect(enumerated?.skipReasons).toMatchObject({ 'out-of-scope': 1 })
      // Internally consistent: the breakdown sums to the count beside it.
      expect(Object.values(enumerated?.skipReasons as Record<string, number>)
        .reduce((sum, n) => sum + n, 0)).toBe(enumerated?.skipped)

      stateStore.deferClaims = true
      await vi.waitFor(() => {
        expect(completions.some((entry) => entry.discoveryDeferred === 'sweep-in-flight')).toBe(true)
      }, { timeout: 5_000 })

      // Every deferred line is internally consistent: zero skips, no breakdown.
      const deferredLines = completions.filter((entry) => entry.discoveryDeferred === 'sweep-in-flight')
      expect(deferredLines.length).toBeGreaterThan(0)
      for (const entry of deferredLines) {
        expect(entry).toMatchObject({ candidates: 0, dispatched: 0, skipped: 0 })
        expect(entry.skipReasons).toEqual({})
      }
      // ...while the published surface still retains the real measurement.
      const retained = factory.status().readinessReconcile
      expect(retained?.candidates).toBe(2)
      expect((retained?.skipped ?? 0)).toBeGreaterThan(0)
      expect(retained?.skipReasons).toMatchObject({ 'out-of-scope': 1 })
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
    // Two sequential waits on a live 50ms loop; vitest's 5s default is not a
    // budget this fits in, and this repo configures no `testTimeout`.
  }, 30_000)

  it('clears an older deferral marker when the next pass fails', async () => {
    class ControllableStateStore extends InMemoryStateStore {
      mode: 'enumerate' | 'defer' | 'fail' = 'enumerate'

      override async claimDiscoverySweep(
        workspaceId: string,
        owner: string,
        nowMs: number,
        leaseMs: number,
      ): Promise<DiscoverySweepClaim> {
        if (this.mode === 'fail') throw new Error('discovery failed after deferral')
        const claim = await super.claimDiscoverySweep(
          workspaceId,
          this.mode === 'defer' ? 'another-process' : owner,
          nowMs,
          leaseMs,
        )
        return this.mode === 'defer' ? { ...claim, acquired: false, lease: undefined } : claim
      }
    }

    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-counters-'))
    const stateStore = new ControllableStateStore({ batchSize: 4 })
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      {
        mount: new FakeMountClient({ [issuePath(981)]: issueFile(981) }),
        fleet: new FakeFleetClient(),
        stateStore,
        triage: new StaticTriage(),
        logger: {},
      },
    )
    try {
      await factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
      })
      await vi.waitFor(() => {
        expect(factory.status().readinessReconcile?.lastEnumeratedAtMs).toBeDefined()
      }, { timeout: 5_000 })

      stateStore.mode = 'defer'
      await vi.waitFor(() => {
        expect(factory.status().readinessReconcile?.discoveryDeferred).toBe('sweep-in-flight')
      }, { timeout: 5_000 })

      stateStore.mode = 'fail'
      await vi.waitFor(() => {
        const failed = factory.status().readinessReconcile
        expect(failed?.lastErrorClass).toBe('Error')
        expect(failed?.lastFailureAtMs).toBeDefined()
        expect(failed && Object.hasOwn(failed, 'discoveryDeferred')).toBe(false)
      }, { timeout: 5_000 })

      // The last real measurement remains useful; only the stale attribution
      // to lease contention is removed by the newer failed pass.
      expect(factory.status().readinessReconcile).toMatchObject({ candidates: 1 })
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  }, 30_000)

  it('publishes counts only: no issue key, path or title reaches the unauthenticated record', async () => {
    const { status } = await sweepReadiness({
      [issuePath(921)]: issueFile(921),
      [issuePath(922)]: issueFile(922, 'Ordinary product issue, not in factory scope'),
    })

    const serialized = JSON.stringify(published(status))
    expect(serialized).not.toContain('AR-921')
    expect(serialized).not.toContain('AR-922')
    expect(serialized).not.toContain('/linear/issues')
    expect(serialized).not.toContain('Ordinary product issue')
    expect(serialized).not.toContain('factory-e2e')
    expect(serialized).not.toContain('AgentWorkforce/pear')
    // Everything it does say is a number or a published code.
    for (const [key, value] of Object.entries(JSON.parse(serialized) as Record<string, unknown>)) {
      if (key === 'state') continue
      if (key === 'skipReasons') {
        for (const count of Object.values(value as Record<string, unknown>)) {
          expect(typeof count).toBe('number')
        }
        continue
      }
      expect(typeof value).not.toBe('object')
    }
  })
})
