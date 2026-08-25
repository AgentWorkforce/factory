import { describe, expect, it } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  FactoryConfigSchema,
  createFactory,
  type FactoryConfig,
  type LinearIssue,
  type TriageDecision,
  type TriageEngine,
} from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { withDeadline } from '../testing/deadline'
import { InMemoryStateStore } from '../state/in-memory-state-store'
import {
  DISCOVERY_SWEEP_TEARDOWN_TIMEOUT_MS,
  DiscoverySweepBudgetExceededError,
  startDiscoverySweepBudget,
  withSweepTeardownDeadline,
} from './sweep-budget'

/**
 * The aggregate sweep budget (#372).
 *
 * Three unbounded calls have wedged this sweep in one day, on three different
 * transports, each one bounded afterwards and each time the wedge returned one
 * layer down. So the subject here is not another call. It is the property that
 * a sweep cannot be in flight for longer than its budget REGARDLESS of what it
 * is waiting on — which is what turns the next unbounded call into a degraded
 * sweep instead of the end of dispatch.
 *
 * Every must-fire below hangs a call that never returns. Before this change
 * each one hangs the test itself, at vitest's 5 s default: that is the defect,
 * in the same shape production had it.
 */

const NEVER = (): Promise<never> => new Promise<never>(() => undefined)

describe('sweep budget primitive', () => {
  it('must-fire: abandons an in-flight await that never returns, naming the phase', async () => {
    const budget = startDiscoverySweepBudget(30)
    try {
      await expect(budget.run('discovery-session', NEVER)).rejects.toThrow(DiscoverySweepBudgetExceededError)
    } finally {
      budget.dispose()
    }
  })

  it('must-fire: charges every phase against ONE clock, so the sum cannot exceed the budget', async () => {
    // The property a per-call deadline cannot have. Each call here is well
    // inside the budget on its own; only their sum is not.
    const budget = startDiscoverySweepBudget(120)
    const sleep = (ms: number) => new Promise<string>((resolve) => setTimeout(() => resolve('served'), ms))
    try {
      expect(await budget.run('discovery-session', () => sleep(40))).toBe('served')
      expect(await budget.run('discovery-checkpoint', () => sleep(40))).toBe('served')
      // The third 40 ms call is identical to the two that were served. It is
      // rejected because the SWEEP is out of time, not because it is slow.
      await expect(budget.run('discovery-commit', () => sleep(80))).rejects.toMatchObject({
        name: 'DiscoverySweepBudgetExceededError',
        phase: 'discovery-commit',
        budgetMs: 120,
      })
    } finally {
      budget.dispose()
    }
  })

  it('must-fire: a bounded call inside an unbounded retry loop still ends at the budget (L3)', async () => {
    // The deployed 0.1.75 shape: #368's per-call deadline fires every time, and
    // the retry around it makes the total unbounded anyway. `attempts` is
    // deliberately unbounded — the budget is the only thing that stops it.
    const budget = startDiscoverySweepBudget(80)
    let attempts = 0
    const alwaysRejects = async (): Promise<never> => {
      attempts += 1
      await new Promise((resolve) => setTimeout(resolve, 5))
      throw new Error('relayfile getEvents did not respond within 5ms')
    }
    const retryForever = async (): Promise<never> => {
      for (;;) {
        try {
          return await budget.run('run-once', alwaysRejects)
        } catch (error) {
          if (error instanceof DiscoverySweepBudgetExceededError) throw error
          // Exactly what L3 does: swallow the per-call deadline and go again.
        }
      }
    }
    try {
      await expect(withDeadline(retryForever(), 4_000, 'retry loop never ended')).rejects
        .toThrow(DiscoverySweepBudgetExceededError)
      expect(attempts).toBeGreaterThan(1)
    } finally {
      budget.dispose()
    }
  })

  it('must-fire: aborts its signal at expiry, so anything that honours one is really cancelled', async () => {
    const budget = startDiscoverySweepBudget(25)
    try {
      expect(budget.signal.aborted).toBe(false)
      await expect(budget.run('run-once', NEVER)).rejects.toThrow(DiscoverySweepBudgetExceededError)
      expect(budget.signal.aborted).toBe(true)
      expect(budget.signal.reason).toBeInstanceOf(DiscoverySweepBudgetExceededError)
    } finally {
      budget.dispose()
    }
  })

  it('must-fire: refuses to start new work once the budget is spent', async () => {
    const budget = startDiscoverySweepBudget(25)
    let started = false
    try {
      await expect(budget.run('run-once', NEVER)).rejects.toThrow(DiscoverySweepBudgetExceededError)
      await expect(budget.run('discovery-commit', async () => {
        started = true
        return 'served'
      })).rejects.toThrow(DiscoverySweepBudgetExceededError)
      // A spent budget must not issue another request against the dependency
      // it just gave up on.
      expect(started).toBe(false)
      expect(() => budget.assertNotExpired('run-once')).toThrow(DiscoverySweepBudgetExceededError)
    } finally {
      budget.dispose()
    }
  })

  it('must-not-fire: work that finishes inside the budget returns unchanged', async () => {
    const budget = startDiscoverySweepBudget(2_000)
    try {
      const report = { dispatched: ['ar-1'], candidates: 2 }
      expect(await budget.run('run-once', async () => report)).toBe(report)
      expect(budget.expired()).toBe(false)
      expect(budget.signal.aborted).toBe(false)
      expect(() => budget.assertNotExpired('run-once')).not.toThrow()
      // The caller's own failure still surfaces as itself, never re-clothed as
      // a budget expiry — the two have completely different remedies.
      const failure = new Error('dispatch failed')
      await expect(budget.run('run-once', async () => { throw failure })).rejects.toBe(failure)
    } finally {
      budget.dispose()
    }
  })

  it('must-not-fire: with no budget the same hung call stays pending, so the rejections above are the budget', async () => {
    // The control for every must-fire above. Without it a wrapper that
    // rejected unconditionally would pass all of them.
    const budget = startDiscoverySweepBudget(0)
    let settled = false
    try {
      const pending = budget.run('run-once', NEVER).then(
        () => { settled = true },
        () => { settled = true },
      )
      await Promise.race([pending, new Promise((resolve) => setTimeout(resolve, 200))])
      expect(settled).toBe(false)
      expect(budget.expired()).toBe(false)
      expect(budget.budgetMs).toBeUndefined()
    } finally {
      budget.dispose()
    }
  })
})

describe('sweep teardown deadline', () => {
  it('must-fire: abandons a teardown step that never returns, rather than holding the sweep open', async () => {
    expect(await withDeadline(
      withSweepTeardownDeadline(25, NEVER),
      4_000,
      'teardown deadline never fired',
    )).toBeUndefined()
  })

  it('must-not-fire: a served teardown returns its value, and a failing one still throws', async () => {
    expect(await withSweepTeardownDeadline(2_000, async () => true)).toBe(true)
    const failure = new Error('release rejected')
    await expect(withSweepTeardownDeadline(2_000, async () => { throw failure })).rejects.toBe(failure)
    // A teardown deadline is not a licence to swallow failures that used to
    // surface, and it is short enough that it cannot be the thing that wedges.
    expect(DISCOVERY_SWEEP_TEARDOWN_TIMEOUT_MS).toBeLessThan(60_000)
  })
})

/* ------------------------------------------------------------------------- */
/* The same invariant, driven through a real sweep.                          */
/* ------------------------------------------------------------------------- */

const ready = '11111111-1111-4111-8111-111111111111'
const implementing = '22222222-2222-4222-8222-222222222222'
const done = '33333333-3333-4333-8333-333333333333'
const planning = '44444444-4444-4444-8444-444444444444'

const config = (sweepBudgetMs: number, registryRoot: string): FactoryConfig => FactoryConfigSchema.parse({
  workspaceId: 'factory-sweep-budget',
  repos: {
    byLabel: { pear: 'AgentWorkforce/pear' },
    clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
    default: 'AgentWorkforce/pear',
  },
  triage: { maxImplementers: 4 },
  batchSize: 4,
  stateIds: { readyForAgent: ready, agentImplementing: implementing, done, inPlanning: planning },
  verification: { enabled: false },
  loop: {
    registryPath: join(registryRoot, 'registry.json'),
    heartbeatPath: join(registryRoot, 'heartbeat.json'),
  },
  liveSubscription: { sweepBudgetMs },
})

const issuePath = (n: number) => `/linear/issues/AR-${n}__uuid-${n}.json`

const issueFile = (n: number) => ({
  provider: 'linear',
  objectType: 'issue',
  objectId: `uuid-${n}`,
  payload: {
    id: `uuid-${n}`,
    identifier: `AR-${n}`,
    title: `[factory-e2e] Fix factory issue ${n}`,
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

/**
 * A mount whose first watermark read never answers.
 *
 * `#prepareDiscoverySession` makes this call immediately after the discovery
 * lease is claimed, so a sweep that hangs here holds the lease — the position
 * the 0.1.74 wedge occupied, and the position every later layer has reoccupied
 * on a different transport.
 */
class HangingWatermarkMount extends FakeMountClient {
  hang = true
  /**
   * How many watermark reads to serve before hanging.
   *
   * `#startLiveSubscription` reads the watermark once itself, BEFORE the
   * startup backfill and outside the sweep — that read is bounded by the
   * per-call relayfile deadline, not by this budget. Serving it is what puts
   * the hang inside the sweep, which is the subject here.
   */
  serveFirst = 0
  served = 0
  hungCalls = 0

  override async getEventHighWatermark(opts: { provider?: string } = {}): Promise<string | undefined> {
    if (this.hang && this.served >= this.serveFirst) {
      this.hungCalls += 1
      return await NEVER()
    }
    this.served += 1
    return await super.getEventHighWatermark(opts)
  }
}

/** A store that records the lease handbacks, so "released" is observed, not inferred. */
class LeaseWatchingStateStore extends InMemoryStateStore {
  readonly released: number[] = []

  override async releaseDiscoverySweep(workspaceId: string, owner: string, epoch: number): Promise<void> {
    this.released.push(epoch)
    await super.releaseDiscoverySweep(workspaceId, owner, epoch)
  }
}

describe('a wedged sweep is bounded end to end (#372)', () => {
  it('must-fire: aborts at the budget, releases the lease, and the NEXT cycle runs clean', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-budget-'))
    const mount = new HangingWatermarkMount({
      [issuePath(901)]: issueFile(901),
    })
    const fleet = new FakeFleetClient()
    const stateStore = new LeaseWatchingStateStore({ batchSize: 4 })
    const factory = createFactory(config(150, root), {
      mount,
      fleet,
      stateStore,
      triage: new StaticTriage(),
      logger: {},
    })

    try {
      // BEFORE this change this call never settles and the test dies at
      // vitest's 5 s default — the production defect, not an assertion detail.
      // The 4 s guard is inside that default so the failure names itself.
      await expect(withDeadline(factory.runOnce(), 4_000, 'sweep never settled'))
        .rejects.toMatchObject({
          name: 'DiscoverySweepBudgetExceededError',
          // The abandoned await is named, which is the diagnostic no per-call
          // bound can produce once the sweep is already wedged.
          phase: 'discovery-session',
          budgetMs: 150,
        })
      expect(mount.hungCalls).toBe(1)
      // The lease is handed back on the way out. Without this the next cycle
      // has nothing to claim and the wedge simply moves.
      expect(stateStore.released).toHaveLength(1)

      // The next cycle. It must start a NEW sweep rather than coalesce onto
      // the abandoned one — the failure mode #296's deadline left behind.
      mount.hang = false
      const report = await withDeadline(factory.runOnce(), 4_000, 'next cycle never ran')
      expect(report.pulled).toHaveLength(1)
      expect(report.dispatched).toHaveLength(1)
      expect(report.discoveryDeferred).toBeUndefined()
      expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-901-impl-pear', 'ar-901-review'])
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('must-fire: a live daemon whose sweep is wedged still shuts down, because nothing is left in flight', async () => {
    // The counterpart to the #296/#301 abandoned-wait tests, which assert that
    // `stop()` must NOT complete while a sweep abandoned by the readiness
    // deadline is still running. That state is real, and it is exactly what
    // held the deployed daemon: the sweep outlives every wait on it. With the
    // budget it cannot happen — the sweep is aborted, not abandoned — so
    // shutdown has nothing to drain.
    const root = await mkdtemp(join(tmpdir(), 'factory-sweep-budget-stop-'))
    const mount = new HangingWatermarkMount({ [issuePath(901)]: issueFile(901) })
    mount.serveFirst = 1
    const factory = createFactory(config(200, root), {
      mount,
      fleet: new FakeFleetClient(),
      stateStore: new LeaseWatchingStateStore({ batchSize: 4 }),
      triage: new StaticTriage(),
      logger: {},
    })
    try {
      // The startup backfill is a discovery pass like any other and wedges on
      // the same call. Before this change `start()` itself never returns.
      await withDeadline(factory.start({
        mode: 'live',
        liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50, reconcileTimeoutMs: 60_000 },
      }), 4_000, 'start never returned')
      expect(mount.hungCalls).toBeGreaterThan(0)
      await withDeadline(factory.stop(), 4_000, 'stop never completed')
    } finally {
      await factory.stop().catch(() => undefined)
      await rm(root, { recursive: true, force: true })
    }
  })

  it('must-not-fire: a sweep that completes inside its budget is not aborted and its result is unchanged', async () => {
    // Without this the trivial wrong fix — abort every sweep immediately —
    // passes the must-fire above.
    const run = async (sweepBudgetMs: number) => {
      const root = await mkdtemp(join(tmpdir(), 'factory-sweep-budget-ok-'))
      const fleet = new FakeFleetClient()
      const factory = createFactory(config(sweepBudgetMs, root), {
        mount: new FakeMountClient({ [issuePath(901)]: issueFile(901), [issuePath(902)]: issueFile(902) }),
        fleet,
        stateStore: new InMemoryStateStore({ batchSize: 4 }),
        triage: new StaticTriage(),
        logger: {},
      })
      try {
        const report = await withDeadline(factory.runOnce(), 4_000, 'healthy sweep never settled')
        return {
          pulled: report.pulled.map((issue) => issue.key).sort(),
          dispatched: report.dispatched.length,
          skipped: report.skipped.length,
          spawns: fleet.spawns.map((spawn) => spawn.name),
        }
      } finally {
        await factory.stop()
        await rm(root, { recursive: true, force: true })
      }
    }

    // A snug budget, and an effectively unbounded control. Identical results
    // are what "the budget did not change this sweep" means.
    const budgeted = await run(30_000)
    const control = await run(90 * 60_000)
    expect(budgeted.dispatched).toBeGreaterThan(0)
    expect(budgeted).toEqual(control)
  })
})
