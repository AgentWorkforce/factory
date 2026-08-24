import { describe, expect, it } from 'vitest'
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
import { RelaySpawnAckTimeoutError } from '../fleet/relay-fleet-client'
import { normalizePublicHealth } from './public-health'
import type { FactoryPublicReadinessReconcileHealth, FactoryReadinessReconcileStatus } from '../types'
import type { SpawnInput, SpawnResult } from '../ports/fleet'

/**
 * Why a dispatch attempt failed, end to end (#355).
 *
 * The live container publishes `skipReasons: { 'dispatch-failed': 5 }` on every
 * sweep, with the control-plane breaker closed, the fleet agent online and
 * `readinessReconcile` healthy. Five eligible issues are selected, dispatch is
 * attempted, and it fails — and the bucket is a count, so it does not say what
 * failed. The daemon knows; it writes the reason to stdout, which does not
 * reach the deployed container's operator.
 *
 * Every assertion below drives the real writer: a live daemon running a real
 * sweep whose real dispatch really throws. Hand-setting `failureCode` on a
 * report fixture would prove the projection copies a field it was handed, and
 * prove nothing about the sweep that has to produce it.
 */

const ready = '11111111-1111-4111-8111-111111111111'
const implementing = '22222222-2222-4222-8222-222222222222'
const done = '33333333-3333-4333-8333-333333333333'
const planning = '44444444-4444-4444-8444-444444444444'

const config = (overrides: Record<string, unknown> = {}): FactoryConfig => FactoryConfigSchema.parse({
  workspaceId: 'factory-dispatch-failure-reasons',
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
 * Triage that throws, so the failure happens before the fleet is ever asked.
 *
 * Also the one dispatch-attempt path that does NOT wrap what it throws, which
 * makes it the only way to drive an unwrapped error all the way to the skip
 * site — see the shed test below.
 */
class ThrowingTriage implements TriageEngine {
  constructor(private readonly error?: Error) {}

  async triage(issue: LinearIssue): Promise<TriageDecision> {
    throw this.error ??
      new Error(`triage backend refused ${issue.key} at /linear/issues/${issue.key}.json`)
  }
}

/**
 * A fleet whose spawn throws for issues this test names.
 *
 * The failure is injected at the spawn, not at the skip site, precisely because
 * the classification has to survive the trip: `#dispatchUnlocked` and
 * `contextualError` both rethrow wrapped, so a classifier reading only the
 * outermost error would see a plain `Error` and lose every named cause.
 */
class SpawnFailingFleetClient extends FakeFleetClient {
  constructor(private readonly failure: (input: SpawnInput) => Error | undefined) {
    super()
  }

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    const error = this.failure(input)
    if (error) throw error
    return super.spawn(input)
  }
}

async function sweepReadiness(
  files: Record<string, unknown>,
  deps: { fleet?: FakeFleetClient; triage?: TriageEngine } = {},
): Promise<FactoryReadinessReconcileStatus> {
  const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-reasons-'))
  const factory = createFactory(
    config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
    {
      mount: new FakeMountClient(files),
      fleet: deps.fleet ?? new FakeFleetClient(),
      triage: deps.triage ?? new StaticTriage(),
      logger: {},
    },
  )
  try {
    await factory.start({
      mode: 'live',
      // The startup backfill is the sweep under test and settles before
      // `start()` resolves; a periodic pass landing mid-read would make the
      // numbers describe a sweep this test never set up.
      liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 600_000 },
    })
    const status = factory.status().readinessReconcile
    if (!status) throw new Error('live daemon published no readinessReconcile status')
    return status
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

/** MUST-FIRE: dispatch was attempted, it failed, and the surface names why. */
const expectNamedDispatchFailures = (readiness: FactoryPublicReadinessReconcileHealth): void => {
  expect(readiness.dispatchFailures).toBeGreaterThan(0)
  expect(readiness.dispatchFailureReasons).toBeDefined()
  expect(Object.values(readiness.dispatchFailureReasons ?? {}).reduce((sum, n) => sum + n, 0))
    .toBe(readiness.dispatchFailures)
}

/** MUST-NOT-FIRE: dispatch was attempted and nothing failed — a real zero. */
const expectNoDispatchFailures = (readiness: FactoryPublicReadinessReconcileHealth): void => {
  expect(readiness.dispatchFailures).toBe(0)
  expect(Object.hasOwn(readiness, 'dispatchFailures')).toBe(true)
  expect(Object.hasOwn(readiness, 'dispatchFailureReasons')).toBe(false)
}

describe('dispatch failure reasons (#355)', () => {
  it('names the cause of a failed dispatch, and the parts sum to the bucket that counted it', async () => {
    const fleet = new SpawnFailingFleetClient((input) =>
      input.name.startsWith('ar-951') || input.name.startsWith('ar-952')
        ? new RelaySpawnAckTimeoutError('spawn', 60_000)
        : undefined)

    const status = await sweepReadiness({
      [issuePath(951)]: issueFile(951),
      [issuePath(952)]: issueFile(952),
      [issuePath(953)]: issueFile(953),
    }, { fleet })

    expect(status).toMatchObject({
      candidates: 3,
      dispatched: 1,
      skipped: 2,
      skipReasons: { 'dispatch-failed': 2 },
      dispatchFailures: 2,
      dispatchFailureReasons: { 'spawn-ack-timeout': 2 },
    })

    const readiness = published(status)
    expectNamedDispatchFailures(readiness)
    expect(readiness.dispatchFailureReasons).toEqual({ 'spawn-ack-timeout': 2 })
    // The breakdown refines exactly the bucket it sits under. A reader holding
    // both numbers can check that, and it is the only integrity check this
    // surface offers.
    expect(readiness.dispatchFailures).toBe(readiness.skipReasons?.['dispatch-failed'])
  })

  it('CONTROL: a zero is published as a zero, so "none failed" never reads as "cannot say"', async () => {
    // The trap this test exists for. `skipReasons` omits zero counts, so on
    // that field alone a sweep in which every dispatch succeeded is the same
    // absence as a producer that has never heard of dispatch failures — and
    // 0.1.72 is in production being exactly the second thing. If
    // `dispatchFailures` were built with a helper that coerced absent to zero,
    // or that dropped a zero as uninteresting, this pair would collapse.
    const clean = published(await sweepReadiness({
      [issuePath(961)]: issueFile(961),
      [issuePath(962)]: issueFile(962),
    }))
    expect(clean).toMatchObject({ candidates: 2, dispatched: 2, skipped: 0 })
    expectNoDispatchFailures(clean)
    // `skipReasons` cannot answer the question, which is why the total exists.
    expect(clean.skipReasons).toBeUndefined()

    const failing = published(await sweepReadiness(
      { [issuePath(963)]: issueFile(963) },
      {
        fleet: new SpawnFailingFleetClient(() => new RelaySpawnAckTimeoutError('spawn', 60_000)),
      },
    ))

    // Neither expectation can stand in for the other: swap them and the suite
    // fails, so a pass here is evidence about the sweep rather than about a
    // constant.
    expect(() => expectNoDispatchFailures(failing)).toThrow()
    expect(() => expectNamedDispatchFailures(clean)).toThrow()
    expect(() => expectNamedDispatchFailures(failing)).not.toThrow()
    expect(() => expectNoDispatchFailures(clean)).not.toThrow()

    // And the zero survives the wire, which is where a coercion would show up.
    const roundTripped = normalizePublicHealth(JSON.parse(JSON.stringify({
      schemaVersion: 1,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: clean,
    })))
    expect(roundTripped?.readinessReconcile?.dispatchFailures).toBe(0)
    expect(Object.hasOwn(roundTripped?.readinessReconcile ?? {}, 'dispatchFailures')).toBe(true)
  })

  it('leaves the field absent until a sweep completes, so "never attempted" stays its own reading', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-reasons-'))
    const factory = createFactory(
      config({ loop: { registryPath: join(root, 'registry.json'), heartbeatPath: join(root, 'heartbeat.json') } }),
      { mount: new FakeMountClient(), fleet: new FakeFleetClient(), triage: new StaticTriage(), logger: {} },
    )
    try {
      const status = factory.status().readinessReconcile
      expect(status?.state).toBe('not-running')
      expect(status && Object.hasOwn(status, 'dispatchFailures')).toBe(false)
      // Three readings, three shapes: absent here, `0` for a clean sweep, a
      // positive integer with a breakdown for a failing one.
      expect(status && Object.hasOwn(published(status), 'dispatchFailures')).toBe(false)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('separates a failure in triage from one in dispatch, because they are different owners', async () => {
    // Nothing named matches either of these, and that is the case the phase
    // codes are for: a unit that never reached the fleet is not a fleet bug,
    // and on a surface carrying no messages the phase is the only thing left
    // that still says who should look.
    const triageFailed = await sweepReadiness(
      { [issuePath(971)]: issueFile(971) },
      { triage: new ThrowingTriage() },
    )
    expect(triageFailed).toMatchObject({
      candidates: 1,
      dispatched: 0,
      dispatchFailures: 1,
      dispatchFailureReasons: { 'unclassified-triage': 1 },
    })

    const dispatchFailed = await sweepReadiness(
      { [issuePath(972)]: issueFile(972) },
      { fleet: new SpawnFailingFleetClient(() => new Error('broker refused the spawn')) },
    )
    expect(dispatchFailed).toMatchObject({
      candidates: 1,
      dispatched: 0,
      dispatchFailures: 1,
      dispatchFailureReasons: { 'unclassified-dispatch': 1 },
    })
  })

  it('classifies through the wrapper, so a rethrown cause does not lose its name', async () => {
    // `#dispatchUnlocked` and `contextualError` both rethrow wrapped. A
    // classifier that read only the outermost error would see a plain `Error`
    // here and report `unclassified-dispatch` for a failure that names itself.
    const wrapped = new Error('dispatch failed for AR-981')
    ;(wrapped as Error & { cause?: unknown }).cause = new RelaySpawnAckTimeoutError('spawn', 60_000)

    const status = await sweepReadiness(
      { [issuePath(981)]: issueFile(981) },
      { fleet: new SpawnFailingFleetClient(() => wrapped) },
    )
    expect(status?.dispatchFailureReasons).toEqual({ 'spawn-ack-timeout': 1 })
  })

  // #361 review (P2, codex): overload classification must follow the same
  // bounded cause chain as every named error. These two pin the direct and
  // wrapped paths to the loop's single shedding predicate.
  it('reports an unwrapped relayfile shed as a shed', async () => {
    const shed = Object.assign(new Error('workspace durable object is busy'), {
      status: 429,
      reason: 'inflight_limit',
    })
    const status = await sweepReadiness(
      { [issuePath(1001)]: issueFile(1001) },
      { triage: new ThrowingTriage(shed) },
    )
    expect(status).toMatchObject({
      dispatchFailures: 1,
      dispatchFailureReasons: { 'relayfile-overloaded': 1 },
    })
  })

  it('treats a relayfile shed wrapped by spawn context as the same shed', async () => {
    // `#spawnAgent` wraps this provider failure before the skip site. The
    // source overload predicate must still drive the loop's counter/fuse path
    // and the published reason; relabelling only the surface would lie.
    const wrapped = Object.assign(new Error('workspace durable object is busy'), {
      status: 429,
      reason: 'inflight_limit',
    })
    const status = await sweepReadiness(
      { [issuePath(1002)]: issueFile(1002) },
      { fleet: new SpawnFailingFleetClient(() => wrapped) },
    )
    expect(status?.dispatchFailureReasons).toEqual({ 'relayfile-overloaded': 1 })
    expect(status?.dispatchFailures).toBe(1)
  })

  it('publishes counts only: no issue key, path, title or error message crosses', async () => {
    const status = await sweepReadiness(
      {
        [issuePath(991)]: issueFile(991),
        [issuePath(992)]: issueFile(992),
      },
      {
        fleet: new SpawnFailingFleetClient((input) =>
          input.name.startsWith('ar-991')
            ? new Error('spawn failed for AR-991 at /linear/issues/AR-991__uuid-991.json (relay token expired)')
            : undefined),
      },
    )

    const serialized = JSON.stringify(published(status))
    expect(serialized).not.toContain('AR-991')
    expect(serialized).not.toContain('/linear/issues')
    expect(serialized).not.toContain('relay token')
    expect(serialized).not.toContain('factory-e2e')
    expect(serialized).not.toContain('AgentWorkforce/pear')

    const record = JSON.parse(serialized) as Record<string, unknown>
    expect(record.dispatchFailures).toBe(1)
    for (const count of Object.values(record.dispatchFailureReasons as Record<string, unknown>)) {
      expect(typeof count).toBe('number')
    }
  })
})
