import { describe, expect, it, vi } from 'vitest'

import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import type { FactoryCloudEventInputV1 } from '../observability/events'
import type { SpawnInput, SpawnResult } from '../ports/fleet'
import type { FactoryEventReporter } from '../ports/observability'
import type { LinearIssue, TriageEngine } from '../types'
import { createHostedFactory, hostedFactoryRunId } from './orchestrator'
import { InMemoryHostedFactoryStateStore } from './state-store'
import type {
  HostedFactoryCompletion,
  HostedFactoryCompletionSource,
  HostedFactoryDiscovery,
  HostedFactoryFleet,
  HostedFactoryIssueRecord,
  HostedFactoryWriteback,
  HostedFactoryWritebackInput,
} from './types'

const workspaceId = 'workspace-hosted'

function config(overrides: { batchSize?: number } = {}): FactoryConfig {
  return FactoryConfigSchema.parse({
    workspaceId,
    repos: {
      byLabel: { factory: 'AgentWorkforce/factory' },
      default: 'AgentWorkforce/factory',
    },
    batchSize: overrides.batchSize ?? 5,
    safety: {
      requireTitlePrefix: '[factory]',
      requireLabel: 'factory',
      requireTeamKey: 'AR',
    },
  })
}

function issue(input: Partial<LinearIssue> = {}): LinearIssue {
  return {
    uuid: 'issue-2778',
    key: 'AR-2778',
    title: '[factory] Run the hosted Factory loop',
    description: [
      'Implement hosted discovery, triage, and dispatch in the Cloud worker.',
      'Persist a cross-host fence and invocation lifecycle in durable state.',
      'Verify completion reconciliation, merge gating, and provider writeback with tests.',
    ].join(' '),
    stateId: 'ready',
    state: { name: 'Ready for Agent' },
    labels: ['factory', 'agent:single'],
    team: 'AR',
    path: '/linear/issues/AR-2778__issue-2778.json',
    raw: {
      title: '[factory] Run the hosted Factory loop',
      team: { key: 'AR' },
      labels: ['factory', 'agent:single'],
    },
    ...input,
  }
}

class Discovery implements HostedFactoryDiscovery {
  issues: LinearIssue[]

  constructor(issues: LinearIssue[]) {
    this.issues = issues
  }

  async discoverReady(): Promise<LinearIssue[]> {
    return structuredClone(this.issues)
  }
}

class Fleet implements HostedFactoryFleet {
  readonly calls: Array<SpawnInput & { invocationId: string }> = []

  async spawn(input: SpawnInput & { invocationId: string }): Promise<SpawnResult & { invocationId: string }> {
    this.calls.push(structuredClone(input))
    return {
      name: input.name,
      invocationId: input.invocationId,
      sessionRef: `session-${input.name}`,
      node: 'cloud-node-1',
      locality: 'remote',
    }
  }
}

class Completions implements HostedFactoryCompletionSource {
  readonly values = new Map<string, HostedFactoryCompletion>()

  async getInvocation(input: { invocationId: string }): Promise<HostedFactoryCompletion | null> {
    return this.values.get(input.invocationId) ?? null
  }
}

class Writeback implements HostedFactoryWriteback {
  readonly clarifications: Array<HostedFactoryWritebackInput & { reason: string }> = []
  readonly dispatches: HostedFactoryWritebackInput[] = []
  readonly completions: HostedFactoryWritebackInput[] = []

  async requestClarification(input: HostedFactoryWritebackInput & { reason: string }): Promise<void> {
    this.clarifications.push(structuredClone(input))
  }

  async dispatched(input: HostedFactoryWritebackInput): Promise<void> {
    this.dispatches.push(structuredClone(input))
  }

  async completed(input: HostedFactoryWritebackInput): Promise<void> {
    this.completions.push(structuredClone(input))
  }
}

class RecordingReporter implements FactoryEventReporter {
  readonly events: FactoryCloudEventInputV1[] = []
  readonly snapshots: Array<{
    event: FactoryCloudEventInputV1
    record: HostedFactoryIssueRecord | null
  }> = []
  readonly #state?: InMemoryHostedFactoryStateStore

  constructor(state?: InMemoryHostedFactoryStateStore) {
    this.#state = state
  }

  async report(event: FactoryCloudEventInputV1): Promise<void> {
    const saved = structuredClone(event)
    this.events.push(saved)
    this.snapshots.push({
      event: saved,
      record: this.#state
        ? await this.#state.getIssue(workspaceId, issue().uuid)
        : null,
    })
  }

  async flush() {
    return {
      delivered: this.events.length,
      pending: 0,
      attempts: this.events.length,
      stoppedReason: 'empty' as const,
    }
  }
}

function harness(input: {
  discovery?: Discovery
  fleet?: HostedFactoryFleet
  completions?: Completions
  writeback?: Writeback
  state?: InMemoryHostedFactoryStateStore
  reporter?: FactoryEventReporter
  ownerId?: string
  triage?: TriageEngine
} = {}) {
  const discovery = input.discovery ?? new Discovery([issue()])
  const fleet = input.fleet ?? new Fleet()
  const completions = input.completions ?? new Completions()
  const writeback = input.writeback ?? new Writeback()
  const state = input.state ?? new InMemoryHostedFactoryStateStore()
  const factory = createHostedFactory({
    workspaceId,
    ownerId: input.ownerId ?? 'host-a',
    config: config(),
  }, {
    discovery,
    fleet,
    completions,
    writeback,
    state,
    triage: input.triage,
    reporter: input.reporter,
    now: () => new Date('2026-07-19T12:00:00.000Z'),
  })
  return { factory, discovery, fleet, completions, writeback, state }
}

describe('HostedFactoryLoop', () => {
  it('discovers, triages, and dispatches a deterministic single recipe', async () => {
    const { factory, fleet, state, writeback } = harness()

    const report = await factory.runOnce()

    expect(report).toMatchObject({
      status: 'completed',
      discovered: 1,
      triaged: ['AR-2778'],
      dispatched: ['AR-2778'],
      errors: [],
    })
    expect(fleet).toBeInstanceOf(Fleet)
    const calls = (fleet as Fleet).calls
    expect(calls.map((call) => call.name)).toEqual(['ar-2778-impl', 'ar-2778-review'])
    expect(calls.every((call) => call.invocationId.startsWith('factory:'))).toBe(true)
    expect(new Set(calls.map((call) => call.invocationId))).toHaveLength(2)

    const stored = await state.getIssue(workspaceId, 'issue-2778')
    expect(stored).toMatchObject({
      phase: 'running',
      decision: { scope: 'single' },
      dispatchWriteback: { status: 'posted', attempts: 1 },
    })
    expect(stored?.invocations.map((invocation) => invocation.status)).toEqual([
      'dispatched',
      'dispatched',
    ])
    expect(writeback.dispatches).toHaveLength(1)
    expect(writeback.dispatches[0]?.idempotencyKey).toBe(
      'factory:workspace-hosted:issue-2778:dispatch',
    )
  })

  it('reports the exact dispatch lifecycle only after each fenced state save', async () => {
    const state = new InMemoryHostedFactoryStateStore()
    const reporter = new RecordingReporter(state)
    const { factory } = harness({ state, reporter })

    await expect(factory.runOnce()).resolves.toMatchObject({
      status: 'completed',
      dispatched: ['AR-2778'],
    })

    expect(reporter.events.map(({ type }) => type)).toEqual([
      'run.started',
      'run.phase_changed',
      'agent.planned',
      'agent.planned',
      'agent.spawned',
      'agent.spawned',
      'run.phase_changed',
      'writeback.applied',
    ])
    expect(new Set(reporter.events.map(({ runId }) => runId))).toEqual(new Set([
      hostedFactoryRunId(workspaceId, 'issue-2778'),
    ]))
    expect(reporter.events[0]?.run).toMatchObject({
      source: 'linear',
      issueKey: 'AR-2778',
    })

    const started = reporter.snapshots.find(({ event }) => event.type === 'run.started')
    expect(started?.record).toMatchObject({
      runId: started?.event.runId,
      phase: 'triaging',
      invocations: [],
    })

    const planned = reporter.snapshots.filter(({ event }) => event.type === 'agent.planned')
    expect(planned).toHaveLength(2)
    expect(planned.every(({ record }) =>
      record?.phase === 'dispatching' &&
      record.invocations.every(({ status }) => status === 'pending')
    )).toBe(true)

    const spawned = reporter.snapshots.filter(({ event }) => event.type === 'agent.spawned')
    expect(spawned).toHaveLength(2)
    expect(spawned.every(({ event, record }) =>
      record?.invocations.find(({ invocationId }) =>
        invocationId === event.attributes?.invocationId
      )?.status === 'dispatched'
    )).toBe(true)

    const phaseChanges = reporter.snapshots.filter(({ event }) => event.type === 'run.phase_changed')
    expect(phaseChanges.map(({ record }) => record?.phase)).toEqual(['dispatching', 'running'])
    expect(reporter.snapshots.at(-1)?.record?.dispatchWriteback).toMatchObject({ status: 'posted' })

    const eventCount = reporter.events.length
    await factory.runOnce()
    expect(reporter.events).toHaveLength(eventCount)
    expect(reporter.events.filter(({ type }) => type === 'run.started')).toHaveLength(1)
    expect(reporter.events.filter(({ type }) => type === 'agent.spawned')).toHaveLength(2)
  })

  it('polls invocation completion, runs the merge gate, and writes back exactly once', async () => {
    const reporter = new RecordingReporter()
    const { factory, completions, state, writeback } = harness({ reporter })
    await factory.runOnce()
    reporter.events.length = 0
    const running = await state.getIssue(workspaceId, 'issue-2778')
    for (const invocation of running?.invocations ?? []) {
      completions.values.set(invocation.invocationId, {
        invocationId: invocation.invocationId,
        status: 'completed',
        output: `${invocation.spec.name} finished`,
      })
    }

    const reconciled = await factory.runOnce()
    const completed = await state.getIssue(workspaceId, 'issue-2778')

    expect(reconciled.reconciled).toEqual(['AR-2778'])
    expect(completed).toMatchObject({
      phase: 'complete',
      mergeGate: { status: 'ready' },
      completionWriteback: { status: 'posted', attempts: 1 },
    })
    expect(writeback.completions).toHaveLength(1)
    expect(reporter.events.map(({ type }) => type)).toEqual([
      'agent.exited',
      'agent.exited',
      'run.phase_changed',
      'writeback.applied',
      'run.succeeded',
    ])
    expect(reporter.events.at(-1)).toMatchObject({
      runId: completed?.runId,
      phase: 'complete',
      status: 'succeeded',
    })
    const terminalInvocationId = completed!.invocations[0]!.invocationId
    await expect(factory.ingestCompletion({
      invocationId: terminalInvocationId,
      status: 'failed',
      error: 'late contradictory delivery',
    })).resolves.toMatchObject({ status: 'terminal', phase: 'complete' })
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('complete')
    expect(reporter.events.filter(({ type }) => type === 'agent.exited')).toHaveLength(2)

    await factory.runOnce()
    expect(writeback.completions).toHaveLength(1)
  })

  it('ingests pushed completion and blocks the merge gate when a spawn fails', async () => {
    const reporter = new RecordingReporter()
    const { factory, state, writeback } = harness({ reporter })
    await factory.runOnce()
    reporter.events.length = 0
    const running = await state.getIssue(workspaceId, 'issue-2778')
    const [implementer, reviewer] = running?.invocations ?? []

    expect(await factory.ingestCompletion({
      invocationId: implementer!.invocationId,
      status: 'completed',
    })).toMatchObject({ status: 'updated', issueKey: 'AR-2778' })
    expect(await factory.ingestCompletion({
      invocationId: reviewer!.invocationId,
      status: 'failed',
      error: 'review failed',
    })).toMatchObject({ status: 'terminal', issueKey: 'AR-2778', phase: 'blocked' })

    expect(await state.getIssue(workspaceId, 'issue-2778')).toMatchObject({
      phase: 'blocked',
      mergeGate: { status: 'blocked' },
    })
    expect(writeback.completions).toHaveLength(1)
    expect(reporter.events.map(({ type }) => type)).toEqual([
      'agent.exited',
      'agent.exited',
      'run.phase_changed',
      'writeback.applied',
      'run.failed',
    ])
    expect(reporter.events.at(-1)).toMatchObject({
      runId: running?.runId,
      phase: 'blocked',
      status: 'failed',
      level: 'error',
    })
    expect(await factory.ingestCompletion({
      invocationId: 'missing-invocation',
      status: 'completed',
    })).toEqual({ status: 'not-found', invocationId: 'missing-invocation' })
  })

  it('re-emits the same invocation after an acknowledgement is lost', async () => {
    class AckLostFleet extends Fleet {
      lost = false

      override async spawn(input: SpawnInput & { invocationId: string }) {
        this.calls.push(structuredClone(input))
        if (!this.lost) {
          this.lost = true
          throw new Error('spawn accepted but acknowledgement was lost')
        }
        return {
          name: input.name,
          invocationId: input.invocationId,
          locality: 'remote' as const,
        }
      }
    }
    const fleet = new AckLostFleet()
    const { factory, state } = harness({ fleet })

    const first = await factory.runOnce()
    expect(first.errors[0]?.message).toContain('acknowledgement was lost')
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('dispatching')

    const second = await factory.runOnce()
    expect(second.dispatched).toEqual(['AR-2778'])
    expect(fleet.calls[0]?.invocationId).toBe(fleet.calls[1]?.invocationId)
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('running')
  })

  it('retains run and invocation identity across a partial-dispatch retry', async () => {
    class PartialAckLostFleet extends Fleet {
      lost = false

      override async spawn(input: SpawnInput & { invocationId: string }) {
        this.calls.push(structuredClone(input))
        if (this.calls.length === 2 && !this.lost) {
          this.lost = true
          throw new Error('second spawn accepted but acknowledgement was lost')
        }
        return {
          name: input.name,
          invocationId: input.invocationId,
          locality: 'remote' as const,
        }
      }
    }
    const reporter = new RecordingReporter()
    const fleet = new PartialAckLostFleet()
    const { factory, state } = harness({ fleet, reporter })

    const first = await factory.runOnce()
    const partial = await state.getIssue(workspaceId, 'issue-2778')
    expect(first.errors[0]?.message).toContain('acknowledgement was lost')
    expect(partial?.invocations.map(({ status }) => status)).toEqual(['dispatched', 'pending'])

    const second = await factory.runOnce()
    const running = await state.getIssue(workspaceId, 'issue-2778')
    expect(second.dispatched).toEqual(['AR-2778'])
    expect(running?.runId).toBe(partial?.runId)
    expect(running?.invocations.map(({ invocationId }) => invocationId)).toEqual(
      partial?.invocations.map(({ invocationId }) => invocationId),
    )
    expect(fleet.calls.map(({ invocationId }) => invocationId)).toEqual([
      running?.invocations[0]?.invocationId,
      running?.invocations[1]?.invocationId,
      running?.invocations[1]?.invocationId,
    ])
    expect(reporter.events.filter(({ type }) => type === 'run.started')).toHaveLength(1)
    expect(reporter.events.filter(({ type }) => type === 'agent.spawned')).toHaveLength(2)
    const failure = reporter.events.find(({ type }) => type === 'factory.failure')
    expect(failure).toMatchObject({
      runId: running?.runId,
      attributes: {
        component: 'orchestrator',
        operation: 'issue_sweep',
        errorClass: 'Error',
      },
    })
    expect(JSON.stringify(failure)).not.toContain('acknowledgement was lost')
  })

  it('retains its deterministic run identity after lease loss and a new host retry', async () => {
    class AckSaveLeaseLostState extends InMemoryHostedFactoryStateStore {
      saves = 0

      override async saveIssue(record: HostedFactoryIssueRecord, lease: Parameters<InMemoryHostedFactoryStateStore['saveIssue']>[1]) {
        this.saves += 1
        if (this.saves === 3) return false
        return await super.saveIssue(record, lease)
      }
    }
    const state = new AckSaveLeaseLostState()
    const reporter = new RecordingReporter()
    const fleet = new Fleet()
    const first = harness({ state, reporter, fleet, ownerId: 'host-a' })

    await expect(first.factory.runOnce()).resolves.toMatchObject({ status: 'lease-lost' })
    const fencedRecord = await state.getIssue(workspaceId, 'issue-2778')
    expect(fencedRecord).toMatchObject({
      runId: hostedFactoryRunId(workspaceId, 'issue-2778'),
      phase: 'dispatching',
    })

    const second = harness({ state, reporter, fleet, ownerId: 'host-b' })
    await expect(second.factory.runOnce()).resolves.toMatchObject({
      status: 'completed',
      dispatched: ['AR-2778'],
    })
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.runId).toBe(fencedRecord?.runId)
    expect(fleet.calls[0]?.invocationId).toBe(fleet.calls[1]?.invocationId)
    expect(reporter.events.filter(({ type }) => type === 'run.started')).toHaveLength(1)
  })

  it('backfills a deterministic run ID before saving a legacy hosted record', async () => {
    const state = new InMemoryHostedFactoryStateStore()
    const seed = await state.claimRunLease(workspaceId, 'seed', 60_000)
    if (!seed.acquired) throw new Error('expected seed lease')
    await state.saveIssue({
      workspaceId,
      issue: issue(),
      phase: 'awaiting-clarification',
      invocations: [],
      clarificationReason: 'legacy row',
      createdAt: '2026-07-18T12:00:00.000Z',
      updatedAt: '2026-07-18T12:00:00.000Z',
    }, seed.lease)
    await state.releaseRunLease(seed.lease)
    const reporter = new RecordingReporter()
    const { factory } = harness({
      state,
      reporter,
      discovery: new Discovery([]),
      ownerId: 'host-after-upgrade',
    })

    await factory.runOnce()

    expect(await state.getIssue(workspaceId, 'issue-2778')).toMatchObject({
      runId: hostedFactoryRunId(workspaceId, 'issue-2778'),
      clarificationWriteback: { status: 'posted' },
    })
    expect(reporter.events).toHaveLength(1)
    expect(reporter.events[0]).toMatchObject({
      type: 'writeback.applied',
      runId: hostedFactoryRunId(workspaceId, 'issue-2778'),
    })
  })

  it('parks thin triage for clarification and dispatches after discovery returns richer context', async () => {
    const thin = issue({ description: 'Please fix it.' })
    const discovery = new Discovery([thin])
    const reporter = new RecordingReporter()
    const { factory, fleet, state, writeback } = harness({ discovery, reporter })

    const first = await factory.runOnce()
    expect(first.awaitingClarification).toEqual(['AR-2778'])
    expect(writeback.clarifications).toHaveLength(1)
    expect((fleet as Fleet).calls).toHaveLength(0)
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('awaiting-clarification')
    expect(reporter.events.map(({ type }) => type)).toEqual([
      'run.started',
      'run.phase_changed',
      'run.waiting',
      'clarification.requested',
      'writeback.applied',
    ])

    discovery.issues = [issue()]
    const resumed = await factory.runOnce()
    expect(resumed.dispatched).toEqual(['AR-2778'])
    expect((fleet as Fleet).calls).toHaveLength(2)
    expect(writeback.clarifications).toHaveLength(1)
  })

  it('isolates a rejecting reporter from dispatch, durable state, and dedupe', async () => {
    const reporter: FactoryEventReporter = {
      report: vi.fn().mockRejectedValue(new Error('telemetry unavailable')),
      flush: vi.fn().mockResolvedValue({
        delivered: 0,
        pending: 1,
        attempts: 1,
        stoppedReason: 'unavailable',
      }),
    }
    const fleet = new Fleet()
    const { factory, state } = harness({ fleet, reporter })

    await expect(factory.runOnce()).resolves.toMatchObject({
      status: 'completed',
      dispatched: ['AR-2778'],
      errors: [],
    })
    expect(await state.getIssue(workspaceId, 'issue-2778')).toMatchObject({
      phase: 'running',
      dispatchWriteback: { status: 'posted' },
    })
    expect(fleet.calls).toHaveLength(2)

    await expect(factory.runOnce()).resolves.toMatchObject({ status: 'completed' })
    expect(fleet.calls).toHaveLength(2)
  })

  it('persists an undispatchable custom triage decision for human clarification', async () => {
    const triage: TriageEngine = {
      triage: async (candidate) => ({
        issue: { uuid: candidate.uuid, key: candidate.key, path: candidate.path },
        routes: [],
        scope: 'workflow',
        implementers: [],
        reviewer: {
          name: 'unused-reviewer',
          role: 'reviewer',
          capability: 'spawn:codex',
          task: 'unused',
          repo: 'AgentWorkforce/factory',
        },
        thin: false,
        confidence: 'high',
        rationale: 'No workflow matched.',
      }),
    }
    const { factory, fleet, state, writeback } = harness({ triage })

    const report = await factory.runOnce()

    expect(report.errors).toEqual([])
    expect(report.awaitingClarification).toEqual(['AR-2778'])
    expect((fleet as Fleet).calls).toEqual([])
    expect(writeback.clarifications).toHaveLength(1)
    expect(writeback.clarifications[0]?.reason).toContain('no dispatchable agent or workflow')
    expect(await state.getIssue(workspaceId, 'issue-2778')).toMatchObject({
      phase: 'awaiting-clarification',
      invocations: [],
      clarificationWriteback: { status: 'posted' },
    })
  })

  it('does not let lease release failures mask run or completion results', async () => {
    class ReleaseFailingState extends InMemoryHostedFactoryStateStore {
      override async releaseRunLease(): Promise<void> {
        throw new Error('storage unavailable during release')
      }
    }
    const runHarness = harness({
      discovery: new Discovery([]),
      state: new ReleaseFailingState(),
    })
    await expect(runHarness.factory.runOnce()).resolves.toMatchObject({
      status: 'completed',
      discovered: 0,
    })

    const completionHarness = harness({ state: new ReleaseFailingState() })
    await expect(completionHarness.factory.ingestCompletion({
      invocationId: 'missing-invocation',
      status: 'completed',
    })).resolves.toEqual({
      status: 'not-found',
      invocationId: 'missing-invocation',
    })
  })

  it('fences a second active control plane for the same workspace', async () => {
    let releaseDiscovery!: () => void
    let markEntered!: () => void
    const entered = new Promise<void>((resolve) => { markEntered = resolve })
    const blocked = new Promise<void>((resolve) => { releaseDiscovery = resolve })
    class BlockingDiscovery extends Discovery {
      override async discoverReady(): Promise<LinearIssue[]> {
        markEntered()
        await blocked
        return []
      }
    }
    const state = new InMemoryHostedFactoryStateStore()
    const shared = {
      discovery: new BlockingDiscovery([]),
      state,
      fleet: new Fleet(),
      completions: new Completions(),
      writeback: new Writeback(),
    }
    const first = createHostedFactory({ workspaceId, ownerId: 'host-a', config: config() }, shared)
    const second = createHostedFactory({ workspaceId, ownerId: 'host-b', config: config() }, shared)

    const firstRun = first.runOnce()
    await entered
    await expect(second.runOnce()).resolves.toMatchObject({ status: 'fenced', discovered: 0 })
    releaseDiscovery()
    await expect(firstRun).resolves.toMatchObject({ status: 'completed' })
  })

  it('continues the sweep when one discovered issue fails', async () => {
    const secondIssue = issue({
      uuid: 'issue-2779',
      key: 'AR-2779',
      title: '[factory] A second hosted issue',
      path: '/linear/issues/AR-2779__issue-2779.json',
    })
    const discovery = new Discovery([issue(), secondIssue])
    const fleet = new Fleet()
    const reporter = new RecordingReporter()
    const spawn = vi.spyOn(fleet, 'spawn')
    spawn.mockRejectedValueOnce(new Error('transient placement failure'))
    const { factory } = harness({ discovery, fleet, reporter })

    const report = await factory.runOnce()

    expect(report.errors).toEqual([
      { issueKey: 'AR-2778', message: 'transient placement failure' },
    ])
    expect(report.dispatched).toEqual(['AR-2779'])
    expect(reporter.events.filter(({ type }) => type === 'factory.failure')).toHaveLength(1)
  })
})
