import { describe, expect, it, vi } from 'vitest'

import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import type { SpawnInput, SpawnResult } from '../ports/fleet'
import type { LinearIssue, TriageEngine } from '../types'
import { createHostedFactory } from './orchestrator'
import { InMemoryHostedFactoryStateStore } from './state-store'
import type {
  HostedFactoryCompletion,
  HostedFactoryCompletionSource,
  HostedFactoryDiscovery,
  HostedFactoryFleet,
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

function harness(input: {
  discovery?: Discovery
  fleet?: HostedFactoryFleet
  completions?: Completions
  writeback?: Writeback
  state?: InMemoryHostedFactoryStateStore
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

  it('polls invocation completion, runs the merge gate, and writes back exactly once', async () => {
    const { factory, completions, state, writeback } = harness()
    await factory.runOnce()
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
    const terminalInvocationId = completed!.invocations[0]!.invocationId
    await expect(factory.ingestCompletion({
      invocationId: terminalInvocationId,
      status: 'failed',
      error: 'late contradictory delivery',
    })).resolves.toMatchObject({ status: 'terminal', phase: 'complete' })
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('complete')

    await factory.runOnce()
    expect(writeback.completions).toHaveLength(1)
  })

  it('ingests pushed completion and blocks the merge gate when a spawn fails', async () => {
    const { factory, state, writeback } = harness()
    await factory.runOnce()
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

  it('parks thin triage for clarification and dispatches after discovery returns richer context', async () => {
    const thin = issue({ description: 'Please fix it.' })
    const discovery = new Discovery([thin])
    const { factory, fleet, state, writeback } = harness({ discovery })

    const first = await factory.runOnce()
    expect(first.awaitingClarification).toEqual(['AR-2778'])
    expect(writeback.clarifications).toHaveLength(1)
    expect((fleet as Fleet).calls).toHaveLength(0)
    expect((await state.getIssue(workspaceId, 'issue-2778'))?.phase).toBe('awaiting-clarification')

    discovery.issues = [issue()]
    const resumed = await factory.runOnce()
    expect(resumed.dispatched).toEqual(['AR-2778'])
    expect((fleet as Fleet).calls).toHaveLength(2)
    expect(writeback.clarifications).toHaveLength(1)
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
    const spawn = vi.spyOn(fleet, 'spawn')
    spawn.mockRejectedValueOnce(new Error('transient placement failure'))
    const { factory } = harness({ discovery, fleet })

    const report = await factory.runOnce()

    expect(report.errors).toEqual([
      { issueKey: 'AR-2778', message: 'transient placement failure' },
    ])
    expect(report.dispatched).toEqual(['AR-2779'])
  })
})
