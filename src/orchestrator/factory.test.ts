import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema, createFactory, parseLinearIssue, type FactoryConfig, type TriageDecision, type TriageEngine } from '../index'
import type { ChangeEvent } from '../ports'
import { FakeFleetClient, FakeMountClient } from '../testing'
import type { LinearIssue } from '../types'
import { BatchTracker } from './batch-tracker'

const ready = 'b9bec744-b60c-4745-8022-d90d6ab59ae3'
const implementing = '39b9881d-1196-4c95-8b80-a20f0c7263f7'

const config = (overrides: Partial<FactoryConfig> = {}): FactoryConfig => FactoryConfigSchema.parse({
  workspaceId: 'factory-test',
  repos: {
    byLabel: { pear: 'AgentWorkforce/pear' },
    clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
    default: 'AgentWorkforce/pear',
  },
  batchSize: 2,
  ...overrides,
})

const issuePath = (n: number) => `/linear/issues/AR-${n}__uuid-${n}.json`

const issuePayload = (n: number, stateId = ready) => ({
  id: `uuid-${n}`,
  identifier: `AR-${n}`,
  title: `[factory-e2e] Fix factory issue ${n}`,
  description: 'Implement the requested fix in packages/factory-sdk/src/orchestrator/factory.ts and verify it with tests.',
  stateId,
  labels: undefined,
  labelIds: ['label-id-not-used-by-parser'],
  team: { key: 'AR', name: 'Agent Relay' },
  project: { name: 'Factory' },
  state: { id: stateId, name: stateId === ready ? 'Ready for Agent' : 'Implementing' },
})

const issueFile = (n: number, stateId = ready) => ({
  provider: 'linear',
  objectType: 'issue',
  objectId: `uuid-${n}`,
  payload: issuePayload(n, stateId),
})

const flush = async () => {
  await new Promise((resolve) => setTimeout(resolve, 0))
}

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

describe('FactoryLoop', () => {
  it('parses wrapped Linear issue records', () => {
    expect(parseLinearIssue(issuePath(1), issueFile(1))).toMatchObject({
      uuid: 'uuid-1',
      key: 'AR-1',
      title: '[factory-e2e] Fix factory issue 1',
      stateId: ready,
      labels: [],
      project: 'Factory',
    })
  })

  it('parses sparse real Linear issue records without labels or stateId', () => {
    expect(parseLinearIssue(issuePath(5), {
      provider: 'linear',
      objectType: 'issue',
      objectId: 'uuid-5',
      payload: {
        id: 'uuid-5',
        identifier: 'AR-5',
        title: 'Real issue without factory marker',
        description: 'Sparse sync shape',
        state: { id: implementing },
        state_name: 'Implementing',
        labels: undefined,
      },
    })).toMatchObject({
      uuid: 'uuid-5',
      key: 'AR-5',
      stateId: implementing,
      state: { name: 'Implementing' },
      labels: [],
    })
  })

  it('runOnce caps active issues, skips stale state, and pulls queued work after completion', async () => {
    const mount = new FakeMountClient({
      [issuePath(1)]: issueFile(1),
      [issuePath(2)]: issueFile(2),
      [issuePath(3)]: issueFile(3),
      [issuePath(4)]: issueFile(4, implementing),
    })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-1-impl', 'session-impl-1')
    fleet.setSessionRef('ar-1-review', 'session-review-1')
    fleet.setSessionRef('ar-2-impl', 'session-impl-2')
    fleet.setSessionRef('ar-2-review', 'session-review-2')
    fleet.setSessionRef('ar-3-impl', 'session-impl-3')
    fleet.setSessionRef('ar-3-review', 'session-review-3')
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled.map((issue) => issue.key)).toEqual(['AR-1', 'AR-2', 'AR-3', 'AR-4'])
    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-1', 'AR-2'])
    expect(report.skipped).toContainEqual({ issue: { uuid: 'uuid-3', key: 'AR-3', path: issuePath(3) }, reason: 'queued or escalated' })
    expect(report.skipped).toContainEqual({ issue: { uuid: 'uuid-4', key: 'AR-4', path: issuePath(4) }, reason: 'live state is not ready-for-agent' })
    expect(fleet.spawns).toHaveLength(4)
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-1', 'AR-2'])
    expect(factory.status().queued.map((issue) => issue.key)).toEqual(['AR-3'])
    expect(mount.writes.some((write) => write.path === issuePath(1) && (write.content as { stateId?: string }).stateId === implementing)).toBe(true)

    fleet.emitAgentExit('ar-1-impl', 'issue-done')
    await flush()

    expect(fleet.releases.map((release) => release.name)).toEqual(['ar-1-impl', 'ar-1-review'])
    expect(fleet.spawns.map((spawn) => spawn.name)).toContain('ar-3-impl')
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-2', 'AR-3'])
    expect(factory.status().queued).toEqual([])
  })

  it('start backfills ready issues and dispatches when capacity is available', async () => {
    const mount = new FakeMountClient({ [issuePath(11)]: issueFile(11) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-11-impl', 'ar-11-review'])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-11'])
    expect(factory.status().queued).toEqual([])
    await factory.stop()
  })

  it('start queues and emits issue-queued when backfill exceeds batch capacity', async () => {
    const mount = new FakeMountClient({
      [issuePath(15)]: issueFile(15),
      [issuePath(16)]: issueFile(16),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ batchSize: 1 }), { mount, fleet, triage: new StaticTriage() })
    const queued: string[] = []
    factory.on('issue-queued', (payload) => {
      if ('issue' in payload && payload.issue) {
        queued.push(payload.issue.key)
      }
    })

    await factory.start()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-15-impl', 'ar-15-review'])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-15'])
    expect(factory.status().queued.map((issue) => issue.key)).toEqual(['AR-16'])
    expect(queued).toEqual(['AR-16'])
    await factory.stop()
  })

  it('coalesces concurrent starts into one subscription and dispatch pass', async () => {
    const mount = new FakeMountClient({ [issuePath(12)]: issueFile(12) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await Promise.all([factory.start(), factory.start()])

    expect(mount.subscribeCount).toBe(1)
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-12-impl', 'ar-12-review'])
    await factory.stop()
  })

  it('dedupes duplicate subscribe events for an already tracked issue', async () => {
    const mount = new FakeMountClient({ [issuePath(17)]: issueFile(17) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start()
    mount.emit(changeEvent(issuePath(17), 'event-duplicate-1'))
    mount.emit(changeEvent(issuePath(17), 'event-duplicate-2'))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-17-impl', 'ar-17-review'])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-17'])
    await factory.stop()
  })

  it('BatchTracker blocks duplicate invocation ids within and across issue records', async () => {
    const tracker = new BatchTracker(5)
    const decisionA = await new StaticTriage().triage(parseLinearIssue(issuePath(12), issueFile(12)))
    const decisionB = await new StaticTriage().triage(parseLinearIssue(issuePath(13), issueFile(13)))
    const recordA = tracker.start(decisionA, false)
    const recordB = tracker.start(decisionB, false)
    const specA = decisionA.implementers[0]
    const specB = decisionB.implementers[0]
    const invocationId = 'shared-invocation'

    expect(recordA).toBeDefined()
    expect(recordB).toBeDefined()
    expect(tracker.shouldSpawn(recordA!, invocationId)).toBe(true)

    tracker.recordSpawn(recordA!, specA, invocationId, { name: specA.name })

    expect(tracker.shouldSpawn(recordA!, invocationId)).toBe(false)
    expect(tracker.shouldSpawn(recordB!, invocationId)).toBe(false)

    tracker.complete(decisionA.issue)

    expect(tracker.shouldSpawn(recordB!, invocationId)).toBe(true)
    tracker.recordSpawn(recordB!, specB, invocationId, { name: specB.name })
    expect(tracker.shouldSpawn(recordB!, invocationId)).toBe(false)
  })

  it('dedupes repeated dispatch by stable invocation id', async () => {
    const mount = new FakeMountClient({ [issuePath(5)]: issueFile(5) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(5), issueFile(5)))

    await factory.dispatch(decision)
    await factory.dispatch(decision)

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-5-impl', 'ar-5-review'])
    expect(new Set(fleet.spawns.map((spawn) => spawn.invocationId)).size).toBe(2)
  })

  it('dedupes dispatch spawns that retry the same invocation id under different agent names', async () => {
    const mount = new FakeMountClient({ [issuePath(14)]: issueFile(14) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(14), issueFile(14)))
    const sharedInvocationId = 'retry-same-invocation'
    const duplicateDecision: TriageDecision = {
      ...decision,
      implementers: [
        { ...decision.implementers[0], invocationId: sharedInvocationId },
        { ...decision.implementers[0], name: 'ar-14-impl-retry', invocationId: sharedInvocationId },
      ],
      reviewer: { ...decision.reviewer, invocationId: 'reviewer-invocation' },
    }

    await factory.dispatch(duplicateDecision)

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-14-impl', 'ar-14-review'])
    expect(fleet.spawns.map((spawn) => spawn.invocationId)).toEqual([sharedInvocationId, 'reviewer-invocation'])
  })

  it('resumes exited open agents by sessionRef with the original capability', async () => {
    const mount = new FakeMountClient({ [issuePath(6)]: issueFile(6) })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-6-review', 'session-review-6')
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(6), issueFile(6)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-6-review', 'crash')
    await flush()

    expect(fleet.resumes).toEqual([{
      name: 'ar-6-review',
      sessionRef: 'session-review-6',
      node: 'self',
      capability: 'spawn:claude',
    }])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-6-impl', 'ar-6-review'])
  })

  it('coalesces duplicate exit callbacks for the same open issue, agent, and sessionRef', async () => {
    const mount = new FakeMountClient({ [issuePath(10)]: issueFile(10) })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-10-review', 'session-review-10')
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(10), issueFile(10)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-10-review', 'exited')
    fleet.emitAgentExit('ar-10-review', 'crashed')
    await flush()
    fleet.emitAgentExit('ar-10-review', 'code:1')
    await flush()

    expect(fleet.resumes).toEqual([{
      name: 'ar-10-review',
      sessionRef: 'session-review-10',
      node: 'self',
      capability: 'spawn:claude',
    }])
  })

  it('fresh-spawns on exit only when sessionRef is absent', async () => {
    const mount = new FakeMountClient({ [issuePath(7)]: issueFile(7) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(7), issueFile(7)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-7-impl', 'crash')
    await flush()

    expect(fleet.resumes).toEqual([])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-7-impl', 'ar-7-review', 'ar-7-impl'])
    expect(fleet.spawns.at(-1)?.invocationId).toContain(':restart:')
  })

  it('emits an escalation on delivery_failed for an in-flight agent', async () => {
    const mount = new FakeMountClient({ [issuePath(8)]: issueFile(8) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const errors: unknown[] = []
    factory.on('error', (payload) => errors.push(payload))
    await factory.start()

    const decision = await factory.triageIssue(parseLinearIssue(issuePath(8), issueFile(8)))
    await factory.dispatch(decision)
    fleet.emitDeliveryFailed({ to: 'ar-8-review', reason: 'dead-lettered' })
    await flush()

    expect(errors).toHaveLength(1)
    expect(errors[0]).toMatchObject({ issue: { key: 'AR-8' } })
    await factory.stop()
  })

  it('emits error and rejects when writeback verification fails', async () => {
    const mount = new FakeMountClient({ [issuePath(9)]: issueFile(9) })
    mount.setConfirmWrite(issuePath(9), 'failed')
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const errors: unknown[] = []
    factory.on('error', (payload) => errors.push(payload))
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(9), issueFile(9)))

    await expect(factory.dispatch(decision)).rejects.toThrow('Writeback not acked')
    expect(errors).toHaveLength(1)
    expect(errors[0]).toMatchObject({ issue: { key: 'AR-9' } })
  })
})

const changeEvent = (path: string, id: string) => ({
  id,
  workspace: 'factory-test',
  type: 'relayfile.changed',
  occurredAt: new Date(0).toISOString(),
  resource: {
    path,
    kind: 'file',
    id: path,
    provider: 'linear',
  },
  summary: {},
  expand: async () => ({ level: 'summary', path, summary: {} }),
}) as unknown as ChangeEvent
