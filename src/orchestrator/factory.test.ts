import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema, createFactory, parseLinearIssue, type FactoryConfig, type TriageDecision, type TriageEngine } from '../index'
import type { ChangeEvent, LinearWriteback, SlackWriteback } from '../ports'
import { FakeFleetClient, FakeMountClient } from '../testing'
import type { CloseProbePrInput, LinearIssue } from '../index'
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
  url: `https://linear.app/agent-relay/issue/AR-${n}/factory-issue-${n}`,
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

class CountingTriage extends StaticTriage {
  count = 0

  override async triage(issue: LinearIssue): Promise<TriageDecision> {
    this.count += 1
    return super.triage(issue)
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

  it('maps state_name-only Ready for Agent records to the configured ready state id', () => {
    expect(parseLinearIssue(issuePath(26), {
      provider: 'linear',
      objectType: 'issue',
      objectId: 'uuid-26',
      payload: {
        id: 'uuid-26',
        identifier: 'AR-26',
        title: '[factory-e2e] State name only',
        description: 'Factory-created issue synced without stateId',
        url: 'https://linear.app/agent-relay/issue/AR-26/state-name-only',
        state_name: 'Ready for Agent',
        labels: undefined,
        team: { key: 'AR', name: 'Agent Relay' },
      },
    })).toMatchObject({
      uuid: 'uuid-26',
      key: 'AR-26',
      stateId: ready,
      state: { name: 'Ready for Agent' },
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

  it('skips ready issues outside factory-e2e scope before triage or dispatch', async () => {
    const unscopedPath = issuePath(21)
    const mount = new FakeMountClient({
      [unscopedPath]: {
        ...issueFile(21),
        payload: {
          ...issuePayload(21),
          title: 'Real ready AR issue without synthetic marker',
          team: { key: 'AR', name: 'Agent Relay' },
        },
      },
    })
    const fleet = new FakeFleetClient()
    const triage = new CountingTriage()
    const factory = createFactory(config(), { mount, fleet, triage })

    const report = await factory.runOnce()

    expect(report.skipped).toContainEqual({
      issue: { uuid: 'uuid-21', key: 'AR-21', path: unscopedPath },
      reason: 'not factory-e2e scope',
    })
    expect(report.triaged).toEqual([])
    expect(report.dispatched).toEqual([])
    expect(triage.count).toBe(0)
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
  })

  it('skips factory-marked draft issues that are not reconciled provider records', async () => {
    const draftPath = '/linear/issues/AR-E2ECANARY.json'
    const mount = new FakeMountClient({
      [draftPath]: {
        provider: 'linear',
        objectType: 'issue',
        payload: {
          id: 'draft-id',
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] Draft should not dispatch',
          description: 'Synthetic draft',
          stateId: ready,
          team: { key: 'AR', name: 'Agent Relay' },
        },
      },
    })
    const fleet = new FakeFleetClient()
    const triage = new CountingTriage()
    const factory = createFactory(config(), { mount, fleet, triage })

    const report = await factory.runOnce()

    expect(report.skipped).toContainEqual({
      issue: { uuid: 'draft-id', key: 'AR-E2ECANARY', path: draftPath },
      reason: 'not reconciled real Linear issue',
    })
    expect(triage.count).toBe(0)
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
  })

  it('refuses explicit dispatch for factory-marked issues without a provider URL', async () => {
    const draft = {
      ...issueFile(24),
      payload: {
        ...issuePayload(24),
        url: undefined,
      },
    }
    const mount = new FakeMountClient({ [issuePath(24)]: draft })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(24), draft))

    await expect(factory.dispatch(decision)).rejects.toThrow(/not reconciled real Linear issue/)
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
  })

  it('dispatches factory-scoped real issues that only carry state_name for readiness', async () => {
    const path = issuePath(27)
    const stateNameOnly = {
      provider: 'linear',
      objectType: 'issue',
      objectId: 'uuid-27',
      payload: {
        ...issuePayload(27),
        stateId: undefined,
        state: undefined,
        state_name: 'Ready for Agent',
      },
    }
    const mount = new FakeMountClient({ [path]: stateNameOnly })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-27'])
    expect(report.skipped).toEqual([])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-27-impl', 'ar-27-review'])
  })

  it('refuses explicit dispatch for issues outside factory-e2e scope before spawning', async () => {
    const unscopedIssue = {
      ...issueFile(22),
      payload: {
        ...issuePayload(22),
        title: 'Real targeted AR issue without synthetic marker',
        team: { key: 'AR', name: 'Agent Relay' },
      },
    }
    const mount = new FakeMountClient({ [issuePath(22)]: unscopedIssue })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(22), unscopedIssue))

    await expect(factory.dispatch(decision)).rejects.toThrow(/not factory-e2e scope/)
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
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

  it('logs and continues when best-effort dispatch comment writeback fails', async () => {
    const mount = new FakeMountClient({ [issuePath(25)]: issueFile(25) })
    const fleet = new FakeFleetClient()
    const warnings: unknown[] = []
    const linear: LinearWriteback = {
      async postComment() {
        throw new Error('unsupported Linear writeback path')
      },
      async setState(issue, stateId) {
        await mount.writeFile(issue.path, { stateId })
      },
      async createIssue() {
        throw new Error('not used')
      },
      async verify() {
        return true
      },
    }
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear,
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => {},
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(25), issueFile(25)))

    await expect(factory.dispatch(decision)).resolves.toMatchObject({
      issue: { key: 'AR-25' },
      stateId: implementing,
    })
    expect(warnings[0]).toEqual([
      '[factory] comment writeback skipped',
      expect.objectContaining({ message: 'unsupported Linear writeback path' }),
    ])
    expect(mount.writes).toContainEqual({ path: issuePath(25), content: { stateId: implementing } })
  })

  it('closes a synthetic probe PR after done writebacks and before release when mergePolicy is never', async () => {
    const order: string[] = []
    const mount = new FakeMountClient({
      [issuePath(18)]: issueFile(18),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/18.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '18',
        payload: {
          number: 18,
          title: '[factory-e2e] AR-18 probe',
          body: 'Synthetic probe for AR-18',
          head_ref: 'factory-e2e/ar-18-probe',
        },
      },
    })
    class OrderedFleetClient extends FakeFleetClient {
      override async release(name: string, reason?: string): Promise<void> {
        order.push(`release:${name}`)
        await super.release(name, reason)
      }
    }
    const fleet = new OrderedFleetClient()
    const linear: LinearWriteback = {
      async setState() {
        order.push('linear-done')
      },
      async postComment() {
        order.push('linear-comment')
      },
      async createIssue() {
        throw new Error('not used')
      },
      async verify() {
        return true
      },
    }
    const slack: SlackWriteback = {
      async postThread() {
        order.push('slack-root')
        return { threadId: 'thread-1' }
      },
      async reply() {
        order.push('slack-reply')
      },
    }
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey'>> = []
    const factory = createFactory(config({ slack: { channel: 'C0FACTORY', style: 'threaded-summarized' } }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear,
      slack,
      probeCloser: async (input) => {
        order.push('probe-close')
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(18), issueFile(18)))

    await factory.dispatch(decision)
    order.length = 0
    fleet.emitAgentExit('ar-18-impl', 'issue-done')
    await flush()

    expect(closeInputs).toEqual([{ repo: 'AgentWorkforce/pear', prNumber: 18, expectedIssueKey: 'AR-18' }])
    expect(order).toEqual([
      'linear-done',
      'slack-root',
      'slack-reply',
      'probe-close',
      'release:ar-18-impl',
      'release:ar-18-review',
    ])
  })

  it('does not close probes for non-never merge policies', async () => {
    const markedMount = new FakeMountClient({ [issuePath(19)]: issueFile(19) })
    const markedFleet = new FakeFleetClient()
    const markedCalls: unknown[] = []
    const markedFactory = createFactory(config({ mergePolicy: 'on-green-with-review' }), {
      mount: markedMount,
      fleet: markedFleet,
      triage: new StaticTriage(),
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 19 }),
      probeCloser: async (input) => {
        markedCalls.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })
    await markedFactory.dispatch(await markedFactory.triageIssue(parseLinearIssue(issuePath(19), issueFile(19))))
    markedFleet.emitAgentExit('ar-19-impl', 'issue-done')
    await flush()
    expect(markedCalls).toEqual([])
  })

  it('does not resolve a probe PR when the factory-e2e marker is only in body or branch', async () => {
    const mount = new FakeMountClient({
      [issuePath(23)]: issueFile(23),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/23.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '23',
        payload: {
          number: 23,
          title: 'AR-23 probe without title marker',
          body: '[factory-e2e] Synthetic probe for AR-23',
          head_ref: 'factory-e2e/ar-23-probe',
        },
      },
    })
    const fleet = new FakeFleetClient()
    const closeInputs: unknown[] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(23), issueFile(23)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-23-impl', 'issue-done')
    await flush()

    expect(closeInputs).toEqual([])
    expect(fleet.releases.map((release) => release.name)).toEqual(['ar-23-impl', 'ar-23-review'])
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
