import { describe, expect, it, vi } from 'vitest'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  FactoryConfigSchema,
  checkFactoryLoopLiveness,
  createFactory,
  parseLinearIssue,
  readFactoryInFlightRegistry,
  readFactoryLoopHeartbeat,
  type FactoryConfig,
  type TriageDecision,
  type TriageEngine,
} from '../index'
import type { ChangeEvent, EventPage, LinearWriteback, ProviderSyncStatus, SlackWriteback, SpawnInput, SpawnResult } from '../ports'
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

const realIssueFile = (n: number, stateId = ready, overrides: Record<string, unknown> = {}) => ({
  ...issueFile(n, stateId),
  payload: {
    ...issuePayload(n, stateId),
    url: `https://linear.app/agent-relay/issue/AR-${n}/factory-issue-${n}`,
    ...overrides,
  },
})

const slackConfig = (channel = 'C0FACTORY__factory-e2e') => ({
  channel,
  style: 'threaded-summarized' as const,
  botUserId: 'U0B2596R7EZ',
  staleAfterMs: 10 * 60_000,
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

class SpawnFailingFleetClient extends FakeFleetClient {
  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    throw new Error('spawnPty failed: cwd does not exist')
  }
}

class ManualClock {
  value = 0

  now(): number {
    return this.value
  }

  advance(ms: number): void {
    this.value += ms
  }

  async sleep(_ms: number): Promise<void> {
    return
  }
}

class TimestampFailingFleetClient extends FakeFleetClient {
  readonly attemptTimes: number[] = []

  constructor(readonly clock: ManualClock) {
    super()
  }

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    this.attemptTimes.push(this.clock.now())
    throw new Error('spawnPty failed: cwd does not exist')
  }
}

class ReleaseFailingFleetClient extends FakeFleetClient {
  readonly releaseAttempts: Array<{ name: string; reason?: string }> = []

  constructor(readonly failNames = new Set<string>()) {
    super()
  }

  override async release(name: string, reason?: string): Promise<void> {
    this.releaseAttempts.push({ name, reason })
    if (this.failNames.has(name)) {
      throw new Error(`release failed for ${name}`)
    }
    await super.release(name, reason)
  }
}

class PidFleetClient extends FakeFleetClient {
  nextPid = 9_000

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    const pid = this.nextPid++
    return { name: input.name, sessionRef: `session-${pid}`, pid }
  }
}

class CountingEventsMount extends FakeMountClient {
  getEventsCalls = 0

  override async getEvents(opts: { cursor?: string; limit?: number }): Promise<EventPage> {
    this.getEventsCalls += 1
    return super.getEvents(opts)
  }
}

class TrackingEventsMount extends CountingEventsMount {
  activeSubscriptions = 0
  unsubscribeCount = 0

  override subscribe(...args: Parameters<FakeMountClient['subscribe']>): ReturnType<FakeMountClient['subscribe']> {
    this.activeSubscriptions += 1
    const subscription = super.subscribe(...args)
    return {
      unsubscribe: async () => {
        this.unsubscribeCount += 1
        this.activeSubscriptions -= 1
        await subscription.unsubscribe()
      },
    }
  }
}

class SlackSyncStatusMount extends FakeMountClient {
  slackStatus: ProviderSyncStatus | undefined

  async getSyncStatus(provider: string): Promise<ProviderSyncStatus | undefined> {
    return provider === 'slack' ? this.slackStatus : undefined
  }
}

class ThrowingSlackSyncStatusMount extends FakeMountClient {
  async getSyncStatus(provider: string): Promise<ProviderSyncStatus | undefined> {
    if (provider === 'slack') {
      throw new Error('sync status unavailable')
    }
    return undefined
  }
}

class NoWatermarkMount extends FakeMountClient {
  override async getEventHighWatermark(): Promise<string | undefined> {
    return undefined
  }
}

class ThrowingWatermarkMount extends FakeMountClient {
  override async getEventHighWatermark(): Promise<string | undefined> {
    throw Object.assign(new Error('Route not found'), { status: 404 })
  }
}

class RecordingSlack implements SlackWriteback {
  readonly roots: Array<{ channel: string; text: string }> = []
  readonly replies: Array<{ threadId: string; text: string }> = []
  threadId = '1780751612.176219'
  failPostThread = false
  failReplies = 0

  async postThread(root: { channel: string; text: string }): Promise<{ threadId: string }> {
    if (this.failPostThread) {
      throw new Error('slack post failed')
    }

    this.roots.push(root)
    return { threadId: this.threadId }
  }

  async reply(threadId: string, text: string): Promise<void> {
    if (this.failReplies > 0) {
      this.failReplies -= 1
      throw new Error('slack reply failed')
    }

    this.replies.push({ threadId, text })
  }
}

class CloudWritebackFakeMountClient extends FakeMountClient {
  constructor(initialFiles: Record<string, unknown> = {}, readonly threadTs = '1780751612.176219') {
    super(initialFiles)
  }

  override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
    await super.writeFile(path, content, opts)
    if (isSlackRootWritePath(path)) {
      this.files.set(path, {
        content: {
          provider: 'slack',
          objectType: 'message',
          payload: {
            ...record(content),
            ts: this.threadTs,
            thread_ts: this.threadTs,
          },
        },
      })
    }
  }
}

class ConfirmRecordingSlackMountClient extends CloudWritebackFakeMountClient {
  readonly confirmedPaths: string[] = []

  override async confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    this.confirmedPaths.push(path)
    return super.confirmWrite(path, opts)
  }
}

class FailFirstSlackReplyMountClient extends CloudWritebackFakeMountClient {
  failedReply = false

  override async confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    if (!this.failedReply && path.includes('/replies/')) {
      this.failedReply = true
      return 'failed'
    }
    return super.confirmWrite(path, opts)
  }
}

class FailSlackRootMountClient extends CloudWritebackFakeMountClient {
  slackStatus: ProviderSyncStatus | undefined

  async getSyncStatus(provider: string): Promise<ProviderSyncStatus | undefined> {
    return provider === 'slack' ? this.slackStatus : undefined
  }

  override async confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    if (isSlackRootWritePath(path)) {
      return 'failed'
    }
    return super.confirmWrite(path, opts)
  }
}

class RecoveringSlackRootMountClient extends CloudWritebackFakeMountClient {
  slackStatus: ProviderSyncStatus | undefined
  failedRootsRemaining = 1

  async getSyncStatus(provider: string): Promise<ProviderSyncStatus | undefined> {
    return provider === 'slack' ? this.slackStatus : undefined
  }

  override async confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    if (isSlackRootWritePath(path) && this.failedRootsRemaining > 0) {
      this.failedRootsRemaining -= 1
      return 'failed'
    }
    return super.confirmWrite(path, opts)
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

  it('runLoop stops at the configured iteration cap, preserves the batch cap, and advances heartbeat liveness', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-loop-heartbeat-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient(Object.fromEntries(
        [51, 52, 53, 54, 55, 56].map((n) => [issuePath(n), issueFile(n)]),
      ))
      const fleet = new FakeFleetClient()
      const factory = createFactory(config({
        batchSize: 5,
        loop: { maxIterations: 3, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), { mount, fleet, triage: new StaticTriage() })

      const reports = await factory.runLoop({ dryRun: true })

      expect(reports).toHaveLength(3)
      expect(factory.status().counters.loopIdle).toBe(1)
      expect(factory.status().inFlight).toHaveLength(5)
      expect(factory.status().queued).toHaveLength(1)
      const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(heartbeat).toMatchObject({ status: 'idle', iteration: 3, maxIterations: 3, pid: process.pid })
      expect(checkFactoryLoopLiveness(heartbeat, { nowMs: heartbeat!.updatedAtMs + 500, staleMs: 10_000 })).toMatchObject({
        ok: true,
        stale: false,
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('writes a durable in-flight registry with agent PID identity signatures', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-loop-registry-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(62)]: issueFile(62) })
      const fleet = new PidFleetClient()
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        processIdentityReader: async (pid) => ({
          pid,
          startTime: `start-${pid}`,
          cmdline: pid === 9_000 ? 'node ar-62-impl worker' : 'node ar-62-review worker',
        }),
      })

      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(62), issueFile(62))))

      const registry = await readFactoryInFlightRegistry(registryPath)
      expect(registry).toMatchObject({
        pid: process.pid,
        heartbeatPath,
        agents: [
          {
            name: 'ar-62-impl',
            pids: [9_000],
            processes: [{ pid: 9_000, agentName: 'ar-62-impl', startTime: 'start-9000', cmdline: 'node ar-62-impl worker' }],
          },
          {
            name: 'ar-62-review',
            pids: [9_001],
            processes: [{ pid: 9_001, agentName: 'ar-62-review', startTime: 'start-9001', cmdline: 'node ar-62-review worker' }],
          },
        ],
      })
      await factory.stop()
      expect((await readFactoryInFlightRegistry(registryPath))?.agents).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reports missing or stale loop heartbeat as not live', () => {
    expect(checkFactoryLoopLiveness(undefined, { nowMs: 2_000, staleMs: 1_000 })).toMatchObject({
      ok: false,
      stale: true,
      reason: 'heartbeat missing',
    })
    expect(checkFactoryLoopLiveness({
      pid: 123,
      status: 'running',
      iteration: 1,
      maxIterations: 3,
      updatedAt: new Date(1_000).toISOString(),
      updatedAtMs: 1_000,
    }, { nowMs: 3_001, staleMs: 2_000 })).toMatchObject({
      ok: false,
      stale: true,
      reason: 'heartbeat stale',
    })
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

  it('stop releases each in-flight factory-dispatched agent', async () => {
    const mount = new FakeMountClient({ [issuePath(60)]: issueFile(60) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    await fleet.spawn({ name: 'external-worker', capability: 'spawn:codex', task: 'external', model: 'codex' })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(60), issueFile(60))))
    await factory.stop()

    expect(fleet.releases).toEqual([
      { name: 'ar-60-impl', reason: 'factory-stopped' },
      { name: 'ar-60-review', reason: 'factory-stopped' },
    ])
  })

  it('stop swallows one release failure and still releases others plus tears down listeners', async () => {
    const mount = new TrackingEventsMount({ [issuePath(61)]: issueFile(61) })
    const fleet = new ReleaseFailingFleetClient(new Set(['ar-61-impl']))
    const warnings: unknown[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => undefined,
      },
    })

    await factory.start()
    expect(mount.activeSubscriptions).toBe(1)
    await expect(factory.stop()).resolves.toBeUndefined()

    expect(fleet.releaseAttempts).toEqual([
      { name: 'ar-61-impl', reason: 'factory-stopped' },
      { name: 'ar-61-review', reason: 'factory-stopped' },
    ])
    expect(fleet.releases).toEqual([
      { name: 'ar-61-review', reason: 'factory-stopped' },
    ])
    expect(mount.activeSubscriptions).toBe(0)
    expect(mount.unsubscribeCount).toBe(1)
    expect(warnings).toEqual([
      [
        '[factory] failed to release ar-61-impl during stop',
        expect.objectContaining({ message: 'release failed for ar-61-impl' }),
      ],
    ])
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

  it('live subscription dispatches a newly-arrived in-scope ready issue from subscribe events', async () => {
    const path = issuePath(25)
    const mount = new FakeMountClient()
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    mount.files.set(path, { content: realIssueFile(25) })
    mount.emit(changeEvent(path, 'event-live-25'))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-25-impl', 'ar-25-review'])
    expect(factory.status().counters.liveEvents).toBe(1)
    expect(factory.status().counters.liveArrivalLatencyMsLast).toBeGreaterThanOrEqual(0)
    await factory.stop()
  })

  it('live subscription default uses subscribe without draining getEvents at startup', async () => {
    const path = issuePath(33)
    const mount = new CountingEventsMount()
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live' })

    expect(mount.getEventsCalls).toBe(0)

    mount.files.set(path, { content: realIssueFile(33) })
    mount.emit(changeEvent(path, 'event-live-default-33'))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-33-impl', 'ar-33-review'])
    await factory.stop()
  })

  it('live subscription default suppresses replayed pre-connect ready issues and accepts new arrivals', async () => {
    const replayPath = issuePath(34)
    const tipPath = issuePath(36)
    const newPath = issuePath(35)
    const mount = new CountingEventsMount({
      [replayPath]: realIssueFile(34),
      [tipPath]: realIssueFile(36),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    mount.emit(changeEvent(replayPath, '9'))
    mount.emit(changeEvent(tipPath, '10'))

    await factory.start({ mode: 'live' })

    expect(mount.getEventsCalls).toBe(0)

    mount.emit(changeEvent(replayPath, '9'))
    await flush()

    expect(fleet.spawns).toEqual([])
    expect(factory.status().counters.liveReplayEventsSuppressed).toBe(1)

    mount.files.set(newPath, { content: realIssueFile(35) })
    mount.emit(changeEvent(newPath, '100'))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-35-impl', 'ar-35-review'])
    await factory.stop()
  })

  it('live subscription suppresses old replay by time when high-watermark is unavailable', async () => {
    const replayPath = issuePath(37)
    const freshPath = issuePath(38)
    const skewPath = issuePath(39)
    const mount = new ThrowingWatermarkMount({
      [replayPath]: realIssueFile(37),
      [freshPath]: realIssueFile(38),
      [skewPath]: realIssueFile(39),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { replaySkewMarginMs: 60_000 } })

    mount.emit(changeEvent(replayPath, '201', new Date(Date.now() - 5 * 60_000).toISOString()))
    await flush()

    expect(fleet.spawns).toEqual([])
    expect(factory.status().counters.liveReplayEventsSuppressedByTime).toBe(1)

    mount.emit(changeEvent(skewPath, '202', new Date(Date.now() - 1_000).toISOString()))
    await flush()

    mount.emit(changeEvent(freshPath, '203', new Date(Date.now()).toISOString()))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual([
      'ar-39-impl',
      'ar-39-review',
      'ar-38-impl',
      'ar-38-review',
    ])
    await factory.stop()
  })

  it('live subscription ignores out-of-scope, non-ready, and non-real issue arrivals', async () => {
    const mount = new FakeMountClient()
    const fleet = new FakeFleetClient()
    const triage = new CountingTriage()
    const factory = createFactory(config(), { mount, fleet, triage })
    const cases = [
      { n: 26, content: realIssueFile(26, ready, { team: { key: 'OTHER', name: 'Other' } }) },
      { n: 27, content: realIssueFile(27, ready, { title: 'Real AR issue without synthetic marker' }) },
      { n: 28, content: realIssueFile(28, implementing) },
      { n: 29, content: realIssueFile(29, ready, { url: undefined }) },
    ]

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    for (const entry of cases) {
      const path = issuePath(entry.n)
      mount.files.set(path, { content: entry.content })
      mount.emit(changeEvent(path, `event-live-${entry.n}`))
    }
    await flush()

    expect(triage.count).toBe(0)
    expect(fleet.spawns).toEqual([])
    await factory.stop()
  })

  it('live subscription starts from the current event cursor and does not replay pre-start history', async () => {
    vi.useFakeTimers()
    try {
      const oldPath = issuePath(30)
      const newPath = issuePath(31)
      const mount = new NoWatermarkMount({ [oldPath]: realIssueFile(30) })
      const fleet = new FakeFleetClient()
      const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
      mount.emit(changeEvent(oldPath, 'event-before-start-30', new Date(Date.now() + 1_000).toISOString()))

      await factory.start({ mode: 'live', liveSubscription: { transport: 'poll', pollIntervalMs: 10 } })
      await vi.advanceTimersByTimeAsync(0)

      expect(fleet.spawns).toEqual([])

      mount.files.set(newPath, { content: realIssueFile(31) })
      mount.emit(changeEvent(newPath, 'event-after-start-31'))
      await vi.advanceTimersByTimeAsync(10)

      expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-31-impl', 'ar-31-review'])
      await factory.stop()
    } finally {
      vi.useRealTimers()
    }
  })

  it('live subscription dispatches a newly-arrived in-scope ready issue from getEvents polling', async () => {
    vi.useFakeTimers()
    try {
      const path = issuePath(32)
      const mount = new FakeMountClient()
      const fleet = new FakeFleetClient()
      const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

      await factory.start({ mode: 'live', liveSubscription: { transport: 'poll', pollIntervalMs: 10 } })
      await vi.advanceTimersByTimeAsync(0)
      mount.files.set(path, { content: realIssueFile(32) })
      mount.emit(changeEvent(path, 'event-live-poll-32'))
      await vi.advanceTimersByTimeAsync(10)

      expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-32-impl', 'ar-32-review'])
      await factory.stop()
    } finally {
      vi.useRealTimers()
    }
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

  it('dedupes dispatch attempts by issue key even when duplicate detections use different paths', async () => {
    const duplicatePath = '/linear/issues/AR-40__uuid-40-duplicate.json'
    const duplicateIssue = realIssueFile(40, ready, { id: 'uuid-40-duplicate' })
    const mount = new FakeMountClient({
      [issuePath(40)]: issueFile(40),
      [duplicatePath]: duplicateIssue,
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const first = await factory.triageIssue(parseLinearIssue(issuePath(40), issueFile(40)))
    const duplicate = await factory.triageIssue(parseLinearIssue(duplicatePath, duplicateIssue))

    await factory.dispatch(first)
    await expect(factory.dispatch(duplicate)).rejects.toThrow(/dispatch already in-flight/)

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-40-impl', 'ar-40-review'])
  })

  it('backs off dispatch errors and enforces a retry gap before the bounded terminal attempt', async () => {
    const clock = new ManualClock()
    const mount = new FakeMountClient({ [issuePath(41)]: issueFile(41) })
    const fleet = new TimestampFailingFleetClient(clock)
    const factory = createFactory(config({
      dispatch: { errorCooldownMs: 1_000, maxAttempts: 2 },
    }), { mount, fleet, triage: new StaticTriage(), clock })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(41), issueFile(41)))

    await expect(factory.dispatch(decision)).rejects.toThrow(/spawnPty failed/)
    expect(fleet.attemptTimes).toEqual([0])

    clock.advance(999)
    await expect(factory.dispatch(decision)).rejects.toThrow(/dispatch backoff active/)
    expect(fleet.attemptTimes).toEqual([0])

    clock.advance(1)
    await expect(factory.dispatch(decision)).rejects.toThrow(/spawnPty failed/)
    expect(fleet.attemptTimes).toEqual([0, 1_000])
    expect(fleet.attemptTimes[1]! - fleet.attemptTimes[0]!).toBeGreaterThanOrEqual(1_000)

    clock.advance(1_000)
    await expect(factory.dispatch(decision)).rejects.toThrow(/dispatch already terminal/)
    expect(fleet.attemptTimes).toEqual([0, 1_000])
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

  it('confirms delivery of the implementer task and reviewer handoff after dispatch', async () => {
    const mount = new FakeMountClient({ [issuePath(62)]: issueFile(62) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(62), issueFile(62))))

    expect(fleet.messages).toHaveLength(2)
    expect(fleet.messages[0]).toMatchObject({
      to: 'ar-62-impl',
      from: 'factory',
      data: { issue: { key: 'AR-62' } },
    })
    expect(fleet.messages[0]!.text).toContain('GitHub repo: AgentWorkforce/pear')
    expect(fleet.messages[0]!.text).toContain('Repo path: /work/pear')
    expect(fleet.messages[0]!.text).toContain('Linear issue: AR-62 - [factory-e2e] Fix factory issue 62')
    expect(fleet.messages[0]!.text).toContain('Full Linear issue description:')
    expect(fleet.messages[0]!.text).toContain('Implement the requested fix in packages/factory-sdk/src/orchestrator/factory.ts')
    expect(fleet.messages[0]!.text).toContain('Create a branch for this issue before editing.')
    expect(fleet.messages[0]!.text).toContain('Commit the implementation and tests.')
    expect(fleet.messages[0]!.text).toContain('Push the branch to origin.')
    expect(fleet.messages[0]!.text).toContain('Open a PR targeting `main` when done.')
    expect(fleet.messages[0]!.text).toContain('Use `gh pr create --base main` and report the PR URL.')
    expect(fleet.messages[0]!.text).toContain('DM the reviewer `ar-62-review` when the PR is ready.')
    expect(fleet.messages[0]!.text).toContain('DM `broker` when fully done.')
    expect(fleet.messages[0]!.text).toContain('Merge policy: never - open the PR for human review and approval; never merge it yourself.')
    expect(fleet.messages[1]).toMatchObject({
      to: 'ar-62-review',
      from: 'factory',
      text: 'Review is queued for AR-62. Watch implementer PR handoff and report readiness.',
    })
    expect(fleet.inputs).toEqual([
      { name: 'ar-62-impl', data: '\r' },
      { name: 'ar-62-review', data: '\r' },
    ])
    expect(fleet.deliveryEvents).toEqual([
      { kind: 'injected', to: 'ar-62-impl', eventId: 'fake-1' },
      { kind: 'input', name: 'ar-62-impl', data: '\r' },
      { kind: 'injected', to: 'ar-62-review', eventId: 'fake-2' },
      { kind: 'input', name: 'ar-62-review', data: '\r' },
    ])
  })

  it('falls back to the message recipient when the live injected ack omits targets', async () => {
    class UndefinedTargetsFleetClient extends FakeFleetClient {
      override async waitForInjected(
        input: Parameters<FakeFleetClient['waitForInjected']>[0],
        _opts?: Parameters<FakeFleetClient['waitForInjected']>[1],
      ): ReturnType<FakeFleetClient['waitForInjected']> {
        this.messages.push(input)
        const eventId = `fake-${this.messages.length}`
        this.deliveryEvents.push({ kind: 'injected', to: input.to, eventId })
        return { eventId, targets: undefined as unknown as string[] }
      }
    }
    const mount = new FakeMountClient({ [issuePath(65)]: issueFile(65) })
    const fleet = new UndefinedTargetsFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await expect(factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(65), issueFile(65))))).resolves.toMatchObject({
      issue: { key: 'AR-65' },
    })

    expect(fleet.inputs).toEqual([
      { name: 'ar-65-impl', data: '\r' },
      { name: 'ar-65-review', data: '\r' },
    ])
    expect(fleet.deliveryEvents).toEqual([
      { kind: 'injected', to: 'ar-65-impl', eventId: 'fake-1' },
      { kind: 'input', name: 'ar-65-impl', data: '\r' },
      { kind: 'injected', to: 'ar-65-review', eventId: 'fake-2' },
      { kind: 'input', name: 'ar-65-review', data: '\r' },
    ])
  })

  it('reinjects the confirmed implementer task after delivery_failed', async () => {
    const mount = new FakeMountClient({ [issuePath(63)]: issueFile(63) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const errors: unknown[] = []
    factory.on('error', (payload) => errors.push(payload))

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(63), issueFile(63))))
    fleet.emitDeliveryFailed({ to: 'ar-63-impl', msgId: 'fake-1', reason: 'dropped' })
    await flush()

    expect(fleet.messages).toHaveLength(3)
    expect(fleet.messages[2]).toMatchObject({
      to: 'ar-63-impl',
      from: 'factory',
      data: { issue: { key: 'AR-63' } },
    })
    expect(fleet.messages[2]!.text).toContain('Linear issue: AR-63 - [factory-e2e] Fix factory issue 63')
    expect(fleet.messages[2]!.text).toContain('Open a PR targeting `main` when done.')
    expect(fleet.messages[2]!.text).toContain('Use `gh pr create --base main` and report the PR URL.')
    expect(fleet.messages[2]!.text).toContain('DM the reviewer `ar-63-review` when the PR is ready.')
    expect(fleet.inputs).toEqual([
      { name: 'ar-63-impl', data: '\r' },
      { name: 'ar-63-review', data: '\r' },
      { name: 'ar-63-impl', data: '\r' },
    ])
    expect(fleet.deliveryEvents.at(-2)).toEqual({ kind: 'injected', to: 'ar-63-impl', eventId: 'fake-3' })
    expect(fleet.deliveryEvents.at(-1)).toEqual({ kind: 'input', name: 'ar-63-impl', data: '\r' })
    expect(errors).toHaveLength(1)
    expect(errors[0]).toMatchObject({ issue: { key: 'AR-63' } })
  })

  it('reinjects the confirmed reviewer handoff after delivery_failed', async () => {
    const mount = new FakeMountClient({ [issuePath(64)]: issueFile(64) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(64), issueFile(64))))
    fleet.emitDeliveryFailed({ to: 'ar-64-review', msgId: 'fake-2', reason: 'dropped' })
    await flush()

    expect(fleet.messages).toHaveLength(3)
    expect(fleet.messages[2]).toMatchObject({
      to: 'ar-64-review',
      from: 'factory',
      text: 'Review is queued for AR-64. Watch implementer PR handoff and report readiness.',
    })
    expect(fleet.inputs).toEqual([
      { name: 'ar-64-impl', data: '\r' },
      { name: 'ar-64-review', data: '\r' },
      { name: 'ar-64-review', data: '\r' },
    ])
    expect(fleet.deliveryEvents.at(-2)).toEqual({ kind: 'injected', to: 'ar-64-review', eventId: 'fake-3' })
    expect(fleet.deliveryEvents.at(-1)).toEqual({ kind: 'input', name: 'ar-64-review', data: '\r' })
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
    expect(errors[0]).toMatchObject({ errorMessage: expect.stringContaining('Writeback not acked') })
    expect(errors[0]).toHaveProperty('errorStack')
    expect(JSON.parse(JSON.stringify(errors[0]))).toMatchObject({
      issue: { key: 'AR-9' },
      errorMessage: expect.stringContaining('Writeback not acked'),
      errorStack: expect.stringContaining('Error: Writeback not acked'),
    })
  })

  it('classifies fleet spawn failures with issue, agent, capability, and cwd context', async () => {
    const mount = new FakeMountClient({ [issuePath(36)]: issueFile(36) })
    const fleet = new SpawnFailingFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const errors: unknown[] = []
    factory.on('error', (payload) => errors.push(payload))
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(36), issueFile(36)))

    await expect(factory.dispatch(decision)).rejects.toThrow(
      'Dispatch spawn failed for AR-36/ar-36-impl (spawn:codex) cwd=/work/pear: spawnPty failed',
    )
    expect(errors).toHaveLength(1)
    expect(JSON.parse(JSON.stringify(errors[0]))).toMatchObject({
      issue: { key: 'AR-36' },
      errorMessage: expect.stringContaining('Dispatch spawn failed for AR-36/ar-36-impl (spawn:codex) cwd=/work/pear'),
      errorStack: expect.stringContaining('Caused by: Error: spawnPty failed: cwd does not exist'),
    })
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
    class OrderedSlackMountClient extends CloudWritebackFakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        if (isSlackRootWritePath(path)) {
          order.push('slack-root')
        } else if (path.includes('/replies/')) {
          order.push('slack-reply')
        }
        await super.writeFile(path, content, opts)
      }
    }
    const mount = new OrderedSlackMountClient({
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
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey'>> = []
    const factory = createFactory(config({ slack: slackConfig('C0FACTORY') }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear,
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

  it('skips Slack writeback while sync is stale, logs once, and keeps Linear dispatch core running', async () => {
    const mount = new SlackSyncStatusMount({
      [issuePath(44)]: issueFile(44),
      [issuePath(45)]: issueFile(45),
      [issuePath(46)]: issueFile(46),
    })
    mount.slackStatus = { provider: 'slack', status: 'lagging' }
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const factory = createFactory(config({ batchSize: 5, slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => undefined,
      },
    })

    const report = await factory.runOnce()

    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-44', 'AR-45', 'AR-46'])
    expect(report.slackDegraded).toBe(true)
    expect(mount.writes.filter((write) => isSlackRootWritePath(write.path))).toEqual([])
    expect(mount.writes.filter((write) =>
      write.path.startsWith('/linear/issues/') &&
      (write.content as { stateId?: string }).stateId === implementing,
    )).toHaveLength(3)
    expect(factory.status()).toMatchObject({
      slackDegraded: true,
      slackDegradedReason: 'slack sync status is lagging',
      counters: {
        dispatched: 3,
        slackDegradedEpisodes: 1,
        slackWritebacksSkipped: 3,
      },
    })
    expect(warnings.filter((warning) => warning[0] === '[factory] Slack sync degraded; skipping Slack writeback')).toHaveLength(1)
  })

  it('treats live-shaped bare Slack sync status with zero Slack events as degraded', async () => {
    const mount = new SlackSyncStatusMount({
      [issuePath(50)]: issueFile(50),
      [issuePath(51)]: issueFile(51),
    })
    mount.slackStatus = { provider: 'slack' }
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => undefined,
      },
    })

    const report = await factory.runOnce()

    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-50', 'AR-51'])
    expect(mount.writes.filter((write) => isSlackRootWritePath(write.path))).toEqual([])
    expect(mount.writes.filter((write) =>
      write.path.startsWith('/linear/issues/') &&
      (write.content as { stateId?: string }).stateId === implementing,
    )).toHaveLength(2)
    expect(factory.status()).toMatchObject({
      slackDegraded: true,
      slackDegradedReason: 'slack sync has no recent event watermark',
      counters: {
        dispatched: 2,
        slackDegradedEpisodes: 1,
        slackWritebacksSkipped: 2,
      },
    })
    expect(warnings.filter((warning) => warning[0] === '[factory] Slack sync degraded; skipping Slack writeback')).toHaveLength(1)
  })

  it('does not skip Slack writeback when sync is healthy', async () => {
    const mount = new SlackSyncStatusMount({ [issuePath(47)]: issueFile(47) })
    mount.slackStatus = { provider: 'slack', lastEventAt: new Date().toISOString() }
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    const report = await factory.runOnce()

    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-47'])
    expect(report.slackDegraded).toBe(false)
    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(1)
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('AR-47: factory agents dispatched.')
    expect(factory.status().slackDegraded).toBe(false)
  })

  it('falls back to Slack event freshness when sync status lookup fails', async () => {
    const mount = new ThrowingSlackSyncStatusMount({ [issuePath(54)]: issueFile(54) })
    const slackEvent = changeEvent('/slack/channels/C0FACTORY__factory-e2e/messages/1781267200_000000/meta.json', 'slack-54')
    mount.emit({
      ...slackEvent,
      resource: {
        ...slackEvent.resource,
        provider: 'slack',
      },
    })
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => undefined,
      },
    })

    const report = await factory.runOnce()

    expect(report.dispatched.map((result) => result.issue.key)).toEqual(['AR-54'])
    expect(report.slackDegraded).toBe(false)
    expect(mount.writes.filter((write) => isSlackRootWritePath(write.path))).toHaveLength(1)
    expect(warnings.filter((warning) =>
      warning[0] === '[factory] Slack sync freshness check failed; proceeding without degradation',
    )).toHaveLength(1)
  })

  it('logs Slack recovery once and resumes writeback after a degraded episode', async () => {
    const mount = new SlackSyncStatusMount({
      [issuePath(48)]: issueFile(48),
      [issuePath(49)]: issueFile(49),
    })
    mount.slackStatus = { provider: 'slack', status: 'lagging' }
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const infos: unknown[][] = []
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        info: (...args: unknown[]) => infos.push(args),
        error: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(48), issueFile(48))))
    mount.slackStatus = { provider: 'slack', status: 'healthy', lastEventAtMs: Date.now() }
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(49), issueFile(49))))

    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(1)
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('AR-49: factory agents dispatched.')
    expect(factory.status()).toMatchObject({
      slackDegraded: false,
      counters: {
        slackDegradedEpisodes: 1,
        slackRecoveredEpisodes: 1,
      },
    })
    expect(warnings.filter((warning) => warning[0] === '[factory] Slack sync degraded; skipping Slack writeback')).toHaveLength(1)
    expect(infos.filter((info) => info[0] === '[factory] Slack sync recovered; resuming Slack writeback')).toHaveLength(1)
  })

  it('marks Slack degraded after one writeback failure and skips the next cycle without retrying', async () => {
    const mount = new FailSlackRootMountClient({
      [issuePath(52)]: issueFile(52),
      [issuePath(53)]: issueFile(53),
    })
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        error: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(52), issueFile(52))))
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(53), issueFile(53))))

    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(1)
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('AR-52: factory agents dispatched.')
    expect(mount.writes.filter((write) =>
      write.path.startsWith('/linear/issues/') &&
      (write.content as { stateId?: string }).stateId === implementing,
    )).toHaveLength(2)
    expect(factory.status()).toMatchObject({
      slackDegraded: true,
      counters: {
        dispatched: 2,
        slackDegradedEpisodes: 1,
        slackWritebacksSkipped: 1,
      },
    })
    expect(factory.status().slackDegradedReason).toMatch(/slack writeback failed/)
    expect(warnings.filter((warning) =>
      warning[0] === '[factory] Slack writeback failed; marking Slack degraded',
    )).toHaveLength(1)
  })

  it('does not clear a writeback-failure latch from a healthy read watermark', async () => {
    const mount = new FailSlackRootMountClient({
      [issuePath(54)]: issueFile(54),
      [issuePath(55)]: issueFile(55),
      [issuePath(56)]: issueFile(56),
    })
    mount.slackStatus = { provider: 'slack', lastEventAt: new Date().toISOString() }
    const fleet = new FakeFleetClient()
    const warnings: unknown[][] = []
    const infos: unknown[][] = []
    const factory = createFactory(config({ slack: slackConfig(), batchSize: 5 }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
        info: (...args: unknown[]) => infos.push(args),
        error: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(54), issueFile(54))))
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(55), issueFile(55))))
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(56), issueFile(56))))

    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(1)
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('AR-54: factory agents dispatched.')
    expect(mount.writes.filter((write) =>
      write.path.startsWith('/linear/issues/') &&
      (write.content as { stateId?: string }).stateId === implementing,
    )).toHaveLength(3)
    expect(factory.status()).toMatchObject({
      slackDegraded: true,
      counters: {
        dispatched: 3,
        slackDegradedEpisodes: 1,
        slackWritebacksSkipped: 2,
      },
    })
    expect(factory.status().slackDegradedReason).toMatch(/slack writeback failed/)
    expect(warnings.filter((warning) =>
      warning[0] === '[factory] Slack writeback failed; marking Slack degraded',
    )).toHaveLength(1)
    expect(infos.filter((info) =>
      info[0] === '[factory] Slack sync recovered; resuming Slack writeback',
    )).toEqual([])
  })

  it('probes after writeback-failure cooldown and clears the latch on success', async () => {
    const clock = new ManualClock()
    const mount = new RecoveringSlackRootMountClient({
      [issuePath(57)]: issueFile(57),
      [issuePath(58)]: issueFile(58),
      [issuePath(59)]: issueFile(59),
    })
    const fleet = new FakeFleetClient()
    const infos: unknown[][] = []
    const factory = createFactory(config({
      slack: slackConfig(),
      batchSize: 5,
      dispatch: { errorCooldownMs: 1_000, maxAttempts: 3 },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      clock,
      logger: {
        info: (...args: unknown[]) => infos.push(args),
        error: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(57), issueFile(57))))
    mount.slackStatus = { provider: 'slack' }
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(58), issueFile(58))))
    clock.advance(10 * 60_000)
    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(59), issueFile(59))))

    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(2)
    expect(slackRoots.map((write) => (write.content as { text?: string }).text)).toEqual([
      expect.stringContaining('AR-57: factory agents dispatched.'),
      expect.stringContaining('AR-59: factory agents dispatched.'),
    ])
    expect(factory.status()).toMatchObject({
      slackDegraded: false,
      slackDegradedReason: undefined,
      counters: {
        dispatched: 3,
        slackDegradedEpisodes: 1,
        slackRecoveredEpisodes: 1,
        slackWritebacksSkipped: 1,
      },
    })
    expect(infos.filter((info) =>
      info[0] === '[factory] Slack writeback recovered; clearing write-failure degradation',
    )).toHaveLength(1)
  })

  it('watches the in-flight factory Slack thread and replies to a human status request with live state, roster, and PR', async () => {
    const mount = new ConfirmRecordingSlackMountClient({ [issuePath(24)]: issueFile(24) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 240 }),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(24), issueFile(24))))
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-1'), 'slack-human-1', {
      text: 'status?',
      user: 'U123',
      user_name: 'human',
      user_is_bot: false,
    })
    await flush()
    await flush()

    const replies = slackReplyWrites(mount)
    expect(slack.replies).toEqual([])
    expect(replies).toHaveLength(1)
    expect(replies[0]?.content.thread_ts).toBe(slack.threadId)
    expect(replies[0]?.content.text).toContain('AR-24')
    expect(replies[0]?.content.text).toContain(implementing)
    expect(replies[0]?.content.text).toContain('ar-24-impl')
    expect(replies[0]?.content.text).toContain('ar-24-review')
    expect(replies[0]?.content.text).toContain('https://github.com/AgentWorkforce/pear/pull/240')
    expect(mount.confirmedPaths.filter((path) => path.includes('/replies/'))).toEqual([replies[0]?.path])
  })

  it('watches top-level inbound Slack thread replies keyed by real reply ts', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(32)]: issueFile(32) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(32), issueFile(32))))
    emitSlackTopLevelMessage(mount, 'C0FACTORY__factory-e2e', '1780751619.000001', 'slack-human-top-level', {
      text: 'status?',
      thread_ts: slack.threadId,
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    const replies = slackReplyWrites(mount)
    expect(slack.replies).toEqual([])
    expect(replies).toHaveLength(1)
    expect(replies[0]?.content.thread_ts).toBe(slack.threadId)
  })

  it.each([
    ['user_is_bot marker', { user: 'U-BOT-MIRROR', user_is_bot: true }],
    ['configured bot user id', { user: 'U0B2596R7EZ', user_is_bot: false }],
  ])('degraded self-ignore: inbound %s is ignored', async (_name, marker) => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(33)]: issueFile(33) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(33), issueFile(33))))
    emitSlackTopLevelMessage(mount, 'C0FACTORY__factory-e2e', `1780751620.${marker.user === 'U0B2596R7EZ' ? '000002' : '000001'}`, `slack-self-${marker.user}`, {
      text: 'status?',
      thread_ts: slack.threadId,
      ...marker,
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])
  })

  it('degraded thread/channel guard: off-thread, mismatched-thread, and wrong-channel replies are skipped', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(34)]: issueFile(34) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(34), issueFile(34))))
    emitSlackTopLevelMessage(mount, 'C0FACTORY__factory-e2e', '1780751621.000001', 'slack-mismatched-thread', {
      text: 'wrong parent',
      thread_ts: '1780759999.000001',
      user: 'U123',
      user_is_bot: false,
    })
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', '1780759999.000001', 'human-off-thread'), 'slack-off-thread', {
      text: 'wrong nested parent',
      user: 'U123',
      user_is_bot: false,
    })
    emitSlackTopLevelMessage(mount, 'C0PRODUCT__general', '1780751621.000002', 'slack-wrong-channel', {
      text: 'right parent wrong channel',
      thread_ts: slack.threadId,
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])
  })

  it('degraded positive control: genuine human reply in the watched thread is answered', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(35)]: issueFile(35) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(35), issueFile(35))))
    emitSlackTopLevelMessage(mount, 'C0FACTORY__factory-e2e', '1780751622.000001', 'slack-human-positive', {
      text: 'status?',
      thread_ts: slack.threadId,
      user: 'U-HUMAN',
      user_is_bot: false,
    })
    await flush()
    await flush()

    const replies = slackReplyWrites(mount)
    expect(replies).toHaveLength(1)
    expect(replies[0]?.content.text).toContain('AR-35')
  })

  it('ignores the factory bot own Slack replies to avoid self-response loops', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(25)]: issueFile(25) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(25), issueFile(25))))
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'bot-1'), 'slack-bot-1', {
      text: 'AR-25: Ready for Agent',
      user: 'U0B2596R7EZ',
      user_name: 'file_by_agent_relay',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])
  })

  it('does not respond to Slack replies outside the watched factory-e2e issue thread', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(26)]: issueFile(26) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(26), issueFile(26))))
    emitSlackReply(mount, slackReplyFixturePath('C0PRODUCT__general', slack.threadId, 'human-product'), 'slack-product-1', {
      text: 'status?',
      user: 'U123',
      user_is_bot: false,
    })
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', '1780751613.000001', 'human-other-thread'), 'slack-other-thread-1', {
      text: 'status?',
      user: 'U123',
      user_is_bot: false,
    })
    emitSlackTopLevelMessage(mount, 'C0FACTORY__factory-e2e', slack.threadId, 'slack-parent-root', {
      text: 'root parent mirror',
      thread_ts: slack.threadId,
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])
  })

  it('connects Slack reply watchers from now and does not reprocess pre-existing thread replies', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(27)]: issueFile(27) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const oldPath = slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'old-human')
    emitSlackReply(mount, oldPath, 'slack-old-human', {
      text: 'old status?',
      user: 'U123',
      user_is_bot: false,
    })
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(27), issueFile(27))))
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])

    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'new-human'), 'slack-new-human', {
      text: 'new status?',
      user: 'U456',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toHaveLength(1)
  })

  it('dedupes duplicate inbound Slack reply delivery by event identity and content', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(28)]: issueFile(28) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })
    const replyPath = slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-duplicate')

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(28), issueFile(28))))
    emitSlackReply(mount, replyPath, 'slack-duplicate-human', {
      text: 'status?',
      user: 'U123',
      user_is_bot: false,
    })
    mount.emit(changeEvent(replyPath, 'slack-duplicate-human'))
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toHaveLength(1)
  })

  it('dedupes Slack status responses by human message ts across poll re-reads with fresh event ids', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(42)]: issueFile(42) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })
    const messageTs = '1780751642.000001'
    const path = slackTopLevelMessageFixturePath('C0FACTORY__factory-e2e', messageTs)

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(42), issueFile(42))))
    mount.files.set(path, {
      content: {
        provider: 'slack',
        objectType: 'message',
        objectId: 'slack-human-reread',
        payload: {
          channel: 'C0FACTORY',
          thread_ts: slack.threadId,
          ts: messageTs,
          text: 'status?',
          user: 'U123',
          user_is_bot: false,
        },
      },
    })
    mount.emit(changeEvent(path, 'slack-human-reread-1'))
    mount.emit(changeEvent(path, 'slack-human-reread-2'))
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toHaveLength(1)
  })

  it('dispose unsubscribes Slack watchers and clears their polling timers', async () => {
    vi.useFakeTimers()
    try {
      const mount = new TrackingEventsMount({ [issuePath(43)]: issueFile(43) })
      const fleet = new FakeFleetClient()
      const slack = new RecordingSlack()
      const factory = createFactory(config({ slack: slackConfig() }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        slack,
      })

      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(43), issueFile(43))))
      await vi.advanceTimersByTimeAsync(0)
      expect(mount.activeSubscriptions).toBe(1)
      const callsBeforeDispose = mount.getEventsCalls

      await factory.dispose()
      expect(mount.activeSubscriptions).toBe(0)
      expect(mount.unsubscribeCount).toBe(1)
      expect(fleet.releases).toEqual([
        { name: 'ar-43-impl', reason: 'factory-stopped' },
        { name: 'ar-43-review', reason: 'factory-stopped' },
      ])

      await vi.advanceTimersByTimeAsync(10_000)
      expect(mount.getEventsCalls).toBe(callsBeforeDispose)
    } finally {
      vi.useRealTimers()
    }
  })

  it('treats Slack dispatch thread startup as best-effort after agents are dispatched', async () => {
    const mount = new FailSlackRootMountClient({ [issuePath(29)]: issueFile(29) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    const result = await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(29), issueFile(29))))

    expect(result.agents.map((agent) => agent.name)).toEqual(['ar-29-impl', 'ar-29-review'])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-29-impl', 'ar-29-review'])
  })

  it('continues processing Slack reply events after one response fails', async () => {
    const mount = new FailFirstSlackReplyMountClient({ [issuePath(30)]: issueFile(30) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(30), issueFile(30))))
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-fails'), 'slack-human-fails', {
      text: 'status?',
      user: 'U123',
      user_is_bot: false,
    })
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-next'), 'slack-human-next', {
      text: 'status again?',
      user: 'U456',
      user_is_bot: false,
    })
    await flush()
    await flush()

    const replies = slackReplyWrites(mount)
    expect(replies).toHaveLength(2)
    expect(replies[1]?.content.text).toContain('AR-30')
  })

  it('uses numeric Slack reply event ids without dropping fresh low-seq replies', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(31)]: issueFile(31) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const warnings: unknown[][] = []
    const logger = {
      warn: (...args: unknown[]) => {
        warnings.push(args)
      },
      error: () => undefined,
      debug: () => undefined,
    }
    mount.emit(changeEvent('/slack/channels/C0FACTORY__factory-e2e/messages/other/replies/old.json', 1))
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
      logger,
    })
    const replyPath = slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-numeric')

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(31), issueFile(31))))
    mount.files.set(replyPath, {
      content: {
        provider: 'slack',
        objectType: 'message',
        objectId: 'slack-human-numeric',
        payload: {
          channel: 'C0FACTORY',
          thread_ts: slack.threadId,
          ts: 'slack-human-numeric',
          text: 'status?',
          user: 'U123',
          user_is_bot: false,
        },
      },
    })
    mount.emit(changeEvent(replyPath, 1))
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toHaveLength(1)
    expect(warnings.flat()).not.toContain('[factory] Slack reply event missing stable identity; falling back to path/content dedupe')
  })
})

const changeEvent = (path: string, id: string | number, occurredAt = new Date().toISOString()) => ({
  id,
  workspace: 'factory-test',
  type: 'relayfile.changed',
  occurredAt,
  resource: {
    path,
    kind: 'file',
    id: path,
    provider: 'linear',
  },
  summary: {},
  expand: async () => ({ level: 'summary', path, summary: {} }),
}) as unknown as ChangeEvent

const slackReplyFixturePath = (channelDir: string, threadId: string, replyId: string): string =>
  `/slack/channels/${channelDir}/messages/${threadId.replace(/\./g, '_')}/replies/${replyId}.json`

const slackTopLevelMessageFixturePath = (channelDir: string, messageTs: string): string =>
  `/slack/channels/${channelDir}/messages/${messageTs.replace(/\./g, '_')}/meta.json`

const emitSlackTopLevelMessage = (
  mount: FakeMountClient,
  channelDir: string,
  messageTs: string,
  id: string,
  payload: Record<string, unknown>,
): void => {
  const path = slackTopLevelMessageFixturePath(channelDir, messageTs)
  const channel = channelDir.split('__')[0]
  mount.files.set(path, {
    content: {
      provider: 'slack',
      objectType: 'message',
      objectId: id,
      payload: {
        channel,
        ts: messageTs,
        ...payload,
      },
    },
  })
  mount.emit(changeEvent(path, id))
}

const emitSlackReply = (
  mount: FakeMountClient,
  path: string,
  id: string,
  payload: Record<string, unknown>,
): void => {
  const threadTs = path.match(/\/messages\/([^/]+)\/replies\//u)?.[1]?.replace(/_/g, '.')
  const channel = path.match(/^\/slack\/channels\/([^/]+)\//u)?.[1]?.split('__')[0]
  mount.files.set(path, {
    content: {
      provider: 'slack',
      objectType: 'message',
      objectId: id,
      payload: {
        channel,
        thread_ts: threadTs,
        ts: id,
        ...payload,
      },
    },
  })
  mount.emit(changeEvent(path, id))
}

const isSlackRootWritePath = (path: string): boolean =>
  /^\/slack\/channels\/[^/]+\/messages\/[^/]+\.json$/u.test(path)

const slackReplyWrites = (mount: FakeMountClient): Array<{ path: string; content: { text?: string; thread_ts?: string } }> =>
  mount.writes
    .filter((write) => write.path.includes('/replies/'))
    .map((write) => ({ path: write.path, content: record(write.content) as { text?: string; thread_ts?: string } }))

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
