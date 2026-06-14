import { describe, expect, it, vi } from 'vitest'
import { readFileSync } from 'node:fs'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { BrokerEvent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import {
  FactoryConfigSchema,
  checkFactoryLoopLiveness,
  closeProbePr,
  createFactory,
  isInFactoryScope,
  parseLinearIssue,
  readFactoryInFlightRegistry,
  readFactoryLoopHeartbeat,
  reapFactoryOrphansOnce,
  type FactoryConfig,
  type TriageDecision,
  type TriageEngine,
} from '../index'
import type { ChangeEvent, EventPage, LinearWriteback, ProviderSyncStatus, SlackWriteback, SpawnInput, SpawnResult } from '../ports'
import { FakeFleetClient, FakeMountClient } from '../testing'
import type { CloseProbePrInput, GithubMergeGatePort, GithubMergeGateVerdict, GithubMergeInput, LinearIssue } from '../index'
import { BatchTracker } from './batch-tracker'
import { keyFromPath } from './factory'
import { InternalFleetClient, type HarnessDriverClientLike } from '../fleet/internal-fleet-client'

const ready = 'b9bec744-b60c-4745-8022-d90d6ab59ae3'
const implementing = '39b9881d-1196-4c95-8b80-a20f0c7263f7'
const done = '83ea5383-bfe9-425a-86ef-517b8190f09a'

type FactoryConfigOverrides = Omit<Partial<FactoryConfig>, 'loop'> & {
  loop?: Partial<FactoryConfig['loop']>
}

const config = (overrides: FactoryConfigOverrides = {}): FactoryConfig => FactoryConfigSchema.parse({
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
const readyAliasPath = (n: number) => `/linear/issues/by-state/ready-for-agent/AR-${n}.json`
const githubIssuePath = (owner: string, repo: string, number: number) => `/github/repos/${owner}/${repo}/issues/by-id/${number}.json`
const capturedReadyCanaryPath = '/linear/issues/AR-133__dac27fce-e8de-4910-bbf6-98ad436df3dd.json'
const capturedStaleDoneCanonicalPath = '/linear/issues/AR-173__40c7e780-59ad-47ee-8809-3a9b8434d8fb.json'
const capturedStaleReadyAliasPath = '/linear/issues/by-state/ready-for-agent/AR-173.json'
const capturedBareUuidPhantomPath = '/linear/issues/40c7e780-59ad-47ee-8809-3a9b8434d8fb.json'

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

const realMergeIssueFile = (n: number, stateId = ready) => realIssueFile(n, stateId, {
  title: `Real product issue ${n}`,
})

const githubIssueFile = (
  number: number,
  payload: {
    owner?: string
    repo?: string
    title?: string
    body?: string
    state?: string
    labels?: Array<string | { name: string }>
    url?: string
  } = {},
) => {
  const owner = payload.owner ?? 'AgentWorkforce'
  const repo = payload.repo ?? 'pear'
  return {
    provider: 'github',
    objectType: 'issue',
    objectId: String(number),
    payload: {
      number,
      title: payload.title ?? `GitHub factory issue ${number}`,
      body: payload.body ?? 'Implement the requested GitHub issue change and verify it with tests.',
      state: payload.state ?? 'open',
      labels: payload.labels ?? [{ name: 'factory' }],
      url: payload.url ?? `https://github.com/${owner}/${repo}/issues/${number}`,
      repository: {
        name: repo,
        owner: { login: owner },
      },
    },
  }
}

const prFile = (
  number: number,
  payload: { title?: string; body?: string; head_ref?: string; isDraft?: boolean } = {},
) => ({
  provider: 'github',
  objectType: 'pull_request',
  objectId: String(number),
  payload: {
    number,
    title: payload.title ?? `AR-${number}: test PR`,
    body: payload.body ?? '',
    head_ref: payload.head_ref ?? `ar-${number}-test`,
    isDraft: payload.isDraft,
  },
})

const ghPr = (
  number: number,
  payload: { title?: string; body?: string; headRefName?: string; isDraft?: boolean; state?: string } = {},
) => ({
  number,
  title: payload.title ?? `AR-${number}: test PR`,
  body: payload.body ?? '',
  headRefName: payload.headRefName ?? `ar-${number}-test`,
  isDraft: payload.isDraft ?? false,
  state: payload.state ?? 'OPEN',
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

class FailingSlackAnswerFleetClient extends FakeFleetClient {
  failuresRemaining = 1

  override async sendInput(name: string, data: string): Promise<void> {
    if (data.startsWith('Slack reply for ') && this.failuresRemaining > 0) {
      this.failuresRemaining -= 1
      throw new Error('sendInput failed')
    }
    await super.sendInput(name, data)
  }
}

class EscalatingTriage extends StaticTriage {
  readonly overrides: Partial<Pick<TriageDecision, 'thin' | 'confidence' | 'rationale'>>

  constructor(overrides: Partial<Pick<TriageDecision, 'thin' | 'confidence' | 'rationale'>> = {}) {
    super()
    this.overrides = overrides
  }

  override async triage(issue: LinearIssue): Promise<TriageDecision> {
    return {
      ...await super.triage(issue),
      thin: this.overrides.thin ?? true,
      confidence: this.overrides.confidence ?? 'low',
      rationale: this.overrides.rationale ?? 'Issue lacks enough acceptance detail.',
    }
  }
}

class SpawnFailingFleetClient extends FakeFleetClient {
  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    throw new Error('spawnPty failed: cwd does not exist')
  }
}

// Mimics the broker rejecting a resume because it never released the agent's
// name on exit (relay#1116-family): http 500 "agent '<name>' already exists".
class ResumeNameCollisionFleetClient extends FakeFleetClient {
  override async resume(input: Parameters<FakeFleetClient['resume']>[0]): Promise<SpawnResult> {
    this.resumes.push(input)
    const name = input.name ?? input.sessionRef
    throw Object.assign(new Error(`agent '${name}' already exists`), {
      code: 'http_500',
      status: 500,
      retryable: true,
      data: { error: `agent '${name}' already exists`, name, success: false },
    })
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

  async sleep(ms: number): Promise<void> {
    this.advance(ms)
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

class ScriptedGithubMergeGate implements GithubMergeGatePort {
  readonly checks: Array<{ repo: string; number: number; expectedHeadSha?: string }> = []
  readonly merges: GithubMergeInput[] = []
  readonly #verdicts: GithubMergeGateVerdict[]
  #mergeResult: { merged: boolean; reason: string }

  constructor(verdicts: GithubMergeGateVerdict[], mergeResult: { merged: boolean; reason: string } = { merged: true, reason: 'merged' }) {
    this.#verdicts = [...verdicts]
    this.#mergeResult = mergeResult
  }

  async check(input: { repo: string; number: number; expectedHeadSha?: string }): Promise<GithubMergeGateVerdict> {
    this.checks.push(input)
    return this.#verdicts.shift() ?? this.#verdicts.at(-1) ?? refusedMergeVerdict('no scripted verdict')
  }

  async merge(input: GithubMergeInput): Promise<{ merged: boolean; reason: string }> {
    this.merges.push(input)
    return this.#mergeResult
  }
}

const readyMergeVerdict = (headRefOid = 'green-sha'): GithubMergeGateVerdict => ({
  verdict: 'READY',
  ready: true,
  reason: 'ready',
  live: {
    mergeable: 'MERGEABLE',
    mergeStateStatus: 'CLEAN',
    headRefOid,
    reviewDecision: 'APPROVED',
    checkStates: ['SUCCESS'],
  },
})

const refusedMergeVerdict = (reason: string): GithubMergeGateVerdict => ({
  verdict: 'REFUSE',
  ready: false,
  reason,
  live: {
    mergeable: 'MERGEABLE',
    mergeStateStatus: 'UNSTABLE',
    headRefOid: 'red-sha',
    reviewDecision: 'APPROVED',
    checkStates: ['FAILURE'],
  },
})

const stateOnlyLinear = (mount: FakeMountClient): LinearWriteback => ({
  async setState(issue, stateId) {
    await mount.writeFile(issue.path, { stateId })
  },
  async postComment() {},
  async createIssue() {
    throw new Error('not used')
  },
  async verify() {
    return true
  },
})

class CapturedPidFleetClient extends FakeFleetClient {
  readonly plans: Map<string, SpawnResult>

  constructor(plans: SpawnResult[]) {
    super()
    this.plans = new Map(plans.map((plan) => [plan.name, plan]))
  }

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    const planned = this.plans.get(input.name)
    return {
      name: input.name,
      sessionRef: planned?.sessionRef ?? `session-${input.name}`,
      pid: planned?.pid,
      pids: planned?.pids,
    }
  }
}

class InjectFailingPidFleetClient extends CapturedPidFleetClient {
  injectionAttempts = 0

  override async waitForInjected(
    input: Parameters<FakeFleetClient['waitForInjected']>[0],
    _opts?: Parameters<FakeFleetClient['waitForInjected']>[1],
  ): ReturnType<FakeFleetClient['waitForInjected']> {
    this.messages.push(input)
    this.injectionAttempts += 1
    throw new Error(`recipient unavailable: ${input.to}`)
  }
}

class SelectiveInjectFailingPidFleetClient extends CapturedPidFleetClient {
  constructor(plans: SpawnResult[], readonly failPrefix: string) {
    super(plans)
  }

  override async waitForInjected(
    input: Parameters<FakeFleetClient['waitForInjected']>[0],
    _opts?: Parameters<FakeFleetClient['waitForInjected']>[1],
  ): ReturnType<FakeFleetClient['waitForInjected']> {
    this.messages.push(input)
    if (input.to.startsWith(this.failPrefix)) {
      throw new Error(`recipient unavailable: ${input.to}`)
    }
    const eventId = `fake-${this.messages.length}`
    this.deliveryEvents.push({ kind: 'injected', to: input.to, eventId })
    return { eventId, targets: [input.to] }
  }
}

class LagThenInjectedFleetClient extends FakeFleetClient {
  injectionAttempts = 0

  override async waitForInjected(
    input: Parameters<FakeFleetClient['waitForInjected']>[0],
    _opts?: Parameters<FakeFleetClient['waitForInjected']>[1],
  ): ReturnType<FakeFleetClient['waitForInjected']> {
    this.messages.push(input)
    this.injectionAttempts += 1
    if (this.injectionAttempts === 1) {
      throw new Error(`recipient unavailable: ${input.to}`)
    }
    const eventId = `fake-${this.messages.length}`
    this.deliveryEvents.push({ kind: 'injected', to: input.to, eventId })
    return { eventId, targets: [input.to] }
  }
}

class UnresolvedPidFleetClient extends FakeFleetClient {
  async resolveAgentPid(_name: string): Promise<{ status: 'unresolved' }> {
    return { status: 'unresolved' }
  }
}

class FoundPidFleetClient extends FakeFleetClient {
  constructor(readonly pidsByName: Map<string, number>) {
    super()
  }

  async resolveAgentPid(name: string): Promise<{ status: 'found'; pid: number } | { status: 'missing' }> {
    const pid = this.pidsByName.get(name)
    return pid ? { status: 'found', pid } : { status: 'missing' }
  }
}

class RosterPidHarnessClient implements HarnessDriverClientLike {
  readonly brokerPid = 68009
  readonly spawned: SpawnPtyInput[] = []
  readonly releases: Array<{ name: string; reason?: string }> = []
  readonly sent: SendMessageInput[] = []
  readonly inputs: Array<{ name: string; data: string }> = []
  readonly eventListeners = new Set<(event: BrokerEvent) => void>()
  readonly agents = new Map<string, { name: string; pid?: number }>()
  readonly pidsByName = new Map<string, number>()

  async spawnPty(input: SpawnPtyInput): Promise<{ name: string; session_ref: string }> {
    this.spawned.push(input)
    this.agents.set(input.name, { name: input.name })
    return { name: input.name, session_ref: `session-${input.name}` }
  }

  async release(name: string, reason?: string): Promise<{ name: string }> {
    this.releases.push({ name, reason })
    this.agents.delete(name)
    return { name }
  }

  async listAgents(): Promise<Array<{ name: string; pid?: number }>> {
    return [...this.agents.values()].map((agent) => ({ ...agent, pid: this.pidsByName.get(agent.name) }))
  }

  async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets?: string[] }> {
    this.sent.push(input)
    const eventId = `event-${this.sent.length}`
    this.emit({ kind: 'delivery_injected', event_id: eventId, name: input.to } as BrokerEvent)
    return { event_id: eventId, targets: [input.to] }
  }

  async sendInput(name: string, data: string): Promise<void> {
    this.inputs.push({ name, data })
  }

  connectEvents(): void {}

  onEvent(listener: (event: BrokerEvent) => void): () => void {
    this.eventListeners.add(listener)
    return () => {
      this.eventListeners.delete(listener)
    }
  }

  addListener(): () => void {
    return () => {}
  }

  emit(event: BrokerEvent): void {
    for (const listener of this.eventListeners) {
      listener(event)
    }
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

class HangingUnsubscribeMount extends TrackingEventsMount {
  unsubscribeStarted = 0
  readonly unsubscribeStartedPromise: Promise<void>
  #releaseUnsubscribe!: () => void
  #resolveUnsubscribeStarted!: () => void
  #unsubscribeReleased = new Promise<void>((resolve) => {
    this.#releaseUnsubscribe = resolve
  })

  constructor(initialFiles: Record<string, unknown> = {}) {
    super(initialFiles)
    this.unsubscribeStartedPromise = new Promise<void>((resolve) => {
      this.#resolveUnsubscribeStarted = resolve
    })
  }

  override subscribe(...args: Parameters<FakeMountClient['subscribe']>): ReturnType<FakeMountClient['subscribe']> {
    const subscription = super.subscribe(...args)
    return {
      unsubscribe: async () => {
        this.unsubscribeStarted += 1
        this.#resolveUnsubscribeStarted()
        await this.#unsubscribeReleased
        await subscription.unsubscribe()
      },
    }
  }

  releaseUnsubscribe(): void {
    this.#releaseUnsubscribe()
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
    throw new Error('watermark unavailable')
  }
}

class CountingListTreeMount extends FakeMountClient {
  readonly listTreePrefixes: string[] = []

  override async listTree(prefix: string): Promise<string[]> {
    this.listTreePrefixes.push(prefix)
    return super.listTree(prefix)
  }
}

class RouteNotFoundCountingListTreeMount extends CountingListTreeMount {
  override async getEventHighWatermark(): Promise<string | undefined> {
    throw Object.assign(new Error('Route not found'), { status: 404 })
  }
}

class RouteNotFoundThrowingPullMount extends FakeMountClient {
  override async getEventHighWatermark(): Promise<string | undefined> {
    throw Object.assign(new Error('Route not found'), { status: 404 })
  }

  override async listTree(): Promise<string[]> {
    throw new Error('startup pull boom')
  }
}

class ArrivesDuringPullMount extends FakeMountClient {
  onFirstListTree?: () => void
  #listed = false

  override async getEventHighWatermark(): Promise<string | undefined> {
    throw Object.assign(new Error('Route not found'), { status: 404 })
  }

  override async listTree(prefix: string): Promise<string[]> {
    const result = await super.listTree(prefix)
    if (!this.#listed) {
      this.#listed = true
      this.onFirstListTree?.()
    }
    // The startup pull never sees AR-51 — it only arrives via a live event mid-pull.
    return result.filter((path) => !path.includes('AR-51'))
  }
}

class BlockingIssueReadMount extends FakeMountClient {
  readCount = 0
  observedStaleWhileReading = false

  constructor(
    initialFiles: Record<string, unknown>,
    readonly clock: ManualClock,
    readonly opts: { heartbeatPath: string; staleMs: number; blockMs: number; advanceMs: number },
  ) {
    super(initialFiles)
  }

  override async readFile(path: string): Promise<{ content: unknown }> {
    if (path.startsWith('/linear/issues/')) {
      this.readCount += 1
      const until = Date.now() + this.opts.blockMs
      while (Date.now() < until) {
        // Simulate CPU-heavy real handler work before the first await can yield.
      }
      this.clock.advance(this.opts.advanceMs)
      const heartbeat = JSON.parse(readFileSync(this.opts.heartbeatPath, 'utf8')) as { updatedAtMs?: number }
      this.observedStaleWhileReading ||= this.clock.now() - (heartbeat.updatedAtMs ?? 0) > this.opts.staleMs
    }
    return super.readFile(path)
  }
}

class DelayedIssueReadMount extends FakeMountClient {
  activeIssueReads = 0
  maxConcurrentIssueReads = 0
  readCount = 0

  constructor(initialFiles: Record<string, unknown>, readonly delayMs: number) {
    super(initialFiles)
  }

  override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
    if (path.startsWith('/linear/issues/') && !path.includes('/by-state/')) {
      this.readCount += 1
      this.activeIssueReads += 1
      this.maxConcurrentIssueReads = Math.max(this.maxConcurrentIssueReads, this.activeIssueReads)
      try {
        await new Promise((resolve) => setTimeout(resolve, this.delayMs))
        return await super.readFile(path)
      } finally {
        this.activeIssueReads -= 1
      }
    }
    return super.readFile(path)
  }
}

class ListingReadTrackingMount extends FakeMountClient {
  readonly readPaths: string[] = []

  constructor(initialFiles: Record<string, unknown> = {}, readonly extraTreePaths: string[] = []) {
    super(initialFiles)
  }

  override async listTree(prefix: string): Promise<string[]> {
    return [...new Set([
      ...await super.listTree(prefix),
      ...this.extraTreePaths.filter((path) => path.startsWith(prefix)),
    ])].sort()
  }

  override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
    this.readPaths.push(path)
    return super.readFile(path)
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

  it('extracts issue keys from canonical and live ready-alias paths', () => {
    expect(keyFromPath('/linear/issues/AR-173__40c7e780-59ad-47ee-8809-3a9b8434d8fb.json')).toBe('AR-173')
    expect(keyFromPath('/linear/issues/by-state/ready-for-agent/AR-173.json')).toBe('AR-173')
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

  it('mirrors factory-labeled GitHub issues from the relayfile mount into Linear create drafts', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, {
        owner: 'AgentWorkforce',
        repo: 'pear',
        title: 'Route GitHub factory issues',
        body: 'Use the synced GitHub mount, not the GitHub API.',
      }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({
      safety: { requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
    }), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.dispatched).toEqual([])
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toHaveLength(1)
    expect(mount.writes[0]?.path).toMatch(/^\/linear\/issues\/factory-create-github-[a-z0-9]+\.json$/u)
    expect(mount.writes[0]?.content).toEqual({
      title: '[factory] Route GitHub factory issues',
      stateId: ready,
      description: 'Use the synced GitHub mount, not the GitHub API.\n\nSource: https://github.com/AgentWorkforce/pear/issues/1116',
      labels: [{ name: 'pear' }],
      team: { key: 'AR' },
      source: {
        provider: 'github',
        owner: 'AgentWorkforce',
        repo: 'pear',
        number: 1116,
        url: 'https://github.com/AgentWorkforce/pear/issues/1116',
        path: ghPath,
      },
    })
    expect(factory.status().counters.githubIssueMirrorsCreated).toBe(1)
  })

  it('dedupes replayed GitHub issue ingestion before Linear canonical sync appears', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, {
        owner: 'AgentWorkforce',
        repo: 'pear',
        title: 'Route GitHub factory issues once',
      }),
    })
    const factory = createFactory(config({
      safety: { requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
    }), { mount, fleet: new FakeFleetClient(), triage: new StaticTriage() })

    await factory.runOnce()
    await factory.runOnce()

    expect(mount.writes).toHaveLength(1)
    expect(factory.status().counters.githubIssueMirrorsCreated).toBe(1)
    expect(factory.status().counters.githubIssueMirrorsDeduped).toBe(1)
  })

  it('closes a Linear mirror when the factory-labeled GitHub issue closes', async () => {
    const ghUrl = 'https://github.com/AgentWorkforce/pear/issues/1116'
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, {
        owner: 'AgentWorkforce',
        repo: 'pear',
        state: 'closed',
        url: ghUrl,
      }),
      [issuePath(258)]: realIssueFile(258, ready, {
        title: '[factory] GitHub mirror',
        description: `Mirrored from GitHub.\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
      }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({
      safety: { requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
    }), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.dispatched).toEqual([])
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toContainEqual({
      path: issuePath(258),
      content: expect.objectContaining({
        title: '[factory] GitHub mirror',
        stateId: done,
        description: `Mirrored from GitHub.\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
      }),
    })
    expect(factory.status().counters.githubIssueMirrorsClosed).toBe(1)
  })

  it('caches the resolved mirror path so repeat cycles do not rescan Linear issues', async () => {
    const ghUrl = 'https://github.com/AgentWorkforce/pear/issues/1116'
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    class CountingIssueListMount extends FakeMountClient {
      issueRootListCalls = 0
      override async listTree(prefix: string): Promise<string[]> {
        if (prefix === '/linear/issues') this.issueRootListCalls += 1
        return super.listTree(prefix)
      }
    }
    const mount = new CountingIssueListMount({
      [ghPath]: githubIssueFile(1116, { url: ghUrl }),
      [issuePath(258)]: realIssueFile(258, done, {
        title: '[factory] GitHub mirror',
        description: `Mirrored from GitHub.\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
      }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({
      safety: { requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
    }), { mount, fleet, triage: new StaticTriage() })

    await factory.runOnce()
    const afterFirst = mount.issueRootListCalls
    await factory.runOnce()
    const secondCycleScans = mount.issueRootListCalls - afterFirst

    expect(secondCycleScans).toBeLessThan(afterFirst)
    expect(factory.status().counters.githubIssueMirrorsDeduped).toBe(2)
  })

  it('completes an in-flight mirror when its GitHub issue closes', async () => {
    const ghUrl = 'https://github.com/AgentWorkforce/pear/issues/1116'
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, { url: ghUrl }),
      [issuePath(258)]: realIssueFile(258, ready, {
        title: '[factory] GitHub mirror',
        description: `Mirrored from GitHub.\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
      }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({
      safety: { requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
    }), { mount, fleet, triage: new StaticTriage() })

    await factory.runOnce()
    expect(factory.status().inFlight.map((issue) => issue.key)).toContain('AR-258')

    mount.files.set(ghPath, { content: githubIssueFile(1116, { state: 'closed', url: ghUrl }) })
    await factory.runOnce()

    expect(fleet.releases.map((release) => release.name)).toEqual(
      expect.arrayContaining(['ar-258-impl', 'ar-258-review']),
    )
    expect(factory.status().counters.githubIssueMirrorsClosed).toBe(1)
    expect(factory.status().inFlight.map((issue) => issue.key)).not.toContain('AR-258')
  })

  it('uses canonical issue state when a ready alias is stale', async () => {
    const canonicalPath = '/linear/issues/AR-67__uuid-67-canonical.json'
    const aliasPath = readyAliasPath(67)
    const mount = new FakeMountClient({
      [canonicalPath]: realIssueFile(67, done, { id: 'uuid-67-canonical' }),
      [aliasPath]: realIssueFile(67, ready, { id: 'uuid-67-alias' }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled).toEqual([{ uuid: 'uuid-67-canonical', key: 'AR-67', path: canonicalPath }])
    expect(report.dispatched).toEqual([])
    expect(report.skipped).toEqual([
      { issue: { uuid: 'uuid-67-canonical', key: 'AR-67', path: canonicalPath }, reason: 'live state is not ready-for-agent' },
    ])
    expect(fleet.spawns).toEqual([])
  })

  it('re-dispatches a terminal issue after a canonical Done to Ready re-open', async () => {
    const mount = new FakeMountClient({ [issuePath(364)]: issueFile(364) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const first = await factory.runOnce()
    expect(first.dispatched.map((result) => result.issue.key)).toEqual(['AR-364'])

    fleet.emitAgentExit('ar-364-impl', 'issue-done')
    await flush()
    expect(factory.status().counters.done).toBe(1)

    await mount.writeFile(issuePath(364), issuePayload(364, ready))
    const reopened = await factory.runOnce()

    expect(reopened.dispatched.map((result) => result.issue.key)).toEqual(['AR-364'])
    expect(reopened.skipped).toEqual([])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual([
      'ar-364-impl',
      'ar-364-review',
      'ar-364-impl',
      'ar-364-review',
    ])
    expect(factory.status().counters.dispatchTerminalReopened).toBe(1)
  })

  it('does not re-dispatch a terminal issue from a stale ready alias when canonical state is still done', async () => {
    const mount = new FakeMountClient({ [issuePath(365)]: issueFile(365) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.runOnce()
    fleet.emitAgentExit('ar-365-impl', 'issue-done')
    await flush()

    await mount.writeFile(readyAliasPath(365), realIssueFile(365, ready))
    const report = await factory.runOnce()

    expect(report.dispatched).toEqual([])
    expect(report.skipped).toEqual([
      { issue: { uuid: 'uuid-365', key: 'AR-365', path: issuePath(365) }, reason: 'dispatch already terminal' },
    ])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-365-impl', 'ar-365-review'])
    expect(factory.status().counters.dispatchTerminalReopened).toBeUndefined()
  })

  it('mirrors a factory-labeled GitHub issue from the mount into one Linear create draft', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, {
        title: 'Relay issue should dispatch',
        body: 'Implement the relay fix.\n\nAcceptance: add tests.',
        labels: [{ name: 'factory' }],
      }),
    })
    const factory = createFactory(config(), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    const report = await factory.runOnce()

    expect(report.dispatched).toEqual([])
    expect(mount.writes).toHaveLength(1)
    expect(mount.writes[0]?.path).toMatch(/^\/linear\/issues\/factory-create-github-[a-z0-9]+\.json$/u)
    expect(mount.writes[0]?.content).toEqual({
      title: '[factory] Relay issue should dispatch',
      stateId: ready,
      description: 'Implement the relay fix.\n\nAcceptance: add tests.\n\nSource: https://github.com/AgentWorkforce/pear/issues/1116',
      labels: [{ name: 'pear' }],
      team: { key: 'AR' },
      source: {
        provider: 'github',
        owner: 'AgentWorkforce',
        repo: 'pear',
        number: 1116,
        url: 'https://github.com/AgentWorkforce/pear/issues/1116',
        path: ghPath,
      },
    })
    expect(factory.status().counters.githubIssueMirrorsCreated).toBe(1)
  })

  it('dedupes GitHub issue mirrors across restart by persisted Linear source metadata', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1117)
    const ghUrl = 'https://github.com/AgentWorkforce/pear/issues/1117'
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1117, { url: ghUrl }),
      [issuePath(258)]: realIssueFile(258, done, {
        title: '[factory] GitHub factory issue 1117',
        description: `Existing mirror\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
        source: {
          provider: 'github',
          owner: 'AgentWorkforce',
          repo: 'pear',
          number: 1117,
          url: ghUrl,
          path: ghPath,
        },
      }),
    })
    const factory = createFactory(config(), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    const report = await factory.runOnce()

    expect(mount.writes).toEqual([])
    expect(report.dispatched).toEqual([])
    expect(factory.status().counters.githubIssueMirrorsDeduped).toBe(1)
  })

  it('closes the Linear mirror when a mirrored GitHub issue closes', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1118)
    const ghUrl = 'https://github.com/AgentWorkforce/pear/issues/1118'
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1118, {
        state: 'closed',
        labels: [],
        url: ghUrl,
      }),
      [issuePath(259)]: realIssueFile(259, ready, {
        title: '[factory] GitHub factory issue 1118',
        description: `Existing mirror\n\nSource: ${ghUrl}`,
        labels: [{ name: 'pear' }],
        source: {
          provider: 'github',
          owner: 'AgentWorkforce',
          repo: 'pear',
          number: 1118,
          url: ghUrl,
          path: ghPath,
        },
      }),
    })
    const factory = createFactory(config(), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    await factory.runOnce()

    expect(mount.writes).toContainEqual({
      path: issuePath(259),
      content: expect.objectContaining({ stateId: done }),
    })
    expect(factory.status().counters.githubIssueMirrorsClosed).toBe(1)
    expect(parseLinearIssue(issuePath(259), (await mount.readFile(issuePath(259))).content).stateId).toBe(done)
  })

  it('mirrors a GitHub issue exposed under the live issues/<n>.json mount shape (no by-id segment)', async () => {
    const ghPath = '/github/repos/AgentWorkforce/pear/issues/1116.json'
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116, {
        title: 'Live mount shape issue',
        body: 'Body for the live-shape issue.',
      }),
    })
    const factory = createFactory(config(), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    await factory.runOnce()

    expect(mount.writes).toHaveLength(1)
    expect(mount.writes[0]?.path).toMatch(/^\/linear\/issues\/factory-create-github-[a-z0-9]+\.json$/u)
    expect(mount.writes[0]?.content).toMatchObject({
      title: '[factory] Live mount shape issue',
      source: expect.objectContaining({ provider: 'github', path: ghPath, number: 1116 }),
    })
    expect(factory.status().counters.githubIssueMirrorsCreated).toBe(1)
  })

  it('routes a GitHub mirror when repos.byLabel is configured with only the bare repo name', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1119)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1119, { title: 'Bare label route' }),
    })
    const factory = createFactory(config({
      repos: {
        // Configured with just "pear", not "AgentWorkforce/pear".
        byLabel: { pear: 'pear' },
        byProject: {},
        keywordRules: [],
        clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
        default: 'AgentWorkforce/pear',
      },
    }), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    await factory.runOnce()

    expect(mount.writes).toHaveLength(1)
    expect(mount.writes[0]?.content).toMatchObject({ labels: [{ name: 'pear' }] })
    expect(factory.status().counters.githubIssueMirrorsCreated).toBe(1)
    expect(factory.status().counters.githubIssueMirrorsSkippedUnroutable ?? 0).toBe(0)
  })

  it('skips GitHub issue ingestion entirely when the GitHub sub-root is not mounted', async () => {
    const ghPath = githubIssuePath('AgentWorkforce', 'pear', 1116)
    const mount = new FakeMountClient({
      [ghPath]: githubIssueFile(1116),
    })
    mount.setSubRoot('/github/repos', 'absent')
    let listedGithub = false
    const originalListTree = mount.listTree.bind(mount)
    mount.listTree = async (prefix: string) => {
      if (prefix === '/github/repos') listedGithub = true
      return originalListTree(prefix)
    }
    const factory = createFactory(config(), {
      mount,
      fleet: new FakeFleetClient(),
      triage: new StaticTriage(),
    })

    await factory.runOnce()

    expect(listedGithub).toBe(false)
    expect(mount.writes).toEqual([])
    expect(factory.status().counters.githubIssueMirrorsCreated ?? 0).toBe(0)
  })

  it('rejects a human-authored [factory] issue under a stricter [factory-e2e] gate but accepts GitHub mirrors', async () => {
    expect(isInFactoryScope(
      { title: '[factory] human issue', team: 'AR', raw: { payload: { title: '[factory] human issue', team: { key: 'AR' } } } },
      { requireTitlePrefix: '[factory-e2e]', requireTeamKey: 'AR' },
    )).toBe(false)

    expect(isInFactoryScope(
      {
        title: '[factory] github mirror',
        team: 'AR',
        raw: { payload: { title: '[factory] github mirror', team: { key: 'AR' }, source: { provider: 'github' } } },
      },
      { requireTitlePrefix: '[factory-e2e]', requireTeamKey: 'AR' },
    )).toBe(true)

    // The configured prefix still matches its own boundary.
    expect(isInFactoryScope(
      { title: '[factory-e2e] human issue', team: 'AR', raw: { payload: { title: '[factory-e2e] human issue', team: { key: 'AR' } } } },
      { requireTitlePrefix: '[factory-e2e]', requireTeamKey: 'AR' },
    )).toBe(true)
  })

  it('uses canonical issue state during startup backfill when a ready alias is stale', async () => {
    const mount = new FakeMountClient({
      [issuePath(69)]: realIssueFile(69, done),
      [readyAliasPath(69)]: realIssueFile(69, ready),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start()

    expect(fleet.spawns).toEqual([])
    expect(factory.status().inFlight).toEqual([])
    await factory.stop()
  })

  it('dedupes canonical and ready alias occurrences while dispatching genuinely ready issues', async () => {
    const mount = new FakeMountClient({
      [issuePath(68)]: realIssueFile(68, ready),
      [readyAliasPath(68)]: realIssueFile(68, ready),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled).toEqual([{ uuid: 'uuid-68', key: 'AR-68', path: issuePath(68) }])
    expect(report.dispatched.map((result) => result.issue)).toEqual([
      { uuid: 'uuid-68', key: 'AR-68', path: issuePath(68) },
    ])
    expect(report.skipped).toEqual([])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-68-impl', 'ar-68-review'])
  })

  it('resolves live-shaped ready alias discovery to the canonical issue record', async () => {
    const canonicalPath = '/linear/issues/AR-70__uuid-70-canonical.json'
    const mount = new FakeMountClient({
      [canonicalPath]: realIssueFile(70, ready, { id: 'uuid-70-canonical' }),
      [readyAliasPath(70)]: realIssueFile(70, ready, { id: 'uuid-70-alias' }),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled).toEqual([{ uuid: 'uuid-70-canonical', key: 'AR-70', path: canonicalPath }])
    expect(report.dispatched.map((result) => result.issue)).toEqual([
      { uuid: 'uuid-70-canonical', key: 'AR-70', path: canonicalPath },
    ])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-70-impl', 'ar-70-review'])
  })

  it('fails closed when a ready alias has no canonical issue record', async () => {
    const mount = new FakeMountClient({
      [readyAliasPath(71)]: realIssueFile(71, ready),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled).toEqual([])
    expect(report.dispatched).toEqual([])
    expect(fleet.spawns).toEqual([])
    expect(factory.status().counters.readyAliasesWithoutCanonical).toBe(1)
  })

  it('discovers only canonical key-shaped issue records from captured live listing shapes', async () => {
    const byIdBareAliasPath = '/linear/issues/by-id/AR-214.json'
    const byIdCanonicalShapedAliasPath = '/linear/issues/by-id/AR-214__uuid-214.json'
    const mount = new ListingReadTrackingMount({
      [capturedReadyCanaryPath]: realIssueFile(133, ready, { id: 'dac27fce-e8de-4910-bbf6-98ad436df3dd' }),
      [readyAliasPath(133)]: realIssueFile(133, ready, { id: 'dac27fce-e8de-4910-bbf6-98ad436df3dd' }),
      [capturedStaleDoneCanonicalPath]: realIssueFile(173, done, { id: '40c7e780-59ad-47ee-8809-3a9b8434d8fb' }),
      [capturedStaleReadyAliasPath]: realIssueFile(173, ready, { id: '40c7e780-59ad-47ee-8809-3a9b8434d8fb' }),
      [byIdBareAliasPath]: realIssueFile(214, ready),
      [byIdCanonicalShapedAliasPath]: realIssueFile(214, ready),
    }, [
      capturedBareUuidPhantomPath,
      '/linear/issues/dac27fce-e8de-4910-bbf6-98ad436df3dd.json',
    ])
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    const report = await factory.runOnce()

    expect(report.pulled.map((issue) => issue.path)).toEqual([
      capturedReadyCanaryPath,
      capturedStaleDoneCanonicalPath,
    ])
    expect(report.dispatched.map((result) => result.issue.path)).toEqual([capturedReadyCanaryPath])
    expect(report.skipped).toContainEqual({
      issue: { uuid: '40c7e780-59ad-47ee-8809-3a9b8434d8fb', key: 'AR-173', path: capturedStaleDoneCanonicalPath },
      reason: 'live state is not ready-for-agent',
    })
    expect(mount.readPaths).not.toContain(byIdBareAliasPath)
    expect(mount.readPaths).not.toContain(byIdCanonicalShapedAliasPath)
    expect(mount.readPaths).not.toContain(capturedBareUuidPhantomPath)
    expect(mount.readPaths).not.toContain('/linear/issues/dac27fce-e8de-4910-bbf6-98ad436df3dd.json')
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-133-impl', 'ar-133-review'])
  })

  it('skips missing canonical-shaped issue paths without aborting discovery', async () => {
    const missingCanonicalPath = '/linear/issues/ZZ-404__00000000-0000-4000-8000-000000000404.json'
    const debugLogs: unknown[][] = []
    const mount = new ListingReadTrackingMount({
      [capturedReadyCanaryPath]: realIssueFile(133, ready, { id: 'dac27fce-e8de-4910-bbf6-98ad436df3dd' }),
    }, [missingCanonicalPath])
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      logger: {
        debug: (...args: unknown[]) => debugLogs.push(args),
        warn: () => undefined,
        error: () => undefined,
      },
    })

    const report = await factory.runOnce()

    expect(report.pulled.map((issue) => issue.path)).toEqual([capturedReadyCanaryPath])
    expect(mount.readPaths).toContain(missingCanonicalPath)
    expect(factory.status().counters.phantomSkipped).toBe(1)
    expect(debugLogs).toContainEqual([
      '[factory] skipped missing issue file discovered from issue tree',
      { path: missingCanonicalPath },
    ])
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

  it('start live writes and refreshes a running loop heartbeat, then marks stopping on stop', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-live-heartbeat-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const clock = new ManualClock()
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 1_000 },
      }), {
        mount: new FakeMountClient(),
        fleet: new FakeFleetClient(),
        triage: new StaticTriage(),
        clock,
      })

      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })

      const initial = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(initial).toMatchObject({
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAtMs: 0,
        registryPath,
      })
      expect(checkFactoryLoopLiveness(initial, { nowMs: 900, staleMs: 1_000 })).toMatchObject({
        ok: true,
        stale: false,
      })

      clock.advance(500)
      await new Promise((resolve) => setTimeout(resolve, 650))

      const refreshed = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(refreshed).toMatchObject({
        status: 'running',
        updatedAtMs: 500,
      })

      await factory.stop()

      const stopped = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(stopped).toMatchObject({
        status: 'stopping',
        updatedAtMs: 500,
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps the live heartbeat fresh while draining a blocking live event burst', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-live-heartbeat-burst-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    const staleMs = 1_000
    const issueCount = 60
    try {
      const clock = new ManualClock()
      const files = Object.fromEntries(
        Array.from({ length: issueCount }, (_, index) => {
          const n = 400 + index
          return [issuePath(n), realIssueFile(n)]
        }),
      )
      const mount = new BlockingIssueReadMount(files, clock, {
        heartbeatPath,
        staleMs,
        blockMs: 20,
        advanceMs: 20,
      })
      const fleet = new FakeFleetClient()
      const factory = createFactory(config({
        batchSize: 5,
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: staleMs },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        clock,
      })

      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
      for (let index = 0; index < issueCount; index += 1) {
        const n = 400 + index
        mount.emit(changeEvent(issuePath(n), `event-live-burst-${n}`))
      }

      await vi.waitFor(() => expect(mount.readCount).toBeGreaterThanOrEqual(issueCount), { timeout: 8_000 })
      await flush()

      expect(mount.observedStaleWhileReading).toBe(false)
      await vi.waitFor(async () => {
        const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
        const ageMs = clock.now() - (heartbeat?.updatedAtMs ?? 0)
        expect(ageMs).toBeLessThan(staleMs / 2)
      }, { timeout: 2_000 })
      expect(factory.status().counters.liveEventDrainYields).toBeGreaterThan(0)
      expect(fleet.spawns.length).toBeGreaterThan(0)

      await factory.stop()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('drains live issue reads with bounded parallelism before yielding to the next batch', async () => {
    const issueCount = 6
    const files = Object.fromEntries(
      Array.from({ length: issueCount }, (_, index) => {
        const n = 470 + index
        return [issuePath(n), realIssueFile(n)]
      }),
    )
    const mount = new DelayedIssueReadMount(files, 25)
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ batchSize: 5 }), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    for (let index = 0; index < issueCount; index += 1) {
      const n = 470 + index
      mount.emit(changeEvent(issuePath(n), `event-live-parallel-${n}`))
    }

    await vi.waitFor(() => expect(mount.readCount).toBeGreaterThanOrEqual(issueCount), { timeout: 5_000 })
    await vi.waitFor(() => expect(factory.status().counters.liveEventDrainYields).toBeGreaterThan(0), { timeout: 5_000 })

    expect(mount.maxConcurrentIssueReads).toBeGreaterThan(1)
    expect(mount.maxConcurrentIssueReads).toBeLessThanOrEqual(5)
    await factory.stop()
  })

  it('start live marks the heartbeat stopping before releasing in-flight agents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-live-heartbeat-stop-order-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(62)]: issueFile(62) })
      const heartbeatStatusesAtRelease: string[] = []
      class HeartbeatObservingFleetClient extends FakeFleetClient {
        override async release(name: string, reason?: string): Promise<void> {
          heartbeatStatusesAtRelease.push((await readFactoryLoopHeartbeat(heartbeatPath))?.status ?? 'missing')
          await super.release(name, reason)
        }
      }
      const fleet = new HeartbeatObservingFleetClient()
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
      })

      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(62), issueFile(62))))
      await factory.stop()

      expect(heartbeatStatusesAtRelease).toEqual(['stopping', 'stopping'])
      expect((await readFactoryInFlightRegistry(registryPath))?.agents).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps stopping heartbeat fresh between progressing multi-agent teardown steps', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-live-heartbeat-stop-progress-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    const staleMs = 1_000
    const clock = new ManualClock()
    const pids = new Map([
      ['ar-363-impl', 36_301],
      ['ar-363-review', 36_302],
    ])
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    try {
      const mount = new FakeMountClient({ [issuePath(363)]: issueFile(363) })
      const reaperReports: Awaited<ReturnType<typeof reapFactoryOrphansOnce>>[] = []
      const kill = (pid: number, signal?: NodeJS.Signals | 0): boolean => {
        killed.push({ pid, signal })
        return true
      }
      const processIdentityReader = async (pid: number) => {
        const agentName = [...pids.entries()].find(([, candidatePid]) => candidatePid === pid)?.[0]
        return agentName
          ? { pid, startTime: `start-${pid}`, cmdline: `node ${agentName} worker` }
          : undefined
      }
      class ReaperObservingFleetClient extends CapturedPidFleetClient {
        override async release(name: string, reason?: string): Promise<void> {
          if (this.releases.length === 1) {
            reaperReports.push(await reapFactoryOrphansOnce({
              heartbeatPath,
              registryPath,
              staleMs,
              nowMs: clock.now(),
              termGraceMs: 0,
              kill,
              readChildPids: async () => [],
              readProcessIdentity: processIdentityReader,
            }))
          }
          await super.release(name, reason)
          if (this.releases.length === 1) {
            clock.advance(staleMs + 100)
          }
        }
      }
      const fleet = new ReaperObservingFleetClient([
        { name: 'ar-363-impl', pid: pids.get('ar-363-impl') },
        { name: 'ar-363-review', pid: pids.get('ar-363-review') },
      ])
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: staleMs },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        clock,
        terminationGraceMs: 0,
        readChildPids: async () => [],
        kill,
        processFinder: async (agentName) => {
          const pid = pids.get(agentName)
          return pid
            ? { status: 'found', identity: { pid, startTime: `start-${pid}`, cmdline: `node ${agentName} worker` } }
            : { status: 'missing' }
        },
        processIdentityReader,
      })

      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(363), issueFile(363))))
      await factory.stop()

      expect(reaperReports).toEqual([{ stale: false, reason: 'loop stopping', reaped: [], skipped: [] }])
      expect(fleet.releases.map((release) => release.name)).toEqual(['ar-363-impl', 'ar-363-review'])
      expect((await readFactoryLoopHeartbeat(heartbeatPath))?.updatedAtMs).toBe(clock.now())
      expect((await readFactoryInFlightRegistry(registryPath))?.agents).toEqual([])
      expect(killed.some((entry) => entry.signal === 'SIGTERM')).toBe(true)
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
      const harness = new RosterPidHarnessClient()
      harness.pidsByName.set('ar-62-impl', 9_000)
      harness.pidsByName.set('ar-62-review', 9_001)
      const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })
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

      expect(harness.spawned).toHaveLength(2)
      expect(harness.spawned.every((spawn) => spawn.name.startsWith('ar-62-'))).toBe(true)
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

  it('persists registry agent names when broker PID registration is still pending', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-loop-registry-pending-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(63)]: issueFile(63) })
      const harness = new RosterPidHarnessClient()
      const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        processIdentityReader: async () => undefined,
      })

      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(63), issueFile(63))))

      const registry = await readFactoryInFlightRegistry(registryPath)
      expect(registry?.agents).toMatchObject([
        { name: 'ar-63-impl', pids: [], processes: [] },
        { name: 'ar-63-review', pids: [], processes: [] },
      ])
      await factory.stop()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('hands spawned agents to the durable reaper registry when dispatch writeback fails', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-registry-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(72)]: issueFile(72) })
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-72-impl', 'session-ar-72-impl')
      fleet.setSessionRef('ar-72-review', 'session-ar-72-review')
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('Live state changed before writeback')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async () => undefined,
      })

      await expect(factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(72), issueFile(72)))))
        .rejects.toThrow('Live state changed before writeback')

      expect(factory.status().inFlight).toEqual([])
      expect(factory.status().counters.dispatchFailureReaperHandoffs).toBe(1)
      const registry = await readFactoryInFlightRegistry(registryPath)
      expect(registry?.agents).toMatchObject([
        {
          name: 'ar-72-impl',
          role: 'implementer',
          sessionRef: 'session-ar-72-impl',
          issue: { key: 'AR-72', uuid: 'uuid-72', path: issuePath(72) },
          pids: [],
          processes: [],
        },
        {
          name: 'ar-72-review',
          role: 'reviewer',
          sessionRef: 'session-ar-72-review',
          issue: { key: 'AR-72', uuid: 'uuid-72', path: issuePath(72) },
          pids: [],
          processes: [],
        },
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('preserves dispatch-failure handoffs through abandon and stop-time registry rewrites', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-loop-registry-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(76)]: issueFile(76) })
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-76-impl', 'session-ar-76-impl')
      fleet.setSessionRef('ar-76-review', 'session-ar-76-review')
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, maxConsecutiveFailures: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async () => undefined,
      })

      await factory.runLoop()

      expect(factory.status().inFlight).toEqual([])
      const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(heartbeat).toMatchObject({ status: 'idle', registryPath })
      const registry = await readFactoryInFlightRegistry(registryPath)
      expect(registry?.agents).toMatchObject([
        { name: 'ar-76-impl', sessionRef: 'session-ar-76-impl', pids: [], processes: [] },
        { name: 'ar-76-review', sessionRef: 'session-ar-76-review', pids: [], processes: [] },
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('runLoop catches spawned-then-inject failures, reaps the handoff, advances heartbeat, and continues', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-loop-registry-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(76)]: issueFile(76) })
      const clock = new ManualClock()
      const alive = new Set([7_601, 7_602, 7_603, 7_604])
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const fleet = new InjectFailingPidFleetClient([
        { name: 'ar-76-impl', sessionRef: 'session-ar-76-impl', pid: 7_601 },
        { name: 'ar-76-review', sessionRef: 'session-ar-76-review', pid: 7_603 },
      ])
      const factory = createFactory(config({
        loop: { maxIterations: 2, maxConsecutiveFailures: 2, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        processIdentityReader: async (pid) => {
          if (pid === 7_601) return { pid, startTime: 'start-7601', cmdline: 'node --agent-name ar-76-impl launcher' }
          if (pid === 7_603) return { pid, startTime: 'start-7603', cmdline: 'node --agent-name ar-76-review launcher' }
          return undefined
        },
        readChildPids: async (pid) => {
          if (pid === 7_601) return [7_602]
          if (pid === 7_603) return [7_604]
          return []
        },
        clock,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          if (signal === 'SIGKILL') alive.delete(pid)
          return true
        },
        terminationGraceMs: 0,
      })

      const reports = await factory.runLoop()

      expect(reports).toHaveLength(2)
      expect(reports[0]?.error?.message).toContain('recipient unavailable')
      expect(reports[1]?.error).toBeUndefined()
      expect(factory.status().inFlight).toEqual([])
      expect(factory.status().counters.loopIterationFailures).toBe(1)
      expect(factory.status().counters.loopDispatchFailureHandoffsReaped).toBe(2)
      expect([...alive]).toEqual([])
      expect(killed.map((entry) => entry.pid)).toEqual(expect.arrayContaining([7_602, 7_601, 7_604, 7_603]))
      const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
      expect(heartbeat).toMatchObject({ status: 'idle', iteration: 2, maxIterations: 2, registryPath })
      const registry = await readFactoryInFlightRegistry(registryPath)
      expect(registry?.agents).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('runLoop catch reaps only failed handoffs and preserves healthy in-flight agents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-no-overreap-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({
        [issuePath(76)]: issueFile(76),
        [issuePath(77)]: issueFile(77),
      })
      const clock = new ManualClock()
      const healthyPids = new Set([7_601, 7_603])
      const failedPids = new Set([7_701, 7_703])
      const alive = new Set([7_601, 7_602, 7_603, 7_604, 7_701, 7_702, 7_703, 7_704])
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const healthyAliveDuringFailedReap: boolean[] = []
      const fleet = new SelectiveInjectFailingPidFleetClient([
        { name: 'ar-76-impl', sessionRef: 'session-ar-76-impl', pid: 7_601 },
        { name: 'ar-76-review', sessionRef: 'session-ar-76-review', pid: 7_603 },
        { name: 'ar-77-impl', sessionRef: 'session-ar-77-impl', pid: 7_701 },
        { name: 'ar-77-review', sessionRef: 'session-ar-77-review', pid: 7_703 },
      ], 'ar-77-')
      const factory = createFactory(config({
        batchSize: 4,
        loop: { maxIterations: 2, maxConsecutiveFailures: 2, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        processIdentityReader: async (pid) => {
          if (pid === 7_601) return { pid, startTime: 'start-7601', cmdline: 'node --agent-name ar-76-impl launcher' }
          if (pid === 7_603) return { pid, startTime: 'start-7603', cmdline: 'node --agent-name ar-76-review launcher' }
          if (pid === 7_701) return { pid, startTime: 'start-7701', cmdline: 'node --agent-name ar-77-impl launcher' }
          if (pid === 7_703) return { pid, startTime: 'start-7703', cmdline: 'node --agent-name ar-77-review launcher' }
          return undefined
        },
        readChildPids: async (pid) => {
          if (pid === 7_601) return [7_602]
          if (pid === 7_603) return [7_604]
          if (pid === 7_701) return [7_702]
          if (pid === 7_703) return [7_704]
          return []
        },
        clock,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (failedPids.has(pid) && signal !== 0) {
            healthyAliveDuringFailedReap.push([...healthyPids].every((healthyPid) => alive.has(healthyPid)))
          }
          if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          if (signal === 'SIGKILL') alive.delete(pid)
          return true
        },
        terminationGraceMs: 0,
      })

      const reports = await factory.runLoop()

      expect(reports).toHaveLength(2)
      expect(reports[0]?.error?.message).toContain('recipient unavailable')
      expect(healthyAliveDuringFailedReap.length).toBeGreaterThan(0)
      expect(healthyAliveDuringFailedReap.every(Boolean)).toBe(true)
      const firstHealthyKill = killed.findIndex((entry) => healthyPids.has(entry.pid) && entry.signal !== 0)
      const firstFailedKill = killed.findIndex((entry) => failedPids.has(entry.pid) && entry.signal !== 0)
      expect(firstFailedKill).toBeGreaterThanOrEqual(0)
      expect(firstHealthyKill).toBeGreaterThan(firstFailedKill)
      expect(factory.status().counters.loopDispatchFailureHandoffsReaped).toBe(2)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('drops dispatch-failure handoffs only after they remain unresolved past the TTL', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-stale-unresolved-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(78)]: issueFile(78) })
      const clock = new ManualClock()
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-78-impl', 'session-ar-78-impl')
      fleet.setSessionRef('ar-78-review', 'session-ar-78-review')
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          clock.advance(5 * 60_000)
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, maxConsecutiveFailures: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        clock,
        processFinder: async () => ({ status: 'ambiguous' }),
        processIdentityReader: async () => undefined,
      })

      await factory.runLoop()

      expect(factory.status().counters.agentTerminateMissingPid).toBe(2)
      expect(factory.status().counters.dispatchFailureReaperHandoffsDroppedStaleUnresolved).toBe(2)
      expect((await readFactoryInFlightRegistry(registryPath))?.agents).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('retains dispatch-failure handoffs that still resolve to protected live pids', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-protected-retained-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(79)]: issueFile(79) })
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-79-impl', 'session-ar-79-impl')
      fleet.setSessionRef('ar-79-review', 'session-ar-79-review')
      const resolvingFleet = fleet as FakeFleetClient & {
        resolveAgentPid: (name: string) => Promise<{ status: 'found'; pid: number } | { status: 'unresolved' }>
        protectedPids: () => Promise<number[]>
      }
      resolvingFleet.resolveAgentPid = async (name: string) => {
        if (name === 'ar-79-impl') return { status: 'found', pid: 7_901 }
        if (name === 'ar-79-review') return { status: 'found', pid: 7_903 }
        return { status: 'unresolved' }
      }
      resolvingFleet.protectedPids = async () => [7_901, 7_903]
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, maxConsecutiveFailures: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async () => undefined,
      })

      await factory.runLoop()

      expect(factory.status().counters.dispatchFailureReaperHandoffsDroppedStaleUnresolved).toBeUndefined()
      expect(factory.status().counters.loopDispatchFailureHandoffsReaped).toBeUndefined()
      expect((await readFactoryInFlightRegistry(registryPath))?.agents.map((agent) => agent.name).sort()).toEqual([
        'ar-79-impl',
        'ar-79-review',
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('retains slow-to-register dispatch-failure handoffs within the TTL and reaps them once registered', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-slow-register-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({
        [issuePath(80)]: issueFile(80),
        [issuePath(81)]: issueFile(81),
      })
      const clock = new ManualClock()
      const fleet = new FakeFleetClient()
      for (const name of ['ar-80-impl', 'ar-80-review', 'ar-81-impl', 'ar-81-review']) {
        fleet.setSessionRef(name, `session-${name}`)
      }
      const resolveAttempts = new Map<string, number>()
      const resolvingFleet = fleet as FakeFleetClient & {
        resolveAgentPid: (name: string) => Promise<{ status: 'found'; pid: number }>
      }
      resolvingFleet.resolveAgentPid = async (name: string) => {
        const attempts = (resolveAttempts.get(name) ?? 0) + 1
        resolveAttempts.set(name, attempts)
        if (name === 'ar-80-impl') {
          if (attempts === 1) throw new Error('broker registration pending')
          return { status: 'found', pid: 8_001 }
        }
        if (name === 'ar-80-review') {
          if (attempts === 1) throw new Error('broker registration pending')
          return { status: 'found', pid: 8_003 }
        }
        throw new Error('broker registration pending')
      }
      const alive = new Set([8_001, 8_002, 8_003, 8_004])
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        batchSize: 4,
        loop: { maxIterations: 2, maxConsecutiveFailures: 2, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        clock,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async (pid) => {
          if (pid === 8_001) return { pid, startTime: 'start-8001', cmdline: 'node --agent-name ar-80-impl launcher' }
          if (pid === 8_003) return { pid, startTime: 'start-8003', cmdline: 'node --agent-name ar-80-review launcher' }
          return undefined
        },
        readChildPids: async (pid) => {
          if (pid === 8_001) return [8_002]
          if (pid === 8_003) return [8_004]
          return []
        },
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          if (signal === 'SIGKILL') alive.delete(pid)
          return true
        },
        terminationGraceMs: 0,
      })

      await factory.runLoop()

      expect(factory.status().counters.dispatchFailureReaperHandoffsDroppedStaleUnresolved).toBeUndefined()
      expect(factory.status().counters.loopDispatchFailureHandoffsReaped).toBe(2)
      expect(killed.map((entry) => entry.pid)).toEqual(expect.arrayContaining([8_002, 8_001, 8_004, 8_003]))
      expect((await readFactoryInFlightRegistry(registryPath))?.agents.map((agent) => agent.name).sort()).toEqual([
        'ar-81-impl',
        'ar-81-review',
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reaper consumes dispatch-failure handoff by resolving name-only agents without touching protected pids', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-reap-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(73)]: issueFile(73) })
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-73-impl', 'session-ar-73-impl')
      fleet.setSessionRef('ar-73-review', 'session-ar-73-review')
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 1_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async () => undefined,
      })

      await expect(factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(73), issueFile(73)))))
        .rejects.toThrow('setState 404')
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 1,
        maxIterations: 1,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        registryPath,
      }), 'utf8')

      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const alive = new Set([7_301, 7_302, 7_303, 68_009])
      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_500,
        termGraceMs: 0,
        fleet: {
          protectedPids: async () => [68_009],
          resolveAgentPid: async (name) => {
            if (name === 'ar-73-impl') return { status: 'found', pid: 7_301 }
            if (name === 'ar-73-review') return { status: 'found', pid: 68_009 }
            return { status: 'unresolved' }
          },
        },
        processFinder: async () => ({ status: 'missing' }),
        readProcessIdentity: async (pid) => {
          if (pid === 7_301) return { pid, startTime: 'start-7301', cmdline: 'node --agent-name ar-73-impl launcher' }
          if (pid === 68_009) return { pid, startTime: 'broker-start', cmdline: 'node --agent-name ar-73-review broker' }
          return undefined
        },
        readParentPid: async () => undefined,
        readChildPids: async (pid) => pid === 7_301 ? [7_302, 7_303] : [],
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          if (signal === 'SIGKILL') alive.delete(pid)
          return true
        },
      })

      expect(report.reaped.map((entry) => entry.pid)).toEqual([7_302, 7_303, 7_301])
      expect(killed.some((entry) => entry.pid === 68_009 && entry.signal !== 0)).toBe(false)
      expect(report.skipped).toContainEqual({ pid: 68_009, reason: 'protected pid' })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('dispatch failure before spawn does not create an orphan handoff', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-before-spawn-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(74)]: issueFile(74) })
      const fleet = new SpawnFailingFleetClient()
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      }), { mount, fleet, triage: new StaticTriage() })

      await expect(factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(74), issueFile(74)))))
        .rejects.toThrow('Dispatch spawn failed for AR-74/ar-74-impl')

      expect(factory.status().inFlight).toEqual([])
      expect(factory.status().counters.dispatchFailureReaperHandoffs).toBeUndefined()
      expect(await readFactoryInFlightRegistry(registryPath)).toBeUndefined()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reaper reports unresolved dispatch-failure handoff pids instead of treating them as success', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-failure-unresolved-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const registryPath = join(root, 'registry.json')
    try {
      const mount = new FakeMountClient({ [issuePath(75)]: issueFile(75) })
      const fleet = new FakeFleetClient()
      fleet.setSessionRef('ar-75-impl', 'session-ar-75-impl')
      fleet.setSessionRef('ar-75-review', 'session-ar-75-review')
      const linear: LinearWriteback = {
        async postComment() {},
        async setState() {
          throw new Error('setState 404')
        },
        async createIssue() {
          throw new Error('not used')
        },
        async verify() {
          return true
        },
      }
      const factory = createFactory(config({
        loop: { maxIterations: 1, heartbeatPath, registryPath, heartbeatStaleMs: 1_000 },
      }), {
        mount,
        fleet,
        triage: new StaticTriage(),
        linear,
        processFinder: async () => ({ status: 'missing' }),
        processIdentityReader: async () => undefined,
      })

      await expect(factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(75), issueFile(75)))))
        .rejects.toThrow('setState 404')
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 1,
        maxIterations: 1,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        registryPath,
      }), 'utf8')

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_500,
        fleet: { resolveAgentPid: async () => ({ status: 'unresolved' }) },
        processFinder: async () => ({ status: 'missing' }),
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([
        { reason: 'pid missing for ar-75-impl' },
        { reason: 'pid missing for ar-75-review' },
      ])
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

    expect(report.pulled).toEqual([])
    expect(report.skipped).toEqual([])
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
    const fleet = new CapturedPidFleetClient([
      { name: 'ar-60-impl', sessionRef: 'session-901969', pid: 901969 },
      { name: 'ar-60-review', sessionRef: 'session-902338', pid: 902338 },
    ])
    const children = new Map<number, number[]>([
      [901969, [901970]],
      [902338, [902339]],
    ])
    const alive = new Set([901969, 901970, 902338, 902339])
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async (pid) => children.get(pid) ?? [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') alive.delete(pid)
        return true
      },
    })
    await fleet.spawn({ name: 'external-worker', capability: 'spawn:codex', task: 'external', model: 'codex' })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(60), issueFile(60))))
    await factory.stop()

    expect(fleet.releases).toEqual([
      { name: 'ar-60-impl', reason: 'factory-stopped' },
      { name: 'ar-60-review', reason: 'factory-stopped' },
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
      902339,
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGKILL').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
      902339,
    ])
    expect(alive).toEqual(new Set())
  })

  it('stop terminates trees using roster PID fallback when spawn ack omits pid', async () => {
    const mount = new FakeMountClient({ [issuePath(63)]: issueFile(63) })
    const harness = new RosterPidHarnessClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/work/pear' })
    const brokerParentPid = 68009
    const children = new Map<number, number[]>([
      [901969, [901970, brokerParentPid]],
      [902338, [902339]],
    ])
    const alive = new Set([brokerParentPid, 901969, 901970, 902338, 902339])
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async (pid) => children.get(pid) ?? [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') alive.delete(pid)
        return true
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(63), issueFile(63))))
    expect(await harness.listAgents()).toEqual([
      { name: 'ar-63-impl', pid: undefined },
      { name: 'ar-63-review', pid: undefined },
    ])
    harness.pidsByName.set('ar-63-impl', 901969)
    harness.pidsByName.set('ar-63-review', 902338)
    await factory.stop()

    expect(harness.spawned).toHaveLength(2)
    expect(harness.releases).toEqual([
      { name: 'ar-63-impl', reason: 'factory-stopped' },
      { name: 'ar-63-review', reason: 'factory-stopped' },
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
      902339,
    ])
    expect(killed.some((entry) => entry.pid === brokerParentPid)).toBe(false)
    expect(alive).toEqual(new Set([brokerParentPid]))
  })

  it('stop discovers child pids before releasing broker sessions', async () => {
    const mount = new FakeMountClient({ [issuePath(66)]: issueFile(66) })
    const fleet = new CapturedPidFleetClient([
      { name: 'ar-66-impl', sessionRef: 'session-901969', pid: 901969 },
      { name: 'ar-66-review', sessionRef: 'session-902338', pid: 902338 },
    ])
    const released = new Set<string>()
    const originalRelease = fleet.release.bind(fleet)
    fleet.release = async (name, reason) => {
      released.add(name)
      await originalRelease(name, reason)
    }
    const children = new Map<number, { agent: string; pids: number[] }>([
      [901969, { agent: 'ar-66-impl', pids: [901970] }],
      [902338, { agent: 'ar-66-review', pids: [902339] }],
    ])
    const alive = new Set([901969, 901970, 902338, 902339])
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async (pid) => {
        const child = children.get(pid)
        return child && !released.has(child.agent) ? child.pids : []
      },
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') alive.delete(pid)
        return true
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(66), issueFile(66))))
    await factory.stop()

    expect(fleet.releases).toEqual([
      { name: 'ar-66-impl', reason: 'factory-stopped' },
      { name: 'ar-66-review', reason: 'factory-stopped' },
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
      902339,
    ])
    expect(alive).toEqual(new Set())
  })

  it('stop reports missing terminate roots instead of silently certifying a no-op', async () => {
    const mount = new FakeMountClient({ [issuePath(65)]: issueFile(65) })
    const fleet = new CapturedPidFleetClient([
      { name: 'ar-65-impl', sessionRef: 'session-ar-65-impl' },
      { name: 'ar-65-review', sessionRef: 'session-ar-65-review' },
    ])
    const errors: unknown[][] = []
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async () => [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        return true
      },
      logger: {
        error: (...args: unknown[]) => errors.push(args),
        warn: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(65), issueFile(65))))
    await factory.stop()

    expect(fleet.releases).toEqual([
      { name: 'ar-65-impl', reason: 'factory-stopped' },
      { name: 'ar-65-review', reason: 'factory-stopped' },
    ])
    expect(killed).toEqual([])
    expect(factory.status().counters.agentTerminateMissingPid).toBe(2)
    expect(errors).toEqual([
      ['[factory] no pid available to terminate ar-65-impl during stop', expect.objectContaining({ agentName: 'ar-65-impl' })],
      ['[factory] no pid available to terminate ar-65-review during stop', expect.objectContaining({ agentName: 'ar-65-review' })],
    ])
  })

  it('stop falls back to a ps-discovered agent process when broker PID is unresolved', async () => {
    const mount = new FakeMountClient({ [issuePath(67)]: issueFile(67) })
    const fleet = new UnresolvedPidFleetClient()
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      processFinder: async (agentName) => ({
        status: 'found',
        identity: {
          pid: agentName === 'ar-67-impl' ? 906700 : 906701,
          startTime: `start-${agentName}`,
          cmdline: `node --agent-name ${agentName}`,
        },
      }),
      readChildPids: async () => [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        return true
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(67), issueFile(67))))
    await factory.stop()

    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      906700,
      906701,
    ])
    expect(factory.status().counters.agentTerminateMissingPid).toBeUndefined()
  })

  it('stop uses the anchored launcher root even when the broker resolves a worker child', async () => {
    const mount = new FakeMountClient({ [issuePath(69)]: issueFile(69) })
    const fleet = new FoundPidFleetClient(new Map([
      ['ar-69-impl', 906910],
      ['ar-69-review', 906911],
    ]))
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const children = new Map<number, number[]>([
      [906900, [906910]],
      [906901, [906911]],
    ])
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      processFinder: async (agentName) => ({
        status: 'found',
        identity: {
          pid: agentName === 'ar-69-impl' ? 906900 : 906901,
          startTime: `launcher-${agentName}`,
          cmdline: `node --agent-name ${agentName} launcher`,
        },
      }),
      readChildPids: async (pid) => children.get(pid) ?? [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        return true
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(69), issueFile(69))))
    await factory.stop()

    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid)).toEqual([
      906910,
      906900,
      906911,
      906901,
    ])
    expect(factory.status().counters.agentTerminateMissingPid).toBeUndefined()
  })

  it('stop treats unresolved broker PID with no ps match as process-less', async () => {
    const mount = new FakeMountClient({ [issuePath(68)]: issueFile(68) })
    const fleet = new UnresolvedPidFleetClient()
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const errors: unknown[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      processFinder: async () => ({ status: 'missing' }),
      readChildPids: async () => [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        return true
      },
      logger: {
        error: (...args: unknown[]) => errors.push(args),
        warn: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(68), issueFile(68))))
    await factory.stop()

    expect(killed).toEqual([])
    expect(factory.status().counters.agentTerminateMissingPid).toBeUndefined()
    expect(errors).toEqual([])
  })

  it('stop does not count an already-exited agent as a missing live PID', async () => {
    const mount = new FakeMountClient({ [issuePath(66)]: issueFile(66) })
    const harness = new RosterPidHarnessClient()
    harness.pidsByName.set('ar-66-impl', 906600)
    harness.pidsByName.set('ar-66-review', 906601)
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const errors: unknown[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async () => [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        return true
      },
      logger: {
        error: (...args: unknown[]) => errors.push(args),
        warn: () => undefined,
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(66), issueFile(66))))
    harness.agents.clear()
    harness.pidsByName.clear()
    await factory.stop()

    expect(killed).toEqual([])
    expect(factory.status().counters.agentTerminateMissingPid).toBeUndefined()
    expect(errors).toEqual([])
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

  it('stop bounds a hanging live subscription unsubscribe and continues shutdown', async () => {
    vi.useFakeTimers()
    const mount = new HangingUnsubscribeMount()
    const fleet = new FakeFleetClient()
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
    let stopped = false
    try {
      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
      expect(mount.activeSubscriptions).toBe(1)

      const stop = factory.stop().then(() => {
        stopped = true
      })
      await mount.unsubscribeStartedPromise
      await vi.advanceTimersByTimeAsync(2_499)
      await Promise.resolve()
      expect(stopped).toBe(false)

      await vi.advanceTimersByTimeAsync(1)
      await stop

      expect(stopped).toBe(true)
      expect(mount.unsubscribeStarted).toBe(1)
      expect(warnings).toEqual([
        [
          '[factory] factory subscription unsubscribe timed out after 2500ms; continuing shutdown and allowing the server-side subscription to expire',
          { timeoutMs: 2_500 },
        ],
      ])
    } finally {
      mount.releaseUnsubscribe()
      await Promise.resolve()
      vi.useRealTimers()
    }
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

  it('suppresses duplicate live event identities within a parallel drain batch', async () => {
    const path = issuePath(18)
    const mount = new FakeMountClient({ [path]: realIssueFile(18) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const event = changeEvent(path, 'event-duplicate-same-id')

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    mount.emit(event)
    mount.emit(event)

    await vi.waitFor(() => expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-18-impl', 'ar-18-review']))
    expect(factory.status().counters.liveDuplicateEventsSuppressed).toBe(1)
    await factory.stop()
  })

  it('does not double-dispatch the same issue from concurrent live events', async () => {
    const path = issuePath(19)
    const mount = new DelayedIssueReadMount({ [path]: realIssueFile(19) }, 25)
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    mount.emit(changeEvent(path, 'event-same-issue-a'))
    mount.emit(changeEvent(path, 'event-same-issue-b'))

    await vi.waitFor(() => expect(mount.readCount).toBeGreaterThanOrEqual(2))
    await vi.waitFor(() => expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-19-impl', 'ar-19-review']))

    expect(factory.status().counters.liveDuplicateIssueEventsSuppressed).toBe(1)
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-19'])
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

  it('live subscription runs a startup full pull when the high-watermark route is unavailable', async () => {
    const path = issuePath(40)
    const mount = new RouteNotFoundCountingListTreeMount({ [path]: realIssueFile(40) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })

    expect(mount.listTreePrefixes).toEqual(['/github/repos', '/linear/issues', '/linear/issues/by-state/ready-for-agent/'])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-40-impl', 'ar-40-review'])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-40'])
    expect(factory.status().counters.liveHighWatermarkUnavailable).toBe(1)
    expect(factory.status().counters.liveHighWatermarkFullPullFallbacks).toBe(1)
    await factory.stop()
  })

  it('live subscription skips the startup full pull when a high-watermark is present', async () => {
    const path = issuePath(41)
    const mount = new CountingListTreeMount({ [path]: realIssueFile(41) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    mount.emit(changeEvent(path, 'event-before-start-41'))

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })

    expect(mount.listTreePrefixes).toEqual([])
    expect(fleet.spawns).toEqual([])
    expect(factory.status().counters.liveHighWatermarkFullPullFallbacks).toBeUndefined()
    await factory.stop()
  })

  it('does not re-dispatch a startup-pulled issue from a later live event', async () => {
    const path = issuePath(42)
    const mount = new RouteNotFoundCountingListTreeMount({ [path]: realIssueFile(42) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    mount.emit(changeEvent(path, 'event-after-start-42'))
    await flush()

    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-42-impl', 'ar-42-review'])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-42'])
    expect(factory.status().counters.liveDuplicateIssueEventsSuppressed).toBe(1)
    await factory.stop()
  })

  it('keeps the daemon up when the startup full pull throws', async () => {
    const mount = new RouteNotFoundThrowingPullMount({})
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })

    await expect(
      factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } }),
    ).resolves.toBeUndefined()

    expect(factory.status().counters.liveHighWatermarkFullPullFallbacks).toBe(1)
    expect(factory.status().counters.liveHighWatermarkFullPullErrors).toBe(1)
    await factory.stop()
  })

  it('dispatches an issue that arrives via a live event during the startup full pull', async () => {
    const pulledPath = issuePath(50)
    const arrivedPath = issuePath(51)
    const mount = new ArrivesDuringPullMount({
      [pulledPath]: realIssueFile(50),
      [arrivedPath]: realIssueFile(51),
    })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    mount.onFirstListTree = () => mount.emit(changeEvent(arrivedPath, 'arrived-during-pull-51'))

    await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
    await flush()

    const names = fleet.spawns.map((spawn) => spawn.name)
    expect(names).toContain('ar-50-impl') // dispatched by the startup full pull
    expect(names).toContain('ar-51-impl') // captured via the buffered live event during the pull
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
    await vi.waitFor(() => expect(fleet.spawns.map((spawn) => spawn.name)).toEqual([
      'ar-39-impl',
      'ar-39-review',
      'ar-38-impl',
      'ar-38-review',
    ]))
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

  it('omits implementer and reviewer models from default heuristic dispatch spawns', async () => {
    const routedIssue = realIssueFile(66, ready, {
      labels: [{ name: 'pear' }],
      labelIds: [],
      description: 'Implement the requested fix in packages/factory-sdk/src/orchestrator/factory.ts, add regression tests, verify the dispatch path, and ensure the spawned implementer and reviewer omit default model flags while preserving all factory safety checks.',
    })
    const mount = new FakeMountClient({ [issuePath(66)]: routedIssue })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), { mount, fleet })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(66), routedIssue))

    expect(decision.implementers[0]?.model).toBeUndefined()
    expect(decision.reviewer.model).toBeUndefined()

    await factory.dispatch(decision)

    expect(fleet.spawns).toHaveLength(2)
    expect(fleet.spawns[0]).toMatchObject({
      name: 'ar-66-impl',
      capability: 'spawn:codex',
    })
    expect(fleet.spawns[0]!.model).toBeUndefined()
    expect(fleet.spawns[1]).toMatchObject({
      name: 'ar-66-review',
      capability: 'spawn:claude',
    })
    expect(fleet.spawns[1]!.model).toBeUndefined()
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

  it('completes an implementer exit without resuming when a PR already exists', async () => {
    const issue = realIssueFile(254, ready, { title: 'Real implementer PR exit terminal' })
    const mount = new FakeMountClient({ [issuePath(254)]: issue })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-254-impl', 'session-impl-254')
    const probedIssues: string[] = []
    const factory = createFactory(config({
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrResolver: async (issue) => {
        probedIssues.push(issue.key)
        return { repo: 'AgentWorkforce/pear', prNumber: 254 }
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(254), issue))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-254-impl', 'worker_exited')

    await vi.waitFor(() => expect(factory.status().counters.done).toBe(1))

    expect(probedIssues).toEqual(['AR-254'])
    expect(fleet.resumes).toEqual([])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-254-impl', 'ar-254-review'])
    expect(fleet.releases).toEqual([
      { name: 'ar-254-impl', reason: 'issue-done' },
      { name: 'ar-254-review', reason: 'issue-done' },
    ])
    expect(factory.status().inFlight).toEqual([])
  })

  it('does not loop when resume hits a leaked broker name ("already exists")', async () => {
    const mount = new FakeMountClient({ [issuePath(80)]: issueFile(80) })
    const fleet = new ResumeNameCollisionFleetClient()
    fleet.setSessionRef('ar-80-review', 'session-review-80')
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage() })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(80), issueFile(80)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-80-review', 'crash')
    await flush()
    // A second exit event must NOT trigger another resume attempt.
    fleet.emitAgentExit('ar-80-review', 'crash')
    await flush()

    expect(fleet.resumes).toHaveLength(1) // resumed once, collided, then short-circuited
    expect(factory.status().counters.resumeNameCollisions).toBe(1)
    expect(factory.status().counters.errors ?? 0).toBe(0) // not surfaced as a hard error
  })

  it('does not complete on an implementer exit when only a draft PR exists', async () => {
    const issue = realIssueFile(256, ready, { title: 'Real implementer draft PR exit' })
    const mount = new FakeMountClient({ [issuePath(256)]: issue })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-256-impl', 'session-impl-256')
    const probedIssues: string[] = []
    const factory = createFactory(config({
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrResolver: async (issue) => {
        probedIssues.push(issue.key)
        return { repo: 'AgentWorkforce/pear', prNumber: 256, draft: true }
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(256), issue))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-256-impl', 'worker_exited')
    await flush()

    // A draft PR is not a completion signal: no done, no release; the exit falls
    // through to the normal resume path (mirrors the no-PR case).
    expect(probedIssues).toEqual(['AR-256'])
    expect(factory.status().counters.done).toBeUndefined()
    expect(fleet.releases).toEqual([])
    expect(fleet.resumes).toEqual([{
      name: 'ar-256-impl',
      sessionRef: 'session-impl-256',
      node: 'self',
      capability: 'spawn:codex',
    }])
  })

  it('still resumes an abnormally exited implementer when no PR exists yet', async () => {
    const issue = realIssueFile(255, ready, { title: 'Real implementer crash resumes' })
    const mount = new FakeMountClient({ [issuePath(255)]: issue })
    const fleet = new FakeFleetClient()
    fleet.setSessionRef('ar-255-impl', 'session-impl-255')
    const probedIssues: string[] = []
    const factory = createFactory(config({
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrResolver: async (issue) => {
        probedIssues.push(issue.key)
        return undefined
      },
    })
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(255), issue))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-255-impl', 'crash')
    await flush()

    expect(probedIssues).toEqual(['AR-255'])
    expect(fleet.resumes).toEqual([{
      name: 'ar-255-impl',
      sessionRef: 'session-impl-255',
      node: 'self',
      capability: 'spawn:codex',
    }])
    expect(fleet.releases).toEqual([])
    expect(factory.status().counters.done).toBeUndefined()
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

  it('retries confirmed task injection when the spawned agent is not registered yet', async () => {
    const clock = new ManualClock()
    const mount = new FakeMountClient({ [issuePath(67)]: issueFile(67) })
    const fleet = new LagThenInjectedFleetClient()
    const factory = createFactory(config(), { mount, fleet, triage: new StaticTriage(), clock })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(67), issueFile(67))))

    expect(fleet.injectionAttempts).toBe(3)
    expect(factory.status().counters.injectionRegistrationLagRetries).toBe(1)
    expect(fleet.messages.map((message) => message.to)).toEqual([
      'ar-67-impl',
      'ar-67-impl',
      'ar-67-review',
    ])
    expect(fleet.inputs).toEqual([
      { name: 'ar-67-impl', data: '\r' },
      { name: 'ar-67-review', data: '\r' },
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
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>> = []
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

    expect(closeInputs).toEqual([{
      repo: 'AgentWorkforce/pear',
      prNumber: 18,
      expectedIssueKey: 'AR-18',
      requireTitleMarker: false,
    }])
    expect(order).toEqual([
      'linear-done',
      'slack-root',
      'slack-reply',
      'probe-close',
      'release:ar-18-impl',
      'release:ar-18-review',
    ])
  })

  it('completion releases and terminates tracked pair process trees', async () => {
    const mount = new FakeMountClient({ [issuePath(64)]: issueFile(64) })
    const fleet = new CapturedPidFleetClient([
      { name: 'ar-64-impl', sessionRef: 'session-901969', pid: 901969 },
      { name: 'ar-64-review', sessionRef: 'session-902338', pid: 902338 },
    ])
    const children = new Map<number, number[]>([[901969, [901970]]])
    const alive = new Set([901969, 901970, 902338])
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      terminationGraceMs: 0,
      readChildPids: async (pid) => children.get(pid) ?? [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!alive.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') alive.delete(pid)
        return true
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(64), issueFile(64))))
    fleet.emitAgentExit('ar-64-impl', 'issue-done')
    await vi.waitFor(() => expect(killed.filter((entry) => entry.signal === 'SIGKILL')).toHaveLength(3))

    expect(fleet.releases).toEqual([
      { name: 'ar-64-impl', reason: 'issue-done' },
      { name: 'ar-64-review', reason: 'issue-done' },
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
    ])
    expect(killed.filter((entry) => entry.signal === 'SIGKILL').map((entry) => entry.pid).sort((a, b) => a - b)).toEqual([
      901969,
      901970,
      902338,
    ])
    expect(alive).toEqual(new Set())
  })

  it('PR-state sweep completes wedged synthetic issues, frees batch slots, and dispatches queued work', async () => {
    const mount = new FakeMountClient({
      [issuePath(351)]: issueFile(351),
      [issuePath(352)]: issueFile(352),
      [issuePath(353)]: issueFile(353),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/351.json': prFile(351, {
        title: 'Add isOdd factory SDK util',
        body: 'Linear: AR-351',
        head_ref: 'ar-351-is-odd-v11b',
      }),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/352.json': prFile(352, {
        title: 'AR-352: add square utility',
        body: '',
        head_ref: 'square-util',
      }),
    })
    const fleet = new FakeFleetClient()
    for (const n of [351, 352, 353]) {
      fleet.setSessionRef(`ar-${n}-impl`, `session-ar-${n}-impl`)
      fleet.setSessionRef(`ar-${n}-review`, `session-ar-${n}-review`)
    }
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>> = []
    const factory = createFactory(config({ batchSize: 2 }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.runOnce()
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-351', 'AR-352'])
    expect(factory.status().queued.map((issue) => issue.key)).toEqual(['AR-353'])

    for (const n of [351, 352]) {
      fleet.emitAgentExit(`ar-${n}-review`, 'worker_exited')
      await flush()
      fleet.emitAgentExit(`ar-${n}-review`, 'worker_exited')
      await flush()
    }

    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-351', 'AR-352'])
    expect(fleet.spawns.map((spawn) => spawn.name)).not.toContain('ar-353-impl')

    await factory.runLoop({ maxIterations: 1 })

    expect(closeInputs).toEqual([
      { repo: 'AgentWorkforce/pear', prNumber: 351, expectedIssueKey: 'AR-351', requireTitleMarker: false },
      { repo: 'AgentWorkforce/pear', prNumber: 352, expectedIssueKey: 'AR-352', requireTitleMarker: false },
    ])
    expect(fleet.releases.filter((release) => release.reason === 'issue-done').map((release) => release.name)).toEqual([
      'ar-351-impl',
      'ar-351-review',
      'ar-352-impl',
      'ar-352-review',
    ])
    expect(fleet.spawns.map((spawn) => spawn.name)).toContain('ar-353-impl')
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-353'])
    expect(factory.status().queued).toEqual([])
    expect(factory.status().counters.completionSweepCompleted).toBe(2)

    await factory.runLoop({ maxIterations: 1 })
    expect(closeInputs).toHaveLength(2)
    expect(factory.status().counters.done).toBe(2)
  })

  it('live mode runs the PR-state completion sweep timer', async () => {
    vi.useFakeTimers()
    try {
      const mount = new FakeMountClient({
        [issuePath(241)]: issueFile(241),
        '/github/repos/AgentWorkforce__pear/pulls/by-id/241.json': prFile(241, {
          title: 'Add live completion sweep coverage',
          body: '',
          head_ref: 'ar-241-live-completion-sweep',
        }),
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

      await factory.start({ mode: 'live', liveSubscription: { transport: 'subscribe' } })
      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(241), issueFile(241))))
      await vi.advanceTimersByTimeAsync(0)

      expect(closeInputs).toEqual([{
        repo: 'AgentWorkforce/pear',
        prNumber: 241,
        expectedIssueKey: 'AR-241',
        requireTitleMarker: false,
      }])
      expect(factory.status().inFlight).toEqual([])
      await factory.stop()
    } finally {
      vi.useRealTimers()
    }
  })

  it('backfill-and-subscribe mode runs the PR-state completion sweep timer', async () => {
    vi.useFakeTimers()
    try {
      const mount = new FakeMountClient({
        [issuePath(243)]: issueFile(243),
        '/github/repos/AgentWorkforce__pear/pulls/by-id/243.json': prFile(243, {
          title: 'Add default start completion sweep coverage',
          body: '',
          head_ref: 'ar-243-default-completion-sweep',
        }),
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

      await factory.start()
      expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-243'])

      await vi.advanceTimersByTimeAsync(0)

      expect(closeInputs).toEqual([{
        repo: 'AgentWorkforce/pear',
        prNumber: 243,
        expectedIssueKey: 'AR-243',
        requireTitleMarker: false,
      }])
      expect(factory.status().inFlight).toEqual([])
      await factory.stop()
    } finally {
      vi.useRealTimers()
    }
  })

  it('coalesces concurrent PR sweep and agent-exit completion triggers', async () => {
    const mount = new FakeMountClient({
      [issuePath(354)]: issueFile(354),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/354.json': prFile(354, {
        title: 'Add idempotent completion coverage',
        body: '',
        head_ref: 'ar-354-idempotent-completion',
      }),
    })
    const fleet = new FakeFleetClient()
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>> = []
    let releaseProbeClose!: () => void
    const probeCloseBlocked = new Promise<void>((release) => {
      releaseProbeClose = release
    })
    let resolveProbeCloseStarted!: () => void
    const probeCloseStarted = new Promise<void>((resolve) => {
      resolveProbeCloseStarted = resolve
    })
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      probeCloser: async (input) => {
        closeInputs.push(input)
        resolveProbeCloseStarted()
        await probeCloseBlocked
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(354), issueFile(354))))
    const sweep = factory.runLoop({ maxIterations: 1 })
    await probeCloseStarted
    fleet.emitAgentExit('ar-354-impl', 'issue-done')
    await flush()
    releaseProbeClose()
    await sweep

    expect(closeInputs).toEqual([{
      repo: 'AgentWorkforce/pear',
      prNumber: 354,
      expectedIssueKey: 'AR-354',
      requireTitleMarker: false,
    }])
    expect(fleet.releases.filter((release) => release.reason === 'issue-done').map((release) => release.name)).toEqual([
      'ar-354-impl',
      'ar-354-review',
    ])
    expect(factory.status().counters.done).toBe(1)
    expect(factory.status().inFlight).toEqual([])
  })

  it('PR-state sweep releases a real issue under mergePolicy never without merging', async () => {
    const mount = new FakeMountClient({
      [issuePath(240)]: realMergeIssueFile(240),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/240.json': prFile(240, {
        title: 'Real product issue 240',
        body: 'Linear: AR-240',
        head_ref: 'ar-240-real-fix',
      }),
    })
    const fleet = new FakeFleetClient()
    const gate = new ScriptedGithubMergeGate([readyMergeVerdict('green-sha')])
    const factory = createFactory(config({
      mergePolicy: 'never',
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      mergeGate: gate,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(240), realMergeIssueFile(240))))
    await factory.runLoop({ maxIterations: 1 })

    expect(fleet.releases.map((release) => release.name)).toEqual(['ar-240-impl', 'ar-240-review'])
    expect(gate.checks).toEqual([])
    expect(gate.merges).toEqual([])
    expect(factory.status().inFlight).toEqual([])
    expect(mount.writes).toContainEqual({ path: issuePath(240), content: { stateId: done } })
  })

  it('PR-state sweep merges a real issue only when policy, checks, review, and head are ready', async () => {
    const mount = new FakeMountClient({
      [issuePath(242)]: realMergeIssueFile(242),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/242.json': prFile(242, {
        title: 'Real product issue 242',
        body: '',
        head_ref: 'ar-242-real-fix',
      }),
    })
    const fleet = new FakeFleetClient()
    const gate = new ScriptedGithubMergeGate([readyMergeVerdict('green-approved-sha')])
    const factory = createFactory(config({
      mergePolicy: 'on-green-with-review',
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      mergeGate: gate,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(242), realMergeIssueFile(242))))
    await factory.runLoop({ maxIterations: 1 })

    expect(gate.merges).toEqual([{
      repo: 'AgentWorkforce/pear',
      number: 242,
      expectedHeadSha: 'green-approved-sha',
    }])
    expect(factory.status().counters.mergeGateMerged).toBe(1)
    expect(factory.status().inFlight).toEqual([])
  })

  it('PR-state sweep does not complete on a wrong PR or draft PR', async () => {
    const mount = new FakeMountClient({
      [issuePath(250)]: issueFile(250),
      [issuePath(251)]: issueFile(251),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/250.json': prFile(250, {
        title: 'Unrelated cleanup',
        body: 'This merely mentions ar-250, but is not the issue PR.',
        head_ref: 'docs-cleanup',
      }),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/251.json': prFile(251, {
        title: 'AR-251: draft work',
        body: '',
        head_ref: 'ar-251-draft-work',
        isDraft: true,
      }),
    })
    const fleet = new FakeFleetClient()
    const closeInputs: unknown[] = []
    const factory = createFactory(config({ batchSize: 2 }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.runOnce()
    await factory.runLoop({ maxIterations: 1 })

    expect(closeInputs).toEqual([])
    expect(fleet.releases.filter((release) => release.reason === 'issue-done')).toEqual([])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-250', 'AR-251'])
    expect(factory.status().counters.completionSweepCompleted).toBeUndefined()
    expect(factory.status().counters.completionSweepMissingPr).toBe(1)
    expect(factory.status().counters.completionSweepDraftPr).toBe(1)
  })

  it('PR-state sweep resolves fresh PRs through gh when the mount is missing them', async () => {
    const mount = new FakeMountClient({
      [issuePath(355)]: issueFile(355),
      [issuePath(356)]: issueFile(356),
      [issuePath(357)]: issueFile(357),
    })
    const fleet = new FakeFleetClient()
    const closeInputs: Array<Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>> = []
    const ghCalls: string[][] = []
    const factory = createFactory(config({ batchSize: 2 }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrGhRunner: async (args) => {
        ghCalls.push(args)
        return {
          stdout: JSON.stringify([
            ghPr(880, {
              title: 'Fix PR-state completion sweep',
              body: 'Regression fixture mentions AR-355 but is not its PR.',
              headRefName: 'factory-sdk-pr-state-completion-sb-impl3',
              state: 'OPEN',
            }),
            ghPr(855, {
              title: 'Add old isOdd util',
              body: '',
              headRefName: 'ar-355-is-odd-v1',
              state: 'CLOSED',
            }),
            ghPr(856, {
              title: 'Add fresh isOdd util',
              body: '',
              headRefName: 'ar-355-is-odd-v2',
              state: 'OPEN',
            }),
            ghPr(857, {
              title: 'AR-356: add square utility',
              body: '',
              headRefName: 'ar-356-square',
              state: 'OPEN',
            }),
          ]),
        }
      },
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.runOnce()
    await factory.runLoop({ maxIterations: 1 })

    expect(ghCalls).toHaveLength(2)
    expect(ghCalls.every((args) => args.includes('--repo') && args.includes('AgentWorkforce/pear'))).toBe(true)
    expect(closeInputs).toEqual([
      { repo: 'AgentWorkforce/pear', prNumber: 856, expectedIssueKey: 'AR-355', requireTitleMarker: false },
      { repo: 'AgentWorkforce/pear', prNumber: 857, expectedIssueKey: 'AR-356', requireTitleMarker: false },
    ])
    expect(fleet.spawns.map((spawn) => spawn.name)).toContain('ar-357-impl')
    expect(factory.status().counters.probePrGhResolveHits).toBe(2)
  })

  it('gh PR fallback rejects fuzzy over-matches and numeric-prefix collisions', async () => {
    const mount = new FakeMountClient({ [issuePath(229)]: issueFile(229) })
    const fleet = new FakeFleetClient()
    const closeInputs: unknown[] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrGhRunner: async () => ({
        stdout: JSON.stringify([
          ghPr(287, {
            title: 'Add PR-state completion sweep',
            body: 'This fix PR mentions AR-229 in tests but is not its issue PR.',
            headRefName: 'factory-sdk-pr-state-completion-sb-impl3',
            state: 'OPEN',
          }),
          ghPr(291, {
            title: 'AR-22: wrong issue',
            body: 'Linear: AR-22',
            headRefName: 'ar-22-9-not-229',
            state: 'OPEN',
          }),
          ghPr(292, {
            title: 'AR-229-1: wrong child issue',
            body: '',
            headRefName: 'ar-229-1-is-positive',
            state: 'OPEN',
          }),
          ghPr(293, {
            title: 'AR-2290: wrong prefix',
            body: '',
            headRefName: 'ar-2290-is-positive',
            state: 'OPEN',
          }),
        ]),
      }),
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(229), issueFile(229))))
    await factory.runLoop({ maxIterations: 1 })

    expect(closeInputs).toEqual([])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-229'])
    expect(factory.status().counters.completionSweepMissingPr).toBe(1)
  })

  it('gh PR fallback fails closed when gh is unavailable', async () => {
    const mount = new FakeMountClient({ [issuePath(358)]: issueFile(358) })
    const fleet = new FakeFleetClient()
    const closeInputs: unknown[] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrGhRunner: async () => {
        throw new Error('gh auth missing')
      },
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(358), issueFile(358))))
    await factory.runLoop({ maxIterations: 1 })

    expect(closeInputs).toEqual([])
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-358'])
    expect(factory.status().counters.completionSweepMissingPr).toBe(1)
    expect(factory.status().counters.done).toBeUndefined()
  })

  it('gh PR fallback backs off repeated not-found lookups', async () => {
    const clock = new ManualClock()
    const mount = new FakeMountClient({ [issuePath(361)]: issueFile(361) })
    const fleet = new FakeFleetClient()
    const ghCalls: string[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      clock,
      probePrGhRunner: async (args) => {
        ghCalls.push(args)
        return {
          stdout: JSON.stringify([
            ghPr(871, {
              title: 'Unrelated factory fix',
              body: 'Mentions AR-361 in a loose sentence only.',
              headRefName: 'factory-sdk-pr-state-completion-sb-impl3',
              state: 'OPEN',
            }),
          ]),
        }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(361), issueFile(361))))
    await factory.runLoop({ maxIterations: 1 })
    await factory.runLoop({ maxIterations: 1 })

    expect(ghCalls).toHaveLength(1)
    expect(factory.status().counters.probePrGhBackoffSkips).toBe(1)
    expect(factory.status().counters.completionSweepMissingPr).toBe(2)

    clock.advance(60_000)
    await factory.runLoop({ maxIterations: 1 })

    expect(ghCalls).toHaveLength(2)
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-361'])
    expect(factory.status().counters.done).toBeUndefined()
  })

  it('gh PR fallback skips draft PRs and backs off repeated unresolved lookups', async () => {
    const clock = new ManualClock()
    const mount = new FakeMountClient({ [issuePath(359)]: issueFile(359) })
    const fleet = new FakeFleetClient()
    const ghCalls: string[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      clock,
      probePrGhRunner: async (args) => {
        ghCalls.push(args)
        return {
          stdout: JSON.stringify([
            ghPr(859, {
              title: 'AR-359: draft work',
              body: '',
              headRefName: 'ar-359-draft-work',
              isDraft: true,
              state: 'OPEN',
            }),
          ]),
        }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(359), issueFile(359))))
    await factory.runLoop({ maxIterations: 1 })
    await factory.runLoop({ maxIterations: 1 })
    expect(ghCalls).toHaveLength(1)
    expect(factory.status().counters.probePrGhBackoffSkips).toBe(1)
    expect(factory.status().counters.completionSweepDraftPr).toBe(1)

    clock.advance(60_000)
    await factory.runLoop({ maxIterations: 1 })
    expect(ghCalls).toHaveLength(2)
    expect(factory.status().inFlight.map((issue) => issue.key)).toEqual(['AR-359'])
  })

  it('treats already-closed gh-resolved probe PRs as completed instead of re-wedging', async () => {
    const mount = new FakeMountClient({ [issuePath(360)]: issueFile(360) })
    const fleet = new FakeFleetClient()
    const closeViewCalls: string[][] = []
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probePrGhRunner: async () => ({
        stdout: JSON.stringify([
          ghPr(860, {
            title: 'Add already closed probe work',
            body: '',
            headRefName: 'ar-360-closed-work',
            state: 'CLOSED',
          }),
        ]),
      }),
      probeCloser: (input) => closeProbePr({
        ...input,
        runner: async (args) => {
          closeViewCalls.push(args)
          return {
            stdout: JSON.stringify({
              state: 'CLOSED',
              title: 'Add already closed probe work',
              body: '',
              headRefName: 'ar-360-closed-work',
            }),
          }
        },
      }),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(360), issueFile(360))))
    await factory.runLoop({ maxIterations: 1 })

    expect(closeViewCalls).toHaveLength(1)
    expect(closeViewCalls[0]).toContain('view')
    expect(fleet.releases.map((release) => release.reason)).toEqual(['issue-done', 'issue-done'])
    expect(factory.status().inFlight).toEqual([])
    expect(factory.status().counters.done).toBe(1)
    expect(factory.status().counters.errors).toBeUndefined()
  })

  it('closes synthetic probe PRs even when real auto-merge is enabled', async () => {
    const markedMount = new FakeMountClient({ [issuePath(19)]: issueFile(19) })
    const markedFleet = new FakeFleetClient()
    const markedCalls: unknown[] = []
    const gate = new ScriptedGithubMergeGate([readyMergeVerdict('synthetic-sha')])
    const markedFactory = createFactory(config({ mergePolicy: 'on-green-with-review' }), {
      mount: markedMount,
      fleet: markedFleet,
      triage: new StaticTriage(),
      mergeGate: gate,
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 19 }),
      probeCloser: async (input) => {
        markedCalls.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })
    await markedFactory.dispatch(await markedFactory.triageIssue(parseLinearIssue(issuePath(19), issueFile(19))))
    markedFleet.emitAgentExit('ar-19-impl', 'issue-done')
    await flush()
    expect(markedCalls).toEqual([{
      repo: 'AgentWorkforce/pear',
      prNumber: 19,
      expectedIssueKey: 'AR-19',
      requireTitleMarker: false,
    }])
    expect(gate.checks).toEqual([])
    expect(gate.merges).toEqual([])
  })

  it('does not merge a real PR while a required check is red', async () => {
    const mount = new FakeMountClient({ [issuePath(20)]: realMergeIssueFile(20) })
    const fleet = new FakeFleetClient()
    const clock = new ManualClock()
    const gate = new ScriptedGithubMergeGate([
      refusedMergeVerdict('checks not merge-ready: FAILURE'),
    ])
    const factory = createFactory(config({
      mergePolicy: 'on-green-with-review',
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      mergeGate: gate,
      clock,
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 20 }),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(20), realMergeIssueFile(20))))
    fleet.emitAgentExit('ar-20-impl', 'issue-done')

    await vi.waitFor(() => expect(fleet.releases.map((release) => release.name)).toEqual(['ar-20-impl', 'ar-20-review']))
    expect(gate.checks).toHaveLength(12)
    expect(gate.merges).toEqual([])
  })

  it('aborts a real PR merge when the guarded head commit has drifted', async () => {
    const mount = new FakeMountClient({ [issuePath(21)]: realMergeIssueFile(21) })
    const fleet = new FakeFleetClient()
    const gate = new ScriptedGithubMergeGate([
      readyMergeVerdict('green-approved-sha'),
    ], { merged: false, reason: 'Head commit changed' })
    const factory = createFactory(config({
      mergePolicy: 'on-green-with-review',
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      mergeGate: gate,
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 21 }),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(21), realMergeIssueFile(21))))
    fleet.emitAgentExit('ar-21-impl', 'issue-done')

    await vi.waitFor(() => expect(fleet.releases.map((release) => release.name)).toEqual(['ar-21-impl', 'ar-21-review']))
    expect(gate.merges).toEqual([{
      repo: 'AgentWorkforce/pear',
      number: 21,
      expectedHeadSha: 'green-approved-sha',
    }])
    expect(factory.status().counters.mergeGateMergeAborted).toBe(1)
    expect(factory.status().counters.mergeGateMerged).toBeUndefined()
  })

  it('merges a real PR once checks are green, review is approved, and head still matches', async () => {
    const mount = new FakeMountClient({ [issuePath(22)]: realMergeIssueFile(22) })
    const fleet = new FakeFleetClient()
    const gate = new ScriptedGithubMergeGate([
      readyMergeVerdict('green-approved-sha'),
    ])
    const factory = createFactory(config({
      mergePolicy: 'on-green-with-review',
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      linear: stateOnlyLinear(mount),
      mergeGate: gate,
      probePrResolver: async () => ({ repo: 'AgentWorkforce/pear', prNumber: 22 }),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(22), realMergeIssueFile(22))))
    fleet.emitAgentExit('ar-22-impl', 'issue-done')

    await vi.waitFor(() => expect(fleet.releases.map((release) => release.name)).toEqual(['ar-22-impl', 'ar-22-review']))
    expect(gate.merges).toEqual([{
      repo: 'AgentWorkforce/pear',
      number: 22,
      expectedHeadSha: 'green-approved-sha',
    }])
    expect(factory.status().counters.mergeGateMerged).toBe(1)
  })

  it('closes synthetic probe PRs with real-agent body, title, or branch issue references', async () => {
    const cases = [
      {
        n: 235,
        pr: {
          number: 277,
          title: 'Add isOdd factory SDK util',
          body: 'Linear: AR-235',
          head_ref: 'feature/is-odd',
        },
      },
      {
        n: 236,
        pr: {
          number: 278,
          title: 'AR-236: add square utility',
          body: '',
          head_ref: 'square-util',
        },
      },
      {
        n: 229,
        pr: {
          number: 279,
          title: 'Add isPositive util',
          body: '',
          head_ref: 'ar-229-is-positive',
        },
      },
    ]

    for (const entry of cases) {
      const mount = new FakeMountClient({
        [issuePath(entry.n)]: issueFile(entry.n),
        [`/github/repos/AgentWorkforce__pear/pulls/by-id/${entry.pr.number}.json`]: {
          provider: 'github',
          objectType: 'pull_request',
          objectId: String(entry.pr.number),
          payload: entry.pr,
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

      await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(entry.n), issueFile(entry.n))))
      fleet.emitAgentExit(`ar-${entry.n}-impl`, 'issue-done')
      await flush()

      expect(closeInputs).toEqual([{
        repo: 'AgentWorkforce/pear',
        prNumber: entry.pr.number,
        expectedIssueKey: `AR-${entry.n}`,
        requireTitleMarker: false,
      }])
    }
  })

  it('does not close PRs for non-synthetic issues', async () => {
    const mount = new FakeMountClient({
      [issuePath(230)]: realMergeIssueFile(230),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/230.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '230',
        payload: {
          number: 230,
          title: 'Real product fix',
          body: 'Linear: AR-230',
          head_ref: 'ar-230-real-fix',
        },
      },
    })
    const fleet = new FakeFleetClient()
    const closeInputs: unknown[] = []
    const factory = createFactory(config({
      safety: { requireTitlePrefix: 'Real', requireTeamKey: 'AR' },
    }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      probeCloser: async (input) => {
        closeInputs.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(230), realMergeIssueFile(230))))
    fleet.emitAgentExit('ar-230-impl', 'issue-done')
    await flush()

    expect(closeInputs).toEqual([])
  })

  it('prefers the exact branch-convention PR over prefix collisions and loose body mentions', async () => {
    const mount = new FakeMountClient({
      [issuePath(229)]: issueFile(229),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/991.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '991',
        payload: {
          number: 991,
          title: 'Unrelated documentation change',
          body: 'This merely mentions ar-229 in passing, but is not the issue PR.',
          head_ref: 'feature/docs-cleanup',
        },
      },
      '/github/repos/AgentWorkforce__pear/pulls/by-id/993.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '993',
        payload: {
          number: 993,
          title: 'AR-229: title match but not branch match',
          body: '',
          head_ref: 'feature/title-match',
        },
      },
      '/github/repos/AgentWorkforce__pear/pulls/by-id/994.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '994',
        payload: {
          number: 994,
          title: 'Body-only match',
          body: 'Linear: AR-229',
          head_ref: 'feature/body-match',
        },
      },
      '/github/repos/AgentWorkforce__pear/pulls/by-id/992.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '992',
        payload: {
          number: 992,
          title: 'AR-22: wrong issue',
          body: 'Linear: AR-22',
          head_ref: 'ar-22-9-not-229',
        },
      },
      '/github/repos/AgentWorkforce__pear/pulls/by-id/279.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '279',
        payload: {
          number: 279,
          title: 'Add isPositive util',
          body: '',
          head_ref: 'ar-229-is-positive',
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
    const decision = await factory.triageIssue(parseLinearIssue(issuePath(229), issueFile(229)))

    await factory.dispatch(decision)
    fleet.emitAgentExit('ar-229-impl', 'issue-done')
    await flush()

    expect(closeInputs).toEqual([{
      repo: 'AgentWorkforce/pear',
      prNumber: 279,
      expectedIssueKey: 'AR-229',
      requireTitleMarker: false,
    }])
    expect(fleet.releases.map((release) => release.name)).toEqual(['ar-229-impl', 'ar-229-review'])
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

  it('posts low-confidence and thin triage escalation to Slack with reason and question', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(20)]: issueFile(20) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new EscalatingTriage({ rationale: 'No repository route matched.' }),
      slack,
    })

    const report = await factory.runOnce()

    expect(report.dispatched).toEqual([])
    expect(report.skipped).toContainEqual({ issue: { uuid: 'uuid-20', key: 'AR-20', path: issuePath(20) }, reason: 'queued or escalated' })
    expect(fleet.spawns).toEqual([])
    const slackRoots = mount.writes.filter((write) => isSlackRootWritePath(write.path))
    expect(slackRoots).toHaveLength(1)
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('AR-20: factory triage escalation for [factory-e2e] Fix factory issue 20')
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('Reason: low-confidence triage and thin issue context: No repository route matched.')
    expect((slackRoots[0]?.content as { text?: string }).text).toContain('Question: Please clarify')
    expect(slack.roots).toEqual([])
  })

  it('ignores a human Slack thread reply after the issue has no in-flight implementer', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(21)]: issueFile(21) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(21), issueFile(21))))
    fleet.emitAgentExit('ar-21-impl', 'issue-done')
    await flush()
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-after-done'), 'slack-human-after-done', {
      text: 'please add one more test',
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(factory.status().inFlight).toEqual([])
    expect(slackAnswerInputs(fleet)).toEqual([])
  })

  it('does not wire Slack answer injection when Slack is unconfigured', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(22)]: issueFile(22) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(22), issueFile(22))))
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', '1780751622.222222', 'human-unconfigured'), 'slack-human-unconfigured', {
      text: 'please use Slack answer injection',
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackAnswerInputs(fleet)).toEqual([])
  })

  it('watches the in-flight factory Slack thread and routes a human reply to the implementer', async () => {
    const mount = new ConfirmRecordingSlackMountClient({ [issuePath(24)]: issueFile(24) })
    const fleet = new FakeFleetClient()
    const slack = new RecordingSlack()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
      slack,
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(24), issueFile(24))))
    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', slack.threadId, 'human-1'), 'slack-human-1', {
      text: 'Please use the existing retry helper.',
      user: 'U123',
      user_name: 'human',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-24-impl', data: 'Slack reply for AR-24:\nPlease use the existing retry helper.\r' },
    ])
    expect(slack.replies).toEqual([])
    expect(slackReplyWrites(mount)).toEqual([])
    expect(mount.confirmedPaths.filter((path) => path.includes('/replies/'))).toEqual([])
  })

  it('routes a mid-task agent question to the Slack dispatch thread and returns the human answer via sendInput', async () => {
    const mount = new ConfirmRecordingSlackMountClient({ [issuePath(36)]: issueFile(36) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(36), issueFile(36))))
    fleet.emitAgentMessage({
      from: 'ar-36-impl',
      target: 'broker',
      body: 'FACTORY_NEEDS_INPUT\nIssue: AR-36\nQuestion: Which retry helper should I use?',
      eventId: 'agent-question-36',
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([
      expect.objectContaining({
        content: expect.objectContaining({
          thread_ts: mount.threadTs,
          text: 'AR-36: ar-36-impl needs input.\nQuestion: Which retry helper should I use?',
        }),
      }),
    ])
    expect(factory.status().counters.agentQuestionsPostedToSlack).toBe(1)
    expect(slackAnswerInputs(fleet)).toEqual([])

    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', mount.threadTs, 'bot-question-echo'), 'bot-question-echo', {
      text: 'AR-36: ar-36-impl needs input.\nQuestion: Which retry helper should I use?',
      user: 'U0B2596R7EZ',
      user_is_bot: false,
    })
    await flush()
    await flush()
    expect(slackAnswerInputs(fleet)).toEqual([])

    emitSlackReply(mount, slackReplyFixturePath('C0FACTORY__factory-e2e', mount.threadTs, 'human-answer-36'), 'human-answer-36', {
      text: 'Use the shared retry helper in factory.ts.',
      user: 'U123',
      user_is_bot: false,
    })
    await flush()
    await flush()

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-36-impl', data: 'Slack reply for AR-36:\nUse the shared retry helper in factory.ts.\r' },
    ])
    expect(fleet.messages).toHaveLength(2)
  })

  it('ignores marked agent questions that are not addressed to the factory', async () => {
    const mount = new ConfirmRecordingSlackMountClient({ [issuePath(37)]: issueFile(37) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(37), issueFile(37))))
    fleet.emitAgentMessage({
      from: 'ar-37-impl',
      target: 'ar-37-review',
      body: 'FACTORY_NEEDS_INPUT\nIssue: AR-37\nQuestion: should not bridge',
      eventId: 'agent-question-37',
    })
    await flush()
    await flush()

    expect(slackReplyWrites(mount)).toEqual([])
    expect(factory.status().counters.agentQuestionsPostedToSlack).toBeUndefined()
  })

  it('treats agent questions as no-ops when Slack is unconfigured without regressing dispatch', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(38)]: issueFile(38) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config(), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    const result = await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(38), issueFile(38))))
    fleet.emitAgentMessage({
      from: 'ar-38-impl',
      target: 'broker',
      body: 'FACTORY_NEEDS_INPUT\nIssue: AR-38\nQuestion: can anyone clarify?',
      eventId: 'agent-question-38',
    })
    await flush()
    await flush()

    expect(result.agents.map((agent) => agent.name)).toEqual(['ar-38-impl', 'ar-38-review'])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-38-impl', 'ar-38-review'])
    expect(slackReplyWrites(mount)).toEqual([])
    expect(factory.status().counters.agentQuestionsSkippedNoSlack).toBe(1)
  })

  it('treats agent questions as no-ops while Slack sync is degraded without regressing dispatch', async () => {
    const mount = new SlackSyncStatusMount({ [issuePath(39)]: issueFile(39) })
    mount.slackStatus = { provider: 'slack', status: 'stale' }
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })

    const result = await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(39), issueFile(39))))
    fleet.emitAgentMessage({
      from: 'ar-39-impl',
      target: 'factory',
      body: '[factory-needs-input] Can anyone clarify the expected retry behavior?',
      eventId: 'agent-question-39',
    })
    await flush()
    await flush()

    expect(result.agents.map((agent) => agent.name)).toEqual(['ar-39-impl', 'ar-39-review'])
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['ar-39-impl', 'ar-39-review'])
    expect(mount.writes.filter((write) => isSlackRootWritePath(write.path) || write.path.includes('/replies/'))).toEqual([])
    expect(factory.status().counters.agentQuestionsSkippedSlackDegraded).toBe(1)
  })

  it('dedupes duplicate agent question events before posting to Slack', async () => {
    const mount = new ConfirmRecordingSlackMountClient({ [issuePath(40)]: issueFile(40) })
    const fleet = new FakeFleetClient()
    const factory = createFactory(config({ slack: slackConfig() }), {
      mount,
      fleet,
      triage: new StaticTriage(),
    })
    const question = {
      from: 'ar-40-impl',
      target: 'factory',
      body: 'FACTORY_NEEDS_INPUT\nIssue: AR-40\nQuestion: Is this duplicate-safe?',
      eventId: 'agent-question-40',
    }

    await factory.dispatch(await factory.triageIssue(parseLinearIssue(issuePath(40), issueFile(40))))
    fleet.emitAgentMessage(question)
    fleet.emitAgentMessage(question)
    await flush()
    await flush()

    expect(slackReplyWrites(mount).map((write) => write.content.text)).toEqual([
      'AR-40: ar-40-impl needs input.\nQuestion: Is this duplicate-safe?',
    ])
    expect(factory.status().counters.agentQuestionsPostedToSlack).toBe(1)
    expect(factory.status().counters.agentQuestionDuplicatesSuppressed).toBe(1)
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-32-impl', data: 'Slack reply for AR-32:\nstatus?\r' },
    ])
    expect(slack.replies).toEqual([])
    expect(slackReplyWrites(mount)).toEqual([])
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
    expect(slackAnswerInputs(fleet)).toEqual([])
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
    expect(slackAnswerInputs(fleet)).toEqual([])
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-35-impl', data: 'Slack reply for AR-35:\nstatus?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
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
    expect(slackAnswerInputs(fleet)).toEqual([])
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-27-impl', data: 'Slack reply for AR-27:\nnew status?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-28-impl', data: 'Slack reply for AR-28:\nstatus?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
  })

  it('dedupes Slack answer injections by human message ts across poll re-reads with fresh event ids', async () => {
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-42-impl', data: 'Slack reply for AR-42:\nstatus?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
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

  it('continues processing Slack reply events after one answer injection fails', async () => {
    const mount = new CloudWritebackFakeMountClient({ [issuePath(30)]: issueFile(30) })
    const fleet = new FailingSlackAnswerFleetClient()
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-30-impl', data: 'Slack reply for AR-30:\nstatus again?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
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

    expect(slackAnswerInputs(fleet)).toEqual([
      { name: 'ar-31-impl', data: 'Slack reply for AR-31:\nstatus?\r' },
    ])
    expect(slackReplyWrites(mount)).toEqual([])
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

const slackAnswerInputs = (fleet: FakeFleetClient): Array<{ name: string; data: string }> =>
  fleet.inputs.filter((input) => input.data !== '\r')

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
