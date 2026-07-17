import { BatchTracker } from '../orchestrator/batch-tracker'
import type {
  BatchSnapshot,
  BabysitterSessionState,
  CriticalRecord,
  DispatchLifecycle,
  DispatchLifecycleClaim,
  DispatchAttemptState,
  GithubIssueCommentWatchState,
  RegistryHandoffAgent,
  StateStore,
  WaitingClarification,
  ClarificationReply,
} from '../ports/state'

type WorkspaceState = {
  batch: BatchTracker
  criticalMessages: Map<string, CriticalRecord>
  resumedExitKeys: Set<string>
  slackThreadIds: Map<string, string>
  githubIssueCommentWatches: Map<string, GithubIssueCommentWatchState>
  seenAgentQuestionKeys: Set<string>
  seenAgentQuestionOrder: string[]
  waitingClarifications: Map<string, WaitingClarification>
  dispatchAttempts: Map<string, DispatchAttemptState>
  canonicalIssueStates: Map<string, string>
  dispatchFailureReaperHandoffs: Map<string, RegistryHandoffAgent>
  babysitterSessions: Map<string, BabysitterSessionState>
  dispatchLifecycles: Map<string, DispatchLifecycle>
}

export type InMemoryStateStoreOptions = {
  batchSize: number
  agentQuestionDedupeLimit?: number
}

export class InMemoryStateStore implements StateStore {
  readonly #batchSize: number
  readonly #agentQuestionDedupeLimit: number
  readonly #workspaces = new Map<string, WorkspaceState>()

  constructor(options: InMemoryStateStoreOptions) {
    this.#batchSize = options.batchSize
    this.#agentQuestionDedupeLimit = Math.max(1, Math.trunc(options.agentQuestionDedupeLimit ?? 500))
  }

  async getBatch(workspaceId: string): Promise<BatchSnapshot> {
    return this.#workspace(workspaceId).batch
  }

  async recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void> {
    this.#workspace(workspaceId).dispatchAttempts.set(issueKey, { ...attempt })
  }

  async getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined> {
    const attempt = this.#workspace(workspaceId).dispatchAttempts.get(issueKey)
    return attempt ? { ...attempt } : undefined
  }

  async releaseInFlight(workspaceId: string, issueKey: string): Promise<void> {
    const attempt = this.#workspace(workspaceId).dispatchAttempts.get(issueKey)
    if (attempt) {
      attempt.inFlight = false
    }
  }

  async claimDispatchLifecycle(
    workspaceId: string,
    key: string,
    seed: DispatchLifecycle,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DispatchLifecycleClaim> {
    const lifecycles = this.#workspace(workspaceId).dispatchLifecycles
    let lifecycle = lifecycles.get(key)
    const created = !lifecycle
    if (!lifecycle) {
      lifecycle = cloneDispatchLifecycle(seed)
      if (activeDispatchLifecycleCount(lifecycles) >= this.#batchSize) lifecycle.phase = 'queued'
      lifecycles.set(key, lifecycle)
    }
    const terminal = lifecycle.phase === 'complete' || lifecycle.phase === 'abandoned'
    const activeOtherOwner = lifecycle.lease && lifecycle.lease.owner !== owner && lifecycle.lease.leaseUntilMs > nowMs
    if (terminal || activeOtherOwner) {
      return { acquired: false, lifecycle: cloneDispatchLifecycle(lifecycle), created }
    }
    const epoch = lifecycle.lease?.owner === owner
      ? lifecycle.lease.epoch
      : (lifecycle.lease?.epoch ?? 0) + 1
    lifecycle.lease = { owner, epoch, leaseUntilMs: nowMs + leaseMs }
    lifecycle.updatedAtMs = nowMs
    return {
      acquired: true,
      lifecycle: cloneDispatchLifecycle(lifecycle),
      lease: { ...lifecycle.lease },
      created,
    }
  }

  async renewDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    const lifecycle = this.#workspace(workspaceId).dispatchLifecycles.get(key)
    if (!lifecycle?.lease || lifecycle.lease.owner !== owner || lifecycle.lease.epoch !== epoch) return false
    lifecycle.lease.leaseUntilMs = nowMs + leaseMs
    lifecycle.updatedAtMs = nowMs
    return true
  }

  async promoteDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
  ): Promise<boolean> {
    const lifecycles = this.#workspace(workspaceId).dispatchLifecycles
    const lifecycle = lifecycles.get(key)
    if (
      (lifecycle?.phase !== 'queued' && lifecycle?.phase !== 'waiting-for-human') ||
      !lifecycle.lease ||
      lifecycle.lease.owner !== owner ||
      lifecycle.lease.epoch !== epoch ||
      lifecycle.lease.leaseUntilMs <= nowMs ||
      activeDispatchLifecycleCount(lifecycles, key) >= this.#batchSize
    ) return false
    lifecycle.phase = 'dispatching'
    lifecycle.updatedAtMs = nowMs
    return true
  }

  async releaseDispatchLifecycleLease(workspaceId: string, key: string, owner: string, epoch: number): Promise<void> {
    const lease = this.#workspace(workspaceId).dispatchLifecycles.get(key)?.lease
    if (lease?.owner !== owner || lease.epoch !== epoch) return
    lease.leaseUntilMs = Number.MIN_SAFE_INTEGER
  }

  async saveDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    lifecycle: DispatchLifecycle,
  ): Promise<boolean> {
    const current = this.#workspace(workspaceId).dispatchLifecycles.get(key)
    if (!current?.lease || current.lease.owner !== owner || current.lease.epoch !== epoch || current.lease.leaseUntilMs <= nowMs) {
      return false
    }
    const next = cloneDispatchLifecycle(lifecycle)
    next.lease = { ...current.lease }
    next.updatedAtMs = nowMs
    this.#workspace(workspaceId).dispatchLifecycles.set(key, next)
    return true
  }

  async getDispatchLifecycle(workspaceId: string, key: string): Promise<DispatchLifecycle | undefined> {
    const lifecycle = this.#workspace(workspaceId).dispatchLifecycles.get(key)
    return lifecycle ? cloneDispatchLifecycle(lifecycle) : undefined
  }

  async listDispatchLifecycles(workspaceId: string): Promise<Array<[string, DispatchLifecycle]>> {
    return [...this.#workspace(workspaceId).dispatchLifecycles]
      .map(([key, lifecycle]) => [key, cloneDispatchLifecycle(lifecycle)])
  }

  async clearDispatchLifecycle(workspaceId: string, key: string): Promise<void> {
    this.#workspace(workspaceId).dispatchLifecycles.delete(key)
  }

  async recordCritical(workspaceId: string, key: string, value: CriticalRecord): Promise<void> {
    this.#workspace(workspaceId).criticalMessages.set(key, value)
  }

  async consumeCritical(workspaceId: string, key: string): Promise<CriticalRecord | undefined> {
    const critical = this.#workspace(workspaceId).criticalMessages.get(key)
    if (critical) {
      this.#workspace(workspaceId).criticalMessages.delete(key)
    }
    return critical
  }

  async isResumed(workspaceId: string, exitKey: string): Promise<boolean> {
    return this.#workspace(workspaceId).resumedExitKeys.has(exitKey)
  }

  async markResumed(workspaceId: string, exitKey: string): Promise<void> {
    this.#workspace(workspaceId).resumedExitKeys.add(exitKey)
  }

  async setSlackThread(workspaceId: string, issueKey: string, threadId: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.set(issueKey, threadId)
  }

  async getSlackThread(workspaceId: string, issueKey: string): Promise<string | undefined> {
    return this.#workspace(workspaceId).slackThreadIds.get(issueKey)
  }

  async clearSlackThread(workspaceId: string, issueKey: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.delete(issueKey)
  }

  async clearSlackThreads(workspaceId: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.clear()
  }

  async setGithubIssueCommentWatch(workspaceId: string, key: string, watch: GithubIssueCommentWatchState): Promise<void> {
    this.#workspace(workspaceId).githubIssueCommentWatches.set(key, cloneGithubIssueCommentWatch(watch))
  }

  async listGithubIssueCommentWatches(workspaceId: string): Promise<Array<[string, GithubIssueCommentWatchState]>> {
    return [...this.#workspace(workspaceId).githubIssueCommentWatches]
      .map(([key, watch]) => [key, cloneGithubIssueCommentWatch(watch)])
  }

  async clearGithubIssueCommentWatch(workspaceId: string, key: string): Promise<void> {
    this.#workspace(workspaceId).githubIssueCommentWatches.delete(key)
  }

  async seenAgentQuestion(workspaceId: string, key: string): Promise<boolean> {
    return this.#workspace(workspaceId).seenAgentQuestionKeys.has(key)
  }

  async markAgentQuestion(workspaceId: string, key: string): Promise<void> {
    this.#rememberAgentQuestion(this.#workspace(workspaceId), key)
  }

  async claimAgentQuestion(workspaceId: string, key: string): Promise<boolean> {
    const state = this.#workspace(workspaceId)
    if (state.seenAgentQuestionKeys.has(key)) {
      return false
    }
    this.#rememberAgentQuestion(state, key)
    return true
  }

  async reserveWaitingClarification(workspaceId: string, issueKey: string, record: WaitingClarification): Promise<boolean> {
    const waiting = this.#workspace(workspaceId).waitingClarifications
    if (waiting.has(issueKey)) return false
    waiting.set(issueKey, cloneWaitingClarification(record))
    return true
  }

  async getWaitingClarification(workspaceId: string, issueKey: string): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    return record ? cloneWaitingClarification(record) : undefined
  }

  async listWaitingClarifications(workspaceId: string): Promise<Array<[string, WaitingClarification]>> {
    return [...this.#workspace(workspaceId).waitingClarifications]
      .map(([key, record]) => [key, cloneWaitingClarification(record)])
  }

  async claimClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record || record.questionPostedAtMs !== undefined || (
      record.questionDelivery?.owner &&
      nowMs - record.questionDelivery.claimedAtMs < leaseMs
    )) return undefined
    record.questionDelivery = {
      owner,
      claimedAtMs: nowMs,
      attempts: (record.questionDelivery?.attempts ?? 0) + 1,
    }
    return cloneWaitingClarification(record)
  }

  async completeClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    postedAtMs: number,
  ): Promise<boolean> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (record?.questionDelivery?.owner !== owner) return false
    record.questionPostedAtMs = postedAtMs
    delete record.questionDelivery
    return true
  }

  async renewClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
  ): Promise<boolean> {
    const delivery = this.#workspace(workspaceId).waitingClarifications.get(issueKey)?.questionDelivery
    if (delivery?.owner !== owner) return false
    delivery.claimedAtMs = nowMs
    return true
  }

  async releaseClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    const delivery = this.#workspace(workspaceId).waitingClarifications.get(issueKey)?.questionDelivery
    if (delivery?.owner !== owner) return
    delivery.owner = ''
    delivery.claimedAtMs = Number.MIN_SAFE_INTEGER
  }

  async claimClarificationReply(
    workspaceId: string,
    issueKey: string,
    reply: ClarificationReply,
  ): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record || (record.questionPostedAtMs === undefined && !record.questionDelivery?.owner) || record.reply) {
      return undefined
    }
    record.reply = { ...reply }
    return cloneWaitingClarification(record)
  }

  async markClarificationAgentReleased(
    workspaceId: string,
    issueKey: string,
    agentName: string,
  ): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record) return undefined
    record.releasedAgents ??= []
    if (!record.releasedAgents.includes(agentName)) record.releasedAgents.push(agentName)
    return cloneWaitingClarification(record)
  }

  async claimClarificationWake(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record?.reply || record.questionPostedAtMs === undefined || record.parkedAtMs === undefined || (record.wake && record.wake.owner !== owner && nowMs - record.wake.claimedAtMs < leaseMs)) {
      return undefined
    }
    record.wake = {
      owner,
      claimedAtMs: nowMs,
      attempts: (record.wake?.attempts ?? 0) + 1,
      injectedAgents: [...(record.wake?.injectedAgents ?? [])],
    }
    return cloneWaitingClarification(record)
  }

  async markClarificationParked(workspaceId: string, issueKey: string, parkedAtMs: number): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record) return undefined
    const released = new Set(record.releasedAgents ?? [])
    if (record.agents.some(({ name }) => !released.has(name))) return undefined
    record.parkedAtMs ??= parkedAtMs
    return cloneWaitingClarification(record)
  }

  async claimClarificationEscalation(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (!record || record.reply || record.escalatedAtMs || (
      record.escalation && record.escalation.owner !== owner && nowMs - record.escalation.claimedAtMs < leaseMs
    )) return undefined
    record.escalation = {
      owner,
      claimedAtMs: nowMs,
      attempts: (record.escalation?.attempts ?? 0) + 1,
    }
    return cloneWaitingClarification(record)
  }

  async completeClarificationEscalation(
    workspaceId: string,
    issueKey: string,
    owner: string,
    escalatedAtMs: number,
  ): Promise<boolean> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (record?.escalation?.owner !== owner) return false
    record.escalatedAtMs = escalatedAtMs
    delete record.escalation
    return true
  }

  async releaseClarificationEscalation(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    const escalation = this.#workspace(workspaceId).waitingClarifications.get(issueKey)?.escalation
    if (escalation?.owner !== owner) return
    escalation.owner = ''
    escalation.claimedAtMs = Number.MIN_SAFE_INTEGER
  }

  async renewClarificationWake(workspaceId: string, issueKey: string, owner: string, nowMs: number): Promise<boolean> {
    const wake = this.#workspace(workspaceId).waitingClarifications.get(issueKey)?.wake
    if (wake?.owner !== owner) return false
    wake.claimedAtMs = nowMs
    return true
  }

  async markClarificationAgentInjected(workspaceId: string, issueKey: string, owner: string, agentName: string): Promise<boolean> {
    const wake = this.#workspace(workspaceId).waitingClarifications.get(issueKey)?.wake
    if (wake?.owner !== owner) return false
    if (!wake.injectedAgents.includes(agentName)) wake.injectedAgents.push(agentName)
    return true
  }

  async completeClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<boolean> {
    const state = this.#workspace(workspaceId)
    if (state.waitingClarifications.get(issueKey)?.wake?.owner !== owner) return false
    state.waitingClarifications.delete(issueKey)
    return true
  }

  async releaseClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    const record = this.#workspace(workspaceId).waitingClarifications.get(issueKey)
    if (record?.wake?.owner === owner) {
      record.wake.owner = ''
      record.wake.claimedAtMs = Number.MIN_SAFE_INTEGER
    }
  }

  async clearWaitingClarification(workspaceId: string, issueKey: string): Promise<void> {
    this.#workspace(workspaceId).waitingClarifications.delete(issueKey)
  }

  #rememberAgentQuestion(state: WorkspaceState, key: string): void {
    state.seenAgentQuestionKeys.add(key)
    state.seenAgentQuestionOrder.push(key)
    while (state.seenAgentQuestionOrder.length > this.#agentQuestionDedupeLimit) {
      const oldest = state.seenAgentQuestionOrder.shift()
      if (oldest) {
        state.seenAgentQuestionKeys.delete(oldest)
      }
    }
  }

  async recordFailureHandoff(workspaceId: string, key: string, handoff: RegistryHandoffAgent): Promise<void> {
    this.#workspace(workspaceId).dispatchFailureReaperHandoffs.set(key, handoff)
  }

  async getFailureHandoff(workspaceId: string, key: string): Promise<RegistryHandoffAgent | undefined> {
    return this.#workspace(workspaceId).dispatchFailureReaperHandoffs.get(key)
  }

  async listFailureHandoffs(workspaceId: string): Promise<Array<[string, RegistryHandoffAgent]>> {
    return [...this.#workspace(workspaceId).dispatchFailureReaperHandoffs]
  }

  async clearFailureHandoff(workspaceId: string, key: string): Promise<void> {
    this.#workspace(workspaceId).dispatchFailureReaperHandoffs.delete(key)
  }

  async setBabysitterSession(
    workspaceId: string,
    issueKey: string,
    session: BabysitterSessionState,
  ): Promise<void> {
    this.#workspace(workspaceId).babysitterSessions.set(issueKey, structuredClone(session))
  }

  async listBabysitterSessions(workspaceId: string): Promise<Array<[string, BabysitterSessionState]>> {
    return [...this.#workspace(workspaceId).babysitterSessions]
      .map(([key, session]) => [key, structuredClone(session)])
  }

  async clearBabysitterSession(workspaceId: string, issueKey: string): Promise<void> {
    this.#workspace(workspaceId).babysitterSessions.delete(issueKey)
  }

  async recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void> {
    this.#workspace(workspaceId).canonicalIssueStates.set(key, stateId)
  }

  async getCanonicalState(workspaceId: string, key: string): Promise<string | undefined> {
    return this.#workspace(workspaceId).canonicalIssueStates.get(key)
  }

  #workspace(workspaceId: string): WorkspaceState {
    let state = this.#workspaces.get(workspaceId)
    if (!state) {
      state = {
        batch: new BatchTracker(this.#batchSize),
        criticalMessages: new Map(),
        resumedExitKeys: new Set(),
        slackThreadIds: new Map(),
        githubIssueCommentWatches: new Map(),
        seenAgentQuestionKeys: new Set(),
        seenAgentQuestionOrder: [],
        waitingClarifications: new Map(),
        dispatchAttempts: new Map(),
        canonicalIssueStates: new Map(),
        dispatchFailureReaperHandoffs: new Map(),
        babysitterSessions: new Map(),
        dispatchLifecycles: new Map(),
      }
      this.#workspaces.set(workspaceId, state)
    }
    return state
  }
}

const cloneDispatchLifecycle = (lifecycle: DispatchLifecycle): DispatchLifecycle => structuredClone(lifecycle)

const activeDispatchLifecycleCount = (lifecycles: Map<string, DispatchLifecycle>, exceptKey?: string): number =>
  [...lifecycles].filter(([key, lifecycle]) => key !== exceptKey && dispatchLifecycleOccupiesSlot(lifecycle)).length

const dispatchLifecycleOccupiesSlot = (lifecycle: DispatchLifecycle): boolean =>
  lifecycle.phase !== 'queued' &&
  lifecycle.phase !== 'waiting-for-human' &&
  lifecycle.phase !== 'releasing' &&
  lifecycle.phase !== 'complete' &&
  lifecycle.phase !== 'abandoned'

const cloneWaitingClarification = (record: WaitingClarification): WaitingClarification =>
  structuredClone(record)

const cloneGithubIssueCommentWatch = (watch: GithubIssueCommentWatchState): GithubIssueCommentWatchState => ({
  ...watch,
  issue: { ...watch.issue },
  source: { ...watch.source },
  processedCommentIds: watch.processedCommentIds ? [...watch.processedCommentIds] : undefined,
  pending: watch.pending.map((pending) => ({
    ...pending,
    ...(pending.decision ? {
      decision: {
        ...pending.decision,
        issue: { ...pending.decision.issue },
        routes: pending.decision.routes.map((route) => ({ ...route })),
        implementers: pending.decision.implementers.map((agent) => ({ ...agent })),
        reviewer: { ...pending.decision.reviewer },
        ...(pending.decision.workflow ? { workflow: { ...pending.decision.workflow } } : {}),
      },
    } : {}),
  })),
})
