import { BatchTracker } from '../orchestrator/batch-tracker'
import type {
  BatchSnapshot,
  CriticalRecord,
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
      }
      this.#workspaces.set(workspaceId, state)
    }
    return state
  }
}

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
