import { randomUUID } from 'node:crypto'

import { BatchTracker } from '../orchestrator/batch-tracker'
import type {
  BatchSnapshot,
  BabysitterGenerationRecord,
  BabysitterSessionState,
  CriticalRecord,
  DispatchLifecycle,
  DispatchLifecycleClaim,
  DispatchAttemptState,
  GithubIssueCommentWatchState,
  RegistryHandoffAgent,
  SlackThreadWatchState,
  ConversationMessage,
  ConversationSessionState,
  DiscoveryCheckpoint,
  DiscoverySweepClaim,
  DiscoverySweepRenewal,
  DiscoverySweepState,
  StateStore,
  WaitingClarification,
  ClarificationReply,
} from '../ports/state'
import { DispatchLifecycleMigrationConflictError } from '../ports/state'
import {
  asMigrationAlias,
  isMigrationAlias,
  migrateDispatchLifecycleKeys,
  planLifecycleMigration,
  prunableMigrationAliases,
} from './work-unit-lifecycle-migration'
import { dispatchLifecycleOccupiesSlot, stampDispatchLifecycleSlot } from './dispatch-lifecycle-slot'

type WorkspaceState = {
  batch: BatchTracker
  criticalMessages: Map<string, CriticalRecord>
  resumedExitKeys: Set<string>
  slackThreadIds: Map<string, string>
  slackThreadWatches: Map<string, SlackThreadWatchState>
  conversationSessions: Map<string, ConversationSessionState>
  githubIssueCommentWatches: Map<string, GithubIssueCommentWatchState>
  seenAgentQuestionKeys: Set<string>
  seenAgentQuestionOrder: string[]
  waitingClarifications: Map<string, WaitingClarification>
  dispatchAttempts: Map<string, DispatchAttemptState>
  canonicalIssueStates: Map<string, string>
  dispatchFailureReaperHandoffs: Map<string, RegistryHandoffAgent>
  babysitterSessions: Map<string, BabysitterSessionState>
  babysitterGenerations: Map<string, BabysitterGenerationRecord>
  dispatchLifecycles: Map<string, DispatchLifecycle>
  discoverySweep: DiscoverySweepState
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

  async claimDiscoverySweep(
    workspaceId: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepClaim> {
    const state = this.#workspace(workspaceId).discoverySweep
    if (state.backoffUntilMs > nowMs) {
      return { acquired: false, reason: 'backoff', state: cloneDiscoverySweepState(state) }
    }
    if (state.lease && state.lease.leaseUntilMs > nowMs) {
      return { acquired: false, reason: 'in-flight', state: cloneDiscoverySweepState(state) }
    }
    const epoch = state.lastEpoch + 1
    state.lastEpoch = epoch
    state.lease = { owner, epoch, leaseUntilMs: nowMs + leaseMs }
    return {
      acquired: true,
      state: cloneDiscoverySweepState(state),
      lease: { ...state.lease },
    }
  }

  async renewDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    return (await this.renewDiscoverySweepWithDetails(workspaceId, owner, epoch, nowMs, leaseMs)).renewed
  }

  async renewDiscoverySweepWithDetails(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepRenewal> {
    const lease = this.#workspace(workspaceId).discoverySweep.lease
    if (!lease) return { renewed: false, reason: 'missing' }
    if (lease.owner !== owner || lease.epoch !== epoch) {
      return { renewed: false, reason: 'contended', observedLease: { ...lease } }
    }
    if (lease.leaseUntilMs <= nowMs) {
      return { renewed: false, reason: 'expired', observedLease: { ...lease } }
    }
    lease.leaseUntilMs = nowMs + leaseMs
    return { renewed: true, lease: { ...lease } }
  }

  async completeDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    checkpoint?: DiscoveryCheckpoint,
  ): Promise<boolean> {
    return this.#completeDiscoverySweep(workspaceId, owner, epoch, checkpoint)
  }

  /**
   * A sweep that committed while Relayfile was shedding it keeps a decayed
   * ratchet and a backoff instead of clearing both outright (#297).
   */
  async completeDiscoverySweepWithOverload(
    workspaceId: string,
    owner: string,
    epoch: number,
    checkpoint: DiscoveryCheckpoint | undefined,
    overload: { consecutiveOverloads: number; backoffUntilMs: number },
  ): Promise<boolean> {
    return this.#completeDiscoverySweep(workspaceId, owner, epoch, checkpoint, overload)
  }

  #completeDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    checkpoint?: DiscoveryCheckpoint,
    overload?: { consecutiveOverloads: number; backoffUntilMs: number },
  ): boolean {
    const state = this.#workspace(workspaceId).discoverySweep
    if (!discoveryLeaseMatches(state, owner, epoch)) return false
    // A missing checkpoint means finalization couldn't get a watermark or
    // change window this cycle (a transient feed hiccup, not "the tree is
    // now empty") — keep the last good checkpoint so the next sweep can
    // still diff from it instead of falling back to a full walk.
    if (checkpoint) state.checkpoint = cloneDiscoveryCheckpoint(checkpoint)
    state.consecutiveOverloads = overload?.consecutiveOverloads ?? 0
    state.backoffUntilMs = overload?.backoffUntilMs ?? 0
    delete state.lease
    return true
  }

  async deferDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    backoffUntilMs: number,
    consecutiveOverloads: number,
  ): Promise<boolean> {
    const state = this.#workspace(workspaceId).discoverySweep
    if (!discoveryLeaseMatches(state, owner, epoch)) return false
    state.backoffUntilMs = backoffUntilMs
    state.consecutiveOverloads = consecutiveOverloads
    delete state.lease
    return true
  }

  async releaseDiscoverySweep(workspaceId: string, owner: string, epoch: number): Promise<void> {
    const state = this.#workspace(workspaceId).discoverySweep
    if (discoveryLeaseMatches(state, owner, epoch)) delete state.lease
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
    // Adopt any row written under a pre-#211 key before deciding no claim
    // exists, so a deploy does not create a second claim for live work.
    applyLifecycleMigration(lifecycles, key, seed, nowMs)
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
    // See FileStateStore#claimDispatchLifecycle: claim starts the #303
    // never-placed clock for rows that predate the field.
    stampDispatchLifecycleSlot(lifecycle, lifecycle, nowMs)
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
    stampDispatchLifecycleSlot(lifecycle, lifecycle, nowMs)
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
    stampDispatchLifecycleSlot(next, current, nowMs)
    this.#workspace(workspaceId).dispatchLifecycles.set(key, next)
    return true
  }

  async getDispatchLifecycle(workspaceId: string, key: string): Promise<DispatchLifecycle | undefined> {
    const lifecycle = this.#workspace(workspaceId).dispatchLifecycles.get(key)
    return lifecycle ? cloneDispatchLifecycle(lifecycle) : undefined
  }

  async listDispatchLifecycles(workspaceId: string): Promise<Array<[string, DispatchLifecycle]>> {
    // No load step here, so this is where a directly-seeded legacy row gets
    // rekeyed before startup adoption can pick up its old key.
    migrateWorkspaceLifecycleKeys(this.#workspace(workspaceId).dispatchLifecycles)
    // Migration aliases are audit evidence, never adoptable work.
    return [...this.#workspace(workspaceId).dispatchLifecycles]
      .filter(([, lifecycle]) => !isMigrationAlias(lifecycle))
      .map(([key, lifecycle]) => [key, cloneDispatchLifecycle(lifecycle)])
  }

  async clearQueuedDispatchLifecycle(
    workspaceId: string,
    key: string,
    expectedLease: DispatchLifecycle['lease'],
  ): Promise<boolean> {
    const lifecycles = this.#workspace(workspaceId).dispatchLifecycles
    const lifecycle = lifecycles.get(key)
    if (lifecycle?.phase !== 'queued' || !dispatchLifecycleLeaseMatches(lifecycle.lease, expectedLease)) {
      return false
    }
    lifecycles.delete(key)
    return true
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

  async setSlackThreadWatch(workspaceId: string, issueKey: string, watch: SlackThreadWatchState): Promise<void> {
    this.#workspace(workspaceId).slackThreadWatches.set(issueKey, structuredClone(watch))
  }

  async listSlackThreadWatches(workspaceId: string): Promise<Array<[string, SlackThreadWatchState]>> {
    return [...this.#workspace(workspaceId).slackThreadWatches]
      .map(([key, watch]) => [key, structuredClone(watch)])
  }

  async clearSlackThreadWatch(workspaceId: string, issueKey: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadWatches.delete(issueKey)
  }

  async reserveConversationSession(
    workspaceId: string,
    conversationId: string,
    session: ConversationSessionState,
  ): Promise<boolean> {
    const sessions = this.#workspace(workspaceId).conversationSessions
    if (sessions.has(conversationId)) return false
    sessions.set(conversationId, cloneConversationSession(session))
    return true
  }

  async getConversationSession(
    workspaceId: string,
    conversationId: string,
  ): Promise<ConversationSessionState | undefined> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    return session ? cloneConversationSession(session) : undefined
  }

  async listConversationSessions(
    workspaceId: string,
  ): Promise<Array<[string, ConversationSessionState]>> {
    return [...this.#workspace(workspaceId).conversationSessions]
      .map(([conversationId, session]) => [conversationId, cloneConversationSession(session)])
  }

  async appendConversationMessage(
    workspaceId: string,
    conversationId: string,
    message: ConversationMessage,
  ): Promise<ConversationSessionState | undefined> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session || conversationHasMessage(session, message.id)) return undefined
    session.processedMessageIds.push(message.id)
    session.pending.push(structuredClone(message))
    return cloneConversationSession(session)
  }

  async claimConversationMessageAcknowledgement(
    workspaceId: string,
    conversationId: string,
    messageId: string,
    claimId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session || !conversationHasMessage(session, messageId)) return false
    if ((session.acknowledgedMessageIds ?? []).includes(messageId)) return false
    session.acknowledgementClaims ??= {}
    const current = session.acknowledgementClaims[messageId]
    if (current && current.claimedAtMs + leaseMs > nowMs) return false
    session.acknowledgementClaims[messageId] = { claimId, claimedAtMs: nowMs }
    return true
  }

  async completeConversationMessageAcknowledgement(
    workspaceId: string,
    conversationId: string,
    messageId: string,
    claimId: string,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (session?.acknowledgementClaims?.[messageId]?.claimId !== claimId) return false
    session.acknowledgedMessageIds ??= []
    if (!session.acknowledgedMessageIds.includes(messageId)) session.acknowledgedMessageIds.push(messageId)
    delete session.acknowledgementClaims[messageId]
    return true
  }

  async renewConversationMessageAcknowledgement(
    workspaceId: string,
    conversationId: string,
    messageId: string,
    claimId: string,
    nowMs: number,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    const claim = session?.acknowledgementClaims?.[messageId]
    if (claim?.claimId !== claimId) return false
    claim.claimedAtMs = nowMs
    return true
  }

  async releaseConversationMessageAcknowledgement(
    workspaceId: string,
    conversationId: string,
    messageId: string,
    claimId: string,
  ): Promise<void> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (session?.acknowledgementClaims?.[messageId]?.claimId === claimId) {
      delete session.acknowledgementClaims[messageId]
    }
  }

  async claimConversationTerminalReceipt(
    workspaceId: string,
    conversationId: string,
    claimId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session) return false
    if (session.terminalReceipt?.posted) return false
    const current = session.terminalReceipt
    if (current && current.claimedAtMs + leaseMs > nowMs) return false
    session.terminalReceipt = { claimId, claimedAtMs: nowMs }
    return true
  }

  async renewConversationTerminalReceipt(
    workspaceId: string,
    conversationId: string,
    claimId: string,
    nowMs: number,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    const receipt = session?.terminalReceipt
    if (receipt?.claimId !== claimId || receipt.posted) return false
    receipt.claimedAtMs = nowMs
    return true
  }

  async completeConversationTerminalReceipt(
    workspaceId: string,
    conversationId: string,
    claimId: string,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (session?.terminalReceipt?.claimId !== claimId) return false
    session.terminalReceipt = { ...session.terminalReceipt, posted: true }
    return true
  }

  async releaseConversationTerminalReceipt(
    workspaceId: string,
    conversationId: string,
    claimId: string,
  ): Promise<void> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (session?.terminalReceipt?.claimId === claimId && !session.terminalReceipt.posted) {
      delete session.terminalReceipt
    }
  }

  async claimConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<ConversationSessionState | undefined> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session) return undefined
    if (session.delivery && session.delivery.claimedAtMs + leaseMs > nowMs) {
      return undefined
    }
    if (session.delivery) {
      session.pending.unshift(...session.delivery.messages)
    }
    session.pending.sort(compareConversationMessages)
    if (!session.agent || session.pending.length === 0) {
      session.delivery = undefined
      return undefined
    }
    const agent = session.agent
    session.delivery = {
      claimId,
      owner,
      claimedAtMs: nowMs,
      attempts: (session.delivery?.attempts ?? 0) + 1,
      messages: session.pending.splice(0),
      agent: {
        name: agent.name,
        sessionRef: agent.sessionRef,
      },
    }
    return cloneConversationSession(session)
  }

  async renewConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    nowMs: number,
  ): Promise<boolean> {
    const delivery = this.#workspace(workspaceId).conversationSessions.get(conversationId)?.delivery
    if (!delivery || delivery.owner !== owner || delivery.claimId !== claimId) return false
    delivery.claimedAtMs = nowMs
    return true
  }

  async completeConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    agent: { name: string; sessionRef?: string },
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session?.delivery || session.delivery.owner !== owner || session.delivery.claimId !== claimId) return false
    session.history = [...session.history, ...session.delivery.messages].slice(-CONVERSATION_HISTORY_LIMIT)
    if (
      session.agent &&
      session.agent.name === session.delivery.agent.name &&
      session.agent.sessionRef === session.delivery.agent.sessionRef
    ) {
      session.agent.name = agent.name
      if (agent.sessionRef) session.agent.sessionRef = agent.sessionRef
    }
    session.delivery = undefined
    return true
  }

  async releaseConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string): Promise<void> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session?.delivery || session.delivery.owner !== owner || session.delivery.claimId !== claimId) return
    session.pending.unshift(...session.delivery.messages)
    session.pending.sort(compareConversationMessages)
    session.delivery = undefined
  }

  async clearConversationSession(workspaceId: string, conversationId: string): Promise<void> {
    this.#workspace(workspaceId).conversationSessions.delete(conversationId)
  }

  async rebindConversationSession(
    workspaceId: string,
    conversationId: string,
    agent: NonNullable<ConversationSessionState['agent']>,
  ): Promise<boolean> {
    const session = this.#workspace(workspaceId).conversationSessions.get(conversationId)
    if (!session) return false
    session.agent = structuredClone(agent)
    return true
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

  async markRunning(
    workspaceId: string,
    ownershipKey: string,
    agentName: string,
    nowMs: number,
    leaseMs: number,
    options?: { force?: boolean },
  ): Promise<{ generationId: string } | null> {
    const generations = this.#workspace(workspaceId).babysitterGenerations
    const current = generations.get(ownershipKey)
    if (current && (
      current.phase !== 'claimed' ||
      current.leaseUntilMs >= nowMs ||
      options?.force !== true
    )) return null

    const generationId = randomUUID()
    generations.set(ownershipKey, {
      generationId,
      agentName,
      claimedAtMs: nowMs,
      leaseUntilMs: nowMs + leaseMs,
      phase: 'claimed',
    })
    return { generationId }
  }

  async renewBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    const current = this.#workspace(workspaceId).babysitterGenerations.get(ownershipKey)
    if (current?.phase !== 'claimed' || current.generationId !== generationId) return false
    current.leaseUntilMs = nowMs + leaseMs
    return true
  }

  async durableCompletionCas(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean> {
    const current = this.#workspace(workspaceId).babysitterGenerations.get(ownershipKey)
    if (current?.phase !== 'claimed' || current.generationId !== generationId) return false
    current.phase = 'completed'
    return true
  }

  async getBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
  ): Promise<BabysitterGenerationRecord | undefined> {
    const current = this.#workspace(workspaceId).babysitterGenerations.get(ownershipKey)
    return current ? structuredClone(current) : undefined
  }

  async clearBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean> {
    const generations = this.#workspace(workspaceId).babysitterGenerations
    if (generations.get(ownershipKey)?.generationId !== generationId) return false
    return generations.delete(ownershipKey)
  }

  async recordCanonicalState(workspaceId: string, key: string, role: string): Promise<void> {
    this.#workspace(workspaceId).canonicalIssueStates.set(key, role)
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
        slackThreadWatches: new Map(),
        conversationSessions: new Map(),
        githubIssueCommentWatches: new Map(),
        seenAgentQuestionKeys: new Set(),
        seenAgentQuestionOrder: [],
        waitingClarifications: new Map(),
        dispatchAttempts: new Map(),
        canonicalIssueStates: new Map(),
        dispatchFailureReaperHandoffs: new Map(),
        babysitterSessions: new Map(),
        babysitterGenerations: new Map(),
        dispatchLifecycles: new Map(),
        discoverySweep: emptyDiscoverySweepState(),
      }
      this.#workspaces.set(workspaceId, state)
    }
    return state
  }
}

const cloneDispatchLifecycle = (lifecycle: DispatchLifecycle): DispatchLifecycle => structuredClone(lifecycle)

const emptyDiscoverySweepState = (): DiscoverySweepState => ({
  consecutiveOverloads: 0,
  backoffUntilMs: 0,
  lastEpoch: 0,
})

const cloneDiscoveryCheckpoint = (checkpoint: DiscoveryCheckpoint): DiscoveryCheckpoint =>
  structuredClone(checkpoint)

const cloneDiscoverySweepState = (state: DiscoverySweepState): DiscoverySweepState =>
  structuredClone(state)

const discoveryLeaseMatches = (state: DiscoverySweepState, owner: string, epoch: number): boolean =>
  state.lease?.owner === owner && state.lease.epoch === epoch

const dispatchLifecycleLeaseMatches = (
  current: DispatchLifecycle['lease'],
  expected: DispatchLifecycle['lease'],
): boolean => current === undefined
  ? expected === undefined
  : expected !== undefined && current.owner === expected.owner && current.epoch === expected.epoch

const CONVERSATION_HISTORY_LIMIT = 50

const cloneConversationSession = (session: ConversationSessionState): ConversationSessionState =>
  structuredClone(session)

const conversationHasMessage = (session: ConversationSessionState, id: string): boolean =>
  session.processedMessageIds.includes(id) ||
  session.history.some((message) => message.id === id) ||
  session.pending.some((message) => message.id === id) ||
  Boolean(session.delivery?.messages.some((message) => message.id === id))

const compareConversationMessages = (left: ConversationMessage, right: ConversationMessage): number =>
  left.receivedAtMs - right.receivedAtMs ||
  (left.providerSequence ?? left.id).localeCompare(right.providerSequence ?? right.id, undefined, { numeric: true })

const activeDispatchLifecycleCount = (lifecycles: Map<string, DispatchLifecycle>, exceptKey?: string): number =>
  [...lifecycles].filter(([key, lifecycle]) =>
    key !== exceptKey && !isMigrationAlias(lifecycle) && dispatchLifecycleOccupiesSlot(lifecycle)).length

/**
 * Moves a row persisted under a pre-#211 key onto the canonical work-unit key,
 * demotes any other matching rows to audit-only aliases, and prunes aliases
 * that have outlived the retention policy.
 *
 * Throws rather than choosing when two keys both hold a live lease.
 */
const migrateWorkspaceLifecycleKeys = (
  lifecycles: Map<string, DispatchLifecycle>,
): boolean => migrateDispatchLifecycleKeys(
  () => [...lifecycles],
  (from, to) => {
    lifecycles.set(to, lifecycles.get(from)!)
    lifecycles.delete(from)
  },
  (key, canonicalKey) => {
    lifecycles.set(key, asMigrationAlias(lifecycles.get(key)!, canonicalKey))
  },
)

const applyLifecycleMigration = (
  lifecycles: Map<string, DispatchLifecycle>,
  canonicalKey: string,
  seed: Pick<DispatchLifecycle, 'issue'>,
  nowMs: number,
): void => {
  const plan = planLifecycleMigration(lifecycles, canonicalKey, seed, nowMs)
  if (plan.outcome === 'conflict') {
    throw new DispatchLifecycleMigrationConflictError(canonicalKey, plan.keys)
  }
  if (plan.outcome === 'adopt') {
    lifecycles.set(canonicalKey, lifecycles.get(plan.from)!)
    lifecycles.delete(plan.from)
  }
  for (const aliasKey of plan.aliases) {
    lifecycles.set(aliasKey, asMigrationAlias(lifecycles.get(aliasKey)!, canonicalKey, nowMs))
  }
  for (const pruned of prunableMigrationAliases(lifecycles, nowMs)) lifecycles.delete(pruned)
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
