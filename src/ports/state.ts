import type { AgentSpec, Capability, SendInput, SpawnResult } from './fleet'
import type { AgentWorktree } from './worktree'
import type {
  DependencyAdmission,
  InFlightIssue,
  ParkedIssue,
  QueuedIssue,
  TrackedAgent,
} from '../orchestrator/batch-tracker'
import type { FactoryDispatchClaimStatus, IssueRef, TriageDecision } from '../types'
import type { RunCostTotal } from '../cost/ledger'

export type CriticalRecord = { issue: IssueRef; input: SendInput }

export type ClarificationReply = {
  id: string
  text: string
  receivedAtMs: number
  source?: 'slack' | 'github'
  author?: string
}

export type WaitingClarificationAgent = {
  name: string
  tracked: TrackedAgent
}

export type WaitingClarification = {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  /** Optional Slack mirror thread. GitHub remains the durable request/response record. */
  threadId?: string
  questionSource?: 'github' | 'slack'
  askerName: string
  question: string
  askedAtMs: number
  agents: WaitingClarificationAgent[]
  questionPostedAtMs?: number
  questionDelivery?: {
    owner: string
    claimedAtMs: number
    attempts: number
  }
  releasedAgents?: string[]
  parkedAtMs?: number
  escalatedAtMs?: number
  escalation?: {
    owner: string
    claimedAtMs: number
    attempts: number
  }
  reply?: ClarificationReply
  wake?: {
    owner: string
    claimedAtMs: number
    attempts: number
    injectedAgents: string[]
  }
}

export type RegistryHandoffAgent = {
  issue: IssueRef
  name: string
  tracked: TrackedAgent
  persistedAtMs: number
  /** Isolated checkout that must survive until this handoff is fully reaped. */
  worktree?: AgentWorktree
}

export type BabysitterSessionState = {
  issue: IssueRef
  repo: string
  prNumber: number
  agentName: string
  path?: string
  critical: boolean
  pendingKinds: string[]
  /** Durable Relayfile subscription identity, if the workspace supports it. */
  resourceSubscription?: {
    subscriptionId: string
    provider: string
    resourceRef: string
    subscriberId: string
    ownerId: string
    expiresAt: string
    /** Terminal delivery accepted or awaiting acceptance; never renew this record. */
    terminal?: boolean
  }
  /** Claims already queued locally but not yet accepted by Relayfile. */
  pendingDeliveryClaims?: Array<{ deliveryId: string; claimToken: string }>
}

export type BabysitterGenerationRecord = {
  generationId: string
  agentName: string
  claimedAtMs: number
  leaseUntilMs: number
  phase: 'claimed' | 'completed'
}

export type ConversationMessage = {
  id: string
  text: string
  receivedAtMs: number
  /** Provider-native ordering identity (Slack message ts, Telegram update id, etc.). */
  providerSequence?: string
  author?: string
}

export type ConversationSessionState = {
  provider: string
  issue: IssueRef
  /** Provider-native conversation/thread identifier. */
  externalId: string
  /** Provider-specific routing metadata; continuity itself stays provider-neutral. */
  context: Record<string, string>
  agent?: {
    name: string
    sessionRef: string
    /**
     * Bound at claim/rebind time so identity-proof resume never depends on a
     * live lookup by the (possibly since-renamed) agent name. Absent on
     * sessions persisted before this field existed; resume falls back to a
     * live lookup for those.
     */
    role?: AgentSpec['role']
    node?: string
    capability?: Capability
    repo?: string
    clonePath?: string
  }
  /** Previously delivered human turns, retained as bounded resume context. */
  history: ConversationMessage[]
  /** Durable dedupe ledger; unlike rendered history, this is never context-trimmed. */
  processedMessageIds: string[]
  /** Human replies whose visible provider receipt has been acknowledged. */
  acknowledgedMessageIds?: string[]
  /** Short durable claims preventing duplicate concurrent provider receipts. */
  acknowledgementClaims?: Record<string, { claimId: string; claimedAtMs: number }>
  /** New replies waiting for the short coalescing window. */
  pending: ConversationMessage[]
  /** Claimed batch; new arrivals remain in pending while this resume runs. */
  delivery?: {
    claimId: string
    owner: string
    claimedAtMs: number
    attempts: number
    messages: ConversationMessage[]
    /** Binding captured at claim time so a later handoff cannot be overwritten. */
    agent: Pick<NonNullable<ConversationSessionState['agent']>, 'name' | 'sessionRef'>
  }
}

/** Durable metadata required to reconstruct a pre-dispatch Slack watcher. */
export type SlackThreadWatchState = {
  kind: 'triage'
  issue: IssueRef
  decision: TriageDecision
  threadId: string
} | {
  kind: 'terminal-grace'
  issue: IssueRef
  decision: TriageDecision
  threadId: string
  /** Provider-message cutoff preventing historical replies from replaying as terminal. */
  retiredAtMs?: number
  expiresAtMs: number
}

export type DispatchAttemptState = {
  attempts: number
  inFlight: boolean
  terminal: boolean
  backoffUntilMs: number
}

/**
 * Durable discovery snapshot. Tree entries are keyed by the exact prefix
 * passed to Relayfile so a later process can update them from the change feed
 * without re-enumerating every configured repository.
 */
export type DiscoveryCheckpoint = {
  highWatermark?: string
  trees: Record<string, string[]>
  updatedAtMs: number
}

export type DiscoverySweepLease = {
  owner: string
  epoch: number
  leaseUntilMs: number
}

export type DiscoverySweepState = {
  checkpoint?: DiscoveryCheckpoint
  consecutiveOverloads: number
  backoffUntilMs: number
  /** Monotonic fencing token retained after a lease is released. */
  lastEpoch: number
  lease?: DiscoverySweepLease
}

export type DiscoverySweepClaim = {
  acquired: boolean
  reason?: 'in-flight' | 'backoff'
  state: DiscoverySweepState
  lease?: NonNullable<DiscoverySweepState['lease']>
  /** Active lease reclaimed because its process owner is no longer running. */
  reclaimedLease?: DiscoverySweepLease
}

export type DiscoverySweepRenewal =
  | { renewed: true; lease: DiscoverySweepLease }
  | {
    renewed: false
    reason: 'missing' | 'contended' | 'expired' | 'unknown'
    observedLease?: DiscoverySweepLease
  }

export type DispatchLifecyclePhase =
  | 'queued'
  | 'dispatching'
  | 'retryable'
  | 'abandoning'
  | 'running'
  | 'parking'
  | 'waiting-for-human'
  | 'publishing'
  | 'published'
  | 'writeback-applied'
  | 'releasing'
  | 'complete'
  | 'abandoned'

/**
 * The phases a dispatch lifecycle can end in. Callers that wait for a terminal
 * outcome — the CLI derives an exit code from one — must not be handed an
 * intermediate phase they could mistake for a result.
 */
export type TerminalDispatchLifecyclePhase = Extract<DispatchLifecyclePhase, 'complete' | 'abandoned'>

export type DispatchLifecycleLease = {
  owner: string
  epoch: number
  leaseUntilMs: number
}

/** Latest cumulative usage reported by one durable agent, keyed by model. */
export type DispatchLifecycleAgentUsage = {
  model: string
  inputTokens: number | null
  outputTokens: number | null
}

export type DispatchLifecycleAgent = {
  name: string
  tracked: TrackedAgent
  releasedAtMs?: number
  /**
   * Durable source for cost-ledger rehydration after a Factory owner restart.
   * Entries are replaced by model, so cumulative runtime reports do not double
   * count when they are replayed.
   */
  costUsage?: DispatchLifecycleAgentUsage[]
}

export type DispatchLifecycle = {
  /** Stable for one dispatch attempt and reused by crash takeover; new after a true reopen. */
  runId: string
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  phase: DispatchLifecyclePhase
  agents: DispatchLifecycleAgent[]
  invocationIds: string[]
  result?: import('../types').DispatchResult
  /** Durable provider claim status retained across dispatch-owner restarts. */
  dispatchClaim?: FactoryDispatchClaimStatus
  /** All repository-specific PR receipts for team dispatches. `pullRequest` remains the primary receipt for compatibility. */
  pullRequests?: import('./mount').GithubPublishPullRequestResult[]
  pullRequest?: import('./mount').GithubPublishPullRequestResult
  releaseReason?: string
  /** Bounded token/USD aggregate updated with durable usage and finalized at terminal save. */
  cost?: RunCostTotal
  /**
   * Durable at-most-once claim for onTicketDispatch fan-out. The lifecycle
   * runId is the work-unit identity, so takeover can distinguish a recovered
   * dispatch from a true reopen before issuing external side effects.
   */
  ticketDispatchNotification?: {
    workUnitId: string
    claimedAtMs: number
  }
  lease?: DispatchLifecycleLease
  /** First successful agent placement for this active team generation. */
  heldSinceAtMs?: number
  updatedAtMs: number
}

export type DispatchLifecycleClaim = {
  /** Actual persisted key. It may be an older GitHub alias adopted atomically. */
  key?: string
  acquired: boolean
  lifecycle: DispatchLifecycle
  lease?: DispatchLifecycleLease
  created: boolean
}

export type GithubIssueCommentWatchPending = {
  correlationId: string
  kind: 'triage' | 'agent-question'
  authorizedAuthor: string
  decision?: TriageDecision
  claimedByCommentId?: string
  /** Accept the first later authorized issue comment without a correlation prefix. */
  replyAfterCommentId?: string
}

export type GithubIssueCommentWatchState = {
  issue: IssueRef
  source: {
    owner: string
    repo: string
    number: number
    url: string
  }
  pending: GithubIssueCommentWatchPending[]
  /** Keep watching this source issue for structured agent question comments. */
  detectAgentQuestions?: boolean
  sinceCommentId?: string
  lastSeenCommentId?: string
  processedCommentIds?: string[]
}

export interface BatchSnapshot {
  readonly size: number
  readonly inFlight: InFlightIssue[]
  readonly queued: QueuedIssue[]
  readonly parked: ParkedIssue[]
  getIssue(issue: IssueRef): InFlightIssue | undefined
  getIssueByAgent(name: string): InFlightIssue | undefined
  isInFlight(issue: IssueRef): boolean
  isQueued(issue: IssueRef): boolean
  isParked(issue: IssueRef): boolean
  getParked(issue: IssueRef): ParkedIssue | undefined
  canStart(): boolean
  start(decision: TriageDecision, dryRun: boolean, dependencyAdmission?: DependencyAdmission): InFlightIssue | undefined
  queue(decision: TriageDecision, dryRun: boolean, dependencyAdmission?: DependencyAdmission): boolean
  clearPark(issue: IssueRef): void
  complete(issue: IssueRef): QueuedIssue | undefined
  abandon(issue: IssueRef): void
  invocationIdFor(issue: IssueRef, spec: InFlightIssue['decision']['reviewer']): string
  shouldSpawn(record: InFlightIssue, invocationId: string): boolean
  recordSpawn(
    record: InFlightIssue,
    spec: InFlightIssue['decision']['reviewer'],
    invocationId: string,
    result: SpawnResult,
  ): void
  recordPlanned(record: InFlightIssue, spec: InFlightIssue['decision']['reviewer']): void
  recordRelease(record: InFlightIssue, agentName: string, releasedAtMs: number): string | undefined
  recordDryRun(record: InFlightIssue, spec: InFlightIssue['decision']['reviewer'], invocationId: string): void
  restore(record: InFlightIssue): InFlightIssue
}

export interface StateStore {
  getBatch(workspaceId: string): Promise<BatchSnapshot>
  recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void>
  getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined>
  releaseInFlight(workspaceId: string, issueKey: string): Promise<void>

  claimDiscoverySweep(
    workspaceId: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepClaim>
  renewDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean>
  /** Atomic rejection diagnostics; legacy/custom stores may omit this. */
  renewDiscoverySweepWithDetails?(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepRenewal>
  completeDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    checkpoint?: DiscoveryCheckpoint,
  ): Promise<boolean>
  deferDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    backoffUntilMs: number,
    consecutiveOverloads: number,
  ): Promise<boolean>
  releaseDiscoverySweep(workspaceId: string, owner: string, epoch: number): Promise<void>

  claimDispatchLifecycle(
    workspaceId: string,
    key: string,
    seed: DispatchLifecycle,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DispatchLifecycleClaim>
  renewDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean>
  promoteDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
  ): Promise<boolean>
  releaseDispatchLifecycleLease(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
  ): Promise<void>
  saveDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    lifecycle: DispatchLifecycle,
  ): Promise<boolean>
  getDispatchLifecycle(workspaceId: string, key: string): Promise<DispatchLifecycle | undefined>
  listDispatchLifecycles(workspaceId: string): Promise<Array<[string, DispatchLifecycle]>>
  clearQueuedDispatchLifecycle(
    workspaceId: string,
    key: string,
    expectedLease: DispatchLifecycleLease | undefined,
  ): Promise<boolean>
  clearDispatchLifecycle(workspaceId: string, key: string): Promise<void>

  recordCritical(workspaceId: string, key: string, value: CriticalRecord): Promise<void>
  consumeCritical(workspaceId: string, key: string): Promise<CriticalRecord | undefined>
  isResumed(workspaceId: string, exitKey: string): Promise<boolean>
  markResumed(workspaceId: string, exitKey: string): Promise<void>

  setSlackThread(workspaceId: string, issueKey: string, threadId: string): Promise<void>
  getSlackThread(workspaceId: string, issueKey: string): Promise<string | undefined>
  clearSlackThread(workspaceId: string, issueKey: string): Promise<void>
  clearSlackThreads(workspaceId: string): Promise<void>
  setSlackThreadWatch(workspaceId: string, issueKey: string, watch: SlackThreadWatchState): Promise<void>
  listSlackThreadWatches(workspaceId: string): Promise<Array<[string, SlackThreadWatchState]>>
  clearSlackThreadWatch(workspaceId: string, issueKey: string): Promise<void>

  reserveConversationSession(workspaceId: string, conversationId: string, session: ConversationSessionState): Promise<boolean>
  getConversationSession(workspaceId: string, conversationId: string): Promise<ConversationSessionState | undefined>
  listConversationSessions(workspaceId: string): Promise<Array<[string, ConversationSessionState]>>
  appendConversationMessage(workspaceId: string, conversationId: string, message: ConversationMessage): Promise<ConversationSessionState | undefined>
  claimConversationMessageAcknowledgement(workspaceId: string, conversationId: string, messageId: string, claimId: string, nowMs: number, leaseMs: number): Promise<boolean>
  completeConversationMessageAcknowledgement(workspaceId: string, conversationId: string, messageId: string, claimId: string): Promise<boolean>
  releaseConversationMessageAcknowledgement(workspaceId: string, conversationId: string, messageId: string, claimId: string): Promise<void>
  claimConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string, nowMs: number, leaseMs: number): Promise<ConversationSessionState | undefined>
  renewConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string, nowMs: number): Promise<boolean>
  completeConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string, agent: { name: string; sessionRef?: string }): Promise<boolean>
  releaseConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string): Promise<void>
  clearConversationSession(workspaceId: string, conversationId: string): Promise<void>
  /**
   * Retarget a durable conversation session onto a different owning agent (e.g.
   * once a babysitter takes over an issue whose Slack thread was reserved by the
   * implementer) without disturbing accumulated history/pending turns.
   */
  rebindConversationSession(workspaceId: string, conversationId: string, agent: NonNullable<ConversationSessionState['agent']>): Promise<boolean>

  setGithubIssueCommentWatch(workspaceId: string, key: string, watch: GithubIssueCommentWatchState): Promise<void>
  listGithubIssueCommentWatches(workspaceId: string): Promise<Array<[string, GithubIssueCommentWatchState]>>
  clearGithubIssueCommentWatch(workspaceId: string, key: string): Promise<void>

  seenAgentQuestion(workspaceId: string, key: string): Promise<boolean>
  markAgentQuestion(workspaceId: string, key: string): Promise<void>
  claimAgentQuestion(workspaceId: string, key: string): Promise<boolean>

  reserveWaitingClarification(workspaceId: string, issueKey: string, record: WaitingClarification): Promise<boolean>
  getWaitingClarification(workspaceId: string, issueKey: string): Promise<WaitingClarification | undefined>
  listWaitingClarifications(workspaceId: string): Promise<Array<[string, WaitingClarification]>>
  claimClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string, nowMs: number, leaseMs: number): Promise<WaitingClarification | undefined>
  renewClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string, nowMs: number): Promise<boolean>
  completeClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string, postedAtMs: number): Promise<boolean>
  releaseClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string): Promise<void>
  claimClarificationReply(workspaceId: string, issueKey: string, reply: ClarificationReply): Promise<WaitingClarification | undefined>
  markClarificationAgentReleased(workspaceId: string, issueKey: string, agentName: string): Promise<WaitingClarification | undefined>
  markClarificationParked(workspaceId: string, issueKey: string, parkedAtMs: number): Promise<WaitingClarification | undefined>
  claimClarificationEscalation(workspaceId: string, issueKey: string, owner: string, nowMs: number, leaseMs: number): Promise<WaitingClarification | undefined>
  completeClarificationEscalation(workspaceId: string, issueKey: string, owner: string, escalatedAtMs: number): Promise<boolean>
  releaseClarificationEscalation(workspaceId: string, issueKey: string, owner: string): Promise<void>
  claimClarificationWake(workspaceId: string, issueKey: string, owner: string, nowMs: number, leaseMs: number): Promise<WaitingClarification | undefined>
  renewClarificationWake(workspaceId: string, issueKey: string, owner: string, nowMs: number): Promise<boolean>
  markClarificationAgentInjected(workspaceId: string, issueKey: string, owner: string, agentName: string): Promise<boolean>
  completeClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<boolean>
  releaseClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<void>
  clearWaitingClarification(workspaceId: string, issueKey: string): Promise<void>

  recordFailureHandoff(workspaceId: string, key: string, handoff: RegistryHandoffAgent): Promise<void>
  getFailureHandoff(workspaceId: string, key: string): Promise<RegistryHandoffAgent | undefined>
  listFailureHandoffs(workspaceId: string): Promise<Array<[string, RegistryHandoffAgent]>>
  clearFailureHandoff(workspaceId: string, key: string): Promise<void>

  setBabysitterSession(workspaceId: string, issueKey: string, session: BabysitterSessionState): Promise<void>
  listBabysitterSessions(workspaceId: string): Promise<Array<[string, BabysitterSessionState]>>
  clearBabysitterSession(workspaceId: string, issueKey: string): Promise<void>

  /**
   * Atomically creates a babysitter generation. Returns null when a record
   * already exists, unless it is a claimed generation whose lease has expired
   * and force is true. Completed generations must be cleared before reuse.
   */
  markRunning(
    workspaceId: string,
    ownershipKey: string,
    agentName: string,
    nowMs: number,
    leaseMs: number,
    options?: { force?: boolean },
  ): Promise<{ generationId: string } | null>
  renewBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean>
  durableCompletionCas(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean>
  getBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
  ): Promise<BabysitterGenerationRecord | undefined>
  clearBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean>

  recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void>
  getCanonicalState(workspaceId: string, key: string): Promise<string | undefined>
}
