import type { SendInput, SpawnResult } from './fleet'
import type { AgentWorktree } from './worktree'
import type {
  DependencyAdmission,
  InFlightIssue,
  ParkedIssue,
  QueuedIssue,
  TrackedAgent,
} from '../orchestrator/batch-tracker'
import type { IssueRef, TriageDecision } from '../types'

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
}

export type DispatchAttemptState = {
  attempts: number
  inFlight: boolean
  terminal: boolean
  backoffUntilMs: number
}

export type DispatchLifecyclePhase =
  | 'queued'
  | 'dispatching'
  | 'retryable'
  | 'running'
  | 'parking'
  | 'waiting-for-human'
  | 'publishing'
  | 'published'
  | 'writeback-applied'
  | 'releasing'
  | 'complete'
  | 'abandoned'

export type DispatchLifecycleLease = {
  owner: string
  epoch: number
  leaseUntilMs: number
}

export type DispatchLifecycle = {
  /** Stable for one dispatch attempt and reused by crash takeover; new after a true reopen. */
  runId: string
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  phase: DispatchLifecyclePhase
  agents: Array<{ name: string; tracked: TrackedAgent; releasedAtMs?: number }>
  invocationIds: string[]
  result?: import('../types').DispatchResult
  /** All repository-specific PR receipts for team dispatches. `pullRequest` remains the primary receipt for compatibility. */
  pullRequests?: import('./mount').GithubPublishPullRequestResult[]
  pullRequest?: import('./mount').GithubPublishPullRequestResult
  releaseReason?: string
  lease?: DispatchLifecycleLease
  updatedAtMs: number
}

export type DispatchLifecycleClaim = {
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
  recordDryRun(record: InFlightIssue, spec: InFlightIssue['decision']['reviewer'], invocationId: string): void
  restore(record: InFlightIssue): InFlightIssue
}

export interface StateStore {
  getBatch(workspaceId: string): Promise<BatchSnapshot>
  recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void>
  getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined>
  releaseInFlight(workspaceId: string, issueKey: string): Promise<void>

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
  clearDispatchLifecycle(workspaceId: string, key: string): Promise<void>

  recordCritical(workspaceId: string, key: string, value: CriticalRecord): Promise<void>
  consumeCritical(workspaceId: string, key: string): Promise<CriticalRecord | undefined>
  isResumed(workspaceId: string, exitKey: string): Promise<boolean>
  markResumed(workspaceId: string, exitKey: string): Promise<void>

  setSlackThread(workspaceId: string, issueKey: string, threadId: string): Promise<void>
  getSlackThread(workspaceId: string, issueKey: string): Promise<string | undefined>
  clearSlackThread(workspaceId: string, issueKey: string): Promise<void>
  clearSlackThreads(workspaceId: string): Promise<void>

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

  recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void>
  getCanonicalState(workspaceId: string, key: string): Promise<string | undefined>
}
