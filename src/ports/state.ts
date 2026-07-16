import type { SendInput, SpawnResult } from './fleet'
import type { InFlightIssue, QueuedIssue, TrackedAgent } from '../orchestrator/batch-tracker'
import type { IssueRef, TriageDecision } from '../types'

export type CriticalRecord = { issue: IssueRef; input: SendInput }

export type ClarificationReply = {
  id: string
  text: string
  receivedAtMs: number
}

export type WaitingClarificationAgent = {
  name: string
  tracked: TrackedAgent
}

export type WaitingClarification = {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  threadId: string
  askerName: string
  question: string
  askedAtMs: number
  agents: WaitingClarificationAgent[]
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
}

export type DispatchAttemptState = {
  attempts: number
  inFlight: boolean
  terminal: boolean
  backoffUntilMs: number
}

export type GithubIssueCommentWatchPending = {
  correlationId: string
  kind: 'triage' | 'agent-question'
  authorizedAuthor: string
  decision?: TriageDecision
  claimedByCommentId?: string
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
  sinceCommentId?: string
  lastSeenCommentId?: string
  processedCommentIds?: string[]
}

export interface BatchSnapshot {
  readonly size: number
  readonly inFlight: InFlightIssue[]
  readonly queued: QueuedIssue[]
  getIssue(issue: IssueRef): InFlightIssue | undefined
  getIssueByAgent(name: string): InFlightIssue | undefined
  isInFlight(issue: IssueRef): boolean
  isQueued(issue: IssueRef): boolean
  canStart(): boolean
  start(decision: TriageDecision, dryRun: boolean): InFlightIssue | undefined
  queue(decision: TriageDecision, dryRun: boolean): boolean
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
  recordDryRun(record: InFlightIssue, spec: InFlightIssue['decision']['reviewer'], invocationId: string): void
}

export interface StateStore {
  getBatch(workspaceId: string): Promise<BatchSnapshot>
  recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void>
  getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined>
  releaseInFlight(workspaceId: string, issueKey: string): Promise<void>

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

  recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void>
  getCanonicalState(workspaceId: string, key: string): Promise<string | undefined>
}
