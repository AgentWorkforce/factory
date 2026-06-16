import type { SendInput, SpawnResult } from './fleet'
import type { InFlightIssue, QueuedIssue, TrackedAgent } from '../orchestrator/batch-tracker'
import type { IssueRef, TriageDecision } from '../types'

export type CriticalRecord = { issue: IssueRef; input: SendInput }

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

  seenAgentQuestion(workspaceId: string, key: string): Promise<boolean>
  markAgentQuestion(workspaceId: string, key: string): Promise<void>
  claimAgentQuestion(workspaceId: string, key: string): Promise<boolean>

  recordFailureHandoff(workspaceId: string, key: string, handoff: RegistryHandoffAgent): Promise<void>
  getFailureHandoff(workspaceId: string, key: string): Promise<RegistryHandoffAgent | undefined>
  listFailureHandoffs(workspaceId: string): Promise<Array<[string, RegistryHandoffAgent]>>
  clearFailureHandoff(workspaceId: string, key: string): Promise<void>

  recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void>
  getCanonicalState(workspaceId: string, key: string): Promise<string | undefined>
}
