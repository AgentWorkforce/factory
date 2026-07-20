import type { FactoryConfig } from '../config/schema'
import type { AgentSpec, SpawnInput, SpawnResult } from '../ports/fleet'
import type { Logger } from '../ports/system'
import type { LinearIssue, TriageDecision, TriageEngine } from '../types'

export type HostedFactoryInvocationStatus =
  | 'pending'
  | 'dispatched'
  | 'completed'
  | 'failed'
  | 'cancelled'

export type HostedFactoryTerminalInvocationStatus = Extract<
  HostedFactoryInvocationStatus,
  'completed' | 'failed' | 'cancelled'
>

export type HostedFactoryIssuePhase =
  | 'triaging'
  | 'awaiting-clarification'
  | 'dispatching'
  | 'running'
  | 'merge-gate'
  | 'complete'
  | 'blocked'

export type HostedFactoryLease = {
  workspaceId: string
  ownerId: string
  epoch: number
  expiresAtMs: number
}

export type HostedFactoryLeaseClaim =
  | { acquired: true; lease: HostedFactoryLease }
  | { acquired: false; lease: HostedFactoryLease }

export type HostedFactoryInvocation = {
  invocationId: string
  spec: AgentSpec
  status: HostedFactoryInvocationStatus
  sessionRef?: string
  node?: string
  dispatchedAt?: string
  completedAt?: string
  output?: string
  error?: string
}

export type HostedFactoryMergeGateResult = {
  status: 'pending' | 'ready' | 'blocked'
  reason: string
  decidedAt?: string
  metadata?: Record<string, unknown>
}

export type HostedFactoryWritebackRecord = {
  status: 'pending' | 'posted' | 'failed'
  attempts: number
  postedAt?: string
  error?: string
}

export type HostedFactoryIssueRecord = {
  workspaceId: string
  issue: LinearIssue
  phase: HostedFactoryIssuePhase
  decision?: TriageDecision
  invocations: HostedFactoryInvocation[]
  clarificationReason?: string
  clarificationWriteback?: HostedFactoryWritebackRecord
  dispatchWriteback?: HostedFactoryWritebackRecord
  completionWriteback?: HostedFactoryWritebackRecord
  mergeGate?: HostedFactoryMergeGateResult
  createdAt: string
  updatedAt: string
}

export interface HostedFactoryStateStore {
  /** Atomically claim the workspace-wide control-plane lease. */
  claimRunLease(workspaceId: string, ownerId: string, ttlMs: number): Promise<HostedFactoryLeaseClaim>
  /** Renew only the exact owner+epoch fence while it is still live. */
  renewRunLease(lease: HostedFactoryLease, ttlMs: number): Promise<HostedFactoryLease | null>
  /** Expire only the exact owner+epoch fence. Epoch history must be retained. */
  releaseRunLease(lease: HostedFactoryLease): Promise<void>

  getIssue(workspaceId: string, issueId: string): Promise<HostedFactoryIssueRecord | null>
  listIssues(workspaceId: string): Promise<HostedFactoryIssueRecord[]>
  findIssueByInvocation(
    workspaceId: string,
    invocationId: string,
  ): Promise<HostedFactoryIssueRecord | null>
  /**
   * Persist only while `lease` is the current, unexpired workspace fence.
   * Returning false tells the caller it lost ownership and must stop.
   */
  saveIssue(record: HostedFactoryIssueRecord, lease: HostedFactoryLease): Promise<boolean>
}

export interface HostedFactoryDiscovery {
  discoverReady(input: { workspaceId: string; limit: number }): Promise<LinearIssue[]>
}

export interface HostedFactoryFleet {
  /**
   * The invocation ID is the at-least-once dedupe key. Implementations must
   * return the same ID when they include one in the acknowledgement.
   */
  spawn(
    input: SpawnInput & { invocationId: string },
  ): Promise<SpawnResult & { invocationId?: string }>
}

export type HostedFactoryCompletion = {
  invocationId: string
  status: HostedFactoryInvocationStatus
  completedAt?: string
  output?: string
  error?: string
}

export interface HostedFactoryCompletionSource {
  getInvocation(input: {
    workspaceId: string
    invocationId: string
  }): Promise<HostedFactoryCompletion | null>
}

export interface HostedFactoryMergeGate {
  evaluate(input: {
    workspaceId: string
    record: HostedFactoryIssueRecord
  }): Promise<HostedFactoryMergeGateResult>
}

export type HostedFactoryWritebackInput = {
  workspaceId: string
  /** Stable provider-side dedupe key; required because a save can lose its fence after a successful write. */
  idempotencyKey: string
  record: HostedFactoryIssueRecord
}

export interface HostedFactoryWriteback {
  requestClarification(
    input: HostedFactoryWritebackInput & { reason: string },
  ): Promise<void>
  dispatched(input: HostedFactoryWritebackInput): Promise<void>
  completed(
    input: HostedFactoryWritebackInput & { mergeGate: HostedFactoryMergeGateResult },
  ): Promise<void>
}

export type HostedFactoryOptions = {
  workspaceId: string
  /** Unique per running host/isolate; store-issued epochs fence every new claim. */
  ownerId: string
  config: FactoryConfig
  leaseTtlMs?: number
  maxIssuesPerRun?: number
}

export type HostedFactoryPorts = {
  discovery: HostedFactoryDiscovery
  state: HostedFactoryStateStore
  fleet: HostedFactoryFleet
  completions: HostedFactoryCompletionSource
  writeback: HostedFactoryWriteback
  triage?: TriageEngine
  mergeGate?: HostedFactoryMergeGate
  logger?: Logger
  now?: () => Date
}

export type HostedFactoryRunReport = {
  status: 'completed' | 'fenced' | 'lease-lost'
  discovered: number
  triaged: string[]
  dispatched: string[]
  reconciled: string[]
  awaitingClarification: string[]
  skipped: Array<{ issueKey: string; reason: string }>
  errors: Array<{ issueKey?: string; message: string }>
}

export type HostedFactoryCompletionResult =
  | { status: 'fenced' | 'not-found'; invocationId: string }
  | {
      status: 'updated' | 'terminal'
      invocationId: string
      issueKey: string
      phase: HostedFactoryIssuePhase
    }

export interface HostedFactory {
  runOnce(): Promise<HostedFactoryRunReport>
  ingestCompletion(completion: HostedFactoryCompletion): Promise<HostedFactoryCompletionResult>
}
