import type { FactoryConfig } from './config/schema'
import type { AgentSpec, FleetClient, GithubRead, LinearWriteback, MountClient, SlackWriteback } from './ports'
import type { Clock, Logger } from './ports/system'
import type { CloseProbePrInput, CloseProbePrResult } from './github/probe-closer'
import type { GithubMergeGate } from './github/merge-gate'
import type { AgentProcessFinder, ProcessIdentity } from './orchestrator/process-identity'

export interface FactoryPorts {
  mount: MountClient
  fleet: FleetClient
  triage?: TriageEngine
  linear?: LinearWriteback
  slack?: SlackWriteback
  github?: GithubRead
  mergeGate?: GithubMergeGate
  probeCloser?: ProbeCloser
  probePrResolver?: ProbePrResolver
  logger?: Logger
  clock?: Clock
  processIdentityReader?: (pid: number) => Promise<ProcessIdentity | undefined>
  processFinder?: AgentProcessFinder
  kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readChildPids?: (pid: number) => Promise<number[]>
  terminationGraceMs?: number
}

export interface Factory {
  start(opts?: FactoryStartOptions): Promise<void>
  stop(): Promise<void>
  dispose(): Promise<void>
  runOnce(opts?: { dryRun?: boolean }): Promise<IterationReport>
  runLoop(opts?: FactoryLoopRunOptions): Promise<IterationReport[]>
  triageIssue(issue: LinearIssue): Promise<TriageDecision>
  dispatch(decision: TriageDecision, opts?: { dryRun?: boolean }): Promise<DispatchResult>
  status(): FactoryStatus
  on(
    event: 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error',
    listener: (payload: FactoryEventPayload) => void,
  ): () => void
}

export interface FactoryStartOptions {
  mode?: 'backfill-and-subscribe' | 'live'
  liveSubscription?: Partial<FactoryLiveSubscriptionOptions>
}

export interface FactoryLiveSubscriptionOptions {
  transport: 'subscribe-and-poll' | 'subscribe' | 'poll'
  pollIntervalMs: number
  eventLimit: number
  replaySkewMarginMs: number
}

export interface FactoryLoopRunOptions {
  dryRun?: boolean
  maxIterations?: number
  maxConsecutiveFailures?: number
  heartbeatPath?: string
  registryPath?: string
}

export type FactoryLoopHeartbeatStatus = 'running' | 'idle' | 'stopping'

export interface FactoryLoopHeartbeat {
  pid: number
  status: FactoryLoopHeartbeatStatus
  iteration: number
  maxIterations: number
  updatedAt: string
  updatedAtMs: number
  registryPath?: string
}

export interface FactoryInFlightRegistryAgent {
  name: string
  role?: AgentSpec['role']
  issue?: IssueRef
  sessionRef?: string
  pids: number[]
  processes?: FactoryInFlightRegistryProcess[]
}

export interface FactoryInFlightRegistryProcess {
  pid: number
  agentName: string
  cmdline: string
  startTime: string
}

export interface FactoryInFlightRegistry {
  pid: number
  heartbeatPath?: string
  updatedAt: string
  updatedAtMs: number
  agents: FactoryInFlightRegistryAgent[]
}

export interface FactoryLoopLiveness {
  ok: boolean
  stale: boolean
  ageMs?: number
  heartbeat?: FactoryLoopHeartbeat
  reason?: string
}

export interface LinearIssue {
  uuid: string
  key: string
  title: string
  description: string
  stateId: string
  state?: { name: string }
  labels: string[]
  project?: string
  team?: string
  assignee?: string
  path: string
  raw: Record<string, unknown>
}

export interface IssueRef {
  uuid: string
  key: string
  path: string
}

export interface IterationReport {
  pulled: IssueRef[]
  triaged: TriageDecision[]
  dispatched: DispatchResult[]
  skipped: Array<{ issue: IssueRef; reason: string }>
  dryRun: boolean
  slackDegraded?: boolean
  error?: { message: string; stack?: string }
}

export interface DispatchResult {
  issue: IssueRef
  agents: Array<{ name: string; role: AgentSpec['role'] }>
  comments?: string[]
  stateId?: string
  dryRun: boolean
}

export interface FactoryStatus {
  inFlight: IssueRef[]
  queued: IssueRef[]
  counters: Record<string, number>
  slackDegraded?: boolean
  slackDegradedReason?: string
}

export type FactoryEventPayload =
  | { issue: IssueRef }
  | { issue: IssueRef; result: DispatchResult }
  | { issue: IssueRef; path: string }
  | { error: unknown; errorMessage: string; errorStack?: string; issue?: IssueRef }

export interface TriageEngine {
  triage(issue: LinearIssue, ctx: TriageContext): Promise<TriageDecision>
}

export interface TriageContext {
  config: FactoryConfig
  repoMap: RepoMapEntry[]
}

export interface RepoMapEntry {
  repo: string
  clonePath?: string
  source: 'label' | 'project' | 'keyword' | 'default'
  key?: string
}

export interface TriageDecision {
  issue: IssueRef
  routes: Array<{ repo: string; clonePath?: string; rationale: string }>
  scope: 'single' | 'team'
  implementers: AgentSpec[]
  reviewer: AgentSpec
  thin: boolean
  confidence: 'high' | 'low'
  rationale: string
}

export interface PrSummary {
  repo: string
  number: number
  title?: string
  url?: string
  /** Advisory only: mount snapshots can lag live GitHub state. Never use this for merge readiness. */
  state?: string
  headRef?: string
  baseRef?: string
  author?: string
  filesChanged?: string[]
}

export type ProbePrRef = Pick<CloseProbePrInput, 'repo' | 'prNumber'>

export type ProbePrResolver = (issue: LinearIssue) => Promise<ProbePrRef | undefined>

export type ProbeCloser = (
  input: Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>,
) => Promise<CloseProbePrResult>
