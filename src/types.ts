import type { FactoryConfig } from './config/schema'
import type { FactoryStateResolution } from './linear/state-resolver'
import type { AgentSpec, FleetClient, GithubRead, GithubWriteback, LinearWriteback, MountClient, SlackWriteback } from './ports'
import type { StateStore } from './ports/state'
import type { Clock, Logger } from './ports/system'
import type { FactoryEventReporter } from './ports/observability'
import type { AgentWorktreeManager } from './ports/worktree'
import type { CloseProbePrInput, CloseProbePrResult } from './github/probe-closer'
import type { GhRunner, GithubMergeGate } from './github/merge-gate'
import type { AgentProcessFinder, ProcessIdentity } from './orchestrator/process-identity'
import type { DispatchRelayflowOptions, RelayflowPolicyRegistry } from './dispatch/relayflow-registry'
import type { VerificationGate } from './environments/verification-pipeline'

export interface FactoryPorts {
  mount: MountClient
  fleet: FleetClient
  stateStore?: StateStore
  // Resolved Linear state mapping (role <-> UUID, per team). When omitted the
  // factory builds one from config.stateIds (explicit UUIDs only). The CLI
  // resolves names against /linear/states and injects it here.
  stateResolution?: FactoryStateResolution
  triage?: TriageEngine
  linear?: LinearWriteback
  slack?: SlackWriteback
  github?: GithubRead
  githubWriteback?: GithubWriteback
  mergeGate?: GithubMergeGate
  verificationGate?: VerificationGate
  probeCloser?: ProbeCloser
  probePrResolver?: ProbePrResolver
  probePrGhRunner?: GhRunner
  logger?: Logger
  /** Optional durable, no-throw progress reporter for the authenticated Cloud dashboard. */
  reporter?: FactoryEventReporter
  clock?: Clock
  processIdentityReader?: (pid: number) => Promise<ProcessIdentity | undefined>
  processFinder?: AgentProcessFinder
  kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readChildPids?: (pid: number) => Promise<number[]>
  terminationGraceMs?: number
  /**
   * How long a babysitter wake may keep failing with a registration-lag
   * (target-unreachable) error before the tight retry loop escalates once and
   * backs off. Test-only override of the built-in default.
   */
  babysitterWakeUnreachableEscalateMs?: number
  /** Slow retry cadence applied after an unreachable babysitter escalation. Test-only override. */
  babysitterWakeUnreachableRetryMs?: number
  /**
   * Maximum wall-clock time a live daemon waits for startup-reconciled agent
   * exits before it continues ready-issue discovery. The exit work remains
   * active in the background. Test-only override of the built-in default.
   */
  startupAgentExitDrainTimeoutMs?: number
  relayflows?: FactoryRelayflowDispatchPort
  /** Local CLI checkout isolation. Remote fleet nodes own their own checkout lifecycle. */
  worktrees?: AgentWorktreeManager
}

export interface FactoryRelayflowDispatchPort extends Omit<DispatchRelayflowOptions, 'cwd'> {
  registry: RelayflowPolicyRegistry
  cwd?: string
}

export interface Factory {
  start(opts?: FactoryStartOptions): Promise<void>
  stop(): Promise<void>
  dispose(): Promise<void>
  runOnce(opts?: { dryRun?: boolean }): Promise<IterationReport>
  runLoop(opts?: FactoryLoopRunOptions): Promise<IterationReport[]>
  triageIssue(issue: LinearIssue): Promise<TriageDecision>
  dispatch(decision: TriageDecision, opts?: { dryRun?: boolean }): Promise<DispatchResult>
  waitForDispatchTerminal(issue: IssueRef): Promise<void>
  status(): FactoryStatus
  on(
    event: 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error',
    listener: (payload: FactoryEventPayload) => void,
  ): () => void
}

export interface FactoryStartOptions {
  mode?: 'backfill-and-subscribe' | 'live' | 'dispatch-owner'
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
  // Remote (relay-backend) placement facts; pids are meaningless off-machine.
  invocationId?: string
  node?: string
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
  hold?: {
    kind: 'capacity' | 'dependency' | 'dependency-cycle'
    blockers?: string[]
    cycle?: string[]
  }
}

export interface FactoryStatus {
  inFlight: IssueRef[]
  queued: IssueRef[]
  parked?: Array<{
    issue: IssueRef
    blockers: string[]
    cycle?: string[]
    capacityBlocked: boolean
  }>
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
  scope: 'single' | 'workflow' | 'team'
  implementers: AgentSpec[]
  workflow?: AgentSpec
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

export type ProbePrRef = Pick<CloseProbePrInput, 'repo' | 'prNumber'> & {
  draft?: boolean
  headRef?: string
  headRepo?: string
  crossRepository?: boolean
  state?: string
  url?: string
  path?: string
}

export type ProbePrResolver = (issue: LinearIssue) => Promise<ProbePrRef | undefined>

export type ProbeCloser = (
  input: Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey' | 'requireTitleMarker'>,
) => Promise<CloseProbePrResult>
