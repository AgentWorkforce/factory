import type { FactoryConfig } from './config/schema'
import type { AgentSpec, FleetClient, GithubRead, LinearWriteback, MountClient, SlackWriteback } from './ports'
import type { Clock, Logger } from './ports/system'

export interface FactoryPorts {
  mount: MountClient
  fleet: FleetClient
  triage?: TriageEngine
  linear?: LinearWriteback
  slack?: SlackWriteback
  github?: GithubRead
  logger?: Logger
  clock?: Clock
}

export interface Factory {
  start(): Promise<void>
  stop(): Promise<void>
  runOnce(opts?: { dryRun?: boolean }): Promise<IterationReport>
  triageIssue(issue: LinearIssue): Promise<TriageDecision>
  dispatch(decision: TriageDecision, opts?: { dryRun?: boolean }): Promise<DispatchResult>
  status(): FactoryStatus
  on(
    event: 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error',
    listener: (payload: FactoryEventPayload) => void,
  ): () => void
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
}

export type FactoryEventPayload =
  | { issue: IssueRef }
  | { issue: IssueRef; result: DispatchResult }
  | { issue: IssueRef; path: string }
  | { error: unknown; issue?: IssueRef }

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
  status?: string
  checks?: Array<{ name: string; status: string; conclusion?: string }>
  mergeable?: boolean | 'unknown'
}
