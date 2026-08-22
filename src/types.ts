import type { FactoryConfig } from './config/schema'
import type { FactoryStateResolution } from './linear/state-resolver'
import type { AgentSpec, FleetClient, GithubRead, GithubWriteback, LinearWriteback, MountClient, PreviewReference, SlackWriteback } from './ports'
import type { DispatchLifecyclePhase, StateStore, TerminalDispatchLifecyclePhase } from './ports/state'
import type { Clock, Logger } from './ports/system'
import type { FactoryEventReporter } from './ports/observability'
import type { AgentWorktreeManager } from './ports/worktree'
import type { CloseProbePrInput, CloseProbePrResult } from './github/probe-closer'
import type { GhRunner, GithubMergeGate } from './github/merge-gate'
import type { AgentProcessFinder, ProcessIdentity } from './orchestrator/process-identity'
import type { DispatchRelayflowOptions, RelayflowPolicyRegistry } from './dispatch/relayflow-registry'
import type { VerificationGate } from './environments/verification-pipeline'
import type { CostLedger } from './cost/ledger'
import type { TicketDispatchDelivery } from './delivery/ticket-dispatch'
import type { FleetControlPlaneStatus } from './fleet/control-plane-circuit'

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
  /** Injectable delivery adapter for onTicketDispatch Slack/Telegram notifications. */
  ticketDispatchDelivery?: TicketDispatchDelivery
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
  /** Optional shared accounting seam; Factory creates an isolated ledger when omitted. */
  costLedger?: CostLedger
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
  /**
   * Resolves once the issue's durable dispatch row reaches a terminal phase,
   * reporting which one. Callers deriving an exit code need the phase: a
   * dispatch held on capacity returns an empty hold result and schedules a
   * durable retry, so the pre-wait result cannot say how the run ended.
   *
   * `undefined` means no terminal phase was observed — either this dispatch
   * never created a lifecycle row (a dependency park, a triage escalation, or
   * a label refusal all return before the claim) or the wait ended because
   * Factory is stopping.
   */
  waitForDispatchTerminal(issue: IssueRef): Promise<TerminalDispatchLifecyclePhase | undefined>
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
  /** Periodic source-of-truth readiness reconciliation, independent of event cursors/watermarks. */
  reconcileIntervalMs: number
  /**
   * Deadline for one reconcile sweep. Expiry rejects the sweep, which is what
   * routes a hang into the failure path that re-arms the loop (#296). Must be
   * sized above realistic worst-case mirror hydration, not to the interval.
   */
  reconcileTimeoutMs: number
}

/**
 * The daemon's own view of its primary Relayfile event listener.  This is
 * intentionally separate from the local mirror's reconcile health: a quiet
 * event stream can be healthy, while a stopped daemon is not listening at all.
 */
export interface FactoryEventListenerStatus {
  state: 'starting' | 'subscribed' | 'polling' | 'not-listening' | 'unknown'
  reason?: string
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
  eventListener?: FactoryEventListenerStatus
  readinessReconcile?: FactoryReadinessReconcileStatus
  /** Batch-slot admission: a full batch is why dispatch stops without failing (#303). */
  dispatchCapacity?: FactoryDispatchCapacityStatus
  /** Daemon-owned dispatch admission state; status readers must prefer this over a fresh local Factory instance. */
  fleetControlPlane?: FleetControlPlaneStatus
  /**
   * Redacted projection of this record, safe to serve unauthenticated (#295).
   *
   * The deployed container reads this file to answer `/healthz` and has no
   * redaction logic of its own, so the daemon publishes the already-safe view
   * rather than leaving the boundary to whoever serves it.
   */
  health?: FactoryPublicHealth
}

/**
 * `stalled` is derived, not written: a sweep that hangs takes neither the
 * success nor the failure path, so nothing updates `state` while it is stuck.
 * See `derivedReadinessReconcileState`.
 */
export type FactoryReadinessReconcileState =
  | 'not-running'
  | 'healthy'
  | 'retrying'
  | 'degraded'
  | 'stalled'

export interface FactoryReadinessReconcileStatus {
  state: FactoryReadinessReconcileState
  consecutiveFailures: number
  failureThreshold: number
  /** Sweep cadence — the denominator that turns `inFlightMs` into missed passes. */
  intervalMs?: number
  lastDurationMs?: number
  lastStartedAtMs?: number
  /**
   * When the oldest sweep still running actually began, published by a daemon
   * that knows rather than inferred from timestamp order (#296).
   *
   * `lastStartedAtMs` is the start of the last *wait*. A wait that ends on its
   * deadline writes a settle timestamp while its `runOnce()` keeps running, so
   * order alone reports "nothing in flight" while work is still stuck. Readers
   * prefer this and fall back to the order inference, which is all a heartbeat
   * from a daemon that does not publish it can offer.
   */
  inFlightSinceMs?: number
  lastCompletedAtMs?: number
  lastFailureAtMs?: number
  /** Age of a pass that started and has neither completed nor failed. */
  inFlightMs?: number
  /** Free text; authenticated surfaces only. */
  lastError?: string
  /** Allowlisted class name of `lastError`; publishable. */
  lastErrorClass?: string
}

/** A subsystem state as published, plus the value an unrecognised one collapses to. */
export type FactoryPublicSubsystemState = FactoryReadinessReconcileState

export interface FactoryPublicReadinessReconcileHealth {
  state: FactoryPublicSubsystemState | 'unknown'
  consecutiveFailures: number
  failureThreshold: number
  intervalMs?: number
  lastDurationMs?: number
  lastStartedAtMs?: number
  lastCompletedAtMs?: number
  lastFailureAtMs?: number
  inFlightMs?: number
  /** `inFlightMs` expressed in sweeps that should have run and did not. */
  missedPasses?: number
  lastErrorClass?: string
}

/**
 * A lifecycle currently holding one of the `batchSize` slots (#303).
 *
 * `agents` and `slotHeldForMs` together are what separate ordinary
 * backpressure from a wedge: a slot held for hours by a row that never placed
 * an agent is the shape that produced a total dispatch outage.
 */
export interface FactoryDispatchSlotOccupant {
  issue: string
  phase?: DispatchLifecyclePhase
  /** Entries recorded on the lifecycle, including planned-but-unspawned. */
  agents: number
  /**
   * Entries that actually reached a spawn result.
   *
   * `agents` counts specs: `BatchTracker#recordPlanned` writes one before the
   * spawn returns. Zero here with a slot held is the wedge signature.
   */
  placedAgents: number
  /** Since the first successful placement, when there has been one. */
  heldForMs?: number
  /** Since the row took the batch slot, whether or not it ever placed an agent. */
  slotHeldForMs?: number
}

/**
 * Batch admission as an operator-readable fact (#303).
 *
 * `promoteDispatchLifecycle` is a silent predicate: it returns `false`, never
 * throws, and the caller swallows the result into a retry. A full batch was
 * therefore indistinguishable from an idle Factory on every surface an
 * operator could reach. This is that predicate, published.
 */
export interface FactoryDispatchCapacityStatus {
  batchSize: number
  /** Lifecycles occupying a slot right now. */
  active: number
  /** Lifecycles waiting on capacity right now. */
  waiting: number
  /** Wall-clock wait past which the wait is treated as dispatch-gating. */
  waitWarnMs: number
  /** Deadline after which a slot that never placed an agent should have been reaped. */
  agentlessHoldTimeoutMs: number
  longestWaitMs?: number
  occupants?: FactoryDispatchSlotOccupant[]
  /** Issue keys waiting on capacity, longest wait first. */
  waitingIssues?: string[]
}

/** Batch occupancy, redacted for the unauthenticated surface (#303). */
export interface FactoryPublicDispatchCapacityHealth {
  state: 'healthy' | 'waiting' | 'stalled'
  batchSize: number
  active: number
  waiting: number
  waitWarnMs: number
  agentlessHoldTimeoutMs: number
  longestWaitMs?: number
  /**
   * Occupied slots that never placed an agent **and** are already past the
   * deadline that should have reaped them.
   *
   * Deliberately not "has no agent yet": `recordPlanned` writes a spec before
   * the spawn returns, so every healthy dispatch is agent-less for as long as
   * its spawn takes — minutes, for a cloud placement. Counting that would make
   * the wedge signature read 1 continuously on a single-slot batch that is
   * working perfectly (#303 review, cubic). Past the deadline, no healthy
   * dispatch is still here.
   */
  agentlessOccupants?: number
  /**
   * Per-occupant age and identity, redacted (#315).
   *
   * `agentlessOccupants` is a COUNT, and a count cannot tell one permanently
   * stuck occupant from a rapid reap-and-reacquire cycle: both read `1` on
   * every sample forever. Distinguishing them took 34 samples over 40 minutes
   * of watching a number that never moved. `slotHeldForMs` settles it in one
   * request — it grows monotonically for a stuck row and resets for a
   * churning one.
   *
   * `id` is a boot-scoped digest, salted per process, so entries can be
   * followed across samples while an occupant lasts. It is deliberately NOT
   * stable across restarts and deliberately not derivable back to an issue
   * key: those carry project and repository names, which is why this surface
   * redacts them in the first place (#303).
   */
  occupants?: FactoryPublicDispatchSlotOccupant[]
}

/** One occupied batch slot, safe to serve unauthenticated (#315). */
export interface FactoryPublicDispatchSlotOccupant {
  /** Boot-scoped opaque identity. Never an issue key. */
  id: string
  /**
   * Entries that actually reached a spawn result; a reported 0 is the wedge
   * signature. Omitted when the producer did not report it — an absent count
   * is not a zero, and reading it as one publishes a wedge nobody claimed.
   */
  placedAgents?: number
  /** Since the row took the batch slot, whether or not it ever placed an agent. */
  slotHeldForMs?: number
  /** True once this occupant is past `agentlessHoldTimeoutMs` with no placement. */
  pastReapDeadline?: boolean
}

export interface FactoryPublicEventListenerHealth {
  state: FactoryEventListenerStatus['state']
}

/**
 * The broker/fleet mutation gate, redacted (#300 review).
 *
 * An open circuit fails every spawn and resume fast, so it gates dispatch as
 * hard as a failing readiness sweep. Its `lastError` is free text — a roster
 * probe failure names sockets and paths — so only the state, the counters and
 * the retry instant cross.
 */
export interface FactoryPublicFleetControlPlaneHealth {
  state: FleetControlPlaneStatus['state'] | 'unknown'
  consecutiveFailures: number
  failureThreshold: number
  lastFailureAtMs?: number
  retryAtMs?: number
}

/**
 * The unauthenticated health record (#295).
 *
 * `ok` is process liveness — the question the container ping endpoint asks,
 * and the only one whose answer may recycle a container. `status` is the
 * amber: dispatch-gating degradation that an operator or monitor must see,
 * carried where no platform will act on it.
 */
export interface FactoryPublicHealth {
  schemaVersion: number
  ok: boolean
  status: 'ok' | 'degraded' | 'unknown'
  /**
   * Stamped when the daemon WROTE this record, not when it was read.
   *
   * A record served out of a file the daemon has stopped updating still says
   * `stale: false`, because it was fresh at write time. Freshness is therefore
   * `updatedAtMs` measured against the serving process's clock — which is what
   * the container's own liveness verdict does, and why that verdict outranks
   * this field (#300 review).
   */
  stale: boolean
  updatedAtMs?: number
  ageMs?: number
  loopStatus?: FactoryLoopHeartbeatStatus | 'unknown'
  /** Dispatch-gating subsystems that are not healthy right now. */
  degradedSubsystems: string[]
  /** Why this is not plain `ok`, assembled from closed vocabularies only. */
  reason?: string
  readinessReconcile?: FactoryPublicReadinessReconcileHealth
  eventListener?: FactoryPublicEventListenerHealth
  fleetControlPlane?: FactoryPublicFleetControlPlaneHealth
  dispatchCapacity?: FactoryPublicDispatchCapacityHealth
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
  /** Durable-claim visibility independent of the provider writeback surface. */
  dispatchClaim?: FactoryDispatchClaimStatus
  heldSinceAtMs?: number
  holdDeadlineAtMs?: number
  waitingForTerminalState?: FactoryConfig['terminalState']
  lifecyclePhase?: DispatchLifecyclePhase
}

export interface FactoryDispatchClaimStatus {
  state: 'pending' | 'verified' | 'degraded'
  write?: string
  attempts?: number
  maxAttempts?: number
  error?: string
  deadLettered?: boolean
  updatedAtMs: number
}

export interface FactoryInFlightDispatchStatus {
  issue: IssueRef
  agents: Array<{
    name: string
    role?: AgentSpec['role']
    sessionRef?: string
    invocationId?: string
    node?: string
  }>
  claim: FactoryDispatchClaimStatus
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

export interface FactoryHeldAgent {
  name: string
  role?: AgentSpec['role']
  issue: IssueRef
  lifecyclePhase?: DispatchLifecyclePhase
  waitingForTerminalState: FactoryConfig['terminalState']
  heldSince: string
  heldSinceAtMs: number
  heldForMs: number
  holdDeadline: string
  holdDeadlineAtMs: number
  pastDeadline: boolean
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

/**
 * Where the work unit actually lives, when the surface that offered it is a
 * mirror rather than the origin.
 *
 * A `[factory]` Linear mirror of a GitHub issue has Linear's uuid, key and
 * sense path but is the same unit of work as the GitHub issue it mirrors.
 * Without the origin recorded structurally, the mirror and the GitHub-native
 * row derive different work-unit identities and both dispatch — the AR-448
 * shape. The surface fields stay authoritative for writeback; this is only
 * for identity.
 */
export interface WorkUnitOrigin {
  provider: 'github'
  owner: string
  repo: string
  number: number
}

export interface IssueRef {
  uuid: string
  key: string
  path: string
  /** Provider-native origin when this ref came from a mirror. Absent for a native surface. */
  origin?: WorkUnitOrigin
}

export interface IterationReport {
  pulled: IssueRef[]
  triaged: TriageDecision[]
  dispatched: DispatchResult[]
  skipped: Array<{ issue: IssueRef; reason: string }>
  dryRun: boolean
  slackDegraded?: boolean
  /** A cross-process owner was already enumerating this workspace. */
  discoveryDeferred?: 'sweep-in-flight'
  error?: { message: string; stack?: string }
}

export interface DispatchResult {
  issue: IssueRef
  issueResolution?: IssueResolution
  agents: Array<{ name: string; role: AgentSpec['role'] }>
  comments?: string[]
  stateId?: string
  previews?: PreviewReference[]
  dryRun: boolean
  hold?: {
    kind: 'capacity' | 'dependency' | 'dependency-cycle'
    blockers?: string[]
    cycle?: string[]
  }
}

export interface FactoryStatus {
  inFlight: IssueRef[]
  /** Registry-backed issue/agent ownership, including degraded GitHub claims. */
  inFlightDispatches?: FactoryInFlightDispatchStatus[]
  queued: IssueRef[]
  parked?: Array<{
    issue: IssueRef
    blockers: string[]
    cycle?: string[]
    capacityBlocked: boolean
  }>
  counters: Record<string, number>
  /** Broker/fleet mutation gate. An open circuit blocks new workers until a successful half-open roster probe. */
  fleetControlPlane: FleetControlPlaneStatus
  slackDegraded?: boolean
  slackDegradedReason?: string
  /** Primary Relayfile subscription/poll registration, not event activity. */
  eventListener?: FactoryEventListenerStatus
  /** Periodic ready-issue backfill health as reported by the live daemon. */
  readinessReconcile?: FactoryReadinessReconcileStatus
  /** Batch-slot admission, including which lifecycles hold the slots (#303). */
  dispatchCapacity?: FactoryDispatchCapacityStatus
  /** Agents retained while their issue waits for its configured terminal state. */
  heldAgents?: FactoryHeldAgent[]
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
  issueResolution?: IssueResolution
  routes: Array<{ repo: string; clonePath?: string; rationale: string }>
  scope: 'single' | 'workflow' | 'team' | 'swarm'
  implementers: AgentSpec[]
  workflow?: AgentSpec
  reviewer: AgentSpec
  thin: boolean
  confidence: 'high' | 'low'
  rationale: string
}

export interface IssueResolution {
  source: 'relayfile-projection' | 'github-api-fallback'
  repo?: string
  detail: string
  projection: {
    outcome: 'matched' | 'no-match'
    localMountDegraded?: boolean
    localMountDegradedReason?: string
    eventListener?: FactoryEventListenerStatus
    githubConnection?: {
      ready: boolean
      state?: string
      initialSyncState?: string
    }
  }
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
