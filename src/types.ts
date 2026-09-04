import type { FactoryConfig } from './config/schema'
import type { FactoryStateResolution } from './linear/state-resolver'
import type { AgentSpec, FleetClient, GithubRead, GithubWriteback, LinearWriteback, MountClient, PreviewReference, SlackWriteback } from './ports'
import type { DispatchLifecyclePhase, StateStore, TerminalDispatchLifecyclePhase } from './ports/state'
import type { Clock, Logger } from './ports/system'
import type { FactoryEventReporter } from './ports/observability'
import type { AgentWorktreeManager } from './ports/worktree'
import type { SandboxPush } from './ports/sandbox-push'
import type { CloseProbePrInput, CloseProbePrResult } from './github/probe-closer'
import type { GhRunner, GithubMergeGate } from './github/merge-gate'
import type { AgentProcessFinder, ProcessIdentity } from './orchestrator/process-identity'
import type { FactorySweepSkipReasonCode } from './orchestrator/sweep-skip-reason'
import type { FactoryDispatchFailureReasonCode } from './orchestrator/dispatch-failure-reason'
import type { DispatchRelayflowOptions, RelayflowPolicyRegistry } from './dispatch/relayflow-registry'
import type { VerificationGate } from './environments/verification-pipeline'
import type { CostLedger } from './cost/ledger'
import type { TicketDispatchDelivery } from './delivery/ticket-dispatch'
import type { FleetControlPlaneStatus } from './fleet/control-plane-circuit'
import type { FleetConnectStatus } from './ports/fleet'

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
  /**
   * Publishes an implementer's sandbox commits as a branch + PR when the
   * placement was remote. Absent on a local-only host, where the implementer's
   * clone is on this filesystem and the existing clone path already publishes.
   */
  sandboxPush?: SandboxPush
  /** @deprecated Ignored. Factory PR discovery uses the authenticated mounted projection. */
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
  /**
   * Re-arm delay for the dispatch-lifecycle and completion-release retries.
   * Test-only override of the built-in 1 s floor, so a suite can exercise the
   * release retry budget without spending ten real seconds waiting for it.
   */
  dispatchLifecycleRetryMs?: number
  /**
   * Interval at which owned dispatch-lifecycle leases are renewed. Test-only
   * override of the built-in 60 s, so a suite can actually observe the renewer
   * run instead of asserting around it. A test that never lets this fire cannot
   * distinguish a relinquished lease from a retained epoch (#391 review, P2).
   */
  dispatchLifecycleRenewMs?: number
  relayflows?: FactoryRelayflowDispatchPort
  /** Local CLI checkout isolation. Remote fleet nodes own their own checkout lifecycle. */
  worktrees?: AgentWorktreeManager
  /**
   * This instance serves a read-only command (`status`, a `--dry-run` sweep) and
   * must be free of workspace side effects.
   *
   * Construction alone used to subscribe to fleet events, and subscribing mints
   * this process's relay identity — so `factory status` created an agent row it
   * then abandoned before presence, which is what wedged cloud dispatch for a
   * week (factory-cloud#55). A read-only instance skips that wiring and refuses
   * `start()`; pair it with a read-only `fleet` client for the hard guarantee.
   */
  readOnly?: boolean
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
  /**
   * Deadline for one relayfile call inside a sweep (#351). Bounds what
   * `reconcileTimeoutMs` cannot: a single dependency call that never returns,
   * which no deadline checked *between* awaits can reach.
   */
  relayfileOperationTimeoutMs: number
  /**
   * Aggregate budget for one whole sweep (#372). Bounds what neither neighbour
   * can: a bounded call wrapped in an unbounded retry, or a hang on a transport
   * nobody has bounded yet. Expiry aborts the sweep from inside its own fence,
   * so the discovery lease is released and the next cycle starts clean.
   */
  sweepBudgetMs: number
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
  /** Writer path for this record; `live-timer` proves process liveness only. */
  source?: 'live-timer' | 'bounded-loop'
  iteration: number
  maxIterations: number
  /** Stable for this loop lifetime; lets readers apply a cold-start grace. */
  startedAt?: string
  startedAtMs?: number
  /** Explicit capability marker for consumers of the progress receipt below. */
  progressContract?: 'discovery-sweep-v1'
  updatedAt: string
  updatedAtMs: number
  /**
   * Advances only after a discovery sweep commits successfully. Timer-only
   * heartbeat refreshes copy this receipt unchanged.
   */
  progress?: {
    sequence: number
    operation: 'discovery-sweep'
    updatedAt: string
    updatedAtMs: number
  }
  registryPath?: string
  eventListener?: FactoryEventListenerStatus
  readinessReconcile?: FactoryReadinessReconcileStatus
  /** Batch-slot admission: a full batch is why dispatch stops without failing (#303). */
  dispatchCapacity?: FactoryDispatchCapacityStatus
  /** Daemon-owned dispatch admission state; status readers must prefer this over a fresh local Factory instance. */
  fleetControlPlane?: FleetControlPlaneStatus
  /**
   * State of the fleet EVENT SOCKET dial that makes this Factory agent
   * `online`. Absent when the backend has no socket. `dialed` is unconfirmed:
   * the SDK accepted `connect()`, but no stream event has proved the socket
   * opened, so a healthy silent workspace may remain in that state.
   *
   * Distinct from `eventListener`, which is the orchestrator's ISSUE
   * subscription. Conflating the two is how a fleet client that registered an
   * agent and never connected read as healthy on every surface.
   */
  fleetConnect?: FleetConnectStatus
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
  /**
   * The deadline on the *caller's wait* for one sweep (#296).
   *
   * Published because its absence was read as its non-existence. An operator
   * looking at a climbing `inFlightMs` beside `intervalMs: 60000` and nothing
   * else has no way to tell "this is bounded, the bound is just far away" from
   * "nothing will ever preempt this" — and the second reading has now been
   * reached twice from the same stanza. `intervalMs` is a scheduler tick and
   * cannot preempt anything; these two are the numbers that can.
   */
  timeoutMs?: number
  /**
   * The aggregate budget for one sweep (#372/#374) — the bound that UNWINDS.
   *
   * Distinct from `timeoutMs` on purpose. `timeoutMs` ends the wait and leaves
   * `runOnce()` running for later cycles to coalesce onto; this one expires
   * from inside the sweep, so the lease goes back and the next cycle starts
   * clean. When a reader wants to know "how long until this recovers", this is
   * the field, and `missedPasses` is how far through it the current pass is.
   */
  sweepBudgetMs?: number
  /** `inFlightMs` expressed in sweeps that should have run and did not. */
  missedPasses?: number
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
  /**
   * Work units the last *enumerating* sweep pulled and evaluated (#355).
   *
   * Written when a pass settles successfully AND enumerated; left alone by a
   * pass that failed, one still running, and one that deferred to another
   * process's lease. `lastEnumeratedAtMs` — not `lastCompletedAtMs` — is what
   * dates them, because the latter advances on deferred passes too.
   *
   * Optional, and never defaulted to zero. A sweep that ran and found nothing
   * publishes `0`; a daemon that has not enumerated a sweep publishes nothing
   * at all, and the whole point of the field is that those two are different
   * facts — `candidates: 0` blames discovery, an absent `candidates` blames
   * nobody yet.
   */
  candidates?: number
  /**
   * Tree reads the last enumerating sweep made, and how many were served empty
   * (#351 follow-up).
   *
   * Read as a pair, against `candidates`. An empty read on its own is ordinary
   * — a healthy sweep lists two path forms per repo and only one exists. The
   * fault is `emptyTreeReads === treeReads` with `treeReads > 0`: the mount
   * served nothing at all, which raises no timeout, no failure and no
   * `lastError`, and is otherwise indistinguishable on a `healthy` surface from
   * a workspace that simply has no ready work.
   *
   * Optional and never defaulted, like `candidates`: absent means no sweep has
   * enumerated *or* the producer predates the fields.
   */
  treeReads?: number
  emptyTreeReads?: number
  /** Work units the last enumerating sweep actually dispatched. */
  dispatched?: number
  /** Work units the last enumerating sweep saw and declined. */
  skipped?: number
  /**
   * `skipped` split by cause. Zero-count codes are omitted; the codes
   * themselves are a fixed published vocabulary, so an absent key is a zero.
   */
  skipReasons?: Partial<Record<FactorySweepSkipReasonCode, number>>
  /**
   * Dispatch attempts the last enumerating sweep made that failed (#355).
   *
   * The same number `skipReasons['dispatch-failed']` carries, published in its
   * own right so it can be a zero. `skipReasons` omits zero-count codes, so
   * "every dispatch succeeded" and "this daemon does not report the field" are
   * the same absence there; here they are not.
   *
   * Optional and never defaulted, exactly like `candidates`: absent means no
   * sweep has completed *or* the producer predates the field, `0` means a sweep
   * completed and nothing it dispatched failed.
   */
  dispatchFailures?: number
  /**
   * `dispatchFailures` split by cause. Zero-count codes are omitted; the codes
   * are a fixed published vocabulary, so an absent key is a zero.
   */
  dispatchFailureReasons?: Partial<Record<FactoryDispatchFailureReasonCode, number>>
  /**
   * When the pass the counts above describe finished enumerating (#359 review).
   *
   * NOT `lastCompletedAtMs`, and the difference is the point. That timestamp
   * advances on every settled pass including a deferred one, which enumerates
   * nothing; this one advances only when a pass actually enumerated. Equal on
   * a daemon that is sweeping normally; where they differ, the gap is how
   * stale the counts are — the freshness a reader otherwise could not
   * recover, since retained counts sat beside an ever-fresh completion stamp.
   */
  lastEnumeratedAtMs?: number
  /**
   * The MOST RECENT pass never enumerated anything: another process held the
   * discovery lease, so it returned an empty report immediately.
   *
   * Without this, that pass is indistinguishable from a sweep that queried the
   * provider and legitimately found no ready work — both would publish
   * `candidates: 0` — and those are opposite diagnoses (#355).
   *
   * Independent of the counts above, which describe the last sweep that
   * actually enumerated. A deferred pass records only this marker: its zeroes
   * measure nothing and must not overwrite a real sweep's numbers, which on a
   * persistently-held lease would erase them entirely (#358 review). So the two
   * together mean "the counts are from an earlier pass"; this one alone means
   * nothing has enumerated yet.
   */
  discoveryDeferred?: 'sweep-in-flight'
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
  /** The deadline on the caller's wait. See the same field on the status record. */
  timeoutMs?: number
  /** The aggregate per-sweep budget — the bound that unwinds and frees the lease. */
  sweepBudgetMs?: number
  lastDurationMs?: number
  lastStartedAtMs?: number
  lastCompletedAtMs?: number
  lastFailureAtMs?: number
  inFlightMs?: number
  /** `inFlightMs` expressed in sweeps that should have run and did not. */
  missedPasses?: number
  /**
   * The last enumerating sweep's arithmetic, published (#355).
   *
   * Counts only — no issue keys, no paths, no titles — and absent rather than
   * zero until a sweep has completed enumeration, so "never enumerated" and
   * "enumerated and found nothing" are two different readings of this surface
   * rather than one. A completed deferral still leaves these absent.
   */
  candidates?: number
  dispatched?: number
  skipped?: number
  /** `skipped` split by a closed vocabulary of causes; zero counts omitted. */
  skipReasons?: Partial<Record<FactorySweepSkipReasonCode, number>>
  /**
   * Failed dispatch attempts in the last enumerating sweep, and why (#355).
   *
   * `dispatchFailures` is absent until a sweep completes and is a zero
   * thereafter, so it separates "never attempted" from "attempted, none
   * failed" — which `skipReasons` alone cannot, since it drops zero counts.
   * The breakdown is counts only, keyed by a fixed vocabulary, and its parts
   * sum to `dispatchFailures`.
   */
  dispatchFailures?: number
  dispatchFailureReasons?: Partial<Record<FactoryDispatchFailureReasonCode, number>>
  /**
   * Tree reads the last enumerating sweep made, and how many came back empty
   * (#351 follow-up; #363 review, codex P1).
   *
   * The counterpart of `FactoryReadinessReconcileStatus.treeReads` on the
   * UNAUTHENTICATED surface, which is where `factory diagnose --deployed`
   * reads it — the fault this pair exists to name is one an operator only ever
   * meets on a deployed instance, so a signal that stops at the internal
   * `status()` object does not exist where it is needed.
   *
   * Counts only, and a pair: published together or not at all, because
   * `emptyTreeReads` alone is not a signal (a healthy sweep lists two path
   * forms per repo and only one exists) and `treeReads` alone says nothing.
   * The fault is `emptyTreeReads === treeReads` with `treeReads > 0` beside a
   * zero `candidates`: the mount served nothing at all, rather than the
   * workspace having no ready work.
   */
  treeReads?: number
  emptyTreeReads?: number
  /**
   * When the pass the counts describe finished enumerating. Dates them —
   * `lastCompletedAtMs` does not, since it advances on deferred passes too.
   */
  lastEnumeratedAtMs?: number
  /**
   * A producer supplied some enumeration counts, but the trio was incomplete
   * or invalid and was rejected during normalization. This is not equivalent
   * to a genuine first-pass deferral with no enumeration evidence.
   */
  enumerationCountsInvalid?: true
  /**
   * The most recent pass deferred to another process's discovery lease. Present
   * alongside the counts it means they are from an earlier pass. Present alone
   * means nothing has enumerated yet only when `enumerationCountsInvalid` is
   * absent; otherwise a supplied snapshot was unusable and prior enumeration
   * is unknown.
   */
  discoveryDeferred?: 'sweep-in-flight'
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
  /**
   * Deadline after which a slot that DID place an agent should have been
   * reaped (#419). Distinct from `agentlessHoldTimeoutMs`: this bounds a team
   * that plausibly ran, and until this field was published there was no
   * maximum hold at all for that shape — one occupant survived 13.5 hours
   * against a 4-hour reaper while the subsystem still read healthy.
   */
  agentHoldTimeoutMs: number
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
  /**
   * Deadline for an occupied slot with a placed agent (#419). The reaper for
   * this shape has always existed but was never surfaced next to
   * `agentlessHoldTimeoutMs`, so `dispatchCapacity` could read `healthy` for
   * an occupant that had held its slot 3× this bound. Published so a reader
   * can check each deadline against its matching age: `slotHeldForMs` for
   * agentless slots, `heldForMs` for placed-agent slots. The two clocks are
   * distinct because a placement whose spawn took hours is not the same as
   * a placement that outran its deadline, and only the second is the wedge
   * the reaper reaps.
   */
  agentHoldTimeoutMs: number
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
   * Occupied slots with a placed agent that have exceeded `agentHoldTimeoutMs`
   * (#419).
   *
   * The other wedge shape: a placed agent that went offline, or a run that
   * exceeded any plausible duration. Distinct from `agentlessOccupants`
   * because the *why* an operator should ask differs — an agentless wedge
   * points at spawn machinery, an occupied wedge points at the placed worker
   * itself. Past the deadline, no healthy dispatch is still here either.
   */
  occupiedOccupants?: number
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
  /**
   * Milliseconds since the first successful placement (#419).
   *
   * Distinct from `slotHeldForMs` in exactly one case that matters: a row that
   * took a slot and then placed an agent. `slotHeldForMs` measures against the
   * slot; `heldForMs` measures against the placement. The occupied-hold
   * reaper anchors on this one.
   */
  heldForMs?: number
  /** True once this occupant is past `agentlessHoldTimeoutMs` with no placement. */
  pastReapDeadline?: boolean
  /**
   * True once this occupant has a placed agent and is past
   * `agentHoldTimeoutMs` (#419). The other wedge shape.
   */
  pastOccupiedDeadline?: boolean
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
/**
 * Unauthenticated view of the fleet socket. State and counters only.
 *
 * `lastError` is deliberately absent for the same reason it is absent from the
 * control-plane block: it stays behind the authenticated `/evidence`.
 */
export interface FactoryPublicFleetConnectHealth {
  state: FleetConnectStatus['state'] | 'unknown'
  attempts?: number
  lastAttemptAtMs?: number
  lastDialedAtMs?: number
  firstEventAtMs?: number
  lastConnectedAtMs?: number
  lastFailureAtMs?: number
}

export interface FactoryPublicFleetControlPlaneHealth {
  state: FleetControlPlaneStatus['state'] | 'unknown'
  consecutiveFailures: number
  failureThreshold: number
  lastFailureAtMs?: number
  retryAtMs?: number
}

/**
 * Which build is answering (#446).
 *
 * Not health — identity. It rides in the public health block because that block
 * is the one part of the heartbeat the container passes through to `/healthz`
 * verbatim (`container/entrypoint.mjs`, `publicHeartbeat()`), so putting the
 * two facts here is what makes them readable from outside the container without
 * a second repository having to learn a new field.
 *
 * Both fields are the literal string `unknown` when the running artifact does
 * not carry them, never a synthesised stand-in. See
 * `src/orchestrator/build-identity.ts` for why an absent stamp must not be
 * filled in from the reader's environment.
 */
export interface FactoryPublicBuildIdentity {
  /** Published npm version of the running package, or `unknown`. */
  version: string
  /** Full 40-hex commit the running build was produced from, or `unknown`. */
  commit: string
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
  /** Fleet event socket. NOT dispatch-gating: see DISPATCH_GATING_SUBSYSTEMS. */
  fleetConnect?: FactoryPublicFleetConnectHealth
  dispatchCapacity?: FactoryPublicDispatchCapacityHealth
  /**
   * Which build is answering (#446). Absent from records published by a
   * Factory older than this change — which is itself the answer to "is the fix
   * I merged running?", so a reader must not read its absence as `unknown`
   * having been reported.
   */
  build?: FactoryPublicBuildIdentity
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
  /** Local instant immediately before the provider claim began; recovery requires a newer connected projection. */
  claimStartedAtMs?: number
  /** Cancellation could not safely undo the provider claim; keep its lifecycle and placements recoverable. */
  cancellationBlocked?: boolean
  /** The provider claim call itself had not settled when ownership was handed off. */
  cancellationPending?: boolean
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
  /**
   * `reason` is free text for an operator; `code` is the closed vocabulary
   * that may cross onto the unauthenticated health surface (#355).
   */
  skipped: Array<{
    issue: IssueRef
    reason: string
    code?: FactorySweepSkipReasonCode
    /**
     * Why the dispatch *attempt* failed, for `code: 'dispatch-failed'` only
     * (#355). Recorded at the skip site by type, never parsed back out of
     * `reason`, and absent on every other skip code.
     */
    failureCode?: FactoryDispatchFailureReasonCode
  }>
  dryRun: boolean
  slackDegraded?: boolean
  /**
   * Orphan recovery did not run for this sweep, so `factory:in-progress`
   * claims were preserved rather than reconciled.
   *
   * `dry-run` is expected and benign: a dry run never releases a claim, so it
   * deliberately skips building the safety context — which on a read-only
   * fleet client would mean minting a workspace identity just to decide what
   * the sweep WOULD do. `context-unavailable` is the real degradation: a live
   * sweep tried to build the context and could not.
   */
  orphanRecoveryDegraded?: 'dry-run' | 'context-unavailable'
  /**
   * Tree reads this sweep issued that the backend served, and how many of them
   * came back with zero entries (#351 follow-up).
   *
   * The companion to the per-call timeout: a bounded read that hangs is loud, a
   * bounded read served with nothing is not, and both end in a sweep that
   * dispatches nothing. Meaningful only as a pair — an empty read is ordinary,
   * `emptyTreeReads === treeReads` is the mount serving nothing at all.
   */
  treeReads?: number
  emptyTreeReads?: number
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
  /** Fleet event socket status. Absent when the backend has no socket. */
  fleetConnect?: FleetConnectStatus
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
