import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import { LINEAR_STATE_IDS, linearByStatePath } from '../constants/linear'
import { GithubMergeGate, closeProbePr, type GhRunner, type GithubMergeGate as GithubMergeGatePort } from '../github'
import type {
  AgentMessage,
  AgentPidResolution,
  AgentSpec,
  ChangeEvent,
  FleetClient,
  LinearWriteback,
  MountClient,
  ProviderSyncStatus,
  SlackWriteback,
  Subscription,
} from '../ports'
import type { Clock, Logger } from '../ports/system'
import { containsExplicitIssueReference, containsIssueKey } from '../issue-key-match'
import { isInFactoryScope } from '../safety/factory-scope'
import { renderAgentTask } from '../dispatch/templates'
import { HeuristicTriage, TieredTriage, babysitterSpec } from '../triage'
import type {
  DispatchResult,
  Factory,
  FactoryEventPayload,
  FactoryPorts,
  FactoryStatus,
  FactoryStartOptions,
  FactoryLiveSubscriptionOptions,
  FactoryLoopRunOptions,
  FactoryLoopHeartbeat,
  FactoryLoopLiveness,
  FactoryInFlightRegistry,
  FactoryInFlightRegistryAgent,
  IssueRef,
  IterationReport,
  LinearIssue,
  ProbeCloser,
  ProbePrResolver,
  TriageDecision,
  TriageEngine,
} from '../types'
import { MountGithubRead, MountLinearWriteback, MountSlackWriteback } from '../writeback'
import { asRecord, parseJsonContent, stableHash, wrappedPayload } from '../writeback/shared'
import { BatchTracker, type InFlightIssue, issueKey, type TrackedAgent } from './batch-tracker'
import { findAgentProcessByName, readProcessIdentity, type AgentProcessFinder } from './process-identity'
import { terminatePids } from './reaper'

type FactoryEvent = 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error'
type Listener = (payload: FactoryEventPayload) => void
type SlackThreadWatcher = { stop(): Promise<void> }
type TerminationRoots = { pids: number[]; status: AgentPidResolution['status'] }
type RegistryHandoffAgent = {
  issue: IssueRef
  name: string
  tracked: TrackedAgent
  persistedAtMs: number
}
type DispatchAttemptState = {
  attempts: number
  inFlight: boolean
  terminal: boolean
  backoffUntilMs: number
}
type ResolvedIssuePr = { repo: string; prNumber: number; draft?: boolean }
type EventHighWatermarkResult = { highWatermark?: string; routeUnavailable: boolean }
type SlackReply = {
  channelDir: string
  threadTs: string
  messageTs: string
  text: string
  isThreadReply: boolean
  isBot: boolean
  raw: Record<string, unknown>
}
type AgentQuestion = {
  agentName: string
  issueKey?: string
  question: string
  eventId?: string
}
type GithubIssueSource = {
  owner: string
  repoName: string
  repo: string
  number: number
  title: string
  body: string
  url: string
  state: string
  labels: string[]
  path: string
  raw: Record<string, unknown>
}
type SlackSyncStatusSeverity = 'soft' | 'hard'
type SlackSyncStatusCheck = { known: boolean; degraded: boolean; reason?: string; severity?: SlackSyncStatusSeverity }
type SlackEventWatermark = { known: boolean; lastEventAtMs?: number }

// Memoizes the existing Linear mirror candidates for one GitHub ingestion pass
// so dedupe lookups reuse a single ISSUE_ROOT scan.
type MirrorCandidateCache = {
  load: () => Promise<LinearIssue[]>
}

const ISSUE_ROOT = '/linear/issues'
const GITHUB_ISSUE_ROOT = '/github/repos'
const READY_EVENTS_LIMIT = 100
const LIVE_ISSUE_GLOB = `${ISSUE_ROOT}/**`
// Subscribe broadly under /github/repos and let isGithubIssueFilePath() /
// githubPullPathParts() re-validate the exact shape in the callback.
// globMatchesPath() treats a non-terminal `**` as a single-segment wildcard, so
// a more specific glob like `.../issues/**/*.json` would miss the two-segment
// <owner>/<repo> prefix and the nested <number>__<slug>/meta.json (and
// directory) event shapes this factory accepts. A terminal `**` prefix-matches
// every descendant, so all supported issue AND pull-request path variants reach
// the handler — including the PR change events the babysitter reacts to.
export const LIVE_GITHUB_ISSUE_GLOB = `${GITHUB_ISSUE_ROOT}/**`
const LIVE_DEDUPE_LIMIT = 5_000
const LIVE_EVENT_DRAIN_BATCH_SIZE = 5
const COMPLETION_SWEEP_INTERVAL_MS = 15_000
const COMPLETION_SWEEP_BATCH_SIZE = 2
const PROBE_PR_GH_BACKOFF_MS = 60_000
const PROBE_PR_GH_CANDIDATE_LIMIT = 200
const STATE_NAME_TO_ID: Record<string, string> = {
  'Ready for Agent': LINEAR_STATE_IDS.readyForAgent,
  'Agent Implementing': LINEAR_STATE_IDS.agentImplementing,
  Implementing: LINEAR_STATE_IDS.agentImplementing,
  Done: LINEAR_STATE_IDS.done,
  'In Planning': LINEAR_STATE_IDS.inPlanning,
}
const SLACK_REPLY_EVENTS_LIMIT = 100
const SLACK_REPLY_POLL_INTERVAL_MS = 5_000
const AGENT_QUESTION_DEDUPE_LIMIT = 500
const AGENT_NEEDS_INPUT_MARKER = '[factory-needs-input]'
const LEGACY_AGENT_NEEDS_INPUT_MARKER = 'FACTORY_NEEDS_INPUT'
// The babysitter DMs `factory` with this marker once it believes the PR is
// green; the orchestrator confirms readiness with one authoritative gh read
// before transitioning the issue to Human Review.
const AGENT_PR_READY_MARKER = '[factory-pr-ready]'
const FACTORY_E2E_MARKER = '[factory-e2e]'
const INJECTION_CONFIRMATION_TIMEOUT_MS = 90_000
const INJECTION_RETRY_DELAY_MS = 1_000
const INJECTION_RETRY_ATTEMPT_TIMEOUT_MS = 15_000
const INJECTION_MAX_ATTEMPTS = 6
const STOP_TEARDOWN_TIMEOUT_MS = 2_500
const SLACK_EVENT_WATERMARK_CACHE_MS = 60_000
const MERGE_GATE_MAX_ATTEMPTS = 12
const MERGE_GATE_POLL_DELAY_MS = 10_000
const DISPATCH_FAILURE_HANDOFF_UNRESOLVED_TTL_MS = 5 * 60_000
const DEFAULT_LIVE_HEARTBEAT_INTERVAL_MS = 15_000
const GITHUB_FACTORY_LABEL = 'factory'
const GITHUB_MIRROR_TITLE_PREFIX = '[factory]'
const GITHUB_MIRROR_SOURCE_PREFIX = 'Source: '
export const DEFAULT_FACTORY_LOOP_HEARTBEAT_PATH = '/tmp/factory-run/factory-loop-heartbeat.json'
export const DEFAULT_FACTORY_LOOP_REGISTRY_PATH = '/tmp/factory-run/factory-loop-registry.json'

const realClock: Clock = {
  now: () => Date.now(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}
export function createFactory(config: FactoryConfig, ports: FactoryPorts): Factory {
  return new FactoryLoop(FactoryConfigSchema.parse(config), ports)
}

export class FactoryLoop implements Factory {
  readonly #config: FactoryConfig
  readonly #mount: MountClient
  readonly #fleet: FleetClient
  readonly #triage: TriageEngine
  readonly #linear: LinearWriteback
  readonly #slack?: SlackWriteback
  readonly #mergeGate: GithubMergeGatePort
  readonly #probeCloser: ProbeCloser
  readonly #probePrResolver: ProbePrResolver
  readonly #customProbePrResolver: boolean
  readonly #probePrGhRunner: GhRunner
  readonly #logger: Logger
  readonly #clock: Clock
  readonly #processIdentityReader: typeof readProcessIdentity
  readonly #processFinder: AgentProcessFinder
  readonly #kill: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readonly #readChildPids: ((pid: number) => Promise<number[]>) | undefined
  readonly #terminationGraceMs: number | undefined
  readonly #batch: BatchTracker
  readonly #listeners = new Map<FactoryEvent, Set<Listener>>()
  readonly #counters: Record<string, number> = {}
  readonly #criticalMessages = new Map<string, { issue: IssueRef; input: Parameters<FleetClient['sendMessage']>[0] }>()
  readonly #resumeInFlight = new Map<string, Promise<void>>()
  readonly #resumedExitKeys = new Set<string>()
  readonly #slackThreadIds = new Map<string, string>()
  readonly #slackWatchers = new Map<string, SlackThreadWatcher>()
  readonly #slackWatcherStarts = new Map<string, Promise<void>>()
  readonly #seenAgentQuestionKeys = new Set<string>()
  readonly #seenAgentQuestionOrder: string[] = []
  readonly #dispatchAttempts = new Map<string, DispatchAttemptState>()
  readonly #canonicalIssueStates = new Map<string, string>()
  readonly #dispatchFailureReaperHandoffs = new Map<string, RegistryHandoffAgent>()
  #slackDegraded = false
  #slackDegradedReason: string | undefined
  #slackWritebackFailureDegraded = false
  #slackWritebackFailureBackoffUntilMs = 0
  #slackEventWatermarkCache?: { checkedAtMs: number; result: SlackEventWatermark }
  #slackEventWatermarkRefresh?: Promise<SlackEventWatermark>
  #subscription?: Subscription
  #livePollTimer?: ReturnType<typeof setTimeout>
  #livePollInFlight = false
  #liveEventCursor?: string
  #liveEventHighWatermark?: string
  #liveConnectStartedAtMs = 0
  #liveReplaySkewMarginMs = 0
  #liveHeartbeatTimer?: ReturnType<typeof setTimeout>
  #liveHeartbeatActive = false
  #liveHeartbeatInFlight = false
  #liveHeartbeatRefresh?: Promise<void>
  #liveHeartbeatLastWriteMs = 0
  #stoppingHeartbeatRefreshActive = false
  readonly #liveEventQueue: ChangeEvent[] = []
  #liveEventDrainScheduled = false
  #liveEventDrainActive = false
  // Holds back the live-event drain while the startup full pull runs, so events
  // that arrive during the pull buffer and drain afterward (batch dedupe then
  // suppresses any overlap with what the pull already dispatched).
  #deferLiveEventDrain = false
  #completionSweepTimer?: ReturnType<typeof setTimeout>
  #completionSweepActive = false
  readonly #completionInFlight = new Set<string>()
  // Issue keys for which a babysitter has already been spawned, so repeated PR
  // webhooks / agent-exit safety nets don't respawn it.
  readonly #babysitterSpawned = new Set<string>()
  // Issue key -> the open PR the babysitter is shepherding, including the
  // webhook-fed mount path so readiness can re-read PR meta without a gh call.
  readonly #babysitterPr = new Map<string, { repo: string; prNumber: number; path?: string }>()
  readonly #probePrGhBackoffUntilMs = new Map<string, number>()
  readonly #probePrResolvedCache = new Map<string, { pr: ResolvedIssuePr; expiresAtMs: number }>()
  // GitHub issue mirror-id -> resolved Linear mirror path, so repeat ingestion
  // cycles read the mirror directly instead of re-scanning all Linear issues.
  readonly #githubMirrorPathCache = new Map<string, string>()
  readonly #seenLiveEvents = new Set<string>()
  #offAgentExit?: () => void
  #offDeliveryFailed?: () => void
  #offAgentMessage?: () => void
  // undefined = readiness not yet probed; resolved lazily so the standalone
  // runOnce() path (which skips #start) still ingests when the mount is present.
  #githubIngestionEnabled?: boolean
  #starting?: Promise<void>
  #started = false
  #stopping = false

  constructor(config: FactoryConfig, ports: FactoryPorts) {
    this.#config = config
    this.#mount = ports.mount
    installFactoryDraftPredicate(this.#mount, config)
    this.#fleet = ports.fleet
    this.#triage = ports.triage ?? new TieredTriage(new HeuristicTriage())
    this.#linear = ports.linear ?? MountLinearWriteback(ports.mount, {
      stateIds: config.stateIds,
      safety: config.safety,
    })
    this.#slack = config.slack ? MountSlackWriteback(ports.mount, config.slack) : ports.slack
    void (ports.github ?? MountGithubRead(ports.mount))
    this.#mergeGate = ports.mergeGate ?? new GithubMergeGate()
    this.#probeCloser = ports.probeCloser ?? closeProbePr
    this.#customProbePrResolver = Boolean(ports.probePrResolver)
    this.#probePrGhRunner = ports.probePrGhRunner ?? failClosedGhRunner
    this.#probePrResolver = ports.probePrResolver ?? ((issue) => this.#resolveIssuePr(issue))
    this.#logger = ports.logger ?? console
    this.#clock = ports.clock ?? realClock
    this.#processIdentityReader = ports.processIdentityReader ?? readProcessIdentity
    this.#processFinder = ports.processFinder ?? ((agentName, opts) => findAgentProcessByName(agentName, {
      readProcessIdentity: this.#processIdentityReader,
      protectedPids: opts?.protectedPids,
    }))
    this.#kill = ports.kill ?? process.kill
    this.#readChildPids = ports.readChildPids
    this.#terminationGraceMs = ports.terminationGraceMs
    this.#batch = new BatchTracker(config.batchSize)
    this.#wireFleetEvents()
  }

  async start(opts: FactoryStartOptions = {}): Promise<void> {
    if (this.#started) {
      return
    }

    if (this.#starting) {
      return this.#starting
    }

    this.#starting = this.#start(opts)
    try {
      await this.#starting
    } finally {
      this.#starting = undefined
    }
  }

  async #start(opts: FactoryStartOptions): Promise<void> {
    this.#stopping = false
    const ready = await this.#mount.ensureSubRoot(ISSUE_ROOT, { timeoutMs: 90_000 })
    if (ready !== 'ready') {
      this.#error(new Error(`${ISSUE_ROOT} sub-root is not mounted`))
      return
    }
    await this.#ensureGithubIngestionReady()

    this.#wireFleetEvents()

    if (opts.mode === 'live') {
      this.#started = true
      try {
        await this.#startLiveSubscription(opts.liveSubscription)
        this.#scheduleCompletionSweep(0)
        return
      } catch (error) {
        this.#started = false
        await this.#stopLiveHeartbeat('stopping')
        throw error
      }
    }

    await this.#backfillReadyIssues()
    this.#subscription = this.#mount.subscribe([`${ISSUE_ROOT}/**/*.json`, LIVE_GITHUB_ISSUE_GLOB], (event) => {
      if (isGithubIssueFilePath(event.resource.path)) {
        void this.#handleGithubIssueChange(event.resource.path, { dryRun: this.#config.dryRun })
        return
      }
      void this.#handleChange(event.resource.path)
    })
    this.#started = true
    this.#scheduleCompletionSweep(0)
  }

  async stop(): Promise<void> {
    this.#started = false
    this.#stopping = true
    if (this.#completionSweepTimer) clearTimeout(this.#completionSweepTimer)
    this.#completionSweepTimer = undefined
    this.#stoppingHeartbeatRefreshActive = await this.#stopLiveHeartbeat('stopping')
    try {
      await this.#releaseInFlightAgents('factory-stopped')
      if (this.#livePollTimer) clearTimeout(this.#livePollTimer)
      this.#livePollTimer = undefined
      this.#livePollInFlight = false
      this.#liveEventQueue.length = 0
      this.#completionInFlight.clear()
      this.#babysitterSpawned.clear()
      this.#babysitterPr.clear()
      const subscription = this.#subscription
      this.#subscription = undefined
      await this.#boundedStopTeardown('factory subscription unsubscribe', () => subscription?.unsubscribe())
      await Promise.all([...this.#slackWatchers.values()].map((watcher) => watcher.stop()))
      this.#slackWatchers.clear()
      this.#slackThreadIds.clear()
      this.#slackWatcherStarts.clear()
      this.#offAgentExit?.()
      this.#offDeliveryFailed?.()
      this.#offAgentMessage?.()
      this.#offAgentExit = undefined
      this.#offDeliveryFailed = undefined
      this.#offAgentMessage = undefined
      await this.#fleet.dispose()
    } finally {
      this.#stoppingHeartbeatRefreshActive = false
    }
  }

  async #boundedStopTeardown(label: string, teardown: () => Promise<void> | void | undefined): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined
    const action = Promise.resolve()
      .then(teardown)
      .then(
        () => ({ status: 'done' as const }),
        (error: unknown) => ({ status: 'error' as const, error }),
      )
    const timeout = new Promise<{ status: 'timeout' }>((resolve) => {
      timer = setTimeout(() => resolve({ status: 'timeout' }), STOP_TEARDOWN_TIMEOUT_MS)
      timer.unref?.()
    })

    const result = await Promise.race([action, timeout])
    if (timer) {
      clearTimeout(timer)
    }
    if (result.status === 'timeout') {
      this.#logger.warn?.(`[factory] ${label} timed out after ${STOP_TEARDOWN_TIMEOUT_MS}ms; continuing shutdown and allowing the server-side subscription to expire`, {
        timeoutMs: STOP_TEARDOWN_TIMEOUT_MS,
      })
    } else if (result.status === 'error') {
      this.#logger.warn?.(`[factory] failed while stopping ${label}; continuing shutdown`, result.error)
    }
  }

  async dispose(): Promise<void> {
    await this.stop()
  }

  async #startLiveSubscription(overrides: Partial<FactoryLiveSubscriptionOptions> = {}): Promise<void> {
    const options = this.#liveOptions(overrides)
    await this.#startLiveHeartbeat()
    this.#liveConnectStartedAtMs = this.#clock.now()
    this.#liveReplaySkewMarginMs = options.replaySkewMarginMs
    const highWatermark = await this.#currentEventHighWatermark()
    this.#liveEventHighWatermark = highWatermark.highWatermark
    this.#seenLiveEvents.clear()
    this.#logger.info?.('[factory] live subscription starting', {
      transport: options.transport,
      highWatermark: this.#liveEventHighWatermark,
      replaySkewMarginMs: this.#liveReplaySkewMarginMs,
      highWatermarkRouteUnavailable: highWatermark.routeUnavailable,
    })

    // Register the live subscription BEFORE the startup full pull so an issue
    // that becomes Ready *during* the pull is captured, not lost in the window
    // between listTree and subscribe. Events buffer (deferred drain) until the
    // pull finishes; batch dedupe then suppresses any overlap with what the
    // pull already dispatched.
    if (options.transport !== 'poll') {
      // LIVE_GITHUB_ISSUE_GLOB is a terminal `${GITHUB_ISSUE_ROOT}/**`, so it
      // already covers the PR change events the babysitter consumes; pull-event
      // *processing* is gated on babysitter.enabled in #prepareLiveEventForDrain.
      this.#subscription = this.#mount.subscribe([LIVE_ISSUE_GLOB, LIVE_GITHUB_ISSUE_GLOB], (event) => {
        this.#enqueueLiveEvent(event)
      }, { from: 'now', coalesce: 'none' })
    }

    if (highWatermark.routeUnavailable) {
      this.#increment('liveHighWatermarkFullPullFallbacks')
      this.#logger.info?.('[factory] live subscription high-watermark route unavailable; running startup full pull before draining buffered events')
      this.#deferLiveEventDrain = true
      try {
        await this.runOnce()
      } catch (error) {
        // A startup pull failure must not abort the daemon: log it and fall back
        // to the live event stream (plus any buffered events) instead of leaving
        // the factory down.
        this.#increment('liveHighWatermarkFullPullErrors')
        this.#error(error)
      } finally {
        this.#deferLiveEventDrain = false
        this.#scheduleLiveEventDrain()
      }
      await this.#refreshLiveHeartbeatIfDue()
    }

    if (options.transport === 'poll') {
      this.#liveEventCursor = await this.#currentEventCursor(options.eventLimit)
      this.#scheduleLivePoll(0, options)
    }
  }

  async #startLiveHeartbeat(): Promise<void> {
    this.#liveHeartbeatActive = true
    await this.#writeLiveHeartbeat('running')
    this.#scheduleLiveHeartbeatRefresh()
  }

  async #stopLiveHeartbeat(status: FactoryLoopHeartbeat['status']): Promise<boolean> {
    if (!this.#liveHeartbeatActive && !this.#liveHeartbeatTimer) {
      return false
    }
    this.#liveHeartbeatActive = false
    if (this.#liveHeartbeatTimer) {
      clearTimeout(this.#liveHeartbeatTimer)
      this.#liveHeartbeatTimer = undefined
    }
    await this.#liveHeartbeatRefresh
    await this.#writeLiveHeartbeat(status)
    return status === 'stopping'
  }

  async #refreshStoppingHeartbeat(): Promise<void> {
    if (!this.#stoppingHeartbeatRefreshActive) {
      return
    }
    try {
      await this.#writeLiveHeartbeat('stopping')
    } catch (error) {
      this.#logger.warn?.('[factory] failed to refresh stopping heartbeat', error)
    }
  }

  #scheduleLiveHeartbeatRefresh(): void {
    if (!this.#liveHeartbeatActive || this.#liveHeartbeatTimer) {
      return
    }
    // This heartbeat proves daemon process liveness for the external crash reaper.
    // MountClient subscriptions do not expose connected/keepalive state here, so
    // subscription-wedge detection remains a separate watchdog concern.
    this.#liveHeartbeatTimer = setTimeout(() => {
      this.#liveHeartbeatTimer = undefined
      this.#liveHeartbeatRefresh = this.#refreshLiveHeartbeat()
        .finally(() => {
          this.#liveHeartbeatRefresh = undefined
        })
    }, liveHeartbeatIntervalMs(this.#config.loop.heartbeatStaleMs))
    this.#liveHeartbeatTimer.unref?.()
  }

  async #refreshLiveHeartbeat(): Promise<void> {
    if (!this.#liveHeartbeatActive || this.#liveHeartbeatInFlight) {
      return
    }
    this.#liveHeartbeatInFlight = true
    try {
      await this.#writeLiveHeartbeat('running')
    } catch (error) {
      this.#logger.warn?.('[factory] failed to refresh live daemon heartbeat', error)
    } finally {
      this.#liveHeartbeatInFlight = false
      this.#scheduleLiveHeartbeatRefresh()
    }
  }

  async #writeLiveHeartbeat(status: FactoryLoopHeartbeat['status']): Promise<void> {
    await this.#writeLoopHeartbeat(
      this.#config.loop.heartbeatPath,
      this.#config.loop.registryPath,
      status,
      0,
      0,
    )
    this.#liveHeartbeatLastWriteMs = this.#clock.now()
  }

  #liveOptions(overrides: Partial<FactoryLiveSubscriptionOptions>): FactoryLiveSubscriptionOptions {
    return {
      transport: overrides.transport ?? this.#config.liveSubscription.transport,
      pollIntervalMs: overrides.pollIntervalMs ?? this.#config.liveSubscription.pollIntervalMs,
      eventLimit: overrides.eventLimit ?? this.#config.liveSubscription.eventLimit,
      replaySkewMarginMs: overrides.replaySkewMarginMs ?? this.#config.liveSubscription.replaySkewMarginMs,
    }
  }

  async #currentEventCursor(limit: number): Promise<string | undefined> {
    let cursor: string | undefined
    for (;;) {
      const page = await this.#mount.getEvents({ cursor, limit })
      cursor = eventCursorAfterPage(cursor, page.events, page.nextCursor)
      if (!page.nextCursor) return cursor
    }
  }

  async #currentEventHighWatermark(): Promise<EventHighWatermarkResult> {
    try {
      return {
        highWatermark: await this.#mount.getEventHighWatermark?.(),
        routeUnavailable: false,
      }
    } catch (error) {
      this.#increment('liveHighWatermarkUnavailable')
      this.#logger.warn?.('[factory] live subscription high-watermark unavailable', error)
      return {
        routeUnavailable: isHighWatermarkRouteUnavailable(error),
      }
    }
  }

  #scheduleLivePoll(delayMs: number, options: FactoryLiveSubscriptionOptions): void {
    if (this.#livePollTimer || !this.#started) return
    this.#livePollTimer = setTimeout(() => {
      this.#livePollTimer = undefined
      void this.#pollLiveEvents(options).finally(() => {
        if (this.#started) this.#scheduleLivePoll(options.pollIntervalMs, options)
      })
    }, delayMs)
  }

  async #pollLiveEvents(options: FactoryLiveSubscriptionOptions): Promise<void> {
    if (this.#livePollInFlight) return
    this.#livePollInFlight = true
    try {
      let cursor = this.#liveEventCursor
      for (;;) {
        const page = await this.#mount.getEvents({ cursor, limit: options.eventLimit })
        await this.#handleLiveEventsWithYield(page.events)
        const nextCursor = eventCursorAfterPage(cursor, page.events, page.nextCursor)
        this.#liveEventCursor = nextCursor
        if (!page.nextCursor || page.nextCursor === cursor) break
        cursor = page.nextCursor
      }
    } catch (error) {
      this.#logger.warn?.('[factory] live subscription poll failed', error)
    } finally {
      this.#livePollInFlight = false
    }
  }

  #enqueueLiveEvent(event: ChangeEvent): void {
    if (!this.#started) return
    this.#liveEventQueue.push(event)
    this.#scheduleLiveEventDrain()
  }

  #scheduleLiveEventDrain(): void {
    if (this.#liveEventDrainScheduled || this.#liveEventDrainActive || this.#deferLiveEventDrain || !this.#started) {
      return
    }
    this.#liveEventDrainScheduled = true
    void liveEventYield()
      .then(() => {
        this.#liveEventDrainScheduled = false
        return this.#drainLiveEventQueue()
      })
      .catch((error: unknown) => {
        this.#liveEventDrainScheduled = false
        this.#logger.warn?.('[factory] live issue event drain failed', error)
      })
  }

  async #drainLiveEventQueue(): Promise<void> {
    if (this.#liveEventDrainActive) {
      return
    }
    this.#liveEventDrainActive = true
    try {
      await this.#handleLiveEventsWithYield(this.#liveEventQueue)
    } finally {
      this.#liveEventDrainActive = false
      if (this.#liveEventQueue.length > 0 && this.#started) {
        this.#scheduleLiveEventDrain()
      }
    }
  }

  async #handleLiveEventsWithYield(events: ChangeEvent[]): Promise<void> {
    let handled = 0
    const seenIssueKeys = new Set<string>()
    while (events.length > 0 && this.#started) {
      const batch = events.splice(0, LIVE_EVENT_DRAIN_BATCH_SIZE)
      const paths = batch
        .map((event) => this.#prepareLiveEventForDrain(event, seenIssueKeys))
        .filter((path): path is string => Boolean(path))
      await Promise.all(paths.map((path) => this.#handlePreparedLiveChange(path)))
      handled += batch.length
      await this.#refreshLiveHeartbeatIfDue()
      if (events.length > 0) {
        this.#increment('liveEventDrainYields')
        await liveEventYield()
      }
    }
    if (handled === 0) {
      await this.#refreshLiveHeartbeatIfDue()
    }
  }

  async #refreshLiveHeartbeatIfDue(): Promise<void> {
    if (!this.#liveHeartbeatActive) return
    const intervalMs = liveHeartbeatIntervalMs(this.#config.loop.heartbeatStaleMs)
    if (this.#clock.now() - this.#liveHeartbeatLastWriteMs < intervalMs) return
    // A scheduled (real-timer) refresh may already be in flight carrying a
    // timestamp captured before this drain batch advanced the clock. The
    // #liveHeartbeatInFlight guard would otherwise make this due refresh a
    // no-op, leaving the heartbeat stamped stale until the next interval. Wait
    // for that write to settle, then re-check and force a current-timestamped
    // write if we are still due so the heartbeat never falls behind the drain.
    await this.#liveHeartbeatRefresh
    if (this.#clock.now() - this.#liveHeartbeatLastWriteMs < intervalMs) return
    await this.#refreshLiveHeartbeat()
  }

  #prepareLiveEventForDrain(event: ChangeEvent, seenIssueKeys: Set<string>): string | undefined {
    const path = event.resource.path
    // PR change events only matter when the babysitter is enabled; ignore them
    // otherwise so the legacy completion path is untouched.
    const isPullPath = this.#config.babysitter.enabled && isGithubPullFilePath(path)
    if (!isIssueFilePath(path) && !isGithubIssueFilePath(path) && !isPullPath) {
      return undefined
    }

    if (isBeforeLiveCutoff(event.occurredAt, this.#liveConnectStartedAtMs, this.#liveReplaySkewMarginMs)) {
      this.#increment('liveReplayEventsSuppressed')
      this.#increment('liveReplayEventsSuppressedByTime')
      this.#logger.debug?.('[factory] suppressed stale live issue event', {
        id: event.id,
        path,
        occurredAt: event.occurredAt,
        connectStartedAt: new Date(this.#liveConnectStartedAtMs).toISOString(),
        replaySkewMarginMs: this.#liveReplaySkewMarginMs,
      })
      return undefined
    }

    if (isAtOrBeforeHighWatermark(event.id, this.#liveEventHighWatermark)) {
      this.#increment('liveReplayEventsSuppressed')
      this.#increment('liveReplayEventsSuppressedByWatermark')
      this.#logger.debug?.('[factory] suppressed replayed live issue event', {
        id: event.id,
        highWatermark: this.#liveEventHighWatermark,
        path,
      })
      return undefined
    }

    const dedupeKey = liveEventDedupeKey(event)
    if (dedupeKey) {
      if (this.#seenLiveEvents.has(dedupeKey)) {
        this.#increment('liveDuplicateEventsSuppressed')
        this.#logger.debug?.('[factory] suppressed duplicate live issue event', {
          id: event.id,
          path,
        })
        return undefined
      }
      rememberLiveEvent(this.#seenLiveEvents, dedupeKey)
    } else {
      this.#increment('liveEventsMissingIdentity')
      this.#logger.warn?.('[factory] live issue event missing stable identity', { path })
    }

    if (isGithubIssueFilePath(path)) {
      const sourceKey = `github:${path}`
      if (seenIssueKeys.has(sourceKey)) {
        this.#increment('liveDuplicateIssueEventsSuppressed')
        this.#logger.debug?.('[factory] suppressed duplicate live GitHub issue event in current drain', {
          id: event.id,
          path,
        })
        return undefined
      }
      seenIssueKeys.add(sourceKey)
      this.#recordArrivalLatency(event)
      return path
    }

    if (isPullPath) {
      // Dedupe PR change events by path within a drain; the babysitter routing
      // re-derives the issue from the PR head ref downstream.
      const sourceKey = `pull:${path}`
      if (seenIssueKeys.has(sourceKey)) {
        this.#increment('liveDuplicatePrEventsSuppressed')
        return undefined
      }
      seenIssueKeys.add(sourceKey)
      this.#recordArrivalLatency(event)
      return path
    }

    const issueKey = keyFromPath(path)
    if (seenIssueKeys.has(issueKey)) {
      this.#increment('liveDuplicateIssueEventsSuppressed')
      this.#logger.debug?.('[factory] suppressed duplicate live issue event for issue in current drain', {
        id: event.id,
        path,
        issue: issueKey,
      })
      return undefined
    }

    const issueRef = { key: issueKey, uuid: uuidFromPath(path) ?? issueKey, path }
    if (this.#batch.isInFlight(issueRef) || this.#batch.isQueued(issueRef)) {
      this.#increment('liveDuplicateIssueEventsSuppressed')
      this.#logger.debug?.('[factory] suppressed duplicate live issue event for tracked issue', {
        id: event.id,
        path,
        issue: issueKey,
      })
      return undefined
    }

    seenIssueKeys.add(issueKey)
    this.#recordArrivalLatency(event)
    return path
  }

  async #handlePreparedLiveChange(path: string): Promise<void> {
    if (this.#config.babysitter.enabled && isGithubPullFilePath(path)) {
      await this.#handlePrChange(path)
      return
    }
    if (isGithubIssueFilePath(path)) {
      await this.#handleGithubIssueChange(path, { dryRun: this.#config.dryRun })
      return
    }
    await this.#handleChange(path, { requireRealIssue: true })
  }

  #scheduleCompletionSweep(delayMs = COMPLETION_SWEEP_INTERVAL_MS): void {
    if (!this.#started || this.#completionSweepTimer || this.#completionSweepActive) {
      return
    }
    this.#completionSweepTimer = setTimeout(() => {
      this.#completionSweepTimer = undefined
      void this.#sweepPrStateCompletions('live-timer')
        .catch((error: unknown) => {
          this.#increment('completionSweepErrors')
          this.#logger.warn?.('[factory] PR completion sweep failed', error)
        })
        .finally(() => {
          if (this.#started) this.#scheduleCompletionSweep()
        })
    }, delayMs)
    this.#completionSweepTimer.unref?.()
  }

  async #sweepPrStateCompletions(reason: 'live-timer' | 'run-loop'): Promise<void> {
    // When the babysitter owns PR-open, completion is driven by PR webhooks +
    // the babysitter's readiness signal (see #handlePrChange / #handleAgentExit),
    // not this polling sweep. Disabling it here is what makes the babysitter path
    // webhook-driven rather than polled.
    if (this.#config.babysitter.enabled) {
      return
    }
    if (this.#completionSweepActive) {
      return
    }
    this.#completionSweepActive = true
    try {
      const records = this.#batch.inFlight
        .filter((record) => !record.dryRun && !this.#completionInFlight.has(issueKey(record.issue)))
      if (records.length === 0) {
        return
      }

      this.#increment('completionSweepRuns')
      for (let index = 0; index < records.length; index += COMPLETION_SWEEP_BATCH_SIZE) {
        const candidates = await Promise.all(
          records.slice(index, index + COMPLETION_SWEEP_BATCH_SIZE).map(async (record) => {
            const issue = await this.#readIssue(record.issue.path)
            if (!issue || !isInFactoryScope(issue, this.#config.safety)) {
              return undefined
            }
            const pr = await this.#completionPrForIssue(issue)
            if (!pr) {
              this.#increment('completionSweepMissingPr')
              return undefined
            }
            if (pr.draft) {
              this.#increment('completionSweepDraftPr')
              this.#probePrGhBackoffUntilMs.set(issue.key, this.#clock.now() + PROBE_PR_GH_BACKOFF_MS)
              return undefined
            }
            return { record, pr }
          }),
        )

        for (const candidate of candidates) {
          if (!candidate || this.#batch.getIssue(candidate.record.issue) !== candidate.record) {
            continue
          }
          this.#increment('completionSweepCompleted')
          this.#logger.info?.('[factory] PR completion sweep completing issue', {
            issue: candidate.record.issue.key,
            repo: candidate.pr.repo,
            prNumber: candidate.pr.prNumber,
            reason,
          })
          // workaround for relay#1116: agents often exit as worker_exited after opening a PR,
          // so PR state is the primary completion signal that frees the batch slot.
          await this.#completeIssue(candidate.record)
        }

        await this.#refreshLiveHeartbeatIfDue()
        if (index + COMPLETION_SWEEP_BATCH_SIZE < records.length) {
          await liveEventYield()
        }
      }
    } finally {
      this.#completionSweepActive = false
    }
  }

  async #completionPrForIssue(issue: LinearIssue): Promise<ResolvedIssuePr | undefined> {
    if (this.#customProbePrResolver) {
      return this.#probePrResolver(issue)
    }
    return this.#resolveIssuePr(issue, {
      titleMarker: FACTORY_E2E_MARKER,
    })
  }

  async #resolveIssuePr(
    issue: LinearIssue,
    opts: { requireTitleMarker?: boolean; titleMarker?: string } = {},
  ): Promise<ResolvedIssuePr | undefined> {
    const key = issue.key
    const now = this.#clock.now()
    const cached = this.#probePrResolvedCache.get(key)
    if (cached && cached.expiresAtMs > now) {
      return cached.pr
    }

    const mountPr = await resolveIssuePrFromMount(this.#mount, this.#config, issue, opts)
    if (mountPr) {
      return mountPr
    }

    const backoffUntil = this.#probePrGhBackoffUntilMs.get(key) ?? 0
    if (backoffUntil > now) {
      this.#increment('probePrGhBackoffSkips')
      return undefined
    }

    const ghPr = await resolveIssuePrFromGh(this.#probePrGhRunner, this.#config, issue, opts, this.#logger)
    this.#increment('probePrGhResolveAttempts')
    if (ghPr) {
      this.#probePrGhBackoffUntilMs.delete(key)
      if (!ghPr.draft) {
        this.#probePrResolvedCache.set(key, { pr: ghPr, expiresAtMs: now + PROBE_PR_GH_BACKOFF_MS })
      }
      this.#increment('probePrGhResolveHits')
      return ghPr
    }

    this.#probePrGhBackoffUntilMs.set(key, now + PROBE_PR_GH_BACKOFF_MS)
    return undefined
  }

  async runOnce(opts: { dryRun?: boolean } = {}): Promise<IterationReport> {
    const dryRun = opts.dryRun ?? this.#config.dryRun
    await this.#ingestGithubIssues({ dryRun })
    const paths = await this.#readyIssuePaths()
    const pulled: IssueRef[] = []
    const triaged: TriageDecision[] = []
    const dispatched: DispatchResult[] = []
    const skipped: IterationReport['skipped'] = []

    for (const path of paths) {
      const issue = await this.#readIssue(path)
      if (issue) {
        this.#recordCanonicalIssueState(issue)
      }
      if (!issue) {
        continue
      }

      pulled.push(issueRef(issue))
      const dispatchBlock = this.#dispatchBlockReason(issue)
      if (dispatchBlock) {
        skipped.push({ issue: issueRef(issue), reason: dispatchBlock })
        continue
      }

      if (this.#batch.isInFlight(issue) || this.#batch.isQueued(issue)) {
        skipped.push({ issue: issueRef(issue), reason: 'already tracked' })
        continue
      }

      if (issue.stateId !== this.#config.stateIds.readyForAgent) {
        skipped.push({ issue: issueRef(issue), reason: 'live state is not ready-for-agent' })
        continue
      }

      if (!isInFactoryScope(issue, this.#config.safety)) {
        skipped.push({ issue: issueRef(issue), reason: 'not factory-e2e scope' })
        continue
      }

      if (!isRealLinearIssue(issue)) {
        skipped.push({ issue: issueRef(issue), reason: 'not reconciled real Linear issue' })
        continue
      }

      const decision = await this.triageIssue(issue)
      triaged.push(decision)
      const result = await this.dispatch(decision, { dryRun })
      if (result.agents.length === 0 && !dryRun) {
        skipped.push({ issue: decision.issue, reason: 'queued or escalated' })
      } else {
        dispatched.push(result)
      }
    }

    return { pulled, triaged, dispatched, skipped, dryRun, slackDegraded: this.#slackDegraded }
  }

  async runLoop(opts: FactoryLoopRunOptions = {}): Promise<IterationReport[]> {
    const maxIterations = Math.min(5, Math.max(1, Math.trunc(opts.maxIterations ?? this.#config.loop.maxIterations)))
    const maxConsecutiveFailures = Math.min(5, Math.max(1, Math.trunc(
      opts.maxConsecutiveFailures ?? this.#config.loop.maxConsecutiveFailures,
    )))
    const heartbeatPath = opts.heartbeatPath ?? this.#config.loop.heartbeatPath
    const registryPath = opts.registryPath ?? this.#config.loop.registryPath
    const reports: IterationReport[] = []
    let consecutiveFailures = 0
    let completed = false
    try {
      for (let iteration = 0; iteration < maxIterations; iteration += 1) {
        await this.#writeLoopHeartbeat(heartbeatPath, registryPath, 'running', iteration, maxIterations)
        try {
          await this.#sweepPrStateCompletions('run-loop')
          reports.push(await this.runOnce({ dryRun: opts.dryRun }))
          consecutiveFailures = 0
        } catch (error) {
          consecutiveFailures += 1
          this.#increment('loopIterationFailures')
          this.#error(error)
          reports.push(failedIterationReport(error, opts.dryRun ?? this.#config.dryRun))
          await this.#reapDispatchFailureHandoffsNow(heartbeatPath, registryPath)
          await this.#writeLoopHeartbeat(heartbeatPath, registryPath, 'running', iteration + 1, maxIterations)
          if (consecutiveFailures >= maxConsecutiveFailures) {
            this.#increment('loopCircuitBreaks')
            this.#logger.error?.('[factory] stopping loop after consecutive iteration failures', {
              consecutiveFailures,
              maxConsecutiveFailures,
            })
            break
          }
          continue
        }
        await this.#writeLoopHeartbeat(heartbeatPath, registryPath, 'running', iteration + 1, maxIterations)
      }
      this.#increment('loopIdle')
      await this.#writeLoopHeartbeat(heartbeatPath, registryPath, 'idle', reports.length, maxIterations)
      completed = true
      return reports
    } finally {
      if (!completed) {
        await this.#writeLoopHeartbeat(heartbeatPath, registryPath, 'stopping', reports.length, maxIterations)
      }
      await this.stop()
    }
  }

  async triageIssue(issue: LinearIssue): Promise<TriageDecision> {
    return this.#triage.triage(issue, {
      config: this.#config,
      repoMap: repoMapFromConfig(this.#config),
    })
  }

  async dispatch(decision: TriageDecision, opts: { dryRun?: boolean } = {}): Promise<DispatchResult> {
    const dryRun = opts.dryRun ?? this.#config.dryRun
    const existingRecord = this.#batch.getIssue(decision.issue)
    if (existingRecord?.result) {
      return existingRecord.result
    }

    const blockReason = this.#dispatchBlockReason(decision.issue)
    if (blockReason) {
      const error = new Error(`Refusing to dispatch ${decision.issue.key}: ${blockReason}`)
      this.#error(error, decision.issue)
      throw error
    }

    const liveIssue = await this.#readIssue(decision.issue.path)
    if (!liveIssue || !isInFactoryScope(liveIssue, this.#config.safety)) {
      const error = new Error(`Refusing to dispatch ${decision.issue.key}: not factory-e2e scope`)
      this.#error(error, decision.issue)
      throw error
    }

    if (!isRealLinearIssue(liveIssue)) {
      const error = new Error(`Refusing to dispatch ${decision.issue.key}: not reconciled real Linear issue`)
      this.#error(error, decision.issue)
      throw error
    }

    const escalationReason = triageEscalationReason(decision)
    if (escalationReason) {
      await this.#escalateTriageToSlack(decision, escalationReason, dryRun)
      this.#error(new Error(`${escalationReason}; escalation required`), decision.issue)
      return { issue: decision.issue, agents: [], dryRun }
    }

    this.#recordDispatchAttempt(decision.issue)
    const record = this.#batch.start(decision, dryRun)
    if (!record) {
      this.#clearDispatchInFlight(decision.issue)
      this.#increment('queued')
      this.#emit('issue-queued', { issue: decision.issue })
      return { issue: decision.issue, agents: [], dryRun }
    }

    if (record.result) {
      return record.result
    }

    const spawnedForReaperHandoff: RegistryHandoffAgent[] = []
    try {
      const agents: DispatchResult['agents'] = []
      for (const spec of [...decision.implementers, decision.reviewer]) {
        const spawned = await this.#spawnAgent(record, spec, dryRun)
        const tracked = record.agents.get(spawned.name)
        if (tracked) {
          spawnedForReaperHandoff.push({
            issue: record.issue,
            name: spawned.name,
            tracked: cloneTrackedAgent(tracked),
            persistedAtMs: this.#clock.now(),
          })
        }
        agents.push({ name: spawned.name, role: spec.role })
      }
      await this.#writeInFlightRegistry()

      const comment = dispatchComment(decision, agents)
      if (!dryRun) {
        const issue = await this.#readIssue(decision.issue.path)
        if (!issue || issue.stateId !== this.#config.stateIds.readyForAgent) {
          throw new Error(`Live state changed before writeback for ${decision.issue.key}`)
        }
        try {
          await this.#linear.postComment(issue, comment)
        } catch (error) {
          this.#logger.warn?.('[factory] comment writeback skipped', error)
        }
        await this.#linear.setState(issue, this.#config.stateIds.agentImplementing)
        this.#emit('writeback-verified', { issue: decision.issue, path: issue.path })
      }

      const result = {
        issue: decision.issue,
        agents,
        comments: [comment],
        stateId: dryRun ? undefined : this.#config.stateIds.agentImplementing,
        dryRun,
      }
      record.result = result
      this.#increment('dispatched')
      this.#emit('dispatched', { issue: decision.issue, result })
      if (!dryRun) {
        await this.#ensureSlackDispatchThread(record, result)
        await this.#sendImplementerTask(record)
        await this.#sendCriticalReviewerMessage(record)
      }
      return result
    } catch (error) {
      await this.#persistDispatchFailureReaperHandoff(record, spawnedForReaperHandoff)
      this.#recordDispatchFailure(decision.issue)
      this.#batch.abandon(decision.issue)
      this.#error(error, decision.issue)
      throw error
    }
  }

  status(): FactoryStatus {
    return {
      inFlight: this.#batch.inFlight.map((record) => record.issue),
      queued: this.#batch.queued.map((queued) => queued.issue),
      counters: { ...this.#counters },
      slackDegraded: this.#slackDegraded,
      slackDegradedReason: this.#slackDegradedReason,
    }
  }

  on(event: FactoryEvent, listener: Listener): () => void {
    let listeners = this.#listeners.get(event)
    if (!listeners) {
      listeners = new Set()
      this.#listeners.set(event, listeners)
    }
    listeners.add(listener)
    return () => {
      listeners?.delete(listener)
    }
  }

  #wireFleetEvents(): void {
    if (!this.#offAgentExit) {
      this.#offAgentExit = this.#fleet.onAgentExit((name, reason) => {
        void this.#handleAgentExit(name, reason)
      })
    }
    if (!this.#offDeliveryFailed) {
      this.#offDeliveryFailed = this.#fleet.onDeliveryFailed?.((info) => {
        void this.#handleDeliveryFailed(info)
      })
    }
    if (!this.#offAgentMessage) {
      this.#offAgentMessage = this.#fleet.onAgentMessage?.((message) => {
        void this.#handleAgentMessage(message)
      })
    }
  }

  async #backfillReadyIssues(): Promise<void> {
    const page = await this.#mount.getEvents({ limit: READY_EVENTS_LIMIT })
    const eventPaths = page.events.map((event) => event.resource.path).filter(isIssueFilePath)
    const githubEventPaths = page.events.map((event) => event.resource.path).filter(isGithubIssueFilePath)
    for (const path of new Set(githubEventPaths)) {
      await this.#handleGithubIssueChange(path, { dryRun: this.#config.dryRun })
    }
    await this.#ingestGithubIssues({ dryRun: this.#config.dryRun })
    const treePaths = await this.#readyIssuePaths()
    for (const path of new Set([...eventPaths, ...treePaths])) {
      await this.#handleChange(path)
    }
  }

  async #handleChange(path: string, opts: { requireRealIssue?: boolean } = {}): Promise<void> {
    if (!isIssueFilePath(path)) {
      return
    }

    try {
      const issue = await this.#readIssue(path)
      if (issue) {
        this.#recordCanonicalIssueState(issue)
      }
      if (issue?.stateId !== this.#config.stateIds.readyForAgent) {
        return
      }

      if (opts.requireRealIssue && !isRealLinearIssue(issue)) {
        return
      }

      if (!isInFactoryScope(issue, this.#config.safety)) {
        return
      }

      if (!isRealLinearIssue(issue)) {
        return
      }

      if (this.#batch.isInFlight(issue) || this.#batch.isQueued(issue)) {
        return
      }

      if (this.#dispatchBlockReason(issue)) {
        return
      }

      const decision = await this.triageIssue(issue)
      const escalationReason = triageEscalationReason(decision)
      if (escalationReason) {
        await this.#escalateTriageToSlack(decision, escalationReason, this.#config.dryRun)
        this.#error(new Error(`${escalationReason}; escalation required`), decision.issue)
        return
      }

      if (this.#batch.canStart()) {
        await this.dispatch(decision, { dryRun: this.#config.dryRun })
      } else {
        if (this.#batch.queue(decision, this.#config.dryRun)) {
          this.#emit('issue-queued', { issue: decision.issue })
        }
      }
    } catch (error) {
      this.#logger.error?.('[factory] failed to handle issue change', error)
    }
  }

  // Probes the GitHub issue sub-root at most once and caches the verdict so
  // repeated iterations skip listTree calls when the mount is absent.
  async #ensureGithubIngestionReady(): Promise<boolean> {
    if (this.#githubIngestionEnabled !== undefined) {
      return this.#githubIngestionEnabled
    }
    const githubReady = await this.#mount.ensureSubRoot(GITHUB_ISSUE_ROOT, { timeoutMs: 90_000 })
    this.#githubIngestionEnabled = githubReady === 'ready'
    if (!this.#githubIngestionEnabled) {
      this.#logger.warn?.(`[factory] ${GITHUB_ISSUE_ROOT} sub-root is not mounted; GitHub issue ingestion disabled`)
    }
    return this.#githubIngestionEnabled
  }

  async #ingestGithubIssues(opts: { dryRun?: boolean } = {}): Promise<void> {
    if (!await this.#ensureGithubIngestionReady()) {
      return
    }
    // Load the existing Linear mirror candidates once for the whole pass so
    // dedupe stays O(N + M) reads instead of re-scanning ISSUE_ROOT for every
    // GitHub issue (gemini perf finding on #findGithubIssueMirror).
    const candidates = this.#newMirrorCandidateCache()
    for (const path of await this.#githubIssuePaths()) {
      await this.#handleGithubIssueChange(path, { ...opts, candidates })
    }
  }

  // Lazily lists+reads ISSUE_ROOT mirror candidates at most once, then memoizes
  // them for reuse across every GitHub issue handled in the same pass.
  #newMirrorCandidateCache(): MirrorCandidateCache {
    let loaded: Promise<LinearIssue[]> | undefined
    return {
      load: () => {
        if (!loaded) {
          loaded = this.#loadLinearMirrorCandidates()
        }
        return loaded
      },
    }
  }

  async #loadLinearMirrorCandidates(): Promise<LinearIssue[]> {
    const candidates: LinearIssue[] = []
    for (const path of await this.#mount.listTree(ISSUE_ROOT)) {
      if (!isLinearIssueMirrorCandidatePath(path)) {
        continue
      }
      const issue = await this.#readIssue(path)
      if (issue) {
        candidates.push(issue)
      }
    }
    return candidates
  }

  async #githubIssuePaths(): Promise<string[]> {
    try {
      const paths = await this.#mount.listTree(GITHUB_ISSUE_ROOT)
      const issuePaths: string[] = []
      for (const path of paths) {
        if (githubIssuePathParts(path) !== undefined) {
          issuePaths.push(path)
        } else if (githubIssueDirectoryPathParts(path) !== undefined) {
          // listTree returns the issue directory entry alongside its
          // meta.json file; githubIssuePathParts() already collected the
          // file, so skip the directory to avoid reading the same issue
          // twice in one backfill pass. Directory paths are only meaningful
          // for live change events, not the tree scan.
          continue
        } else if (isGithubIssueTreePath(path)) {
          this.#increment('githubIssuesIgnoredByPathRegex')
          this.#logger.debug?.('[factory] ignored GitHub issue path with unsupported relayfile shape', { path })
        }
      }
      return issuePaths.sort()
    } catch (error) {
      this.#increment('githubIssueListFailures')
      this.#logger.warn?.('[factory] failed to list GitHub issue source tree', error)
      return []
    }
  }

  async #handleGithubIssueChange(
    path: string,
    opts: { dryRun?: boolean; candidates?: MirrorCandidateCache } = {},
  ): Promise<void> {
    if (this.#githubIngestionEnabled === false || !isGithubIssueFilePath(path)) {
      return
    }

    try {
      const ghIssue = await this.#readGithubIssue(path)
      if (!ghIssue) {
        return
      }

      if (githubIssueIsClosed(ghIssue)) {
        const mirror = await this.#findGithubIssueMirror(ghIssue, opts.candidates)
        if (mirror && mirror.stateId !== this.#config.stateIds.done) {
          if (!opts.dryRun) {
            const record = this.#batch.getIssue(mirror)
            if (record) {
              await this.#completeIssue(record)
            } else {
              await this.#linear.setState(mirror, this.#config.stateIds.done)
            }
          }
          this.#increment('githubIssueMirrorsClosed')
        }
        return
      }

      if (!githubIssueHasFactoryLabel(ghIssue)) {
        return
      }

      const mirror = await this.#findGithubIssueMirror(ghIssue, opts.candidates)
      if (mirror) {
        this.#increment('githubIssueMirrorsDeduped')
        return
      }

      const repoLabel = this.#repoLabelForGithubIssue(ghIssue)
      if (!repoLabel) {
        this.#increment('githubIssueMirrorsSkippedUnroutable')
        this.#logger.warn?.('[factory] skipped factory-labeled GitHub issue without repos.byLabel route', {
          path,
          repo: ghIssue.repo,
          url: ghIssue.url,
        })
        return
      }

      if (!opts.dryRun) {
        await this.#linear.createIssue(githubIssueMirrorPayload(ghIssue, repoLabel, this.#config))
      }
      this.#increment('githubIssueMirrorsCreated')
    } catch (error) {
      this.#logger.error?.('[factory] failed to ingest GitHub issue', error)
    }
  }

  async #readGithubIssue(path: string): Promise<GithubIssueSource | undefined> {
    const candidatePaths = githubIssueReadCandidatePaths(path)
    try {
      for (const candidatePath of candidatePaths) {
        try {
          const { content } = await this.#mount.readFile(candidatePath)
          return parseGithubIssue(candidatePath, content)
        } catch (error) {
          if (isMissingIssueFileError(error) && candidatePath !== candidatePaths.at(-1)) {
            continue
          }
          throw error
        }
      }
    } catch (error) {
      if (isMissingIssueFileError(error)) {
        this.#increment('githubIssuePhantomSkipped')
        this.#logger.debug?.('[factory] skipped missing GitHub issue file discovered from issue tree', { path })
        return undefined
      }
      throw error
    }
  }

  async #findGithubIssueMirror(
    ghIssue: GithubIssueSource,
    candidates?: MirrorCandidateCache,
  ): Promise<LinearIssue | undefined> {
    const draftPath = githubIssueMirrorDraftPath(ghIssue)
    try {
      return parseLinearIssue(draftPath, (await this.#mount.readFile(draftPath)).content)
    } catch {
      // The draft path only exists before the Linear provider reconciles the
      // create writeback into a canonical AR-* issue. Fall through to persisted
      // source metadata on existing Linear issues for restart/replay dedupe.
    }

    // Cached resolved mirror path: skip the full ISSUE_ROOT scan for mirrors we
    // already located on an earlier cycle. Re-validate the cached path and fall
    // back to a scan if the mirror was deleted or renamed.
    const cacheKey = githubIssueMirrorId(ghIssue)
    const cachedPath = this.#githubMirrorPathCache.get(cacheKey)
    if (cachedPath) {
      const cached = await this.#readIssue(cachedPath)
      if (cached && linearIssueMirrorsGithubIssue(cached, ghIssue)) {
        return cached
      }
      this.#githubMirrorPathCache.delete(cacheKey)
    }

    // During a full ingestion pass the candidate list is loaded once and shared
    // across every GitHub issue; the live single-event path passes none and
    // loads on demand.
    const mirrorCandidates = candidates
      ? await candidates.load()
      : await this.#loadLinearMirrorCandidates()
    const mirror = mirrorCandidates.find((issue) => linearIssueMirrorsGithubIssue(issue, ghIssue))
    if (mirror) {
      this.#githubMirrorPathCache.set(cacheKey, mirror.path)
    }
    return mirror
  }

  #repoLabelForGithubIssue(ghIssue: GithubIssueSource): string | undefined {
    const fullName = ghIssue.repo.toLowerCase()
    const bareName = ghIssue.repoName.toLowerCase()
    const entry = Object.entries(this.#config.repos.byLabel)
      .find(([, repo]) => {
        const configured = repo.toLowerCase()
        // ghIssue.repo is always owner/name, so byLabel entries configured with
        // just the bare repo name still match via repoName.
        return configured === fullName || configured === bareName
      })
    return entry?.[0]
  }

  #dispatchBlockReason(issue: IssueRef): string | undefined {
    const key = issue.key
    const state = this.#dispatchAttempts.get(key)
    if (!state) return undefined
    if (state.terminal) return 'dispatch already terminal'
    if (state.inFlight) return 'dispatch already in-flight'
    const now = this.#clock.now()
    if (state.backoffUntilMs > now) {
      return 'dispatch backoff active'
    }
    if (state.attempts >= this.#config.dispatch.maxAttempts) {
      state.terminal = true
      return 'dispatch retry limit reached'
    }
    return undefined
  }

  #recordDispatchAttempt(issue: IssueRef): void {
    const key = issue.key
    const state = this.#dispatchAttempts.get(key) ?? {
      attempts: 0,
      inFlight: false,
      terminal: false,
      backoffUntilMs: 0,
    }
    state.attempts += 1
    state.inFlight = true
    state.backoffUntilMs = 0
    this.#dispatchAttempts.set(key, state)
  }

  #clearDispatchInFlight(issue: IssueRef): void {
    const state = this.#dispatchAttempts.get(issue.key)
    if (state) state.inFlight = false
  }

  #recordDispatchFailure(issue: IssueRef): void {
    const state = this.#dispatchAttempts.get(issue.key)
    if (!state) return
    state.inFlight = false
    if (state.attempts >= this.#config.dispatch.maxAttempts) {
      state.terminal = true
      state.backoffUntilMs = 0
      this.#increment('dispatchTerminalFailures')
      return
    }
    state.backoffUntilMs = this.#clock.now() + this.#config.dispatch.errorCooldownMs
    this.#increment('dispatchBackoffs')
  }

  #recordDispatchTerminal(issue: IssueRef): void {
    const state = this.#dispatchAttempts.get(issue.key) ?? {
      attempts: 0,
      inFlight: false,
      terminal: false,
      backoffUntilMs: 0,
    }
    state.inFlight = false
    state.terminal = true
    state.backoffUntilMs = 0
    this.#dispatchAttempts.set(issue.key, state)
  }

  #recordCanonicalIssueState(issue: Pick<LinearIssue, 'key' | 'stateId'>): void {
    const previousStateId = this.#canonicalIssueStates.get(issue.key)
    if (previousStateId === this.#config.stateIds.done && issue.stateId === this.#config.stateIds.readyForAgent) {
      const dispatchState = this.#dispatchAttempts.get(issue.key)
      if (dispatchState?.terminal) {
        dispatchState.attempts = 0
        dispatchState.inFlight = false
        dispatchState.terminal = false
        dispatchState.backoffUntilMs = 0
        this.#increment('dispatchTerminalReopened')
      }
    }
    this.#canonicalIssueStates.set(issue.key, issue.stateId)
  }

  async #writeLoopHeartbeat(
    path: string,
    registryPath: string,
    status: FactoryLoopHeartbeat['status'],
    iteration: number,
    maxIterations: number,
  ): Promise<void> {
    const updatedAtMs = this.#clock.now()
    const heartbeat: FactoryLoopHeartbeat = {
      pid: process.pid,
      status,
      iteration,
      maxIterations,
      updatedAt: new Date(updatedAtMs).toISOString(),
      updatedAtMs,
      registryPath,
    }
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, `${JSON.stringify(heartbeat, null, 2)}\n`, 'utf8')
    await this.#writeInFlightRegistry(registryPath, path)
  }

  async #reapDispatchFailureHandoffsNow(heartbeatPath: string, registryPath: string): Promise<void> {
    if (this.#dispatchFailureReaperHandoffs.size === 0) {
      return
    }

    try {
      const protectedPids = await this.#protectedPids()
      let registryChanged = false
      for (const [key, handoff] of [...this.#dispatchFailureReaperHandoffs]) {
        const roots = await this.#terminationRoots(handoff.name, handoff.tracked, protectedPids)
        if (roots.pids.length === 0 && roots.status === 'unresolved') {
          const unresolvedAgeMs = this.#clock.now() - handoff.persistedAtMs
          this.#increment('agentTerminateMissingPid')
          this.#logger.error?.('[factory] no pid available to reap dispatch-failed handoff during loop catch', {
            agentName: handoff.name,
            issue: handoff.issue,
            sessionRef: handoff.tracked.sessionRef,
            unresolvedAgeMs,
          })
          if (unresolvedAgeMs >= DISPATCH_FAILURE_HANDOFF_UNRESOLVED_TTL_MS) {
            this.#dispatchFailureReaperHandoffs.delete(key)
            registryChanged = true
            this.#increment('dispatchFailureReaperHandoffsDroppedStaleUnresolved')
            this.#logger.warn?.('[factory] dropped stale unresolved dispatch-failed handoff', {
              agentName: handoff.name,
              issue: handoff.issue,
              unresolvedAgeMs,
              ttlMs: DISPATCH_FAILURE_HANDOFF_UNRESOLVED_TTL_MS,
            })
            try {
              await this.#fleet.release(handoff.name, 'dispatch failed')
            } catch (error) {
              this.#logger.warn?.('[factory] failed to release unresolved dispatch-failure handoff after pruning', {
                agentName: handoff.name,
                error,
              })
            }
          }
          continue
        }

        let blockingSkip = false
        if (roots.pids.length > 0) {
          const report = await terminatePids(roots.pids, {
            kill: this.#kill,
            readChildPids: this.#readChildPids,
            sleep: this.#clock.sleep,
            termGraceMs: this.#terminationGraceMs,
            protectedPids,
          })
          if (report.terminated.length > 0) {
            this.#increment('loopDispatchFailureHandoffsReaped')
          }
          for (const skipped of report.skipped) {
            if (skipped.reason !== 'pid not running') {
              blockingSkip = true
              this.#logger.warn?.('[factory] dispatch-failure handoff reap skipped during loop catch', {
                ...skipped,
                agentName: handoff.name,
              })
            }
          }
        }

        if (!blockingSkip) {
          this.#dispatchFailureReaperHandoffs.delete(key)
          registryChanged = true
          try {
            await this.#fleet.release(handoff.name, 'dispatch failed')
          } catch (error) {
            this.#logger.warn?.(`[factory] failed to release ${handoff.name} after dispatch-failure reap`, error)
          }
        }
      }
      if (registryChanged) {
        await this.#writeInFlightRegistry(registryPath, heartbeatPath)
      }
    } catch (error) {
      this.#increment('loopDispatchFailureHandoffReapFailures')
      this.#error(error)
    }
  }

  async #readyIssuePaths(): Promise<string[]> {
    const pathsByKey = new Map<string, string>()
    const canonicalPathsByKey = new Map<string, string>()
    for (const path of await this.#mount.listTree(ISSUE_ROOT)) {
      if (isIssueFilePath(path)) {
        const key = keyFromPath(path)
        canonicalPathsByKey.set(key, path)
        pathsByKey.set(key, path)
      }
    }
    for (const path of await this.#mount.listTree(linearByStatePath('ready-for-agent'))) {
      if (isIssueAliasFilePath(path)) {
        const canonicalPath = canonicalPathsByKey.get(keyFromPath(path))
        if (canonicalPath) {
          pathsByKey.set(keyFromPath(path), canonicalPath)
        } else {
          this.#increment('readyAliasesWithoutCanonical')
          this.#logger.debug?.('[factory] skipped ready alias without canonical issue', { path })
        }
      }
    }
    return [...pathsByKey.values()].sort()
  }

  async #readIssue(path: string): Promise<LinearIssue | undefined> {
    try {
      const { content } = await this.#mount.readFile(path)
      return parseLinearIssue(path, content)
    } catch (error) {
      if (isMissingIssueFileError(error) && isIssuePathUnderRoot(path)) {
        this.#increment('phantomSkipped')
        this.#logger.debug?.('[factory] skipped missing issue file discovered from issue tree', { path })
        return undefined
      }
      this.#logger.warn?.(`Unable to read issue ${path}`, error)
      return undefined
    }
  }

  async #releaseInFlightAgents(reason: string): Promise<void> {
    const agents = new Map<string, TrackedAgent>()
    for (const record of this.#batch.inFlight) {
      if (record.dryRun) {
        continue
      }
      for (const [agentName, tracked] of record.agents) {
        agents.set(agentName, tracked)
      }
    }

    await this.#releaseAndTerminateAgents([...agents], reason, 'stop')
    await this.#writeInFlightRegistry(undefined, undefined, true)
  }

  async #releaseAndTerminateAgents(
    agents: Array<[string, TrackedAgent]>,
    reason: string,
    context: 'stop' | 'completion',
  ): Promise<void> {
    const protectedPids = await this.#protectedPids()
    for (const [agentName, tracked] of agents) {
      if (context === 'stop') {
        await this.#refreshStoppingHeartbeat()
      }
      const roots = await this.#terminationRoots(agentName, tracked, protectedPids)
      if (roots.pids.length === 0 && roots.status === 'unresolved') {
        this.#increment('agentTerminateMissingPid')
        this.#logger.error?.(`[factory] no pid available to terminate ${agentName} during ${context}`, {
          agentName,
          reason,
          sessionRef: tracked.sessionRef,
        })
      }

      if (roots.pids.length > 0) {
        const report = await terminatePids(roots.pids, {
          kill: this.#kill,
          readChildPids: this.#readChildPids,
          sleep: this.#clock.sleep,
          termGraceMs: this.#terminationGraceMs,
          protectedPids,
        })
        for (const skipped of report.skipped) {
          if (skipped.reason !== 'pid not running') {
            this.#logger.warn?.(`[factory] failed to terminate pid ${skipped.pid} for ${agentName} during ${context}`, skipped.reason)
          }
        }
      }
      if (context === 'stop') {
        await this.#refreshStoppingHeartbeat()
      }

      try {
        await this.#fleet.release(agentName, reason)
      } catch (error) {
        this.#logger.warn?.(`[factory] failed to release ${agentName} during ${context}`, error)
      }
      if (context === 'stop') {
        await this.#refreshStoppingHeartbeat()
      }
    }
  }

  async #terminationRoots(agentName: string, tracked: TrackedAgent, protectedPids: number[] = []): Promise<TerminationRoots> {
    const pids = pidsFromSpawnResult(tracked.result)
    if (!this.#fleet.resolveAgentPid) {
      return pids.length > 0 ? { pids, status: 'found' } : { pids: [], status: 'unresolved' }
    }

    const scan = await this.#processFinder(agentName, { protectedPids })
    if (
      scan.status === 'found' &&
      Number.isInteger(scan.identity.pid) &&
      scan.identity.pid > 0 &&
      scan.identity.cmdline.includes(agentName)
    ) {
      return { pids: [scan.identity.pid], status: 'found' }
    }
    if (scan.status === 'ambiguous') {
      this.#logger.warn?.(`[factory] ambiguous process lookup for ${agentName}`)
      return { pids: [], status: 'unresolved' }
    }

    if (pids.length > 0) {
      return { pids, status: 'found' }
    }

    try {
      const resolution = await this.#fleet.resolveAgentPid?.(agentName)
      if (!resolution) {
        return { pids: [], status: 'unresolved' }
      }
      if (resolution.status === 'found' && Number.isInteger(resolution.pid) && resolution.pid > 0) {
        return { pids: [resolution.pid], status: 'found' }
      }
      if (resolution.status === 'unresolved' && scan.status === 'missing') {
        return { pids: [], status: 'missing' }
      }
      return { pids: [], status: resolution.status }
    } catch (error) {
      this.#logger.warn?.(`[factory] failed to resolve pid for ${agentName}`, error)
      return { pids: [], status: 'unresolved' }
    }
  }

  async #protectedPids(): Promise<number[]> {
    try {
      return await this.#fleet.protectedPids?.() ?? []
    } catch (error) {
      this.#logger.warn?.('[factory] failed to resolve protected fleet pids', error)
      return []
    }
  }

  async #persistDispatchFailureReaperHandoff(record: InFlightIssue, handoffAgents: RegistryHandoffAgent[]): Promise<void> {
    if (record.dryRun || handoffAgents.length === 0) {
      return
    }

    try {
      for (const agent of handoffAgents) {
        this.#dispatchFailureReaperHandoffs.set(registryHandoffKey(agent.issue, agent.name), agent)
      }
      await this.#writeInFlightRegistry()
      this.#increment('dispatchFailureReaperHandoffs')
      this.#logger.warn?.('[factory] persisted dispatch-failed agents for orphan reaper', {
        issue: record.issue,
        agents: handoffAgents.map((agent) => agent.name).sort(),
      })
    } catch (error) {
      this.#increment('dispatchFailureReaperHandoffFailures')
      for (const agent of handoffAgents) {
        this.#dispatchFailureReaperHandoffs.delete(registryHandoffKey(agent.issue, agent.name))
      }
      this.#logger.error?.('[factory] failed to persist dispatch-failed agents for orphan reaper', {
        issue: record.issue,
        error,
      })
      this.#error(error, record.issue)
    }
  }

  async #writeInFlightRegistry(
    path = this.#config.loop.registryPath,
    heartbeatPath = this.#config.loop.heartbeatPath,
    empty = false,
  ): Promise<void> {
    const updatedAtMs = this.#clock.now()
    const agents: FactoryInFlightRegistryAgent[] = []
    const seenAgents = new Set<string>()
    const appendAgent = async (issue: IssueRef, agentName: string, tracked: TrackedAgent): Promise<void> => {
      const key = registryHandoffKey(issue, agentName)
      if (seenAgents.has(key)) {
        return
      }
      seenAgents.add(key)
      const { pids } = await this.#terminationRoots(agentName, tracked)
      const processes = []
      for (const pid of pids) {
        const identity = await this.#processIdentityReader(pid)
        if (identity && identity.cmdline.includes(agentName)) {
          processes.push({ ...identity, agentName })
        }
      }
      agents.push({
        name: agentName,
        role: tracked.spec.role,
        issue,
        sessionRef: tracked.sessionRef,
        pids,
        processes,
      })
    }

    if (!empty) {
      for (const record of this.#batch.inFlight) {
        if (record.dryRun) continue
        for (const [agentName, tracked] of record.agents) {
          await appendAgent(record.issue, agentName, tracked)
        }
      }
    }
    for (const agent of this.#dispatchFailureReaperHandoffs.values()) {
      await appendAgent(agent.issue, agent.name, agent.tracked)
    }

    const registry: FactoryInFlightRegistry = {
      pid: process.pid,
      heartbeatPath,
      updatedAt: new Date(updatedAtMs).toISOString(),
      updatedAtMs,
      agents,
    }
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, `${JSON.stringify(registry, null, 2)}\n`, 'utf8')
  }

  async #spawnAgent(record: InFlightIssue, spec: AgentSpec, dryRun: boolean): Promise<{ name: string }> {
    const invocationId = this.#batch.invocationIdFor(record.issue, spec)
    const existing = record.agents.get(spec.name)
    if (existing) {
      return { name: existing.result?.name ?? spec.name }
    }

    if (!this.#batch.shouldSpawn(record, invocationId)) {
      return { name: spec.name }
    }

    if (dryRun) {
      this.#batch.recordDryRun(record, spec, invocationId)
      return { name: spec.name }
    }

    let roster
    try {
      roster = await this.#fleet.roster()
    } catch (error) {
      throw contextualError(`Dispatch roster lookup failed for ${record.issue.key}`, error)
    }
    if (roster.agents.some((agent) => agent.name === spec.name)) {
      this.#batch.recordSpawn(record, spec, invocationId, { name: spec.name, sessionRef: spec.sessionRef })
      return { name: spec.name }
    }

    let result
    try {
      result = await this.#fleet.spawn({
        name: spec.name,
        capability: spec.capability,
        node: spec.node ?? 'self',
        task: spec.task,
        model: spec.model,
        cwd: spec.clonePath,
        sessionRef: spec.sessionRef,
        invocationId,
        restartPolicy: spec.restartPolicy ?? defaultRestartPolicy(spec),
        channel: spec.channel,
      })
    } catch (error) {
      throw contextualError(
        `Dispatch spawn failed for ${record.issue.key}/${spec.name} (${spec.capability}) cwd=${spec.clonePath ?? 'default'}`,
        error,
      )
    }
    this.#batch.recordSpawn(record, spec, invocationId, result)
    return { name: result.name }
  }

  async #handleAgentExit(name: string, reason?: string): Promise<void> {
    if (this.#stopping) {
      return
    }

    const record = this.#batch.getIssueByAgent(name)
    if (!record) {
      return
    }

    const exiting = record.agents.get(name)

    if (isCompletionReason(reason)) {
      if (this.#config.babysitter.enabled && !record.dryRun) {
        // Babysitter path: an implementer/reviewer finishing does NOT mark the
        // issue done — it hands the open PR to the babysitter. The babysitter
        // itself finishing means it believes the PR is ready, so re-check and
        // advance to Human Review.
        if (exiting?.spec.role === 'babysitter') {
          await this.#maybeAdvanceToHumanReview(record)
        } else {
          await this.#ensureBabysitterForIssue(record)
        }
        return
      }
      await this.#completeIssue(record)
      return
    }

    const tracked = exiting
    if (!tracked || record.dryRun) {
      return
    }

    try {
      if (tracked.spec.role === 'implementer' && await this.#issueHasCompletionPr(record)) {
        if (this.#config.babysitter.enabled) {
          await this.#ensureBabysitterForIssue(record)
          return
        }
        await this.#completeIssue(record)
        return
      }

      if (tracked.sessionRef) {
        const resumeKey = `${issueKey(record.issue)}:${name}:${tracked.sessionRef}`
        if (this.#resumedExitKeys.has(resumeKey)) {
          return
        }

        const existing = this.#resumeInFlight.get(resumeKey)
        if (existing) {
          await existing
          return
        }

        const resume = this.#resumeTrackedAgent(record, name, tracked)
        this.#resumeInFlight.set(resumeKey, resume)
        try {
          await resume
          this.#resumedExitKeys.add(resumeKey)
        } catch (error) {
          if (isAgentAlreadyExistsError(error)) {
            // The broker never released this agent's name on exit
            // (relay#1116-family), so re-registering collides with the stuck
            // name. The error is marked retryable but isn't — retrying just
            // re-collides forever. Treat it as terminal for this name: record
            // the resume key so subsequent exit events short-circuit, count it,
            // and warn once instead of spamming a 500 stack trace. The external
            // reaper / a broker restart reclaims the leaked name.
            this.#resumedExitKeys.add(resumeKey)
            this.#increment('resumeNameCollisions')
            this.#logger.warn?.('[factory] resume skipped: broker still holds agent name (relay#1116); not retrying', {
              issue: record.issue.key,
              name,
            })
          } else {
            throw error
          }
        } finally {
          this.#resumeInFlight.delete(resumeKey)
        }
      } else {
        const invocationId = `${this.#batch.invocationIdFor(record.issue, tracked.spec)}:restart:${this.#clock.now()}`
        const result = await this.#fleet.spawn({
          name: tracked.spec.name,
          capability: tracked.spec.capability,
          node: tracked.spec.node ?? 'self',
          task: tracked.spec.task,
          model: tracked.spec.model,
          cwd: tracked.spec.clonePath,
          sessionRef: tracked.spec.sessionRef,
          invocationId,
          restartPolicy: defaultRestartPolicy(tracked.spec),
          channel: tracked.spec.channel,
        })
        this.#batch.recordSpawn(record, tracked.spec, invocationId, result)
      }
    } catch (error) {
      this.#error(error, record.issue)
    }
  }

  async #issueHasCompletionPr(record: InFlightIssue): Promise<boolean> {
    try {
      const issue = await this.#readIssue(record.issue.path)
      if (!issue) {
        return false
      }
      // Only a NON-DRAFT (ready) PR counts as completion. A draft PR means the
      // work isn't review-ready, so an implementer exiting with only a draft PR
      // must NOT mark the issue done / release agents — mirror the
      // #sweepPrStateCompletions draft guard, which keeps draft-PR issues in flight.
      const pr = await this.#completionPrForIssue(issue)
      return Boolean(pr && !pr.draft)
    } catch (error) {
      this.#logger.warn?.('[factory] PR probe failed after implementer exit; preserving restart behavior', {
        issue: record.issue.key,
        error: describeError(error).errorMessage,
      })
      this.#increment('exitPrProbeFailures')
      return false
    }
  }

  async #resumeTrackedAgent(
    record: InFlightIssue,
    name: string,
    tracked: NonNullable<ReturnType<InFlightIssue['agents']['get']>>,
  ): Promise<void> {
    if (!tracked.sessionRef) {
      return
    }

    const result = await this.#fleet.resume({
      name,
      sessionRef: tracked.sessionRef,
      node: tracked.spec.node ?? 'self',
      capability: tracked.spec.capability,
    })
    tracked.result = result
    tracked.sessionRef = result.sessionRef ?? tracked.sessionRef
    record.agents.delete(name)
    record.agents.set(result.name, tracked)
  }

  async #handleDeliveryFailed(info: { to: string; msgId?: string; reason?: string }): Promise<void> {
    const critical = this.#criticalMessages.get(info.msgId ?? '')
    const record = this.#batch.getIssueByAgent(info.to)
    const issue = critical?.issue ?? record?.issue
    const error = new Error(`Critical delivery failed to ${info.to}${info.reason ? `: ${info.reason}` : ''}`)
    this.#error(error, issue)

    if (critical && this.#fleet.waitForInjected) {
      try {
        const ack = await this.#waitForInjectedAndSubmit(critical.input)
        this.#criticalMessages.set(ack.eventId, critical)
      } catch (retryError) {
        this.#error(retryError, critical.issue)
      }
    }
  }

  async #handleAgentMessage(message: AgentMessage): Promise<void> {
    // The babysitter signals "PR is green" by DMing factory. Confirm with an
    // authoritative readiness read before advancing to Human Review.
    if (this.#config.babysitter.enabled && isFactoryQuestionTarget(message.target)) {
      const ready = parsePrReadySignal(message)
      if (ready) {
        const record = this.#batch.getIssueByAgent(ready.agentName)
        if (!record || record.dryRun) {
          this.#increment('prReadySignalsIgnoredNoInFlight')
          return
        }
        if (ready.issueKey && ready.issueKey !== record.issue.key) {
          this.#increment('prReadySignalsIgnoredIssueMismatch')
          return
        }
        await this.#maybeAdvanceToHumanReview(record)
        return
      }
    }

    const question = parseAgentQuestion(message)
    if (!question || !isFactoryQuestionTarget(message.target)) {
      return
    }

    const record = this.#batch.getIssueByAgent(question.agentName)
    if (!record || record.dryRun) {
      this.#increment('agentQuestionsIgnoredNoInFlight')
      return
    }

    if (question.issueKey && question.issueKey !== record.issue.key) {
      this.#increment('agentQuestionsIgnoredIssueMismatch')
      this.#logger.warn?.('[factory] ignored agent question for mismatched issue', {
        from: question.agentName,
        requestedIssue: question.issueKey,
        activeIssue: record.issue.key,
      })
      return
    }

    const dedupeKey = agentQuestionDedupeKey(record.issue, question)
    if (this.#seenAgentQuestionKeys.has(dedupeKey)) {
      this.#increment('agentQuestionDuplicatesSuppressed')
      this.#logger.debug?.('[factory] suppressed duplicate agent question', {
        from: question.agentName,
        issue: record.issue.key,
      })
      return
    }
    this.#rememberAgentQuestion(dedupeKey)

    if (!question.eventId) {
      this.#increment('agentQuestionsMissingIdentity')
      this.#logger.warn?.('[factory] agent question event missing stable identity; falling back to sender/content dedupe', {
        from: question.agentName,
        issue: record.issue.key,
      })
    }

    await this.#postAgentQuestionToSlack(record, question)
  }

  async #postAgentQuestionToSlack(record: InFlightIssue, question: AgentQuestion): Promise<void> {
    if (!this.#slack || !this.#config.slack) {
      this.#increment('agentQuestionsSkippedNoSlack')
      this.#logger.warn?.('[factory] agent question has no Slack channel configured', {
        issue: record.issue,
        from: question.agentName,
      })
      return
    }

    if (await this.#shouldSkipSlackWriteback('agent-question')) {
      this.#increment('agentQuestionsSkippedSlackDegraded')
      return
    }

    const key = issueKey(record.issue)
    try {
      await this.#slackWatcherStarts.get(key)
    } catch {
      // The initiator logs Slack watcher startup failures.
    }

    const threadId = this.#slackThreadIds.get(key)
    if (!threadId) {
      this.#increment('agentQuestionsSkippedMissingThread')
      this.#logger.warn?.('[factory] agent question has no Slack dispatch thread', {
        issue: record.issue,
        from: question.agentName,
      })
      return
    }

    try {
      await this.#slack.reply(threadId, agentQuestionSlackText(record.issue, question))
      this.#increment('agentQuestionsPostedToSlack')
      this.#recordSlackWritebackSuccess('agent-question')
    } catch (error) {
      this.#markSlackWritebackFailure('agent-question', error)
      this.#logger.warn?.(`[factory] failed to post agent question for ${record.issue.key}`, error)
    }
  }

  #rememberAgentQuestion(key: string): void {
    if (this.#seenAgentQuestionKeys.has(key)) {
      return
    }
    this.#seenAgentQuestionKeys.add(key)
    this.#seenAgentQuestionOrder.push(key)
    while (this.#seenAgentQuestionOrder.length > AGENT_QUESTION_DEDUPE_LIMIT) {
      const oldest = this.#seenAgentQuestionOrder.shift()
      if (oldest) this.#seenAgentQuestionKeys.delete(oldest)
    }
  }

  #recordArrivalLatency(event: ChangeEvent): void {
    const occurredAt = Date.parse(event.occurredAt)
    if (!Number.isFinite(occurredAt)) return
    const latencyMs = Math.max(0, this.#clock.now() - occurredAt)
    this.#counters.liveEvents = (this.#counters.liveEvents ?? 0) + 1
    this.#counters.liveArrivalLatencyMsLast = latencyMs
    this.#counters.liveArrivalLatencyMsMax = Math.max(this.#counters.liveArrivalLatencyMsMax ?? 0, latencyMs)
    this.#logger.debug?.('[factory] live issue event latency recorded', {
      eventId: event.id,
      path: event.resource.path,
      latencyMs,
    })
  }

  async #sendCriticalReviewerMessage(record: InFlightIssue): Promise<void> {
    if (!this.#fleet.waitForInjected) {
      return
    }

    const reviewer = [...record.agents.values()].find((agent) => agent.spec.role === 'reviewer')
    if (!reviewer) {
      return
    }

    const input = {
      to: reviewer.result?.name ?? reviewer.spec.name,
      text: `Review is queued for ${record.issue.key}. Watch implementer PR handoff and report readiness.`,
      from: 'factory',
      data: { issue: record.issue },
    }
    const ack = await this.#waitForInjectedAndSubmit(input)
    this.#criticalMessages.set(ack.eventId, { issue: record.issue, input })
  }

  async #sendImplementerTask(record: InFlightIssue): Promise<void> {
    if (!this.#fleet.waitForInjected) {
      return
    }

    const implementers = [...record.agents.values()].filter((agent) => agent.spec.role === 'implementer')
    if (implementers.length === 0) {
      return
    }

    const issue = await this.#readIssue(record.issue.path)
    const reviewer = [...record.agents.values()].find((agent) => agent.spec.role === 'reviewer')
    const reviewerName = reviewer?.result?.name ?? reviewer?.spec.name ?? 'reviewer'
    const implementerNames = implementers.map((agent) => agent.result?.name ?? agent.spec.name)
    for (const implementer of implementers) {
      const input = {
        to: implementer.result?.name ?? implementer.spec.name,
        text: renderAgentTask({
          issue: templateIssueFromRecord(record, issue),
          route: routeForImplementer(record, implementer.spec),
          role: 'implementer',
          config: { mergePolicy: this.#config.mergePolicy, terminalState: this.#config.terminalState },
          reviewerName,
          implementerNames,
          slackDispatchThread: this.#slackDispatchThreadFor(record),
        }),
        from: 'factory',
        data: { issue: record.issue },
      }
      const ack = await this.#waitForInjectedAndSubmit(input)
      this.#criticalMessages.set(ack.eventId, { issue: record.issue, input })
    }
  }

  async #waitForInjectedAndSubmit(
    input: Parameters<FleetClient['sendMessage']>[0],
  ): Promise<{ eventId: string; targets: string[] }> {
    if (!this.#fleet.waitForInjected) {
      throw new Error('Fleet client does not support confirmed task injection')
    }

    const ack = await this.#waitForInjectedWithRetry(input)
    await this.#submitInjectedTask(input, ack)
    return ack
  }

  async #waitForInjectedWithRetry(
    input: Parameters<FleetClient['sendMessage']>[0],
  ): Promise<{ eventId: string; targets: string[] }> {
    if (!this.#fleet.waitForInjected) {
      throw new Error('Fleet client does not support confirmed task injection')
    }

    const startedAt = this.#clock.now()
    let attempt = 0
    let lastError: unknown
    while (attempt < INJECTION_MAX_ATTEMPTS && this.#clock.now() - startedAt < INJECTION_CONFIRMATION_TIMEOUT_MS) {
      attempt += 1
      const elapsed = Math.max(0, this.#clock.now() - startedAt)
      const remaining = Math.max(1, INJECTION_CONFIRMATION_TIMEOUT_MS - elapsed)
      try {
        return await this.#fleet.waitForInjected(input, {
          timeoutMs: Math.min(INJECTION_RETRY_ATTEMPT_TIMEOUT_MS, remaining),
        })
      } catch (error) {
        lastError = error
        if (
          !isRegistrationLagInjectionError(error) ||
          remaining <= INJECTION_RETRY_DELAY_MS ||
          attempt >= INJECTION_MAX_ATTEMPTS
        ) {
          throw error
        }
        this.#increment('injectionRegistrationLagRetries')
        this.#logger.warn?.('[factory] task injection target not registered yet; retrying', {
          to: input.to,
          attempt,
          error: describeError(error).errorMessage,
        })
        await this.#clock.sleep(Math.min(INJECTION_RETRY_DELAY_MS, remaining))
      }
    }

    throw lastError instanceof Error ? lastError : new Error(`Timed out waiting to inject task to ${input.to}`)
  }

  async #submitInjectedTask(
    input: Parameters<FleetClient['sendMessage']>[0],
    ack: { targets?: string[] },
  ): Promise<void> {
    if (!this.#fleet.sendInput) {
      return
    }

    const targets = ack.targets && ack.targets.length > 0 ? ack.targets : [input.to]
    for (const target of new Set(targets)) {
      await this.#fleet.sendInput(target, '\r')
    }
  }

  // ── PR babysitter ──────────────────────────────────────────────────────────
  // Webhook-driven: a change event on the PR's webhook-fed mount file
  // (/github/repos/<owner>/<repo>/pulls/<n>/meta.json) — PR opened, new commits,
  // draft toggle, closed/merged — routes here. The PR readiness *verdict* (CI
  // green, conflicts resolved, comments addressed) is owned by the babysitter
  // agent, which sees the same per-event webhook data in its sandbox and signals
  // `[factory-pr-ready]`; the orchestrator never runs `gh`. PR meta events here
  // only (a) spawn the babysitter on open and (b) carry the latest open/draft/
  // merged state used to guard the final transition.
  async #handlePrChange(path: string): Promise<void> {
    const parts = githubPullPathParts(path)
    if (!parts) {
      return
    }

    let snapshot: PullSnapshot | undefined
    try {
      snapshot = parsePullSnapshot((await this.#mount.readFile(path)).content, parts.number)
    } catch (error) {
      this.#logger.debug?.('[factory] babysitter could not read PR snapshot', { path, error: describeError(error).errorMessage })
      return
    }
    if (!snapshot) {
      return
    }

    // Map the PR to an in-flight issue by its head ref (reuses the existing
    // key matcher; no branch-naming assumption).
    const headRef = snapshot.headRef ?? ''
    const record = this.#batch.inFlight.find((candidate) =>
      !candidate.dryRun && headRef && containsIssueKey(headRef, candidate.issue.key))
    if (!record) {
      return
    }

    if (snapshot.state && snapshot.state.trim().toUpperCase() !== 'OPEN') {
      // Closed/merged PR — completion of Human Review -> Done stays with the
      // merge gate / the operator; nothing for the babysitter to spawn.
      return
    }
    if (snapshot.draft) {
      this.#increment('babysitterDraftPrSkipped')
      return
    }

    await this.#ensureBabysitter(record, { repo: `${parts.owner}/${parts.repo}`, prNumber: snapshot.number, url: snapshot.url, path })
  }

  // Safety net for a missed PR-open mount event: resolve the PR via the existing
  // probe resolver and spawn the babysitter. Triggered by an implementer exiting
  // after opening its PR (an event, not a poll).
  async #ensureBabysitterForIssue(record: InFlightIssue): Promise<void> {
    if (this.#babysitterSpawned.has(record.issue.key)) {
      return
    }
    const issue = await this.#readIssue(record.issue.path)
    if (!issue) {
      return
    }
    const pr = await this.#completionPrForIssue(issue)
    if (!pr || pr.draft) {
      return
    }
    await this.#ensureBabysitter(record, { repo: pr.repo, prNumber: pr.prNumber })
  }

  async #ensureBabysitter(record: InFlightIssue, prRef: { repo: string; prNumber: number; url?: string; path?: string }): Promise<void> {
    this.#babysitterPr.set(record.issue.key, { repo: prRef.repo, prNumber: prRef.prNumber, path: prRef.path })
    if (this.#babysitterSpawned.has(record.issue.key)) {
      return
    }
    if ([...record.agents.values()].some((agent) => agent.spec.role === 'babysitter')) {
      this.#babysitterSpawned.add(record.issue.key)
      return
    }
    // Reserve up-front so concurrent PR events in a drain don't double-spawn.
    this.#babysitterSpawned.add(record.issue.key)

    try {
      const issue = await this.#readIssue(record.issue.path)
      if (!issue) {
        this.#babysitterSpawned.delete(record.issue.key)
        return
      }

      const route = record.decision.routes.find((candidate) => candidate.repo === prRef.repo)
        ?? record.decision.routes[0]
      const spec = babysitterSpec(issue, this.#config, route)
      const reviewer = [...record.agents.values()].find((agent) => agent.spec.role === 'reviewer')
      const reviewerName = reviewer?.result?.name ?? reviewer?.spec.name ?? `${spec.name.replace(/-babysit$/, '')}-review`
      const implementerNames = [...record.agents.values()]
        .filter((agent) => agent.spec.role === 'implementer')
        .map((agent) => agent.result?.name ?? agent.spec.name)
      const task = renderAgentTask({
        issue: templateIssueFromRecord(record, issue),
        route: route ?? { repo: prRef.repo },
        role: 'babysitter',
        config: { mergePolicy: this.#config.mergePolicy, terminalState: this.#config.terminalState },
        reviewerName,
        implementerNames,
        pr: { number: prRef.prNumber, url: prRef.url },
        slackDispatchThread: this.#slackDispatchThreadFor(record),
      })

      const spawned = await this.#spawnAgent(record, { ...spec, task }, false)
      await this.#writeInFlightRegistry()
      this.#increment('babysittersSpawned')
      this.#logger.info?.('[factory] babysitter spawned for open PR', {
        issue: record.issue.key,
        repo: prRef.repo,
        prNumber: prRef.prNumber,
        babysitter: spawned.name,
      })

      if (this.#fleet.waitForInjected) {
        const tracked = record.agents.get(spawned.name)
        const input = {
          to: tracked?.result?.name ?? spawned.name,
          text: task,
          from: 'factory',
          data: { issue: record.issue },
        }
        const ack = await this.#waitForInjectedAndSubmit(input)
        this.#criticalMessages.set(ack.eventId, { issue: record.issue, input })
      }
    } catch (error) {
      // Allow a later event to retry the spawn.
      this.#babysitterSpawned.delete(record.issue.key)
      this.#increment('babysitterSpawnFailures')
      this.#error(error, record.issue)
    }
  }

  // The babysitter owns the readiness verdict (CI green + conflicts resolved +
  // review comments addressed) — it sees the per-event PR webhook data in its
  // sandbox, exactly like AgentWorkforce/agents review. It signals readiness by
  // DMing `[factory-pr-ready]`. The orchestrator trusts that signal and only
  // guards on the PR's OWN webhook-fed meta (still open, not a draft, not already
  // merged) before flipping the issue to Human Review. No `gh` call.
  async #maybeAdvanceToHumanReview(record: InFlightIssue): Promise<void> {
    if (this.#completionInFlight.has(issueKey(record.issue))) {
      return
    }

    const snapshot = await this.#readBabysatPrSnapshot(record)
    if (snapshot) {
      const guard = prMetaAllowsHumanReview(snapshot)
      if (!guard.ok) {
        this.#increment('babysitterReadinessGuardBlocked')
        this.#logger.info?.('[factory] babysitter ready signal ignored; PR meta not eligible', {
          issue: record.issue.key,
          reason: guard.reason,
        })
        return
      }
    }

    this.#increment('babysitterReadinessReady')
    this.#logger.info?.('[factory] babysitter signalled PR ready; advancing to human review', {
      issue: record.issue.key,
      prMetaChecked: Boolean(snapshot),
    })
    await this.#completeIssue(record, { humanReview: true })
  }

  // Re-read the babysat PR's webhook-fed meta from the mount (no gh). Prefers the
  // exact path captured when the babysitter was spawned; otherwise scans the
  // repo's pulls subtree for the PR number across known layout shapes.
  async #readBabysatPrSnapshot(record: InFlightIssue): Promise<PullSnapshot | undefined> {
    const ref = this.#babysitterPr.get(record.issue.key)
    if (!ref) {
      return undefined
    }
    const candidatePaths = ref.path ? [ref.path] : await this.#pullMetaPathsFor(ref.repo, ref.prNumber)
    for (const path of candidatePaths) {
      try {
        const snapshot = parsePullSnapshot((await this.#mount.readFile(path)).content, ref.prNumber)
        if (snapshot) {
          return snapshot
        }
      } catch {
        // try the next candidate path
      }
    }
    return undefined
  }

  async #pullMetaPathsFor(repo: string, prNumber: number): Promise<string[]> {
    const [owner, name] = repo.split('/')
    if (!owner || !name) {
      return []
    }
    // Scan both the nested <owner>/<repo> and the flat <owner>__<repo> pulls
    // roots so the readiness guard finds the PR meta regardless of mount layout.
    const roots = [`/github/repos/${owner}/${name}/pulls/`, `/github/repos/${owner}__${name}/pulls/`]
    // Match .../pulls/<n>(__slug)?(/...)? for this PR number, in either the
    // nested directory or flat <n>.json / by-id/<n>.json forms.
    const numberSegment = new RegExp(`/pulls/(?:by-id/)?${prNumber}(?:__[^/]*)?(?:/|\\.json$)`, 'u')
    const found: string[] = []
    for (const root of roots) {
      try {
        const tree = await this.#mount.listTree(root)
        found.push(...tree.filter((path) => path.endsWith('.json') && numberSegment.test(path)))
      } catch {
        // try the next root
      }
    }
    return found
  }

  async #completeIssue(record: InFlightIssue, opts: { humanReview?: boolean } = {}): Promise<void> {
    const completionKey = issueKey(record.issue)
    if (this.#completionInFlight.has(completionKey)) {
      return
    }
    this.#completionInFlight.add(completionKey)
    // Land in `human-review` only when the operator opted into that terminal
    // state AND configured its UUID; otherwise fall back to `done` (the legacy
    // terminal) so an operator who sets terminalState: 'done' keeps it and the
    // issue never gets stuck waiting on an unconfigured state.
    const humanReview = opts.humanReview === true &&
      this.#config.terminalState === 'human-review' &&
      Boolean(this.#config.stateIds.humanReview)
    const targetState = humanReview ? this.#config.stateIds.humanReview! : this.#config.stateIds.done
    const statusLabel = humanReview ? 'in human review' : 'done'
    try {
      const issue = await this.#readIssue(record.issue.path)
      if (issue) {
        await this.#linear.setState(issue, targetState)
        this.#recordCanonicalIssueState({ key: issue.key, stateId: targetState })
        this.#emit('writeback-verified', { issue: record.issue, path: issue.path })
      }

      if (this.#slack && this.#config.slack && !await this.#shouldSkipSlackWriteback('completion-thread')) {
        try {
          const root = await this.#slack.postThread({
            channel: this.#config.slack.channel,
            text: `${record.issue.key}: factory agents completed.\nStatus: ${statusLabel}\nMerge policy: ${this.#config.mergePolicy}`,
          })
          await this.#slack.reply(root.threadId, `${record.issue.key}: Linear state set to ${statusLabel}.`)
          this.#recordSlackWritebackSuccess('completion-thread')
        } catch (error) {
          this.#markSlackWritebackFailure('completion-thread', error)
        }
      }
      // Only auto-merge on the `done` terminal path. Human Review parks the PR
      // for an operator — the merge gate (which requires an APPROVED review)
      // would refuse anyway, and we must not merge before the human has looked.
      if (issue && !humanReview) {
        await this.#runCompletionMergeGate(issue)
      }

      await this.#releaseAndTerminateAgents([...record.agents], humanReview ? 'issue-human-review' : 'issue-done', 'completion')

      this.#increment(humanReview ? 'humanReview' : 'done')
      this.#emit('issue-done', { issue: record.issue })
      await this.#stopSlackWatcher(record.issue)
      this.#recordDispatchTerminal(record.issue)
      const next = this.#batch.complete(record.issue)
      if (next) {
        await this.dispatch(next.decision, { dryRun: next.dryRun })
      }
      await this.#writeInFlightRegistry()
    } catch (error) {
      this.#error(error, record.issue)
    } finally {
      this.#completionInFlight.delete(completionKey)
      this.#probePrGhBackoffUntilMs.delete(completionKey)
      this.#probePrResolvedCache.delete(completionKey)
      this.#babysitterSpawned.delete(record.issue.key)
      this.#babysitterPr.delete(record.issue.key)
    }
  }

  #emit(event: FactoryEvent, payload: FactoryEventPayload): void {
    for (const listener of this.#listeners.get(event) ?? []) {
      listener(payload)
    }
  }

  #error(error: unknown, issue?: IssueRef): void {
    this.#increment('errors')
    this.#logger.error?.('[factory] error', error)
    this.#emit('error', { error, ...describeError(error), issue })
  }

  #increment(name: string): void {
    this.#counters[name] = (this.#counters[name] ?? 0) + 1
  }

  async #shouldSkipSlackWriteback(context: string): Promise<boolean> {
    if (!this.#config.slack) return false

    const freshness = await this.#slackFreshness()
    if (this.#slackWritebackFailureDegraded) {
      if (this.#slackWritebackFailureBackoffUntilMs > this.#clock.now()) {
        this.#increment('slackWritebacksSkipped')
        return true
      }
      return false
    }

    if (!freshness.degraded && freshness.known) {
      if (this.#slackDegraded) {
        this.#logger.info?.('[factory] Slack sync recovered; resuming Slack writeback', { context })
        this.#increment('slackRecoveredEpisodes')
      }
      this.#slackDegraded = false
      this.#slackDegradedReason = undefined
      return false
    }

    if (!freshness.degraded && this.#slackDegraded) {
      this.#increment('slackWritebacksSkipped')
      return true
    }

    if (!freshness.degraded) {
      return false
    }

    this.#slackDegradedReason = freshness.reason
    this.#increment('slackWritebacksSkipped')
    if (!this.#slackDegraded) {
      this.#slackDegraded = true
      this.#increment('slackDegradedEpisodes')
      this.#logger.warn?.('[factory] Slack sync degraded; skipping Slack writeback', {
        context,
        reason: freshness.reason,
        status: freshness.status,
      })
    }
    return true
  }

  #markSlackWritebackFailure(context: string, error: unknown): void {
    this.#slackWritebackFailureDegraded = true
    this.#slackWritebackFailureBackoffUntilMs = this.#clock.now() + (this.#config.slack?.staleAfterMs ?? 10 * 60_000)
    this.#slackDegradedReason = `slack writeback failed: ${describeError(error).errorMessage}`
    if (!this.#slackDegraded) {
      this.#slackDegraded = true
      this.#increment('slackDegradedEpisodes')
      this.#logger.warn?.('[factory] Slack writeback failed; marking Slack degraded', {
        context,
        reason: this.#slackDegradedReason,
      })
    }
  }

  #recordSlackWritebackSuccess(context: string): void {
    if (this.#slackWritebackFailureDegraded) {
      this.#logger.info?.('[factory] Slack writeback recovered; clearing write-failure degradation', { context })
      this.#increment('slackRecoveredEpisodes')
    }
    this.#slackWritebackFailureDegraded = false
    this.#slackWritebackFailureBackoffUntilMs = 0
    if (this.#slackDegraded) {
      this.#slackDegraded = false
      this.#slackDegradedReason = undefined
    }
  }

  async #slackFreshness(): Promise<{ known: boolean; degraded: boolean; reason?: string; status?: ProviderSyncStatus }> {
    const staleAfterMs = this.#config.slack?.staleAfterMs ?? 10 * 60_000
    let sawSlackStatus = false
    let softStatusResult: SlackSyncStatusCheck | undefined
    let softStatus: ProviderSyncStatus | undefined
    try {
      const status = await this.#mount.getSyncStatus?.('slack')
      sawSlackStatus = status?.provider === 'slack'
      const statusResult = slackSyncStatusResult(status, this.#clock.now(), staleAfterMs)
      if (statusResult.known) {
        if (!statusResult.degraded || statusResult.severity === 'hard') {
          return { known: true, degraded: statusResult.degraded, reason: statusResult.reason, status }
        }
        softStatusResult = statusResult
        softStatus = status
      }
    } catch (error) {
      this.#logger.warn?.('[factory] Slack sync freshness check failed; proceeding without degradation', error)
    }

    try {
      const watermark = await this.#slackEventWatermark()
      if (watermark.lastEventAtMs === undefined) {
        return sawSlackStatus
          ? {
              known: true,
              degraded: true,
              reason: softStatusResult?.reason ?? 'slack sync has no recent event watermark',
              status: softStatus,
            }
          : { known: false, degraded: false }
      }
      const ageMs = this.#clock.now() - watermark.lastEventAtMs
      if (ageMs <= staleAfterMs) {
        if (softStatusResult?.degraded) {
          this.#increment('slackGateBypassedByEventWatermark')
          this.#logger.info?.('[factory] Slack sync soft-degraded but event watermark is fresh; continuing Slack writeback', {
            reason: softStatusResult.reason,
            status: softStatus,
            lastSlackEventAtMs: watermark.lastEventAtMs,
            eventWatermarkAgeMs: ageMs,
          })
        }
        return { known: true, degraded: false }
      }
      return {
        known: true,
        degraded: true,
        reason: softStatusResult?.reason ?? `slack event watermark stale by ${ageMs}ms`,
        status: softStatus,
      }
    } catch (error) {
      this.#logger.warn?.(
        softStatusResult?.degraded
          ? '[factory] Slack event freshness fallback failed; honoring soft sync degradation'
          : '[factory] Slack event freshness fallback failed; proceeding without degradation',
        error,
      )
      return softStatusResult?.degraded
        ? { known: true, degraded: true, reason: softStatusResult.reason, status: softStatus }
        : { known: false, degraded: false }
    }
  }

  async #slackEventWatermark(): Promise<SlackEventWatermark> {
    const nowMs = this.#clock.now()
    if (this.#slackEventWatermarkCache && nowMs - this.#slackEventWatermarkCache.checkedAtMs <= SLACK_EVENT_WATERMARK_CACHE_MS) {
      return this.#slackEventWatermarkCache.result
    }

    if (this.#slackEventWatermarkRefresh) {
      return this.#slackEventWatermarkRefresh
    }

    this.#slackEventWatermarkRefresh = this.#refreshSlackEventWatermark()
    try {
      return await this.#slackEventWatermarkRefresh
    } finally {
      this.#slackEventWatermarkRefresh = undefined
    }
  }

  async #refreshSlackEventWatermark(): Promise<SlackEventWatermark> {
    this.#increment('slackEventWatermarkRefreshes')
    const page = await this.#mount.getEvents({ provider: 'slack', last: 100, limit: 100 })
    const lastEventAtMs = page.events
      .filter((event) => eventProvider(event) === 'slack')
      .map((event) => eventOccurredAtMs(event))
      .filter((time): time is number => time !== undefined && Number.isFinite(time))
      .sort((a, b) => b - a)[0]
    const result = lastEventAtMs === undefined
      ? { known: true }
      : { known: true, lastEventAtMs }
    this.#slackEventWatermarkCache = { checkedAtMs: this.#clock.now(), result }
    return result
  }

  async #ensureSlackDispatchThread(record: InFlightIssue, result: DispatchResult): Promise<void> {
    if (!this.#slack || !this.#config.slack || result.dryRun) {
      return
    }

    if (await this.#shouldSkipSlackWriteback('dispatch-thread')) {
      return
    }

    const key = issueKey(record.issue)
    if (this.#slackThreadIds.has(key) || this.#slackWatcherStarts.has(key)) {
      try {
        await this.#slackWatcherStarts.get(key)
      } catch {
        // The initiator logs Slack watcher startup failures.
      }
      return
    }

    const start = this.#postAndWatchSlackDispatchThread(record, result)
    this.#slackWatcherStarts.set(key, start)
    try {
      await start
    } catch (error) {
      this.#markSlackWritebackFailure('dispatch-thread', error)
      this.#logger.warn?.(`[factory] failed to establish Slack dispatch thread for ${record.issue.key}`, error)
    } finally {
      this.#slackWatcherStarts.delete(key)
    }
  }

  async #postAndWatchSlackDispatchThread(record: InFlightIssue, result: DispatchResult): Promise<void> {
    if (!this.#slack || !this.#config.slack) {
      return
    }

    const root = await this.#slack.postThread({
      channel: this.#config.slack.channel,
      text: [
        `${record.issue.key}: factory agents dispatched.`,
        `State: ${result.stateId ?? 'dispatching'}`,
        `Agents: ${result.agents.map((agent) => agent.name).join(', ') || 'none'}`,
      ].join('\n'),
    })
    this.#slackThreadIds.set(issueKey(record.issue), root.threadId)
    await this.#watchSlackThread(record, root.threadId)
    this.#recordSlackWritebackSuccess('dispatch-thread')
  }

  async #escalateTriageToSlack(decision: TriageDecision, reason: string, dryRun: boolean): Promise<void> {
    if (dryRun) {
      return
    }

    if (!this.#slack || !this.#config.slack) {
      this.#logger.warn?.('[factory] triage escalation has no Slack channel configured', {
        issue: decision.issue,
        reason,
      })
      return
    }

    if (await this.#shouldSkipSlackWriteback('triage-escalation')) {
      return
    }

    const key = issueKey(decision.issue)
    if (this.#slackThreadIds.has(key) || this.#slackWatcherStarts.has(key)) {
      try {
        await this.#slackWatcherStarts.get(key)
      } catch {
        // The initiator logs Slack watcher startup failures.
      }
      return
    }

    const start = this.#postAndWatchSlackEscalationThread(decision, reason)
    this.#slackWatcherStarts.set(key, start)
    try {
      await start
    } catch (error) {
      this.#markSlackWritebackFailure('triage-escalation', error)
      this.#logger.warn?.(`[factory] failed to establish Slack escalation thread for ${decision.issue.key}`, error)
    } finally {
      this.#slackWatcherStarts.delete(key)
    }
  }

  async #postAndWatchSlackEscalationThread(decision: TriageDecision, reason: string): Promise<void> {
    if (!this.#slack || !this.#config.slack) {
      return
    }

    const issue = await this.#readIssue(decision.issue.path)
    const root = await this.#slack.postThread({
      channel: this.#config.slack.channel,
      text: [
        `${decision.issue.key}: factory triage escalation for ${issue?.title ?? decision.issue.key}`,
        `Reason: ${reason}`,
        `Question: Please clarify the intended repo/approach or add enough acceptance detail for the factory agent to proceed.`,
      ].join('\n'),
    })
    this.#slackThreadIds.set(issueKey(decision.issue), root.threadId)
    await this.#watchSlackThread({
      issue: decision.issue,
      decision,
      agents: new Map(),
      invocationIds: new Set(),
      dryRun: false,
    }, root.threadId)
    this.#recordSlackWritebackSuccess('triage-escalation')
  }

  async #watchSlackThread(record: InFlightIssue, threadId: string): Promise<void> {
    if (!this.#config.slack) {
      return
    }

    const key = issueKey(record.issue)
    if (this.#slackWatchers.has(key)) {
      return
    }

    const channelDir = this.#config.slack.channel
    const messagesPrefix = slackChannelMessagesPrefix(channelDir)
    const preExistingPaths = new Set<string>()
    const seenReplies = new Set<string>()
    const seenReplyMessages = new Set<string>()
    let missingIdentityLogged = false
    let cursor: string | undefined
    let stopped = false
    let pollTimer: ReturnType<typeof setTimeout> | undefined

    const markPreExisting = async (): Promise<void> => {
      try {
        const page = await this.#mount.getEvents({ limit: SLACK_REPLY_EVENTS_LIMIT })
        cursor = page.nextCursor ?? undefined
        for (const event of page.events) {
          if (event.resource.path.startsWith(messagesPrefix)) {
            preExistingPaths.add(event.resource.path)
          }
        }
      } catch (error) {
        this.#logger.warn?.('[factory] unable to seed Slack reply watcher event cursor', error)
      }
    }

    const handle = async (event: ChangeEvent): Promise<void> => {
      try {
        if (stopped || !event.resource.path.startsWith(messagesPrefix)) {
          return
        }

        const eventKey = eventIdentity(event)
        if (!eventKey) {
          if (!missingIdentityLogged) {
            missingIdentityLogged = true
            this.#logger.warn?.('[factory] Slack reply event missing stable identity; falling back to path/content dedupe')
          }
        }

        if (preExistingPaths.has(event.resource.path)) {
          return
        }

        const reply = await this.#readSlackReply(event.resource.path)
        if (!reply || !reply.isThreadReply || reply.threadTs !== threadId || reply.channelDir !== channelDir) {
          return
        }

        const replyMessageKey = `${reply.threadTs}:${reply.messageTs}`
        if (seenReplyMessages.has(replyMessageKey)) {
          this.#logger.debug?.('[factory] suppressed duplicate Slack reply message', { issue: record.issue.key, path: event.resource.path })
          return
        }

        const replyKey = `${eventKey ?? event.resource.path}:${stableHash(JSON.stringify(reply.raw))}`
        if (seenReplies.has(replyKey)) {
          this.#logger.debug?.('[factory] suppressed duplicate Slack reply payload', { issue: record.issue.key, path: event.resource.path })
          return
        }
        seenReplies.add(replyKey)

        if (reply.isBot) {
          return
        }
        seenReplyMessages.add(replyMessageKey)

        await this.#routeSlackAnswerToImplementers(record, reply)
      } catch (error) {
        this.#logger.error?.('[factory] failed to handle Slack reply event', error)
      }
    }

    await markPreExisting()

    let subscription: Subscription | undefined
    try {
      subscription = this.#mount.subscribe([`${messagesPrefix}**`], (event) => {
        void handle(event)
      })
    } catch (error) {
      this.#logger.warn?.('[factory] Slack reply subscribe failed; relying on event polling', error)
    }

    const poll = async (): Promise<void> => {
      if (stopped) {
        return
      }
      try {
        const page = await this.#mount.getEvents({ cursor, limit: SLACK_REPLY_EVENTS_LIMIT })
        cursor = page.nextCursor ?? cursor
        for (const event of page.events) {
          await handle(event)
        }
      } catch (error) {
        this.#logger.warn?.('[factory] Slack reply polling failed', error)
      }
      if (!stopped) {
        pollTimer = setTimeout(() => {
          void poll()
        }, SLACK_REPLY_POLL_INTERVAL_MS)
        pollTimer.unref?.()
      }
    }
    void poll()

    this.#slackWatchers.set(key, {
      stop: async () => {
        stopped = true
        if (pollTimer) {
          clearTimeout(pollTimer)
          pollTimer = undefined
        }
        await this.#boundedStopTeardown('Slack reply subscription unsubscribe', () => subscription?.unsubscribe())
      },
    })
  }

  async #stopSlackWatcher(issue: IssueRef): Promise<void> {
    const key = issueKey(issue)
    const watcher = this.#slackWatchers.get(key)
    this.#slackWatchers.delete(key)
    this.#slackThreadIds.delete(key)
    await watcher?.stop()
  }

  async #readSlackReply(path: string): Promise<SlackReply | undefined> {
    try {
      const { content } = await this.#mount.readFile(path)
      return parseSlackReply(path, content, this.#config.slack?.botUserId ?? 'U0B2596R7EZ')
    } catch (error) {
      this.#logger.warn?.(`Unable to read Slack reply ${path}`, error)
      return undefined
    }
  }

  async #routeSlackAnswerToImplementers(record: InFlightIssue, reply: SlackReply): Promise<void> {
    if (!this.#config.slack || !this.#fleet.sendInput) {
      return
    }

    const liveRecord = this.#batch.getIssue(record.issue)
    if (!liveRecord || liveRecord.dryRun) {
      this.#increment('slackAnswersIgnoredNoInFlight')
      return
    }

    const text = reply.text.trim()
    if (!text) {
      this.#increment('slackAnswersIgnoredEmpty')
      return
    }

    // Route human replies to the implementers AND the babysitter — once a PR is
    // open the babysitter is the one chatting with the human about it.
    const recipients = [...liveRecord.agents.values()]
      .filter((agent) => agent.spec.role === 'implementer' || agent.spec.role === 'babysitter')
      .map((agent) => agent.result?.name ?? agent.spec.name)
      .filter((name): name is string => Boolean(name))

    if (recipients.length === 0) {
      this.#increment('slackAnswersIgnoredNoImplementer')
      return
    }

    const input = slackAnswerInput(liveRecord.issue, text)
    for (const recipient of new Set(recipients)) {
      await this.#fleet.sendInput(recipient, input)
      this.#increment('slackAnswersInjected')
    }
  }

  #slackDispatchThreadFor(record: InFlightIssue): { channel: string; threadId: string } | undefined {
    if (!this.#config.slack) {
      return undefined
    }

    const threadId = this.#slackThreadIds.get(issueKey(record.issue))
    return threadId ? { channel: this.#config.slack.channel, threadId } : undefined
  }

  async #runCompletionMergeGate(issue: LinearIssue): Promise<void> {
    if (this.#isSyntheticProbeIssue(issue)) {
      await this.#closeSyntheticProbeIfPresent(issue)
      return
    }

    if (!isRealLinearIssue(issue)) {
      this.#logger.warn?.('[factory] merge gate skipped non-real Linear issue', { issue: issue.key })
      this.#increment('mergeGateSkippedNonReal')
      return
    }

    if (this.#config.mergePolicy !== 'on-green-with-review') {
      return
    }

    const pr = await this.#probePrResolver(issue)
    if (!pr) {
      this.#logger.warn?.('[factory] merge gate found no PR for real issue', { issue: issue.key })
      this.#increment('mergeGateMissingPr')
      return
    }

    const ready = await this.#waitForMergeReady(pr)
    const headSha = ready?.live.headRefOid
    if (!ready || !headSha) {
      this.#increment('mergeGateNotMerged')
      return
    }

    const result = await this.#mergeGate.merge({
      repo: pr.repo,
      number: pr.prNumber,
      expectedHeadSha: headSha,
    })
    if (!result.merged) {
      this.#logger.warn?.('[factory] merge gate aborted guarded merge', {
        issue: issue.key,
        repo: pr.repo,
        prNumber: pr.prNumber,
        headSha,
        reason: result.reason,
      })
      this.#increment('mergeGateMergeAborted')
      return
    }

    this.#logger.info?.('[factory] merge gate merged PR', {
      issue: issue.key,
      repo: pr.repo,
      prNumber: pr.prNumber,
      headSha,
    })
    this.#increment('mergeGateMerged')
  }

  async #closeSyntheticProbeIfPresent(issue: LinearIssue): Promise<void> {
    const probe = this.#customProbePrResolver
      ? await this.#probePrResolver(issue)
      : await this.#resolveIssuePr(issue, {
        titleMarker: FACTORY_E2E_MARKER,
      })
    if (!probe) {
      return
    }

    await this.#probeCloser({
      repo: probe.repo,
      prNumber: probe.prNumber,
      expectedIssueKey: issue.key,
      requireTitleMarker: false,
    })
    this.#increment('mergeGateSyntheticClosed')
  }

  async #waitForMergeReady(pr: { repo: string; prNumber: number }): Promise<Awaited<ReturnType<GithubMergeGatePort['check']>> | undefined> {
    let lastReason = 'not checked'
    for (let attempt = 1; attempt <= MERGE_GATE_MAX_ATTEMPTS; attempt += 1) {
      const verdict = await this.#mergeGate.check({ repo: pr.repo, number: pr.prNumber })
      lastReason = verdict.reason
      if (verdict.ready && verdict.live.headRefOid) {
        return verdict
      }

      if (attempt < MERGE_GATE_MAX_ATTEMPTS) {
        await this.#clock.sleep(MERGE_GATE_POLL_DELAY_MS)
      }
    }

    this.#logger.warn?.('[factory] merge gate left PR open; readiness timeout', {
      repo: pr.repo,
      prNumber: pr.prNumber,
      attempts: MERGE_GATE_MAX_ATTEMPTS,
      reason: lastReason,
    })
    return undefined
  }

  #isSyntheticProbeIssue(issue: LinearIssue): boolean {
    return hasTitlePrefix(issue.title, FACTORY_E2E_MARKER)
  }
}

export function parseLinearIssue(path: string, content: unknown): LinearIssue {
  const parsed = parseJsonContent(content)
  const payload = wrappedPayload(parsed)
  const wrapper = asRecord(parsed) ?? {}
  const state = asRecord(payload.state)
  const labels = Array.isArray(payload.labels)
    ? payload.labels.map(labelName).filter((label): label is string => Boolean(label))
    : []
  const project = recordName(payload.project)
  const team = recordName(payload.team)
  const assignee = recordName(payload.assignee)
  const key = stringValue(payload.identifier) ?? keyFromPath(path)
  const uuid = stringValue(payload.id) ?? stringValue(wrapper.objectId) ?? uuidFromPath(path) ?? key
  const stateName = stringValue(state?.name) ?? stringValue(payload.state_name)
  const stateId = stringValue(payload.stateId) ?? stringValue(state?.id) ?? stateNameToId(stateName) ?? ''

  return {
    uuid,
    key,
    title: stringValue(payload.title) ?? '',
    description: stringValue(payload.description) ?? '',
    stateId,
    state: state || stateName ? { name: stateName ?? '' } : undefined,
    labels,
    project,
    team,
    assignee,
    path,
    raw: asRecord(parsed) ?? payload,
  }
}

export function parseGithubIssue(path: string, content: unknown): GithubIssueSource {
  const parsed = parseJsonContent(content)
  const payload = wrappedPayload(parsed)
  const pathParts = githubIssuePathParts(path)
  const repository = asRecord(payload.repository)
  const owner = stringValue(asRecord(repository?.owner)?.login) ?? stringValue(asRecord(repository?.owner)?.name) ?? pathParts?.owner
  const repoName = stringValue(repository?.name) ?? pathParts?.repo
  const number = numberValue(payload.number) ?? pathParts?.number
  const url = stringValue(payload.html_url) ?? stringValue(payload.url) ?? stringValue(payload.issue_url)
  if (!owner || !repoName || typeof number !== 'number' || !url) {
    throw new Error(`GitHub issue ${path} is missing owner, repo, number, or url`)
  }

  return {
    owner,
    repoName,
    repo: `${owner}/${repoName}`,
    number,
    title: stringValue(payload.title) ?? '',
    body: stringValue(payload.body) ?? '',
    url,
    state: (stringValue(payload.state) ?? '').toLowerCase(),
    labels: Array.isArray(payload.labels)
      ? payload.labels.map(labelName).filter((label): label is string => Boolean(label))
      : [],
    path,
    raw: asRecord(parsed) ?? payload,
  }
}

export async function readFactoryLoopHeartbeat(
  path = DEFAULT_FACTORY_LOOP_HEARTBEAT_PATH,
): Promise<FactoryLoopHeartbeat | undefined> {
  try {
    return parseJsonContent(await readFile(path, 'utf8')) as FactoryLoopHeartbeat
  } catch {
    return undefined
  }
}

export function checkFactoryLoopLiveness(
  heartbeat: FactoryLoopHeartbeat | undefined,
  opts: { nowMs?: number; staleMs?: number } = {},
): FactoryLoopLiveness {
  if (!heartbeat) {
    return { ok: false, stale: true, reason: 'heartbeat missing' }
  }

  const nowMs = opts.nowMs ?? Date.now()
  const staleMs = opts.staleMs ?? 60_000
  const ageMs = Math.max(0, nowMs - heartbeat.updatedAtMs)
  const stale = ageMs > staleMs
  if (stale) {
    return { ok: false, stale: true, ageMs, heartbeat, reason: 'heartbeat stale' }
  }
  if (heartbeat.status === 'stopping') {
    return { ok: false, stale: false, ageMs, heartbeat, reason: 'loop stopping' }
  }
  return { ok: true, stale: false, ageMs, heartbeat }
}

export function isRealLinearIssue(issue: LinearIssue): boolean {
  const payload = wrappedPayload(issue.raw)
  const identifier = stringValue(payload.identifier) ?? issue.key
  return identifier === issue.key &&
    /^[A-Z]+-\d+$/u.test(identifier) &&
    typeof payload.url === 'string' &&
    payload.url.length > 0
}

const issueRef = (issue: LinearIssue): IssueRef => ({ uuid: issue.uuid, key: issue.key, path: issue.path })

const pidsFromSpawnResult = (result: { pid?: number; pids?: number[] } | undefined): number[] => {
  const pids = new Set<number>()
  for (const pid of result?.pids ?? []) {
    if (Number.isInteger(pid) && pid > 0) pids.add(pid)
  }
  if (Number.isInteger(result?.pid) && result!.pid! > 0) {
    pids.add(result!.pid!)
  }
  return [...pids].sort((a, b) => a - b)
}

const dispatchComment = (decision: TriageDecision, agents: DispatchResult['agents']): string => [
  `Factory dispatch for ${decision.issue.key}`,
  `Implementers: ${agents.filter((agent) => agent.role === 'implementer').map((agent) => agent.name).join(', ') || 'none'}`,
  `Reviewer: ${agents.find((agent) => agent.role === 'reviewer')?.name ?? 'none'}`,
].join('\n')

const templateIssueFromRecord = (record: InFlightIssue, issue: LinearIssue | undefined) => ({
  key: issue?.key ?? record.issue.key,
  title: issue?.title ?? record.issue.key,
  description: issue?.description ?? '',
})

const routeForImplementer = (record: InFlightIssue, spec: AgentSpec) => {
  const route = record.decision.routes.find((candidate) =>
    candidate.repo === spec.repo && candidate.clonePath === spec.clonePath,
  ) ?? record.decision.routes.find((candidate) => candidate.repo === spec.repo)

  return {
    repo: spec.repo,
    clonePath: spec.clonePath,
    rationale: route?.rationale,
  }
}

const repoMapFromConfig = (config: FactoryConfig) => {
  const repos = new Set([
    ...Object.values(config.repos.byLabel),
    ...Object.values(config.repos.byProject),
    ...config.repos.keywordRules.map((rule) => rule.repo),
    config.repos.default,
  ].filter((repo): repo is string => Boolean(repo)))

  return [...repos].map((repo) => ({
    repo,
    clonePath: config.repos.clonePaths[repo],
    source: 'default' as const,
  }))
}

export const githubIssuePathParts = (path: string): { owner: string; repo: string; number: number; slug?: string } | undefined => {
  // Canonical GitHub relayfile issue entries are nested as
  // /github/repos/<owner>/<repo>/issues/<number>__<slug>/meta.json. Keep the
  // legacy flat and by-id forms for older mount state, and accept metadata.json
  // as a read-only compatibility alias for historical local mount snapshots.
  const match = path.match(
    /^\/github\/repos\/([^/]+)\/([^/]+)\/issues\/(?:(?:by-id\/)?(\d+)\.json|(\d+)(?:__([^/]+))?\/(?:meta|metadata)\.json)$/u,
  )
  if (!match) {
    return undefined
  }
  const number = Number(match[3] ?? match[4])
  return {
    owner: match[1]!,
    repo: match[2]!,
    number,
    slug: match[5],
  }
}

const githubIssueReadCandidatePaths = (path: string): string[] => {
  if (path.endsWith('/')) {
    return [`${path}meta.json`, `${path}metadata.json`]
  }
  if (path.endsWith('/meta.json')) {
    return [path, path.replace(/\/meta\.json$/u, '/metadata.json')]
  }
  if (path.endsWith('/metadata.json')) {
    return [path, path.replace(/\/metadata\.json$/u, '/meta.json')]
  }
  if (githubIssuePathParts(path)) {
    return [path]
  }
  if (githubIssueDirectoryPathParts(path)) {
    // meta.json is the canonical relayfile GitHub issue basename. metadata.json
    // remains a legacy read fallback for older local mount-state snapshots.
    return [`${path}/meta.json`, `${path}/metadata.json`]
  }
  return [path]
}

const githubIssueDirectoryPathParts = (path: string): { owner: string; repo: string; number: number; slug?: string } | undefined => {
  const match = path.match(/^\/github\/repos\/([^/]+)\/([^/]+)\/issues\/(\d+)(?:__([^/]+))?$/u)
  if (!match) {
    return undefined
  }
  return {
    owner: match[1]!,
    repo: match[2]!,
    number: Number(match[3]),
    slug: match[4],
  }
}

const githubIssueHasFactoryLabel = (issue: GithubIssueSource): boolean =>
  issue.labels.some((label) => label.toLowerCase() === GITHUB_FACTORY_LABEL)

const githubIssueIsClosed = (issue: GithubIssueSource): boolean =>
  issue.state === 'closed'

const githubIssueMirrorPayload = (
  issue: GithubIssueSource,
  repoLabel: string,
  config: FactoryConfig,
): Record<string, unknown> => ({
  id: githubIssueMirrorId(issue),
  title: `${GITHUB_MIRROR_TITLE_PREFIX} ${issue.title}`.trim(),
  description: githubIssueMirrorDescription(issue),
  stateId: config.stateIds.readyForAgent,
  labels: [{ name: repoLabel }],
  team: { key: config.safety.requireTeamKey },
  source: {
    provider: 'github',
    owner: issue.owner,
    repo: issue.repoName,
    number: issue.number,
    url: issue.url,
    path: issue.path,
  },
})

const githubIssueMirrorDescription = (issue: GithubIssueSource): string => {
  const body = issue.body.trim()
  const source = `${GITHUB_MIRROR_SOURCE_PREFIX}${issue.url}`
  return body ? `${body}\n\n${source}` : source
}

const githubIssueMirrorId = (issue: GithubIssueSource): string =>
  `github-${stableHash(`${issue.repo.toLowerCase()}#${issue.number}:${issue.url}`)}`

const githubIssueMirrorDraftPath = (issue: GithubIssueSource): string =>
  `/linear/issues/factory-create-${githubIssueMirrorId(issue)}.json`

const linearIssueMirrorsGithubIssue = (issue: LinearIssue, ghIssue: GithubIssueSource): boolean => {
  const payload = wrappedPayload(issue.raw)
  const source = asRecord(payload.source)
  if (
    stringValue(source?.provider)?.toLowerCase() === 'github' &&
    stringValue(source?.url) === ghIssue.url
  ) {
    return true
  }
  if (
    stringValue(source?.provider)?.toLowerCase() === 'github' &&
    stringValue(source?.owner)?.toLowerCase() === ghIssue.owner.toLowerCase() &&
    stringValue(source?.repo)?.toLowerCase() === ghIssue.repoName.toLowerCase() &&
    numberValue(source?.number) === ghIssue.number
  ) {
    return true
  }
  return issue.description
    .split(/\r?\n/u)
    .some((line) => line.trim() === `${GITHUB_MIRROR_SOURCE_PREFIX}${ghIssue.url}`)
}

const resolveIssuePrFromMount = async (
  mount: MountClient,
  config: FactoryConfig,
  issue: LinearIssue,
  opts: { requireTitleMarker?: boolean; titleMarker?: string } = {},
): Promise<ResolvedIssuePr | undefined> => {
  const candidates: Array<ResolvedIssuePr & { score: number }> = []
  for (const repo of reposFromConfig(config)) {
    for (const path of await mount.listTree(githubPullRoot(repo))) {
      if (!path.endsWith('.json')) continue
      const pr = await readProbePrCandidate(mount, path)
      const score = pr
        ? issuePrMatchScore(pr, issue, opts.titleMarker ?? config.safety.requireTitlePrefix, opts)
        : 0
      if (!pr || score <= 0) continue
      candidates.push({ repo, prNumber: pr.number, draft: pr.draft, score })
    }
  }

  return candidates.sort((a, b) => b.score - a.score || b.prNumber - a.prNumber)[0]
}

const resolveIssuePrFromGh = async (
  run: GhRunner,
  config: FactoryConfig,
  issue: LinearIssue,
  opts: { requireTitleMarker?: boolean; titleMarker?: string } = {},
  logger?: Logger,
): Promise<ResolvedIssuePr | undefined> => {
  const candidates: Array<ResolvedIssuePr & { score: number; open: boolean }> = []
  for (const repo of reposFromConfig(config)) {
    let payload: unknown
    try {
      const result = await run([
        'pr',
        'list',
        '--repo',
        repo,
        '--state',
        'all',
        '--json',
        'number,title,body,headRefName,isDraft,state',
        '--limit',
        String(PROBE_PR_GH_CANDIDATE_LIMIT),
      ])
      if (!result.stdout.trim()) {
        logger?.warn?.('[factory] gh PR resolver returned empty output', { issue: issue.key, repo })
        continue
      }
      payload = parseJsonContent(result.stdout)
    } catch (error) {
      logger?.warn?.('[factory] gh PR resolver failed', { issue: issue.key, repo, error })
      continue
    }

    if (!Array.isArray(payload)) {
      logger?.warn?.('[factory] gh PR resolver returned non-array payload', { issue: issue.key, repo })
      continue
    }
    if (payload.length >= PROBE_PR_GH_CANDIDATE_LIMIT) {
      logger?.warn?.('[factory] gh PR resolver hit candidate limit', { issue: issue.key, repo, limit: PROBE_PR_GH_CANDIDATE_LIMIT })
    }

    for (const entry of payload) {
      const pr = ghProbePrCandidate(entry)
      if (!pr || !containsIssueKey(pr.headRef, issue.key)) continue
      const score = issuePrMatchScore(pr, issue, opts.titleMarker ?? config.safety.requireTitlePrefix, opts)
      if (score <= 0) continue
      candidates.push({
        repo,
        prNumber: pr.number,
        draft: pr.draft,
        score,
        open: normalizePrState(pr.state) === 'OPEN',
      })
    }
  }

  return candidates.sort((a, b) =>
    b.score - a.score ||
    Number(b.open) - Number(a.open) ||
    b.prNumber - a.prNumber
  )[0]
}

const reposFromConfig = (config: FactoryConfig): string[] => {
  const repos = new Set([
    ...Object.values(config.repos.byLabel),
    ...Object.values(config.repos.byProject),
    ...config.repos.keywordRules.map((rule) => rule.repo),
    config.repos.default,
  ].filter((repo): repo is string => Boolean(repo)))
  return [...repos]
}

const githubPullRoot = (repo: string): string => {
  const [owner, name] = repo.split('/')
  return owner && name ? `/github/repos/${owner}__${name}/pulls/by-id/` : `/github/repos/${repo}/pulls/by-id/`
}

const readProbePrCandidate = async (
  mount: MountClient,
  path: string,
): Promise<{ number: number; title: string; body: string; headRef: string; draft?: boolean } | undefined> => {
  try {
    const payload = wrappedPayload((await mount.readFile(path)).content)
    const number = typeof payload.number === 'number'
      ? payload.number
      : Number(path.split('/').at(-1)?.replace(/\.json$/, ''))
    if (!Number.isInteger(number) || number <= 0) return undefined
    return {
      number,
      title: stringValue(payload.title) ?? '',
      body: stringValue(payload.body) ?? '',
      headRef: refName(payload.headRef) ?? refName(payload.head) ?? stringValue(payload.head_ref) ?? '',
      draft: booleanValue(payload.isDraft) ?? booleanValue(payload.draft),
    }
  } catch {
    return undefined
  }
}

const ghProbePrCandidate = (
  value: unknown,
): { number: number; title: string; body: string; headRef: string; draft?: boolean; state?: string } | undefined => {
  const payload = asRecord(value)
  if (!payload) return undefined
  const number = numberValue(payload.number)
  if (typeof number !== 'number' || !Number.isInteger(number) || number <= 0) return undefined
  return {
    number,
    title: stringValue(payload.title) ?? '',
    body: stringValue(payload.body) ?? '',
    headRef: stringValue(payload.headRefName) ?? '',
    draft: booleanValue(payload.isDraft),
    state: stringValue(payload.state),
  }
}

const issuePrMatchScore = (
  pr: { title: string; body: string; headRef: string },
  issue: LinearIssue,
  marker: string,
  opts: { requireTitleMarker?: boolean; titleMarker?: string } = {},
): number => {
  if (opts.requireTitleMarker && !hasTitlePrefix(pr.title, marker)) return 0

  if (containsIssueKey(pr.headRef, issue.key)) return 30
  if (containsIssueKey(pr.title, issue.key)) return 20
  if (containsExplicitIssueReference(pr.body, issue.key)) return 10
  return 0
}

const hasTitlePrefix = (title: string, marker: string): boolean =>
  title === marker || title.startsWith(`${marker} `)

const normalizePrState = (state?: string): string | undefined => state?.toUpperCase()

const failClosedGhRunner: GhRunner = async () => ({ stdout: '[]' })

const ISSUE_KEY_PATTERN = /^[A-Z]+-\d+$/u

const isIssuePathUnderRoot = (path: string): boolean =>
  path.startsWith(`${ISSUE_ROOT}/`) && path.endsWith('.json')

const isIssueFilePath = (path: string): boolean =>
  isIssuePathUnderRoot(path) &&
  !path.includes('/comments/') &&
  !path.includes('/by-state/') &&
  !path.includes('/by-id/') &&
  isCanonicalIssueFileBasename(path.split('/').at(-1) ?? '')

const isLinearIssueMirrorCandidatePath = (path: string): boolean =>
  isIssueFilePath(path) || /^\/linear\/issues\/factory-create-[^/]+\.json$/u.test(path)

const isGithubIssueFilePath = (path: string): boolean =>
  githubIssuePathParts(path) !== undefined || githubIssueDirectoryPathParts(path) !== undefined

const isGithubIssueTreePath = (path: string): boolean =>
  /^\/github\/repos\/[^/]+\/[^/]+\/issues\/.+/u.test(path)

const githubPullPathParts = (path: string): { owner: string; repo: string; number: number } | undefined => {
  // Tolerate every webhook-fed PR mount layout we've seen, across both the
  // nested <owner>/<repo> directory shape and the flat <owner>__<repo> shape
  // (the latter is what githubPullRoot / resolveIssuePrFromMount still use):
  //   .../<owner>/<repo>/pulls/<n>__<slug>/meta.json   (current adapters shape)
  //   .../<owner>__<repo>/pulls/by-id/<n>.json         (flat mount shape)
  //   .../pulls/<n>/meta.json | metadata.json | .../pulls/<n>.json   (variants)
  // Deliberately ignores siblings like pulls/_index.json and pulls/<n>/comments/*.
  const match = path.match(
    /^\/github\/repos\/(?:([^/]+)\/([^/]+)|([^/]+)__([^/]+))\/pulls\/(?:by-id\/)?(\d+)(?:__[^/]*)?(?:\/(?:meta|metadata)\.json|\.json)$/u,
  )
  if (!match) return undefined
  const owner = match[1] ?? match[3]
  const repo = match[2] ?? match[4]
  if (!owner || !repo) return undefined
  return { owner, repo, number: Number(match[5]) }
}

const isGithubPullFilePath = (path: string): boolean =>
  githubPullPathParts(path) !== undefined

type PullSnapshot = { number: number; state?: string; headRef?: string; draft?: boolean; url?: string; title?: string; merged?: boolean }

const parsePullSnapshot = (content: unknown, fallbackNumber: number): PullSnapshot | undefined => {
  const payload = wrappedPayload(content)
  const number = typeof payload.number === 'number' ? payload.number : fallbackNumber
  if (!Number.isInteger(number) || number <= 0) return undefined
  return {
    number,
    state: stringValue(payload.state),
    headRef: refName(payload.headRef) ?? refName(payload.head) ?? stringValue(payload.head_ref) ?? stringValue(payload.headRefName),
    draft: booleanValue(payload.isDraft) ?? booleanValue(payload.draft),
    url: stringValue(payload.url) ?? stringValue(payload.html_url),
    title: stringValue(payload.title),
    merged: booleanValue(payload.merged),
  }
}

// Guard applied to the babysitter's ready signal: the PR's own webhook-fed meta
// must still be eligible for human review. A missing `state` is treated as open
// (older mount layouts omit it). The CI/conflict/review verdict is NOT re-derived
// here — the babysitter already owns it.
const prMetaAllowsHumanReview = (snapshot: PullSnapshot): { ok: boolean; reason: string } => {
  const state = snapshot.state?.trim().toUpperCase()
  if (state && state !== 'OPEN') {
    return { ok: false, reason: `pr state is ${state}` }
  }
  if (snapshot.merged === true) {
    return { ok: false, reason: 'pr already merged' }
  }
  if (snapshot.draft === true) {
    return { ok: false, reason: 'pr is a draft' }
  }
  return { ok: true, reason: 'pr open and not a draft' }
}

const isIssueAliasFilePath = (path: string): boolean =>
  path.startsWith(linearByStatePath('ready-for-agent')) &&
  path.endsWith('.json') &&
  !path.includes('/comments/') &&
  ISSUE_KEY_PATTERN.test(keyFromPath(path))

const isCanonicalIssueFileBasename = (basename: string): boolean => {
  const stem = basename.replace(/\.json$/u, '')
  const parts = stem.split('__')
  return parts.length === 2 && ISSUE_KEY_PATTERN.test(parts[0]) && parts[1].length > 0
}

const isMissingIssueFileError = (error: unknown): boolean => {
  const record = asRecord(error)
  const status = record?.status ?? record?.statusCode
  if (status === 404) return true
  return error instanceof Error && /(?:404|not found|file not found)/iu.test(error.message)
}

export const keyFromPath = (path: string): string =>
  path.split('/').at(-1)?.replace(/\.json$/, '').split('__')[0] ?? path

const uuidFromPath = (path: string): string | undefined => path.split('__')[1]?.replace(/\.json$/, '')

const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined
const booleanValue = (value: unknown): boolean | undefined => typeof value === 'boolean' ? value : undefined
const numberValue = (value: unknown): number | undefined => typeof value === 'number' ? value : undefined

const stateNameToId = (name: string | undefined): string | undefined =>
  name ? STATE_NAME_TO_ID[name] : undefined

const liveHeartbeatIntervalMs = (staleMs: number): number =>
  Math.min(DEFAULT_LIVE_HEARTBEAT_INTERVAL_MS, Math.max(500, Math.floor(staleMs / 4)))

const installFactoryDraftPredicate = (mount: MountClient, config: FactoryConfig): void => {
  mount.setDefaultAllowedDraftPredicate?.((path, content, opts) =>
    isAllowedFactoryDraft(path, content, opts, mount, config))
}

const isAllowedFactoryDraft = async (
  path: string,
  content: unknown,
  opts: { guarded?: boolean } | undefined,
  mount: MountClient,
  config: FactoryConfig,
): Promise<boolean> => {
  if (!opts?.guarded) return false

  // Comment writeback nested under its issue: /linear/issues/<ref>/comments/<draft>.json.
  // Scope-check the owning issue (the draft content is a comment, not an issue).
  const nestedComment = /^\/linear\/issues\/([^/]+)\/comments\/[^/]+$/u.exec(path)
  if (nestedComment) {
    return isIssuePathInFactoryScope(mount, `/linear/issues/${nestedComment[1]}.json`, config)
  }

  if (path.startsWith('/linear/issues/')) {
    if (isInFactoryScope(scopeIssueFromDraftContent(content), config.safety)) return true
    return isIssuePathInFactoryScope(mount, path, config)
  }

  if (/^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)) {
    return true
  }

  return false
}

const isIssuePathInFactoryScope = async (
  mount: MountClient,
  path: string,
  config: FactoryConfig,
): Promise<boolean> => {
  try {
    return isInFactoryScope(parseLinearIssue(path, (await mount.readFile(path)).content), config.safety)
  } catch {
    return false
  }
}

const scopeIssueFromDraftContent = (content: unknown) => ({
  title: typeof asRecord(content)?.title === 'string' ? asRecord(content)?.title as string : '',
  team: typeof asRecord(asRecord(content)?.team)?.key === 'string'
    ? asRecord(asRecord(content)?.team)?.key as string
    : undefined,
  raw: asRecord(content) ?? {},
})

const eventCursorAfterPage = (
  cursor: string | undefined,
  events: ChangeEvent[],
  nextCursor?: string | null,
): string | undefined => {
  if (nextCursor) return nextCursor
  if (events.length === 0) return cursor
  const numericCursor = cursor === undefined ? 0 : Number(cursor)
  if (Number.isInteger(numericCursor) && numericCursor >= 0) {
    return String(numericCursor + events.length)
  }
  return events.at(-1)?.id ?? cursor
}

const liveEventDedupeKey = (event: ChangeEvent): string | undefined => {
  if (!event.id) return undefined
  const resource = asRecord(event.resource) ?? {}
  return [
    event.id,
    event.type,
    event.resource.path,
    stringValue(resource.revision) ?? '',
    event.digest ?? '',
  ].join('\u001f')
}

const isBeforeLiveCutoff = (
  occurredAt: string,
  connectStartedAtMs: number,
  skewMarginMs: number,
): boolean => {
  const occurredAtMs = Date.parse(occurredAt)
  if (!Number.isFinite(occurredAtMs)) return false
  return occurredAtMs < connectStartedAtMs - skewMarginMs
}

const isAtOrBeforeHighWatermark = (eventId: string | undefined, highWatermark: string | undefined): boolean => {
  if (!eventId || !highWatermark) return false
  if (eventId === highWatermark) return true
  const eventSequence = eventSequenceNumber(eventId)
  const watermarkSequence = eventSequenceNumber(highWatermark)
  if (eventSequence !== undefined && watermarkSequence !== undefined) {
    return eventSequence <= watermarkSequence
  }
  return false
}

const isHighWatermarkRouteUnavailable = (error: unknown): boolean => {
  const details = asRecord(error) ?? {}
  const response = asRecord(details.response) ?? {}
  const status = details.status ?? details.statusCode ?? response.status ?? response.statusCode
  if (status === 404 || status === '404') {
    return true
  }

  const code = stringValue(details.code)?.toLowerCase()
  if (code === 'route_not_found') {
    return true
  }

  return error instanceof Error && /route not found/i.test(error.message)
}

/**
 * Slack writeback liveness follows the provider signal hierarchy:
 * getEvents() provider watermarks are the authoritative ingest signal, sync
 * status is advisory, and only explicit error/failed states fail closed. Soft
 * sync states can be stale while Slack mount writes are still live, so
 * #slackFreshness lets fresh event watermarks override them instead of blocking
 * operator question writebacks.
 */
const slackSyncStatusResult = (
  status: ProviderSyncStatus | undefined,
  nowMs: number,
  staleAfterMs: number,
): SlackSyncStatusCheck => {
  if (!status) return { known: false, degraded: false }
  const normalized = status.status?.toLowerCase()
  if (normalized && ['error', 'failed'].includes(normalized)) {
    return { known: true, degraded: true, reason: `slack sync status is ${status.status}`, severity: 'hard' }
  }
  if (normalized && ['lagging', 'stale', 'degraded'].includes(normalized)) {
    return { known: true, degraded: true, reason: `slack sync status is ${status.status}`, severity: 'soft' }
  }

  const lastEventAtMs = status.lastEventAtMs ??
    (status.lastEventAt ? Date.parse(status.lastEventAt) : undefined) ??
    (status.watermarkTs ? Date.parse(status.watermarkTs) : undefined)
  if (lastEventAtMs !== undefined && Number.isFinite(lastEventAtMs)) {
    const ageMs = nowMs - lastEventAtMs
    return ageMs > staleAfterMs
      ? { known: true, degraded: true, reason: `slack sync watermark stale by ${ageMs}ms`, severity: 'soft' }
      : { known: true, degraded: false }
  }

  if (status.lagSeconds !== undefined && Number.isFinite(status.lagSeconds)) {
    const lagMs = status.lagSeconds * 1000
    return lagMs > staleAfterMs
      ? { known: true, degraded: true, reason: `slack sync lag is ${lagMs}ms`, severity: 'soft' }
      : { known: true, degraded: false }
  }

  if (normalized && ['ok', 'healthy', 'fresh', 'synced', 'ready'].includes(normalized)) {
    return { known: true, degraded: false }
  }

  return { known: false, degraded: false }
}

const eventProvider = (event: ChangeEvent): string | undefined => {
  const flat = asRecord(event) ?? {}
  return stringValue(asRecord(flat.resource)?.provider) ?? stringValue(flat.provider)
}

const eventOccurredAtMs = (event: ChangeEvent): number | undefined => {
  const flat = asRecord(event) ?? {}
  const timestamp = stringValue(flat.occurredAt) ?? stringValue(flat.timestamp)
  const parsed = timestamp ? Date.parse(timestamp) : Number.NaN
  return Number.isFinite(parsed) ? parsed : undefined
}

const eventSequenceNumber = (eventId: string): number | undefined => {
  const whole = Number(eventId)
  if (Number.isFinite(whole)) return whole
  const trailing = eventId.match(/(\d+)$/u)?.[1]
  if (!trailing) return undefined
  const parsed = Number(trailing)
  return Number.isFinite(parsed) ? parsed : undefined
}

const rememberLiveEvent = (seen: Set<string>, key: string): void => {
  seen.add(key)
  if (seen.size <= LIVE_DEDUPE_LIMIT) return
  const oldest = seen.values().next().value
  if (oldest) seen.delete(oldest)
}

const recordName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.key) ?? stringValue(record?.id)
}

const refName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.ref)
}

const labelName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name)
}

const isCompletionReason = (reason?: string): boolean =>
  reason === 'issue-done' || reason === 'done' || reason === 'completed'

// The broker rejects re-registering a name it never released on exit
// (relay#1116-family) with a 500 "agent '<name>' already exists". Detect it from
// the structured payload or the message so resume can treat it as terminal
// rather than retrying the (falsely) "retryable" error forever.
const isAgentAlreadyExistsError = (error: unknown): boolean => {
  const record = asRecord(error)
  const data = asRecord(record?.data)
  const message = stringValue(data?.error) ?? (error instanceof Error ? error.message : '')
  return /already exists/iu.test(message)
}

const defaultRestartPolicy = (spec: AgentSpec): AgentSpec['restartPolicy'] | undefined =>
  // Implementers and babysitters are both long-running and resumable — the
  // babysitter shepherds an open PR over many CI/review cycles, so an abnormal
  // exit should resume its session rather than drop the PR. The reviewer is
  // short-lived and keeps the fleet default.
  spec.role === 'implementer' || spec.role === 'babysitter'
    ? { maxRestarts: 3, strategy: 'resume' } as AgentSpec['restartPolicy']
    : spec.restartPolicy

const slackPayloadTs = (threadId: string): string => threadId.replace(/_/g, '.')

const slackChannelMessagesPrefix = (channelDir: string): string => `/slack/channels/${channelDir}/messages/`

const eventIdentity = (event: ChangeEvent): string | undefined => {
  const record = event as unknown as Record<string, unknown>
  const rawId = record.id ?? record.event_id ?? record.seq
  const id = typeof rawId === 'string' || typeof rawId === 'number' ? String(rawId) : undefined
  return id ? `event:${id}` : undefined
}

const describeError = (error: unknown): { errorMessage: string; errorStack?: string } => {
  if (error instanceof Error) {
    return {
      errorMessage: error.message || error.name || 'Error',
      errorStack: error.stack,
    }
  }
  if (typeof error === 'string') {
    return { errorMessage: error }
  }
  try {
    const serialized = JSON.stringify(error)
    if (serialized && serialized !== '{}') {
      return { errorMessage: serialized }
    }
  } catch {
    // Fall through to String(error).
  }
  return { errorMessage: String(error) }
}

const failedIterationReport = (error: unknown, dryRun: boolean): IterationReport => {
  const details = describeError(error)
  return {
    pulled: [],
    triaged: [],
    dispatched: [],
    skipped: [],
    dryRun,
    error: {
      message: details.errorMessage,
      ...(details.errorStack ? { stack: details.errorStack } : {}),
    },
  }
}

const isRegistrationLagInjectionError = (error: unknown): boolean => {
  const { errorMessage } = describeError(error)
  return /recipient unavailable|not registered|unknown recipient|no such (agent|recipient)|timed out waiting for delivery_injected/i
    .test(errorMessage)
}

const contextualError = (context: string, error: unknown): Error => {
  const details = describeError(error)
  const wrapped = new Error(`${context}: ${details.errorMessage}`)
  if (details.errorStack) {
    wrapped.stack = `${wrapped.stack ?? wrapped.message}\nCaused by: ${details.errorStack}`
  }
  const withCause = wrapped as Error & { cause?: unknown }
  withCause.cause = error
  return wrapped
}

const registryHandoffKey = (issue: IssueRef, agentName: string): string =>
  `${issueKey(issue)}:${agentName}`

const cloneTrackedAgent = (tracked: TrackedAgent): TrackedAgent => ({
  spec: { ...tracked.spec },
  result: tracked.result ? { ...tracked.result } : undefined,
  sessionRef: tracked.sessionRef,
})

const parseSlackReply = (path: string, content: unknown, botUserId: string): SlackReply | undefined => {
  const raw = asRecord(parseJsonContent(content)) ?? {}
  const payload = wrappedPayload(raw)
  const channelDir = path.match(/^\/slack\/channels\/([^/]+)\//u)?.[1] ?? ''
  const pathMatch = path.match(/^\/slack\/channels\/[^/]+\/messages\/([^/]+)(?:\/replies\/([^/]+))?/u)
  const parentFromPath = pathMatch?.[2] ? slackPayloadTs(pathMatch[1]) : undefined
  const messageFromPath = slackPayloadTs(pathMatch?.[2] ?? pathMatch?.[1] ?? '')
  const messageTs = stringValue(payload.ts) ?? messageFromPath
  const threadTs = stringValue(payload.thread_ts) ?? parentFromPath
  if (!channelDir || !threadTs || !messageTs) {
    return undefined
  }

  return {
    channelDir,
    threadTs,
    messageTs,
    text: stringValue(payload.text) ?? '',
    isThreadReply: Boolean(parentFromPath) || threadTs !== messageTs,
    isBot: isOwnSlackBotReply(payload, botUserId),
    raw,
  }
}

const triageEscalationReason = (decision: TriageDecision): string | undefined => {
  const reasons: string[] = []
  if (decision.confidence === 'low') {
    reasons.push('low-confidence triage')
  }
  if (decision.thin) {
    reasons.push('thin issue context')
  }
  if (reasons.length === 0) {
    return undefined
  }
  return `${reasons.join(' and ')}${decision.rationale ? `: ${decision.rationale}` : ''}`
}

const slackAnswerInput = (issue: IssueRef, text: string): string =>
  `Slack reply for ${issue.key}:\n${text}\r`

const isFactoryQuestionTarget = (target: string): boolean => {
  const normalized = target.trim().replace(/^@/u, '').toLowerCase()
  return normalized === 'broker' || normalized === 'factory'
}

const parseAgentQuestion = (message: AgentMessage): AgentQuestion | undefined => {
  const markerPattern = new RegExp(
    `(^|\\n)\\s*(?:${escapeRegExp(AGENT_NEEDS_INPUT_MARKER)}|${escapeRegExp(LEGACY_AGENT_NEEDS_INPUT_MARKER)})\\s*(?::\\s*)?`,
    'iu',
  )
  const match = markerPattern.exec(message.body)
  if (!match || !message.from.trim()) {
    return undefined
  }

  const markerEnd = match.index + match[0].length
  const bodyAfterMarker = message.body.slice(markerEnd).trim()
  const issueKey = message.body.match(/(?:^|\n)\s*Issue:\s*([A-Z]+-\d+)\s*(?:\n|$)/iu)?.[1]?.toUpperCase()
  const question = extractQuestionText(bodyAfterMarker)
  if (!question) {
    return undefined
  }

  return {
    agentName: message.from.trim(),
    issueKey,
    question,
    eventId: message.eventId,
  }
}

const parsePrReadySignal = (message: AgentMessage): { agentName: string; issueKey?: string } | undefined => {
  if (!message.from.trim()) {
    return undefined
  }
  const markerPattern = new RegExp(`(^|\\n|\\s)${escapeRegExp(AGENT_PR_READY_MARKER)}\\s*(?::\\s*)?([A-Z]+-\\d+)?`, 'iu')
  const match = markerPattern.exec(message.body)
  if (!match) {
    return undefined
  }
  return { agentName: message.from.trim(), issueKey: match[2]?.toUpperCase() }
}

const extractQuestionText = (bodyAfterMarker: string): string => {
  const questionMatch = bodyAfterMarker.match(/(?:^|\n)\s*Question:\s*([\s\S]+)$/iu)
  const raw = questionMatch?.[1] ?? bodyAfterMarker
  return raw
    .split(/\r?\n/u)
    .filter((line) => !/^\s*Issue:\s*[A-Z]+-\d+\s*$/iu.test(line))
    .join('\n')
    .trim()
}

const agentQuestionDedupeKey = (issue: IssueRef, question: AgentQuestion): string =>
  `${question.eventId ?? 'missing'}:${stableHash(JSON.stringify({
    issue: issue.key,
    from: question.agentName,
    question: question.question,
  }))}`

const agentQuestionSlackText = (issue: IssueRef, question: AgentQuestion): string => [
  `${issue.key}: ${question.agentName} needs input.`,
  `Question: ${question.question}`,
].join('\n')

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')

const isOwnSlackBotReply = (payload: Record<string, unknown>, botUserId: string): boolean =>
  payload.user_is_bot === true ||
  stringValue(payload.user) === botUserId

const unrefDelay = (ms: number): Promise<void> => new Promise((resolve) => {
  const timer = setTimeout(resolve, ms)
  timer.unref?.()
})

const liveEventYield = (): Promise<void> => new Promise((resolve) => {
  if (typeof setImmediate === 'function') {
    setImmediate(resolve)
    return
  }
  setTimeout(resolve, 0)
})
