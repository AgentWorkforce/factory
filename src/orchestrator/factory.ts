import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import { LINEAR_STATE_IDS, linearByStatePath } from '../constants/linear'
import { GithubMergeGate, closeProbePr, type GhRunner, type GithubMergeGate as GithubMergeGatePort } from '../github'
import type {
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
import { HeuristicTriage, TieredTriage } from '../triage'
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
  isThreadReply: boolean
  isBot: boolean
  raw: Record<string, unknown>
}

const ISSUE_ROOT = '/linear/issues'
const READY_EVENTS_LIMIT = 100
const LIVE_ISSUE_GLOB = `${ISSUE_ROOT}/**`
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
const FACTORY_E2E_MARKER = '[factory-e2e]'
const INJECTION_CONFIRMATION_TIMEOUT_MS = 90_000
const INJECTION_RETRY_DELAY_MS = 1_000
const INJECTION_RETRY_ATTEMPT_TIMEOUT_MS = 15_000
const INJECTION_MAX_ATTEMPTS = 6
const STOP_TEARDOWN_TIMEOUT_MS = 2_500
const MERGE_GATE_MAX_ATTEMPTS = 12
const MERGE_GATE_POLL_DELAY_MS = 10_000
const DISPATCH_FAILURE_HANDOFF_UNRESOLVED_TTL_MS = 5 * 60_000
const DEFAULT_LIVE_HEARTBEAT_INTERVAL_MS = 15_000
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
  readonly #dispatchAttempts = new Map<string, DispatchAttemptState>()
  readonly #canonicalIssueStates = new Map<string, string>()
  readonly #dispatchFailureReaperHandoffs = new Map<string, RegistryHandoffAgent>()
  #slackDegraded = false
  #slackDegradedReason: string | undefined
  #slackWritebackFailureDegraded = false
  #slackWritebackFailureBackoffUntilMs = 0
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
  #completionSweepTimer?: ReturnType<typeof setTimeout>
  #completionSweepActive = false
  readonly #completionInFlight = new Set<string>()
  readonly #probePrGhBackoffUntilMs = new Map<string, number>()
  readonly #probePrResolvedCache = new Map<string, { pr: ResolvedIssuePr; expiresAtMs: number }>()
  readonly #seenLiveEvents = new Set<string>()
  #offAgentExit?: () => void
  #offDeliveryFailed?: () => void
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
    this.#subscription = this.#mount.subscribe([`${ISSUE_ROOT}/**/*.json`], (event) => {
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
      const subscription = this.#subscription
      this.#subscription = undefined
      await this.#boundedStopTeardown('factory subscription unsubscribe', () => subscription?.unsubscribe())
      await Promise.all([...this.#slackWatchers.values()].map((watcher) => watcher.stop()))
      this.#slackWatchers.clear()
      this.#slackThreadIds.clear()
      this.#slackWatcherStarts.clear()
      this.#offAgentExit?.()
      this.#offDeliveryFailed?.()
      this.#offAgentExit = undefined
      this.#offDeliveryFailed = undefined
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

    if (highWatermark.routeUnavailable) {
      this.#increment('liveHighWatermarkFullPullFallbacks')
      this.#logger.info?.('[factory] live subscription high-watermark route unavailable; running startup full pull before event stream')
      await this.runOnce()
      await this.#refreshLiveHeartbeatIfDue()
    }

    if (options.transport !== 'poll') {
      this.#subscription = this.#mount.subscribe([LIVE_ISSUE_GLOB], (event) => {
        this.#enqueueLiveEvent(event)
      }, { from: 'now', coalesce: 'none' })
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
    if (this.#liveEventDrainScheduled || this.#liveEventDrainActive || !this.#started) {
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
    await this.#refreshLiveHeartbeat()
  }

  #prepareLiveEventForDrain(event: ChangeEvent, seenIssueKeys: Set<string>): string | undefined {
    const path = event.resource.path
    if (!isIssueFilePath(path)) {
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

    if (decision.confidence === 'low') {
      const error = new Error(`Low-confidence triage for ${decision.issue.key}; escalation required`)
      this.#error(error, decision.issue)
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
  }

  async #backfillReadyIssues(): Promise<void> {
    const page = await this.#mount.getEvents({ limit: READY_EVENTS_LIMIT })
    const eventPaths = page.events.map((event) => event.resource.path).filter(isIssueFilePath)
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
      if (decision.confidence === 'low') {
        this.#error(new Error(`Low-confidence triage for ${decision.issue.key}; escalation required`), decision.issue)
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

    if (isCompletionReason(reason)) {
      await this.#completeIssue(record)
      return
    }

    const tracked = record.agents.get(name)
    if (!tracked || record.dryRun) {
      return
    }

    try {
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
          config: { mergePolicy: this.#config.mergePolicy },
          reviewerName,
          implementerNames,
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

  async #completeIssue(record: InFlightIssue): Promise<void> {
    const completionKey = issueKey(record.issue)
    if (this.#completionInFlight.has(completionKey)) {
      return
    }
    this.#completionInFlight.add(completionKey)
    try {
      const issue = await this.#readIssue(record.issue.path)
      if (issue) {
        await this.#linear.setState(issue, this.#config.stateIds.done)
        this.#recordCanonicalIssueState({ key: issue.key, stateId: this.#config.stateIds.done })
        this.#emit('writeback-verified', { issue: record.issue, path: issue.path })
      }

      if (this.#slack && this.#config.slack && !await this.#shouldSkipSlackWriteback('completion-thread')) {
        try {
          const root = await this.#slack.postThread({
            channel: this.#config.slack.channel,
            text: `${record.issue.key}: factory agents completed.\nStatus: done\nMerge policy: ${this.#config.mergePolicy}`,
          })
          await this.#slack.reply(root.threadId, `${record.issue.key}: Linear state set to done.`)
          this.#recordSlackWritebackSuccess('completion-thread')
        } catch (error) {
          this.#markSlackWritebackFailure('completion-thread', error)
        }
      }
      if (issue) {
        await this.#runCompletionMergeGate(issue)
      }

      await this.#releaseAndTerminateAgents([...record.agents], 'issue-done', 'completion')

      this.#increment('done')
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
    try {
      const status = await this.#mount.getSyncStatus?.('slack')
      sawSlackStatus = status?.provider === 'slack'
      const statusResult = slackSyncStatusResult(status, this.#clock.now(), staleAfterMs)
      if (statusResult.known) {
        return { known: true, degraded: statusResult.degraded, reason: statusResult.reason, status }
      }
    } catch (error) {
      this.#logger.warn?.('[factory] Slack sync freshness check failed; proceeding without degradation', error)
    }

    try {
      const page = await this.#mount.getEvents({ limit: 100 })
      const lastSlackEvent = page.events
        .filter((event) => event.resource.provider === 'slack')
        .map((event) => Date.parse(event.occurredAt))
        .filter((time) => Number.isFinite(time))
        .sort((a, b) => b - a)[0]
      if (lastSlackEvent === undefined) {
        return sawSlackStatus
          ? { known: true, degraded: true, reason: 'slack sync has no recent event watermark' }
          : { known: false, degraded: false }
      }
      const ageMs = this.#clock.now() - lastSlackEvent
      return ageMs > staleAfterMs
        ? { known: true, degraded: true, reason: `slack event watermark stale by ${ageMs}ms` }
        : { known: true, degraded: false }
    } catch (error) {
      this.#logger.warn?.('[factory] Slack event freshness fallback failed; proceeding without degradation', error)
      return { known: false, degraded: false }
    }
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

        await this.#respondToSlackStatus(record, threadId)
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

  async #respondToSlackStatus(record: InFlightIssue, threadId: string): Promise<void> {
    if (!this.#slack || !this.#config.slack) {
      return
    }

    if (await this.#shouldSkipSlackWriteback('status-responder')) {
      return
    }

    const issue = await this.#readIssue(record.issue.path)
    if (!issue || !isInFactoryScope(issue, this.#config.safety)) {
      return
    }

    const [roster, probe] = await Promise.all([
      this.#fleet.roster(),
      this.#probePrResolver(issue),
    ])
    const activeAgents = new Set(record.agents.keys())
    const liveAgents = roster.agents
      .map((agent) => agent.name)
      .filter((name) => activeAgents.has(name))
      .sort()

    try {
      await this.#slack.reply(threadId, [
        `${issue.key}: ${issueStateLabel(issue)}`,
        `Agents: ${liveAgents.join(', ') || [...activeAgents].sort().join(', ') || 'none'}`,
        `PR: ${probe ? githubPrUrl(probe.repo, probe.prNumber) : 'not found yet'}`,
      ].join('\n'))
      this.#recordSlackWritebackSuccess('status-responder')
    } catch (error) {
      this.#markSlackWritebackFailure('status-responder', error)
    }
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

  if (path.startsWith('/linear/issues/')) {
    if (isInFactoryScope(scopeIssueFromDraftContent(content), config.safety)) return true
    return isIssuePathInFactoryScope(mount, path, config)
  }

  if (path.startsWith('/linear/comments/')) {
    const issueKey = path.split('/').at(-1)?.split('__')[0]
    if (!issueKey) return false
    const candidates = await mount.listTree('/linear/issues/')
    const issuePath = candidates.find((candidate) => candidate.startsWith(`/linear/issues/${issueKey}__`))
    return issuePath ? isIssuePathInFactoryScope(mount, issuePath, config) : false
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

const slackSyncStatusResult = (
  status: ProviderSyncStatus | undefined,
  nowMs: number,
  staleAfterMs: number,
): { known: boolean; degraded: boolean; reason?: string } => {
  if (!status) return { known: false, degraded: false }
  const normalized = status.status?.toLowerCase()
  if (normalized && ['lagging', 'stale', 'degraded', 'error', 'failed'].includes(normalized)) {
    return { known: true, degraded: true, reason: `slack sync status is ${status.status}` }
  }

  const lastEventAtMs = status.lastEventAtMs ??
    (status.lastEventAt ? Date.parse(status.lastEventAt) : undefined) ??
    (status.watermarkTs ? Date.parse(status.watermarkTs) : undefined)
  if (lastEventAtMs !== undefined && Number.isFinite(lastEventAtMs)) {
    const ageMs = nowMs - lastEventAtMs
    return ageMs > staleAfterMs
      ? { known: true, degraded: true, reason: `slack sync watermark stale by ${ageMs}ms` }
      : { known: true, degraded: false }
  }

  if (status.lagSeconds !== undefined && Number.isFinite(status.lagSeconds)) {
    const lagMs = status.lagSeconds * 1000
    return lagMs > staleAfterMs
      ? { known: true, degraded: true, reason: `slack sync lag is ${lagMs}ms` }
      : { known: true, degraded: false }
  }

  if (normalized && ['ok', 'healthy', 'fresh', 'synced', 'ready'].includes(normalized)) {
    return { known: true, degraded: false }
  }

  return { known: false, degraded: false }
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

const defaultRestartPolicy = (spec: AgentSpec): AgentSpec['restartPolicy'] | undefined =>
  spec.role === 'implementer' ? { maxRestarts: 3, strategy: 'resume' } as AgentSpec['restartPolicy'] : spec.restartPolicy

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
    isThreadReply: Boolean(parentFromPath) || threadTs !== messageTs,
    isBot: isOwnSlackBotReply(payload, botUserId),
    raw,
  }
}

const isOwnSlackBotReply = (payload: Record<string, unknown>, botUserId: string): boolean =>
  payload.user_is_bot === true ||
  stringValue(payload.user) === botUserId

const issueStateLabel = (issue: LinearIssue): string => {
  const name = issue.state?.name?.trim()
  if (name && issue.stateId) {
    return `${name} (${issue.stateId})`
  }
  return name || issue.stateId || 'unknown state'
}

const githubPrUrl = (repo: string, prNumber: number): string => `https://github.com/${repo}/pull/${prNumber}`

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
