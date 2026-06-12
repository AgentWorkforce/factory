import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import { LINEAR_STATE_IDS, linearByStatePath } from '../constants/linear'
import { GithubMergeGate, closeProbePr, type GithubMergeGate as GithubMergeGatePort } from '../github'
import type { AgentSpec, ChangeEvent, FleetClient, LinearWriteback, MountClient, SlackWriteback, Subscription } from '../ports'
import type { Clock, Logger } from '../ports/system'
import { isInFactoryScope } from '../safety/factory-scope'
import { HeuristicTriage, TieredTriage } from '../triage'
import type {
  DispatchResult,
  Factory,
  FactoryEventPayload,
  FactoryPorts,
  FactoryStatus,
  FactoryStartOptions,
  FactoryLiveSubscriptionOptions,
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
import { BatchTracker, type InFlightIssue, issueKey } from './batch-tracker'

type FactoryEvent = 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error'
type Listener = (payload: FactoryEventPayload) => void
type SlackThreadWatcher = { stop(): Promise<void> }
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
const STATE_NAME_TO_ID: Record<string, string> = {
  'Ready for Agent': LINEAR_STATE_IDS.readyForAgent,
  'Agent Implementing': LINEAR_STATE_IDS.agentImplementing,
  Implementing: LINEAR_STATE_IDS.agentImplementing,
  Done: LINEAR_STATE_IDS.done,
  'In Planning': LINEAR_STATE_IDS.inPlanning,
}
const SLACK_REPLY_EVENTS_LIMIT = 100
const SLACK_REPLY_POLL_INTERVAL_MS = 5_000

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
  readonly #logger: Logger
  readonly #clock: Clock
  readonly #batch: BatchTracker
  readonly #listeners = new Map<FactoryEvent, Set<Listener>>()
  readonly #counters: Record<string, number> = {}
  readonly #criticalMessages = new Map<string, { issue: IssueRef; input: Parameters<FleetClient['sendMessage']>[0] }>()
  readonly #resumeInFlight = new Map<string, Promise<void>>()
  readonly #resumedExitKeys = new Set<string>()
  readonly #slackThreadIds = new Map<string, string>()
  readonly #slackWatchers = new Map<string, SlackThreadWatcher>()
  readonly #slackWatcherStarts = new Map<string, Promise<void>>()
  #subscription?: Subscription
  #livePollTimer?: ReturnType<typeof setTimeout>
  #livePollInFlight = false
  #liveEventCursor?: string
  #liveEventHighWatermark?: string
  #liveConnectStartedAtMs = 0
  #liveReplaySkewMarginMs = 0
  readonly #seenLiveEvents = new Set<string>()
  #offAgentExit?: () => void
  #offDeliveryFailed?: () => void
  #starting?: Promise<void>
  #started = false

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
    this.#probePrResolver = ports.probePrResolver ?? ((issue) => resolveProbePrFromMount(this.#mount, this.#config, issue))
    this.#logger = ports.logger ?? console
    this.#clock = ports.clock ?? realClock
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
        return
      } catch (error) {
        this.#started = false
        throw error
      }
    }

    await this.#backfillReadyIssues()
    this.#subscription = this.#mount.subscribe([`${ISSUE_ROOT}/**/*.json`], (event) => {
      void this.#handleChange(event.resource.path)
    })
    this.#started = true
  }

  async stop(): Promise<void> {
    this.#started = false
    if (this.#livePollTimer) clearTimeout(this.#livePollTimer)
    this.#livePollTimer = undefined
    this.#livePollInFlight = false
    await this.#subscription?.unsubscribe()
    this.#subscription = undefined
    await Promise.all([...this.#slackWatchers.values()].map((watcher) => watcher.stop()))
    this.#slackWatchers.clear()
    this.#slackThreadIds.clear()
    this.#slackWatcherStarts.clear()
    this.#offAgentExit?.()
    this.#offDeliveryFailed?.()
    this.#offAgentExit = undefined
    this.#offDeliveryFailed = undefined
  }

  async #startLiveSubscription(overrides: Partial<FactoryLiveSubscriptionOptions> = {}): Promise<void> {
    const options = this.#liveOptions(overrides)
    this.#liveConnectStartedAtMs = this.#clock.now()
    this.#liveReplaySkewMarginMs = options.replaySkewMarginMs
    this.#liveEventHighWatermark = await this.#currentEventHighWatermark()
    this.#seenLiveEvents.clear()
    this.#logger.info?.('[factory] live subscription starting', {
      transport: options.transport,
      highWatermark: this.#liveEventHighWatermark,
      replaySkewMarginMs: this.#liveReplaySkewMarginMs,
    })

    if (options.transport !== 'poll') {
      this.#subscription = this.#mount.subscribe([LIVE_ISSUE_GLOB], (event) => {
        void this.#handleLiveChange(event)
      }, { from: 'now', coalesce: 'none' })
    }

    if (options.transport === 'poll') {
      this.#liveEventCursor = await this.#currentEventCursor(options.eventLimit)
      this.#scheduleLivePoll(0, options)
    }
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

  async #currentEventHighWatermark(): Promise<string | undefined> {
    try {
      return await this.#mount.getEventHighWatermark?.()
    } catch (error) {
      this.#increment('liveHighWatermarkUnavailable')
      this.#logger.warn?.('[factory] live subscription high-watermark unavailable', error)
      return undefined
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
        for (const event of page.events) {
          await this.#handleLiveChange(event)
        }
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

  async #handleLiveChange(event: ChangeEvent): Promise<void> {
    const path = event.resource.path
    if (!isIssueFilePath(path)) {
      return
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
      return
    }

    if (isAtOrBeforeHighWatermark(event.id, this.#liveEventHighWatermark)) {
      this.#increment('liveReplayEventsSuppressed')
      this.#increment('liveReplayEventsSuppressedByWatermark')
      this.#logger.debug?.('[factory] suppressed replayed live issue event', {
        id: event.id,
        highWatermark: this.#liveEventHighWatermark,
        path,
      })
      return
    }

    const dedupeKey = liveEventDedupeKey(event)
    if (dedupeKey) {
      if (this.#seenLiveEvents.has(dedupeKey)) {
        this.#increment('liveDuplicateEventsSuppressed')
        this.#logger.debug?.('[factory] suppressed duplicate live issue event', {
          id: event.id,
          path,
        })
        return
      }
      rememberLiveEvent(this.#seenLiveEvents, dedupeKey)
    } else {
      this.#increment('liveEventsMissingIdentity')
      this.#logger.warn?.('[factory] live issue event missing stable identity', { path })
    }

    this.#recordArrivalLatency(event)
    await this.#handleChange(path, { requireRealIssue: true })
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
      if (!issue) {
        continue
      }

      pulled.push(issueRef(issue))
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

    return { pulled, triaged, dispatched, skipped, dryRun }
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

    const record = this.#batch.start(decision, dryRun)
    if (!record) {
      this.#increment('queued')
      this.#emit('issue-queued', { issue: decision.issue })
      return { issue: decision.issue, agents: [], dryRun }
    }

    if (record.result) {
      return record.result
    }

    try {
      const agents: DispatchResult['agents'] = []
      for (const spec of [...decision.implementers, decision.reviewer]) {
        const spawned = await this.#spawnAgent(record, spec, dryRun)
        agents.push({ name: spawned.name, role: spec.role })
      }

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
      await this.#ensureSlackDispatchThread(record, result)
      await this.#sendCriticalReviewerMessage(record)
      return result
    } catch (error) {
      this.#error(error, decision.issue)
      throw error
    }
  }

  status(): FactoryStatus {
    return {
      inFlight: this.#batch.inFlight.map((record) => record.issue),
      queued: this.#batch.queued.map((queued) => queued.issue),
      counters: { ...this.#counters },
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
      this.#error(error)
    }
  }

  async #readyIssuePaths(): Promise<string[]> {
    const paths = new Set<string>()
    for (const path of await this.#mount.listTree(ISSUE_ROOT)) {
      if (isIssueFilePath(path)) {
        paths.add(path)
      }
    }
    for (const path of await this.#mount.listTree(linearByStatePath('ready-for-agent'))) {
      if (isIssueFilePath(path)) {
        paths.add(path)
      }
    }
    return [...paths].sort()
  }

  async #readIssue(path: string): Promise<LinearIssue | undefined> {
    try {
      const { content } = await this.#mount.readFile(path)
      return parseLinearIssue(path, content)
    } catch (error) {
      this.#logger.warn?.(`Unable to read issue ${path}`, error)
      return undefined
    }
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

    const roster = await this.#fleet.roster()
    if (roster.agents.some((agent) => agent.name === spec.name)) {
      this.#batch.recordSpawn(record, spec, invocationId, { name: spec.name, sessionRef: spec.sessionRef })
      return { name: spec.name }
    }

    const result = await this.#fleet.spawn({
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
    this.#batch.recordSpawn(record, spec, invocationId, result)
    return { name: result.name }
  }

  async #handleAgentExit(name: string, reason?: string): Promise<void> {
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
        const ack = await this.#fleet.waitForInjected(critical.input, { timeoutMs: 90_000 })
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
    const ack = await this.#fleet.waitForInjected(input, { timeoutMs: 90_000 })
    this.#criticalMessages.set(ack.eventId, { issue: record.issue, input })
  }

  async #completeIssue(record: InFlightIssue): Promise<void> {
    try {
      const issue = await this.#readIssue(record.issue.path)
      if (issue) {
        await this.#linear.setState(issue, this.#config.stateIds.done)
        this.#emit('writeback-verified', { issue: record.issue, path: issue.path })
      }

      if (this.#slack && this.#config.slack) {
        const root = await this.#slack.postThread({
          channel: this.#config.slack.channel,
          text: `${record.issue.key}: factory agents completed.\nStatus: done\nMerge policy: ${this.#config.mergePolicy}`,
        })
        await this.#slack.reply(root.threadId, `${record.issue.key}: Linear state set to done.`)
      }
      void this.#mergeGate

      if (issue) {
        await this.#closeProbeIfRequired(issue)
      }

      for (const agent of record.agents.keys()) {
        await this.#fleet.release(agent, 'issue-done')
      }

      this.#increment('done')
      this.#emit('issue-done', { issue: record.issue })
      await this.#stopSlackWatcher(record.issue)
      const next = this.#batch.complete(record.issue)
      if (next) {
        await this.dispatch(next.decision, { dryRun: next.dryRun })
      }
    } catch (error) {
      this.#error(error, record.issue)
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
    this.#emit('error', { error, issue })
  }

  #increment(name: string): void {
    this.#counters[name] = (this.#counters[name] ?? 0) + 1
  }

  async #ensureSlackDispatchThread(record: InFlightIssue, result: DispatchResult): Promise<void> {
    if (!this.#slack || !this.#config.slack || result.dryRun) {
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
    let missingIdentityLogged = false
    let cursor: string | undefined
    let stopped = false

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

        const replyKey = `${eventKey ?? event.resource.path}:${stableHash(JSON.stringify(reply.raw))}`
        if (seenReplies.has(replyKey)) {
          this.#logger.debug?.('[factory] suppressed duplicate Slack reply payload', { issue: record.issue.key, path: event.resource.path })
          return
        }
        seenReplies.add(replyKey)

        if (reply.isBot) {
          return
        }

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
      while (!stopped) {
        try {
          const page = await this.#mount.getEvents({ cursor, limit: SLACK_REPLY_EVENTS_LIMIT })
          cursor = page.nextCursor ?? cursor
          for (const event of page.events) {
            await handle(event)
          }
        } catch (error) {
          this.#logger.warn?.('[factory] Slack reply polling failed', error)
        }
        await unrefDelay(SLACK_REPLY_POLL_INTERVAL_MS)
      }
    }
    void poll()

    this.#slackWatchers.set(key, {
      stop: async () => {
        stopped = true
        await subscription?.unsubscribe()
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

    await this.#slack.reply(threadId, [
      `${issue.key}: ${issueStateLabel(issue)}`,
      `Agents: ${liveAgents.join(', ') || [...activeAgents].sort().join(', ') || 'none'}`,
      `PR: ${probe ? githubPrUrl(probe.repo, probe.prNumber) : 'not found yet'}`,
    ].join('\n'))
  }

  async #closeProbeIfRequired(issue: LinearIssue): Promise<void> {
    if (this.#config.mergePolicy !== 'never' || !this.#isSyntheticProbeIssue(issue)) {
      return
    }

    const probe = await this.#probePrResolver(issue)
    if (!probe) {
      return
    }

    await this.#probeCloser({
      repo: probe.repo,
      prNumber: probe.prNumber,
      expectedIssueKey: issue.key,
    })
  }

  #isSyntheticProbeIssue(issue: LinearIssue): boolean {
    return isInFactoryScope(issue, this.#config.safety)
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

export function isRealLinearIssue(issue: LinearIssue): boolean {
  const payload = wrappedPayload(issue.raw)
  const identifier = stringValue(payload.identifier) ?? issue.key
  return identifier === issue.key &&
    /^[A-Z]+-\d+$/u.test(identifier) &&
    typeof payload.url === 'string' &&
    payload.url.length > 0
}

const issueRef = (issue: LinearIssue): IssueRef => ({ uuid: issue.uuid, key: issue.key, path: issue.path })

const dispatchComment = (decision: TriageDecision, agents: DispatchResult['agents']): string => [
  `Factory dispatch for ${decision.issue.key}`,
  `Implementers: ${agents.filter((agent) => agent.role === 'implementer').map((agent) => agent.name).join(', ') || 'none'}`,
  `Reviewer: ${agents.find((agent) => agent.role === 'reviewer')?.name ?? 'none'}`,
].join('\n')

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

const resolveProbePrFromMount = async (
  mount: MountClient,
  config: FactoryConfig,
  issue: LinearIssue,
): Promise<{ repo: string; prNumber: number } | undefined> => {
  const candidates: Array<{ repo: string; prNumber: number }> = []
  for (const repo of reposFromConfig(config)) {
    for (const path of await mount.listTree(githubPullRoot(repo))) {
      if (!path.endsWith('.json')) continue
      const pr = await readProbePrCandidate(mount, path)
      if (!pr || !probePrMatchesIssue(pr, issue, config.safety.requireTitlePrefix)) continue
      candidates.push({ repo, prNumber: pr.number })
    }
  }

  return candidates.sort((a, b) => b.prNumber - a.prNumber)[0]
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
): Promise<{ number: number; title: string; body: string; headRef: string } | undefined> => {
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
    }
  } catch {
    return undefined
  }
}

const probePrMatchesIssue = (
  pr: { title: string; body: string; headRef: string },
  issue: LinearIssue,
  marker: string,
): boolean => {
  const haystack = `${pr.title}\n${pr.body}\n${pr.headRef}`
  return containsIssueKey(haystack, issue.key) && (pr.title === marker || pr.title.startsWith(`${marker} `))
}

const containsIssueKey = (value: string, issueKey: string): boolean => {
  const escaped = issueKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return new RegExp(`(^|[^A-Za-z0-9-])${escaped}([^A-Za-z0-9-]|$)`, 'i').test(value)
}

const isIssueFilePath = (path: string): boolean =>
  path.startsWith(`${ISSUE_ROOT}/`) &&
  path.endsWith('.json') &&
  !path.includes('/comments/') &&
  !path.includes('/by-state/')

const keyFromPath = (path: string): string => path.split('/').at(-1)?.split('__')[0] ?? path

const uuidFromPath = (path: string): string | undefined => path.split('__')[1]?.replace(/\.json$/, '')

const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined

const stateNameToId = (name: string | undefined): string | undefined =>
  name ? STATE_NAME_TO_ID[name] : undefined

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
