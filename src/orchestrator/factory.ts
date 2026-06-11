import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import { linearByStatePath } from '../constants/linear'
import { GithubMergeGate, type GithubMergeGate as GithubMergeGatePort } from '../github/merge-gate'
import type { AgentSpec, FleetClient, LinearWriteback, MountClient, SlackWriteback, Subscription } from '../ports'
import type { Clock, Logger } from '../ports/system'
import { HeuristicTriage, TieredTriage } from '../triage'
import type {
  DispatchResult,
  Factory,
  FactoryEventPayload,
  FactoryPorts,
  FactoryStatus,
  IssueRef,
  IterationReport,
  LinearIssue,
  TriageDecision,
  TriageEngine,
} from '../types'
import { MountGithubRead, MountLinearWriteback, MountSlackWriteback } from '../writeback'
import { asRecord, parseJsonContent, wrappedPayload } from '../writeback/shared'
import { BatchTracker, type InFlightIssue, issueKey } from './batch-tracker'

type FactoryEvent = 'issue-queued' | 'dispatched' | 'issue-done' | 'writeback-verified' | 'error'
type Listener = (payload: FactoryEventPayload) => void

const ISSUE_ROOT = '/linear/issues'
const READY_EVENTS_LIMIT = 100

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
  readonly #logger: Logger
  readonly #clock: Clock
  readonly #batch: BatchTracker
  readonly #listeners = new Map<FactoryEvent, Set<Listener>>()
  readonly #counters: Record<string, number> = {}
  readonly #criticalMessages = new Map<string, { issue: IssueRef; input: Parameters<FleetClient['sendMessage']>[0] }>()
  readonly #resumeInFlight = new Map<string, Promise<void>>()
  readonly #resumedExitKeys = new Set<string>()
  #subscription?: Subscription
  #offAgentExit?: () => void
  #offDeliveryFailed?: () => void
  #starting?: Promise<void>
  #started = false

  constructor(config: FactoryConfig, ports: FactoryPorts) {
    this.#config = config
    this.#mount = ports.mount
    this.#fleet = ports.fleet
    this.#triage = ports.triage ?? new TieredTriage(new HeuristicTriage())
    this.#linear = ports.linear ?? MountLinearWriteback(ports.mount, {
      stateIds: config.stateIds,
      safety: config.safety,
    })
    this.#slack = ports.slack ?? (config.slack ? MountSlackWriteback(ports.mount, config.slack) : undefined)
    void (ports.github ?? MountGithubRead(ports.mount))
    this.#mergeGate = ports.mergeGate ?? new GithubMergeGate()
    this.#logger = ports.logger ?? console
    this.#clock = ports.clock ?? realClock
    this.#batch = new BatchTracker(config.batchSize)
    this.#wireFleetEvents()
  }

  async start(): Promise<void> {
    if (this.#started) {
      return
    }

    if (this.#starting) {
      return this.#starting
    }

    this.#starting = this.#start()
    try {
      await this.#starting
    } finally {
      this.#starting = undefined
    }
  }

  async #start(): Promise<void> {
    const ready = await this.#mount.ensureSubRoot(ISSUE_ROOT, { timeoutMs: 90_000 })
    if (ready !== 'ready') {
      this.#error(new Error(`${ISSUE_ROOT} sub-root is not mounted`))
      return
    }

    this.#wireFleetEvents()

    await this.#backfillReadyIssues()
    this.#subscription = this.#mount.subscribe([`${ISSUE_ROOT}/**/*.json`], (event) => {
      void this.#handleChange(event.resource.path)
    })
    this.#started = true
  }

  async stop(): Promise<void> {
    this.#started = false
    await this.#subscription?.unsubscribe()
    this.#subscription = undefined
    this.#offAgentExit?.()
    this.#offDeliveryFailed?.()
    this.#offAgentExit = undefined
    this.#offDeliveryFailed = undefined
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
        await this.#linear.postComment(issue, comment)
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

  async #handleChange(path: string): Promise<void> {
    if (!isIssueFilePath(path)) {
      return
    }

    try {
      const issue = await this.#readIssue(path)
      if (issue?.stateId !== this.#config.stateIds.readyForAgent) {
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

      for (const agent of record.agents.keys()) {
        await this.#fleet.release(agent, 'issue-done')
      }

      this.#increment('done')
      this.#emit('issue-done', { issue: record.issue })
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
  const stateId = stringValue(payload.stateId) ?? stringValue(state?.id) ?? ''
  const stateName = stringValue(state?.name) ?? stringValue(payload.state_name)

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

const isIssueFilePath = (path: string): boolean =>
  path.startsWith(`${ISSUE_ROOT}/`) &&
  path.endsWith('.json') &&
  !path.includes('/comments/') &&
  !path.includes('/by-state/')

const keyFromPath = (path: string): string => path.split('/').at(-1)?.split('__')[0] ?? path

const uuidFromPath = (path: string): string | undefined => path.split('__')[1]?.replace(/\.json$/, '')

const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined

const recordName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.key) ?? stringValue(record?.id)
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
