import type {
  ChangeEvent,
  EventPage,
  FleetClient,
  GithubConnectionWrite,
  AgentLifecycleSignal,
  AgentMessage,
  AgentUsage,
  MountClient,
  RosterEntry,
  SendInput,
  SpawnInput,
  SpawnResult,
  TeammateAgent,
  TeammateQuery,
  SubscribeOptions,
  Subscription,
  Capability,
  PreviewReference,
  PreviewStartInput,
  PreviewSweepInput,
  PreviewSweepResult,
} from '../ports'
import type { ResourceSubscriptionsClient } from '../subscriptions'

type ExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void
type AgentLifecycleSignalListener = (signal: AgentLifecycleSignal) => void | Promise<void>
type AgentUsageListener = (usage: AgentUsage) => void | Promise<void>

export class FakeMountClient implements MountClient {
  readonly writebackTransport = 'test'
  githubWrite?: GithubConnectionWrite
  resourceSubscriptions?: ResourceSubscriptionsClient
  readonly files = new Map<string, { content: unknown; revision?: string }>()
  readonly writes: Array<{ path: string; content: unknown }> = []
  readonly deletes: string[] = []
  readonly reads: string[] = []
  subscribeCount = 0
  /** Test toggle: when true, `isLocalMountAuthDegraded()` reports the terminal
   * scope-shortfall state so consumers (e.g. resume gating) can be exercised. */
  authDegraded = false

  #subscribers = new Set<(event: ChangeEvent) => void>()
  #events: ChangeEvent[] = []
  #readySubRoots = new Set<string>()
  #absentSubRoots = new Set<string>()
  #confirmations = new Map<string, 'acked' | 'pending' | 'failed' | 'timeout'>()

  constructor(initialFiles: Record<string, unknown> = {}, githubWrite?: GithubConnectionWrite) {
    this.githubWrite = githubWrite
    for (const [path, content] of Object.entries(initialFiles)) {
      this.files.set(path, { content })
    }
  }

  async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
    this.reads.push(path)
    const entry = this.files.get(path)
    if (!entry) {
      throw new Error(`File not found: ${path}`)
    }

    return { ...entry }
  }

  async writeFile(
    path: string,
    content: unknown,
    opts?: { guarded?: boolean; baseRevision?: string },
  ): Promise<{ targetRevision: string } | void> {
    // A missing path reads as revision '0', matching the cloud mount: when a
    // caller supplies no base it writes a missing file at '0' (404 -> keep the
    // default), so '0' is the create sentinel and must not read as a conflict
    // here (#346 review, cubic).
    if (opts?.baseRevision !== undefined && (this.files.get(path)?.revision ?? '0') !== opts.baseRevision) {
      throw Object.assign(new Error(`Revision conflict for ${path}`), { status: 409 })
    }
    const revision = String((Number(this.files.get(path)?.revision ?? 0) || 0) + 1)
    const existing = this.files.get(path)?.content
    const storedContent = mergedLinearIssueContent(existing, content) ?? content
    this.files.set(path, { content: storedContent, revision })
    this.writes.push({ path, content })
    return { targetRevision: revision }
  }

  async deleteFile(path: string): Promise<void> {
    if (!this.files.has(path)) {
      throw new Error(`File not found: ${path}`)
    }
    this.files.delete(path)
    this.deletes.push(path)
  }

  async listTree(prefix: string): Promise<string[]> {
    return [...this.files.keys()].filter((path) => path.startsWith(prefix)).sort()
  }

  isLocalMountAuthDegraded(): boolean {
    return this.authDegraded
  }

  subscribe(_globs: string[], onChange: (event: ChangeEvent) => void, _opts?: SubscribeOptions): Subscription {
    this.subscribeCount += 1
    this.#subscribers.add(onChange)

    return {
      unsubscribe: async () => {
        this.#subscribers.delete(onChange)
      },
    }
  }

  async getEvents(opts: { cursor?: string; limit?: number; provider?: string; last?: number }): Promise<EventPage> {
    const allEvents = opts.provider
      ? this.#events.filter((event) => eventProvider(event) === opts.provider)
      : this.#events
    const sourceEvents = opts.last === undefined
      ? allEvents
      : allEvents.slice(-Math.max(0, Math.trunc(opts.last)))
    const start = opts.cursor ? Number(opts.cursor) : 0
    const limit = opts.limit ?? sourceEvents.length
    const events = sourceEvents.slice(start, start + limit)
    const next = start + events.length

    return {
      events,
      nextCursor: next < sourceEvents.length ? String(next) : null,
    }
  }

  async getEventHighWatermark(opts: { provider?: string } = {}): Promise<string | undefined> {
    const events = opts.provider
      ? this.#events.filter((event) => event.resource.provider === opts.provider)
      : this.#events
    return events.at(-1)?.id
  }

  async confirmWrite(path: string, _opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    return this.#confirmations.get(path) ?? 'acked'
  }

  async ensureSubRoot(prefix: string, _opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'> {
    if (this.#absentSubRoots.has(prefix)) {
      return 'absent'
    }

    return this.#readySubRoots.size === 0 || this.#readySubRoots.has(prefix) ? 'ready' : 'absent'
  }

  setConfirmWrite(path: string, status: 'acked' | 'pending' | 'failed' | 'timeout'): void {
    this.#confirmations.set(path, status)
  }

  setSubRoot(prefix: string, status: 'ready' | 'absent'): void {
    if (status === 'ready') {
      this.#readySubRoots.add(prefix)
      this.#absentSubRoots.delete(prefix)
    } else {
      this.#absentSubRoots.add(prefix)
      this.#readySubRoots.delete(prefix)
    }
  }

  emit(event: ChangeEvent): void {
    this.#events.push(event)
    for (const subscriber of this.#subscribers) {
      subscriber(event)
    }
  }
}

const eventProvider = (event: ChangeEvent): string | undefined => {
  const record = event as unknown as Record<string, unknown>
  const resource = record.resource as Record<string, unknown> | undefined
  return typeof resource?.provider === 'string'
    ? resource.provider
    : typeof record.provider === 'string'
      ? record.provider
      : undefined
}

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const mergedLinearIssueContent = (existing: unknown, content: unknown): unknown | undefined => {
  const existingRecord = record(existing)
  const payload = record(existingRecord?.payload)
  const update = record(content)
  if (!existingRecord || existingRecord.objectType !== 'issue' || !payload || !update) return undefined
  if (Object.keys(update).some((key) => key !== 'stateId')) return undefined
  return {
    ...existingRecord,
    payload: {
      ...payload,
      ...update,
    },
  }
}

export class FakeFleetClient implements FleetClient {
  readonly placementLocality: 'local' | 'remote' = 'local'
  readonly lifecycleActionName?: string
  readonly spawns: SpawnInput[] = []
  readonly resumes: Array<{
    name?: string
    sessionRef: string
    identityKey?: string
    node?: 'self' | string
    capability?: Capability
    repo?: string
    clonePath?: string
    task?: string
  }> = []
  readonly releases: Array<{ name: string; reason?: string }> = []
  readonly messages: SendInput[] = []
  readonly inputs: Array<{ name: string; data: string }> = []
  readonly deliveryEvents: Array<
    | { kind: 'injected'; to: string; eventId: string }
    | { kind: 'input'; name: string; data: string }
  > = []
  readonly hydrated: Array<{ name: string; invocationId?: string; node?: string }> = []
  reconciles = 0
  preservedInfrastructure = 0
  readonly previewStarts: PreviewStartInput[] = []
  readonly previewRemovals: PreviewReference[] = []
  readonly previewSweeps: PreviewSweepInput[] = []
  readonly teammates: TeammateAgent[] = []

  #agents = new Set<string>()
  #tracked = new Map<string, { invocationId?: string; node?: string }>()
  #exitListeners = new Set<ExitListener>()
  #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  #agentMessageListeners = new Set<AgentMessageListener>()
  #agentLifecycleSignalListeners = new Set<AgentLifecycleSignalListener>()
  #agentUsageListeners = new Set<AgentUsageListener>()
  #sessionRefs = new Map<string, string | undefined>()

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    this.#agents.add(input.name)
    this.#tracked.set(input.name, {
      invocationId: input.invocationId,
      ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
    })
    return { name: input.name, sessionRef: this.#sessionRefs.get(input.name) ?? input.sessionRef }
  }

  async resume(input: {
    name?: string
    sessionRef: string
    identityKey?: string
    node?: 'self' | string
    capability?: Capability
    repo?: string
    clonePath?: string
    task?: string
  }): Promise<SpawnResult> {
    this.resumes.push(input)
    const name = input.name ?? input.sessionRef
    this.#agents.add(name)
    return { name, sessionRef: input.sessionRef }
  }

  async release(name: string, reason?: string): Promise<void> {
    this.releases.push({ name, reason })
    this.#agents.delete(name)
    this.#tracked.delete(name)
  }

  async discoverTeammates(query: TeammateQuery): Promise<TeammateAgent[]> {
    const skill = query.skill?.trim().toLowerCase()
    const tag = query.tag?.trim().toLowerCase()
    const q = query.q?.trim().toLowerCase()
    return this.teammates.filter((teammate) => {
      if (skill && !teammate.skills.some((candidate) =>
        candidate.id?.toLowerCase() === skill || candidate.name.toLowerCase() === skill)) return false
      if (tag && ![...teammate.tags, ...teammate.skills.flatMap((candidate) => candidate.tags ?? [])]
        .some((candidate) => candidate.toLowerCase() === tag)) return false
      if (q && !JSON.stringify(teammate).toLowerCase().includes(q)) return false
      return true
    })
  }

  async createPreview(input: PreviewStartInput): Promise<PreviewReference> {
    this.previewStarts.push(structuredClone(input))
    const httpsPort = input.preferredHttpsPort ?? 10_000 + this.previewStarts.length - 1
    return {
      id: `preview-${this.previewStarts.length}`,
      provider: 'tailscale-serve',
      namespace: input.namespace,
      owner: input.owner,
      service: input.service,
      repo: input.repo,
      url: `https://factory-node.tailnet.ts.net:${httpsPort}/`,
      configuredTargetPort: input.targetPort,
      targetPort: input.targetPort,
      httpsPort,
      access: 'tailnet',
      lifetime: 'issue',
      createdAt: '2026-07-20T12:00:00.000Z',
      startCommand: input.startCommand,
      process: {
        pid: 10_000 + this.previewStarts.length,
        startTime: '2026-07-20T12:00:00.000Z',
        cmdline: `factory-preview ${input.owner}`,
        cwd: input.checkoutPath,
        marker: `factory-preview-${this.previewStarts.length}`,
      },
      ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
    }
  }

  async removePreview(preview: PreviewReference): Promise<boolean> {
    this.previewRemovals.push(structuredClone(preview))
    return true
  }

  async reapPreviews(input: PreviewSweepInput): Promise<PreviewSweepResult> {
    this.previewSweeps.push(structuredClone(input))
    return { reaped: [], skipped: [] }
  }

  trackedAgents(): ReadonlyMap<string, { invocationId?: string; node?: string }> {
    return this.#tracked
  }

  hydrateTracked(agents: Array<{ name: string; invocationId?: string; node?: string }>): void {
    for (const agent of agents) {
      this.hydrated.push(agent)
      this.#tracked.set(agent.name, { invocationId: agent.invocationId, node: agent.node })
      this.#agents.add(agent.name)
    }
  }

  async reconcileTrackedAgents(): Promise<void> {
    this.reconciles += 1
  }

  async listAgents(): Promise<Array<{ name: string }>> {
    return [...this.#agents].sort().map((name) => ({ name }))
  }

  async roster(): Promise<RosterEntry> {
    return {
      agents: await this.listAgents(),
      nodes: [{ name: 'self', capabilities: ['spawn:codex', 'spawn:claude', 'workflow:run'], live: true }],
    }
  }

  async sendMessage(input: SendInput): Promise<void> {
    this.messages.push(input)
  }

  async waitForInjected(
    input: SendInput,
    _opts?: { timeoutMs?: number },
  ): Promise<{ eventId: string; targets: string[] }> {
    this.messages.push(input)
    const eventId = `fake-${this.messages.length}`
    this.deliveryEvents.push({ kind: 'injected', to: input.to, eventId })
    return { eventId, targets: [input.to] }
  }

  async sendInput(name: string, data: string): Promise<void> {
    this.inputs.push({ name, data })
    this.deliveryEvents.push({ kind: 'input', name, data })
  }

  onAgentExit(listener: ExitListener): () => void {
    this.#exitListeners.add(listener)
    return () => {
      this.#exitListeners.delete(listener)
    }
  }

  onDeliveryFailed(listener: DeliveryFailedListener): () => void {
    this.#deliveryFailedListeners.add(listener)
    return () => {
      this.#deliveryFailedListeners.delete(listener)
    }
  }

  onAgentMessage(listener: AgentMessageListener): () => void {
    this.#agentMessageListeners.add(listener)
    return () => {
      this.#agentMessageListeners.delete(listener)
    }
  }

  onAgentLifecycleSignal(listener: AgentLifecycleSignalListener): () => void {
    this.#agentLifecycleSignalListeners.add(listener)
    return () => {
      this.#agentLifecycleSignalListeners.delete(listener)
    }
  }

  onAgentUsage(listener: AgentUsageListener): () => void {
    this.#agentUsageListeners.add(listener)
    return () => {
      this.#agentUsageListeners.delete(listener)
    }
  }

  preserveInfrastructureOnDispose(): void {
    this.preservedInfrastructure += 1
  }

  async dispose(): Promise<void> {}

  setSessionRef(name: string, sessionRef?: string): void {
    this.#sessionRefs.set(name, sessionRef)
  }

  emitAgentExit(name: string, reason?: string): void {
    this.#agents.delete(name)
    this.#tracked.delete(name)
    for (const listener of this.#exitListeners) {
      listener(name, reason)
    }
  }

  emitDeliveryFailed(info: { to: string; msgId?: string; reason?: string }): void {
    for (const listener of this.#deliveryFailedListeners) {
      listener(info)
    }
  }

  emitAgentMessage(message: AgentMessage): void {
    for (const listener of this.#agentMessageListeners) {
      listener(message)
    }
  }

  async emitAgentLifecycleSignal(signal: AgentLifecycleSignal): Promise<void> {
    for (const listener of this.#agentLifecycleSignalListeners) {
      await listener(signal)
    }
  }

  async emitAgentUsage(usage: AgentUsage): Promise<void> {
    for (const listener of this.#agentUsageListeners) {
      await listener(usage)
    }
  }
}
