import type {
  ChangeEvent,
  EventPage,
  FleetClient,
  AgentMessage,
  MountClient,
  RosterEntry,
  SendInput,
  SpawnInput,
  SpawnResult,
  SubscribeOptions,
  Subscription,
  Capability,
} from '../ports'

type ExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void

export class FakeMountClient implements MountClient {
  readonly writebackTransport = 'test'
  readonly files = new Map<string, { content: unknown; revision?: string }>()
  readonly writes: Array<{ path: string; content: unknown }> = []
  readonly deletes: string[] = []
  readonly reads: string[] = []
  subscribeCount = 0

  #subscribers = new Set<(event: ChangeEvent) => void>()
  #events: ChangeEvent[] = []
  #readySubRoots = new Set<string>()
  #absentSubRoots = new Set<string>()
  #confirmations = new Map<string, 'acked' | 'pending' | 'failed' | 'timeout'>()

  constructor(initialFiles: Record<string, unknown> = {}) {
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

  async writeFile(path: string, content: unknown, _opts?: { guarded?: boolean }): Promise<void> {
    const revision = String((Number(this.files.get(path)?.revision ?? 0) || 0) + 1)
    const existing = this.files.get(path)?.content
    const storedContent = mergedLinearIssueContent(existing, content) ?? content
    this.files.set(path, { content: storedContent, revision })
    this.writes.push({ path, content })
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
  readonly spawns: SpawnInput[] = []
  readonly resumes: Array<{ name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }> = []
  readonly releases: Array<{ name: string; reason?: string }> = []
  readonly messages: SendInput[] = []
  readonly inputs: Array<{ name: string; data: string }> = []
  readonly deliveryEvents: Array<
    | { kind: 'injected'; to: string; eventId: string }
    | { kind: 'input'; name: string; data: string }
  > = []

  #agents = new Set<string>()
  #exitListeners = new Set<ExitListener>()
  #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  #agentMessageListeners = new Set<AgentMessageListener>()
  #sessionRefs = new Map<string, string | undefined>()

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    this.#agents.add(input.name)
    return { name: input.name, sessionRef: this.#sessionRefs.get(input.name) ?? input.sessionRef }
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    this.resumes.push(input)
    const name = input.name ?? input.sessionRef
    this.#agents.add(name)
    return { name, sessionRef: input.sessionRef }
  }

  async release(name: string, reason?: string): Promise<void> {
    this.releases.push({ name, reason })
    this.#agents.delete(name)
  }

  async listAgents(): Promise<Array<{ name: string }>> {
    return [...this.#agents].sort().map((name) => ({ name }))
  }

  async roster(): Promise<RosterEntry> {
    return {
      agents: await this.listAgents(),
      nodes: [{ name: 'self', capabilities: ['spawn:codex', 'spawn:claude'], live: true }],
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

  async dispose(): Promise<void> {}

  setSessionRef(name: string, sessionRef?: string): void {
    this.#sessionRefs.set(name, sessionRef)
  }

  emitAgentExit(name: string, reason?: string): void {
    this.#agents.delete(name)
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
}
