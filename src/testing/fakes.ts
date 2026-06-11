import type {
  ChangeEvent,
  EventPage,
  FleetClient,
  MountClient,
  RosterEntry,
  SendInput,
  SpawnInput,
  SpawnResult,
  SubscribeOptions,
  Subscription,
} from '../ports'

type ExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void

export class FakeMountClient implements MountClient {
  readonly files = new Map<string, { content: unknown; revision?: string }>()
  readonly writes: Array<{ path: string; content: unknown }> = []

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
    const entry = this.files.get(path)
    if (!entry) {
      throw new Error(`File not found: ${path}`)
    }

    return { ...entry }
  }

  async writeFile(path: string, content: unknown): Promise<void> {
    const revision = String((Number(this.files.get(path)?.revision ?? 0) || 0) + 1)
    this.files.set(path, { content, revision })
    this.writes.push({ path, content })
  }

  async listTree(prefix: string): Promise<string[]> {
    return [...this.files.keys()].filter((path) => path.startsWith(prefix)).sort()
  }

  subscribe(_globs: string[], onChange: (event: ChangeEvent) => void, _opts?: SubscribeOptions): Subscription {
    this.#subscribers.add(onChange)

    return {
      unsubscribe: async () => {
        this.#subscribers.delete(onChange)
      },
    }
  }

  async getEvents(opts: { cursor?: string; limit?: number }): Promise<EventPage> {
    const start = opts.cursor ? Number(opts.cursor) : 0
    const limit = opts.limit ?? this.#events.length
    const events = this.#events.slice(start, start + limit)
    const next = start + events.length

    return {
      events,
      nextCursor: next < this.#events.length ? String(next) : null,
    }
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

export class FakeFleetClient implements FleetClient {
  readonly spawns: SpawnInput[] = []
  readonly resumes: Array<{ name?: string; sessionRef: string; node?: 'self' | string }> = []
  readonly releases: Array<{ name: string; reason?: string }> = []
  readonly messages: SendInput[] = []

  #agents = new Set<string>()
  #exitListeners = new Set<ExitListener>()
  #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  #sessionRefs = new Map<string, string | undefined>()

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    this.spawns.push(input)
    this.#agents.add(input.name)
    return { name: input.name, sessionRef: this.#sessionRefs.get(input.name) ?? input.sessionRef }
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string }): Promise<SpawnResult> {
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
    return { eventId: `fake-${this.messages.length}`, targets: [input.to] }
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
}
