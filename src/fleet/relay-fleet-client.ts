import { randomUUID } from 'node:crypto'

import type { AgentMessage, Capability, FleetClient, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports'

type AgentExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void
type RelayFleetEventListener = (event: RelayFleetEvent) => void

export type RelayFleetEvent = Record<string, unknown> & { type?: string }
type RelayFetchLike = (url: string, init?: {
  method?: string
  headers?: Record<string, string>
  body?: string
}) => Promise<{
  ok: boolean
  status: number
  statusText: string
  json(): Promise<unknown>
  text(): Promise<string>
}>
type RelayWebSocketLike = {
  onmessage: ((event: { data: unknown }) => void) | null
  onclose: (() => void) | null
  onerror: (() => void) | null
  close(): void
}
type RelayWebSocketConstructor = new (url: string) => RelayWebSocketLike

export type RelayInvocationStatus = 'pending' | 'dispatched' | 'invoked' | 'completed' | 'failed' | 'denied' | string

export interface RelayActionAck {
  invocationId: string
  actionName: string
  status?: RelayInvocationStatus
  dispatchedNodeId?: string | null
  input?: Record<string, unknown>
}

export interface RelayActionInvocation extends RelayActionAck {
  output?: unknown
  error?: string | null
  completedAt?: string | null
}

export interface RelayFleetNode {
  name: string
  status?: 'online' | 'offline' | 'unknown' | string
  live?: boolean
  capabilities?: Array<string | { name?: string }>
}

export interface RelayFleetAgent {
  name: string
}

export interface RelayFleetTransport {
  invokeAction(actionName: string, input: Record<string, unknown>, opts: { invocationId: string }): Promise<RelayActionAck>
  getInvocation(actionName: string, invocationId: string): Promise<RelayActionInvocation>
  listAgents(): Promise<RelayFleetAgent[]>
  listNodes(): Promise<RelayFleetNode[]>
  sendMessage(input: SendInput): Promise<{ eventId?: string; targets?: string[] }>
  waitForDelivery?(eventId: string, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }>
  onEvent?(listener: RelayFleetEventListener): () => void
  dispose?(): Promise<void> | void
}

export interface RelayFleetClientOptions {
  transport?: RelayFleetTransport
  baseUrl?: string
  workspaceKey?: string
  agentToken?: string
  spawnActionName?: string
  releaseActionName?: string
  completionTimeoutMs?: number
  pollIntervalMs?: number
  fetch?: RelayFetchLike
  sleep?: (ms: number) => Promise<void>
}

const knownCapabilities = new Set<Capability>(['spawn:claude', 'spawn:codex', 'workflow:run'])
const openStatuses = new Set(['pending', 'dispatched', 'invoked'])
const terminalStatuses = new Set(['completed', 'failed', 'denied'])
const DEFAULT_COMPLETION_TIMEOUT_MS = 5 * 60_000
const DEFAULT_POLL_INTERVAL_MS = 1_000

export class RelayFleetClient implements FleetClient {
  readonly #options: RelayFleetClientOptions
  // Built lazily on first network use so that constructing the client (and
  // therefore createFleet({ backend: 'relay' })) never throws merely because no
  // token is configured. The token check lives in HttpRelayFleetTransport's
  // constructor and only fires when a method actually needs the transport.
  #transport: RelayFleetTransport | undefined
  readonly #spawnActionName: string
  readonly #releaseActionName: string
  readonly #completionTimeoutMs: number
  readonly #pollIntervalMs: number
  readonly #sleep: (ms: number) => Promise<void>
  readonly #agentExitListeners = new Set<AgentExitListener>()
  readonly #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  readonly #agentMessageListeners = new Set<AgentMessageListener>()
  readonly #eventUnsubscribers: Array<() => void> = []
  #subscribed = false
  #disposed = false

  constructor(options: RelayFleetClientOptions = {}) {
    this.#options = options
    this.#spawnActionName = options.spawnActionName ?? 'spawn'
    this.#releaseActionName = options.releaseActionName ?? 'release'
    this.#completionTimeoutMs = options.completionTimeoutMs ?? DEFAULT_COMPLETION_TIMEOUT_MS
    this.#pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)))
    this.#transport = options.transport
  }

  #getTransport(): RelayFleetTransport {
    this.#transport ??= new HttpRelayFleetTransport(this.#options)
    return this.#transport
  }

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    const invocationId = input.invocationId ?? newInvocationId('spawn', input.name)
    const actionInput = spawnActionInput(input, { includeSelfNode: false })
    const ack = await this.#getTransport().invokeAction(this.#spawnActionName, actionInput, { invocationId })
    const invocation = await this.#awaitInvocation(this.#spawnActionName, ack.invocationId || invocationId, ack)
    return spawnResultFromInvocation(input.name, input.sessionRef, invocation)
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    const name = input.name ?? input.sessionRef
    const invocationId = newInvocationId('resume', name)
    const actionInput = spawnActionInput({
      name,
      capability: input.capability ?? 'spawn:codex',
      node: input.node,
      sessionRef: input.sessionRef,
    }, { includeSelfNode: false })
    const ack = await this.#getTransport().invokeAction(this.#spawnActionName, actionInput, { invocationId })
    const invocation = await this.#awaitInvocation(this.#spawnActionName, ack.invocationId || invocationId, ack)
    return spawnResultFromInvocation(name, input.sessionRef, invocation)
  }

  async release(name: string, reason?: string): Promise<void> {
    const invocationId = newInvocationId('release', name)
    const ack = await this.#getTransport().invokeAction(this.#releaseActionName, {
      name,
      agent: name,
      ...(reason ? { reason } : {}),
    }, { invocationId })
    await this.#awaitInvocation(this.#releaseActionName, ack.invocationId || invocationId, ack)
  }

  async roster(): Promise<RosterEntry> {
    const transport = this.#getTransport()
    const [agents, nodes] = await Promise.all([
      transport.listAgents(),
      transport.listNodes(),
    ])

    return {
      agents: agents.map((agent) => ({ name: agent.name })),
      nodes: nodes.map((node) => ({
        name: node.name,
        capabilities: normalizeCapabilities(node.capabilities),
        live: node.live ?? node.status === 'online',
      })),
    }
  }

  async sendMessage(input: SendInput): Promise<void> {
    await this.#getTransport().sendMessage(input)
  }

  async waitForInjected(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }> {
    const transport = this.#getTransport()
    const sent = await transport.sendMessage(input)
    const eventId = sent.eventId
    const targets = sent.targets ?? [input.to]
    if (!eventId) {
      return { eventId: newInvocationId('message', input.to), targets }
    }
    return await transport.waitForDelivery?.(eventId, opts) ?? { eventId, targets }
  }

  onDeliveryFailed(listener: DeliveryFailedListener): () => void {
    this.#ensureEventSubscription()
    this.#deliveryFailedListeners.add(listener)
    return () => {
      this.#deliveryFailedListeners.delete(listener)
    }
  }

  onAgentMessage(listener: AgentMessageListener): () => void {
    this.#ensureEventSubscription()
    this.#agentMessageListeners.add(listener)
    return () => {
      this.#agentMessageListeners.delete(listener)
    }
  }

  onAgentExit(listener: AgentExitListener): () => void {
    this.#ensureEventSubscription()
    this.#agentExitListeners.add(listener)
    return () => {
      this.#agentExitListeners.delete(listener)
    }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    for (const unsubscribe of this.#eventUnsubscribers.splice(0)) {
      unsubscribe()
    }
    this.#agentExitListeners.clear()
    this.#deliveryFailedListeners.clear()
    this.#agentMessageListeners.clear()
    // Only dispose a transport we actually built; never construct one here.
    await this.#transport?.dispose?.()
    this.#subscribed = false
  }

  async #awaitInvocation(
    actionName: string,
    invocationId: string,
    initial: Pick<RelayActionInvocation, 'status'>,
  ): Promise<RelayActionInvocation> {
    let status = initial.status ?? 'pending'
    let invocation: RelayActionInvocation | undefined
    const deadline = Date.now() + this.#completionTimeoutMs

    while (!terminalStatuses.has(status)) {
      if (Date.now() > deadline) {
        throw new Error(`Timed out waiting for ${actionName} invocation ${invocationId} to complete (last status: ${status})`)
      }
      if (!openStatuses.has(status)) {
        throw new Error(`Unexpected ${actionName} invocation ${invocationId} status: ${status}`)
      }
      await this.#sleep(this.#pollIntervalMs)
      invocation = await this.#getTransport().getInvocation(actionName, invocationId)
      status = invocation.status ?? 'pending'
    }

    invocation ??= await this.#getTransport().getInvocation(actionName, invocationId)
    if (status === 'failed' || status === 'denied') {
      throw new Error(`${actionName} invocation ${invocationId} ${status}${invocation.error ? `: ${invocation.error}` : ''}`)
    }
    return invocation
  }

  #ensureEventSubscription(): void {
    if (this.#subscribed) return
    this.#subscribed = true
    const unsubscribe = this.#getTransport().onEvent?.((event) => this.#handleEvent(event))
    if (unsubscribe) this.#eventUnsubscribers.push(unsubscribe)
  }

  #handleEvent(event: RelayFleetEvent): void {
    const deliveryFailed = deliveryFailedFromEvent(event)
    if (deliveryFailed) {
      for (const listener of this.#deliveryFailedListeners) {
        listener(deliveryFailed)
      }
    }

    const message = agentMessageFromEvent(event)
    if (message) {
      for (const listener of this.#agentMessageListeners) {
        listener(message)
      }
    }

    const exit = agentExitFromEvent(event)
    if (exit) {
      for (const listener of this.#agentExitListeners) {
        listener(exit.name, exit.reason)
      }
    }
  }
}

class HttpRelayFleetTransport implements RelayFleetTransport {
  readonly #baseUrl: string
  readonly #token: string
  readonly #fetch: RelayFetchLike
  readonly #eventListeners = new Set<RelayFleetEventListener>()
  #socket?: RelayWebSocketLike

  constructor(options: RelayFleetClientOptions) {
    this.#baseUrl = (options.baseUrl ?? env('RELAYCAST_BASE_URL') ?? env('AGENT_RELAY_BASE_URL') ?? 'https://api.relaycast.dev').replace(/\/+$/, '')
    this.#token = options.agentToken ?? env('RELAY_AGENT_TOKEN') ?? options.workspaceKey ?? env('RELAY_WORKSPACE_KEY') ?? env('RELAY_API_KEY') ?? ''
    if (!this.#token) {
      throw new Error('RelayFleetClient requires agentToken or workspaceKey (or RELAY_AGENT_TOKEN / RELAY_WORKSPACE_KEY)')
    }
    this.#fetch = options.fetch ?? globalFetch
  }

  async invokeAction(actionName: string, input: Record<string, unknown>, opts: { invocationId: string }): Promise<RelayActionAck> {
    const data = await this.#request(`/v1/actions/${encodeURIComponent(actionName)}/invoke`, {
      input,
      invocation_id: opts.invocationId,
      invocationId: opts.invocationId,
    })
    return invocationAckFrom(data, opts.invocationId, actionName)
  }

  async getInvocation(actionName: string, invocationId: string): Promise<RelayActionInvocation> {
    const data = await this.#request(`/v1/actions/${encodeURIComponent(actionName)}/invocations/${encodeURIComponent(invocationId)}`)
    return invocationFrom(data, invocationId, actionName)
  }

  async listAgents(): Promise<RelayFleetAgent[]> {
    const data = await this.#request('/v1/agents?status=all')
    return Array.isArray(data) ? data.map(agentFrom).filter(isRelayFleetAgent) : []
  }

  async listNodes(): Promise<RelayFleetNode[]> {
    const data = await this.#request('/v1/nodes')
    return Array.isArray(data) ? data.map(nodeFrom).filter(isRelayFleetNode) : []
  }

  async sendMessage(input: SendInput): Promise<{ eventId?: string; targets?: string[] }> {
    const data = input.to.startsWith('#')
      ? await this.#request(`/v1/channels/${encodeURIComponent(input.to.slice(1))}/messages`, {
        text: input.text,
        ...(input.data ? { metadata: input.data } : {}),
      })
      : await this.#request('/v1/dm', {
        to: input.to.startsWith('@') ? input.to.slice(1) : input.to,
        text: input.text,
        ...(input.data ? { metadata: input.data } : {}),
      })
    return {
      eventId: readString(asRecord(data), 'id', 'message_id', 'event_id'),
      targets: [input.to],
    }
  }

  onEvent(listener: RelayFleetEventListener): () => void {
    this.#eventListeners.add(listener)
    this.#connectEvents()
    return () => {
      this.#eventListeners.delete(listener)
      if (this.#eventListeners.size === 0) {
        this.#socket?.close()
        this.#socket = undefined
      }
    }
  }

  dispose(): void {
    this.#socket?.close()
    this.#socket = undefined
    this.#eventListeners.clear()
  }

  async #request(path: string, body?: Record<string, unknown>): Promise<unknown> {
    const response = await this.#fetch(`${this.#baseUrl}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: {
        Authorization: `Bearer ${this.#token}`,
        ...(body ? { 'Content-Type': 'application/json' } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    })
    const payload = await readResponsePayload(response)
    const record = asRecord(payload)
    if (!response.ok || record?.ok === false) {
      const error = asRecord(record?.error)
      const message = readString(error, 'message') ?? response.statusText
      throw new Error(`Relay fleet request failed (${response.status}): ${message}`)
    }
    return record && 'data' in record ? record.data : payload
  }

  #connectEvents(): void {
    if (this.#socket) return
    const WebSocketCtor = (globalThis as { WebSocket?: RelayWebSocketConstructor }).WebSocket
    if (!WebSocketCtor) return

    const url = new URL('/v1/ws', this.#baseUrl.replace(/^http/, 'ws'))
    url.searchParams.set('token', this.#token)
    const socket = new WebSocketCtor(url.toString())
    this.#socket = socket
    socket.onmessage = (message) => {
      try {
        const parsed = JSON.parse(String(message.data))
        if (parsed && typeof parsed === 'object') {
          this.#emitEvent(parsed as RelayFleetEvent)
        }
      } catch {
        // Drop malformed event frames; the polling path remains authoritative for invocations.
      }
    }
    socket.onclose = () => {
      if (this.#socket === socket) this.#socket = undefined
    }
    socket.onerror = () => {
      if (this.#socket === socket) this.#socket = undefined
      try {
        socket.close()
      } catch {
        // noop
      }
    }
  }

  #emitEvent(event: RelayFleetEvent): void {
    for (const listener of this.#eventListeners) {
      listener(event)
    }
  }
}

function spawnActionInput(input: SpawnInput, opts: { includeSelfNode: boolean }): Record<string, unknown> {
  return definedRecord({
    capability: input.capability,
    name: input.name,
    agent: input.name,
    node: input.node && (opts.includeSelfNode || input.node !== 'self') ? input.node : undefined,
    session_ref: input.sessionRef,
    invocationId: input.invocationId,
    task: input.task,
    workflow: input.workflow,
    inputs: input.inputs,
    model: input.model,
    cwd: input.cwd,
    channels: input.channel ? [input.channel] : undefined,
    restart_policy: input.restartPolicy,
  })
}

function spawnResultFromInvocation(fallbackName: string, fallbackSessionRef: string | undefined, invocation: RelayActionInvocation): SpawnResult {
  const output = asRecord(invocation.output)
  const agent = asRecord(output?.agent)
  const name = readString(output, 'name', 'agent_name', 'agentName') ?? readString(agent, 'name') ?? fallbackName
  const sessionRef = readString(output, 'session_ref', 'sessionRef', 'session_id', 'sessionId')
    ?? readString(agent, 'session_ref', 'sessionRef', 'session_id', 'sessionId')
    ?? fallbackSessionRef
  const pid = readNumber(output, 'pid') ?? readNumber(agent, 'pid')
  const pids = readNumberArray(output, 'pids') ?? readNumberArray(agent, 'pids')
  return {
    name,
    ...(sessionRef ? { sessionRef } : {}),
    ...(pid !== undefined ? { pid } : {}),
    ...(pids ? { pids } : {}),
  }
}

function normalizeCapabilities(capabilities: RelayFleetNode['capabilities']): Capability[] {
  const names = (capabilities ?? [])
    .map((capability) => typeof capability === 'string' ? capability : capability.name)
    .filter((capability): capability is Capability => typeof capability === 'string' && knownCapabilities.has(capability as Capability))
  return [...new Set(names)]
}

function deliveryFailedFromEvent(event: RelayFleetEvent): { to: string; msgId?: string; reason?: string } | undefined {
  const type = event.type
  if (type !== 'delivery.failed' && type !== 'delivery_failed') return undefined
  const to = readString(event, 'to', 'recipient', 'recipient_name', 'recipientName', 'agent_name', 'agentName')
  if (!to) return undefined
  return definedRecord({
    to,
    msgId: readString(event, 'message_id', 'messageId', 'msg_id', 'msgId', 'id'),
    reason: readString(event, 'reason', 'error'),
  }) as { to: string; msgId?: string; reason?: string }
}

function agentMessageFromEvent(event: RelayFleetEvent): AgentMessage | undefined {
  const type = event.type
  if (type !== 'message.created' && type !== 'dm.received' && type !== 'group_dm.received' && type !== 'thread.reply') {
    return undefined
  }
  const message = asRecord(event.message) ?? event
  const fromRecord = asRecord(message.from)
  const target = asRecord(message.target)
  const from = readString(fromRecord, 'name') ?? readString(message, 'from', 'from_name', 'fromName', 'agent_name', 'agentName')
  const body = readString(message, 'text', 'body')
  if (!from || body === undefined) return undefined
  const targetName = readString(target, 'agentName', 'agent_name', 'channelName', 'channel_name')
    ?? readString(message, 'to', 'target', 'channel', 'channel_name', 'channelName')
    ?? 'factory'
  return definedRecord({
    from,
    target: targetName,
    body,
    threadId: readString(message, 'threadId', 'thread_id', 'parentId', 'parent_id'),
    eventId: readString(message, 'id', 'messageId', 'message_id', 'event_id', 'eventId'),
  }) as AgentMessage
}

function agentExitFromEvent(event: RelayFleetEvent): { name: string; reason?: string } | undefined {
  const type = event.type
  if (type !== 'agent.exited' && type !== 'agent.exit' && type !== 'agent.status.offline' && type !== 'session.released') {
    return undefined
  }
  const agent = asRecord(event.agent)
  const name = readString(event, 'name', 'agent_name', 'agentName') ?? readString(agent, 'name')
  if (!name) return undefined
  return definedRecord({
    name,
    reason: readString(event, 'reason', 'status') ?? (type === 'agent.status.offline' ? 'offline' : undefined),
  }) as { name: string; reason?: string }
}

function invocationAckFrom(value: unknown, fallbackInvocationId: string, fallbackActionName: string): RelayActionAck {
  const record = asRecord(value) ?? {}
  return {
    invocationId: readString(record, 'invocation_id', 'invocationId') ?? fallbackInvocationId,
    actionName: readString(record, 'action_name', 'actionName') ?? fallbackActionName,
    status: readString(record, 'status'),
    dispatchedNodeId: readString(record, 'dispatched_node_id', 'dispatchedNodeId') ?? null,
    input: asRecord(record.input),
  }
}

function invocationFrom(value: unknown, fallbackInvocationId: string, fallbackActionName: string): RelayActionInvocation {
  const record = asRecord(value) ?? {}
  return {
    ...invocationAckFrom(record, fallbackInvocationId, fallbackActionName),
    output: record.output,
    error: readString(record, 'error') ?? null,
    completedAt: readString(record, 'completed_at', 'completedAt') ?? null,
  }
}

function agentFrom(value: unknown): RelayFleetAgent | undefined {
  const record = asRecord(value)
  const name = readString(record, 'name')
  return name ? { name } : undefined
}

function isRelayFleetAgent(value: RelayFleetAgent | undefined): value is RelayFleetAgent {
  return !!value
}

function nodeFrom(value: unknown): RelayFleetNode | undefined {
  const record = asRecord(value)
  const name = readString(record, 'name')
  if (!record || !name) return undefined
  return {
    name,
    status: readString(record, 'status'),
    live: readBoolean(record, 'live') ?? readBoolean(record, 'handlers_live', 'handlersLive'),
    capabilities: Array.isArray(record.capabilities) ? record.capabilities as RelayFleetNode['capabilities'] : [],
  }
}

function isRelayFleetNode(value: RelayFleetNode | undefined): value is RelayFleetNode {
  return !!value
}

async function readResponsePayload(response: Awaited<ReturnType<RelayFetchLike>>): Promise<unknown> {
  try {
    return await response.json()
  } catch {
    return await response.text()
  }
}

function definedRecord(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined))
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function readString(record: Record<string, unknown> | undefined, ...keys: string[]): string | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'string' && value.length > 0) return value
  }
  return undefined
}

function readNumber(record: Record<string, unknown> | undefined, ...keys: string[]): number | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'number' && Number.isFinite(value)) return value
  }
  return undefined
}

function readBoolean(record: Record<string, unknown> | undefined, ...keys: string[]): boolean | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'boolean') return value
  }
  return undefined
}

function readNumberArray(record: Record<string, unknown> | undefined, ...keys: string[]): number[] | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (Array.isArray(value)) {
      const numbers = value.filter((item): item is number => typeof item === 'number' && Number.isFinite(item))
      if (numbers.length > 0) return numbers
    }
  }
  return undefined
}

function newInvocationId(prefix: string, name: string): string {
  return `factory:${prefix}:${name}:${randomUUID()}`
}

function env(name: string): string | undefined {
  const value = process.env[name]
  return value && value.trim() ? value.trim() : undefined
}

function globalFetch(...args: Parameters<RelayFetchLike>): ReturnType<RelayFetchLike> {
  return fetch(...args) as ReturnType<RelayFetchLike>
}
