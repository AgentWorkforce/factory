import { HarnessDriverClient } from '@agent-relay/harness-driver'

import type { BrokerEvent, ListAgent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import type { Capability, FleetClient, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports/fleet'
import type { Logger } from '../ports/system'

type SpawnedHandleLike = { name: string; sessionId?: string; session_ref?: string; sessionRef?: string; pid?: number }
type HarnessEventListener = (event: BrokerEvent) => void
type DriverAgentLike = { name: string; sessionId?: string }
type DriverDeliveryEventLike = BrokerEvent

export interface HarnessDriverClientLike {
  spawnPty(input: SpawnPtyInput): Promise<SpawnedHandleLike>
  release(name: string, reason?: string): Promise<{ name: string }>
  listAgents(): Promise<Array<Pick<ListAgent, 'name'>>>
  sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }>
  connectEvents?(sinceSeq?: number): void
  onEvent?(listener: HarnessEventListener): () => void
  addListener?(event: 'agentExited', listener: (agent: DriverAgentLike) => void): () => void
  addListener?(event: 'deliveryUpdate', listener: (event: DriverDeliveryEventLike) => void): () => void
}

export interface InternalFleetClientOptions {
  client?: HarnessDriverClientLike
  cwd?: string
  connectionPath?: string
  resumeCapability?: Capability
  logger?: Logger
}

type AgentExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type PendingInjectedWait = {
  targets: string[]
  timeout: ReturnType<typeof setTimeout>
  resolve: (result: { eventId: string; targets: string[] }) => void
  reject: (error: Error) => void
}

const capabilityCli: Record<Capability, string> = {
  'spawn:claude': 'claude',
  'spawn:codex': 'codex',
}

const selfNode: RosterEntry['nodes'][number] = {
  name: 'self',
  capabilities: ['spawn:claude', 'spawn:codex'],
  live: true,
}

export class InternalFleetClient implements FleetClient {
  readonly #client: HarnessDriverClientLike
  readonly #cwd?: string
  readonly #resumeCapability: Capability
  readonly #logger?: Logger
  readonly #agentExitListeners = new Set<AgentExitListener>()
  readonly #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  readonly #seenEvents: string[] = []
  readonly #seenEventKeys = new Set<string>()
  readonly #pendingInjected = new Map<string, PendingInjectedWait>()
  readonly #injectedEventIds: string[] = []
  readonly #injectedEventIdSet = new Set<string>()
  readonly #failedDeliveries = new Map<string, Error>()
  readonly #failedDeliveryIds: string[] = []
  readonly #exitedAgentNames = new Set<string>()
  #suppressedDuplicateEvents = 0
  #suppressedDuplicateAgentExits = 0
  #missingIdentityEvents = 0
  #subscribed = false

  constructor(options: InternalFleetClientOptions = {}) {
    this.#cwd = options.cwd
    this.#resumeCapability = options.resumeCapability ?? 'spawn:codex'
    this.#logger = options.logger
    this.#client = options.client ?? HarnessDriverClient.connect({ cwd: options.cwd, connectionPath: options.connectionPath })
  }

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    assertSelfNode(input.node)

    const handle = await this.#client.spawnPty({
      name: input.name,
      cli: capabilityCli[input.capability],
      channels: input.channel ? [input.channel] : undefined,
      task: input.task,
      model: input.model,
      cwd: input.cwd ?? this.#cwd,
      restartPolicy: input.restartPolicy,
      continueFrom: input.sessionRef,
    })

    this.#clearAgentExitLatch(handle.name)

    return spawnResultFrom(handle)
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    assertSelfNode(input.node)

    const handle = await this.#client.spawnPty({
      name: input.name ?? input.sessionRef,
      // followups [fleet→W6]: W6 owns resume-vs-respawn and passes the per-agent capability.
      cli: capabilityCli[input.capability ?? this.#resumeCapability],
      cwd: this.#cwd,
      continueFrom: input.sessionRef,
    })

    this.#clearAgentExitLatch(handle.name)

    return { ...spawnResultFrom(handle), sessionRef: sessionRefFrom(handle) ?? input.sessionRef }
  }

  async release(name: string, reason?: string): Promise<void> {
    await this.#client.release(name, reason)
  }

  async roster(): Promise<RosterEntry> {
    const agents = await this.#client.listAgents()
    return {
      agents: agents.map((agent) => ({ name: agent.name })),
      nodes: [selfNode],
    }
  }

  async sendMessage(input: SendInput): Promise<void> {
    await this.#client.sendMessage(messageInputFrom(input))
  }

  async waitForInjected(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }> {
    this.#ensureEventSubscription()
    const result = await this.#client.sendMessage(messageInputFrom(input))
    const eventId = result.event_id

    if (this.#injectedEventIdSet.has(eventId)) {
      return { eventId, targets: result.targets }
    }

    const priorFailure = this.#failedDeliveries.get(eventId)
    if (priorFailure) {
      throw priorFailure
    }

    return await new Promise((resolve, reject) => {
      const timeoutMs = opts?.timeoutMs ?? 30_000
      const timeout = setTimeout(() => {
        this.#pendingInjected.delete(eventId)
        reject(new Error(`Timed out waiting for delivery_injected for ${eventId}`))
      }, timeoutMs)

      this.#pendingInjected.set(eventId, {
        targets: result.targets,
        timeout,
        resolve,
        reject,
      })
    })
  }

  onDeliveryFailed(listener: DeliveryFailedListener): () => void {
    this.#ensureEventSubscription()
    this.#deliveryFailedListeners.add(listener)
    return () => {
      this.#deliveryFailedListeners.delete(listener)
    }
  }

  onAgentExit(listener: AgentExitListener): () => void {
    this.#ensureEventSubscription()
    this.#agentExitListeners.add(listener)
    return () => {
      this.#agentExitListeners.delete(listener)
    }
  }

  #ensureEventSubscription(): void {
    if (this.#subscribed) {
      return
    }

    this.#subscribed = true
    this.#client.onEvent?.((event) => this.#handleBrokerEvent(event))
    this.#client.addListener?.('deliveryUpdate', (event) => this.#handleBrokerEvent(event))
    this.#client.addListener?.('agentExited', (agent) =>
      this.#emitAgentExit(agent.name, 'exited', {
        key: `agentExited:${JSON.stringify(agent)}`,
        hasStableId: false,
      }),
    )
    this.#client.connectEvents?.()
  }

  #handleBrokerEvent(event: BrokerEvent): void {
    if (event.kind === 'delivery_injected') {
      this.#resolveInjected(event.event_id)
      return
    }

    if (event.kind === 'delivery_failed') {
      this.#rejectInjected(event.event_id, event.reason)
      this.#emitDeliveryFailed(
        {
          to: event.name,
          msgId: event.event_id,
          reason: event.reason,
        },
        eventIdentity(event),
      )
      return
    }

    if (event.kind === 'message_delivery_failed') {
      if (event.event_id) {
        this.#rejectInjected(event.event_id, event.lastError)
      }
      this.#emitDeliveryFailed(
        {
          to: event.to,
          msgId: event.event_id,
          reason: event.lastError,
        },
        eventIdentity(event),
      )
      return
    }

    if (event.kind === 'agent_exit') {
      this.#emitAgentExit(event.name, event.reason, eventIdentity(event))
      return
    }

    if (event.kind === 'agent_exited') {
      this.#emitAgentExit(event.name, event.reason ?? exitReason(event), eventIdentity(event))
    }
  }

  #resolveInjected(eventId: string): void {
    rememberRecent(eventId, this.#injectedEventIds, this.#injectedEventIdSet)

    const pending = this.#pendingInjected.get(eventId)
    if (!pending) {
      return
    }

    clearTimeout(pending.timeout)
    this.#pendingInjected.delete(eventId)
    pending.resolve({ eventId, targets: pending.targets })
  }

  #rejectInjected(eventId: string, reason?: string): void {
    const error = new Error(reason ? `Delivery failed for ${eventId}: ${reason}` : `Delivery failed for ${eventId}`)
    this.#failedDeliveries.set(eventId, error)
    this.#failedDeliveryIds.push(eventId)
    if (this.#failedDeliveryIds.length > 500) {
      const oldest = this.#failedDeliveryIds.shift()
      if (oldest) {
        this.#failedDeliveries.delete(oldest)
      }
    }

    const pending = this.#pendingInjected.get(eventId)
    if (!pending) {
      return
    }

    clearTimeout(pending.timeout)
    this.#pendingInjected.delete(eventId)
    pending.reject(error)
  }

  #emitDeliveryFailed(info: { to: string; msgId?: string; reason?: string }, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) {
      return
    }

    for (const listener of this.#deliveryFailedListeners) {
      listener(info)
    }
  }

  #emitAgentExit(name: string, reason: string | undefined, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) {
      return
    }

    if (this.#rememberAgentExit(name)) {
      return
    }

    for (const listener of this.#agentExitListeners) {
      listener(name, reason)
    }
  }

  #rememberAgentExit(name: string): boolean {
    if (this.#exitedAgentNames.has(name)) {
      this.#suppressedDuplicateAgentExits += 1
      if (this.#suppressedDuplicateAgentExits <= 3 || this.#suppressedDuplicateAgentExits % 100 === 0) {
        this.#logger?.debug?.('[factory-sdk] suppressed duplicate agent exit', {
          count: this.#suppressedDuplicateAgentExits,
          name,
        })
      }
      return true
    }

    this.#exitedAgentNames.add(name)
    return false
  }

  #clearAgentExitLatch(name: string): void {
    this.#exitedAgentNames.delete(name)
  }

  #rememberEvent(identity: EventIdentity): boolean {
    const { key } = identity
    if (!identity.hasStableId) {
      this.#missingIdentityEvents += 1
      if (this.#missingIdentityEvents === 1) {
        this.#logger?.warn?.('[factory-sdk] broker event missing stable identity; deduping by full event content')
      }
    }

    if (this.#seenEventKeys.has(key)) {
      this.#suppressedDuplicateEvents += 1
      if (this.#suppressedDuplicateEvents <= 3 || this.#suppressedDuplicateEvents % 100 === 0) {
        this.#logger?.debug?.('[factory-sdk] suppressed duplicate broker event', {
          count: this.#suppressedDuplicateEvents,
          key,
        })
      }
      return true
    }

    this.#seenEventKeys.add(key)
    this.#seenEvents.push(key)
    if (this.#seenEvents.length > 500) {
      const oldest = this.#seenEvents.shift()
      if (oldest) {
        this.#seenEventKeys.delete(oldest)
      }
    }

    return false
  }
}

function assertSelfNode(node: SpawnInput['node']): void {
  if (node && node !== 'self') {
    throw new Error(`InternalFleetClient only supports node 'self' tonight; received ${node}`)
  }
}

function sessionRefFrom(handle: SpawnedHandleLike): string | undefined {
  return handle.session_ref ?? handle.sessionRef ?? handle.sessionId
}

function spawnResultFrom(handle: SpawnedHandleLike): SpawnResult {
  const result: SpawnResult = { name: handle.name }
  const sessionRef = sessionRefFrom(handle)
  if (sessionRef) result.sessionRef = sessionRef
  if (typeof handle.pid === 'number') result.pid = handle.pid
  return result
}

function messageInputFrom(input: SendInput): SendMessageInput {
  return {
    to: input.to,
    text: input.text,
    from: input.from,
    data: input.data,
  }
}

type EventIdentity = { key: string; hasStableId: boolean }

function eventIdentity(event: BrokerEvent): EventIdentity {
  const record = event as BrokerEvent & { event_id?: string; delivery_id?: string }
  const stable = record.event_id ?? record.delivery_id
  return {
    key: `${event.kind}:${stable ?? ''}:${JSON.stringify(event)}`,
    hasStableId: Boolean(stable),
  }
}

function rememberRecent(value: string, values: string[], set: Set<string>): void {
  if (set.has(value)) {
    return
  }

  set.add(value)
  values.push(value)
  if (values.length > 500) {
    const oldest = values.shift()
    if (oldest) {
      set.delete(oldest)
    }
  }
}

function exitReason(event: Extract<BrokerEvent, { kind: 'agent_exited' }>): string {
  if (event.signal) {
    return `signal:${event.signal}`
  }

  if (typeof event.code === 'number') {
    return `code:${event.code}`
  }

  return 'exited'
}
