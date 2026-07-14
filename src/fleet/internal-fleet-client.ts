import { HarnessDriverClient } from '@agent-relay/harness-driver'
import { accessSync, constants } from 'node:fs'
import { createRequire } from 'node:module'
import { readFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import type { BrokerEvent, ListAgent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import type { AgentMessage, AgentPidResolution, Capability, FleetClient, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports/fleet'
import type { Logger } from '../ports/system'
import { resolveRelayWorkspaceKey } from './relay-workspace-key'

const requireForResolve = createRequire(import.meta.url)

type SpawnedHandleLike = { name: string; sessionId?: string; session_ref?: string; sessionRef?: string; pid?: number }
type HarnessEventListener = (event: BrokerEvent) => void
type DriverAgentLike = { name: string; sessionId?: string; pid?: number }
type DriverDeliveryEventLike = BrokerEvent
export type AgentRelayMcpCommand = { command: string; args: string[] }

export interface HarnessDriverClientLike {
  readonly brokerPid?: number
  spawnPty(input: SpawnPtyInput): Promise<SpawnedHandleLike>
  release(name: string, reason?: string): Promise<{ name: string }>
  listAgents(): Promise<Array<Pick<ListAgent, 'name'> & { pid?: number }>>
  sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets?: string[] }>
  sendInput(name: string, data: string): Promise<unknown>
  connectEvents?(sinceSeq?: number): void
  disconnect?(): void
  // Shut the broker down. Only used when this client OWNS the broker (we spawned
  // it); reused brokers are only ever disconnected, never shut down.
  shutdown?(): Promise<void> | void
  onEvent?(listener: HarnessEventListener): () => void
  addListener?(event: 'agentExited', listener: (agent: DriverAgentLike) => void): () => void
  addListener?(event: 'deliveryUpdate', listener: (event: DriverDeliveryEventLike) => void): () => void
}

export interface InternalFleetClientOptions {
  client?: HarnessDriverClientLike
  // True when the injected client owns a broker we spawned, so dispose() shuts it
  // down instead of merely disconnecting (which would leave it running).
  ownsBroker?: boolean
  cwd?: string
  connectionPath?: string
  workspaceKey?: string
  resumeCapability?: Capability
  logger?: Logger
  resolveAgentRelayMcpCommand?: () => AgentRelayMcpCommand | undefined
}

type AgentExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void
type PendingInjectedWait = {
  input: SendInput
  eventIds: Set<string>
  currentEventId: string
  targets: string[]
  timeout: ReturnType<typeof setTimeout>
  resolve: (result: { eventId: string; targets: string[] }) => void
  reject: (error: Error) => void
  resendTimer?: ReturnType<typeof setTimeout>
  resendInFlight: boolean
  resendTriggered: boolean
  settled: boolean
}

export const capabilityCli: Record<Capability, string> = {
  'spawn:claude': 'claude',
  'spawn:codex': 'codex',
  'workflow:run': 'relayflows',
}

const selfNode: RosterEntry['nodes'][number] = {
  name: 'self',
  capabilities: ['spawn:claude', 'spawn:codex', 'workflow:run'],
  live: true,
}
const PID_RESOLVE_ATTEMPTS = 3
const PID_RESOLVE_BACKOFF_MS = 75
const READY_RESEND_DELAY_MS = 1_000

export class InternalFleetClient implements FleetClient {
  readonly #client: HarnessDriverClientLike
  readonly #ownsBroker: boolean
  readonly #cwd?: string
  readonly #connectionPath?: string
  readonly #workspaceKey?: string
  readonly #resumeCapability: Capability
  readonly #logger?: Logger
  readonly #resolveAgentRelayMcpCommand: () => AgentRelayMcpCommand | undefined
  readonly #agentExitListeners = new Set<AgentExitListener>()
  readonly #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  readonly #agentMessageListeners = new Set<AgentMessageListener>()
  readonly #eventUnsubscribers: Array<() => void> = []
  readonly #seenEvents: string[] = []
  readonly #seenEventKeys = new Set<string>()
  readonly #pendingInjected = new Map<string, PendingInjectedWait>()
  readonly #injectedEventIds: string[] = []
  readonly #injectedEventIdSet = new Set<string>()
  readonly #failedDeliveries = new Map<string, Error>()
  readonly #failedDeliveryIds: string[] = []
  readonly #exitedAgentNames = new Set<string>()
  readonly #readyAgentNames = new Set<string>()
  #suppressedDuplicateEvents = 0
  #suppressedDuplicateAgentExits = 0
  #missingIdentityEvents = 0
  #subscribed = false
  #disposed = false

  constructor(options: InternalFleetClientOptions = {}) {
    this.#cwd = options.cwd
    this.#connectionPath = options.connectionPath
    this.#workspaceKey = options.workspaceKey
    this.#resumeCapability = options.resumeCapability ?? 'spawn:codex'
    this.#logger = options.logger
    this.#resolveAgentRelayMcpCommand = options.resolveAgentRelayMcpCommand ?? resolveAgentRelayMcpCommand
    this.#client = options.client ?? HarnessDriverClient.connect({ cwd: options.cwd, connectionPath: options.connectionPath })
    this.#ownsBroker = options.ownsBroker ?? false
  }

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    assertSelfNode(input.node)
    // The broker has no delivery-target registration event. Subscribe before
    // spawn so worker_ready can act as a bounded re-send trigger if the child's
    // fallback MCP registration races the first confirmed task injection.
    this.#ensureEventSubscription()
    this.#readyAgentNames.delete(input.name)

    const spawnInput = this.#withAgentRelayMcpHarness({
      name: input.name,
      cli: capabilityCli[input.capability],
      channels: input.channel ? [input.channel] : undefined,
      task: input.task,
      model: input.model,
      cwd: input.cwd ?? this.#cwd,
      restartPolicy: input.restartPolicy,
      continueFrom: input.sessionRef,
      spawnMode: 'task_exit',
      exitAfterTask: true,
    })
    const handle = await this.#client.spawnPty(spawnInput)

    this.#clearAgentExitLatch(handle.name)

    return spawnResultFrom(handle)
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    assertSelfNode(input.node)

    const spawnInput = this.#withAgentRelayMcpHarness({
      name: input.name ?? input.sessionRef,
      // followups [fleet→W6]: W6 owns resume-vs-respawn and passes the per-agent capability.
      cli: capabilityCli[input.capability ?? this.#resumeCapability],
      cwd: this.#cwd,
      continueFrom: input.sessionRef,
    })
    const handle = await this.#client.spawnPty(spawnInput)

    this.#clearAgentExitLatch(handle.name)

    return { ...spawnResultFrom(handle), sessionRef: sessionRefFrom(handle) ?? input.sessionRef }
  }

  #withAgentRelayMcpHarness(input: SpawnPtyInput): SpawnPtyInput {
    if (input.harnessConfig || (input.cli !== 'codex' && input.cli !== 'claude')) {
      return input
    }

    const command = this.#resolveAgentRelayMcpCommand()
    if (!command) {
      this.#logger?.warn?.('[factory-sdk] agent-relay MCP command not found; spawning without MCP injection', {
        agent: input.name,
        cli: input.cli,
      })
      return input
    }

    return {
      ...input,
      harnessConfig: buildRelayMcpHarnessConfig(input, command, this.#workspaceKey),
    }
  }

  async release(name: string, reason?: string): Promise<void> {
    await this.#client.release(name, reason)
    this.#readyAgentNames.delete(name)
  }

  async roster(): Promise<RosterEntry> {
    const agents = await this.#client.listAgents()
    return {
      agents: agents.map((agent) => ({ name: agent.name })),
      nodes: [selfNode],
    }
  }

  async protectedPids(): Promise<number[]> {
    const pids = new Set<number>()
    if (Number.isInteger(this.#client.brokerPid) && this.#client.brokerPid! > 0) {
      pids.add(this.#client.brokerPid!)
    }
    const connectionPid = await this.#connectionFilePid()
    if (connectionPid) {
      pids.add(connectionPid)
    }
    return [...pids].sort((a, b) => a - b)
  }

  async resolveAgentPid(name: string): Promise<AgentPidResolution> {
    try {
      let sawAgent = false
      for (let attempt = 1; attempt <= PID_RESOLVE_ATTEMPTS; attempt += 1) {
        const agent = (await this.#client.listAgents()).find((candidate) => candidate.name === name)
        if (agent) {
          sawAgent = true
        }
        if (typeof agent?.pid === 'number') {
          return { status: 'found', pid: agent.pid }
        }
        if (attempt < PID_RESOLVE_ATTEMPTS) {
          await sleep(PID_RESOLVE_BACKOFF_MS)
        }
      }
      return sawAgent ? { status: 'unresolved' } : { status: 'missing' }
    } catch (error) {
      this.#logger?.warn?.('[factory-sdk] unable to resolve spawned agent pid from roster', error)
      return { status: 'unresolved' }
    }
  }

  async #connectionFilePid(): Promise<number | undefined> {
    const path = this.#connectionPath ?? connectionPathForCwd(this.#cwd)
    if (!path) return undefined
    try {
      const parsed = JSON.parse(await readFile(path, 'utf8')) as { pid?: unknown }
      const pid = parsed.pid
      return typeof pid === 'number' && Number.isInteger(pid) && pid > 0 ? pid : undefined
    } catch {
      return undefined
    }
  }

  async sendMessage(input: SendInput): Promise<void> {
    await this.#client.sendMessage(messageInputFrom(input))
  }

  async waitForInjected(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }> {
    this.#ensureEventSubscription()
    const targetWasReady = this.#readyAgentNames.has(input.to)
    const result = await this.#client.sendMessage(messageInputFrom(input))
    const eventId = result.event_id
    const targets = result.targets ?? []

    if (this.#injectedEventIdSet.has(eventId)) {
      return { eventId, targets }
    }

    const priorFailure = this.#failedDeliveries.get(eventId)
    if (priorFailure) {
      throw priorFailure
    }

    return await new Promise((resolve, reject) => {
      const timeoutMs = opts?.timeoutMs ?? 30_000
      const timeout = setTimeout(() => {
        this.#rejectPendingInjected(
          pending,
          new Error(`Timed out waiting for delivery_injected for ${pending.currentEventId}`),
        )
      }, timeoutMs)

      const pending: PendingInjectedWait = {
        input,
        eventIds: new Set([eventId]),
        currentEventId: eventId,
        targets,
        timeout,
        resolve,
        reject,
        resendInFlight: false,
        resendTriggered: false,
        settled: false,
      }
      this.#pendingInjected.set(eventId, pending)

      // worker_ready may arrive while sendMessage is in flight, before the
      // pending waiter is installed. Re-check after installation to close that
      // gap, while preserving the one-send path for workers already ready.
      if (!targetWasReady && this.#readyAgentNames.has(input.to)) {
        this.#triggerReadyResend(pending)
      } else if (targetWasReady) {
        pending.resendTimer = setTimeout(() => this.#triggerReadyResend(pending), READY_RESEND_DELAY_MS)
      }

      // delivery_injected can likewise race the sendMessage response and the
      // pending-map write. Resolve from the recent-event latch when it does.
      if (this.#injectedEventIdSet.has(eventId)) {
        this.#resolvePendingInjected(pending, eventId)
      }
    })
  }

  async sendInput(name: string, data: string): Promise<void> {
    await this.#client.sendInput(name, data)
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
    if (this.#disposed) {
      return
    }
    this.#disposed = true

    const pending = [...new Set(this.#pendingInjected.values())]
    this.#pendingInjected.clear()
    for (const entry of pending) {
      clearTimeout(entry.timeout)
      if (entry.resendTimer) clearTimeout(entry.resendTimer)
      entry.settled = true
      entry.reject(new Error('InternalFleetClient disposed before delivery was confirmed'))
    }

    for (const unsubscribe of this.#eventUnsubscribers.splice(0)) {
      unsubscribe()
    }
    this.#agentExitListeners.clear()
    this.#deliveryFailedListeners.clear()
    this.#agentMessageListeners.clear()
    this.#failedDeliveries.clear()
    this.#failedDeliveryIds.length = 0
    this.#readyAgentNames.clear()
    // If we started the broker, shut it down so the process can exit cleanly
    // (a spawned broker's owner-lease renewal otherwise keeps the event loop
    // alive). A reused broker is only disconnected — never shut down — so we
    // never take down the operator's running broker.
    if (this.#ownsBroker && this.#client.shutdown) {
      try {
        await this.#client.shutdown()
      } catch (error) {
        this.#logger?.warn?.('[factory-sdk] failed to shut down the spawned relay broker on dispose', error)
        this.#client.disconnect?.()
      }
    } else {
      this.#client.disconnect?.()
    }
    this.#subscribed = false
  }

  #ensureEventSubscription(): void {
    if (this.#subscribed) {
      return
    }

    this.#subscribed = true
    const offEvent = this.#client.onEvent?.((event) => this.#handleBrokerEvent(event))
    if (offEvent) this.#eventUnsubscribers.push(offEvent)
    const offDeliveryUpdate = this.#client.addListener?.('deliveryUpdate', (event) => this.#handleBrokerEvent(event))
    if (offDeliveryUpdate) this.#eventUnsubscribers.push(offDeliveryUpdate)
    const offAgentExited = this.#client.addListener?.('agentExited', (agent) => {
      this.#readyAgentNames.delete(agent.name)
      this.#emitAgentExit(agent.name, 'exited', {
        key: `agentExited:${JSON.stringify(agent)}`,
        hasStableId: false,
      })
    })
    if (offAgentExited) this.#eventUnsubscribers.push(offAgentExited)
    this.#client.connectEvents?.()
  }

  #handleBrokerEvent(event: BrokerEvent): void {
    if (event.kind === 'worker_ready') {
      this.#readyAgentNames.add(event.name)
      for (const pending of new Set(this.#pendingInjected.values())) {
        if (pending.input.to === event.name) {
          this.#triggerReadyResend(pending)
        }
      }
      return
    }

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

    if (event.kind === 'relay_inbound') {
      this.#emitAgentMessage({
        from: event.from,
        target: event.target,
        body: event.body,
        threadId: event.thread_id,
        eventId: event.event_id,
      }, eventIdentity(event))
      return
    }

    if (event.kind === 'agent_exit') {
      this.#readyAgentNames.delete(event.name)
      this.#emitAgentExit(event.name, event.reason, eventIdentity(event))
      return
    }

    if (event.kind === 'agent_exited') {
      this.#readyAgentNames.delete(event.name)
      this.#emitAgentExit(event.name, event.reason ?? exitReason(event), eventIdentity(event))
    }
  }

  #resolveInjected(eventId: string): void {
    rememberRecent(eventId, this.#injectedEventIds, this.#injectedEventIdSet)

    const pending = this.#pendingInjected.get(eventId)
    if (!pending) {
      return
    }

    this.#resolvePendingInjected(pending, eventId)
  }

  #resolvePendingInjected(pending: PendingInjectedWait, eventId: string): void {
    if (pending.settled) return
    pending.settled = true
    clearTimeout(pending.timeout)
    if (pending.resendTimer) clearTimeout(pending.resendTimer)
    for (const pendingEventId of pending.eventIds) {
      this.#pendingInjected.delete(pendingEventId)
    }
    pending.resolve({ eventId, targets: pending.targets })
  }

  #rejectPendingInjected(pending: PendingInjectedWait, error: Error): void {
    if (pending.settled) return
    pending.settled = true
    clearTimeout(pending.timeout)
    if (pending.resendTimer) clearTimeout(pending.resendTimer)
    for (const pendingEventId of pending.eventIds) {
      this.#pendingInjected.delete(pendingEventId)
    }
    pending.reject(error)
  }

  #triggerReadyResend(pending: PendingInjectedWait): void {
    if (pending.settled || pending.resendTriggered) return
    if (pending.resendTimer) clearTimeout(pending.resendTimer)
    pending.resendTriggered = true
    pending.resendInFlight = true
    void this.#resendPendingInjected(pending)
  }

  async #resendPendingInjected(pending: PendingInjectedWait): Promise<void> {
    try {
      const result = await this.#client.sendMessage(messageInputFrom(pending.input))
      pending.resendInFlight = false
      if (pending.settled) return

      const eventId = result.event_id
      pending.currentEventId = eventId
      pending.targets = result.targets ?? []
      pending.eventIds.add(eventId)
      this.#pendingInjected.set(eventId, pending)

      if (this.#injectedEventIdSet.has(eventId)) {
        this.#resolvePendingInjected(pending, eventId)
        return
      }
      const priorFailure = this.#failedDeliveries.get(eventId)
      if (priorFailure) {
        this.#failPendingInjectedEvent(pending, eventId, priorFailure)
      }
    } catch (error) {
      pending.resendInFlight = false
      if (pending.eventIds.size === 0) {
        this.#rejectPendingInjected(pending, error instanceof Error ? error : new Error(String(error)))
      }
    }
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

    this.#failPendingInjectedEvent(pending, eventId, error)
  }

  #failPendingInjectedEvent(pending: PendingInjectedWait, eventId: string, error: Error): void {
    this.#pendingInjected.delete(eventId)
    pending.eventIds.delete(eventId)
    // A readiness-triggered re-send aliases the old and new broker event ids
    // into one logical waiter. A late failure for the superseded id must not
    // reject a newer send that can still be acknowledged.
    if (pending.eventIds.size === 0 && !pending.resendInFlight) {
      this.#rejectPendingInjected(pending, error)
    }
  }

  #emitDeliveryFailed(info: { to: string; msgId?: string; reason?: string }, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) {
      return
    }

    for (const listener of this.#deliveryFailedListeners) {
      listener(info)
    }
  }

  #emitAgentMessage(message: AgentMessage, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) {
      return
    }

    for (const listener of this.#agentMessageListeners) {
      listener(message)
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

function connectionPathForCwd(cwd: string | undefined): string | undefined {
  const stateDir = process.env.AGENT_RELAY_STATE_DIR
  if (stateDir) return join(stateDir, 'connection.json')
  return cwd ? join(cwd, '.agentworkforce', 'relay', 'connection.json') : undefined
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

function assertSelfNode(node: SpawnInput['node']): void {
  if (node && node !== 'self') {
    throw new Error(`InternalFleetClient only supports node 'self' tonight; received ${node}`)
  }
}

function sessionRefFrom(handle: SpawnedHandleLike): string | undefined {
  return handle.session_ref ?? handle.sessionRef ?? handle.sessionId
}

function spawnResultFrom(handle: SpawnedHandleLike, resolvedPid = handle.pid): SpawnResult {
  const result: SpawnResult = { name: handle.name }
  const sessionRef = sessionRefFrom(handle)
  if (sessionRef) result.sessionRef = sessionRef
  if (typeof resolvedPid === 'number') result.pid = resolvedPid
  return result
}

export function resolveAgentRelayMcpCommand(): AgentRelayMcpCommand | undefined {
  try {
    const packageJsonPath = requireForResolve.resolve('agent-relay/package.json')
    const cliPath = join(dirname(packageJsonPath), 'dist', 'cli', 'index.js')
    accessSync(cliPath, constants.R_OK)
    return { command: process.execPath, args: [cliPath, 'mcp'] }
  } catch {
    return undefined
  }
}

export function buildRelayMcpHarnessConfig(
  input: SpawnPtyInput,
  command: AgentRelayMcpCommand,
  workspaceKey?: string,
): NonNullable<SpawnPtyInput['harnessConfig']> {
  const relayEnv = relayMcpEnv(input.name, input.agentToken, workspaceKey)
  return {
    runtime: 'pty',
    command: input.cli,
    args: input.cli === 'claude'
      ? claudeHarnessArgs(input, command, relayEnv)
      : codexHarnessArgs(input, command, relayEnv),
    ...(input.cwd ? { cwd: input.cwd } : {}),
    env: relayEnv,
    metadata: {
      factoryRelayMcp: true,
      relayMcpCommand: command.command,
      relayMcpArgs: command.args,
    },
  }
}

function claudeHarnessArgs(input: SpawnPtyInput, command: AgentRelayMcpCommand, relayEnv: Record<string, string>): string[] {
  return [
    ...modelArgs(input.model),
    '--mcp-config',
    JSON.stringify({
      mcpServers: {
        'agent-relay': stdioMcpServer(command, relayEnv),
      },
    }),
    '--strict-mcp-config',
    ...(input.args ?? []),
    ...taskArgs(input.task),
  ]
}

function codexHarnessArgs(input: SpawnPtyInput, command: AgentRelayMcpCommand, relayEnv: Record<string, string>): string[] {
  const prefix = 'mcp_servers.agent-relay'
  return [
    ...modelArgs(stripProviderPrefix(input.model)),
    '--config',
    `${prefix}.command=${tomlString(command.command)}`,
    '--config',
    `${prefix}.args=${tomlArray(command.args)}`,
    '--config',
    `${prefix}.env=${tomlInlineTable(relayEnv)}`,
    ...(input.args ?? []),
    ...taskArgs(input.task),
  ]
}

function stdioMcpServer(command: AgentRelayMcpCommand, relayEnv: Record<string, string>): Record<string, unknown> {
  return {
    type: 'stdio',
    command: command.command,
    args: command.args,
    env: relayEnv,
  }
}

function relayMcpEnv(agentName: string, agentToken?: string, workspaceKey?: string): Record<string, string> {
  const env: Record<string, string> = {
    RELAY_AGENT_NAME: agentName,
    RELAY_AGENT_TYPE: 'agent',
    RELAY_STRICT_AGENT_NAME: '1',
  }
  const resolvedWorkspaceKey = resolveRelayWorkspaceKey({ workspaceKey })
  if (resolvedWorkspaceKey) {
    env.RELAY_WORKSPACE_KEY = resolvedWorkspaceKey
    env.RELAY_API_KEY = resolvedWorkspaceKey
  } else {
    // No workspace key in the daemon env: the spawned agent's agent-relay MCP
    // will boot WITHOUT credentials, so it joins a bare relaycast workspace and
    // can't reach .integrations (no GitHub reads, no Slack/Linear writebacks).
    // That silently breaks the whole dispatch — make it loud so it's diagnosable.
    console.warn(
      `[factory] spawning ${agentName} with NO relay workspace key ` +
        '(set RELAY_WORKSPACE_KEY / AGENT_RELAY_WORKSPACE_KEY / RELAY_API_KEY); ' +
        'its agent-relay MCP cannot reach .integrations — writebacks and GitHub reads will fail.',
    )
  }
  const baseUrl = nonEmpty(process.env.RELAY_BASE_URL)
  if (baseUrl) env.RELAY_BASE_URL = baseUrl
  const defaultWorkspace = nonEmpty(process.env.RELAY_DEFAULT_WORKSPACE)
  if (defaultWorkspace) env.RELAY_DEFAULT_WORKSPACE = defaultWorkspace
  const workspacesJson = nonEmpty(process.env.RELAY_WORKSPACES_JSON)
  if (workspacesJson) env.RELAY_WORKSPACES_JSON = workspacesJson
  const relayAgentToken = nonEmpty(agentToken)
  if (relayAgentToken) env.RELAY_AGENT_TOKEN = relayAgentToken
  return env
}

function modelArgs(model: string | undefined): string[] {
  return model ? ['--model', model] : []
}

function taskArgs(task: string | undefined): string[] {
  return task ? [task] : []
}

function stripProviderPrefix(model: string | undefined): string | undefined {
  if (!model) return undefined
  const idx = model.indexOf('/')
  return idx >= 0 ? model.slice(idx + 1) : model
}

function tomlString(value: string): string {
  return JSON.stringify(value)
}

function tomlArray(values: string[]): string {
  return `[${values.map(tomlString).join(', ')}]`
}

function tomlInlineTable(values: Record<string, string>): string {
  return `{ ${Object.entries(values)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${tomlString(key)} = ${tomlString(value)}`)
    .join(', ')} }`
}

function nonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed || undefined
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
