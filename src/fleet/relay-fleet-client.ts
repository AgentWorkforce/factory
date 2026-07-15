import { AgentRelay } from '@agent-relay/sdk'

import { resolveRelayAgentToken, resolveRelayWorkspaceKey } from './relay-workspace-key'

import type { AgentMessage, Capability, FleetClient, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports'
import type {
  RelayActionInvocation,
  RelayActionInvocationAck,
  RelayMessage,
  RelayMessaging,
  RelayMessagingEvent,
  RelayNodeCapability,
} from '@agent-relay/sdk'

type AgentExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void

export interface TrackedAgent {
  invocationId?: string
  node?: string
  spawnedAtMs: number
  nodeOfflineSinceMs?: number
}

export interface RelayClientLike {
  messaging: RelayMessaging
}

export interface RelayClientFactoryOptions {
  workspaceKey?: string
  agentToken?: string
  baseUrl?: string
}

export interface RelayFleetClientOptions {
  /** Agent-scoped messaging surface. When provided, identity bootstrap is skipped. */
  messaging?: RelayMessaging
  workspaceKey?: string
  agentToken?: string
  /** Workspace agent identity the factory registers/rotates for itself. */
  agentName?: string
  /** Engine base URL override. Absent means the SDK default (cast.agentrelay.com). */
  baseUrl?: string
  /** Timeout for a spawn/release invocation to reach a terminal ack status. */
  spawnAckTimeoutMs?: number
  pollIntervalMs?: number
  /** Queue TTL passed to placement when no eligible node is currently live. */
  placementTtlMs?: number
  createRelay?: (options: RelayClientFactoryOptions) => RelayClientLike
  /**
   * Credential environment. Providing one makes resolution hermetic: only the
   * given env is consulted, never the on-disk cloud workspace store.
   */
  env?: NodeJS.ProcessEnv
  /** Cadence of the roster-reconciliation exit watcher. */
  exitWatchIntervalMs?: number
  /** How long a tracked agent's node must be dead before synthesizing an exit. */
  nodeOfflineGraceMs?: number
  /** How long a freshly spawned agent may be absent from the roster before it counts as exited. */
  registrationGraceMs?: number
  now?: () => number
  sleep?: (ms: number) => Promise<void>
  log?: (message: string) => void
}

const knownCapabilities = new Set<Capability>(['spawn:claude', 'spawn:codex', 'workflow:run'])
const openStatuses = new Set(['pending', 'dispatched', 'invoked'])
const terminalStatuses = new Set(['completed', 'failed', 'denied'])
const DEFAULT_AGENT_NAME = 'factory'
const DEFAULT_SPAWN_ACK_TIMEOUT_MS = 5 * 60_000
const DEFAULT_POLL_INTERVAL_MS = 1_000
const DEFAULT_EXIT_WATCH_INTERVAL_MS = 15_000
// 2× the engine's 45s node-liveness TTL so one missed heartbeat sweep cannot
// synthesize a false exit.
const DEFAULT_NODE_OFFLINE_GRACE_MS = 90_000
const DEFAULT_REGISTRATION_GRACE_MS = 60_000

export class RelayFleetClient implements FleetClient {
  readonly #options: RelayFleetClientOptions
  readonly #agentName: string
  readonly #spawnAckTimeoutMs: number
  readonly #pollIntervalMs: number
  readonly #exitWatchIntervalMs: number
  readonly #nodeOfflineGraceMs: number
  readonly #registrationGraceMs: number
  readonly #createRelay: (options: RelayClientFactoryOptions) => RelayClientLike
  readonly #now: () => number
  readonly #sleep: (ms: number) => Promise<void>
  readonly #log: (message: string) => void
  readonly #agentExitListeners = new Set<AgentExitListener>()
  readonly #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  readonly #agentMessageListeners = new Set<AgentMessageListener>()
  readonly #eventUnsubscribers: Array<() => void> = []
  readonly #tracked = new Map<string, TrackedAgent>()
  // Resolved lazily on first network use so constructing the client (and
  // therefore createFleet({ backend: 'relay' })) never throws merely because no
  // token is configured.
  #messaging: RelayMessaging | undefined
  #messagingReady: Promise<RelayMessaging> | undefined
  #eventsStarted = false
  #disposed = false
  #watchTimer: ReturnType<typeof setInterval> | undefined
  #reconciling: Promise<void> | undefined

  constructor(options: RelayFleetClientOptions = {}) {
    this.#options = options
    this.#agentName = options.agentName ?? DEFAULT_AGENT_NAME
    this.#spawnAckTimeoutMs = options.spawnAckTimeoutMs ?? DEFAULT_SPAWN_ACK_TIMEOUT_MS
    this.#pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
    this.#exitWatchIntervalMs = options.exitWatchIntervalMs ?? DEFAULT_EXIT_WATCH_INTERVAL_MS
    this.#nodeOfflineGraceMs = options.nodeOfflineGraceMs ?? DEFAULT_NODE_OFFLINE_GRACE_MS
    this.#registrationGraceMs = options.registrationGraceMs ?? DEFAULT_REGISTRATION_GRACE_MS
    this.#createRelay = options.createRelay ?? ((relayOptions) => new AgentRelay(relayOptions))
    this.#now = options.now ?? Date.now
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)))
    this.#log = options.log ?? (() => {})
    this.#messaging = options.messaging
  }

  /** Agents spawned through this client that have not exited or been released. */
  trackedAgents(): ReadonlyMap<string, TrackedAgent> {
    return this.#tracked
  }

  /** Re-adopt agents recorded in the in-flight registry after a restart. */
  hydrateTracked(agents: Array<{ name: string; invocationId?: string; node?: string }>): void {
    for (const agent of agents) {
      if (this.#tracked.has(agent.name)) continue
      // spawnedAtMs of 0 skips the registration grace: a hydrated agent was
      // registered long ago, so absence from the roster is a real exit.
      this.#tracked.set(agent.name, {
        invocationId: agent.invocationId,
        node: agent.node,
        spawnedAtMs: 0,
      })
    }
    this.#syncExitWatcher()
  }

  /**
   * Reconcile tracked agents against the engine roster, synthesizing exits for
   * agents that went offline (or whose node died) without a push signal. The
   * exit watcher runs this on an interval; callers may invoke it directly for
   * a deterministic sweep (startup recovery, tests).
   */
  reconcileTrackedAgents(): Promise<void> {
    this.#reconciling ??= this.#reconcileTracked().finally(() => {
      this.#reconciling = undefined
    })
    return this.#reconciling
  }

  async spawn(input: SpawnInput): Promise<SpawnResult> {
    const messaging = await this.#ensureMessaging()
    const ack = await messaging.placement.spawn({
      capability: input.capability,
      // 'self' from the orchestrator means "no placement preference": let the
      // engine pick the least-loaded eligible node.
      ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
      ...(input.repo ? { repo: input.repo } : {}),
      input: spawnActionInput(input),
      ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
      log: this.#log,
    })
    const invocation = await this.#awaitInvocation(ack.actionName || 'spawn', ack)
    const result = spawnResultFromInvocation(input.name, input.sessionRef, invocation)
    this.#track(result.name, ack)
    return result
  }

  async resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    const name = input.name ?? input.sessionRef
    return await this.spawn({
      name,
      capability: input.capability ?? 'spawn:codex',
      node: input.node,
      sessionRef: input.sessionRef,
    })
  }

  async release(name: string, reason?: string): Promise<void> {
    const messaging = await this.#ensureMessaging()
    try {
      const ack = await messaging.commands.invoke('release', {
        name,
        agent: name,
        ...(reason ? { reason } : {}),
      })
      await this.#awaitInvocation(ack.actionName || 'release', ack)
    } finally {
      this.#tracked.delete(name)
      this.#syncExitWatcher()
    }
  }

  async roster(): Promise<RosterEntry> {
    const messaging = await this.#ensureMessaging()
    const [agents, nodes] = await Promise.all([
      messaging.agents.list({ status: 'all' }),
      messaging.nodes.list(),
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

  // `from`/`data` are not representable on the agent-scoped messaging surface:
  // every send is authored by the factory's own agent identity.
  async sendMessage(input: SendInput): Promise<void> {
    await this.#send(input)
  }

  // The SDK exposes no sender-side delivery query yet, so a successful send —
  // a durable inbox row on the engine — is the injected signal. Upstream:
  // deliveries.forMessage(messageId) would make this authoritative.
  async waitForInjected(input: SendInput, _opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }> {
    const message = await this.#send(input)
    return { eventId: message.id, targets: [input.to] }
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
    this.#syncExitWatcher()
    return () => {
      this.#agentExitListeners.delete(listener)
      this.#syncExitWatcher()
    }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    if (this.#watchTimer) {
      clearInterval(this.#watchTimer)
      this.#watchTimer = undefined
    }
    for (const unsubscribe of this.#eventUnsubscribers.splice(0)) {
      unsubscribe()
    }
    this.#agentExitListeners.clear()
    this.#deliveryFailedListeners.clear()
    this.#agentMessageListeners.clear()
    this.#tracked.clear()
    if (this.#eventsStarted) {
      await this.#messaging?.events.disconnect().catch(() => {})
    }
  }

  async #send(input: SendInput): Promise<RelayMessage> {
    const messaging = await this.#ensureMessaging()
    try {
      if (input.to.startsWith('#')) {
        return await messaging.messages.send({ channel: input.to.slice(1), text: input.text })
      }
      return await messaging.messages.direct({
        to: input.to.startsWith('@') ? input.to.slice(1) : input.to,
        text: input.text,
      })
    } catch (error) {
      // Keep the orchestrator's registration-lag retry classification working:
      // a DM to an agent the engine has not registered yet is retryable.
      if (isUnknownRecipientError(error)) {
        throw new Error(`recipient unavailable: ${errorMessage(error)}`)
      }
      throw error
    }
  }

  #ensureMessaging(): Promise<RelayMessaging> {
    if (this.#messaging) return Promise.resolve(this.#messaging)
    this.#messagingReady ??= this.#bootstrapMessaging().catch((error) => {
      // Allow a later call to retry a failed bootstrap (transient network, etc).
      this.#messagingReady = undefined
      throw error
    })
    return this.#messagingReady
  }

  async #bootstrapMessaging(): Promise<RelayMessaging> {
    const env = this.#options.env
    const workspaceKey = resolveRelayWorkspaceKey({
      workspaceKey: this.#options.workspaceKey,
      ...(env ? { env, activeWorkspaceKey: () => undefined } : {}),
    })
    let agentToken = resolveRelayAgentToken({
      agentToken: this.#options.agentToken,
      ...(env ? { env } : {}),
    })
    if (!workspaceKey && !agentToken) {
      throw new Error('RelayFleetClient requires a workspace key (rk_live_…) or agent token (at_live_…); set RELAY_WORKSPACE_KEY or RELAY_AGENT_TOKEN')
    }
    if (!agentToken) {
      agentToken = await this.#registerFactoryAgent(workspaceKey as string)
    }
    const relay = this.#createRelay({
      ...(workspaceKey ? { workspaceKey } : {}),
      agentToken,
      ...(this.#options.baseUrl ? { baseUrl: this.#options.baseUrl } : {}),
    })
    this.#messaging = relay.messaging
    return this.#messaging
  }

  // Rotate-on-start is idempotent and leaves nothing secret on disk: the
  // factory adopts its standing workspace identity and mints a fresh token.
  async #registerFactoryAgent(workspaceKey: string): Promise<string> {
    const bootstrap = this.#createRelay({
      workspaceKey,
      ...(this.#options.baseUrl ? { baseUrl: this.#options.baseUrl } : {}),
    })
    const agents = bootstrap.messaging.agents
    const register = agents.registerOrRotate?.bind(agents) ?? agents.register.bind(agents)
    const registration = await register({ name: this.#agentName })
    return registration.token
  }

  async #awaitInvocation(actionName: string, ack: RelayActionInvocationAck): Promise<RelayActionInvocation> {
    const messaging = await this.#ensureMessaging()
    let status = ack.status ?? 'pending'
    let invocation: RelayActionInvocation | undefined
    const deadline = Date.now() + this.#spawnAckTimeoutMs

    while (!terminalStatuses.has(status)) {
      if (Date.now() > deadline) {
        throw new Error(`Timed out waiting for ${actionName} invocation ${ack.invocationId} to complete (last status: ${status})`)
      }
      if (!openStatuses.has(status)) {
        throw new Error(`Unexpected ${actionName} invocation ${ack.invocationId} status: ${status}`)
      }
      await this.#sleep(this.#pollIntervalMs)
      invocation = await messaging.commands.getInvocation(actionName, ack.invocationId)
      status = invocation.status || 'pending'
    }

    invocation ??= await messaging.commands.getInvocation(actionName, ack.invocationId)
    if (status === 'failed' || status === 'denied') {
      throw new Error(`${actionName} invocation ${ack.invocationId} ${status}${invocation.error ? `: ${invocation.error}` : ''}`)
    }
    return invocation
  }

  #track(name: string, ack: { invocationId: string; dispatchedNodeId?: string | null; placement?: { node?: string } }): void {
    this.#tracked.set(name, {
      invocationId: ack.invocationId,
      node: ack.placement?.node ?? ack.dispatchedNodeId ?? undefined,
      spawnedAtMs: this.#now(),
    })
    this.#syncExitWatcher()
  }

  #syncExitWatcher(): void {
    const shouldRun = !this.#disposed && this.#tracked.size > 0 && this.#agentExitListeners.size > 0
    if (shouldRun && !this.#watchTimer) {
      this.#watchTimer = setInterval(() => {
        void this.reconcileTrackedAgents().catch((error) => {
          this.#log(`relay fleet exit reconciliation failed: ${errorMessage(error)}`)
        })
      }, this.#exitWatchIntervalMs)
      this.#watchTimer.unref?.()
    } else if (!shouldRun && this.#watchTimer) {
      clearInterval(this.#watchTimer)
      this.#watchTimer = undefined
    }
  }

  async #reconcileTracked(): Promise<void> {
    if (this.#tracked.size === 0) return
    const messaging = await this.#ensureMessaging()
    const [agents, nodes] = await Promise.all([
      messaging.agents.list({ status: 'all' }),
      messaging.nodes.list(),
    ])
    const agentsByName = new Map(agents.map((agent) => [agent.name, agent]))
    const nodeLive = new Map(nodes.map((node) => [node.name, node.live ?? node.status === 'online']))
    const nowMs = this.#now()

    for (const [name, entry] of [...this.#tracked]) {
      const row = agentsByName.get(name)
      if (!row || row.status === 'offline') {
        // A just-spawned agent may not have registered with the engine yet;
        // absence only counts as an exit once the grace window has passed.
        if (nowMs - entry.spawnedAtMs < this.#registrationGraceMs) continue
        this.#emitExit(name, 'exited')
        continue
      }
      if (!entry.node) continue
      if (nodeLive.get(entry.node) === false || !nodeLive.has(entry.node)) {
        entry.nodeOfflineSinceMs ??= nowMs
        if (nowMs - entry.nodeOfflineSinceMs >= this.#nodeOfflineGraceMs) {
          this.#emitExit(name, 'node-offline')
        }
      } else {
        entry.nodeOfflineSinceMs = undefined
      }
    }
  }

  #emitExit(name: string, reason: string): void {
    this.#tracked.delete(name)
    this.#syncExitWatcher()
    for (const listener of this.#agentExitListeners) {
      listener(name, reason)
    }
  }

  #ensureEventSubscription(): void {
    if (this.#eventsStarted) return
    this.#eventsStarted = true
    void this.#subscribeEvents().catch((error) => {
      this.#eventsStarted = false
      this.#log(`relay fleet event subscription failed: ${errorMessage(error)}`)
    })
  }

  async #subscribeEvents(): Promise<void> {
    const messaging = await this.#ensureMessaging()
    if (this.#disposed) return
    messaging.events.connect()
    this.#eventUnsubscribers.push(messaging.events.on('any', (event) => this.#handleEvent(event)))
  }

  #handleEvent(event: RelayMessagingEvent): void {
    switch (event.type) {
      case 'dmReceived':
      case 'groupDmReceived':
        this.#emitAgentMessage(event.message, this.#agentName)
        break
      case 'messageCreated':
      case 'threadReply':
        this.#emitAgentMessage(event.message, event.channel)
        break
      case 'agentOffline':
        this.#handleAgentOffline(event.agent.name)
        break
      default:
        break
    }
  }

  #emitAgentMessage(message: RelayMessage, fallbackTarget: string): void {
    const from = message.from?.name
    if (!from || from === this.#agentName) return
    let target: string | undefined
    const messageTarget = message.target
    if (messageTarget?.kind === 'agent' && typeof messageTarget.agentName === 'string') {
      target = messageTarget.agentName
    } else if (messageTarget?.kind === 'channel' && typeof messageTarget.channelName === 'string') {
      target = messageTarget.channelName
    }
    const agentMessage: AgentMessage = {
      from,
      target: target ?? fallbackTarget,
      body: message.text,
      ...(message.threadId || message.parentId ? { threadId: message.threadId ?? message.parentId } : {}),
      ...(message.id ? { eventId: message.id } : {}),
    }
    for (const listener of this.#agentMessageListeners) {
      listener(agentMessage)
    }
  }

  // Presence offline is a push hint for exits of agents this client spawned.
  // The roster reconciliation watcher remains the authoritative exit detector.
  #handleAgentOffline(name: string): void {
    if (!this.#tracked.has(name)) return
    this.#emitExit(name, 'offline')
  }
}

// Placement injects capability/node/target_node/repo/cli on top of this
// payload; spawn_mode/exit_after_task request task-exit lifecycle from the
// broker on the placed node.
function spawnActionInput(input: SpawnInput): Record<string, unknown> {
  return definedRecord({
    name: input.name,
    agent: input.name,
    clone_path: input.clonePath,
    clonePath: input.clonePath,
    session_ref: input.sessionRef,
    invocationId: input.invocationId,
    task: input.task,
    workflow: input.workflow,
    inputs: input.inputs,
    model: input.model,
    cwd: input.cwd,
    channels: input.channel ? [input.channel] : undefined,
    restart_policy: input.restartPolicy,
    ...(input.capability.startsWith('spawn:') ? { spawn_mode: 'task_exit', exit_after_task: true } : {}),
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

function normalizeCapabilities(capabilities: RelayNodeCapability[] | undefined): Capability[] {
  const names = (capabilities ?? [])
    .map((capability) => capability.name)
    .filter((name): name is Capability => knownCapabilities.has(name as Capability))
  return [...new Set(names)]
}

function isUnknownRecipientError(error: unknown): boolean {
  const message = errorMessage(error)
  return /not[ _]found|unknown (agent|recipient)|no such (agent|recipient)|unregistered/i.test(message)
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
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
