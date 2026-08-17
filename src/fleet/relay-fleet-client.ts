import { AgentRelay } from '@agent-relay/sdk'

import { resolveRelayAgentToken, resolveRelayWorkspaceKey } from './relay-workspace-key'

import type { AgentLifecycleSignal, AgentMessage, AgentUsage, Capability, FleetClient, NodeCapability, PreviewReference, PreviewStartInput, PreviewSweepInput, PreviewSweepResult, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports/fleet'
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
type AgentLifecycleSignalListener = (signal: AgentLifecycleSignal) => void | Promise<void>
type AgentUsageListener = (usage: AgentUsage) => void | Promise<void>

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
  /** Stable workspace action used for durable agent lifecycle reports. */
  lifecycleActionName?: string
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

const knownCapabilities = new Set<NodeCapability>(['spawn:claude', 'spawn:codex', 'workflow:run', 'preview:tailscale-serve'])
const openStatuses = new Set(['pending', 'dispatched', 'invoked'])
const terminalStatuses = new Set(['completed', 'failed', 'denied'])
const DEFAULT_AGENT_NAME = 'factory'
export const DEFAULT_LIFECYCLE_ACTION_NAME = 'factory.lifecycle'
const DEFAULT_SPAWN_ACK_TIMEOUT_MS = 5 * 60_000
const DEFAULT_POLL_INTERVAL_MS = 1_000
const DEFAULT_EXIT_WATCH_INTERVAL_MS = 15_000
// 2× the engine's 45s node-liveness TTL so one missed heartbeat sweep cannot
// synthesize a false exit.
const DEFAULT_NODE_OFFLINE_GRACE_MS = 90_000
const DEFAULT_REGISTRATION_GRACE_MS = 60_000

export class RelayFleetClient implements FleetClient {
  readonly placementLocality = 'remote' as const
  readonly durableOwnership = true
  readonly lifecycleActionName: string
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
  readonly #agentLifecycleSignalListeners = new Set<AgentLifecycleSignalListener>()
  readonly #agentUsageListeners = new Set<AgentUsageListener>()
  readonly #lifecycleInvocationsInFlight = new Set<string>()
  readonly #eventUnsubscribers: Array<() => void> = []
  readonly #tracked = new Map<string, TrackedAgent>()
  // Resolved lazily on first network use so constructing the client (and
  // therefore createFleet({ backend: 'relay' })) never throws merely because no
  // token is configured.
  #messaging: RelayMessaging | undefined
  #messagingReady: Promise<RelayMessaging> | undefined
  #lifecycleActionReady: Promise<void> | undefined
  #authenticatedAgentName: string
  #eventsStarted = false
  #disposed = false
  #watchTimer: ReturnType<typeof setInterval> | undefined
  #reconciling: Promise<void> | undefined

  constructor(options: RelayFleetClientOptions = {}) {
    this.#options = options
    this.#agentName = options.agentName ?? DEFAULT_AGENT_NAME
    this.#authenticatedAgentName = this.#agentName
    this.lifecycleActionName = options.lifecycleActionName ?? DEFAULT_LIFECYCLE_ACTION_NAME
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
    if (input.capability.startsWith('spawn:')) {
      await this.#ensureLifecycleAction(messaging)
      // A transient startup registration failure may have torn down the first
      // subscription attempt. Re-arm it after the required action is durable so
      // the invocation cannot be accepted without a live Factory consumer.
      this.#ensureEventSubscription()
    }
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
    const result = spawnResultFromInvocation(input.name, input.sessionRef, invocation, ack)
    try {
      assertNamedRemotePlacement(result, ack.placement?.node)
    } catch (error) {
      // A completed placement invocation has already launched the worker. If
      // Relay cannot prove that it ran on a named remote node, tear that worker
      // down before refusing the result so a rejected spawn cannot keep acting
      // outside Factory's lifecycle tracking.
      try {
        await this.release(result.name, 'unverified-placement')
      } catch (releaseError) {
        this.#log(
          `Failed to release ${result.name} after unverified Relay placement: ${errorMessage(releaseError)}`,
        )
      }
      throw error
    }
    this.#track(result.name, ack)
    return result
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
    const name = input.name ?? input.sessionRef
    return await this.spawn({
      name,
      capability: input.capability ?? 'spawn:codex',
      identityKey: input.identityKey,
      node: input.node,
      repo: input.repo,
      clonePath: input.clonePath,
      sessionRef: input.sessionRef,
      task: input.task,
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

  async createPreview(input: PreviewStartInput): Promise<PreviewReference> {
    const messaging = await this.#ensureMessaging()
    const ack = await messaging.placement.spawn({
      capability: 'preview:tailscale-serve',
      ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
      repo: input.repo,
      input: {
        operation: 'start',
        namespace: input.namespace,
        owner: input.owner,
        issueKey: input.issueKey,
        service: input.service,
        repo: input.repo,
        targetPort: input.targetPort,
        ...(input.preferredHttpsPort !== undefined ? { preferredHttpsPort: input.preferredHttpsPort } : {}),
        startCommand: input.startCommand,
        checkoutPath: input.checkoutPath,
      },
      ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
      log: this.#log,
    })
    const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack)
    return previewReferenceFromInvocation(invocation, ack.placement?.node ?? ack.dispatchedNodeId)
  }

  async removePreview(preview: PreviewReference): Promise<boolean> {
    const messaging = await this.#ensureMessaging()
    const ack = await messaging.placement.spawn({
      capability: 'preview:tailscale-serve',
      ...(preview.node ? { node: preview.node } : {}),
      input: { operation: 'remove', preview },
      ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
      log: this.#log,
    })
    const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack)
    return asRecord(invocation.output)?.removed === true
  }

  async reapPreviews(input: PreviewSweepInput): Promise<PreviewSweepResult> {
    const messaging = await this.#ensureMessaging()
    const nodes = (await this.roster()).nodes.filter((node) =>
      node.live && node.capabilities.includes('preview:tailscale-serve'),
    )
    const reports = await Promise.all(nodes.map(async (node): Promise<PreviewSweepResult> => {
      try {
        const ack = await messaging.placement.spawn({
          capability: 'preview:tailscale-serve',
          node: node.name,
          input: {
            operation: 'sweep',
            namespace: input.namespace,
            activeOwners: input.activeOwners,
            ...(input.activePreviewIds ? { activePreviewIds: input.activePreviewIds } : {}),
          },
          ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
          log: this.#log,
        })
        const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack)
        const output = asRecord(invocation.output)
        return {
          reaped: Array.isArray(output?.reaped)
            ? output.reaped.map((value) => previewReference(value, node.name)).filter((value): value is PreviewReference => Boolean(value))
            : [],
          skipped: Array.isArray(output?.skipped)
            ? output.skipped.map((value) => {
                const record = asRecord(value)
                return {
                  ...(readString(record, 'id') ? { id: readString(record, 'id') } : {}),
                  reason: readString(record, 'reason') ?? 'unknown provider result',
                  node: node.name,
                }
              })
            : [],
        }
      } catch (error) {
        return { reaped: [], skipped: [{ reason: errorMessage(error), node: node.name }] }
      }
    }))
    return {
      reaped: reports.flatMap((report) => report.reaped),
      skipped: reports.flatMap((report) => report.skipped),
    }
  }

  markAgentTerminal(name: string, _reason?: string): void {
    this.#tracked.delete(name)
    this.#syncExitWatcher()
  }

  async roster(): Promise<RosterEntry> {
    const messaging = await this.#ensureMessaging()
    const [presence, agents, nodes] = await Promise.all([
      // Presence is the SDK's canonical liveness surface. Agent-scoped list
      // responses can omit status and normalize those rows to `unknown`, which
      // made dead deterministic names look live and suppressed recovery.
      messaging.agents.presence(),
      // List data is supplemental metadata only; its status is never used to
      // decide liveness.
      messaging.agents.list({ status: 'all' }),
      messaging.nodes.list(),
    ])
    const agentsByName = new Map(agents.map((agent) => [agent.name, agent]))
    return {
      agents: presence
        .filter((agent) => agent.status === 'online')
        .map((agent) => {
          const record = asRecord(agentsByName.get(agent.agentName))
          const node = readString(record, 'node', 'node_id', 'nodeId')
          return { name: agent.agentName, ...(node ? { node } : {}) }
        }),
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

  onAgentLifecycleSignal(listener: AgentLifecycleSignalListener): () => void {
    this.#ensureEventSubscription()
    this.#agentLifecycleSignalListeners.add(listener)
    return () => {
      this.#agentLifecycleSignalListeners.delete(listener)
    }
  }

  onAgentUsage(listener: AgentUsageListener): () => void {
    this.#ensureEventSubscription()
    this.#agentUsageListeners.add(listener)
    return () => {
      this.#agentUsageListeners.delete(listener)
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
    this.#agentLifecycleSignalListeners.clear()
    this.#agentUsageListeners.clear()
    this.#lifecycleInvocationsInFlight.clear()
    this.#tracked.clear()
    if (this.#eventsStarted) {
      await this.#messaging?.events.disconnect().catch(() => {})
    }
  }

  async #send(input: SendInput): Promise<RelayMessage> {
    const messaging = await this.#ensureMessaging()
    try {
      if (input.to.startsWith('#')) {
        return await messaging.messages.send({
          channel: input.to.slice(1),
          text: input.text,
          ...(input.mode ? { mode: input.mode } : {}),
        })
      }
      return await messaging.messages.direct({
        to: input.to.startsWith('@') ? input.to.slice(1) : input.to,
        text: input.text,
        ...(input.mode ? { mode: input.mode } : {}),
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
    const [presence, nodes] = await Promise.all([
      messaging.agents.presence(),
      messaging.nodes.list(),
    ])
    const onlineAgentNames = new Set(
      presence.filter((agent) => agent.status === 'online').map((agent) => agent.agentName),
    )
    const nodeLive = new Map(nodes.map((node) => [node.name, node.live ?? node.status === 'online']))
    const nowMs = this.#now()

    for (const [name, entry] of [...this.#tracked]) {
      if (!onlineAgentNames.has(name)) {
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
    await this.#ensureLifecycleAction(messaging)
    if (this.#disposed) return
    messaging.events.connect()
    this.#eventUnsubscribers.push(messaging.events.on('any', (event) => this.#handleEvent(event)))
  }

  #handleEvent(event: RelayMessagingEvent): void {
    switch (event.type) {
      case 'dmReceived':
      case 'groupDmReceived':
        this.#emitAgentMessage(event.message, this.#authenticatedAgentName)
        break
      case 'messageCreated':
      case 'threadReply':
        this.#emitAgentMessage(event.message, event.channel)
        break
      case 'agentOffline':
        this.#handleAgentOffline(event.agent.name)
        break
      case 'actionInvoked':
        if (event.actionName === this.lifecycleActionName) {
          void this.#handleLifecycleInvocation(event)
        }
        break
      default:
        break
    }
  }

  #emitAgentMessage(message: RelayMessage, fallbackTarget: string): void {
    const from = message.from?.name
    if (!from || from === this.#authenticatedAgentName || from === this.#agentName) return
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

  async #ensureLifecycleAction(existingMessaging?: RelayMessaging): Promise<void> {
    if (this.#lifecycleActionReady) return this.#lifecycleActionReady
    this.#lifecycleActionReady = (async () => {
      const messaging = existingMessaging ?? await this.#ensureMessaging()
      if (!messaging.commands.available()) {
        throw new Error('Relay lifecycle signaling requires the durable action surface')
      }
      const identity = await messaging.agents.me()
      if (!identity.name?.trim()) {
        throw new Error('Relay lifecycle signaling could not resolve the authenticated handler identity')
      }
      this.#authenticatedAgentName = identity.name
      await messaging.commands.register({
        command: this.lifecycleActionName,
        description: 'Report a Factory task lifecycle transition without relying on a named DM recipient.',
        handlerAgent: identity.name,
        inputSchema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            kind: { type: 'string', enum: ['completed', 'ready', 'blocked'] },
            issueKey: { type: 'string' },
            role: { type: 'string', enum: ['implementer', 'reviewer', 'babysitter', 'workflow'] },
            question: { type: 'string' },
            usage: {
              type: 'object',
              additionalProperties: false,
              properties: {
                model: { type: 'string' },
                inputTokens: { type: 'integer', minimum: 0 },
                outputTokens: { type: 'integer', minimum: 0 },
              },
            },
          },
          required: ['kind', 'issueKey', 'role'],
        },
        outputSchema: {
          type: 'object',
          properties: { accepted: { type: 'boolean' } },
          required: ['accepted'],
        },
      })
    })().catch((error) => {
      this.#lifecycleActionReady = undefined
      throw error
    })
    return this.#lifecycleActionReady
  }

  async #handleLifecycleInvocation(
    event: Extract<RelayMessagingEvent, { type: 'actionInvoked' }>,
  ): Promise<void> {
    if (this.#lifecycleInvocationsInFlight.has(event.invocationId)) return
    this.#lifecycleInvocationsInFlight.add(event.invocationId)
    let messaging: RelayMessaging | undefined
    try {
      messaging = await this.#ensureMessaging()
      const invocation = await messaging.commands.getInvocation(this.lifecycleActionName, event.invocationId)
      if (terminalStatuses.has(invocation.status)) return
      const signal = lifecycleSignalFromInvocation(
        invocation.input,
        invocation.callerName ?? event.callerName,
        event.invocationId,
      )
      if (!signal) {
        throw new Error(`Invalid ${this.lifecycleActionName} input for invocation ${event.invocationId}`)
      }
      const usage = lifecycleUsageFromInvocation(invocation.input, signal.name)
      if (usage) await this.#emitAgentUsage(usage)
      if (this.#agentLifecycleSignalListeners.size === 0) {
        throw new Error('Factory lifecycle handler is not active')
      }
      for (const listener of this.#agentLifecycleSignalListeners) {
        await listener(signal)
      }
      await messaging.commands.completeInvocation(this.lifecycleActionName, event.invocationId, {
        output: { accepted: true },
      })
    } catch (error) {
      if (messaging) {
        await messaging.commands.completeInvocation(this.lifecycleActionName, event.invocationId, {
          error: errorMessage(error),
        }).catch((completionError) => {
          this.#log(`relay lifecycle invocation ${event.invocationId} failed without terminal ack: ${errorMessage(completionError)}`)
        })
      }
      this.#log(`relay lifecycle invocation ${event.invocationId} rejected: ${errorMessage(error)}`)
    } finally {
      this.#lifecycleInvocationsInFlight.delete(event.invocationId)
    }
  }

  async #emitAgentUsage(usage: AgentUsage): Promise<void> {
    const results = await Promise.allSettled(
      [...this.#agentUsageListeners].map(async (listener) => listener({ ...usage })),
    )
    if (results.some((result) => result.status === 'rejected')) {
      this.#log(`relay usage listener rejected an update for ${usage.name}`)
    }
  }
}

// Placement injects capability/node/target_node/repo/cli on top of this
// payload; spawn_mode/exit_after_task request task-exit lifecycle from the
// broker on the placed node.
function spawnActionInput(input: SpawnInput): Record<string, unknown> {
  return definedRecord({
    name: input.name,
    agent: input.name,
    identity_key: input.identityKey,
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

function spawnResultFromInvocation(
  fallbackName: string,
  fallbackSessionRef: string | undefined,
  invocation: RelayActionInvocation,
  ack: RelayActionInvocationAck & { placement?: { node?: string } },
): SpawnResult {
  const output = asRecord(invocation.output)
  const agent = asRecord(output?.agent)
  const name = readString(output, 'name', 'agent_name', 'agentName') ?? readString(agent, 'name') ?? fallbackName
  const sessionRef = readString(output, 'session_ref', 'sessionRef', 'session_id', 'sessionId')
    ?? readString(agent, 'session_ref', 'sessionRef', 'session_id', 'sessionId')
    ?? fallbackSessionRef
  const pid = readNumber(output, 'pid') ?? readNumber(agent, 'pid')
  const pids = readNumberArray(output, 'pids') ?? readNumberArray(agent, 'pids')
  const node = readString(output, 'node') ?? readString(agent, 'node') ?? ack.placement?.node ?? ack.dispatchedNodeId ?? undefined
  return {
    name,
    ...(sessionRef ? { sessionRef } : {}),
    ...(pid !== undefined ? { pid } : {}),
    ...(pids ? { pids } : {}),
    ...(node ? { node } : {}),
    locality: 'remote',
  }
}

function assertNamedRemotePlacement(result: SpawnResult, acknowledgedNode: string | undefined): void {
  // The placement acknowledgement is authoritative. Action output may name
  // the node executing the spawn handler even when Relay acknowledged `self`,
  // so accepting the synthesized SpawnResult would let self-placement pass.
  const node = acknowledgedNode?.trim()
  if (!node || node === 'self') {
    throw new Error(
      `Relay placement did not prove a named remote node for ${result.name}; ` +
      `refusing to accept the spawn result`,
    )
  }
}

function lifecycleSignalFromInvocation(
  input: Record<string, unknown> | undefined,
  callerName: string | null | undefined,
  invocationId: string,
): AgentLifecycleSignal | undefined {
  const name = callerName?.trim()
  const kind = readString(input, 'kind')
  const issueKey = readString(input, 'issueKey', 'issue_key')
  const role = readString(input, 'role')
  const question = readString(input, 'question')
  if (
    !name ||
    !issueKey ||
    !kind ||
    !['completed', 'ready', 'blocked'].includes(kind) ||
    !role ||
    !['implementer', 'reviewer', 'babysitter', 'workflow'].includes(role) ||
    (kind === 'blocked' && !question)
  ) {
    return undefined
  }
  return {
    name,
    kind: kind as AgentLifecycleSignal['kind'],
    issueKey,
    role: role as NonNullable<AgentLifecycleSignal['role']>,
    ...(question ? { question } : {}),
    invocationId,
  }
}

function lifecycleUsageFromInvocation(
  input: Record<string, unknown> | undefined,
  name: string,
): AgentUsage | undefined {
  const usage = asRecord(input?.usage)
  if (!usage) return undefined
  const model = readString(usage, 'model', 'model_id', 'modelId')
  return {
    name,
    ...(model ? { model } : {}),
    inputTokens: nullableTokenCount(usage, 'inputTokens', 'input_tokens', 'promptTokens', 'prompt_tokens'),
    outputTokens: nullableTokenCount(usage, 'outputTokens', 'output_tokens', 'completionTokens', 'completion_tokens'),
  }
}

function nullableTokenCount(record: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue
    const value = record[key]
    return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null
  }
  return null
}

function normalizeCapabilities(capabilities: RelayNodeCapability[] | undefined): NodeCapability[] {
  const names = (capabilities ?? [])
    .map((capability) => capability.name)
    .filter((name): name is NodeCapability => knownCapabilities.has(name as NodeCapability))
  return [...new Set(names)]
}

function previewReferenceFromInvocation(
  invocation: RelayActionInvocation,
  placementNode?: string,
): PreviewReference {
  const output = asRecord(invocation.output)
  const preview = previewReference(output?.preview, placementNode)
  if (!preview) throw new Error('Preview provider returned an invalid reference')
  return preview
}

function previewReference(value: unknown, placementNode?: string): PreviewReference | undefined {
  const record = asRecord(value)
  const id = readString(record, 'id')
  const namespace = readString(record, 'namespace')
  const owner = readString(record, 'owner')
  const service = readString(record, 'service')
  const repo = readString(record, 'repo')
  const url = readString(record, 'url')
  const configuredTargetPort = readNumber(record, 'configuredTargetPort', 'configured_target_port')
  const targetPort = readNumber(record, 'targetPort', 'target_port')
  const httpsPort = readNumber(record, 'httpsPort', 'https_port')
  const createdAt = readString(record, 'createdAt', 'created_at')
  const node = readString(record, 'node') ?? placementNode
  if (
    !id || !namespace || !owner || !service || !repo || !url || !createdAt ||
    targetPort === undefined || httpsPort === undefined ||
    record?.provider !== 'tailscale-serve' || record.access !== 'tailnet' || record.lifetime !== 'issue'
  ) return undefined
  const startCommand = readString(record, 'startCommand', 'start_command')
  if (!startCommand) return undefined
  const rawProcess = asRecord(record?.process)
  const processPid = readNumber(rawProcess, 'pid')
  const processStartTime = readString(rawProcess, 'startTime', 'start_time')
  const processCmdline = readString(rawProcess, 'cmdline')
  const processCwd = readString(rawProcess, 'cwd')
  const processMarker = readString(rawProcess, 'marker')
  const process = processPid !== undefined && processStartTime && processCmdline && processCwd && processMarker
    ? {
        pid: processPid,
        startTime: processStartTime,
        cmdline: processCmdline,
        cwd: processCwd,
        marker: processMarker,
      }
    : undefined
  return {
    id,
    provider: 'tailscale-serve',
    namespace,
    owner,
    service,
    repo,
    url,
    ...(configuredTargetPort !== undefined ? { configuredTargetPort } : {}),
    targetPort,
    httpsPort,
    access: 'tailnet',
    lifetime: 'issue',
    createdAt,
    startCommand,
    ...(process ? { process } : {}),
    ...(node ? { node } : {}),
  }
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
