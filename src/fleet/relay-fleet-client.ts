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
  /** A rejected spawn whose compensating release must be retried. */
  pendingReleaseReason?: string
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
  /** Recorded on the takeover audit row; set it to the real node where known. */
  nodeId?: string
  /** Injectable for tests; the takeover endpoint is not on the SDK surface. */
  fetch?: typeof globalThis.fetch
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
// A backstop against an unbounded re-registration loop, not a tight retry
// budget: a name conflict already fails immediately, so this only bounds
// *consecutive* transient failures and is deliberately forgiving of a cold
// start against a slow relay.
export const MAX_REGISTRATION_ATTEMPTS = 10
// Matches the default @agent-relay/sdk resolves when no baseUrl is configured,
// so a takeover always targets the host the register went to.
const DEFAULT_RELAY_BASE_URL = 'https://cast.agentrelay.com'
// Every engine status that means somebody is holding the identity. `offline` is
// the only one that is not here, and `unknown` is what the SDK substitutes when
// status is missing — so an absent status never reads as safe.
const LIVE_AGENT_STATUSES = new Set(['online', 'active', 'idle', 'blocked', 'waiting', 'unknown'])
export const DEFAULT_LIFECYCLE_ACTION_NAME = 'factory.lifecycle'
const DEFAULT_SPAWN_ACK_TIMEOUT_MS = 5 * 60_000
const DEFAULT_POLL_INTERVAL_MS = 1_000
const DEFAULT_EXIT_WATCH_INTERVAL_MS = 15_000
// 2× the engine's 45s node-liveness TTL so one missed heartbeat sweep cannot
// synthesize a false exit.
const DEFAULT_NODE_OFFLINE_GRACE_MS = 90_000
const DEFAULT_REGISTRATION_GRACE_MS = 60_000
/**
 * Compensating release for a placement Relay accepted after we stopped waiting.
 *
 * Giving up locally does not cancel a remote spawn: the engine may already have
 * launched the worker. Without this the #306 bound would trade an infinite hang
 * for a leaked agent — the same shape #304 fixed with `LatePlacementReleasedError`
 * and `#releaseOrphanedLatePlacement` one layer up (#307 review, cubic).
 */
const LATE_PLACEMENT_RELEASE_REASON = 'late-placement-timeout'

/** Distinguishes "the call outlived its budget" from any value the call returns. */
const CALL_TIMED_OUT = Symbol('relay.placement.callTimedOut')

type CallOutcome<T> = { ok: true; value: T } | { ok: false; error: unknown }

/**
 * A placement call that outlived the operation's remaining budget (#306).
 *
 * Deliberately **not** registered in `isClassifiedPerItemDispatchFailure`. The
 * classified conditions are per-item and self-healing — the unit goes back to
 * the queue and the next pass dispatches it. This one is neither. A placement
 * that cannot reach a terminal status inside the ack budget is evidence about
 * the fleet, not about the work unit: the node is gone, wedged, or running a
 * broker that acks without launching. Retrying that against the same fleet
 * produces the same timeout, so a run of them is a pass-wide fault and should
 * trip the #292 fuse rather than be exempted from it.
 *
 * Classifying it would rebuild the outage in slow motion — instead of one
 * sweep hung forever, an unbounded series of five-minute sweeps that never
 * abort, never alert, and never dispatch. The fuse is the alarm; this error is
 * meant to reach it.
 */
export class RelaySpawnAckTimeoutError extends Error {
  readonly operation: string
  readonly timeoutMs: number

  constructor(operation: string, timeoutMs: number) {
    super(`Relay placement timed out after ${timeoutMs}ms waiting for ${operation}`)
    this.name = 'RelaySpawnAckTimeoutError'
    this.operation = operation
    this.timeoutMs = timeoutMs
  }
}

export class RelayFleetClient implements FleetClient {
  readonly placementLocality = 'remote' as const
  readonly durableOwnership = true
  readonly lifecycleActionName: string
  readonly #options: RelayFleetClientOptions
  readonly #agentName: string
  // The token minted by this process's own registration. Reused across
  // bootstrap retries so a failure *after* registration never re-registers
  // the same name — see #registerFactoryAgent (factory#316).
  #registeredAgentToken?: string
  #registrationAttempts = 0
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
  #pendingReleaseRetry: Promise<void> | undefined

  /**
   * Workspace identity this client registers as, after defaulting.
   *
   * Exposed read-only so a caller (and its tests) can confirm which identity a
   * given configuration actually resolved to. Registration reads `#agentName`
   * directly; this changes nothing about it.
   */
  get agentName(): string {
    return this.#agentName
  }

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
    // One budget for the whole placement: bootstrap, lifecycle registration,
    // the placement call and every poll after it share it (#306). Anchoring it
    // here rather than inside `#awaitInvocation` is what stops the time already
    // spent from being free.
    const deadlineAtMs = this.#operationDeadline()
    const messaging = await this.#withinDeadline('messaging bootstrap', deadlineAtMs, () => this.#ensureMessaging())
    if (input.capability.startsWith('spawn:')) {
      await this.#withinDeadline(
        'lifecycle action registration',
        deadlineAtMs,
        () => this.#ensureLifecycleAction(messaging),
      )
      // A transient startup registration failure may have torn down the first
      // subscription attempt. Re-arm it after the required action is durable so
      // the invocation cannot be accepted without a live Factory consumer.
      this.#ensureEventSubscription()
    }
    const ack = await this.#withinDeadline('placement.spawn', deadlineAtMs, () => messaging.placement.spawn({
      capability: input.capability,
      // 'self' from the orchestrator means "no placement preference": let the
      // engine pick the least-loaded eligible node.
      ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
      ...(input.repo ? { repo: input.repo } : {}),
      input: spawnActionInput(input),
      ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
      // An ack proves the engine accepted the dispatch, not that the node
      // launched anything: a node advertising `spawn:<harness>` on an obsolete
      // broker acks and launches nothing, and that is indistinguishable from a
      // real spawn until someone reads the invocation back. `confirm` makes the
      // SDK do that read, bounded, and fail as `spawn_unconfirmed` instead of
      // handing us an ack we would wait on forever (#306).
      confirm: true,
      confirmTimeoutMs: Math.max(1, deadlineAtMs - this.#now()),
      confirmPollIntervalMs: this.#pollIntervalMs,
      log: this.#log,
      // Giving up on the wait does not cancel the placement. If Relay accepts
      // it after we have already reported failure, a worker is live that
      // nothing is tracking — so release it (#307 review, cubic).
    }), (inFlight) => this.#releaseAbandonedPlacement(input.name, inFlight))
    // A confirmed placement already carries the terminal invocation. Polling
    // for it again would spend the same budget twice over on a spawn that has
    // already proven it launched.
    let invocation: RelayActionInvocation
    try {
      invocation = ack.confirmation
        ?? await this.#awaitInvocation(ack.actionName || 'spawn', ack, deadlineAtMs)
    } catch (error) {
      // Holding an ack means Relay accepted the placement, so running out of
      // budget while polling leaves a worker we asked for and never adopted.
      // This is the certain leak; the abandoned-placement case above is the
      // possible one.
      if (error instanceof RelaySpawnAckTimeoutError) {
        await this.#releaseLatePlacement(input.name, ack)
      }
      throw error
    }
    const result = spawnResultFromInvocation(input.name, input.sessionRef, invocation, ack)
    let acknowledgedNode: string
    try {
      acknowledgedNode = assertNamedRemotePlacement(result, ack.placement?.node)
    } catch (error) {
      // A completed placement invocation has already launched the worker. If
      // Relay cannot prove that it ran on a named remote node, tear that worker
      // down before refusing the result so a rejected spawn cannot keep acting
      // outside Factory's lifecycle tracking.
      try {
        await this.release(result.name, 'unverified-placement')
      } catch (releaseError) {
        // Do not forget a live worker merely because its compensating release
        // failed. Retain it outside the accepted placement result and let the
        // reconciliation loop retry the idempotent release until Relay
        // confirms cleanup (or roster evidence proves the worker exited).
        this.#track(result.name, {
          invocationId: ack.invocationId,
          pendingReleaseReason: 'unverified-placement',
        })
        this.#log(
          `Failed to release ${result.name} after unverified Relay placement: ${errorMessage(releaseError)}`,
        )
      }
      throw error
    }
    const trustedResult = { ...result, node: acknowledgedNode }
    this.#track(trustedResult.name, { invocationId: ack.invocationId, node: acknowledgedNode })
    return trustedResult
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
    // `release` goes through `commands.invoke`, not `placement.spawn`, so the
    // SDK's `confirm` cannot bound it — it needs the explicit budget (#306).
    // An unbounded release is how the reaper's own teardown wedges.
    const deadlineAtMs = this.#operationDeadline()
    const messaging = await this.#withinDeadline('messaging bootstrap', deadlineAtMs, () => this.#ensureMessaging())
    const ack = await this.#withinDeadline('release invoke', deadlineAtMs, () => messaging.commands.invoke('release', {
      name,
      agent: name,
      ...(reason ? { reason } : {}),
    }))
    await this.#awaitInvocation(ack.actionName || 'release', ack, deadlineAtMs)
    this.#tracked.delete(name)
    this.#syncExitWatcher()
  }

  async createPreview(input: PreviewStartInput): Promise<PreviewReference> {
    // Previews reach the same unbounded placement surface as agent spawns and
    // need the same budget (#306). They keep the poll-based read-back rather
    // than `confirm`: their failure semantics are the preview reaper's, not the
    // dispatch fuse's, and bounding is what they were missing.
    const deadlineAtMs = this.#operationDeadline()
    const messaging = await this.#withinDeadline('messaging bootstrap', deadlineAtMs, () => this.#ensureMessaging())
    const ack = await this.#withinDeadline('preview placement.spawn', deadlineAtMs, () => messaging.placement.spawn({
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
    }))
    const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack, deadlineAtMs)
    return previewReferenceFromInvocation(invocation, ack.placement?.node ?? ack.dispatchedNodeId)
  }

  async removePreview(preview: PreviewReference): Promise<boolean> {
    const deadlineAtMs = this.#operationDeadline()
    const messaging = await this.#withinDeadline('messaging bootstrap', deadlineAtMs, () => this.#ensureMessaging())
    const ack = await this.#withinDeadline('preview placement.spawn', deadlineAtMs, () => messaging.placement.spawn({
      capability: 'preview:tailscale-serve',
      ...(preview.node ? { node: preview.node } : {}),
      input: { operation: 'remove', preview },
      ...(this.#options.placementTtlMs !== undefined ? { ttlMs: this.#options.placementTtlMs } : {}),
      log: this.#log,
    }))
    const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack, deadlineAtMs)
    return asRecord(invocation.output)?.removed === true
  }

  async reapPreviews(input: PreviewSweepInput): Promise<PreviewSweepResult> {
    const deadlineAtMs = this.#operationDeadline()
    const messaging = await this.#withinDeadline('messaging bootstrap', deadlineAtMs, () => this.#ensureMessaging())
    // The roster is three unbounded reads (`agents.presence`, `agents.list`,
    // `nodes.list`). Leaving it outside the budget left exactly the hang this
    // change removes everywhere else: `#reapPreviewOrphans` keeps the sweep
    // promise in `#previewSweepInFlight` and schedules the next sweep only from
    // its `.finally()`, so one stalled read stops preview cleanup for good
    // (#307 review, codex/coderabbit/cubic).
    const roster = await this.#withinDeadline('preview roster', deadlineAtMs, () => this.roster())
    const nodes = roster.nodes.filter((node) =>
      node.live && node.capabilities.includes('preview:tailscale-serve'),
    )
    const reports = await Promise.all(nodes.map(async (node): Promise<PreviewSweepResult> => {
      try {
        const ack = await this.#withinDeadline('preview placement.spawn', deadlineAtMs, () => messaging.placement.spawn({
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
        }))
        const invocation = await this.#awaitInvocation(ack.actionName || 'preview:tailscale-serve', ack, deadlineAtMs)
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

    // A rejected placement may already have launched a worker. If its first
    // compensating release failed, disposal must not erase the only ownership
    // record before giving cleanup one final synchronous retry. Refuse to
    // dispose while any such release remains unconfirmed; callers then get a
    // hard failure containing the deterministic worker name instead of a
    // successful shutdown that silently strands it.
    if ([...this.#tracked.values()].some((agent) => agent.pendingReleaseReason)) {
      await this.#retryPendingReleases()
      const pendingNames = [...this.#tracked]
        .filter(([, agent]) => agent.pendingReleaseReason)
        .map(([name]) => name)
      if (pendingNames.length > 0) {
        throw new Error(
          `Refusing to dispose Relay fleet client with unconfirmed worker cleanup: ${pendingNames.join(', ')}`,
        )
      }
    }

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
    // Registration is not idempotent: the engine answers a repeat of a name it
    // already holds with 409 agent_already_exists. Reusing the token this
    // process already minted is what keeps a post-registration bootstrap
    // failure from colliding with our own record (factory#316).
    agentToken ??= this.#registeredAgentToken
    if (!agentToken) {
      agentToken = await this.#registerFactoryAgent(workspaceKey as string)
      this.#registeredAgentToken = agentToken
    }
    const relay = this.#createRelay({
      ...(workspaceKey ? { workspaceKey } : {}),
      agentToken,
      ...(this.#options.baseUrl ? { baseUrl: this.#options.baseUrl } : {}),
    })
    this.#messaging = relay.messaging
    return this.#messaging
  }

  // Mints this process's agent identity from the workspace key.
  //
  // `registerOrRotate`/`registerOrGet` used to recover from a name conflict by
  // rotating the existing agent's token. In @relaycast/sdk 8.2.0 both became
  // deprecated aliases for plain `register`, and the engine simultaneously
  // closed the rotate path to workspace keys, so a conflict stopped being
  // recoverable. Because the methods still *exist*, an optional-chain fallback
  // never fires — the client just kept re-registering into 409 forever
  // (factory#316). Call `register` directly, and reclaim the name by audited
  // takeover when it is already held.
  async #registerFactoryAgent(workspaceKey: string): Promise<string> {
    this.#registrationAttempts += 1
    if (this.#registrationAttempts > MAX_REGISTRATION_ATTEMPTS) {
      throw new FactoryAgentRegistrationError(
        this.#agentName,
        `gave up after ${MAX_REGISTRATION_ATTEMPTS} registration attempts`,
      )
    }
    const bootstrap = this.#createRelay({
      workspaceKey,
      ...(this.#options.baseUrl ? { baseUrl: this.#options.baseUrl } : {}),
    })
    const agents = bootstrap.messaging.agents
    try {
      const registration = await agents.register({ name: this.#agentName })
      this.#registrationAttempts = 0
      return registration.token
    } catch (error) {
      // Re-registering is what looped for six days. The only way out is to
      // reclaim the record we collided with — or to fail by name.
      if (isAgentNameConflictError(error)) {
        return await this.#reclaimFactoryAgent(workspaceKey, agents, error)
      }
      throw error
    }
  }

  /** The wall-clock instant one placement operation must be finished by. */
  #operationDeadline(): number {
    return this.#now() + this.#spawnAckTimeoutMs
  }

  /**
   * Await `call`, or give up on it once the operation's budget is spent.
   *
   * The Relay messaging surface takes no `AbortSignal`, so racing the wait is
   * the only bound available — it abandons the *wait*, not the call. That is
   * the whole point of #306: a deadline read only between calls cannot bound
   * any of them. `#awaitInvocation` checked `Date.now() > deadline` at the top
   * of its poll loop around an unbounded `commands.getInvocation`, so a read
   * that never settled never returned control to the check. Production ran one
   * readiness sweep for 62 minutes against a 5-minute bound.
   *
   * `remainingMs` comes from the shared deadline rather than a per-call
   * constant, so many polls cost one budget between them instead of a fresh
   * budget each. Rejection is folded into the outcome value so a slow call we
   * stopped waiting on cannot later surface as an unhandled rejection.
   *
   * Takes a thunk rather than a promise so an exhausted budget refuses *before*
   * the request is made. An already-started promise would mean the call had
   * been sent — a mutating one, on the placement path — while this method was
   * still deciding there was no time left to make it (#307 review, cubic).
   */
  async #withinDeadline<T>(
    operation: string,
    deadlineAtMs: number,
    start: () => Promise<T>,
    /**
     * Called when the budget runs out with the call still in flight, receiving
     * the abandoned work. Abandoning the *wait* does not cancel the *call*, so
     * a mutating operation needs a way to clean up whatever it still lands.
     */
    onAbandoned?: (inFlight: Promise<CallOutcome<T>>) => void,
  ): Promise<T> {
    const remainingMs = deadlineAtMs - this.#now()
    if (remainingMs <= 0) {
      throw new RelaySpawnAckTimeoutError(operation, this.#spawnAckTimeoutMs)
    }
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      // Folded to an outcome once, so the abandoned handler shares this promise
      // rather than attaching a second rejection path to the same call.
      const inFlight: Promise<CallOutcome<T>> = start().then(
        (value) => ({ ok: true, value }) as const,
        (error) => ({ ok: false, error }) as const,
      )
      const outcome = await Promise.race<CallOutcome<T> | typeof CALL_TIMED_OUT>([
        inFlight,
        new Promise<typeof CALL_TIMED_OUT>((resolve) => {
          timer = setTimeout(() => resolve(CALL_TIMED_OUT), remainingMs)
          timer.unref?.()
        }),
      ])
      if (outcome === CALL_TIMED_OUT) {
        onAbandoned?.(inFlight)
        throw new RelaySpawnAckTimeoutError(operation, this.#spawnAckTimeoutMs)
      }
      if (outcome.ok) return outcome.value
      throw outcome.error
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  /**
   * Release a worker whose placement Relay accepted after the local deadline.
   *
   * Tracked *before* the release is attempted so a failed cleanup is retried by
   * the reconciliation loop instead of forgotten — the same retain-and-retry the
   * `unverified-placement` path uses. Losing the release would leave a live
   * worker outside Factory's lifecycle tracking.
   */
  async #releaseLatePlacement(name: string, ack: RelayActionInvocationAck & { placement?: { node?: string } }): Promise<void> {
    const node = ack.placement?.node ?? ack.dispatchedNodeId
    this.#track(name, {
      invocationId: ack.invocationId,
      ...(node ? { node } : {}),
      pendingReleaseReason: LATE_PLACEMENT_RELEASE_REASON,
    })
    this.#log(`Releasing ${name}: Relay accepted its placement after the local deadline expired`)
    try {
      await this.release(name, LATE_PLACEMENT_RELEASE_REASON)
    } catch (error) {
      // Retained above, so reconciliation retries the idempotent release.
      this.#log(`Failed to release late placement ${name}: ${errorMessage(error)}`)
    }
  }

  /**
   * Attach a compensating release to a placement we stopped waiting for.
   *
   * If the call ultimately fails, nothing was launched and there is nothing to
   * release. If it succeeds, a worker exists that this process has already
   * reported as failed, so it must be torn down.
   */
  #releaseAbandonedPlacement(
    name: string,
    inFlight: Promise<CallOutcome<RelayActionInvocationAck & { placement?: { node?: string } }>>,
  ): void {
    void inFlight.then(async (outcome) => {
      if (!outcome.ok) return
      await this.#releaseLatePlacement(name, outcome.value)
    })
  }

  // The name exists and we hold no token for it. `recover` cannot help: its
  // three authorities are agent-token, origin-node and work-unit-proof, and a
  // workspace key establishes none of them. `/takeover` is the engine's own
  // workspace-admin escape hatch for exactly this, and it is gated by
  // requireWorkspaceKey — the credential we already have. It is absent from the
  // `@agent-relay/sdk` agents surface, so this calls the endpoint directly.
  async #reclaimFactoryAgent(
    workspaceKey: string,
    agents: RelayMessaging['agents'],
    conflict: unknown,
  ): Promise<string> {
    let lastError: unknown = conflict
    // The engine rejects a stale `expected_agent_id` with 409
    // agent_identity_conflict; that means the record moved under us, so re-read
    // it and try once more rather than treating it as terminal.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const existing = asRecord(await agents.get(this.#agentName).catch(() => undefined))
      const agentId = readString(existing, 'id', 'agent_id', 'agentId')
      if (!agentId) {
        throw new FactoryAgentRegistrationError(
          this.#agentName,
          `name is taken but the record could not be read back: ${errorMessage(lastError)}`,
          { cause: lastError },
        )
      }
      await this.#assertAgentNotLive(agents, existing)
      try {
        return await this.#takeOverAgent(workspaceKey, agentId)
      } catch (error) {
        lastError = error
        if (isIdentityMovedError(error) && attempt === 0) continue
        if (error instanceof FactoryAgentRegistrationError) throw error
        throw new FactoryAgentRegistrationError(
          this.#agentName,
          `could not reclaim the existing identity: ${errorMessage(error)}`,
          { cause: error },
        )
      }
    }
    throw new FactoryAgentRegistrationError(
      this.#agentName,
      `could not reclaim the existing identity: ${errorMessage(lastError)}`,
      { cause: lastError },
    )
  }

  // Seizing a name from a LIVE agent would strand its credential. Our case is a
  // collision with our own dead record, but the guard is coded rather than
  // assumed — the situation is not a safety property.
  async #assertAgentNotLive(
    agents: RelayMessaging['agents'],
    existing: Record<string, unknown> | undefined,
  ): Promise<void> {
    // Allow-list, not a deny-list. The engine also has active/idle/blocked/
    // waiting, all of which mean someone is holding this identity, and
    // `unknown` is what the SDK substitutes when status is missing. Only a
    // record that says offline is safe to seize.
    const status = readString(existing, 'status')
    let live = status !== 'offline'
    if (!live) {
      // `/v1/agents/presence` lists the whole workspace, offline rows included,
      // and always carries an explicit status. Match on the live statuses
      // rather than "not offline" so a row that somehow omits status cannot
      // lock recovery out forever — the record check above is the strict gate.
      const presence = await agents.presence().catch(() => undefined)
      live = Array.isArray(presence) && presence.some((entry) => {
        const seen = asRecord(entry)
        if (readString(seen, 'agentName', 'agent_name', 'name') !== this.#agentName) return false
        const seenStatus = readString(seen, 'status')
        return seenStatus !== undefined && LIVE_AGENT_STATUSES.has(seenStatus)
      })
    }
    if (live) {
      throw new FactoryAgentRegistrationError(
        this.#agentName,
        `refusing to take over agent in status "${status ?? 'unknown'}"; another factory may still hold this identity`,
      )
    }
  }

  async #takeOverAgent(workspaceKey: string, expectedAgentId: string): Promise<string> {
    const base = (this.#options.baseUrl ?? DEFAULT_RELAY_BASE_URL).replace(/\/+$/, '')
    const url = `${base}/v1/agents/${encodeURIComponent(this.#agentName)}/takeover`
    const doFetch = this.#options.fetch ?? globalThis.fetch
    // Every field lands in the audit row, so make them all say something.
    const response = await doFetch(url, {
      method: 'POST',
      headers: { authorization: `Bearer ${workspaceKey}`, 'content-type': 'application/json' },
      body: JSON.stringify({
        expected_agent_id: expectedAgentId,
        actor: `factory:${this.#agentName}`,
        reason: 'factory bootstrap reclaiming its own workspace identity after a registration conflict (factory#316)',
        session_ref: `factory-bootstrap-${this.#now()}`,
        node_id: this.#options.nodeId ?? 'factory-cloud-container',
      }),
    })
    const payload = asRecord(await response.json().catch(() => undefined))
    if (!response.ok) {
      const failure = asRecord(payload?.error)
      const error = new Error(
        readString(failure, 'message') ?? `takeover failed with HTTP ${response.status}`,
      ) as Error & { code?: string; statusCode?: number }
      error.code = readString(failure, 'code')
      error.statusCode = response.status
      throw error
    }
    const data = asRecord(payload?.data) ?? payload
    const token = readString(data, 'token')
    if (!token) throw new Error('takeover succeeded but returned no agent token')
    this.#registrationAttempts = 0
    return token
  }

  /**
   * Poll an invocation to a terminal status inside `deadlineAtMs`.
   *
   * Callers that already spent part of the budget — `spawn` burns some on
   * bootstrap and placement — pass their own deadline so the total stays
   * bounded. A caller that omits it is starting a fresh operation (`release`,
   * the preview paths) and gets a full budget of its own.
   */
  async #awaitInvocation(
    actionName: string,
    ack: RelayActionInvocationAck,
    deadlineAtMs: number = this.#operationDeadline(),
  ): Promise<RelayActionInvocation> {
    const messaging = await this.#withinDeadline(
      `${actionName} messaging bootstrap`,
      deadlineAtMs,
      () => this.#ensureMessaging(),
    )
    let status = ack.status ?? 'pending'
    let invocation: RelayActionInvocation | undefined

    while (!terminalStatuses.has(status)) {
      if (this.#now() >= deadlineAtMs) {
        throw new RelaySpawnAckTimeoutError(
          `${actionName} invocation ${ack.invocationId} to complete (last status: ${status})`,
          this.#spawnAckTimeoutMs,
        )
      }
      if (!openStatuses.has(status)) {
        throw new Error(`Unexpected ${actionName} invocation ${ack.invocationId} status: ${status}`)
      }
      // Never sleep past the deadline: that only buys one more pointless read.
      await this.#sleep(Math.max(0, Math.min(this.#pollIntervalMs, deadlineAtMs - this.#now())))
      invocation = await this.#withinDeadline(
        `${actionName} invocation ${ack.invocationId}`,
        deadlineAtMs,
        () => messaging.commands.getInvocation(actionName, ack.invocationId),
      )
      status = invocation.status || 'pending'
    }

    invocation ??= await this.#withinDeadline(
      `${actionName} invocation ${ack.invocationId}`,
      deadlineAtMs,
      () => messaging.commands.getInvocation(actionName, ack.invocationId),
    )
    if (status === 'failed' || status === 'denied') {
      throw new Error(`${actionName} invocation ${ack.invocationId} ${status}${invocation.error ? `: ${invocation.error}` : ''}`)
    }
    return invocation
  }

  #track(name: string, placement: {
    invocationId: string
    node?: string
    pendingReleaseReason?: string
  }): void {
    this.#tracked.set(name, {
      invocationId: placement.invocationId,
      ...(placement.node ? { node: placement.node } : {}),
      ...(placement.pendingReleaseReason
        ? { pendingReleaseReason: placement.pendingReleaseReason }
        : {}),
      spawnedAtMs: this.#now(),
    })
    this.#syncExitWatcher()
  }

  #syncExitWatcher(): void {
    const hasPendingRelease = [...this.#tracked.values()].some((agent) => agent.pendingReleaseReason)
    const shouldRun = !this.#disposed && this.#tracked.size > 0 && (
      this.#agentExitListeners.size > 0 || hasPendingRelease
    )
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
    // Cleanup is more important than roster metadata and does not depend on
    // it. Retry rejected-placement releases first so a presence/node outage
    // cannot prevent the compensating action.
    await this.#retryPendingReleases()
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

  #retryPendingReleases(): Promise<void> {
    this.#pendingReleaseRetry ??= this.#runPendingReleaseRetries().finally(() => {
      this.#pendingReleaseRetry = undefined
    })
    return this.#pendingReleaseRetry
  }

  async #runPendingReleaseRetries(): Promise<void> {
    for (const [name, entry] of [...this.#tracked]) {
      if (!entry.pendingReleaseReason) continue
      try {
        await this.release(name, entry.pendingReleaseReason)
      } catch (error) {
        this.#log(`Pending release retry failed for ${name}: ${errorMessage(error)}`)
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

function assertNamedRemotePlacement(result: SpawnResult, acknowledgedNode: string | undefined): string {
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
  return node
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

/**
 * Registration could not converge on a usable agent identity. Named so a
 * six-day silent 409 loop surfaces as one actionable failure instead.
 */
export class FactoryAgentRegistrationError extends Error {
  readonly agentName: string

  constructor(agentName: string, detail: string, options?: { cause?: unknown }) {
    super(`Factory agent "${agentName}" could not register with the relay workspace: ${detail}`)
    this.name = 'FactoryAgentRegistrationError'
    this.agentName = agentName
    if (options && 'cause' in options) {
      ;(this as Error & { cause?: unknown }).cause = options.cause
    }
  }
}

// The SDK reports the engine's 409 agent_already_exists as a RelayError with
// `code: 'name_conflict'` and `rawCode: 'agent_already_exists'`. Match on the
// structured fields first and keep the message probe for stubs and older SDKs.
function isAgentNameConflictError(error: unknown): boolean {
  const record = asRecord(error)
  if (record) {
    if (record.rawCode === 'agent_already_exists' || record.code === 'name_conflict') return true
    if (record.statusCode === 409 || record.status === 409) return true
  }
  return /already exists|name[ _]conflict|agent_already_exists/i.test(errorMessage(error))
}

// 409 agent_identity_conflict means the record moved between our read and the
// takeover — retryable once, unlike agent_already_exists which is the conflict
// we are already handling.
function isIdentityMovedError(error: unknown): boolean {
  const record = asRecord(error)
  if (record?.code === 'agent_identity_conflict') return true
  return /agent_identity_conflict|no longer has expected id/i.test(errorMessage(error))
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
