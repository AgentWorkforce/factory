import { HarnessDriverClient } from '@agent-relay/harness-driver'
import { AgentRelay } from '@agent-relay/sdk'
import { accessSync, constants, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { readFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import type { BrokerEvent, ListAgent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import type { PreviewConfig } from '../config/schema'
import type { AgentMessage, AgentPidResolution, AgentUsage, Capability, FleetClient, FleetTrackedAgent, PreviewReference, PreviewStartInput, PreviewSweepInput, PreviewSweepResult, RosterEntry, SendInput, SpawnInput, SpawnResult, TeammateAgent, TeammateQuery } from '../ports/fleet'
import type { Logger } from '../ports/system'
import { normalizeLogger } from '../logging'
import { TailscalePreviewManager, type PreviewManager } from '../node/tailscale-preview'
import { resolveRelayWorkspaceKey } from './relay-workspace-key'
import { RelaycastTeammateDirectory, type TeammateDirectory } from './teammates'

const requireForResolve = createRequire(import.meta.url)

type SpawnedHandleLike = { name: string; sessionId?: string; session_ref?: string; sessionRef?: string; pid?: number }
type HarnessEventListener = (event: BrokerEvent) => void
type DriverAgentLike = { name: string; sessionId?: string; pid?: number }
type DriverDeliveryEventLike = BrokerEvent
export type AgentRelayMcpCommand = { command: string; args: string[] }

export interface HarnessDriverClientLike {
  readonly brokerPid?: number
  /**
   * Broker URL this client is bound to. Optional because test doubles do not
   * have one; the real client exposes it so a failure can name the address it
   * actually attempted instead of Node's unattributable bare `fetch failed`.
   */
  readonly baseUrl?: string
  getStatus?(): Promise<{ node_delivery?: { connected?: boolean } } | null | undefined>
  spawnPty(input: SpawnPtyInput): Promise<SpawnedHandleLike>
  release(name: string, reason?: string): Promise<{ name: string }>
  listAgents(): Promise<Array<Pick<ListAgent, 'name' | 'cli' | 'pid'>>>
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
  // Maximum time dispose() waits for agents spawned through this client to
  // exit before forcing shutdown of an owned broker.
  ownedBrokerAgentExitTimeoutMs?: number
  cwd?: string
  connectionPath?: string
  /**
   * Opens a client against whatever `connection.json` currently names. Called
   * at construction (when no `client` is injected) and again whenever the file
   * turns out to name a different broker than the one we are attached to.
   * Injected by tests; defaults to the real Harness Driver connect.
   */
  connect?: (options: { cwd?: string; connectionPath?: string }) => HarnessDriverClientLike
  workspaceKey?: string
  /** Card-aware directory seam; useful with a local broker plus mock directory. */
  teammateDirectory?: TeammateDirectory
  directoryBaseUrl?: string
  directoryFetch?: typeof globalThis.fetch
  directoryTimeoutMs?: number
  /** Canonical cloud liveness lookup. Injected by tests; derived from workspaceKey in production. */
  listCanonicalOnlineAgentNames?: () => Promise<readonly string[]>
  /** Local process-liveness probe used to preserve workers that intentionally run without Relay MCP presence. */
  isProcessAlive?: (pid: number) => boolean
  now?: () => number
  resumeCapability?: Capability
  logger?: Logger
  resolveAgentRelayMcpCommand?: () => AgentRelayMcpCommand | undefined
  previewConfig?: PreviewConfig
  previewManager?: PreviewManager
}

type AgentExitListener = (name: string, reason?: string) => void
type DeliveryFailedListener = (info: { to: string; msgId?: string; reason?: string }) => void
type AgentMessageListener = (message: AgentMessage) => void
type AgentUsageListener = (usage: AgentUsage) => void | Promise<void>
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

const PID_RESOLVE_ATTEMPTS = 3
const PID_RESOLVE_BACKOFF_MS = 75
const READY_RESEND_DELAY_MS = 1_000
// One-shot commands normally finish well inside this guard. Keep it bounded so
// a hung task_exit worker cannot hold a cold-started broker open indefinitely.
const OWNED_BROKER_AGENT_EXIT_TIMEOUT_MS = 30 * 60 * 1_000
// Broker release replies occasionally drop under cold-start races and surface
// as an explicitly `retryable: true` HTTP 500 (`internal reply dropped`). The
// agent is already teardown-bound on our side, so a bounded retry recovers a
// graceful release without holding up completion. Kept small — a truly wedged
// broker still falls through to the caller's warn-and-continue path quickly.
const RELEASE_RETRY_MAX_ATTEMPTS = 3
const RELEASE_RETRY_BACKOFF_MS = 250
const CANONICAL_PRESENCE_REGISTRATION_GRACE_MS = 60_000

export class InternalFleetClient implements FleetClient {
  readonly placementLocality = 'local' as const
  readonly durableOwnership = true
  #client: HarnessDriverClientLike
  readonly #connect: (options: { cwd?: string; connectionPath?: string }) => HarnessDriverClientLike
  // Identifies the broker this client is attached to, as `connection.json`
  // described it when we attached. A rebind rewrites that file, so a mismatch
  // is the signal — and the ONLY signal — that reconnecting is worthwhile.
  #connectedBroker: string | undefined
  #ownsBroker: boolean
  readonly #ownedBrokerAgentExitTimeoutMs: number
  readonly #cwd?: string
  readonly #connectionPath?: string
  readonly #workspaceKey?: string
  readonly #teammateDirectory?: TeammateDirectory
  readonly #listCanonicalOnlineAgentNames?: () => Promise<readonly string[]>
  readonly #isProcessAlive: (pid: number) => boolean
  readonly #now: () => number
  readonly #resumeCapability: Capability
  readonly #logger?: Logger
  readonly #resolveAgentRelayMcpCommand: () => AgentRelayMcpCommand | undefined
  readonly #previewManager?: PreviewManager
  readonly #agentExitListeners = new Set<AgentExitListener>()
  readonly #deliveryFailedListeners = new Set<DeliveryFailedListener>()
  readonly #agentMessageListeners = new Set<AgentMessageListener>()
  readonly #agentUsageListeners = new Set<AgentUsageListener>()
  readonly #eventUnsubscribers: Array<() => void> = []
  readonly #seenEvents: string[] = []
  readonly #seenEventKeys = new Set<string>()
  readonly #pendingInjected = new Map<string, PendingInjectedWait>()
  readonly #activeInjectedWaits = new Set<PendingInjectedWait>()
  readonly #injectedEventIds: string[] = []
  readonly #injectedEventIdSet = new Set<string>()
  readonly #injectedByAgent = new Map<string, { sequence: number; eventId: string }>()
  readonly #failedDeliveries = new Map<string, Error>()
  readonly #failedDeliveryIds: string[] = []
  readonly #exitedAgentNames = new Set<string>()
  readonly #agentExitSequences = new Map<string, number>()
  readonly #readyAgentNames = new Set<string>()
  readonly #activeSpawnedAgentNames = new Set<string>()
  readonly #locallySpawnedAtMs = new Map<string, number>()
  readonly #tracked = new Map<string, FleetTrackedAgent>()
  readonly #activeAgentsDrainedListeners = new Set<() => void>()
  #suppressedDuplicateEvents = 0
  #suppressedDuplicateAgentExits = 0
  #missingIdentityEvents = 0
  #agentExitSequence = 0
  #subscribed = false
  #disposed = false

  constructor(options: InternalFleetClientOptions = {}) {
    this.#cwd = options.cwd
    this.#connectionPath = options.connectionPath
    this.#workspaceKey = options.workspaceKey
    const directoryToken = resolveRelayWorkspaceKey({ workspaceKey: options.workspaceKey })
    this.#teammateDirectory = options.teammateDirectory ?? (directoryToken
      ? new RelaycastTeammateDirectory({
          baseUrl: options.directoryBaseUrl ?? process.env.RELAY_BASE_URL,
          token: directoryToken,
          fetch: options.directoryFetch,
          timeoutMs: options.directoryTimeoutMs,
        })
      : undefined)
    if (options.listCanonicalOnlineAgentNames) {
      this.#listCanonicalOnlineAgentNames = options.listCanonicalOnlineAgentNames
    } else if (options.workspaceKey) {
      const presence = new AgentRelay({ workspaceKey: options.workspaceKey }).messaging.agents
      this.#listCanonicalOnlineAgentNames = async () => (await presence.presence())
        .filter((agent) => agent.status === 'online')
        .map((agent) => agent.agentName)
    }
    this.#now = options.now ?? Date.now
    this.#isProcessAlive = options.isProcessAlive ?? isProcessAlive
    this.#resumeCapability = options.resumeCapability ?? 'spawn:codex'
    this.#logger = options.logger ? normalizeLogger(options.logger) : undefined
    this.#resolveAgentRelayMcpCommand = options.resolveAgentRelayMcpCommand ?? resolveAgentRelayMcpCommand
    this.#previewManager = options.previewManager ?? (options.previewConfig
      ? new TailscalePreviewManager({ config: options.previewConfig })
      : undefined)
    this.#connect = options.connect ?? ((connectOptions) => HarnessDriverClient.connect(connectOptions))
    this.#client = options.client ?? this.#connect({ cwd: options.cwd, connectionPath: options.connectionPath })
    // One read at construction so a later failure can tell "the file still
    // names the broker I am talking to" (nothing to do) apart from "the file
    // now names a different one" (rebind — reconnect). Reading it here keeps
    // the successful path free of any file access at all.
    this.#connectedBroker = this.#readConnectionFile()?.fingerprint
    this.#ownsBroker = options.ownsBroker ?? false
    this.#ownedBrokerAgentExitTimeoutMs = options.ownedBrokerAgentExitTimeoutMs ?? OWNED_BROKER_AGENT_EXIT_TIMEOUT_MS
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
    }, input.identityKey)
    const exitSequenceAtSpawnStart = this.#trackAgentStart(input.name)
    this.#tracked.set(input.name, { invocationId: input.invocationId })
    let handle: SpawnedHandleLike
    try {
      handle = await this.#callBroker('spawnPty', (client) => client.spawnPty(spawnInput))
    } catch (error) {
      this.#trackAgentExit(input.name)
      throw error
    }
    this.#reconcileTrackedAgentName(input.name, handle.name, exitSequenceAtSpawnStart)
    // A fresh broker's event stream can miss the first worker_ready edge while
    // it is still connecting. The successful spawn response is the broker's
    // authoritative registration signal, so feed it through the same bounded
    // re-send path.
    this.#markAgentReady(handle.name)

    return spawnResultFrom(handle)
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
    assertSelfNode(input.node)
    this.#ensureEventSubscription()
    this.#readyAgentNames.delete(input.name ?? input.sessionRef)

    const spawnInput = this.#withAgentRelayMcpHarness({
      name: input.name ?? input.sessionRef,
      // followups [fleet→W6]: W6 owns resume-vs-respawn and passes the per-agent capability.
      cli: capabilityCli[input.capability ?? this.#resumeCapability],
      cwd: input.clonePath ?? this.#cwd,
      continueFrom: input.sessionRef,
      task: input.task,
    }, input.identityKey)
    const requestedName = input.name ?? input.sessionRef
    const exitSequenceAtSpawnStart = this.#trackAgentStart(requestedName)
    this.#tracked.set(requestedName, {})
    let handle: SpawnedHandleLike
    try {
      handle = await this.#callBroker('spawnPty', (client) => client.spawnPty(spawnInput))
    } catch (error) {
      this.#trackAgentExit(requestedName)
      throw error
    }
    this.#reconcileTrackedAgentName(requestedName, handle.name, exitSequenceAtSpawnStart)

    return { ...spawnResultFrom(handle), sessionRef: sessionRefFrom(handle) ?? input.sessionRef }
  }

  #withAgentRelayMcpHarness(input: SpawnPtyInput, identityKey?: string): SpawnPtyInput {
    if (input.harnessConfig || (input.cli !== 'codex' && input.cli !== 'claude')) {
      return input
    }

    const command = this.#resolveAgentRelayMcpCommand()
    if (!command) {
      if (identityKey) {
        throw new Error(`Cannot spawn ${input.name}: agent-relay MCP is unavailable, so its identity proof cannot be installed`)
      }
      this.#logger?.warn?.('[factory-sdk] agent-relay MCP command not found; spawning without MCP injection', {
        agent: input.name,
        cli: input.cli,
      })
      return input
    }

    return {
      ...input,
      harnessConfig: buildRelayMcpHarnessConfig(input, command, this.#workspaceKey, identityKey),
    }
  }

  async release(name: string, reason?: string): Promise<void> {
    try {
      await this.#releaseWithRetry(name, reason)
    } finally {
      this.#readyAgentNames.delete(name)
      // An explicit release attempt is terminal for this client's lifecycle
      // bookkeeping even if the broker reports an error. Factory.stop() catches
      // that error and dispose() must not then wait for the full agent timeout.
      this.#trackAgentExit(name)
    }
  }

  markAgentTerminal(name: string, reason?: string): void {
    this.#readyAgentNames.delete(name)
    this.#rememberAgentExit(name)
    this.#trackAgentExit(name)
    this.#logger?.debug?.('[factory-sdk] marked spawned agent terminal', { name, reason })
  }

  async #releaseWithRetry(name: string, reason?: string): Promise<void> {
    for (let attempt = 1; attempt <= RELEASE_RETRY_MAX_ATTEMPTS; attempt += 1) {
      try {
        await this.#callBroker('release', (client) => client.release(name, reason))
      } catch (error) {
        if (attempt >= RELEASE_RETRY_MAX_ATTEMPTS || !isRetryableReleaseError(error)) {
          throw error
        }
        this.#logger?.warn?.('[factory-sdk] release rejected as retryable; retrying', {
          name,
          attempt,
          maxAttempts: RELEASE_RETRY_MAX_ATTEMPTS,
          error,
        })
        await sleep(RELEASE_RETRY_BACKOFF_MS * attempt)
        continue
      }

      // A successful HTTP acknowledgement is not sufficient proof that the
      // broker name is reusable. Under concurrent startup reconciliation the
      // worker can disappear while a stale inventory row remains, making an
      // immediate same-name resume fail with `agent already exists`. Verify the
      // raw Harness Driver inventory and repeat the public release until the
      // broker itself no longer advertises the name.
      let retained: boolean
      try {
        const agents = await this.#callBroker('listAgents', (client) => client.listAgents(), { retry: true })
        if (!Array.isArray(agents)) {
          throw new TypeError('Expected Harness Driver listAgents() to return an array')
        }
        retained = agents.some((agent) => agent?.name === name)
      } catch (error) {
        if (attempt >= RELEASE_RETRY_MAX_ATTEMPTS) throw error
        this.#logger?.warn?.('[factory-sdk] unable to verify released broker name; retrying release', {
          name,
          attempt,
          maxAttempts: RELEASE_RETRY_MAX_ATTEMPTS,
          error,
        })
        await sleep(RELEASE_RETRY_BACKOFF_MS * attempt)
        continue
      }
      if (!retained) return

      const retainedError = Object.assign(
        new Error(`Broker still reports agent ${name} after a successful release acknowledgement`),
        { code: 'release_not_observed', retryable: true },
      )
      if (attempt >= RELEASE_RETRY_MAX_ATTEMPTS) throw retainedError
      this.#logger?.warn?.('[factory-sdk] released broker name is still present; retrying release', {
        name,
        attempt,
        maxAttempts: RELEASE_RETRY_MAX_ATTEMPTS,
        error: retainedError,
      })
      await sleep(RELEASE_RETRY_BACKOFF_MS * attempt)
    }
  }

  async roster(): Promise<RosterEntry> {
    const agents = await this.#listLiveAgents()
    return {
      agents: agents.map((agent) => ({ name: agent.name })),
      nodes: [{
        name: 'self',
        capabilities: [
          'spawn:claude',
          'spawn:codex',
          'workflow:run',
          ...(this.#previewManager ? ['preview:tailscale-serve' as const] : []),
        ],
        live: true,
      }],
    }
  }

  async discoverTeammates(query: TeammateQuery): Promise<TeammateAgent[]> {
    if (!this.#teammateDirectory) {
      throw new Error('InternalFleetClient teammate discovery requires a directory or Relay workspace key')
    }
    return await this.#teammateDirectory.discover(query)
  }

  async createPreview(input: PreviewStartInput): Promise<PreviewReference> {
    assertSelfNode(input.node)
    if (!this.#previewManager) throw new Error('Tailscale preview provider is not configured')
    return await this.#previewManager.start(input)
  }

  async removePreview(preview: PreviewReference): Promise<boolean> {
    if (!this.#previewManager) throw new Error('Tailscale preview provider is not configured')
    return await this.#previewManager.remove(preview)
  }

  async reapPreviews(input: PreviewSweepInput): Promise<PreviewSweepResult> {
    if (!this.#previewManager) return { reaped: [], skipped: [] }
    return await this.#previewManager.sweep(input)
  }

  trackedAgents(): ReadonlyMap<string, FleetTrackedAgent> {
    return this.#tracked
  }

  hydrateTracked(agents: Array<{ name: string; invocationId?: string; node?: string }>): void {
    this.#ensureEventSubscription()
    for (const agent of agents) {
      if (this.#tracked.has(agent.name)) continue
      this.#clearAgentExitLatch(agent.name)
      this.#tracked.set(agent.name, {
        invocationId: agent.invocationId,
        node: agent.node,
      })
      this.#activeSpawnedAgentNames.add(agent.name)
    }
  }

  async reconcileTrackedAgents(): Promise<void> {
    this.#ensureEventSubscription()
    const online = new Set((await this.#listLiveAgents()).map((agent) => agent.name))
    for (const name of [...this.#tracked.keys()]) {
      if (online.has(name)) continue
      this.#emitAgentExit(name, 'reconciled-missing', {
        key: `reconciled-missing:${name}`,
        hasStableId: true,
      })
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
        const agent = (await this.#callBroker('listAgents', (client) => client.listAgents(), { retry: true }))
          .find((candidate) => candidate.name === name)
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

  /**
   * Run a broker call, and recover from a broker that rebound its ephemeral
   * port underneath us.
   *
   * The broker binds `AGENT_RELAY_BROKER_PORT=0`, so every restart picks a new
   * port and rewrites `connection.json`. A daemon that captured the URL at boot
   * would otherwise call the dead port for the rest of its life.
   *
   * Cost: nothing is read while calls succeed. `connection.json` is consulted
   * only after a call has already failed, and only to answer one question —
   * does the file still name the broker we are attached to? If it does, the
   * broker is simply down and the error propagates untouched; we do not
   * reconnect, and repeated failures cannot turn into a reconnect loop.
   *
   * `retry` is for reads only. A write that fails may still have been accepted
   * by the broker that vanished, so those are never replayed — the transport is
   * repaired and the error is surfaced for the caller to retry deliberately.
   */
  async #callBroker<T>(
    operation: string,
    call: (client: HarnessDriverClientLike) => Promise<T>,
    options?: { retry?: boolean },
  ): Promise<T> {
    const attemptedClient = this.#client
    const attemptedBaseUrl = attemptedClient.baseUrl
    try {
      return await call(attemptedClient)
    } catch (error) {
      // Only one of several in-flight calls wins the reconnect. The rest fail
      // on the client it just retired and would find the file already matching
      // the new broker — so also treat "the client I used has since been
      // replaced" as a reconnect, or concurrent rosters keep surfacing stale
      // errors from a broker that no longer exists.
      // The `#disposed` guard mirrors the one in #reconnectIfBrokerChanged:
      // once dispose() has begun we stop touching the broker, so a call that
      // fails on a client some earlier reconnect retired is not retried.
      const reconnected = this.#reconnectIfBrokerChanged(error, operation)
        || (!this.#disposed && this.#client !== attemptedClient)
      if (!reconnected || options?.retry !== true) {
        throw attributeBrokerError(error, attemptedBaseUrl)
      }
      // Snapshot the retry's client too: yet another caller can reconnect while
      // this retry is in flight, and attributing the failure to whatever
      // `#client` happens to be by then would name a broker that never saw the
      // request — the exact mis-diagnosis this attribution exists to prevent.
      const retryClient = this.#client
      try {
        return await call(retryClient)
      } catch (retryError) {
        throw attributeBrokerError(retryError, retryClient.baseUrl)
      }
    }
  }

  /**
   * Reconnect only when the connection file names a broker other than the one
   * we are attached to. That single condition is what keeps this from firing on
   * a broker that is merely down: nothing rewrites `connection.json` in that
   * case, so there is no new address to move to and the failure stands.
   */
  #reconnectIfBrokerChanged(error: unknown, operation: string): boolean {
    if (this.#disposed) return false
    // A broker that answered is reachable, so its rejection is not a routing
    // problem. The exception is an auth rejection: a rebound broker mints a new
    // api key, and if it happens to land on the same port a stale key is the
    // only symptom we would see.
    if (!isBrokerTransportError(error) && !isBrokerAuthError(error)) return false

    const current = this.#readConnectionFile()
    if (!current || current.fingerprint === this.#connectedBroker) return false

    // We own a broker only when we spawned it, and dispose() is responsible for
    // shutting it down. If that process is still alive, the file naming someone
    // else is not a cue to abandon it — switching would orphan it. Only move on
    // once the broker we started is actually gone.
    const ownedBrokerPid = this.#client.brokerPid
    if (this.#ownsBroker
      && typeof ownedBrokerPid === 'number'
      && ownedBrokerPid > 0
      && this.#isProcessAlive(ownedBrokerPid)) {
      return false
    }

    let next: HarnessDriverClientLike
    try {
      next = this.#connect({ cwd: this.#cwd, connectionPath: this.#connectionPath })
    } catch (connectError) {
      // A rewritten file that still will not open (dead pid, corrupt JSON)
      // leaves us no better address. Keep the current client and let the
      // original error propagate.
      this.#logger?.warn?.('[factory-sdk] relay broker connection file changed but reconnect failed', {
        operation,
        connectionPath: this.#connectionPath ?? connectionPathForCwd(this.#cwd),
        error: connectError,
      })
      return false
    }

    const previous = this.#client
    const previousBaseUrl = previous.baseUrl
    const wasSubscribed = this.#subscribed
    for (const unsubscribe of this.#eventUnsubscribers.splice(0)) {
      try {
        unsubscribe()
      } catch (unsubscribeError) {
        this.#logger?.debug?.('[factory-sdk] failed to detach a listener from the previous broker', unsubscribeError)
      }
    }
    this.#subscribed = false
    try {
      previous.disconnect?.()
    } catch (disconnectError) {
      this.#logger?.debug?.('[factory-sdk] failed to disconnect the previous broker transport', disconnectError)
    }

    this.#client = next
    this.#connectedBroker = current.fingerprint
    // Whatever we just attached to, we did not spawn it — the broker we spawned
    // is the one that went away. Never shut down a broker we do not own.
    this.#ownsBroker = false
    if (wasSubscribed) this.#ensureEventSubscription()

    this.#logger?.warn?.('[factory-sdk] relay broker rebound; reconnected from the refreshed connection file', {
      operation,
      previousBaseUrl,
      baseUrl: current.url,
      brokerPid: current.pid,
    })
    return true
  }

  #readConnectionFile(): { fingerprint: string; url: string; pid: number } | undefined {
    const path = this.#connectionPath ?? connectionPathForCwd(this.#cwd)
    if (!path) return undefined
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf8')) as { url?: unknown; api_key?: unknown; pid?: unknown }
      const { url, api_key: apiKey, pid } = parsed
      if (typeof url !== 'string' || !url) return undefined
      if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return undefined
      // The api key rotates with the broker, so it belongs in the identity: a
      // restart that reuses the same port is still a different broker.
      return { fingerprint: `${url}\u0000${typeof apiKey === 'string' ? apiKey : ''}\u0000${pid}`, url, pid }
    } catch {
      return undefined
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
    await this.#callBroker('sendMessage', (client) => client.sendMessage(messageInputFrom(input)))
  }

  async waitForInjected(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }> {
    this.#ensureEventSubscription()
    const targetWasReady = this.#readyAgentNames.has(input.to)
    const injectedSequenceAtSendStart = this.#injectedByAgent.get(input.to)?.sequence ?? 0
    const result = await this.#callBroker('sendMessage', (client) => client.sendMessage(messageInputFrom(input)))
    // dispose() can land while the send is in flight. It drains the waiters
    // that exist at that moment, so installing a new one afterwards would leave
    // a timer running on a torn-down client and defer the rejection by a full
    // timeout. Reject on the disposal that already happened instead.
    if (this.#disposed) {
      throw new Error('InternalFleetClient disposed before delivery was confirmed')
    }
    const eventId = result.event_id
    const targets = result.targets ?? []

    if (this.#injectedEventIdSet.has(eventId)) {
      return { eventId, targets }
    }
    const injectedDuringSend = this.#injectedByAgent.get(input.to)
    if (injectedDuringSend && injectedDuringSend.sequence > injectedSequenceAtSendStart) {
      return { eventId: injectedDuringSend.eventId, targets }
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
      this.#activeInjectedWaits.add(pending)

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
        return
      }

      // delivery_failed can also arrive before sendMessage resolves. Defer its
      // handling until the logical waiter exists so captured readiness can keep
      // the recovery re-send alive while the failed event id is retired.
      const priorFailure = this.#failedDeliveries.get(eventId)
      if (priorFailure) {
        if (targetWasReady || this.#readyAgentNames.has(input.to)) {
          this.#triggerReadyResend(pending)
        }
        this.#failPendingInjectedEvent(pending, eventId, priorFailure)
      }
    })
  }

  async sendInput(name: string, data: string): Promise<void> {
    await this.#callBroker('sendInput', (client) => client.sendInput(name, data))
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
    return () => {
      this.#agentExitListeners.delete(listener)
    }
  }

  preserveInfrastructureOnDispose(): void {
    this.#ownsBroker = false
  }

  async dispose(): Promise<void> {
    if (this.#disposed) {
      return
    }
    this.#disposed = true

    // A one-shot command returns as soon as task injection is confirmed, while
    // task_exit workers continue editing and opening their PR. If this client
    // cold-started the broker, keep its event stream alive until those workers
    // report exit; otherwise shutdown would terminate them mid-task.
    if (this.#ownsBroker && this.#client.shutdown) {
      await this.#waitForSpawnedAgentsToExit()
    }

    const pending = [...this.#activeInjectedWaits]
    this.#activeInjectedWaits.clear()
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
    this.#agentUsageListeners.clear()
    this.#failedDeliveries.clear()
    this.#failedDeliveryIds.length = 0
    this.#readyAgentNames.clear()
    this.#tracked.clear()
    this.#activeSpawnedAgentNames.clear()
    this.#locallySpawnedAtMs.clear()
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
    const usage = agentUsageFromRuntimeEvent(event)
    if (usage) {
      this.#emitAgentUsage(usage, eventIdentity(event))
      return
    }

    if (event.kind === 'worker_ready') {
      this.#markAgentReady(event.name)
      return
    }

    if (event.kind === 'delivery_injected') {
      this.#resolveInjected(event.event_id, event.name)
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

  #markAgentReady(name: string): void {
    this.#readyAgentNames.add(name)
    for (const pending of new Set(this.#pendingInjected.values())) {
      if (pending.input.to === name) {
        this.#triggerReadyResend(pending)
      }
    }
  }

  #resolveInjected(eventId: string, name?: string): void {
    rememberRecent(eventId, this.#injectedEventIds, this.#injectedEventIdSet)
    if (name) {
      const previous = this.#injectedByAgent.get(name)
      this.#injectedByAgent.set(name, {
        sequence: (previous?.sequence ?? 0) + 1,
        eventId,
      })
    }

    const pending = this.#pendingInjected.get(eventId)
    if (pending) {
      this.#resolvePendingInjected(pending, eventId)
      return
    }

    // The broker's delivery_injected event id belongs to its own
    // delivery-tracking id space (a snowflake/`init_…` id) and does NOT equal
    // the id sendMessage returned for our send — that is an inbound-request
    // correlation id (`http_…`) the broker never re-emits on delivery events.
    // So matching purely by event id can never confirm a real cold-start
    // injection; the delivery lands and is read, yet the waiter times out.
    // Correlate by the delivery target name instead: a delivery_injected for
    // the agent we are waiting on confirms our injection reached its terminal.
    // Waits are serialized per target (the orchestrator awaits each injection
    // before the next), so at most one active waiter matches a given name.
    if (name) {
      const waiter = this.#activeInjectedWaitFor(name)
      if (waiter) {
        this.#resolvePendingInjected(waiter, eventId)
      }
    }
  }

  #activeInjectedWaitFor(name: string): PendingInjectedWait | undefined {
    for (const waiter of this.#activeInjectedWaits) {
      if (!waiter.settled && waiter.input.to === name) {
        return waiter
      }
    }
    return undefined
  }

  #resolvePendingInjected(pending: PendingInjectedWait, eventId: string): void {
    if (pending.settled) return
    pending.settled = true
    this.#activeInjectedWaits.delete(pending)
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
    this.#activeInjectedWaits.delete(pending)
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
      const result = await this.#callBroker('sendMessage', (client) => client.sendMessage(messageInputFrom(pending.input)))
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
      } else {
        this.#logger?.warn?.(
          '[factory-sdk] readiness-triggered resend failed, but original event is still active',
          error,
        )
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

  #emitAgentUsage(usage: AgentUsage, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) return
    for (const listener of this.#agentUsageListeners) {
      try {
        void Promise.resolve(listener({ ...usage })).catch((error) => {
          this.#logger?.warn?.('[factory-sdk] agent usage listener rejected an update', {
            name: usage.name,
            error,
          })
        })
      } catch (error) {
        this.#logger?.warn?.('[factory-sdk] agent usage listener rejected an update', {
          name: usage.name,
          error,
        })
      }
    }
  }

  #emitAgentExit(name: string, reason: string | undefined, identity: EventIdentity): void {
    if (this.#rememberEvent(identity)) {
      return
    }

    if (this.#rememberAgentExit(name)) {
      return
    }

    // Deduplicate before draining the tracked lifetime. In particular, an
    // exact replay of an old exit must not finish a newly resumed same-name
    // agent after #trackAgentStart() clears the per-name exit latch.
    this.#trackAgentExit(name)

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
    this.#agentExitSequence += 1
    this.#agentExitSequences.set(name, this.#agentExitSequence)
    return false
  }

  #clearAgentExitLatch(name: string): void {
    this.#exitedAgentNames.delete(name)
  }

  #trackAgentStart(name: string): number {
    const exitSequenceAtStart = this.#agentExitSequence
    this.#clearAgentExitLatch(name)
    this.#activeSpawnedAgentNames.add(name)
    this.#locallySpawnedAtMs.set(name, this.#now())
    return exitSequenceAtStart
  }

  #reconcileTrackedAgentName(requestedName: string, actualName: string, exitSequenceAtSpawnStart: number): void {
    if (requestedName === actualName) return

    this.#activeSpawnedAgentNames.delete(requestedName)
    const locallySpawnedAtMs = this.#locallySpawnedAtMs.get(requestedName)
    this.#locallySpawnedAtMs.delete(requestedName)
    const tracked = this.#tracked.get(requestedName)
    this.#tracked.delete(requestedName)
    const actualExitSequence = this.#agentExitSequences.get(actualName) ?? 0
    const exitedDuringSpawn = this.#exitedAgentNames.has(actualName) && actualExitSequence > exitSequenceAtSpawnStart
    if (!exitedDuringSpawn) {
      // An exit stamp at or before the spawn boundary belongs to an older
      // lifetime of the broker-returned name and must not suppress tracking.
      this.#clearAgentExitLatch(actualName)
      this.#activeSpawnedAgentNames.add(actualName)
      if (locallySpawnedAtMs !== undefined) this.#locallySpawnedAtMs.set(actualName, locallySpawnedAtMs)
      if (tracked) this.#tracked.set(actualName, tracked)
    }
    this.#notifyIfActiveAgentsDrained()
  }

  #trackAgentExit(name: string): void {
    this.#tracked.delete(name)
    this.#locallySpawnedAtMs.delete(name)
    if (!this.#activeSpawnedAgentNames.delete(name)) return
    this.#notifyIfActiveAgentsDrained()
  }

  async #listLiveAgents(): Promise<Array<Pick<ListAgent, 'name' | 'cli' | 'pid'>>> {
    const listAgents = () => this.#callBroker('listAgents', (client) => client.listAgents(), { retry: true })
    if (!this.#listCanonicalOnlineAgentNames) return listAgents()

    const [agents, canonicalOnlineAgentNames] = await Promise.all([
      listAgents(),
      this.#listCanonicalOnlineAgentNames(),
    ])
    const online = new Set(canonicalOnlineAgentNames)
    const nowMs = this.#now()
    return agents.filter((agent) => {
      if (online.has(agent.name)) return true
      const spawnedAtMs = this.#locallySpawnedAtMs.get(agent.name)
      if (spawnedAtMs !== undefined && nowMs - spawnedAtMs < CANONICAL_PRESENCE_REGISTRATION_GRACE_MS) {
        return true
      }
      // Relayflows and custom CLIs do not register through the Relay MCP
      // harness. Codex/Claude can also intentionally use the supported
      // no-MCP fallback when that command is unavailable. Preserve any such
      // worker while its broker-reported process is actually alive; only dead
      // MCP-capable rows are safe to classify as historical inventory.
      if (agent.cli !== 'codex' && agent.cli !== 'claude') return true
      return agent.pid !== undefined && this.#isProcessAlive(agent.pid)
    })
  }

  #notifyIfActiveAgentsDrained(): void {
    if (this.#activeSpawnedAgentNames.size > 0) return
    for (const listener of [...this.#activeAgentsDrainedListeners]) {
      listener()
    }
  }

  async #waitForSpawnedAgentsToExit(): Promise<void> {
    if (this.#activeSpawnedAgentNames.size === 0) return

    this.#logger?.info?.(
      '[factory-sdk] waiting for spawned agents to exit before shutting down the owned relay broker',
      {
        agents: [...this.#activeSpawnedAgentNames].sort(),
        timeoutMs: this.#ownedBrokerAgentExitTimeoutMs,
        remainingCount: this.#activeSpawnedAgentNames.size,
      },
    )

    const timedOut = await new Promise<boolean>((resolve) => {
      let settled = false
      const settle = (didTimeOut: boolean) => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        this.#activeAgentsDrainedListeners.delete(onDrained)
        resolve(didTimeOut)
      }
      const onDrained = () => settle(false)
      const timeout = setTimeout(() => settle(true), this.#ownedBrokerAgentExitTimeoutMs)
      this.#activeAgentsDrainedListeners.add(onDrained)

      // Close the gap between the initial size check and listener registration.
      if (this.#activeSpawnedAgentNames.size === 0) onDrained()
    })

    if (timedOut) {
      this.#logger?.warn?.(
        '[factory-sdk] timed out waiting for spawned agents to exit; shutting down the owned relay broker',
        {
          timeoutMs: this.#ownedBrokerAgentExitTimeoutMs,
          agents: [...this.#activeSpawnedAgentNames].sort(),
        },
      )
    }
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

// Connection faults, as they reach us through Node's fetch. `fetch failed` is a
// bare TypeError with the address buried in `cause`, which is exactly why the
// daemon log carried 1469 unattributable failures.
const BROKER_TRANSPORT_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EPIPE',
  'ETIMEDOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_SOCKET',
])

function isBrokerTransportError(error: unknown): boolean {
  // A HarnessDriverProtocolError carries the broker's own HTTP status, which
  // means the broker answered. Reachable is reachable — not a routing fault.
  if (typeof (error as { status?: unknown } | undefined)?.status === 'number') return false
  for (let current: unknown = error, depth = 0; current instanceof Error && depth < 5; depth += 1) {
    const code = (current as { code?: unknown }).code
    if (typeof code === 'string' && BROKER_TRANSPORT_ERROR_CODES.has(code)) return true
    if (current.message === 'fetch failed') return true
    if (current.name === 'TimeoutError' || current.name === 'AbortError') return true
    current = (current as { cause?: unknown }).cause
  }
  return false
}

function isBrokerAuthError(error: unknown): boolean {
  const status = (error as { status?: unknown } | undefined)?.status
  return status === 401 || status === 403
}

/**
 * Name the address a transport failure was aimed at. Mutates the message in
 * place rather than wrapping so callers that duck-type `retryable` / `code` off
 * the error keep working.
 */
function attributeBrokerError(error: unknown, baseUrl: string | undefined): unknown {
  if (!baseUrl || !(error instanceof Error)) return error
  if (!isBrokerTransportError(error) || error.message.includes(baseUrl)) return error
  error.message = `${error.message} (broker at ${baseUrl})`
  return error
}

function connectionPathForCwd(cwd: string | undefined): string | undefined {
  const stateDir = process.env.AGENT_RELAY_STATE_DIR
  if (stateDir) return join(stateDir, 'connection.json')
  return cwd ? join(cwd, '.agentworkforce', 'relay', 'connection.json') : undefined
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const isProcessAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && 'code' in error && error.code === 'ESRCH')
  }
}

function isRetryableReleaseError(error: unknown): boolean {
  // HarnessDriverProtocolError carries a first-class `retryable` flag. Duck-type
  // rather than instanceof so mocks and cross-realm instances match too.
  if (!error || typeof error !== 'object') return false
  return (error as { retryable?: unknown }).retryable === true
}

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
  identityKey?: string,
): NonNullable<SpawnPtyInput['harnessConfig']> {
  const relayEnv = relayMcpEnv(input.name, input.agentToken, workspaceKey, identityKey)
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

function relayMcpEnv(
  agentName: string,
  agentToken?: string,
  workspaceKey?: string,
  identityKey?: string,
): Record<string, string> {
  const env: Record<string, string> = {
    RELAY_AGENT_NAME: agentName,
    RELAY_AGENT_TYPE: 'agent',
    RELAY_STRICT_AGENT_NAME: '1',
    ...(identityKey ? { RELAY_AGENT_IDENTITY_KEY: identityKey } : {}),
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
    ...(input.mode ? { mode: input.mode } : {}),
  }
}

/**
 * Harness Driver versions predating the typed usage event still forward
 * provider events through onEvent. Normalize the stable field aliases here so
 * upgrading the runtime does not require an orchestration rewrite.
 */
function agentUsageFromRuntimeEvent(event: unknown): AgentUsage | undefined {
  const record = runtimeRecord(event)
  const kind = runtimeString(record, 'kind', 'type')
  if (!kind || !['agent_usage', 'usage_updated', 'usage.updated', 'token_usage'].includes(kind)) {
    return undefined
  }
  const agent = runtimeRecord(record?.agent)
  const usage = runtimeRecord(record?.usage)
  const name = runtimeString(record, 'name', 'agent_name', 'agentName')
    ?? runtimeString(agent, 'name', 'agent_name', 'agentName')
  if (!name) return undefined
  const model = runtimeString(record, 'model', 'model_id', 'modelId')
    ?? runtimeString(usage, 'model', 'model_id', 'modelId')
  return {
    name,
    ...(model ? { model } : {}),
    inputTokens: runtimeTokenCount(
      runtimeValue(record, 'inputTokens', 'input_tokens', 'promptTokens', 'prompt_tokens')
      ?? runtimeValue(usage, 'inputTokens', 'input_tokens', 'promptTokens', 'prompt_tokens'),
    ),
    outputTokens: runtimeTokenCount(
      runtimeValue(record, 'outputTokens', 'output_tokens', 'completionTokens', 'completion_tokens')
      ?? runtimeValue(usage, 'outputTokens', 'output_tokens', 'completionTokens', 'completion_tokens'),
    ),
  }
}

const runtimeRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const runtimeString = (record: Record<string, unknown> | undefined, ...keys: string[]): string | undefined => {
  for (const key of keys) {
    const value = record?.[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}

const runtimeValue = (record: Record<string, unknown> | undefined, ...keys: string[]): unknown => {
  for (const key of keys) {
    if (record && Object.prototype.hasOwnProperty.call(record, key)) return record[key]
  }
  return undefined
}

const runtimeTokenCount = (value: unknown): number | null =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null

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
