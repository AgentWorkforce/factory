// `workflow:run` is a fleet node capability. The node-side Phase 4 handler
// invokes the Relayflows SDK in that node's repo checkout; the
// factory only emits the workflow path and inputs through the relay fleet.
export type Capability = 'spawn:codex' | 'spawn:claude' | 'workflow:run'
export type PreviewCapability = 'preview:tailscale-serve'
export type NodeCapability = Capability | PreviewCapability
export type RestartPolicy = import('@agent-relay/harness-driver').SpawnPtyInput['restartPolicy']
export type A2aSkill = import('@relaycast/a2a').A2aSkill

export type PreviewReference = {
  id: string
  provider: 'tailscale-serve'
  /** Factory workspace namespace that fences startup sweeps. */
  namespace: string
  /** Stable dispatch identity used by guarded teardown and orphan sweeps. */
  owner: string
  service: string
  repo: string
  url: string
  /** Configured service port before node-local collision-free allocation. */
  configuredTargetPort?: number
  targetPort: number
  httpsPort: number
  access: 'tailnet'
  lifetime: 'issue'
  createdAt: string
  /** Foreground command owned by the node for the issue lifetime. */
  startCommand: string
  /** Exact identity used to recover and terminate the node-owned command. */
  process?: PreviewProcessReference
  node?: string
}

export type PreviewProcessReference = {
  pid: number
  startTime: string
  cmdline: string
  cwd: string
  marker: string
}

export type PreviewStartInput = {
  namespace: string
  owner: string
  issueKey: string
  service: string
  repo: string
  targetPort: number
  preferredHttpsPort?: number
  startCommand: string
  /** Advertised checkout in which the node starts the preview command. */
  checkoutPath: string
  node?: 'self' | string
}

export type PreviewSweepInput = {
  namespace: string
  activeOwners: string[]
  /** Exact persisted references that remain authoritative for active owners. */
  activePreviewIds?: string[]
}

export type PreviewSweepResult = {
  reaped: PreviewReference[]
  skipped: Array<{ id?: string; reason: string; node?: string }>
}

export interface SpawnInput {
  name: string
  capability: Capability
  /** Stable proof that this deterministic agent name belongs to the same logical work unit. */
  identityKey?: string
  node?: 'self' | string
  repo?: string
  clonePath?: string
  task?: string
  workflow?: string
  inputs?: Record<string, unknown>
  model?: string
  cwd?: string
  sessionRef?: string
  invocationId?: string
  restartPolicy?: RestartPolicy
  channel?: string
}

export interface SpawnResult {
  name: string
  sessionRef?: string
  pid?: number
  pids?: number[]
  /** Placement node that owns this process. Local PID operations are unsafe when this is remote. */
  node?: string
  /** Explicit process locality; never infer whether a PID is local from a node name. */
  locality?: 'local' | 'remote'
  /**
   * Provider sandbox this agent was placed into, when the placement was
   * JIT-provisioned. Present only on the `provisionSandbox` path, and it is
   * the only handle factory has on the box holding the agent's clone — the
   * node name is not one, because a sandbox is not labelled with the name the
   * enrollment generates for it. Publishing the agent's commits needs this
   * (see `ports/sandbox-push`), so a placement that omits it can be dispatched
   * but not published from.
   */
  sandboxId?: string
}

export interface RosterEntry {
  agents: Array<{ name: string; node?: string }>
  nodes: Array<{ name: string; capabilities: NodeCapability[]; live: boolean }>
}

export interface TeammateQuery {
  /** Exact A2A skill id/name to match. */
  skill?: string
  /** Exact agent- or skill-level tag to match. */
  tag?: string
  /** Free-text directory query. */
  q?: string
}

export interface TeammateAgent {
  /** Directory identity shown to workers. */
  name: string
  /** Human-readable card description, when the directory provides one. */
  description?: string
  /** Canonical A2A skill records from the agent card. */
  skills: A2aSkill[]
  /** A2A endpoint advertised by the directory. */
  url: string
  kind: 'native' | 'a2a'
  /** Relay/A2A target used for the discover -> engage hop. */
  address: string
  tags: string[]
  status?: string
  certification?: string
}

export type AgentPidResolution =
  | { status: 'found'; pid: number }
  | { status: 'missing' }
  | { status: 'unresolved' }

export type SendInput = {
  to: string
  text: string
  from?: string
  data?: Record<string, unknown>
  mode?: 'wait' | 'steer'
}

/**
 * Positive transport evidence that a correlated message cannot be delivered.
 * Unlike a delivery-confirmation timeout, this makes an uncorrelated retry safe.
 */
export class FleetDeliveryRejectedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'FleetDeliveryRejectedError'
  }
}

export type AgentMessage = { from: string; target: string; body: string; threadId?: string; eventId?: string }
export type AgentLifecycleSignal = {
  name: string
  kind: 'completed' | 'ready' | 'blocked'
  issueKey?: string
  role?: AgentSpec['role']
  question?: string
  invocationId?: string
}
/** Latest cumulative runtime totals for one spawned agent and model. */
export type AgentUsage = {
  name: string
  /** Runtime-reported model wins over the spawn request when the provider supplies it. */
  model?: string
  /** Null means the runtime did not report that token count; Factory never infers it. */
  inputTokens: number | null
  /** Null means the runtime did not report that token count; Factory never infers it. */
  outputTokens: number | null
}
export type FleetTrackedAgent = { invocationId?: string; node?: string }

/**
 * Lifecycle of the fleet event socket: the dial that makes this agent `online`.
 * `dialed` means the SDK accepted `connect()` but Factory has not observed a
 * stream event yet. It is unconfirmed, not necessarily failed: a live socket in
 * a silent workspace can remain `dialed` until its first event arrives.
 */
export type FleetConnectState = 'never-attempted' | 'connecting' | 'dialed' | 'connected' | 'failed'

/**
 * Why the fleet event subscription is (or is not) live.
 *
 * This exists because the dial had NO status anywhere. `#ensureEventSubscription`
 * starts `#subscribeEvents()` with `void ... .catch()` and reports a rejection by
 * calling `#log` only, so a fleet client that registered an agent and then failed
 * to connect looked identical to a healthy one on every surface. Callers read
 * `eventListener` and saw `subscribed` -- but that is the orchestrator's ISSUE
 * subscription, a different subsystem entirely, which is how a broken fleet
 * socket stayed invisible while every instrument reported healthy.
 */
export interface FleetConnectStatus {
  state: FleetConnectState
  /** How many times a subscription has been started, including the current one. */
  attempts: number
  lastAttemptAtMs?: number
  /** The SDK accepted `connect()`; this does not prove that the socket opened. */
  lastDialedAtMs?: number
  /** First event that proved this client received data from the stream. */
  firstEventAtMs?: number
  lastConnectedAtMs?: number
  lastFailureAtMs?: number
  /** Reduced to `Name (CODE)`; never a raw transport message. */
  lastError?: string
}

export interface FleetClient {
  /** Backend-wide placement locality, used when recovering a spawn ack crash gap. */
  readonly placementLocality?: 'local' | 'remote'
  /**
   * The backend can re-adopt spawned workers after an orchestrator restart.
   * Backends that enable this must implement the tracked-agent hydration hooks.
   */
  readonly durableOwnership?: boolean
  /** Durable Relay action agents invoke instead of messaging a named control identity. */
  readonly lifecycleActionName?: string
  spawn(input: SpawnInput): Promise<SpawnResult>
  resume(input: {
    name?: string
    sessionRef: string
    /** Same work-unit proof used for the original spawn. */
    identityKey?: string
    node?: 'self' | string
    capability?: Capability
    repo?: string
    clonePath?: string
    /** Fresh task delivered atomically with the resumed harness spawn. */
    task?: string
  }): Promise<SpawnResult>
  release(name: string, reason?: string): Promise<void>
  roster(): Promise<RosterEntry>
  /** One bounded-poll sample proving a spawned remote identity is broker-visible on its expected host. */
  isAgentRegistered?(input: {
    name: string
    node: string
    capability: Capability
  }): Promise<boolean>
  /** Find addressable teammate agents by their published A2A cards. */
  discoverTeammates(query: TeammateQuery): Promise<TeammateAgent[]>
  /**
   * Whether this backend's event socket is connected, and why not when it is not.
   * Optional: backends with no socket (the internal fleet) simply omit it, and an
   * absent value is reported as such rather than being invented as healthy.
   */
  fleetConnectStatus?(): FleetConnectStatus
  resolveAgentPid?(name: string): Promise<AgentPidResolution>
  protectedPids?(): Promise<number[]>
  sendMessage(input: SendInput): Promise<void>
  /**
   * Identity this backend actually authors sends as, when it cannot honour
   * `SendInput.from`. A teammate replies to that identity, not to the caller's
   * requested `from`, so a reply waiter must match against this instead.
   * Backends that faithfully carry `from` leave it undefined.
   *
   * Async because the real identity may only be knowable after an
   * authentication round trip: a configured name can differ from the one the
   * server actually authenticated, and matching on the pre-auth guess would
   * reject every reply.
   */
  effectiveSender?(): Promise<string | undefined>
  /**
   * Stable, opaque identity of the inbound message stream used by this backend.
   * Distinct client objects that receive the same messages must return the
   * same identity so uncorrelated reply claims are shared across them. Clients
   * for genuinely separate streams must return different identities.
   *
   * The result must remain referentially/string-equal across calls. Backends
   * that omit this method are conservatively scoped to the client object.
   */
  messageStreamIdentity?(): Promise<string | object | undefined>
  waitForInjected?(input: SendInput, opts?: { timeoutMs?: number }): Promise<{
    eventId: string
    targets: string[]
    /** A separately accepted resend can still produce an uncorrelated duplicate reply. */
    duplicateDeliveryPossible?: true
  }>
  sendInput?(name: string, data: string): Promise<void>
  markAgentTerminal?(name: string, reason?: string): void
  onDeliveryFailed?(listener: (info: { to: string; msgId?: string; reason?: string }) => void): () => void
  onAgentMessage?(listener: (message: AgentMessage) => void): () => void
  /**
   * Resolves once the inbound-message transport is genuinely listening.
   * `onAgentMessage` may return before that is true on a backend whose
   * subscription is established asynchronously, so a caller that registers a
   * listener and then sends must await this in between or risk losing a reply
   * that arrives first.
   */
  whenMessagesObservable?(): Promise<void>
  onAgentLifecycleSignal?(listener: (signal: AgentLifecycleSignal) => void | Promise<void>): () => void
  onAgentUsage?(listener: (usage: AgentUsage) => void | Promise<void>): () => void
  onAgentExit(listener: (name: string, reason?: string) => void): () => void
  // Durable backends track spawned-and-not-exited agents so the orchestrator
  // can persist them for crash recovery and re-adopt them after a restart.
  trackedAgents?(): ReadonlyMap<string, FleetTrackedAgent>
  hydrateTracked?(agents: Array<{ name: string; invocationId?: string; node?: string }>): void
  reconcileTrackedAgents?(): Promise<void>
  /** Create a provider-owned, issue-lifetime route on the placement node. */
  createPreview?(input: PreviewStartInput): Promise<PreviewReference>
  /** Remove only the exact provider route described by the reference. */
  removePreview?(preview: PreviewReference): Promise<boolean>
  /** Reap Factory-owned routes whose owner is absent from durable in-flight state. */
  reapPreviews?(input: PreviewSweepInput): Promise<PreviewSweepResult>
  // A successfully spawned fire-and-forget worker may need infrastructure this
  // client cold-started to outlive the invoking CLI process. Backends that own
  // such infrastructure can relinquish cleanup responsibility after spawn.
  preserveInfrastructureOnDispose?(): void
  dispose(): Promise<void>
}

export type AgentSpec = {
  name: string
  role: 'implementer' | 'reviewer' | 'babysitter' | 'workflow'
  /** Principal that initiated the agent session, when supplied by the dispatcher. */
  principal?: string
  /** Compatibility alias for dispatchers that identify the initiating principal as an owner. */
  owner?: string
  capability: Capability
  model?: string
  task: string
  workflow?: string
  inputs?: Record<string, unknown>
  repo: string
  /** Configured shared checkout from which an isolated local worktree was created. */
  baseClonePath?: string
  clonePath?: string
  channel?: string
  node?: 'self' | string
  sessionRef?: string
  invocationId?: string
  restartPolicy?: RestartPolicy
  /** Durable, exact PR ownership for a lazily-spawned babysitter. */
  ownedPullRequest?: {
    repo: string
    number: number
    path?: string
  }
  /** Coalesced metadata-only wake retained until its safe PTY submit completes. */
  pendingPullRequestWake?: {
    repo: string
    number: number
    kinds: string[]
  }
  /** Deterministic pushed branch used by the durable cross-node PR publisher. */
  branch?: string
  /** Existing same-repository PR head authorized for isolated legacy-branch adoption. */
  existingPullRequestBranch?: boolean
  /** Shared live preview owned by the issue lifecycle, not this agent process. */
  preview?: PreviewReference
  /** Set only for scope 'swarm': this implementer's position in the live-collaborating team. */
  swarmRole?: 'lead' | 'worker'
}
