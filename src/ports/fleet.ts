// `workflow:run` is a fleet node capability. The node-side Phase 4 handler
// invokes the Relayflows SDK in that node's repo checkout; the
// factory only emits the workflow path and inputs through the relay fleet.
export type Capability = 'spawn:codex' | 'spawn:claude' | 'workflow:run'
export type PreviewCapability = 'preview:tailscale-serve'
export type NodeCapability = Capability | PreviewCapability
export type RestartPolicy = import('@agent-relay/harness-driver').SpawnPtyInput['restartPolicy']

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
}

export interface RosterEntry {
  agents: Array<{ name: string; node?: string }>
  nodes: Array<{ name: string; capabilities: NodeCapability[]; live: boolean }>
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
export type AgentMessage = { from: string; target: string; body: string; threadId?: string; eventId?: string }
export type AgentLifecycleSignal = {
  name: string
  kind: 'completed' | 'ready' | 'blocked'
  issueKey?: string
  role?: AgentSpec['role']
  question?: string
  invocationId?: string
}
export type FleetTrackedAgent = { invocationId?: string; node?: string }

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
    node?: 'self' | string
    capability?: Capability
    repo?: string
    clonePath?: string
    /** Fresh task delivered atomically with the resumed harness spawn. */
    task?: string
  }): Promise<SpawnResult>
  release(name: string, reason?: string): Promise<void>
  roster(): Promise<RosterEntry>
  resolveAgentPid?(name: string): Promise<AgentPidResolution>
  protectedPids?(): Promise<number[]>
  sendMessage(input: SendInput): Promise<void>
  waitForInjected?(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }>
  sendInput?(name: string, data: string): Promise<void>
  markAgentTerminal?(name: string, reason?: string): void
  onDeliveryFailed?(listener: (info: { to: string; msgId?: string; reason?: string }) => void): () => void
  onAgentMessage?(listener: (message: AgentMessage) => void): () => void
  onAgentLifecycleSignal?(listener: (signal: AgentLifecycleSignal) => void | Promise<void>): () => void
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
}
