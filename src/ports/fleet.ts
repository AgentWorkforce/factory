// `workflow:run` is a fleet node capability. The node-side Phase 4 handler
// invokes the Relayflows SDK in that node's repo checkout; the
// factory only emits the workflow path and inputs through the relay fleet.
export type Capability = 'spawn:codex' | 'spawn:claude' | 'workflow:run'
export type RestartPolicy = import('@agent-relay/harness-driver').SpawnPtyInput['restartPolicy']

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
}

export interface RosterEntry {
  agents: Array<{ name: string }>
  nodes: Array<{ name: string; capabilities: Capability[]; live: boolean }>
}

export type AgentPidResolution =
  | { status: 'found'; pid: number }
  | { status: 'missing' }
  | { status: 'unresolved' }

export type SendInput = { to: string; text: string; from?: string; data?: Record<string, unknown> }
export type AgentMessage = { from: string; target: string; body: string; threadId?: string; eventId?: string }

export interface FleetClient {
  spawn(input: SpawnInput): Promise<SpawnResult>
  resume(input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult>
  release(name: string, reason?: string): Promise<void>
  roster(): Promise<RosterEntry>
  resolveAgentPid?(name: string): Promise<AgentPidResolution>
  protectedPids?(): Promise<number[]>
  sendMessage(input: SendInput): Promise<void>
  waitForInjected?(input: SendInput, opts?: { timeoutMs?: number }): Promise<{ eventId: string; targets: string[] }>
  sendInput?(name: string, data: string): Promise<void>
  onDeliveryFailed?(listener: (info: { to: string; msgId?: string; reason?: string }) => void): () => void
  onAgentMessage?(listener: (message: AgentMessage) => void): () => void
  onAgentExit(listener: (name: string, reason?: string) => void): () => void
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
  clonePath?: string
  channel?: string
  node?: 'self' | string
  sessionRef?: string
  invocationId?: string
  restartPolicy?: RestartPolicy
}
