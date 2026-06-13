export type Capability = 'spawn:codex' | 'spawn:claude'
export type RestartPolicy = import('@agent-relay/harness-driver').SpawnPtyInput['restartPolicy']

export interface SpawnInput {
  name: string
  capability: Capability
  node?: 'self' | string
  task?: string
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
  onAgentExit(listener: (name: string, reason?: string) => void): () => void
  dispose(): Promise<void>
}

export type AgentSpec = {
  name: string
  role: 'implementer' | 'reviewer'
  capability: Capability
  model?: string
  task: string
  repo: string
  clonePath?: string
  channel?: string
  node?: 'self' | string
  sessionRef?: string
  invocationId?: string
  restartPolicy?: RestartPolicy
}
