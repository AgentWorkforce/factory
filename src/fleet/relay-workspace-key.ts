import { activeWorkspaceKey } from '@agent-relay/cloud'

export interface ResolveRelayWorkspaceKeyOptions {
  workspaceKey?: string
  env?: NodeJS.ProcessEnv
  activeWorkspaceKey?: (env: NodeJS.ProcessEnv) => string | undefined
}

export function resolveRelayWorkspaceKey(options: ResolveRelayWorkspaceKeyOptions = {}): string | undefined {
  const env = options.env ?? process.env
  return nonEmpty(options.workspaceKey)
    ?? nonEmpty(env.RELAY_WORKSPACE_KEY)
    ?? nonEmpty(env.AGENT_RELAY_WORKSPACE_KEY)
    ?? relayWorkspaceKeyFromApiKey(env.RELAY_API_KEY)
    ?? nonEmpty((options.activeWorkspaceKey ?? activeWorkspaceKey)(env))
}

export function relayWorkspaceKeyFromApiKey(value: string | undefined): string | undefined {
  const trimmed = nonEmpty(value)
  return trimmed?.startsWith('rk_live_') ? trimmed : undefined
}

export interface ResolveRelayAgentTokenOptions {
  agentToken?: string
  env?: NodeJS.ProcessEnv
}

// Only at_live_ tokens are agent identities; node (nt_live_) and observer
// (ot_live_) tokens must never be treated as one.
export function resolveRelayAgentToken(options: ResolveRelayAgentTokenOptions = {}): string | undefined {
  const env = options.env ?? process.env
  const candidate = nonEmpty(options.agentToken) ?? nonEmpty(env.RELAY_AGENT_TOKEN)
  return candidate?.startsWith('at_live_') ? candidate : undefined
}

function nonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed || undefined
}
