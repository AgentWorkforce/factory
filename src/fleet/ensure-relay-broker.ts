import { HarnessDriverClient } from '@agent-relay/harness-driver'

import type { Logger } from '../ports/system'
import type { HarnessDriverClientLike } from './internal-fleet-client'
import { resolveRelayWorkspaceKey } from './relay-workspace-key'

export interface EnsureRelayBrokerOptions {
  cwd?: string
  connectionPath?: string
  // When false, never start a broker — surface the connect error instead. This
  // lets callers opt back into strict reuse-only behavior.
  autoStart?: boolean
  // Workspace key the spawned broker uses to JOIN the existing workspace instead
  // of creating a new (colliding) one. Defaults to RELAY_WORKSPACE_KEY /
  // AGENT_RELAY_WORKSPACE_KEY / RELAY_API_KEY from the env.
  workspaceKey?: string
  logger?: Logger
  // Seams for tests so they never connect to or spawn a real broker.
  connect?: (options: { cwd?: string; connectionPath?: string }) => HarnessDriverClientLike
  spawn?: (options: {
    cwd?: string
    workspaceKey?: string
    binaryArgs?: { persist?: boolean; stateDir?: string }
  }) => Promise<HarnessDriverClientLike>
  env?: NodeJS.ProcessEnv
  resolveWorkspaceKey?: (env: NodeJS.ProcessEnv) => string | undefined
}

export interface RelayBrokerHandle {
  client: HarnessDriverClientLike
  // True when we spawned the broker (we own it and must shut it down on
  // dispose). False when we reused a broker that was already running — that one
  // belongs to the operator and must never be killed.
  started: boolean
  workspaceKey?: string
}

// Resolve the relay broker for the internal fleet backend: reuse the broker that
// is already running for this workspace (the operator's Pear broker) and only
// start a fresh one when none is reachable. We connect first and spawn on
// failure — we NEVER kill an existing broker (that is what made `agent-relay up`
// dangerous). With no running broker, `HarnessDriverClient.connect` throws and we
// fall through to spawning one whose broker writes connection.json under
// `{cwd}/.agentworkforce/relay/`.
export async function ensureRelayBroker(options: EnsureRelayBrokerOptions = {}): Promise<RelayBrokerHandle> {
  const connect = options.connect ?? ((opts) => HarnessDriverClient.connect(opts))
  const spawn = options.spawn ?? ((opts) => HarnessDriverClient.spawn(opts))
  const env = options.env ?? process.env
  const stateDir = env.AGENT_RELAY_STATE_DIR?.trim() || undefined
  const workspaceKey = resolveRelayWorkspaceKey({
    workspaceKey: options.workspaceKey,
    env,
    activeWorkspaceKey: options.resolveWorkspaceKey,
  })

  try {
    const client = connect({ cwd: options.cwd, connectionPath: options.connectionPath })
    options.logger?.info?.('[factory] reusing the relay broker that is already running')
    return { client, started: false, workspaceKey }
  } catch (error) {
    if (options.autoStart === false) {
      throw error
    }
    // Spawn a broker. It must JOIN the existing workspace, not create a new one:
    // a keyless `init` tries to create a workspace and collides ("failed to
    // initialize relaycast session: insert into workspaces"). The workspace key
    // (rk_live_…) makes the broker join. Pear injects it at spawn; standalone the
    // operator supplies it via RELAY_WORKSPACE_KEY.
    options.logger?.info?.('[factory] no relay broker running; starting one', {
      reason: error instanceof Error ? error.message : String(error),
      joiningWorkspace: Boolean(workspaceKey),
    })
    try {
      const client = await spawn({
        cwd: options.cwd,
        workspaceKey,
        ...(stateDir ? { binaryArgs: { persist: true, stateDir } } : {}),
      })
      return { client, started: true, workspaceKey }
    } catch (spawnError) {
      if (!workspaceKey) {
        throw new Error(
          'Failed to start a relay broker and no workspace key was available to join the existing workspace. ' +
          'Set RELAY_WORKSPACE_KEY (your rk_live_… workspace key) so the broker can JOIN your workspace, ' +
          'or start a broker first. ' +
          `Underlying error: ${spawnError instanceof Error ? spawnError.message : String(spawnError)}`,
          { cause: spawnError },
        )
      }
      throw spawnError
    }
  }
}
