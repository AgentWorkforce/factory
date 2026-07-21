import { HarnessDriverClient } from '@agent-relay/harness-driver'
import { basename, dirname } from 'node:path'

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
  nodeDeliveryTimeoutMs?: number
  nodeDeliveryPollMs?: number
  sleep?: (ms: number) => Promise<void>
}

export interface RelayBrokerHandle {
  client: HarnessDriverClientLike
  // True when we spawned the broker (we own it and must shut it down on
  // dispose). False when we reused a broker that was already running — that one
  // belongs to the operator and must never be killed.
  started: boolean
  workspaceKey?: string
}

const NODE_DELIVERY_TIMEOUT_MS = 45_000
const NODE_DELIVERY_POLL_MS = 250

async function waitForNodeDelivery(
  client: HarnessDriverClientLike,
  options: Pick<EnsureRelayBrokerOptions, 'nodeDeliveryTimeoutMs' | 'nodeDeliveryPollMs' | 'sleep'>,
): Promise<void> {
  if (!client.getStatus) return

  const timeoutMs = options.nodeDeliveryTimeoutMs ?? NODE_DELIVERY_TIMEOUT_MS
  const pollMs = options.nodeDeliveryPollMs ?? NODE_DELIVERY_POLL_MS
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)))
  const deadline = Date.now() + timeoutMs

  for (;;) {
    const status = await client.getStatus()
    // Older brokers do not report node delivery. Preserve compatibility with
    // them; current brokers must confirm their Relaycast node connection before
    // Factory can safely spawn workers whose task delivery depends on it.
    if (!status || !status.node_delivery || status.node_delivery.connected === true) return
    if (Date.now() >= deadline) {
      throw new Error(
        `Relay broker is reachable, but its cloud node delivery did not become ready within ${timeoutMs}ms`,
      )
    }
    await sleep(pollMs)
  }
}

function projectCwdForConnectionPath(cwd: string | undefined, connectionPath: string | undefined): string | undefined {
  if (!connectionPath || basename(connectionPath) !== 'connection.json') return cwd
  const relayDir = dirname(connectionPath)
  const agentWorkforceDir = dirname(relayDir)
  if (basename(relayDir) !== 'relay' || basename(agentWorkforceDir) !== '.agentworkforce') return cwd
  return dirname(agentWorkforceDir)
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

  let connectedClient: HarnessDriverClientLike | undefined
  try {
    connectedClient = connect({ cwd: options.cwd, connectionPath: options.connectionPath })
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
    let client: HarnessDriverClientLike | undefined
    try {
      client = await spawn({
        cwd: projectCwdForConnectionPath(options.cwd, options.connectionPath),
        workspaceKey,
        ...(stateDir ? { binaryArgs: { persist: true, stateDir } } : {}),
      })
      await waitForNodeDelivery(client, options)
      return { client, started: true, workspaceKey }
    } catch (spawnError) {
      try {
        await client?.shutdown?.()
      } catch (shutdownError) {
        options.logger?.warn?.('[factory] failed to shut down the spawned relay broker during cleanup', {
          errorClass: shutdownError instanceof Error ? shutdownError.name : 'Error',
        })
      }
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

  try {
    await waitForNodeDelivery(connectedClient, options)
    options.logger?.info?.('[factory] reusing the relay broker that is already running')
    return { client: connectedClient, started: false, workspaceKey }
  } catch (error) {
    // A reachable broker owns its connection file. Never start a competing
    // broker merely because the existing one has not completed cloud delivery.
    connectedClient.disconnect?.()
    throw error
  }
}
