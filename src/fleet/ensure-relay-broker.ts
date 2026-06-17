import { HarnessDriverClient } from '@agent-relay/harness-driver'

import type { Logger } from '../ports/system'
import type { HarnessDriverClientLike } from './internal-fleet-client'

export interface EnsureRelayBrokerOptions {
  cwd?: string
  connectionPath?: string
  // When false, never start a broker — surface the connect error instead. This
  // lets callers opt back into strict reuse-only behavior.
  autoStart?: boolean
  logger?: Logger
  // Seams for tests so they never connect to or spawn a real broker.
  connect?: (options: { cwd?: string; connectionPath?: string }) => HarnessDriverClientLike
  spawn?: (options: { cwd?: string }) => Promise<HarnessDriverClientLike>
}

export interface RelayBrokerHandle {
  client: HarnessDriverClientLike
  // True when we spawned the broker (we own it and must shut it down on
  // dispose). False when we reused a broker that was already running — that one
  // belongs to the operator and must never be killed.
  started: boolean
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

  try {
    const client = connect({ cwd: options.cwd, connectionPath: options.connectionPath })
    options.logger?.info?.('[factory] reusing the relay broker that is already running')
    return { client, started: false }
  } catch (error) {
    if (options.autoStart === false) {
      throw error
    }
    options.logger?.info?.('[factory] no relay broker running; starting one', {
      reason: error instanceof Error ? error.message : String(error),
    })
    const client = await spawn({ cwd: options.cwd })
    return { client, started: true }
  }
}
