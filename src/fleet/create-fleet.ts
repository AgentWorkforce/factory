import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'
import { RelayFleetClient } from './relay-fleet-client'
import type { Logger } from '../ports/system'
import type { PreviewConfig } from '../config/schema'

export type FleetBackend = 'internal' | 'relay'
export const FACTORY_AGENT_EXIT_TIMEOUT_ENV = 'FACTORY_AGENT_EXIT_TIMEOUT_MS'

export interface CreateFleetOptions {
  backend?: FleetBackend
  cwd?: string
  connectionPath?: string
  previewConfig?: PreviewConfig
  /**
   * Workspace identity the relay backend registers as (config `relay.agentName`).
   * Relay-only; the internal backend has no workspace registration. Undefined
   * leaves `RelayFleetClient`'s own default in place.
   */
  relayAgentName?: string
}

export interface CreateFleetDeps {
  harnessClient?: HarnessDriverClientLike
  /** Proven inbound-stream identity for an injected Harness Driver client. */
  messageStreamScope?: string | object
  // True when harnessClient owns a broker we spawned, so the fleet shuts it down
  // on dispose instead of leaving it running.
  ownsBroker?: boolean
  ownedBrokerAgentExitTimeoutMs?: number
  workspaceKey?: string
  logger?: Logger
  /** Hermetic credential environment for the relay backend (tests). */
  env?: NodeJS.ProcessEnv
}

export function parseOwnedBrokerAgentExitTimeoutMs(value: unknown): number | undefined {
  if (value === undefined || value === null || value === '') return undefined
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined
}

export function resolveOwnedBrokerAgentExitTimeoutMs(
  explicit: number | undefined,
  env: NodeJS.ProcessEnv = process.env,
): number | undefined {
  if (explicit !== undefined) {
    const parsed = parseOwnedBrokerAgentExitTimeoutMs(explicit)
    if (parsed === undefined) {
      throw new Error('ownedBrokerAgentExitTimeoutMs must be a positive integer number of milliseconds')
    }
    return parsed
  }
  // Invalid environment values are treated as unset so InternalFleetClient's
  // bounded 30-minute default remains the final fallback.
  return parseOwnedBrokerAgentExitTimeoutMs(env[FACTORY_AGENT_EXIT_TIMEOUT_ENV])
}

export function createFleet(options: CreateFleetOptions = {}, deps: CreateFleetDeps = {}) {
  const backend = options.backend ?? 'internal'

  if (backend === 'relay') {
    return new RelayFleetClient({
      workspaceKey: deps.workspaceKey,
      agentName: options.relayAgentName,
      env: deps.env,
      log: deps.logger ? (message) => deps.logger?.info?.(message) : undefined,
    })
  }

  return new InternalFleetClient({
    client: deps.harnessClient,
    messageStreamScope: deps.messageStreamScope,
    ownsBroker: deps.ownsBroker,
    ownedBrokerAgentExitTimeoutMs: resolveOwnedBrokerAgentExitTimeoutMs(deps.ownedBrokerAgentExitTimeoutMs),
    workspaceKey: deps.workspaceKey,
    logger: deps.logger,
    cwd: options.cwd,
    connectionPath: options.connectionPath,
    previewConfig: options.previewConfig,
  })
}
