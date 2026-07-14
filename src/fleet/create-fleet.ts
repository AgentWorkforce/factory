import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'
import { RelayFleetClient } from './relay-fleet-client'
import type { Logger } from '../ports/system'

export type FleetBackend = 'internal' | 'relay'

export interface CreateFleetOptions {
  backend?: FleetBackend
  cwd?: string
  connectionPath?: string
}

export interface CreateFleetDeps {
  harnessClient?: HarnessDriverClientLike
  // True when harnessClient owns a broker we spawned, so the fleet shuts it down
  // on dispose instead of leaving it running.
  ownsBroker?: boolean
  ownedBrokerAgentExitTimeoutMs?: number
  workspaceKey?: string
  logger?: Logger
}

export function createFleet(options: CreateFleetOptions = {}, deps: CreateFleetDeps = {}) {
  const backend = options.backend ?? 'internal'

  if (backend === 'relay') {
    return new RelayFleetClient()
  }

  return new InternalFleetClient({
    client: deps.harnessClient,
    ownsBroker: deps.ownsBroker,
    ownedBrokerAgentExitTimeoutMs: deps.ownedBrokerAgentExitTimeoutMs,
    workspaceKey: deps.workspaceKey,
    logger: deps.logger,
    cwd: options.cwd,
    connectionPath: options.connectionPath,
  })
}
