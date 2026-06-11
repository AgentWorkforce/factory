import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'
import { RelayFleetClient } from './relay-fleet-client'

export type FleetBackend = 'internal' | 'relay'

export interface CreateFleetOptions {
  backend?: FleetBackend
  cwd?: string
  connectionPath?: string
}

export interface CreateFleetDeps {
  harnessClient?: HarnessDriverClientLike
}

export function createFleet(options: CreateFleetOptions = {}, deps: CreateFleetDeps = {}) {
  const backend = options.backend ?? 'internal'

  if (backend === 'relay') {
    return new RelayFleetClient()
  }

  return new InternalFleetClient({
    client: deps.harnessClient,
    cwd: options.cwd,
    connectionPath: options.connectionPath,
  })
}
