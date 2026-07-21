/** Lifecycle states shared by every disposable verification substrate. */
export type EnvironmentStatus =
  | 'provisioning'
  | 'ready'
  | 'failed'
  | 'destroying'
  | 'destroyed'

/**
 * A provisioned, issue-scoped verification environment.
 *
 * `ttl` is a duration in milliseconds. Providers persist an absolute expiry
 * alongside their own resource identity so a restarted reaper can safely
 * reclaim the environment.
 */
export interface Environment {
  id: string
  provider: string
  dispatchNamespace: string
  endpoints: Record<string, string>
  bindings: Record<string, string>
  status: EnvironmentStatus
  createdAt: string
  ttl: number
}

export interface ProvisionEnvironmentSpec {
  customerId: string
  repository: string
  ownerId: string
  ttl?: number
  /** Provider-specific stack data supplied by the descriptor/deployer seam. */
  stack?: unknown
}

/** Swappable provisioning seam used by deployers and verification gates. */
export interface EnvironmentProvider {
  provision(spec: ProvisionEnvironmentSpec): Promise<Environment>
  status(id: string): Promise<EnvironmentStatus>
  endpoints(id: string): Promise<Record<string, string>>
  destroy(id: string): Promise<void>
}
