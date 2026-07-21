export interface ProvisionEnvironmentInput {
  runId: string
  repository: string
  namespacePrefix: string
  ttlMs: number
  maxActiveEnvironments: number
  kubeContext?: string
  signal?: AbortSignal
}

export interface VerificationEnvironment {
  id: string
  namespace: string
  endpoints: Record<string, string>
  /** Cluster-routable endpoints used by in-cluster load generators. */
  internalEndpoints: Record<string, string>
  kubeContext?: string
  expiresAt: string
}

export interface DeployManifest {
  path: string
}

export interface DeployReadinessCheck {
  resource: string
  condition: string
  timeoutMs: number
}

export interface DeployEndpoint {
  service: string
  port: number
  scheme: 'http' | 'https'
  path: string
  portForward: boolean
}

export interface DeployEnvironmentInput {
  repositoryPath: string
  manifests: DeployManifest[]
  readiness: DeployReadinessCheck[]
  endpoints: Record<string, DeployEndpoint>
  signal?: AbortSignal
}

/** Namespace-lease seam used by the live verification merge gate. */
export interface VerificationEnvironmentProvider {
  provision(input: ProvisionEnvironmentInput): Promise<VerificationEnvironment>
  deploy(
    environment: VerificationEnvironment,
    input: DeployEnvironmentInput,
  ): Promise<VerificationEnvironment>
  teardown(environment: VerificationEnvironment, options?: { signal?: AbortSignal }): Promise<void>
}

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
