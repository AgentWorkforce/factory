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

export interface VerificationEnvironmentProvider {
  provision(input: ProvisionEnvironmentInput): Promise<VerificationEnvironment>
  deploy(
    environment: VerificationEnvironment,
    input: DeployEnvironmentInput,
  ): Promise<VerificationEnvironment>
  teardown(environment: VerificationEnvironment, options?: { signal?: AbortSignal }): Promise<void>
}

export type EnvironmentStatus =
  | 'provisioning'
  | 'ready'
  | 'failed'
  | 'destroying'
  | 'destroyed'

export interface KubernetesEnvironmentTarget {
  type: 'kubernetes'
  namespace: string
  kubeconfig?: string
  context?: string
}

export interface EnvironmentSpec {
  /** Stable caller-provided id. Providers generate one when it is omitted. */
  id?: string
  ttl?: number
  labels?: Record<string, string>
  bindings?: Record<string, string>
  annotations?: Record<string, string>
  signal?: AbortSignal
}

/**
 * A provisioned, isolated verification target. Provider-specific connection
 * details live in `target`; callers should otherwise treat environments as
 * substrate agnostic.
 */
export interface Environment {
  id: string
  status: EnvironmentStatus
  createdAt: string
  /** Environment lifetime in milliseconds. */
  ttl: number
  endpoints: Record<string, string>
  bindings: Record<string, string>
  target?: KubernetesEnvironmentTarget | { type: string; [key: string]: unknown }
  /** Compatibility fields for providers whose native isolation primitive is named directly. */
  namespace?: string
  dispatchNamespace?: string
}

export interface EnvironmentProvider {
  provision(spec: EnvironmentSpec): Promise<Environment>
  status(id: string): Promise<EnvironmentStatus>
  endpoints(id: string): Promise<Record<string, string>>
  destroy(id: string, options?: { signal?: AbortSignal }): Promise<void>
}
