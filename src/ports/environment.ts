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
