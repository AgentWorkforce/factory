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
  destroy(id: string): Promise<void>
}
