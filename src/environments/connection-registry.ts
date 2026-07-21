import { z } from 'zod'

export const DEFAULT_MANAGED_FIDELITY_CAVEAT =
  'Managed verification runs on Factory infrastructure; Kubernetes version, CNI, IAM/IRSA, ingress, add-ons, and node types may differ from the customer cluster.'

const secretReferenceSchema = z.string().trim().min(1).max(512).superRefine((value, context) => {
  if (!/^[a-z][a-z0-9+.-]*:\S+$/iu.test(value)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'must be an opaque scheme-prefixed secret-manager reference',
    })
  }
  if (/-----BEGIN|apiVersion\s*:|current-context\s*:|\{\s*"/iu.test(value)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'must be an opaque secret-manager reference, never inline kubeconfig or credential data',
    })
  }
})

const tolerationSchema = z.object({
  key: z.string().min(1).optional(),
  operator: z.enum(['Exists', 'Equal']).optional(),
  value: z.string().optional(),
  effect: z.enum(['NoSchedule', 'PreferNoSchedule', 'NoExecute']).optional(),
  tolerationSeconds: z.number().int().nonnegative().optional(),
}).strict()

export const KubernetesCredentialReferenceSchema = z.object({
  kind: z.enum(['kubeconfig', 'irsa']),
  secretRef: secretReferenceSchema,
}).strict()

export const KubernetesConnectionSchema = z.object({
  id: z.string().trim().min(1).max(63).regex(/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/u),
  target: z.enum(['byoc', 'managed']).default('byoc'),
  customers: z.array(z.string().trim().min(1)).min(1).default(['*']),
  repositories: z.array(z.string().trim().min(1)).min(1).default(['*']),
  credential: KubernetesCredentialReferenceSchema,
  context: z.string().trim().min(1).optional(),
  protectedNamespaces: z.array(z.string().trim().min(1)).default(['default', 'prod', 'production']),
  allowClusterScopedResources: z.boolean().default(false),
  allowedClusterScopedKinds: z.array(z.string().trim().min(1)).default([]),
  nodeSelector: z.record(z.string(), z.string()).default({}),
  tolerations: z.array(tolerationSchema).default([]),
  fidelityCaveat: z.string().trim().min(1).optional(),
}).strict().superRefine((connection, context) => {
  if (connection.target === 'managed' && !connection.fidelityCaveat) {
    connection.fidelityCaveat = DEFAULT_MANAGED_FIDELITY_CAVEAT
  }
  if (!connection.allowClusterScopedResources && connection.allowedClusterScopedKinds.length > 0) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['allowedClusterScopedKinds'],
      message: 'requires allowClusterScopedResources=true',
    })
  }
})

export const KubernetesGuardrailDefaultsSchema = z.object({
  ttlMs: z.number().int().min(60_000).max(7 * 24 * 60 * 60_000).default(60 * 60_000),
  readinessTimeoutMs: z.number().int().min(1_000).max(60 * 60_000).default(10 * 60_000),
  resourceQuota: z.record(z.string(), z.string()).default({
    'requests.cpu': '2',
    'requests.memory': '4Gi',
    'limits.cpu': '4',
    'limits.memory': '8Gi',
    pods: '20',
    'count/services': '20',
    'count/persistentvolumeclaims': '10',
    'requests.storage': '20Gi',
    'services.loadbalancers': '0',
    'services.nodeports': '0',
  }),
  limitRange: z.object({
    default: z.record(z.string(), z.string()).default({ cpu: '500m', memory: '512Mi' }),
    defaultRequest: z.record(z.string(), z.string()).default({ cpu: '50m', memory: '64Mi' }),
    max: z.record(z.string(), z.string()).default({ cpu: '2', memory: '2Gi' }),
  }).default({}),
}).strict().default({})

export const KubernetesEnvironmentConfigSchema = z.object({
  connections: z.array(KubernetesConnectionSchema).default([]),
  defaults: KubernetesGuardrailDefaultsSchema,
}).strict().superRefine((config, context) => {
  const seen = new Set<string>()
  for (const [index, connection] of config.connections.entries()) {
    if (seen.has(connection.id)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['connections', index, 'id'],
        message: `duplicate Kubernetes connection id ${connection.id}`,
      })
    }
    seen.add(connection.id)
  }
}).default({})

export type KubernetesConnection = z.output<typeof KubernetesConnectionSchema>
export type KubernetesConnectionInput = z.input<typeof KubernetesConnectionSchema>
export type KubernetesEnvironmentConfig = z.output<typeof KubernetesEnvironmentConfigSchema>
export type KubernetesGuardrailDefaults = z.output<typeof KubernetesGuardrailDefaultsSchema>

export interface ResolvedKubernetesCredential {
  /** Path to a short-lived/scoped kubeconfig. The credential itself never enters config. */
  kubeconfigPath: string
  /** Optional hermetic process environment for an exec-based kubeconfig (for example AWS IRSA). */
  environment?: NodeJS.ProcessEnv
}

export interface KubernetesCredentialResolver {
  resolve(
    reference: string,
    kind: KubernetesConnection['credential']['kind'],
  ): Promise<ResolvedKubernetesCredential>
}

export interface ResolvedKubernetesConnection extends Omit<KubernetesConnection, 'credential'> {
  credentialKind: KubernetesConnection['credential']['kind']
  kubeconfigPath: string
  environment?: NodeJS.ProcessEnv
}

export interface ResolveKubernetesConnectionInput {
  customerId: string
  repository: string
  target?: KubernetesConnection['target']
}

export type KubernetesConnectionResolution =
  | { id: string; connection: ResolvedKubernetesConnection; error?: never }
  | { id: string; connection?: never; error: unknown }

/** Resolve a kubeconfig path stored in an environment-backed CI secret reference. */
export class EnvironmentKubernetesCredentialResolver implements KubernetesCredentialResolver {
  readonly #env: NodeJS.ProcessEnv

  constructor(env: NodeJS.ProcessEnv = process.env) {
    this.#env = env
  }

  async resolve(reference: string): Promise<ResolvedKubernetesCredential> {
    if (!reference.startsWith('env:')) {
      throw new Error(`Unsupported Kubernetes secret reference ${reference}; configure a secret resolver for this scheme`)
    }
    const variable = reference.slice('env:'.length)
    if (!/^[A-Z_][A-Z0-9_]*$/u.test(variable)) {
      throw new Error(`Invalid environment secret reference ${reference}`)
    }
    const kubeconfigPath = this.#env[variable]
    if (!kubeconfigPath) {
      throw new Error(`Kubernetes credential reference ${reference} is unavailable`)
    }
    return { kubeconfigPath, environment: this.#env }
  }
}

/** Customer/repository-aware registry for BYOC and managed cluster connections. */
export class KubernetesConnectionRegistry {
  readonly config: KubernetesEnvironmentConfig
  readonly #resolver: KubernetesCredentialResolver

  constructor(
    config: KubernetesEnvironmentConfig | z.input<typeof KubernetesEnvironmentConfigSchema>,
    resolver: KubernetesCredentialResolver = new EnvironmentKubernetesCredentialResolver(),
  ) {
    this.config = KubernetesEnvironmentConfigSchema.parse(config)
    this.#resolver = resolver
  }

  async resolve(input: ResolveKubernetesConnectionInput): Promise<ResolvedKubernetesConnection> {
    const target = input.target ?? 'byoc'
    const matches = this.config.connections
      .filter((connection) => connection.target === target)
      .filter((connection) => matchesScope(connection.customers, input.customerId))
      .filter((connection) => matchesScope(connection.repositories, input.repository))
      .map((connection) => ({
        connection,
        specificity: scopeSpecificity(connection.customers, input.customerId) +
          scopeSpecificity(connection.repositories, input.repository),
      }))
      .sort((left, right) => right.specificity - left.specificity || left.connection.id.localeCompare(right.connection.id))

    if (matches.length === 0) {
      throw new Error(
        `No ${target} Kubernetes connection is configured for customer ${input.customerId} and repository ${input.repository}`,
      )
    }
    if (matches.length > 1 && matches[0].specificity === matches[1].specificity) {
      throw new Error(
        `Ambiguous ${target} Kubernetes connections for customer ${input.customerId} and repository ${input.repository}: ` +
        matches.filter((entry) => entry.specificity === matches[0].specificity).map((entry) => entry.connection.id).join(', '),
      )
    }
    return await this.#resolveCredential(matches[0].connection)
  }

  async all(): Promise<ResolvedKubernetesConnection[]> {
    return await Promise.all(this.config.connections.map(async (connection) => await this.#resolveCredential(connection)))
  }

  /** Resolve every configured connection without letting one unavailable secret hide the others. */
  async resolveAll(): Promise<KubernetesConnectionResolution[]> {
    return await Promise.all(this.config.connections.map(async (connection) => {
      try {
        return { id: connection.id, connection: await this.#resolveCredential(connection) }
      } catch (error) {
        return { id: connection.id, error }
      }
    }))
  }

  async #resolveCredential(connection: KubernetesConnection): Promise<ResolvedKubernetesConnection> {
    const credential = await this.#resolver.resolve(connection.credential.secretRef, connection.credential.kind)
    if (!credential.kubeconfigPath) {
      throw new Error(`Kubernetes credential resolver returned no kubeconfig path for ${connection.id}`)
    }
    const { credential: reference, ...rest } = connection
    return {
      ...rest,
      credentialKind: reference.kind,
      kubeconfigPath: credential.kubeconfigPath,
      ...(credential.environment ? { environment: credential.environment } : {}),
    }
  }
}

const matchesScope = (values: string[], candidate: string): boolean =>
  values.includes('*') || values.includes(candidate)

const scopeSpecificity = (values: string[], candidate: string): number =>
  values.includes(candidate) ? 1 : 0
