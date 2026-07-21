import { randomUUID } from 'node:crypto'

import { normalizeLogger } from '../logging.js'
import type {
  Environment,
  EnvironmentProvider,
  EnvironmentStatus,
  ProvisionEnvironmentSpec,
} from '../ports/environment.js'
import type { Logger } from '../ports/system.js'
import {
  KubernetesConnectionRegistry,
  KubernetesGuardrailDefaultsSchema,
  type KubernetesGuardrailDefaults,
  type ResolvedKubernetesConnection,
} from './connection-registry.js'
import {
  KubectlKubernetesClient,
  type KubernetesClient,
  type KubernetesPortForward,
  type KubernetesResource,
} from './kubernetes-client.js'
import {
  KubernetesStackDescriptorSchema,
  type KubernetesEndpoint,
  type KubernetesStackDescriptor,
  type KubernetesStackDescriptorInput,
  type ReferencedKubernetesSecret,
} from './stack-descriptor.js'

export const KUBERNETES_MANAGED_BY_LABEL = 'app.kubernetes.io/managed-by'
export const KUBERNETES_ENVIRONMENT_ID_LABEL = 'factory.agentworkforce.dev/environment-id'
export const KUBERNETES_OWNER_ID_LABEL = 'factory.agentworkforce.dev/owner-id'
export const KUBERNETES_EXPIRES_AT_LABEL = 'factory.agentworkforce.dev/expires-at'
export const KUBERNETES_OWNER_ID_ANNOTATION = 'factory.agentworkforce.dev/owner-id'
export const KUBERNETES_CUSTOMER_ID_ANNOTATION = 'factory.agentworkforce.dev/customer-id'
export const KUBERNETES_REPOSITORY_ANNOTATION = 'factory.agentworkforce.dev/repository'
export const KUBERNETES_CONNECTION_ID_ANNOTATION = 'factory.agentworkforce.dev/connection-id'
export const KUBERNETES_TARGET_ANNOTATION = 'factory.agentworkforce.dev/target'
export const KUBERNETES_CREATED_AT_ANNOTATION = 'factory.agentworkforce.dev/created-at'
export const KUBERNETES_ENDPOINTS_ANNOTATION = 'factory.agentworkforce.dev/endpoints'
export const KUBERNETES_CLUSTER_RESOURCES_ANNOTATION = 'factory.agentworkforce.dev/cluster-resources'

const MANAGED_BY_VALUE = 'factory'
const RESERVED_RESOURCE_PREFIX = 'factory-guardrail-'
const DEPLOYER_SERVICE_ACCOUNT = `${RESERVED_RESOURCE_PREFIX}deployer`
const WORKLOAD_SERVICE_ACCOUNT = `${RESERVED_RESOURCE_PREFIX}workload`

export interface KubernetesStackProvisionInput {
  descriptor: KubernetesStackDescriptorInput
  repoRoot: string
}

export interface KubernetesProvisionSpec extends Omit<ProvisionEnvironmentSpec, 'stack'> {
  target?: 'byoc' | 'managed'
  stack: KubernetesStackProvisionInput
}

export interface KubernetesStackSecretResolver {
  resolve(reference: string): Promise<string>
}

export class EnvironmentKubernetesStackSecretResolver implements KubernetesStackSecretResolver {
  readonly #env: NodeJS.ProcessEnv

  constructor(env: NodeJS.ProcessEnv = process.env) {
    this.#env = env
  }

  async resolve(reference: string): Promise<string> {
    if (!reference.startsWith('env:')) {
      throw new Error(`Unsupported stack secret reference ${reference}; configure a secret-manager resolver for this scheme`)
    }
    const variable = reference.slice('env:'.length)
    if (!/^[A-Z_][A-Z0-9_]*$/u.test(variable)) throw new Error(`Invalid environment secret reference ${reference}`)
    const value = this.#env[variable]
    if (value === undefined) throw new Error(`Required stack secret reference ${reference} is unavailable`)
    return value
  }
}

interface EnvironmentRecord {
  environment: Environment
  connection: ResolvedKubernetesConnection
  endpoints: KubernetesEndpoint[]
  clusterResources: KubernetesResource[]
  forwards: Map<string, KubernetesPortForward>
}

export interface KubernetesEnvironmentProviderOptions {
  registry: KubernetesConnectionRegistry
  client?: KubernetesClient
  secretResolver?: KubernetesStackSecretResolver
  defaults?: Partial<KubernetesGuardrailDefaults>
  now?: () => Date
  randomId?: () => string
  logger?: Logger
  ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
}

export interface KubernetesReapReport {
  reaped: Array<{ id: string; connectionId: string; reason: 'ttl-expired' | 'owner-gone' }>
  skipped: Array<{ id?: string; connectionId: string; reason: string }>
}

/** Namespace-isolated Kubernetes provider for customer EKS and managed fallback clusters. */
export class KubernetesEnvironmentProvider implements EnvironmentProvider {
  readonly #registry: KubernetesConnectionRegistry
  readonly #client: KubernetesClient
  readonly #secrets: KubernetesStackSecretResolver
  readonly #defaults: KubernetesGuardrailDefaults
  readonly #now: () => Date
  readonly #randomId: () => string
  readonly #logger?: Logger
  readonly #ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
  readonly #records = new Map<string, EnvironmentRecord>()

  constructor(options: KubernetesEnvironmentProviderOptions) {
    this.#registry = options.registry
    this.#client = options.client ?? new KubectlKubernetesClient()
    this.#secrets = options.secretResolver ?? new EnvironmentKubernetesStackSecretResolver()
    this.#defaults = KubernetesGuardrailDefaultsSchema.parse({
      ...options.registry.config.defaults,
      ...options.defaults,
      limitRange: {
        ...options.registry.config.defaults.limitRange,
        ...options.defaults?.limitRange,
      },
    })
    this.#now = options.now ?? (() => new Date())
    this.#randomId = options.randomId ?? (() => randomUUID().replaceAll('-', '').slice(0, 10))
    this.#logger = options.logger ? normalizeLogger(options.logger) : undefined
    this.#ownerIsAlive = options.ownerIsAlive
  }

  async provision(spec: KubernetesProvisionSpec): Promise<Environment> {
    const descriptor = KubernetesStackDescriptorSchema.parse(spec.stack.descriptor)
    const target = spec.target ?? descriptor.target ?? 'byoc'
    if (descriptor.target !== target) {
      throw new Error(`Kubernetes descriptor target ${descriptor.target} does not match requested target ${target}`)
    }
    const ttl = spec.ttl ?? this.#defaults.ttlMs
    if (!Number.isInteger(ttl) || ttl < 60_000 || ttl > 7 * 24 * 60 * 60_000) {
      throw new Error('Kubernetes environment ttl must be between 60000 and 604800000 milliseconds')
    }
    const connection = await this.#registry.resolve({
      customerId: spec.customerId,
      repository: spec.repository,
      target,
    })
    const createdAt = this.#now()
    const id = generatedNamespaceName(spec.repository, this.#randomId())
    const expiresAtSeconds = Math.ceil((createdAt.getTime() + ttl) / 1_000)
    const rendered = await this.#client.render(descriptor.deployment, id, spec.stack.repoRoot, connection)
    const safeResources = enforceKubernetesResourceSafety(rendered, {
      namespace: id,
      environmentId: id,
      connection,
      descriptorAllowsClusterScope: descriptor.allowClusterScopedResources,
    })
    const clusterResources = safeResources.filter((resource) => isClusterScopedResource(resource, connection))
    const namespacedResources = safeResources.filter((resource) => !isClusterScopedResource(resource, connection))
    const namespace = namespaceResource({
      id,
      spec,
      descriptor,
      connection,
      createdAt,
      expiresAtSeconds,
      clusterResources,
    })
    let namespaceCreated = false
    const createdClusterResources: KubernetesResource[] = []
    let ambiguousClusterResource: KubernetesResource | undefined

    try {
      // `create`, not `apply`, is deliberate: a collision can never reuse an
      // existing (and potentially production) namespace.
      await this.#client.createNamespace(namespace, connection)
      namespaceCreated = true
      await this.#client.apply(
        createKubernetesGuardrailResources(id, connection, this.#defaults),
        id,
        connection,
      )
      const secretResources = await this.#secretResources(descriptor.secrets, id)
      if (secretResources.length > 0) await this.#client.apply(secretResources, id, connection)

      for (const resource of clusterResources) {
        // Cluster-scoped resources are always created, never applied, so a
        // customer object with the same name cannot be adopted or overwritten.
        ambiguousClusterResource = resource
        await this.#client.createClusterResource(resource, connection)
        createdClusterResources.push(resource)
        ambiguousClusterResource = undefined
      }
      await this.#client.apply(namespacedResources, id, connection)
      await this.#client.waitForReady(
        id,
        connection,
        descriptor.readinessTimeoutMs ?? this.#defaults.readinessTimeoutMs,
      )

      const environment: Environment = {
        id,
        provider: 'kubernetes',
        dispatchNamespace: id,
        endpoints: {},
        bindings: {
          'kubernetes.connection': connection.id,
          'kubernetes.target': connection.target,
          ...(connection.target === 'managed' && connection.fidelityCaveat
            ? { 'kubernetes.fidelityCaveat': connection.fidelityCaveat }
            : {}),
        },
        status: 'ready',
        createdAt: createdAt.toISOString(),
        ttl,
      }
      const record: EnvironmentRecord = {
        environment,
        connection,
        endpoints: descriptor.endpoints,
        clusterResources,
        forwards: new Map(),
      }
      this.#records.set(id, record)
      environment.endpoints = await this.endpoints(id)
      this.#logger?.info?.('[kubernetes-environment] provisioned environment', {
        id,
        connectionId: connection.id,
        target: connection.target,
        expiresAt: new Date(expiresAtSeconds * 1_000).toISOString(),
      })
      return cloneEnvironment(environment)
    } catch (error) {
      const partialRecord = this.#records.get(id)
      if (partialRecord) {
        await this.#stopForwards(partialRecord)
        this.#records.delete(id)
      }
      const cleanupErrors = await this.#deleteOwnedClusterResources(
        createdClusterResources,
        id,
        connection,
      )
      cleanupErrors.push(...await this.#deleteOwnedClusterResources(
        ambiguousClusterResource ? [ambiguousClusterResource] : [],
        id,
        connection,
        { skipUnowned: true },
      ))
      // Keep the namespace (and its persisted cluster-resource identities) if
      // cluster cleanup failed, so a later reaper can retry without guessing.
      if (namespaceCreated && cleanupErrors.length === 0) {
        try {
          const current = await this.#client.getNamespace(id, connection)
          if (current && isOwnedNamespace(current, id, connection.id)) await this.#client.deleteNamespace(id, connection)
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError)
        }
      }
      if (cleanupErrors.length > 0) {
        throw new AggregateError([error, ...cleanupErrors], `Kubernetes provision failed and cleanup also failed for ${id}`)
      }
      throw error
    }
  }

  async status(id: string): Promise<EnvironmentStatus> {
    const record = await this.#findRecord(id)
    if (!record) return 'destroyed'
    const namespace = await this.#client.getNamespace(id, record.connection)
    if (!namespace) {
      record.environment.status = 'destroyed'
      await this.#stopForwards(record)
      this.#records.delete(id)
      return 'destroyed'
    }
    assertOwnedNamespace(namespace, id, record.connection.id)
    if (namespace.metadata?.deletionTimestamp || record.environment.status === 'destroying') return 'destroying'
    if (record.environment.status === 'failed') return 'failed'
    return record.environment.status === 'provisioning' ? 'provisioning' : 'ready'
  }

  async endpoints(id: string): Promise<Record<string, string>> {
    const record = await this.#findRecord(id)
    if (!record) throw new Error(`Kubernetes environment ${id} does not exist`)
    const namespace = await this.#client.getNamespace(id, record.connection)
    if (!namespace) throw new Error(`Kubernetes environment ${id} does not exist`)
    assertOwnedNamespace(namespace, id, record.connection.id)

    for (const endpoint of record.endpoints) {
      if (record.environment.endpoints[endpoint.name]) continue
      if (endpoint.ingressUrl) {
        record.environment.endpoints[endpoint.name] = endpointUrl(endpoint.ingressUrl, endpoint.path)
        continue
      }
      const forward = await this.#client.portForwardService(
        id,
        endpoint.service,
        endpoint.port,
        endpoint.protocol,
        record.connection,
      )
      record.forwards.set(endpoint.name, forward)
      record.environment.endpoints[endpoint.name] = endpointUrl(forward.url, endpoint.path)
    }
    return { ...record.environment.endpoints }
  }

  async destroy(id: string): Promise<void> {
    const record = await this.#findRecord(id)
    if (!record) return
    record.environment.status = 'destroying'
    await this.#stopForwards(record)
    const namespace = await this.#client.getNamespace(id, record.connection)
    if (!namespace) {
      const cleanupErrors = await this.#deleteOwnedClusterResources(record.clusterResources, id, record.connection)
      if (cleanupErrors.length > 0) {
        record.environment.status = 'failed'
        throw new AggregateError(cleanupErrors, `Could not safely delete cluster-scoped resources for ${id}`)
      }
      record.environment.status = 'destroyed'
      this.#records.delete(id)
      return
    }
    assertOwnedNamespace(namespace, id, record.connection.id)
    const cleanupErrors = await this.#deleteOwnedClusterResources(record.clusterResources, id, record.connection)
    if (cleanupErrors.length > 0) {
      record.environment.status = 'failed'
      throw new AggregateError(cleanupErrors, `Could not safely delete cluster-scoped resources for ${id}`)
    }
    await this.#client.deleteNamespace(id, record.connection)
    record.environment.status = 'destroyed'
    this.#records.delete(id)
    this.#logger?.info?.('[kubernetes-environment] destroyed environment', {
      id,
      connectionId: record.connection.id,
    })
  }

  async reap(): Promise<KubernetesReapReport> {
    const report: KubernetesReapReport = { reaped: [], skipped: [] }
    const nowMs = this.#now().getTime()
    for (const resolution of await this.#registry.resolveAll()) {
      if (!resolution.connection) {
        report.skipped.push({
          connectionId: resolution.id,
          reason: `credential resolution failed: ${errorMessage(resolution.error)}`,
        })
        continue
      }
      const connection = resolution.connection
      let namespaces: KubernetesResource[]
      try {
        namespaces = await this.#client.listNamespaces(
          connection,
          `${KUBERNETES_MANAGED_BY_LABEL}=${MANAGED_BY_VALUE}`,
        )
      } catch (error) {
        report.skipped.push({ connectionId: connection.id, reason: `list failed: ${errorMessage(error)}` })
        continue
      }
      for (const namespace of namespaces) {
        const id = namespace.metadata?.name
        if (!id) {
          report.skipped.push({ connectionId: connection.id, reason: 'owned namespace has no name' })
          continue
        }
        if (!isOwnedNamespace(namespace, id, connection.id)) {
          report.skipped.push({ id, connectionId: connection.id, reason: 'ownership identity mismatch' })
          continue
        }
        const expiresAtSeconds = Number(namespace.metadata?.labels?.[KUBERNETES_EXPIRES_AT_LABEL])
        const ownerId = namespace.metadata?.annotations?.[KUBERNETES_OWNER_ID_ANNOTATION]
        let reason: KubernetesReapReport['reaped'][number]['reason'] | undefined
        if (Number.isFinite(expiresAtSeconds) && expiresAtSeconds * 1_000 <= nowMs) {
          reason = 'ttl-expired'
        } else if (ownerId && this.#ownerIsAlive) {
          try {
            if (await this.#ownerIsAlive(ownerId) === false) reason = 'owner-gone'
          } catch (error) {
            report.skipped.push({ id, connectionId: connection.id, reason: `owner check failed: ${errorMessage(error)}` })
            continue
          }
        }
        if (!reason) continue
        const record = this.#records.get(id)
        if (record) await this.#stopForwards(record)
        let clusterResources: KubernetesResource[]
        try {
          clusterResources = parsePersistedClusterResources(
            namespace.metadata?.annotations?.[KUBERNETES_CLUSTER_RESOURCES_ANNOTATION],
          )
        } catch (error) {
          report.skipped.push({
            id,
            connectionId: connection.id,
            reason: `invalid cluster resource identity: ${errorMessage(error)}`,
          })
          continue
        }
        const cleanupErrors = await this.#deleteOwnedClusterResources(clusterResources, id, connection)
        if (cleanupErrors.length > 0) {
          report.skipped.push({
            id,
            connectionId: connection.id,
            reason: `cluster resource cleanup failed: ${cleanupErrors.map(errorMessage).join('; ')}`,
          })
          continue
        }
        try {
          await this.#client.deleteNamespace(id, connection)
        } catch (error) {
          report.skipped.push({
            id,
            connectionId: connection.id,
            reason: `namespace deletion failed: ${errorMessage(error)}`,
          })
          continue
        }
        if (record) {
          record.environment.status = 'destroyed'
          this.#records.delete(id)
        }
        report.reaped.push({ id, connectionId: connection.id, reason })
        this.#logger?.warn?.('[kubernetes-environment] reaped environment', {
          id,
          connectionId: connection.id,
          reason,
        })
      }
    }
    return report
  }

  async #findRecord(id: string): Promise<EnvironmentRecord | undefined> {
    const known = this.#records.get(id)
    if (known) return known
    const resolutionFailures: Error[] = []
    for (const resolution of await this.#registry.resolveAll()) {
      if (!resolution.connection) {
        resolutionFailures.push(new Error(
          `Kubernetes connection ${resolution.id} is unavailable: ${errorMessage(resolution.error)}`,
        ))
        continue
      }
      const connection = resolution.connection
      const namespace = await this.#client.getNamespace(id, connection)
      if (!namespace) continue
      assertOwnedNamespace(namespace, id, connection.id)
      const annotations = namespace.metadata?.annotations ?? {}
      const createdAt = annotations[KUBERNETES_CREATED_AT_ANNOTATION]
      const expirySeconds = Number(namespace.metadata?.labels?.[KUBERNETES_EXPIRES_AT_LABEL])
      const createdAtMs = Date.parse(createdAt ?? '')
      const ttl = Number.isFinite(expirySeconds) && Number.isFinite(createdAtMs)
        ? Math.max(0, expirySeconds * 1_000 - createdAtMs)
        : this.#defaults.ttlMs
      const endpoints = parsePersistedEndpoints(annotations[KUBERNETES_ENDPOINTS_ANNOTATION])
      const clusterResources = parsePersistedClusterResources(annotations[KUBERNETES_CLUSTER_RESOURCES_ANNOTATION])
      const record: EnvironmentRecord = {
        environment: {
          id,
          provider: 'kubernetes',
          dispatchNamespace: id,
          endpoints: {},
          bindings: {
            'kubernetes.connection': connection.id,
            'kubernetes.target': connection.target,
            ...(connection.target === 'managed' && connection.fidelityCaveat
              ? { 'kubernetes.fidelityCaveat': connection.fidelityCaveat }
              : {}),
          },
          status: namespace.metadata?.deletionTimestamp ? 'destroying' : 'ready',
          createdAt: Number.isFinite(createdAtMs) ? new Date(createdAtMs).toISOString() : this.#now().toISOString(),
          ttl,
        },
        connection,
        endpoints,
        clusterResources,
        forwards: new Map(),
      }
      this.#records.set(id, record)
      return record
    }
    if (resolutionFailures.length > 0) {
      throw new AggregateError(
        resolutionFailures,
        `Could not safely determine whether Kubernetes environment ${id} exists`,
      )
    }
    return undefined
  }

  async #secretResources(secrets: ReferencedKubernetesSecret[], namespace: string): Promise<KubernetesResource[]> {
    const grouped = new Map<string, Record<string, string>>()
    for (const secret of secrets) {
      const value = await this.#secrets.resolve(secret.secretRef)
      const data = grouped.get(secret.name) ?? {}
      data[secret.key] = Buffer.from(value, 'utf8').toString('base64')
      grouped.set(secret.name, data)
    }
    return [...grouped.entries()].map(([name, data]) => ({
      apiVersion: 'v1',
      kind: 'Secret',
      metadata: { name, namespace },
      type: 'Opaque',
      data,
    }))
  }

  async #stopForwards(record: EnvironmentRecord): Promise<void> {
    const forwards = [...record.forwards.values()]
    record.forwards.clear()
    record.environment.endpoints = {}
    await Promise.allSettled(forwards.map(async (forward) => await forward.stop()))
  }

  async #deleteOwnedClusterResources(
    resources: KubernetesResource[],
    environmentId: string,
    connection: ResolvedKubernetesConnection,
    options: { skipUnowned?: boolean } = {},
  ): Promise<unknown[]> {
    const errors: unknown[] = []
    for (const resource of [...resources].reverse()) {
      try {
        const current = await this.#client.getClusterResource(resource, connection)
        if (!current) continue
        if (current.metadata?.labels?.[KUBERNETES_ENVIRONMENT_ID_LABEL] !== environmentId) {
          if (options.skipUnowned) continue
          throw new Error(
            `Refusing to delete cluster-scoped ${resource.kind}/${resource.metadata?.name}: ownership identity does not match`,
          )
        }
        await this.#client.deleteClusterResource(resource, connection)
      } catch (error) {
        errors.push(error)
      }
    }
    return errors
  }
}

export class KubernetesEnvironmentReaper {
  readonly #provider: KubernetesEnvironmentProvider
  readonly #intervalMs: number
  #timer: ReturnType<typeof setTimeout> | undefined
  #running = false
  #stopped = false

  constructor(provider: KubernetesEnvironmentProvider, options: { intervalMs?: number } = {}) {
    this.#provider = provider
    this.#intervalMs = options.intervalMs ?? 60_000
  }

  start(): void {
    if (this.#timer || this.#stopped) return
    this.#schedule(0)
  }

  async stop(): Promise<void> {
    this.#stopped = true
    if (this.#timer) clearTimeout(this.#timer)
    this.#timer = undefined
  }

  #schedule(delayMs: number): void {
    this.#timer = setTimeout(() => {
      this.#timer = undefined
      void this.#tick()
    }, delayMs)
  }

  async #tick(): Promise<void> {
    if (this.#stopped || this.#running) return
    this.#running = true
    try {
      await this.#provider.reap()
    } finally {
      this.#running = false
      if (!this.#stopped) this.#schedule(this.#intervalMs)
    }
  }
}

interface GuardrailResourceOptions {
  namespace: string
  environmentId: string
  connection: ResolvedKubernetesConnection
  descriptorAllowsClusterScope: boolean
}

const CLUSTER_SCOPED_KINDS = new Set([
  'APIService',
  'CertificateSigningRequest',
  'ClusterRole',
  'ClusterRoleBinding',
  'CustomResourceDefinition',
  'CSIDriver',
  'CSINode',
  'IngressClass',
  'MutatingWebhookConfiguration',
  'Namespace',
  'Node',
  'PersistentVolume',
  'PodSecurityPolicy',
  'PriorityClass',
  'RuntimeClass',
  'StorageClass',
  'ValidatingAdmissionPolicy',
  'ValidatingAdmissionPolicyBinding',
  'ValidatingWebhookConfiguration',
  'VolumeAttachment',
])

export function enforceKubernetesResourceSafety(
  input: KubernetesResource[],
  options: GuardrailResourceOptions,
): KubernetesResource[] {
  const renderedClusterRoles = new Set(input
    .filter((resource) => resource.kind === 'ClusterRole')
    .map((resource) => resource.metadata?.name)
    .filter((name): name is string => Boolean(name)))
  return input.map((raw) => {
    const resource = structuredClone(raw)
    const kind = resource.kind
    const apiVersion = resource.apiVersion
    const name = resource.metadata?.name
    if (!kind || !apiVersion || !name) throw new Error('Rendered Kubernetes resources require apiVersion, kind, and metadata.name')
    if (kind === 'Namespace') throw new Error('Customer stacks may not create or modify namespaces')
    if (kind === 'Secret') {
      throw new Error(`Customer stack Secret/${name} is denied; declare secretRef entries in the verification descriptor`)
    }
    if (name.startsWith(RESERVED_RESOURCE_PREFIX)) {
      throw new Error(`Customer stack resource ${kind}/${name} uses Factory's reserved guardrail prefix`)
    }
    if (kind === 'NetworkPolicy' && customerPolicyAllowsEgress(resource)) {
      throw new Error(`Customer NetworkPolicy/${name} may not add egress rules that bypass Factory isolation`)
    }
    validateCustomerRbac(resource, options.namespace, renderedClusterRoles)

    const clusterScoped = isClusterScopedResource(resource, options.connection)
    if (clusterScoped) {
      const qualifiedKind = `${apiVersion}/${kind}`
      const allowedKinds = options.connection.allowedClusterScopedKinds
      if (
        !options.descriptorAllowsClusterScope ||
        !options.connection.allowClusterScopedResources ||
        (!allowedKinds.includes(kind) && !allowedKinds.includes(qualifiedKind))
      ) {
        throw new Error(
          `Cluster-scoped resource ${qualifiedKind}/${name} is denied; both descriptor and customer connection must opt in and allow the kind`,
        )
      }
      if (resource.metadata?.namespace) delete resource.metadata.namespace
    } else {
      const declaredNamespace = resource.metadata?.namespace
      if (declaredNamespace && declaredNamespace !== options.namespace) {
        throw new Error(
          `Customer stack resource ${kind}/${name} targets namespace ${declaredNamespace}; only generated namespace ${options.namespace} is allowed`,
        )
      }
      resource.metadata = { ...resource.metadata, namespace: options.namespace }
    }
    resource.metadata = {
      ...resource.metadata,
      labels: {
        ...resource.metadata?.labels,
        [KUBERNETES_ENVIRONMENT_ID_LABEL]: options.environmentId,
      },
    }
    applyPlacementAndIdentity(resource, options.connection)
    return resource
  })
}

export function createKubernetesGuardrailResources(
  namespace: string,
  connection: ResolvedKubernetesConnection,
  defaultsInput: KubernetesGuardrailDefaults,
): KubernetesResource[] {
  const defaults = KubernetesGuardrailDefaultsSchema.parse(defaultsInput)
  const protectedNamespaces = [...new Set([...connection.protectedNamespaces, 'kube-system'])]
  const namespaceEgress = protectedNamespaces.length === 0 ? [] : [{
    to: [{
      namespaceSelector: {
        matchExpressions: [{
          key: 'kubernetes.io/metadata.name',
          operator: 'NotIn',
          values: protectedNamespaces,
        }],
      },
    }],
  }]
  const metadata = (name: string): KubernetesResource['metadata'] => ({
    name: `${RESERVED_RESOURCE_PREFIX}${name}`,
    namespace,
    labels: { [KUBERNETES_ENVIRONMENT_ID_LABEL]: namespace },
  })
  return [
    {
      apiVersion: 'v1', kind: 'ServiceAccount',
      metadata: { ...metadata('deployer'), name: DEPLOYER_SERVICE_ACCOUNT },
      automountServiceAccountToken: false,
    },
    {
      apiVersion: 'v1', kind: 'ServiceAccount',
      metadata: { ...metadata('workload'), name: WORKLOAD_SERVICE_ACCOUNT },
      automountServiceAccountToken: false,
    },
    {
      apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'Role',
      metadata: metadata('deployer'),
      rules: [
        {
          apiGroups: [''],
          resources: ['configmaps', 'endpoints', 'persistentvolumeclaims', 'pods', 'pods/log', 'secrets', 'services', 'serviceaccounts'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
        {
          apiGroups: ['apps'],
          resources: ['deployments', 'statefulsets', 'daemonsets', 'replicasets'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
        {
          apiGroups: ['batch'],
          resources: ['jobs', 'cronjobs'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
        {
          apiGroups: ['networking.k8s.io'],
          resources: ['ingresses'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
        {
          apiGroups: ['autoscaling'], resources: ['horizontalpodautoscalers'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
        {
          apiGroups: ['policy'], resources: ['poddisruptionbudgets'],
          verbs: ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete'],
        },
      ],
    },
    {
      apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding',
      metadata: metadata('deployer'),
      roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: `${RESERVED_RESOURCE_PREFIX}deployer` },
      subjects: [{ kind: 'ServiceAccount', name: DEPLOYER_SERVICE_ACCOUNT, namespace }],
    },
    {
      apiVersion: 'v1', kind: 'ResourceQuota',
      metadata: metadata('quota'),
      spec: { hard: defaults.resourceQuota },
    },
    {
      apiVersion: 'v1', kind: 'LimitRange',
      metadata: metadata('limits'),
      spec: {
        limits: [{
          type: 'Container',
          default: defaults.limitRange.default,
          defaultRequest: defaults.limitRange.defaultRequest,
          max: defaults.limitRange.max,
        }],
      },
    },
    {
      apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy',
      metadata: metadata('default-deny'),
      spec: { podSelector: {}, policyTypes: ['Ingress', 'Egress'] },
    },
    {
      apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy',
      metadata: metadata('same-namespace'),
      spec: {
        podSelector: {},
        policyTypes: ['Ingress', 'Egress'],
        ingress: [{ from: [{ podSelector: {} }] }],
        egress: [{ to: [{ podSelector: {} }] }],
      },
    },
    {
      apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy',
      metadata: metadata('dns'),
      spec: {
        podSelector: {},
        policyTypes: ['Egress'],
        egress: [{
          to: [{ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'kube-system' } } }],
          ports: [{ protocol: 'UDP', port: 53 }, { protocol: 'TCP', port: 53 }],
        }],
      },
    },
    {
      apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy',
      metadata: metadata('approved-egress'),
      spec: {
        podSelector: {},
        policyTypes: ['Egress'],
        egress: [
          ...namespaceEgress,
          {
            to: [{
              ipBlock: {
                cidr: '0.0.0.0/0',
                except: ['10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12', '192.168.0.0/16'],
              },
            }],
          },
          {
            to: [{ ipBlock: { cidr: '::/0', except: ['fc00::/7', 'fe80::/10'] } }],
          },
        ],
      },
    },
  ]
}

interface NamespaceResourceInput {
  id: string
  spec: KubernetesProvisionSpec
  descriptor: KubernetesStackDescriptor
  connection: ResolvedKubernetesConnection
  createdAt: Date
  expiresAtSeconds: number
  clusterResources: KubernetesResource[]
}

function namespaceResource(input: NamespaceResourceInput): KubernetesResource {
  return {
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      name: input.id,
      labels: {
        [KUBERNETES_MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
        [KUBERNETES_ENVIRONMENT_ID_LABEL]: input.id,
        [KUBERNETES_OWNER_ID_LABEL]: labelValue(input.spec.ownerId),
        [KUBERNETES_EXPIRES_AT_LABEL]: String(input.expiresAtSeconds),
        'pod-security.kubernetes.io/enforce': 'restricted',
        'pod-security.kubernetes.io/enforce-version': 'latest',
        'pod-security.kubernetes.io/audit': 'restricted',
        'pod-security.kubernetes.io/warn': 'restricted',
      },
      annotations: {
        [KUBERNETES_OWNER_ID_ANNOTATION]: input.spec.ownerId,
        [KUBERNETES_CUSTOMER_ID_ANNOTATION]: input.spec.customerId,
        [KUBERNETES_REPOSITORY_ANNOTATION]: input.spec.repository,
        [KUBERNETES_CONNECTION_ID_ANNOTATION]: input.connection.id,
        [KUBERNETES_TARGET_ANNOTATION]: input.connection.target,
        [KUBERNETES_CREATED_AT_ANNOTATION]: input.createdAt.toISOString(),
        [KUBERNETES_ENDPOINTS_ANNOTATION]: JSON.stringify(input.descriptor.endpoints),
        [KUBERNETES_CLUSTER_RESOURCES_ANNOTATION]: JSON.stringify(
          input.clusterResources.map(clusterResourceIdentity),
        ),
      },
    },
  }
}

function applyPlacementAndIdentity(resource: KubernetesResource, connection: ResolvedKubernetesConnection): void {
  const podSpec = podSpecFor(resource)
  if (!podSpec) return
  hardenPodSpec(resource, podSpec)
  if (podSpec.serviceAccountName === DEPLOYER_SERVICE_ACCOUNT) {
    throw new Error(`${resource.kind}/${resource.metadata?.name} may not run as Factory's deployer service account`)
  }
  const currentNodeSelector = objectRecord(podSpec.nodeSelector)
  podSpec.nodeSelector = { ...currentNodeSelector, ...connection.nodeSelector }
  const currentTolerations = Array.isArray(podSpec.tolerations) ? podSpec.tolerations : []
  podSpec.tolerations = [...currentTolerations, ...connection.tolerations]
  if (podSpec.serviceAccountName === undefined || podSpec.serviceAccountName === 'default') {
    podSpec.serviceAccountName = WORKLOAD_SERVICE_ACCOUNT
  }
  if (podSpec.automountServiceAccountToken === undefined) podSpec.automountServiceAccountToken = false
}

function hardenPodSpec(resource: KubernetesResource, podSpec: Record<string, unknown>): void {
  const name = resource.metadata?.name ?? '<unnamed>'
  for (const field of ['hostNetwork', 'hostPID', 'hostIPC'] as const) {
    if (podSpec[field] === true) throw new Error(`${resource.kind}/${name} requests ${field}, which bypasses namespace isolation`)
  }
  const volumes = Array.isArray(podSpec.volumes) ? podSpec.volumes as Array<Record<string, unknown>> : []
  if (volumes.some((volume) => volume.hostPath !== undefined)) {
    throw new Error(`${resource.kind}/${name} requests a hostPath volume, which bypasses namespace isolation`)
  }
  const podSecurityContext = objectRecord(podSpec.securityContext)
  if (podSecurityContext.runAsNonRoot === false || podSecurityContext.runAsUser === 0) {
    throw new Error(`${resource.kind}/${name} explicitly requests a root workload`)
  }
  podSpec.securityContext = {
    ...podSecurityContext,
    runAsNonRoot: podSecurityContext.runAsNonRoot ?? true,
    seccompProfile: podSecurityContext.seccompProfile ?? { type: 'RuntimeDefault' },
  }

  for (const field of ['initContainers', 'containers', 'ephemeralContainers'] as const) {
    const containers = Array.isArray(podSpec[field]) ? podSpec[field] as Array<Record<string, unknown>> : []
    for (const container of containers) {
      const containerName = typeof container.name === 'string' ? container.name : '<unnamed>'
      const securityContext = objectRecord(container.securityContext)
      if (securityContext.privileged === true || securityContext.allowPrivilegeEscalation === true) {
        throw new Error(`${resource.kind}/${name} container ${containerName} requests privileged execution`)
      }
      if (securityContext.runAsNonRoot === false || securityContext.runAsUser === 0) {
        throw new Error(`${resource.kind}/${name} container ${containerName} explicitly requests root execution`)
      }
      const capabilities = objectRecord(securityContext.capabilities)
      const addedCapabilities = Array.isArray(capabilities.add) ? capabilities.add : []
      if (addedCapabilities.some((capability) => capability !== 'NET_BIND_SERVICE')) {
        throw new Error(`${resource.kind}/${name} container ${containerName} requests unsafe Linux capabilities`)
      }
      const ports = Array.isArray(container.ports) ? container.ports as Array<Record<string, unknown>> : []
      if (ports.some((port) => port.hostPort !== undefined && port.hostPort !== 0)) {
        throw new Error(`${resource.kind}/${name} container ${containerName} requests a host port`)
      }
      container.securityContext = {
        ...securityContext,
        allowPrivilegeEscalation: false,
        capabilities: {
          ...capabilities,
          drop: [...new Set([...(Array.isArray(capabilities.drop) ? capabilities.drop : []), 'ALL'])],
        },
      }
    }
  }
}

function validateCustomerRbac(
  resource: KubernetesResource,
  namespace: string,
  renderedClusterRoles: Set<string>,
): void {
  const name = resource.metadata?.name ?? '<unnamed>'
  if (resource.kind === 'Role' || resource.kind === 'ClusterRole') {
    if (resource.kind === 'ClusterRole' && Object.prototype.hasOwnProperty.call(resource, 'aggregationRule')) {
      throw new Error(`ClusterRole/${name} may not aggregate unreviewed cluster permissions`)
    }
    const rules = Array.isArray((resource as Record<string, unknown>).rules)
      ? (resource as { rules: Array<Record<string, unknown>> }).rules
      : []
    const protectedResources = new Set([
      '*', 'networkpolicies', 'resourcequotas', 'limitranges', 'roles', 'rolebindings',
      'serviceaccounts/token',
    ])
    const protectedClusterResources = new Set([
      ...protectedResources,
      'clusterroles', 'clusterrolebindings', 'namespaces', 'nodes', 'persistentvolumes',
      'secrets', 'serviceaccounts', 'serviceaccounts/token', 'subjectaccessreviews', 'tokenreviews',
    ])
    for (const rule of rules) {
      const resources = Array.isArray(rule.resources) ? rule.resources : []
      const verbs = Array.isArray(rule.verbs) ? rule.verbs : []
      const apiGroups = Array.isArray(rule.apiGroups) ? rule.apiGroups : []
      const nonResourceURLs = Array.isArray(rule.nonResourceURLs) ? rule.nonResourceURLs : []
      const protectedSet = resource.kind === 'ClusterRole' ? protectedClusterResources : protectedResources
      if (
        resources.some((entry) => protectedSet.has(String(entry))) ||
        verbs.some((entry) => entry === '*' || entry === 'impersonate' || entry === 'escalate' || entry === 'bind') ||
        apiGroups.includes('*') ||
        nonResourceURLs.length > 0
      ) {
        throw new Error(`${resource.kind}/${name} can mutate Factory guardrails or escalate privileges`)
      }
    }
  }
  if (resource.kind === 'RoleBinding') {
    const record = resource as Record<string, unknown>
    const roleRef = objectRecord(record.roleRef)
    const subjects = Array.isArray(record.subjects) ? record.subjects as Array<Record<string, unknown>> : []
    if (roleRef.kind !== 'Role' || roleRef.name === `${RESERVED_RESOURCE_PREFIX}deployer`) {
      throw new Error(`RoleBinding/${name} may bind only a customer Role in the verification namespace`)
    }
    if (
      subjects.length === 0 ||
      subjects.some((subject) =>
        subject.kind !== 'ServiceAccount' ||
        subject.namespace !== namespace ||
        subject.name === DEPLOYER_SERVICE_ACCOUNT)
    ) {
      throw new Error(`RoleBinding/${name} may bind only customer service accounts in generated namespace ${namespace}`)
    }
  }
  if (resource.kind === 'ClusterRoleBinding') {
    const record = resource as Record<string, unknown>
    const roleRef = objectRecord(record.roleRef)
    const subjects = Array.isArray(record.subjects) ? record.subjects as Array<Record<string, unknown>> : []
    if (
      roleRef.kind !== 'ClusterRole' ||
      typeof roleRef.name !== 'string' ||
      !renderedClusterRoles.has(roleRef.name)
    ) {
      throw new Error(`ClusterRoleBinding/${name} must bind a ClusterRole created by the same stack`)
    }
    if (
      subjects.length === 0 ||
      subjects.some((subject) =>
        subject.kind !== 'ServiceAccount' ||
        subject.namespace !== namespace ||
        subject.name === DEPLOYER_SERVICE_ACCOUNT)
    ) {
      throw new Error(`ClusterRoleBinding/${name} may bind only service accounts in generated namespace ${namespace}`)
    }
  }
}

function podSpecFor(resource: KubernetesResource): Record<string, unknown> | undefined {
  const record = resource as Record<string, unknown>
  const spec = objectRecord(record.spec)
  if (resource.kind === 'Pod') return spec
  if (['Deployment', 'StatefulSet', 'DaemonSet', 'ReplicaSet', 'Job'].includes(resource.kind ?? '')) {
    return objectRecord(objectRecord(spec.template).spec)
  }
  if (resource.kind === 'CronJob') {
    return objectRecord(objectRecord(objectRecord(objectRecord(spec.jobTemplate).spec).template).spec)
  }
  return undefined
}

function customerPolicyAllowsEgress(resource: KubernetesResource): boolean {
  const spec = objectRecord((resource as Record<string, unknown>).spec)
  const policyTypes = Array.isArray(spec.policyTypes) ? spec.policyTypes : []
  return Object.prototype.hasOwnProperty.call(spec, 'egress') || policyTypes.includes('Egress')
}

function assertOwnedNamespace(namespace: KubernetesResource, expectedId: string, connectionId: string): void {
  if (!isOwnedNamespace(namespace, expectedId, connectionId)) {
    throw new Error(`Refusing to operate on namespace ${expectedId}: Factory ownership identity does not match`)
  }
}

function isOwnedNamespace(namespace: KubernetesResource, expectedId: string, connectionId: string): boolean {
  return namespace.kind === 'Namespace' &&
    namespace.metadata?.name === expectedId &&
    expectedId.startsWith('factory-') &&
    namespace.metadata?.labels?.[KUBERNETES_MANAGED_BY_LABEL] === MANAGED_BY_VALUE &&
    namespace.metadata?.labels?.[KUBERNETES_ENVIRONMENT_ID_LABEL] === expectedId &&
    namespace.metadata?.annotations?.[KUBERNETES_CONNECTION_ID_ANNOTATION] === connectionId
}

function generatedNamespaceName(repository: string, suffix: string): string {
  const repoName = repository.split('/').at(-1) ?? 'verification'
  const slug = repoName.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-+|-+$/gu, '') || 'verification'
  const safeSuffix = suffix.toLowerCase().replace(/[^a-z0-9]+/gu, '').slice(0, 12) || randomUUID().replaceAll('-', '').slice(0, 10)
  return `factory-${slug.slice(0, 42)}-${safeSuffix}`.slice(0, 63).replace(/-$/u, '')
}

function labelValue(value: string): string {
  const normalized = value.toLowerCase().replace(/[^a-z0-9_.-]+/gu, '-').replace(/^[^a-z0-9]+|[^a-z0-9]+$/gu, '')
  return (normalized || 'unknown').slice(0, 63).replace(/[^a-z0-9]$/u, '0')
}

function endpointUrl(base: string, path: string): string {
  const url = new URL(base)
  url.pathname = path
  return url.toString()
}

function parsePersistedEndpoints(value: string | undefined): KubernetesEndpoint[] {
  if (!value) return []
  try {
    const parsed = JSON.parse(value) as unknown
    const result = KubernetesStackDescriptorSchema.shape.endpoints.safeParse(parsed)
    return result.success ? result.data : []
  } catch {
    return []
  }
}

function parsePersistedClusterResources(value: string | undefined): KubernetesResource[] {
  if (!value) return []
  const parsed = JSON.parse(value) as unknown
  if (!Array.isArray(parsed)) throw new Error('cluster resource annotation must be an array')
  return parsed.map((resource) => clusterResourceIdentity(resource as KubernetesResource))
}

function clusterResourceIdentity(resource: KubernetesResource): KubernetesResource {
  if (!resource.apiVersion || !resource.kind || !resource.metadata?.name) {
    throw new Error('Cluster-scoped resource identity requires apiVersion, kind, and metadata.name')
  }
  return {
    apiVersion: resource.apiVersion,
    kind: resource.kind,
    metadata: { name: resource.metadata.name },
  }
}

function isClusterScopedResource(
  resource: KubernetesResource,
  connection: ResolvedKubernetesConnection,
): boolean {
  const kind = resource.kind ?? ''
  const qualifiedKind = `${resource.apiVersion ?? ''}/${kind}`
  return CLUSTER_SCOPED_KINDS.has(kind) ||
    connection.allowedClusterScopedKinds.includes(kind) ||
    connection.allowedClusterScopedKinds.includes(qualifiedKind)
}

function objectRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

const cloneEnvironment = (environment: Environment): Environment => ({
  ...environment,
  endpoints: { ...environment.endpoints },
  bindings: { ...environment.bindings },
})

const errorMessage = (error: unknown): string => error instanceof Error ? error.message : String(error)
