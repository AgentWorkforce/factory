import { randomUUID } from 'node:crypto'

import { z } from 'zod'

import { normalizeLogger } from '../logging.js'
import type {
  Environment,
  EnvironmentProvider,
  EnvironmentStatus,
  ProvisionEnvironmentSpec,
} from '../ports/environment.js'
import type { Logger } from '../ports/system.js'

export const CLOUDFLARE_METADATA_SCRIPT = 'factory-environment'
export const CLOUDFLARE_MANAGED_TAG = 'factory-managed'
export const CLOUDFLARE_ENVIRONMENT_TAG = 'factory-verification-environment'

export const CLOUDFLARE_ENVIRONMENT_BINDINGS = {
  environmentId: 'FACTORY_ENVIRONMENT_ID',
  ownerId: 'FACTORY_OWNER_ID',
  customerId: 'FACTORY_CUSTOMER_ID',
  repository: 'FACTORY_REPOSITORY',
  createdAt: 'FACTORY_CREATED_AT',
  expiresAt: 'FACTORY_EXPIRES_AT',
  maxRunCostUsd: 'FACTORY_MAX_RUN_COST_USD',
  containerMaxInstances: 'FACTORY_CONTAINER_MAX_INSTANCES',
  workerCpuMs: 'FACTORY_WORKER_CPU_MS',
  workerSubrequests: 'FACTORY_WORKER_SUBREQUESTS',
} as const

const dnsLabelSchema = z.string().trim().min(1).max(40).regex(
  /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u,
  'must be a lowercase DNS label',
)

const containerInstanceTypeSchema = z.enum([
  'lite',
  'basic',
  'standard-1',
  'standard-2',
  'standard-3',
  'standard-4',
])

/**
 * Source-control-safe Cloudflare policy. Account credentials deliberately do
 * not belong here; `resource` names the SST Resource.*-style object supplied
 * to `CloudflareEnvironmentProvider.fromResource` at runtime.
 */
export const CloudflareEnvironmentConfigSchema = z.object({
  resource: z.string().trim().min(1).default('FactoryTestInfra'),
  dispatchNamespacePrefix: dnsLabelSchema.default('factory-verification'),
  maxConcurrentEnvironments: z.number().int().min(1).max(100).default(2),
  defaultTtlMs: z.number().int().min(60_000).max(24 * 60 * 60_000).default(15 * 60_000),
  maxTtlMs: z.number().int().min(60_000).max(24 * 60 * 60_000).default(60 * 60_000),
  maxRunCostUsd: z.number().positive().max(1_000).default(1),
  workerLimits: z.object({
    cpuMs: z.number().int().min(1).max(300_000).default(100),
    subrequests: z.number().int().min(1).max(10_000_000).default(100),
  }).strict().default({}),
  container: z.object({
    instanceType: containerInstanceTypeSchema.default('lite'),
    maxInstances: z.number().int().min(1).max(100).default(3),
  }).strict().default({}),
}).strict().superRefine((config, context) => {
  if (config.defaultTtlMs > config.maxTtlMs) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['defaultTtlMs'],
      message: 'must be less than or equal to maxTtlMs',
    })
  }
}).default({})

/** Runtime secret/output object linked by factory-test-infra. */
export const CloudflareEnvironmentResourceSchema = z.object({
  accountId: z.string().trim().min(1).max(32),
  apiToken: z.string().min(1),
  /** Optional path template exposed by the infra dispatch Worker. */
  dispatcherUrlTemplate: z.string().url().refine(
    (value) => value.includes('{namespace}') && value.includes('{script}'),
    'must contain {namespace} and {script} placeholders',
  ).optional(),
}).strict()

export const CloudflareProvisionStackSchema = z.object({
  /** Requested concurrent live Container instances for this run. */
  containerInstances: z.number().int().min(0).default(0),
  /** Hard upper cost reservation for the infra usage monitor. */
  runCostBudgetUsd: z.number().positive().optional(),
}).strict().default({})

export type CloudflareEnvironmentConfig = z.output<typeof CloudflareEnvironmentConfigSchema>
export type CloudflareEnvironmentConfigInput = z.input<typeof CloudflareEnvironmentConfigSchema>
export type CloudflareEnvironmentResource = z.output<typeof CloudflareEnvironmentResourceSchema>
export type CloudflareProvisionStack = z.output<typeof CloudflareProvisionStackSchema>

export interface CloudflareProvisionSpec extends Omit<ProvisionEnvironmentSpec, 'stack'> {
  stack?: z.input<typeof CloudflareProvisionStackSchema>
}

export interface CloudflareDispatchNamespace {
  namespace_name: string
  namespace_id?: string
  created_on?: string
  modified_on?: string
  script_count?: number
  trusted_workers?: boolean
}

export interface CloudflareWorkerBinding {
  name: string
  type: string
  text?: string
  [key: string]: unknown
}

export interface UploadCloudflareWorkerInput {
  namespace: string
  scriptName: string
  source: string
  compatibilityDate: string
  bindings: CloudflareWorkerBinding[]
  tags: string[]
  limits: {
    cpu_ms: number
    subrequests: number
  }
}

/** Small API seam so lifecycle behavior can be tested without Cloudflare. */
export interface CloudflareEnvironmentClient {
  listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]>
  getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined>
  createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace>
  deleteDispatchNamespace(name: string): Promise<void>
  uploadDispatchWorker(input: UploadCloudflareWorkerInput): Promise<void>
  getDispatchWorkerBindings(namespace: string, scriptName: string): Promise<CloudflareWorkerBinding[] | undefined>
}

interface CloudflareApiEnvelope<T> {
  success: boolean
  result?: T
  errors?: Array<{ code?: number; message?: string }>
  messages?: Array<{ code?: number; message?: string }>
}

export interface HttpCloudflareEnvironmentClientOptions {
  resource: CloudflareEnvironmentResource
  fetch?: typeof globalThis.fetch
  apiBaseUrl?: string
}

export class CloudflareApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly method: string,
    public readonly path: string,
  ) {
    super(message)
    this.name = 'CloudflareApiError'
  }
}

/** Minimal Workers for Platforms client; no credential is read from process.env. */
export class HttpCloudflareEnvironmentClient implements CloudflareEnvironmentClient {
  readonly #resource: CloudflareEnvironmentResource
  readonly #fetch: typeof globalThis.fetch
  readonly #apiBaseUrl: string

  constructor(options: HttpCloudflareEnvironmentClientOptions) {
    this.#resource = CloudflareEnvironmentResourceSchema.parse(options.resource)
    this.#fetch = options.fetch ?? globalThis.fetch
    this.#apiBaseUrl = (options.apiBaseUrl ?? 'https://api.cloudflare.com/client/v4').replace(/\/+$/u, '')
  }

  async listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]> {
    return await this.#request<CloudflareDispatchNamespace[]>('GET', this.#namespacePath()) ?? []
  }

  async getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined> {
    return await this.#request<CloudflareDispatchNamespace>(
      'GET',
      `${this.#namespacePath()}/${encodeURIComponent(name)}`,
      undefined,
      { notFoundIsUndefined: true },
    )
  }

  async createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace> {
    const created = await this.#request<CloudflareDispatchNamespace>('POST', this.#namespacePath(), {
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    })
    if (!created?.namespace_name) throw new Error('Cloudflare create dispatch namespace returned no namespace identity')
    return created
  }

  async deleteDispatchNamespace(name: string): Promise<void> {
    await this.#request<unknown>('DELETE', `${this.#namespacePath()}/${encodeURIComponent(name)}`)
  }

  async uploadDispatchWorker(input: UploadCloudflareWorkerInput): Promise<void> {
    const moduleName = `${input.scriptName}.mjs`
    const metadata = {
      main_module: moduleName,
      compatibility_date: input.compatibilityDate,
      bindings: input.bindings,
      tags: input.tags,
      limits: input.limits,
    }
    const form = new FormData()
    form.append('metadata', new Blob([JSON.stringify(metadata)], { type: 'application/json' }))
    form.append(moduleName, new Blob([input.source], { type: 'application/javascript+module' }), moduleName)
    await this.#request<unknown>(
      'PUT',
      `${this.#namespacePath()}/${encodeURIComponent(input.namespace)}/scripts/${encodeURIComponent(input.scriptName)}`,
      { body: form },
    )
  }

  async getDispatchWorkerBindings(
    namespace: string,
    scriptName: string,
  ): Promise<CloudflareWorkerBinding[] | undefined> {
    return await this.#request<CloudflareWorkerBinding[]>(
      'GET',
      `${this.#namespacePath()}/${encodeURIComponent(namespace)}/scripts/${encodeURIComponent(scriptName)}/bindings`,
      undefined,
      { notFoundIsUndefined: true },
    )
  }

  #namespacePath(): string {
    return `/accounts/${encodeURIComponent(this.#resource.accountId)}/workers/dispatch/namespaces`
  }

  async #request<T>(
    method: string,
    path: string,
    init: Omit<RequestInit, 'method'> = {},
    options: { notFoundIsUndefined?: boolean } = {},
  ): Promise<T | undefined> {
    const response = await this.#fetch(`${this.#apiBaseUrl}${path}`, {
      ...init,
      method,
      headers: {
        authorization: `Bearer ${this.#resource.apiToken}`,
        ...init.headers,
      },
    })
    if (response.status === 404 && options.notFoundIsUndefined) return undefined

    const envelope = await response.json().catch(() => undefined) as CloudflareApiEnvelope<T> | undefined
    if (!response.ok || envelope?.success !== true) {
      const details = [...(envelope?.errors ?? []), ...(envelope?.messages ?? [])]
        .map((entry) => entry.message || (entry.code === undefined ? '' : String(entry.code)))
        .filter(Boolean)
        .join('; ')
      throw new CloudflareApiError(
        `Cloudflare API ${method} ${path} failed with HTTP ${response.status}${details ? `: ${details}` : ''}`,
        response.status,
        method,
        path,
      )
    }
    return envelope.result
  }
}

interface CloudflareEnvironmentRecord {
  environment: Environment
  ownerId: string
  expiresAt: string
}

export interface CloudflareEnvironmentProviderOptions {
  config?: CloudflareEnvironmentConfigInput
  resource: CloudflareEnvironmentResource
  client?: CloudflareEnvironmentClient
  now?: () => Date
  randomId?: () => string
  logger?: Logger
  ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
}

export interface CloudflareEnvironmentProviderResourceOptions extends Omit<
  CloudflareEnvironmentProviderOptions,
  'resource' | 'config'
> {
  config?: CloudflareEnvironmentConfigInput
  resources: Record<string, unknown>
}

export interface CloudflareReapReport {
  reaped: Array<{ id: string; reason: 'ttl-expired' | 'owner-gone' }>
  skipped: Array<{ id?: string; reason: string }>
}

export class CloudflareEnvironmentQuotaError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CloudflareEnvironmentQuotaError'
  }
}

/**
 * One-untrusted-dispatch-namespace-per-run Cloudflare provider.
 *
 * The metadata Worker is intentionally inert. Its bindings are the durable,
 * identity-checked lease that both Factory and factory-test-infra's Cron reaper
 * can inspect before deleting a namespace.
 */
export class CloudflareEnvironmentProvider implements EnvironmentProvider {
  readonly config: CloudflareEnvironmentConfig
  readonly #resource: CloudflareEnvironmentResource
  readonly #client: CloudflareEnvironmentClient
  readonly #now: () => Date
  readonly #randomId: () => string
  readonly #logger?: Logger
  readonly #ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
  readonly #records = new Map<string, CloudflareEnvironmentRecord>()
  #provisionLock: Promise<void> = Promise.resolve()

  constructor(options: CloudflareEnvironmentProviderOptions) {
    this.config = CloudflareEnvironmentConfigSchema.parse(options.config ?? {})
    this.#resource = CloudflareEnvironmentResourceSchema.parse(options.resource)
    this.#client = options.client ?? new HttpCloudflareEnvironmentClient({ resource: this.#resource })
    this.#now = options.now ?? (() => new Date())
    this.#randomId = options.randomId ?? (() => randomUUID().replaceAll('-', '').slice(0, 10))
    this.#logger = options.logger ? normalizeLogger(options.logger) : undefined
    this.#ownerIsAlive = options.ownerIsAlive
  }

  static fromResource(options: CloudflareEnvironmentProviderResourceOptions): CloudflareEnvironmentProvider {
    const config = CloudflareEnvironmentConfigSchema.parse(options.config ?? {})
    const resource = options.resources[config.resource]
    if (resource === undefined) {
      throw new Error(`Cloudflare environment resource ${JSON.stringify(config.resource)} is unavailable`)
    }
    const { resources: _resources, ...providerOptions } = options
    return new CloudflareEnvironmentProvider({
      ...providerOptions,
      config,
      resource: CloudflareEnvironmentResourceSchema.parse(resource),
    })
  }

  async provision(spec: CloudflareProvisionSpec): Promise<Environment> {
    return await this.#withProvisionLock(async () => {
      const stack = CloudflareProvisionStackSchema.parse(spec.stack ?? {})
      const ttl = spec.ttl ?? this.config.defaultTtlMs
      this.#assertRequestWithinGuardrails(ttl, stack)

      const active = (await this.#client.listDispatchNamespaces())
        .filter((namespace) => isManagedNamespaceName(namespace.namespace_name, this.config.dispatchNamespacePrefix))
      if (active.length >= this.config.maxConcurrentEnvironments) {
        throw new CloudflareEnvironmentQuotaError(
          `Cloudflare max-concurrent environment cap of ${this.config.maxConcurrentEnvironments} is exhausted`,
        )
      }
      const preexistingNames = new Set(active.map((namespace) => namespace.namespace_name))

      const createdAt = this.#now()
      const expiresAt = new Date(createdAt.getTime() + ttl)
      const id = cloudflareEnvironmentName(
        this.config.dispatchNamespacePrefix,
        spec.repository,
        this.#randomId(),
      )
      let namespaceCreated = false
      try {
        const namespace = await this.#client.createDispatchNamespace(id)
        namespaceCreated = true
        if (namespace.namespace_name !== id) {
          throw new Error(`Cloudflare returned dispatch namespace ${namespace.namespace_name} for requested identity ${id}`)
        }
        if (namespace.trusted_workers === true) {
          throw new Error(`Cloudflare dispatch namespace ${id} is trusted; verification namespaces must be untrusted`)
        }

        // The in-process lock closes races within one Factory instance. This
        // reconciliation also closes the common multi-run race where two
        // providers both observe one free slot before either creates it.
        const reconciled = (await this.#client.listDispatchNamespaces())
          .filter((candidate) => isManagedNamespaceName(
            candidate.namespace_name,
            this.config.dispatchNamespacePrefix,
          ))
          .sort((left, right) => (
            Number(preexistingNames.has(right.namespace_name)) - Number(preexistingNames.has(left.namespace_name)) ||
            compareDispatchNamespaces(left, right)
          ))
        if (reconciled.length > this.config.maxConcurrentEnvironments &&
          reconciled.findIndex((candidate) => candidate.namespace_name === id) >=
            this.config.maxConcurrentEnvironments) {
          throw new CloudflareEnvironmentQuotaError(
            `Cloudflare max-concurrent environment cap of ${this.config.maxConcurrentEnvironments} ` +
            'was exceeded by a concurrent provision',
          )
        }

        const costBudget = stack.runCostBudgetUsd ?? this.config.maxRunCostUsd
        await this.#client.uploadDispatchWorker({
          namespace: id,
          scriptName: CLOUDFLARE_METADATA_SCRIPT,
          source: metadataWorkerSource(),
          compatibilityDate: createdAt.toISOString().slice(0, 10),
          bindings: environmentMetadataBindings({
            id,
            spec,
            createdAt,
            expiresAt,
            costBudget,
            containerMaxInstances: stack.containerInstances || this.config.container.maxInstances,
            workerCpuMs: this.config.workerLimits.cpuMs,
            workerSubrequests: this.config.workerLimits.subrequests,
          }),
          tags: [CLOUDFLARE_MANAGED_TAG, CLOUDFLARE_ENVIRONMENT_TAG, `factory-environment-${id}`],
          limits: {
            cpu_ms: this.config.workerLimits.cpuMs,
            subrequests: this.config.workerLimits.subrequests,
          },
        })

        const environment = this.#environmentFromMetadata({
          id,
          ownerId: spec.ownerId,
          createdAt: createdAt.toISOString(),
          expiresAt: expiresAt.toISOString(),
          ttl,
          containerMaxInstances: stack.containerInstances || this.config.container.maxInstances,
          costBudget,
        })
        this.#records.set(id, { environment, ownerId: spec.ownerId, expiresAt: expiresAt.toISOString() })
        this.#logger?.info?.('[cloudflare-environment] provisioned environment', {
          id,
          expiresAt: expiresAt.toISOString(),
          containerMaxInstances: environment.bindings['cloudflare.container.maxInstances'],
          maxRunCostUsd: environment.bindings['cloudflare.maxRunCostUsd'],
        })
        return cloneEnvironment(environment)
      } catch (error) {
        if (!namespaceCreated) throw error
        try {
          await this.#client.deleteDispatchNamespace(id)
        } catch (cleanupError) {
          throw new AggregateError(
            [error, cleanupError],
            `Cloudflare provision failed and cleanup also failed for ${id}`,
          )
        }
        throw error
      }
    })
  }

  async status(id: string): Promise<EnvironmentStatus> {
    const namespace = await this.#client.getDispatchNamespace(id)
    if (!namespace) {
      this.#records.delete(id)
      return 'destroyed'
    }
    await this.#readOwnedMetadata(id)
    return this.#records.get(id)?.environment.status ?? 'ready'
  }

  async endpoints(id: string): Promise<Record<string, string>> {
    const namespace = await this.#client.getDispatchNamespace(id)
    if (!namespace) throw new Error(`Cloudflare environment ${id} does not exist`)
    await this.#readOwnedMetadata(id)
    return {}
  }

  async destroy(id: string): Promise<void> {
    const namespace = await this.#client.getDispatchNamespace(id)
    if (!namespace) {
      this.#records.delete(id)
      return
    }
    await this.#readOwnedMetadata(id)
    const record = this.#records.get(id)
    if (record) record.environment.status = 'destroying'
    await this.#client.deleteDispatchNamespace(id)
    if (record) record.environment.status = 'destroyed'
    this.#records.delete(id)
    this.#logger?.info?.('[cloudflare-environment] destroyed environment', { id })
  }

  async reap(): Promise<CloudflareReapReport> {
    const report: CloudflareReapReport = { reaped: [], skipped: [] }
    const nowMs = this.#now().getTime()
    let namespaces: CloudflareDispatchNamespace[]
    try {
      namespaces = (await this.#client.listDispatchNamespaces())
        .filter((namespace) => isManagedNamespaceName(namespace.namespace_name, this.config.dispatchNamespacePrefix))
    } catch (error) {
      return { reaped: [], skipped: [{ reason: `list failed: ${errorMessage(error)}` }] }
    }

    for (const namespace of namespaces) {
      const id = namespace.namespace_name
      let metadata: Map<string, string>
      try {
        metadata = await this.#readOwnedMetadata(id)
      } catch (error) {
        report.skipped.push({ id, reason: errorMessage(error) })
        continue
      }
      const expiresAt = Date.parse(metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.expiresAt) ?? '')
      const ownerId = metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.ownerId)
      let reason: CloudflareReapReport['reaped'][number]['reason'] | undefined
      if (Number.isFinite(expiresAt) && expiresAt <= nowMs) {
        reason = 'ttl-expired'
      } else if (ownerId && this.#ownerIsAlive) {
        try {
          if (await this.#ownerIsAlive(ownerId) === false) reason = 'owner-gone'
        } catch (error) {
          report.skipped.push({ id, reason: `owner check failed: ${errorMessage(error)}` })
          continue
        }
      }
      if (!reason) continue

      try {
        await this.#client.deleteDispatchNamespace(id)
      } catch (error) {
        report.skipped.push({ id, reason: `namespace deletion failed: ${errorMessage(error)}` })
        continue
      }
      const record = this.#records.get(id)
      if (record) record.environment.status = 'destroyed'
      this.#records.delete(id)
      report.reaped.push({ id, reason })
      this.#logger?.warn?.('[cloudflare-environment] reaped environment', { id, reason })
    }
    return report
  }

  #assertRequestWithinGuardrails(ttl: number, stack: CloudflareProvisionStack): void {
    if (!Number.isInteger(ttl) || ttl < 60_000 || ttl > this.config.maxTtlMs) {
      throw new CloudflareEnvironmentQuotaError(
        `Cloudflare environment ttl must be between 60000 and ${this.config.maxTtlMs} milliseconds`,
      )
    }
    if (stack.containerInstances > this.config.container.maxInstances) {
      throw new CloudflareEnvironmentQuotaError(
        `Cloudflare Container request of ${stack.containerInstances} exceeds per-environment cap of ${this.config.container.maxInstances}`,
      )
    }
    if ((stack.runCostBudgetUsd ?? 0) > this.config.maxRunCostUsd) {
      throw new CloudflareEnvironmentQuotaError(
        `Cloudflare run cost budget $${stack.runCostBudgetUsd} exceeds per-run cap of $${this.config.maxRunCostUsd}`,
      )
    }
  }

  async #readOwnedMetadata(id: string): Promise<Map<string, string>> {
    if (!isManagedNamespaceName(id, this.config.dispatchNamespacePrefix)) {
      throw new Error(`Refusing Cloudflare namespace ${id}: name is outside the managed prefix`)
    }
    const bindings = await this.#client.getDispatchWorkerBindings(id, CLOUDFLARE_METADATA_SCRIPT)
    if (!bindings) {
      throw new Error(`Refusing Cloudflare namespace ${id}: ownership metadata Worker is missing`)
    }
    const metadata = new Map(bindings.flatMap((binding) => (
      binding.type === 'plain_text' && typeof binding.text === 'string'
        ? [[binding.name, binding.text] as const]
        : []
    )))
    if (metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.environmentId) !== id) {
      throw new Error(`Refusing Cloudflare namespace ${id}: ownership identity mismatch`)
    }
    const createdAt = metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.createdAt)
    const expiresAt = metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.expiresAt)
    const ownerId = metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.ownerId)
    const createdAtMs = Date.parse(createdAt ?? '')
    const expiresAtMs = Date.parse(expiresAt ?? '')
    if (!ownerId || !Number.isFinite(createdAtMs) || !Number.isFinite(expiresAtMs) || expiresAtMs <= createdAtMs) {
      throw new Error(`Refusing Cloudflare namespace ${id}: ownership lease is incomplete or invalid`)
    }
    if (!this.#records.has(id)) {
      const ttl = expiresAtMs - createdAtMs
      const costBudget = parseFiniteNumber(metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.maxRunCostUsd))
      const containerMaxInstances = parseFiniteNumber(
        metadata.get(CLOUDFLARE_ENVIRONMENT_BINDINGS.containerMaxInstances),
      )
      this.#records.set(id, {
        environment: this.#environmentFromMetadata({
          id,
          ownerId,
          createdAt: new Date(createdAtMs).toISOString(),
          expiresAt: new Date(expiresAtMs).toISOString(),
          ttl,
          costBudget,
          containerMaxInstances,
        }),
        ownerId,
        expiresAt: new Date(expiresAtMs).toISOString(),
      })
    }
    return metadata
  }

  #environmentFromMetadata(input: {
    id: string
    ownerId: string
    createdAt: string
    expiresAt: string
    ttl: number
    costBudget: number
    containerMaxInstances: number
  }): Environment {
    return {
      id: input.id,
      provider: 'cloudflare',
      dispatchNamespace: input.id,
      endpoints: {},
      bindings: {
        'cloudflare.resource': this.config.resource,
        'cloudflare.metadataScript': CLOUDFLARE_METADATA_SCRIPT,
        'cloudflare.expiresAt': input.expiresAt,
        'cloudflare.maxRunCostUsd': String(input.costBudget),
        'cloudflare.container.instanceType': this.config.container.instanceType,
        'cloudflare.container.maxInstances': String(input.containerMaxInstances),
        'cloudflare.worker.cpuMs': String(this.config.workerLimits.cpuMs),
        'cloudflare.worker.subrequests': String(this.config.workerLimits.subrequests),
      },
      status: 'ready',
      createdAt: input.createdAt,
      ttl: input.ttl,
    }
  }

  async #withProvisionLock<T>(work: () => Promise<T>): Promise<T> {
    const previous = this.#provisionLock
    let release = (): void => undefined
    this.#provisionLock = new Promise<void>((resolve) => { release = resolve })
    await previous
    try {
      return await work()
    } finally {
      release()
    }
  }
}

export class CloudflareEnvironmentReaper {
  readonly #provider: CloudflareEnvironmentProvider
  readonly #intervalMs: number
  #timer: ReturnType<typeof setTimeout> | undefined
  #running = false
  #stopped = false

  constructor(provider: CloudflareEnvironmentProvider, options: { intervalMs?: number } = {}) {
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

export function cloudflareEnvironmentName(prefix: string, repository: string, randomId: string): string {
  const suffix = randomId.toLowerCase().replace(/[^a-z0-9]+/gu, '').slice(0, 12)
  if (!suffix) throw new Error('Cloudflare environment random identity must contain a letter or number')
  const repositoryName = (repository.split('/').pop() ?? repository)
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-+|-+$/gu, '') || 'repo'
  const available = 63 - prefix.length - suffix.length - 2
  if (available < 1) throw new Error('Cloudflare environment prefix and random identity leave no room for a repository name')
  const repo = repositoryName.slice(0, Math.max(1, available)).replace(/-+$/gu, '') || 'r'
  return `${prefix}-${repo}-${suffix}`
}

function isManagedNamespaceName(name: string, prefix: string): boolean {
  return name.startsWith(`${prefix}-`) && name.length <= 63
}

function compareDispatchNamespaces(left: CloudflareDispatchNamespace, right: CloudflareDispatchNamespace): number {
  const created = (left.created_on ?? '').localeCompare(right.created_on ?? '')
  return created || left.namespace_name.localeCompare(right.namespace_name)
}

function metadataWorkerSource(): string {
  return `export default { async fetch() { return new Response('not found', { status: 404 }); } };\n`
}

function environmentMetadataBindings(input: {
  id: string
  spec: CloudflareProvisionSpec
  createdAt: Date
  expiresAt: Date
  costBudget: number
  containerMaxInstances: number
  workerCpuMs: number
  workerSubrequests: number
}): CloudflareWorkerBinding[] {
  const values: Array<[string, string]> = [
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.environmentId, input.id],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.ownerId, input.spec.ownerId],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.customerId, input.spec.customerId],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.repository, input.spec.repository],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.createdAt, input.createdAt.toISOString()],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.expiresAt, input.expiresAt.toISOString()],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.maxRunCostUsd, String(input.costBudget)],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.containerMaxInstances, String(input.containerMaxInstances)],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.workerCpuMs, String(input.workerCpuMs)],
    [CLOUDFLARE_ENVIRONMENT_BINDINGS.workerSubrequests, String(input.workerSubrequests)],
  ]
  return values.map(([name, text]) => ({ name, type: 'plain_text', text }))
}

function parseFiniteNumber(value: string | undefined): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) throw new Error(`Cloudflare environment metadata number is invalid: ${value}`)
  return parsed
}

function cloneEnvironment(environment: Environment): Environment {
  return {
    ...environment,
    endpoints: { ...environment.endpoints },
    bindings: { ...environment.bindings },
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
