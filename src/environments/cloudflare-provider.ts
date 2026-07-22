import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { isAbsolute, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { z } from 'zod'

import { normalizeLogger } from '../logging.js'
import type {
  Environment,
  EnvironmentProvider,
  EnvironmentStatus,
  ProvisionEnvironmentSpec,
} from '../ports/environment.js'
import type { Logger } from '../ports/system.js'
import {
  CloudflareEnvironmentConfigSchema,
  type CloudflareEnvironmentConfig,
} from './cloudflare-config.js'

export const CLOUDFLARE_ENVIRONMENT_METADATA_WORKER = 'factory-metadata'
export const CLOUDFLARE_ENVIRONMENT_METADATA_BINDING = 'FACTORY_ENVIRONMENT_METADATA'
export const CLOUDFLARE_ENVIRONMENT_TAG = 'factory-environment'

const WORKER_NAME_PATTERN = /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u
const BINDING_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/u
const ENVIRONMENT_BINDING_NAMES = [
  'FACTORY_ENVIRONMENT_ID',
  'FACTORY_OWNER_ID',
  'FACTORY_CUSTOMER_ID',
  'FACTORY_REPOSITORY',
] as const
const RESERVED_ENVIRONMENT_BINDINGS = new Set<string>(ENVIRONMENT_BINDING_NAMES)

const endpointSchema = z.object({
  name: z.string().trim().min(1).max(64),
  path: z.string().startsWith('/').default('/'),
}).strict()

const secretSchema = z.object({
  name: z.string().regex(BINDING_NAME_PATTERN, 'must be a Worker binding identifier'),
  secretRef: z.string().trim().min(1).max(512),
}).strict()

const bindingSchema = z.object({
  type: z.string().trim().min(1),
  name: z.string().regex(BINDING_NAME_PATTERN, 'must be a Worker binding identifier'),
}).passthrough().superRefine((binding, context) => {
  if (binding.type === 'secret_text' || binding.type === 'secret_key') {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['type'],
      message: 'secret bindings must use secretRef entries, never inline values',
    })
  }
})

const workerSchema = z.object({
  name: z.string().trim().min(1).max(63).regex(WORKER_NAME_PATTERN),
  script: z.string().min(1),
  mainModule: z.string().trim().min(1).default('worker.mjs'),
  compatibilityDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/u).optional(),
  bindings: z.array(bindingSchema).default([]),
  secrets: z.array(secretSchema).default([]),
  endpoint: endpointSchema.optional(),
}).strict().superRefine((worker, context) => {
  if (worker.name === CLOUDFLARE_ENVIRONMENT_METADATA_WORKER) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['name'],
      message: `${CLOUDFLARE_ENVIRONMENT_METADATA_WORKER} is reserved for Factory ownership metadata`,
    })
  }
  validateBindingNames(worker.bindings, worker.secrets, context)
})

const wranglerProjectSchema = z.object({
  name: z.string().trim().min(1).max(63).regex(WORKER_NAME_PATTERN),
  cwd: z.string().trim().min(1),
  configPath: z.string().trim().min(1).optional(),
  containerApplications: z.array(z.string().trim().min(1)).default([]),
  secrets: z.array(secretSchema).default([]),
  endpoint: endpointSchema.optional(),
}).strict().superRefine((project, context) => {
  if (project.name === CLOUDFLARE_ENVIRONMENT_METADATA_WORKER) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['name'],
      message: `${CLOUDFLARE_ENVIRONMENT_METADATA_WORKER} is reserved for Factory ownership metadata`,
    })
  }
  validateBindingNames([], project.secrets, context)
  if (new Set(project.containerApplications).size !== project.containerApplications.length) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['containerApplications'],
      message: 'Container application names must be unique',
    })
  }
})

export const CloudflareEnvironmentStackSchema = z.object({
  workers: z.array(workerSchema).default([]),
  wranglerProjects: z.array(wranglerProjectSchema).default([]),
}).strict().superRefine((stack, context) => {
  const names = new Set<string>()
  const endpoints = new Set<string>()
  const containers = new Set<string>()
  ;[...stack.workers, ...stack.wranglerProjects].forEach((worker, index) => {
    if (names.has(worker.name)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['workers', index, 'name'],
        message: `duplicate Worker name ${JSON.stringify(worker.name)}`,
      })
    }
    names.add(worker.name)
    if (worker.endpoint) {
      if (endpoints.has(worker.endpoint.name)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['workers', index, 'endpoint', 'name'],
          message: `duplicate endpoint name ${JSON.stringify(worker.endpoint.name)}`,
        })
      }
      endpoints.add(worker.endpoint.name)
    }
  })
  stack.wranglerProjects.forEach((project, projectIndex) => {
    project.containerApplications.forEach((container, containerIndex) => {
      if (containers.has(container)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['wranglerProjects', projectIndex, 'containerApplications', containerIndex],
          message: `duplicate Container application ${JSON.stringify(container)}`,
        })
      }
      containers.add(container)
    })
  })
})

export type CloudflareWorkerBinding = z.infer<typeof bindingSchema>
export type CloudflareWorkerSpec = z.infer<typeof workerSchema>
export type CloudflareWranglerProject = z.infer<typeof wranglerProjectSchema>
export type CloudflareEnvironmentStack = z.infer<typeof CloudflareEnvironmentStackSchema>
export type CloudflareEnvironmentStackInput = z.input<typeof CloudflareEnvironmentStackSchema>

export interface CloudflareProvisionSpec extends Omit<ProvisionEnvironmentSpec, 'stack'> {
  stack: CloudflareEnvironmentStackInput
}

export interface CloudflareCredentials {
  accountId: string
  apiToken: string
}

export interface CloudflareCredentialResolver {
  resolve(reference: string): Promise<string>
}

export type CloudflareResourceValue = string | { value: string }

/** Adapter for SST-style injected Resource.* values; it never reads process.env. */
export class ResourceCloudflareCredentialResolver implements CloudflareCredentialResolver {
  readonly #resources: Readonly<Record<string, CloudflareResourceValue | undefined>>

  constructor(resources: Readonly<Record<string, CloudflareResourceValue | undefined>>) {
    this.#resources = resources
  }

  async resolve(reference: string): Promise<string> {
    const match = /^Resource\.([A-Za-z][A-Za-z0-9_]*)$/u.exec(reference)
    if (!match) throw new Error(`Unsupported Cloudflare secret reference ${reference}`)
    const resource = this.#resources[match[1]]
    const value = typeof resource === 'string' ? resource : resource?.value
    if (!value) throw new Error(`Cloudflare secret reference ${reference} is unavailable`)
    return value
  }
}

export interface CloudflareDispatchNamespace {
  id: string
  name: string
  createdAt?: string
  scriptCount?: number
  trustedWorkers?: boolean
}

export interface CloudflareDispatchWorkerUpload {
  name: string
  script: string
  mainModule: string
  compatibilityDate: string
  bindings: CloudflareWorkerBinding[]
  tags?: string[]
  limits?: { cpuMs: number; subrequests: number }
}

export interface CloudflareApi {
  createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace>
  getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined>
  listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]>
  deleteDispatchNamespace(name: string): Promise<void>
  uploadDispatchWorker(namespace: string, worker: CloudflareDispatchWorkerUpload): Promise<void>
  dispatchWorkerExists(namespace: string, worker: string): Promise<boolean>
  getDispatchWorkerBindings(namespace: string, worker: string): Promise<CloudflareWorkerBinding[] | undefined>
  putDispatchWorkerSecret(namespace: string, worker: string, name: string, value: string): Promise<void>
  deleteDispatchWorker(namespace: string, worker: string): Promise<void>
  uploadIngressWorker(name: string, namespace: string, script: string): Promise<void>
  ingressWorkerExists(name: string): Promise<boolean>
  enableIngressSubdomain(name: string): Promise<void>
  deleteIngressWorker(name: string): Promise<void>
  workersSubdomain(): Promise<string>
}

export interface CloudflareContainerApplication {
  id: string
  name: string
  state?: 'degraded' | 'provisioning' | 'active' | 'ready'
}

export interface CloudflareWranglerDeployInput {
  namespace: string
  project: CloudflareWranglerProject
  bindings: Record<string, string>
  secrets: Record<string, string>
  credentials: CloudflareCredentials
}

export interface CloudflareWranglerRuntime {
  deploy(input: CloudflareWranglerDeployInput): Promise<CloudflareContainerApplication[]>
  containerStatus(application: CloudflareContainerApplication, credentials: CloudflareCredentials): Promise<EnvironmentStatus>
  deleteContainer(application: CloudflareContainerApplication, credentials: CloudflareCredentials): Promise<void>
}

interface CloudflareEnvironmentMetadata {
  version: 1
  namespacePrefix: string
  namespaceId: string
  environment: Environment
  ownerId: string
  ingressWorker?: string
  workers: string[]
  containers: CloudflareContainerApplication[]
}

interface EnvironmentRecord {
  metadata: CloudflareEnvironmentMetadata
}

export interface CloudflareEnvironmentProviderOptions {
  config: CloudflareEnvironmentConfig | z.input<typeof CloudflareEnvironmentConfigSchema>
  credentialResolver: CloudflareCredentialResolver
  secretResolver?: CloudflareCredentialResolver
  api?: CloudflareApi
  apiFactory?: (credentials: CloudflareCredentials) => CloudflareApi
  wrangler?: CloudflareWranglerRuntime
  now?: () => Date
  randomId?: () => string
  ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
  logger?: Logger
}

export interface CloudflareReapReport {
  reaped: Array<{ id: string; reason: 'ttl-expired' | 'owner-gone' }>
  skipped: Array<{ id?: string; reason: string }>
}

/** Workers-for-Platforms implementation of the substrate-neutral environment port. */
export class CloudflareEnvironmentProvider implements EnvironmentProvider {
  readonly #config: CloudflareEnvironmentConfig
  readonly #credentialResolver: CloudflareCredentialResolver
  readonly #secretResolver: CloudflareCredentialResolver
  readonly #providedApi?: CloudflareApi
  readonly #apiFactory: (credentials: CloudflareCredentials) => CloudflareApi
  readonly #providedWrangler?: CloudflareWranglerRuntime
  readonly #now: () => Date
  readonly #randomId: () => string
  readonly #ownerIsAlive?: (ownerId: string) => Promise<boolean | undefined>
  readonly #logger?: Logger
  readonly #records = new Map<string, EnvironmentRecord>()
  #servicesPromise?: Promise<{ credentials: CloudflareCredentials; api: CloudflareApi; wrangler: CloudflareWranglerRuntime }>
  #provisionLock: Promise<void> = Promise.resolve()

  constructor(options: CloudflareEnvironmentProviderOptions) {
    this.#config = CloudflareEnvironmentConfigSchema.parse(options.config)
    this.#credentialResolver = options.credentialResolver
    this.#secretResolver = options.secretResolver ?? options.credentialResolver
    this.#providedApi = options.api
    this.#apiFactory = options.apiFactory ?? ((credentials) => new FetchCloudflareApi(credentials))
    this.#providedWrangler = options.wrangler
    this.#now = options.now ?? (() => new Date())
    this.#randomId = options.randomId ?? (() => randomUUID().replaceAll('-', '').slice(0, 10))
    this.#ownerIsAlive = options.ownerIsAlive
    this.#logger = options.logger ? normalizeLogger(options.logger) : undefined
  }

  async provision(specInput: CloudflareProvisionSpec): Promise<Environment> {
    return await this.#withProvisionLock(async () => await this.#provision(specInput))
  }

  async #provision(specInput: CloudflareProvisionSpec): Promise<Environment> {
    const stack = CloudflareEnvironmentStackSchema.parse(specInput.stack)
    const ttl = specInput.ttl ?? this.#config.ttlMs
    if (!Number.isInteger(ttl) || ttl < this.#config.minTtlMs || ttl > this.#config.maxTtlMs) {
      throw new Error(`Cloudflare environment ttl must be between ${this.#config.minTtlMs} and ${this.#config.maxTtlMs} milliseconds`)
    }
    const workerCount = stack.workers.length + stack.wranglerProjects.length
    const containerCount = stack.wranglerProjects.reduce((total, project) => total + project.containerApplications.length, 0)
    if (workerCount > this.#config.limits.maxWorkersPerEnvironment) {
      throw new Error(`Cloudflare environment declares ${workerCount} Workers; limit is ${this.#config.limits.maxWorkersPerEnvironment}`)
    }
    if (containerCount > this.#config.limits.maxContainersPerEnvironment) {
      throw new Error(`Cloudflare environment declares ${containerCount} Containers; limit is ${this.#config.limits.maxContainersPerEnvironment}`)
    }

    const { api, credentials, wrangler } = await this.#services()
    const active = (await api.listDispatchNamespaces()).filter(({ name }) => (
      name.startsWith(`${this.#config.namespacePrefix}-`)
    ))
    if (active.length >= this.#config.limits.maxActiveEnvironments) {
      throw new Error(`Cloudflare active environment limit ${this.#config.limits.maxActiveEnvironments} reached`)
    }

    const createdAt = this.#now()
    const id = environmentName(this.#config.namespacePrefix, specInput.repository, this.#randomId())
    const environmentBindings = {
      FACTORY_ENVIRONMENT_ID: id,
      FACTORY_OWNER_ID: specInput.ownerId,
      FACTORY_CUSTOMER_ID: specInput.customerId,
      FACTORY_REPOSITORY: specInput.repository,
    }
    const environment: Environment = {
      id,
      provider: 'cloudflare',
      dispatchNamespace: id,
      endpoints: {},
      bindings: {
        'factory.environmentId': id,
        'factory.ownerId': specInput.ownerId,
        'factory.customerId': specInput.customerId,
        'factory.repository': specInput.repository,
      },
      status: 'provisioning',
      createdAt: createdAt.toISOString(),
      ttl,
    }

    let namespace: CloudflareDispatchNamespace | undefined
    const workerNames = [...stack.workers, ...stack.wranglerProjects].map(({ name }) => name)
    const containers: CloudflareContainerApplication[] = []
    const metadata: CloudflareEnvironmentMetadata = {
      version: 1,
      namespacePrefix: this.#config.namespacePrefix,
      namespaceId: '',
      environment: cloneEnvironment(environment),
      ownerId: specInput.ownerId,
      workers: workerNames,
      containers,
    }
    try {
      namespace = await api.createDispatchNamespace(id)
      if (namespace.name !== id) {
        throw new Error(`Cloudflare returned dispatch namespace ${namespace.name} for requested identity ${id}`)
      }
      if (namespace.trustedWorkers === true) {
        throw new Error(`Cloudflare dispatch namespace ${id} is trusted; verification namespaces must be untrusted`)
      }
      metadata.namespaceId = namespace.id
      await api.uploadDispatchWorker(id, metadataWorkerUpload(metadata, createdAt))
      this.#records.set(id, { metadata })

      // Serialize same-process provisions and reconcile after creation so
      // independent providers that observed the same free slot cannot both
      // retain it. Existing namespaces always keep priority.
      const preexisting = new Set(active.map(({ name }) => name))
      const reconciled = (await api.listDispatchNamespaces())
        .filter(({ name }) => name.startsWith(`${this.#config.namespacePrefix}-`))
        .sort((left, right) => (
          Number(preexisting.has(right.name)) - Number(preexisting.has(left.name)) ||
          compareDispatchNamespaces(left, right)
        ))
      if (reconciled.length > this.#config.limits.maxActiveEnvironments &&
        reconciled.findIndex(({ name }) => name === id) >= this.#config.limits.maxActiveEnvironments) {
        throw new Error(
          `Cloudflare active environment limit ${this.#config.limits.maxActiveEnvironments} was exceeded by a concurrent provision`,
        )
      }

      for (const worker of stack.workers) {
        const bindings = mergeEnvironmentBindings(worker.bindings, environmentBindings)
        await api.uploadDispatchWorker(id, {
          name: worker.name,
          script: worker.script,
          mainModule: worker.mainModule,
          compatibilityDate: worker.compatibilityDate ?? compatibilityDate(createdAt),
          bindings,
          tags: [CLOUDFLARE_ENVIRONMENT_TAG, `environment:${id}`],
          limits: {
            cpuMs: this.#config.limits.workerCpuMs,
            subrequests: this.#config.limits.workerSubrequests,
          },
        })
        for (const secret of worker.secrets) {
          const value = await this.#secretResolver.resolve(secret.secretRef)
          await api.putDispatchWorkerSecret(id, worker.name, secret.name, value)
          environment.bindings[`${worker.name}.${secret.name}`] = 'secret_text'
        }
        for (const binding of bindings) environment.bindings[`${worker.name}.${binding.name}`] = binding.type
      }

      for (const project of stack.wranglerProjects) {
        const secrets = await resolveSecrets(project.secrets, this.#secretResolver)
        const deployedContainers = await wrangler.deploy({
          namespace: id,
          project,
          bindings: environmentBindings,
          secrets,
          credentials,
        })
        containers.push(...deployedContainers)
        metadata.containers = containers.map((container) => ({ ...container }))
        await api.uploadDispatchWorker(id, metadataWorkerUpload(metadata, createdAt))
        for (const key of Object.keys(environmentBindings)) environment.bindings[`${project.name}.${key}`] = 'plain_text'
        for (const key of Object.keys(secrets)) environment.bindings[`${project.name}.${key}`] = 'secret_text'
        for (const container of deployedContainers) {
          environment.bindings[`cloudflare.container.${container.name}`] = container.id
        }
      }

      const endpointTargets = [...stack.workers, ...stack.wranglerProjects].filter(
        (worker): worker is typeof worker & { endpoint: NonNullable<typeof worker.endpoint> } => Boolean(worker.endpoint),
      )
      if (endpointTargets.length > 0) {
        const ingressWorker = ingressWorkerName(id)
        metadata.ingressWorker = ingressWorker
        await api.uploadDispatchWorker(id, metadataWorkerUpload(metadata, createdAt))
        await api.uploadIngressWorker(
          ingressWorker,
          id,
          dispatchWorkerSource(endpointTargets.map(({ name }) => name)),
        )
        await api.enableIngressSubdomain(ingressWorker)
        const baseUrl = `https://${ingressWorker}.${await api.workersSubdomain()}.workers.dev`
        for (const worker of endpointTargets) {
          environment.endpoints[worker.endpoint.name] = `${baseUrl}/${worker.name}${worker.endpoint.path}`
        }
      }

      environment.status = await containerEnvironmentStatus(containers, wrangler, credentials)
      if (environment.status === 'failed') {
        throw new Error(`Cloudflare Container deployment failed for ${id}`)
      }
      metadata.environment = cloneEnvironment(environment)
      metadata.containers = containers.map((container) => ({ ...container }))
      await api.uploadDispatchWorker(id, metadataWorkerUpload(metadata, createdAt))
      this.#records.set(id, { metadata })
      this.#logger?.info?.('[cloudflare-environment] provisioned environment', {
        id,
        namespaceId: namespace.id,
        expiresAt: new Date(createdAt.getTime() + ttl).toISOString(),
      })
      return cloneEnvironment(environment)
    } catch (error) {
      if (namespace) {
        const cleanupErrors = await this.#cleanupPartial({ namespace, metadata, credentials, api, wrangler })
        if (cleanupErrors.length > 0) {
          throw new AggregateError([error, ...cleanupErrors], `Cloudflare provision failed and cleanup also failed for ${id}`)
        }
      }
      throw error
    }
  }

  async status(id: string): Promise<EnvironmentStatus> {
    const { api, credentials, wrangler } = await this.#services()
    const namespace = await api.getDispatchNamespace(id)
    if (!namespace) {
      // Retain an in-memory record so a subsequent idempotent destroy can
      // still reconcile account-global ingress and Container resources.
      return 'destroyed'
    }
    const record = await this.#loadRecord(id, api)
    assertMetadataIdentity(record.metadata, namespace, this.#config.namespacePrefix)
    for (const worker of [CLOUDFLARE_ENVIRONMENT_METADATA_WORKER, ...record.metadata.workers]) {
      if (!await api.dispatchWorkerExists(id, worker)) return 'failed'
    }
    if (record.metadata.ingressWorker && !await api.ingressWorkerExists(record.metadata.ingressWorker)) {
      return 'failed'
    }
    let sawProvisioning = false
    for (const container of record.metadata.containers) {
      const status = await wrangler.containerStatus(container, credentials)
      if (status === 'failed') return 'failed'
      if (status !== 'ready') sawProvisioning = true
    }
    return sawProvisioning ? 'provisioning' : 'ready'
  }

  async endpoints(id: string): Promise<Record<string, string>> {
    const { api } = await this.#services()
    const namespace = await api.getDispatchNamespace(id)
    if (!namespace) throw new Error(`Cloudflare environment ${id} does not exist`)
    const record = await this.#loadRecord(id, api)
    assertMetadataIdentity(record.metadata, namespace, this.#config.namespacePrefix)
    return { ...record.metadata.environment.endpoints }
  }

  async destroy(id: string): Promise<void> {
    const { api, credentials, wrangler } = await this.#services()
    const namespace = await api.getDispatchNamespace(id)
    if (!namespace) {
      const known = this.#records.get(id)
      if (known) await this.#destroyKnown(known.metadata, api, wrangler, credentials)
      return
    }
    const record = await this.#loadRecord(id, api)
    assertMetadataIdentity(record.metadata, namespace, this.#config.namespacePrefix)
    await this.#destroyKnown(record.metadata, api, wrangler, credentials)
  }

  async reap(): Promise<CloudflareReapReport> {
    const { api, credentials, wrangler } = await this.#services()
    const report: CloudflareReapReport = { reaped: [], skipped: [] }
    const nowMs = this.#now().getTime()
    const namespaces = (await api.listDispatchNamespaces()).filter(({ name }) => (
      name.startsWith(`${this.#config.namespacePrefix}-`)
    ))
    for (const namespace of namespaces) {
      let metadata: CloudflareEnvironmentMetadata
      try {
        metadata = (await this.#loadRecord(namespace.name, api)).metadata
        assertMetadataIdentity(metadata, namespace, this.#config.namespacePrefix)
      } catch (error) {
        report.skipped.push({ id: namespace.name, reason: `identity check failed: ${errorMessage(error)}` })
        continue
      }
      let reason: CloudflareReapReport['reaped'][number]['reason'] | undefined
      const expiresAt = Date.parse(metadata.environment.createdAt) + metadata.environment.ttl
      if (Number.isFinite(expiresAt) && expiresAt <= nowMs) {
        reason = 'ttl-expired'
      } else if (this.#ownerIsAlive) {
        try {
          if (await this.#ownerIsAlive(metadata.ownerId) === false) reason = 'owner-gone'
        } catch (error) {
          report.skipped.push({ id: namespace.name, reason: `owner check failed: ${errorMessage(error)}` })
          continue
        }
      }
      if (!reason) continue
      try {
        await this.#destroyKnown(metadata, api, wrangler, credentials)
        report.reaped.push({ id: namespace.name, reason })
        this.#logger?.warn?.('[cloudflare-environment] reaped environment', { id: namespace.name, reason })
      } catch (error) {
        report.skipped.push({ id: namespace.name, reason: `deletion failed: ${errorMessage(error)}` })
      }
    }
    return report
  }

  async #services(): Promise<{ credentials: CloudflareCredentials; api: CloudflareApi; wrangler: CloudflareWranglerRuntime }> {
    this.#servicesPromise ??= (async () => {
      const credentials = {
        accountId: await this.#credentialResolver.resolve(this.#config.accountId),
        apiToken: await this.#credentialResolver.resolve(this.#config.apiToken),
      }
      return {
        credentials,
        api: this.#providedApi ?? this.#apiFactory(credentials),
        wrangler: this.#providedWrangler ?? new WranglerCloudflareRuntime(),
      }
    })()
    return await this.#servicesPromise
  }

  async #loadRecord(id: string, api: CloudflareApi): Promise<EnvironmentRecord> {
    const known = this.#records.get(id)
    if (known) return known
    const bindings = await api.getDispatchWorkerBindings(id, CLOUDFLARE_ENVIRONMENT_METADATA_WORKER)
    const encoded = bindings?.find(({ name }) => name === CLOUDFLARE_ENVIRONMENT_METADATA_BINDING)?.text
    if (typeof encoded !== 'string') {
      throw new Error(`Cloudflare namespace ${id} has no Factory ownership metadata`)
    }
    let metadata: CloudflareEnvironmentMetadata
    try {
      metadata = parseMetadata(JSON.parse(encoded))
    } catch (error) {
      throw new Error(`Cloudflare namespace ${id} has invalid Factory ownership metadata: ${errorMessage(error)}`)
    }
    const record = { metadata }
    this.#records.set(id, record)
    return record
  }

  async #destroyKnown(
    metadata: CloudflareEnvironmentMetadata,
    api: CloudflareApi,
    wrangler: CloudflareWranglerRuntime,
    credentials: CloudflareCredentials,
  ): Promise<void> {
    const id = metadata.environment.id
    const current = await api.getDispatchNamespace(id)
    if (current) assertMetadataIdentity(metadata, current, this.#config.namespacePrefix)
    const cleanupErrors: unknown[] = []
    for (const container of [...metadata.containers].reverse()) {
      try {
        await wrangler.deleteContainer(container, credentials)
      } catch (error) {
        cleanupErrors.push(error)
      }
    }
    if (metadata.ingressWorker) {
      try {
        await api.deleteIngressWorker(metadata.ingressWorker)
      } catch (error) {
        cleanupErrors.push(error)
      }
    }
    if (current) {
      for (const script of metadata.workers) {
        try {
          await api.deleteDispatchWorker(id, script)
        } catch (error) {
          cleanupErrors.push(error)
        }
      }
    }
    if (cleanupErrors.length > 0) {
      throw new AggregateError(cleanupErrors, `Cloudflare environment cleanup failed for ${id}`)
    }

    if (!current) {
      metadata.environment.status = 'destroyed'
      this.#records.delete(id)
      return
    }

    const beforeNamespaceDelete = await this.#waitForMetadataOnlyNamespace(metadata, api)
    if (!beforeNamespaceDelete) {
      metadata.environment.status = 'destroyed'
      this.#records.delete(id)
      return
    }
    await api.deleteDispatchWorker(id, CLOUDFLARE_ENVIRONMENT_METADATA_WORKER)
    try {
      await api.deleteDispatchNamespace(id)
    } catch (error) {
      const afterFailure = await api.getDispatchNamespace(id)
      if (afterFailure) {
        assertMetadataIdentity(metadata, afterFailure, this.#config.namespacePrefix)
        try {
          await api.uploadDispatchWorker(id, metadataWorkerUpload(metadata, new Date(metadata.environment.createdAt)))
        } catch (restoreError) {
          throw new AggregateError(
            [error, restoreError],
            `Cloudflare namespace deletion and ownership metadata restoration failed for ${id}`,
          )
        }
        throw error
      }
    }
    metadata.environment.status = 'destroyed'
    this.#records.delete(id)
    this.#logger?.info?.('[cloudflare-environment] destroyed environment', { id, namespaceId: metadata.namespaceId })
  }

  async #cleanupPartial(input: {
    namespace: CloudflareDispatchNamespace
    metadata: CloudflareEnvironmentMetadata
    credentials: CloudflareCredentials
    api: CloudflareApi
    wrangler: CloudflareWranglerRuntime
  }): Promise<unknown[]> {
    try {
      const current = await input.api.getDispatchNamespace(input.namespace.name)
      if (!current || current.id !== input.namespace.id) {
        throw new Error(
          `Refusing partial cleanup for Cloudflare namespace ${input.namespace.name}: ownership identity changed`,
        )
      }
      if (!await input.api.dispatchWorkerExists(
        input.namespace.name,
        CLOUDFLARE_ENVIRONMENT_METADATA_WORKER,
      )) {
        await input.api.uploadDispatchWorker(
          input.namespace.name,
          metadataWorkerUpload(input.metadata, new Date(input.metadata.environment.createdAt)),
        )
      }
      await this.#destroyKnown(input.metadata, input.api, input.wrangler, input.credentials)
      return []
    } catch (error) {
      return [error]
    }
  }

  async #waitForMetadataOnlyNamespace(
    metadata: CloudflareEnvironmentMetadata,
    api: CloudflareApi,
  ): Promise<CloudflareDispatchNamespace | undefined> {
    const id = metadata.environment.id
    let current: CloudflareDispatchNamespace | undefined
    for (let attempt = 0; attempt < 20; attempt += 1) {
      current = await api.getDispatchNamespace(id)
      if (!current) return undefined
      assertMetadataIdentity(metadata, current, this.#config.namespacePrefix)
      if (current.scriptCount === 1 &&
        await api.dispatchWorkerExists(id, CLOUDFLARE_ENVIRONMENT_METADATA_WORKER)) {
        return current
      }
      if (attempt < 19) await new Promise((resolvePromise) => setTimeout(resolvePromise, 250))
    }
    throw new Error(
      `Refusing to delete Cloudflare namespace ${id}: expected only the ownership metadata Worker, ` +
      `found ${String(current?.scriptCount)} scripts`,
    )
  }

  async #withProvisionLock<T>(work: () => Promise<T>): Promise<T> {
    const previous = this.#provisionLock
    let release = (): void => undefined
    this.#provisionLock = new Promise<void>((resolvePromise) => { release = resolvePromise })
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
  readonly #logger?: Logger
  #timer?: ReturnType<typeof setTimeout>
  #inFlight?: Promise<void>
  #running = false
  #stopped = false

  constructor(provider: CloudflareEnvironmentProvider, options: { intervalMs?: number; logger?: Logger } = {}) {
    this.#provider = provider
    this.#intervalMs = options.intervalMs ?? 60_000
    this.#logger = options.logger ? normalizeLogger(options.logger) : undefined
  }

  start(): void {
    if (!this.#timer && !this.#stopped) this.#schedule(0)
  }

  async stop(): Promise<void> {
    this.#stopped = true
    if (this.#timer) clearTimeout(this.#timer)
    this.#timer = undefined
    await this.#inFlight
  }

  #schedule(delay: number): void {
    this.#timer = setTimeout(() => {
      this.#timer = undefined
      const tick = this.#tick()
      this.#inFlight = tick
      void tick.finally(() => {
        if (this.#inFlight === tick) this.#inFlight = undefined
      })
    }, delay)
  }

  async #tick(): Promise<void> {
    if (this.#stopped || this.#running) return
    this.#running = true
    try {
      await this.#provider.reap()
    } catch (error) {
      this.#logger?.error?.('[cloudflare-environment] reaper sweep failed', { error: errorMessage(error) })
    } finally {
      this.#running = false
      if (!this.#stopped) this.#schedule(this.#intervalMs)
    }
  }
}

interface CloudflareEnvelope<T> {
  success: boolean
  result: T
  errors?: Array<{ code?: number; message?: string }>
}

/** Minimal REST adapter kept entirely behind CloudflareEnvironmentProvider. */
export class FetchCloudflareApi implements CloudflareApi {
  readonly #credentials: CloudflareCredentials
  readonly #fetch: typeof globalThis.fetch
  readonly #baseUrl: string

  constructor(
    credentials: CloudflareCredentials,
    options: { fetch?: typeof globalThis.fetch; baseUrl?: string } = {},
  ) {
    this.#credentials = credentials
    this.#fetch = options.fetch ?? globalThis.fetch
    this.#baseUrl = options.baseUrl ?? 'https://api.cloudflare.com/client/v4'
  }

  async createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace> {
    const result = await this.#json<Record<string, unknown>>(
      'POST', this.#dispatchPath(), { json: { name, trusted_workers: false } },
    )
    return namespaceFromApi(result, name)
  }

  async getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined> {
    const result = await this.#json<Record<string, unknown>>(
      'GET', this.#dispatchPath(name), { allowNotFound: true },
    )
    return result ? namespaceFromApi(result, name) : undefined
  }

  async listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]> {
    const result = await this.#json<Array<Record<string, unknown>>>('GET', this.#dispatchPath())
    return result.map((namespace) => namespaceFromApi(namespace))
  }

  async deleteDispatchNamespace(name: string): Promise<void> {
    await this.#json('DELETE', this.#dispatchPath(name), { allowNotFound: true })
  }

  async uploadDispatchWorker(namespace: string, worker: CloudflareDispatchWorkerUpload): Promise<void> {
    const form = workerForm(worker)
    await this.#json('PUT', this.#dispatchScriptPath(namespace, worker.name), { body: form })
  }

  async dispatchWorkerExists(namespace: string, worker: string): Promise<boolean> {
    return await this.#exists(this.#dispatchScriptPath(namespace, worker))
  }

  async getDispatchWorkerBindings(
    namespace: string,
    worker: string,
  ): Promise<CloudflareWorkerBinding[] | undefined> {
    const result = await this.#json<CloudflareWorkerBinding[]>(
      'GET', `${this.#dispatchScriptPath(namespace, worker)}/bindings`, { allowNotFound: true },
    )
    if (!result) return undefined
    return Array.isArray(result) ? result.filter(isRecord).map((binding) => binding as CloudflareWorkerBinding) : []
  }

  async putDispatchWorkerSecret(namespace: string, worker: string, name: string, value: string): Promise<void> {
    await this.#json('PUT', `${this.#dispatchScriptPath(namespace, worker)}/secrets`, {
      json: { name, text: value, type: 'secret_text' },
    })
  }

  async deleteDispatchWorker(namespace: string, worker: string): Promise<void> {
    await this.#json('DELETE', this.#dispatchScriptPath(namespace, worker), { allowNotFound: true })
  }

  async uploadIngressWorker(name: string, namespace: string, script: string): Promise<void> {
    const form = workerForm({
      name,
      script,
      mainModule: 'dispatcher.mjs',
      compatibilityDate: compatibilityDate(new Date()),
      bindings: [{ type: 'dispatch_namespace', name: 'DISPATCHER', namespace }],
    })
    await this.#json('PUT', this.#workerPath(name), { body: form })
  }

  async ingressWorkerExists(name: string): Promise<boolean> {
    return await this.#exists(this.#workerPath(name))
  }

  async enableIngressSubdomain(name: string): Promise<void> {
    await this.#json('POST', `${this.#workerPath(name)}/subdomain`, {
      json: { enabled: true, previews_enabled: true },
    })
  }

  async deleteIngressWorker(name: string): Promise<void> {
    await this.#json('DELETE', this.#workerPath(name), { allowNotFound: true })
  }

  async workersSubdomain(): Promise<string> {
    const result = await this.#json<Record<string, unknown>>('GET', `${this.#accountPath()}/workers/subdomain`)
    if (typeof result.subdomain !== 'string' || !result.subdomain) {
      throw new Error('Cloudflare account has no workers.dev subdomain')
    }
    return result.subdomain
  }

  #accountPath(): string {
    return `/accounts/${encodeURIComponent(this.#credentials.accountId)}`
  }

  #dispatchPath(namespace?: string): string {
    const root = `${this.#accountPath()}/workers/dispatch/namespaces`
    return namespace ? `${root}/${encodeURIComponent(namespace)}` : root
  }

  #dispatchScriptPath(namespace: string, worker: string): string {
    return `${this.#dispatchPath(namespace)}/scripts/${encodeURIComponent(worker)}`
  }

  #workerPath(name: string): string {
    return `${this.#accountPath()}/workers/scripts/${encodeURIComponent(name)}`
  }

  async #exists(path: string): Promise<boolean> {
    const response = await this.#fetch(`${this.#baseUrl}${path}`, {
      headers: { Authorization: `Bearer ${this.#credentials.apiToken}` },
    })
    if (response.status === 404) return false
    if (!response.ok) throw await cloudflareHttpError(response, path)
    return true
  }

  async #json<T = unknown>(
    method: string,
    path: string,
    options: { json?: unknown; body?: FormData | string; allowNotFound?: boolean } = {},
  ): Promise<T> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.#credentials.apiToken}`,
    }
    let body = options.body
    if (options.json !== undefined) {
      headers['Content-Type'] = 'application/json'
      body = JSON.stringify(options.json)
    }
    const response = await this.#fetch(`${this.#baseUrl}${path}`, { method, headers, body })
    if (response.status === 404 && options.allowNotFound) return undefined as T
    if (!response.ok) throw await cloudflareHttpError(response, path)
    if (response.status === 204) return undefined as T
    const text = await response.text()
    if (!text) return undefined as T
    let envelope: CloudflareEnvelope<T>
    try {
      envelope = JSON.parse(text) as CloudflareEnvelope<T>
    } catch (error) {
      throw new Error(`Cloudflare API returned invalid JSON for ${method} ${path}: ${errorMessage(error)}`)
    }
    if (envelope.success !== true) {
      throw new Error(`Cloudflare API rejected ${method} ${path}: ${cloudflareErrorMessages(envelope.errors)}`)
    }
    return envelope.result
  }
}

export interface WranglerCommandResult {
  stdout: string
  stderr: string
}

export interface WranglerCommandRunner {
  run(args: string[], options: { cwd?: string; env: NodeJS.ProcessEnv }): Promise<WranglerCommandResult>
}

export interface WranglerCloudflareRuntimeOptions {
  command?: string
  commandArgs?: string[]
  runner?: WranglerCommandRunner
}

/** Wrangler-only Container deployment is isolated behind this provider adapter. */
export class WranglerCloudflareRuntime implements CloudflareWranglerRuntime {
  readonly #command: string
  readonly #commandArgs: string[]
  readonly #runner: WranglerCommandRunner

  constructor(options: WranglerCloudflareRuntimeOptions = {}) {
    this.#command = options.command ?? process.execPath
    this.#commandArgs = options.commandArgs ?? (
      options.command ? [] : [fileURLToPath(import.meta.resolve('wrangler/bin/wrangler.js'))]
    )
    this.#runner = options.runner ?? defaultWranglerCommandRunner(this.#command, this.#commandArgs)
  }

  async deploy(input: CloudflareWranglerDeployInput): Promise<CloudflareContainerApplication[]> {
    const cwd = resolve(input.project.cwd)
    const expectedContainers = new Set(input.project.containerApplications)
    const before = await this.#listContainers(input.credentials)
    const collisions = before.filter((container) => expectedContainers.has(container.name))
    if (collisions.length > 0) {
      throw new Error(
        `Refusing to deploy ${input.project.name}: Container application name already exists (${collisions.map(({ name }) => name).join(', ')})`,
      )
    }
    const beforeIds = new Set(before.map(({ id }) => id))
    const temporary = await mkdtemp(join(tmpdir(), 'factory-cloudflare-secrets-'))
    try {
      const args = ['deploy', '--name', input.project.name, '--dispatch-namespace', input.namespace]
      if (input.project.configPath) {
        const configPath = isAbsolute(input.project.configPath)
          ? input.project.configPath
          : resolve(cwd, input.project.configPath)
        args.push('--config', configPath)
      }
      for (const [name, value] of Object.entries(input.bindings)) args.push('--var', `${name}:${value}`)
      if (Object.keys(input.secrets).length > 0) {
        const secretsPath = join(temporary, 'secrets.json')
        await writeFile(secretsPath, JSON.stringify(input.secrets), { mode: 0o600 })
        args.push('--secrets-file', secretsPath)
      }
      if (expectedContainers.size > 0) args.push('--containers-rollout', 'immediate')
      try {
        await this.#run(args, input.credentials, cwd)
        const after = await this.#listContainers(input.credentials)
        const created = after.filter((container) => !beforeIds.has(container.id))
        const matched = created.filter((container) => expectedContainers.has(container.name))
        const unexpected = created.filter((container) => !expectedContainers.has(container.name))
        if (unexpected.length > 0) {
          throw new Error(
            `Wrangler deployed undeclared Container applications for ${input.project.name}: ` +
            unexpected.map(({ name }) => name).join(', '),
          )
        }
        for (const name of expectedContainers) {
          if (!matched.some((container) => container.name === name)) {
            throw new Error(`Wrangler deployed ${input.project.name} but Container application ${name} was not found`)
          }
        }
        return matched
      } catch (error) {
        const cleanupErrors: unknown[] = []
        if (expectedContainers.size > 0) {
          try {
            const created = (await this.#listContainers(input.credentials)).filter((container) => (
              expectedContainers.has(container.name) && !beforeIds.has(container.id)
            ))
            for (const container of created) {
              try { await this.deleteContainer(container, input.credentials) } catch (cleanupError) { cleanupErrors.push(cleanupError) }
            }
          } catch (cleanupError) {
            cleanupErrors.push(cleanupError)
          }
        }
        if (cleanupErrors.length > 0) {
          throw new AggregateError([error, ...cleanupErrors], `Wrangler deployment and Container cleanup failed for ${input.project.name}`)
        }
        throw error
      }
    } finally {
      await rm(temporary, { recursive: true, force: true })
    }
  }

  async containerStatus(
    application: CloudflareContainerApplication,
    credentials: CloudflareCredentials,
  ): Promise<EnvironmentStatus> {
    const containers = await this.#listContainers(credentials)
    const current = containers.find(({ id }) => id === application.id)
    if (!current || current.name !== application.name || current.state === 'degraded') return 'failed'
    if (current.state === 'provisioning') return 'provisioning'
    return 'ready'
  }

  async deleteContainer(
    application: CloudflareContainerApplication,
    credentials: CloudflareCredentials,
  ): Promise<void> {
    const containers = await this.#listContainers(credentials)
    const current = containers.find(({ id }) => id === application.id)
    if (!current) return
    if (current.name !== application.name) {
      throw new Error(`Refusing to delete Cloudflare Container ${application.id}: application identity changed`)
    }
    await this.#run(['containers', 'delete', application.id], credentials)
  }

  async #listContainers(credentials: CloudflareCredentials): Promise<CloudflareContainerApplication[]> {
    const result = await this.#run(['containers', 'list', '--json', '--per-page', '1000'], credentials)
    return parseContainerList(result.stdout)
  }

  async #run(args: string[], credentials: CloudflareCredentials, cwd?: string): Promise<WranglerCommandResult> {
    const inheritedEnvironment = inheritedWranglerEnvironment(process.env)
    return await this.#runner.run(args, {
      cwd,
      env: {
        // Do not expose unrelated host secrets to repository-controlled build
        // commands. Credentials come only from the scoped Resource.* values.
        ...inheritedEnvironment,
        CLOUDFLARE_ACCOUNT_ID: credentials.accountId,
        CLOUDFLARE_API_TOKEN: credentials.apiToken,
      },
    })
  }
}

function inheritedWranglerEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const allowed = [
    'PATH', 'HOME', 'TMPDIR', 'TMP', 'TEMP',
    'DOCKER_HOST', 'DOCKER_CONFIG',
    'CI', 'NO_COLOR', 'FORCE_COLOR',
    'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY',
    'NODE_EXTRA_CA_CERTS', 'SSL_CERT_FILE',
  ]
  return Object.fromEntries(allowed.flatMap((name) => (
    environment[name] === undefined ? [] : [[name, environment[name]]]
  )))
}

function defaultWranglerCommandRunner(command: string, commandArgs: string[]): WranglerCommandRunner {
  return {
    run: async (args, options) => await new Promise((resolvePromise, reject) => {
      const child = spawn(command, [...commandArgs, ...args], {
        cwd: options.cwd,
        env: options.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let stdout = ''
      let stderr = ''
      child.stdout.setEncoding('utf8')
      child.stderr.setEncoding('utf8')
      child.stdout.on('data', (chunk: string) => { stdout += chunk })
      child.stderr.on('data', (chunk: string) => { stderr += chunk })
      child.once('error', reject)
      child.once('close', (code) => {
        if (code === 0) resolvePromise({ stdout, stderr })
        else reject(new Error(`wrangler ${args.join(' ')} failed (${code}): ${stderr || stdout}`))
      })
    }),
  }
}

function workerForm(worker: CloudflareDispatchWorkerUpload): FormData {
  const metadata: Record<string, unknown> = {
    main_module: worker.mainModule,
    compatibility_date: worker.compatibilityDate,
    bindings: worker.bindings,
    ...(worker.tags ? { tags: worker.tags } : {}),
    ...(worker.limits ? { limits: { cpu_ms: worker.limits.cpuMs, subrequests: worker.limits.subrequests } } : {}),
  }
  const form = new FormData()
  form.append('metadata', new Blob([JSON.stringify(metadata)], { type: 'application/json' }))
  form.append(worker.mainModule, new Blob([worker.script], { type: 'application/javascript+module' }), worker.mainModule)
  return form
}

function namespaceFromApi(value: Record<string, unknown>, fallbackName?: string): CloudflareDispatchNamespace {
  const id = value.namespace_id
  const name = value.namespace_name ?? fallbackName
  if (typeof id !== 'string' || !id || typeof name !== 'string' || !name) {
    throw new Error('Cloudflare API returned a dispatch namespace without its stable identity')
  }
  return {
    id,
    name,
    ...(typeof value.created_on === 'string' ? { createdAt: value.created_on } : {}),
    ...(typeof value.script_count === 'number' ? { scriptCount: value.script_count } : {}),
    ...(typeof value.trusted_workers === 'boolean' ? { trustedWorkers: value.trusted_workers } : {}),
  }
}

async function cloudflareHttpError(response: Response, path: string): Promise<Error> {
  const body = await response.text().catch(() => '')
  return new Error(`Cloudflare API ${response.status} for ${path}${body ? `: ${body}` : ''}`)
}

function cloudflareErrorMessages(errors: CloudflareEnvelope<unknown>['errors']): string {
  return errors?.map(({ code, message }) => [code, message].filter(Boolean).join(' ')).join('; ') || 'unknown error'
}

function parseContainerList(stdout: string): CloudflareContainerApplication[] {
  let value: unknown
  try {
    value = JSON.parse(stdout)
  } catch (error) {
    throw new Error(`wrangler containers list returned invalid JSON: ${errorMessage(error)}`)
  }
  const entries = Array.isArray(value) ? value : Array.isArray(asRecord(value).result) ? asRecord(value).result as unknown[] : undefined
  if (!entries) throw new Error('wrangler containers list returned no application array')
  return entries.flatMap((entry) => {
    const record = asRecord(entry)
    const id = record.id
    const name = record.name
    const state = record.state
    return typeof id === 'string' && typeof name === 'string'
      ? [{
          id,
          name,
          ...(state === 'degraded' || state === 'provisioning' || state === 'active' || state === 'ready'
            ? { state }
            : {}),
        }]
      : []
  })
}

function mergeEnvironmentBindings(
  bindings: CloudflareWorkerBinding[],
  values: Record<string, string>,
): CloudflareWorkerBinding[] {
  const reserved = new Set(Object.keys(values))
  for (const binding of bindings) {
    if (reserved.has(binding.name)) throw new Error(`Worker binding ${binding.name} is reserved by Factory`)
  }
  return [
    ...bindings,
    ...Object.entries(values).map(([name, text]) => ({ type: 'plain_text', name, text })),
  ]
}

function validateBindingNames(
  bindings: Array<{ name?: string }>,
  secrets: Array<{ name?: string }>,
  context: z.RefinementCtx,
): void {
  const seen = new Set<string>()
  for (const [kind, entries] of [['bindings', bindings], ['secrets', secrets]] as const) {
    entries.forEach(({ name }, index) => {
      if (typeof name !== 'string') return
      if (RESERVED_ENVIRONMENT_BINDINGS.has(name)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: [kind, index, 'name'],
          message: `${name} is reserved for Factory environment identity`,
        })
      }
      if (seen.has(name)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: [kind, index, 'name'],
          message: `duplicate Worker binding ${JSON.stringify(name)}`,
        })
      }
      seen.add(name)
    })
  }
}

async function containerEnvironmentStatus(
  containers: CloudflareContainerApplication[],
  wrangler: CloudflareWranglerRuntime,
  credentials: CloudflareCredentials,
): Promise<EnvironmentStatus> {
  let status: EnvironmentStatus = 'ready'
  for (const container of containers) {
    const current = await wrangler.containerStatus(container, credentials)
    if (current === 'failed') return 'failed'
    if (current !== 'ready') status = 'provisioning'
  }
  return status
}

async function resolveSecrets(
  secrets: Array<{ name: string; secretRef: string }>,
  resolver: CloudflareCredentialResolver,
): Promise<Record<string, string>> {
  const resolved: Record<string, string> = {}
  for (const secret of secrets) resolved[secret.name] = await resolver.resolve(secret.secretRef)
  return resolved
}

function metadataWorkerUpload(
  metadata: CloudflareEnvironmentMetadata,
  createdAt: Date,
): CloudflareDispatchWorkerUpload {
  return {
    name: CLOUDFLARE_ENVIRONMENT_METADATA_WORKER,
    mainModule: 'metadata.mjs',
    compatibilityDate: compatibilityDate(createdAt),
    script: 'export default { fetch() { return new Response("Factory environment metadata", { status: 404 }) } }',
    bindings: [{
      type: 'plain_text',
      name: CLOUDFLARE_ENVIRONMENT_METADATA_BINDING,
      text: JSON.stringify(metadata),
    }],
    tags: [CLOUDFLARE_ENVIRONMENT_TAG, `environment:${metadata.environment.id}`],
  }
}

function parseMetadata(value: unknown): CloudflareEnvironmentMetadata {
  const record = asRecord(value)
  const environment = asRecord(record.environment)
  if (
    record.version !== 1 ||
    typeof record.namespacePrefix !== 'string' ||
    typeof record.namespaceId !== 'string' ||
    typeof record.ownerId !== 'string' ||
    typeof environment.id !== 'string' ||
    environment.provider !== 'cloudflare' ||
    !Array.isArray(record.workers) || !record.workers.every((worker) => typeof worker === 'string') ||
    !Array.isArray(record.containers)
  ) {
    throw new Error('unsupported metadata shape')
  }
  const containers = record.containers.map((container) => {
    const item = asRecord(container)
    if (typeof item.id !== 'string' || typeof item.name !== 'string') throw new Error('invalid Container identity')
    return { id: item.id, name: item.name }
  })
  return {
    version: 1,
    namespacePrefix: record.namespacePrefix,
    namespaceId: record.namespaceId,
    ownerId: record.ownerId,
    ...(typeof record.ingressWorker === 'string' ? { ingressWorker: record.ingressWorker } : {}),
    workers: record.workers as string[],
    containers,
    environment: valueEnvironment(environment),
  }
}

function valueEnvironment(value: Record<string, unknown>): Environment {
  if (
    typeof value.id !== 'string' ||
    typeof value.dispatchNamespace !== 'string' ||
    typeof value.createdAt !== 'string' ||
    typeof value.ttl !== 'number' ||
    typeof value.status !== 'string'
  ) throw new Error('invalid environment metadata')
  return {
    id: value.id,
    provider: 'cloudflare',
    dispatchNamespace: value.dispatchNamespace,
    endpoints: stringRecord(value.endpoints),
    bindings: stringRecord(value.bindings),
    status: value.status as EnvironmentStatus,
    createdAt: value.createdAt,
    ttl: value.ttl,
  }
}

function assertMetadataIdentity(
  metadata: CloudflareEnvironmentMetadata,
  namespace: CloudflareDispatchNamespace,
  namespacePrefix: string,
): void {
  if (
    metadata.namespacePrefix !== namespacePrefix ||
    metadata.environment.id !== namespace.name ||
    metadata.environment.dispatchNamespace !== namespace.name ||
    metadata.namespaceId !== namespace.id
  ) {
    throw new Error(`Refusing to manage Cloudflare namespace ${namespace.name}: ownership identity does not match`)
  }
}

function dispatchWorkerSource(workers: string[]): string {
  return `const allowed = new Set(${JSON.stringify(workers)});
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split('/').filter(Boolean);
    const workerName = parts.shift();
    if (!workerName || !allowed.has(workerName)) return new Response('Not found', { status: 404 });
    url.pathname = '/' + parts.join('/');
    return env.DISPATCHER.get(workerName).fetch(new Request(url, request));
  }
}`
}

function environmentName(prefix: string, repository: string, suffix: string): string {
  const repo = repository.split('/').at(-1)?.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'environment'
  const safeSuffix = suffix.toLowerCase().replace(/[^a-z0-9]+/gu, '').slice(0, 12) || randomUUID().replaceAll('-', '').slice(0, 10)
  const available = 30 - prefix.length - safeSuffix.length
  if (available < 1) throw new Error('Cloudflare namespace prefix and random identity leave no room for a repository name')
  return `${prefix}-${repo.slice(0, Math.max(1, available)).replace(/-$/u, '')}-${safeSuffix}`
}

function compareDispatchNamespaces(
  left: CloudflareDispatchNamespace,
  right: CloudflareDispatchNamespace,
): number {
  return (left.createdAt ?? '').localeCompare(right.createdAt ?? '') || left.name.localeCompare(right.name)
}

function ingressWorkerName(id: string): string {
  return `${id.slice(0, 55).replace(/-$/u, '')}-ingress`
}

function compatibilityDate(date: Date): string {
  return date.toISOString().slice(0, 10)
}

function cloneEnvironment(environment: Environment): Environment {
  return {
    ...environment,
    endpoints: { ...environment.endpoints },
    bindings: { ...environment.bindings },
  }
}

function stringRecord(value: unknown): Record<string, string> {
  const record = asRecord(value)
  if (!Object.values(record).every((item) => typeof item === 'string')) throw new Error('expected string record')
  return record as Record<string, string>
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {}
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
