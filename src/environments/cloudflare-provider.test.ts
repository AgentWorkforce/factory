import { describe, expect, it, vi } from 'vitest'

import {
  CLOUDFLARE_ENVIRONMENT_METADATA_BINDING,
  CLOUDFLARE_ENVIRONMENT_METADATA_WORKER,
  CloudflareEnvironmentProvider,
  ResourceCloudflareCredentialResolver,
  WranglerCloudflareRuntime,
  type CloudflareApi,
  type CloudflareContainerApplication,
  type CloudflareCredentials,
  type CloudflareDispatchNamespace,
  type CloudflareDispatchWorkerUpload,
  type CloudflareWranglerDeployInput,
  type CloudflareWranglerRuntime,
  type WranglerCommandRunner,
} from './cloudflare-provider.js'
import type { EnvironmentStatus } from '../ports/environment.js'

interface FakeWorker {
  upload: CloudflareDispatchWorkerUpload
  secrets: Record<string, string>
}

class FakeCloudflareApi implements CloudflareApi {
  readonly namespaces = new Map<string, CloudflareDispatchNamespace>()
  readonly workers = new Map<string, Map<string, FakeWorker>>()
  readonly ingress = new Map<string, { namespace: string; script: string }>()
  nextNamespaceId = 1

  async createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace> {
    if (this.namespaces.has(name)) throw new Error('namespace collision')
    const namespace = { id: `namespace-${this.nextNamespaceId++}`, name, createdAt: new Date().toISOString() }
    this.namespaces.set(name, namespace)
    this.workers.set(name, new Map())
    return namespace
  }

  async getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined> {
    const namespace = this.namespaces.get(name)
    return namespace ? { ...namespace } : undefined
  }

  async listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]> {
    return [...this.namespaces.values()].map((namespace) => ({ ...namespace }))
  }

  async deleteDispatchNamespace(name: string): Promise<void> {
    if ((this.workers.get(name)?.size ?? 0) > 0) throw new Error('namespace still has Workers')
    this.namespaces.delete(name)
    this.workers.delete(name)
  }

  async uploadDispatchWorker(namespace: string, worker: CloudflareDispatchWorkerUpload): Promise<void> {
    const workers = this.workers.get(namespace)
    if (!workers) throw new Error('namespace missing')
    workers.set(worker.name, { upload: structuredClone(worker), secrets: {} })
  }

  async dispatchWorkerExists(namespace: string, worker: string): Promise<boolean> {
    return this.workers.get(namespace)?.has(worker) ?? false
  }

  async listDispatchWorkers(namespace: string): Promise<string[]> {
    return [...(this.workers.get(namespace)?.keys() ?? [])]
  }

  async getDispatchWorkerBindings(
    namespace: string,
    worker: string,
  ): Promise<CloudflareDispatchWorkerUpload['bindings'] | undefined> {
    const value = this.workers.get(namespace)?.get(worker)
    return value ? structuredClone(value.upload.bindings) : undefined
  }

  async putDispatchWorkerSecret(namespace: string, worker: string, name: string, value: string): Promise<void> {
    const target = this.workers.get(namespace)?.get(worker)
    if (!target) throw new Error('Worker missing')
    target.secrets[name] = value
  }

  async deleteDispatchWorker(namespace: string, worker: string): Promise<void> {
    this.workers.get(namespace)?.delete(worker)
  }

  async uploadIngressWorker(name: string, namespace: string, script: string): Promise<void> {
    this.ingress.set(name, { namespace, script })
  }

  async ingressWorkerExists(name: string): Promise<boolean> {
    return this.ingress.has(name)
  }

  async enableIngressSubdomain(name: string): Promise<void> {
    if (!this.ingress.has(name)) throw new Error('ingress missing')
  }

  async deleteIngressWorker(name: string): Promise<void> {
    this.ingress.delete(name)
  }

  async workersSubdomain(): Promise<string> {
    return 'factory-test'
  }
}

class FakeWranglerRuntime implements CloudflareWranglerRuntime {
  readonly applications = new Map<string, CloudflareContainerApplication>()
  readonly #api?: FakeCloudflareApi

  constructor(api?: FakeCloudflareApi) {
    this.#api = api
  }

  readonly deploy = vi.fn(async (input: CloudflareWranglerDeployInput) => {
    this.#api?.workers.get(input.namespace)?.set(input.project.name, {
      upload: {
        name: input.project.name,
        script: 'wrangler deployment',
        mainModule: 'worker.mjs',
        compatibilityDate: '2026-07-21',
        bindings: Object.entries(input.bindings).map(([name, text]) => ({ type: 'plain_text', name, text })),
      },
      secrets: { ...input.secrets },
    })
    return input.project.containerApplications.map((name, index) => {
      const application = { id: `${input.namespace}-container-${index}`, name }
      this.applications.set(application.id, application)
      return application
    })
  })
  readonly deleteContainer = vi.fn(async (
    application: CloudflareContainerApplication,
    _credentials: CloudflareCredentials,
  ) => {
    const current = this.applications.get(application.id)
    if (current && current.name !== application.name) throw new Error('identity changed')
    this.applications.delete(application.id)
  })
  readonly containerStatus = vi.fn(async (
    application: CloudflareContainerApplication,
    _credentials: CloudflareCredentials,
  ): Promise<EnvironmentStatus> => this.applications.has(application.id) ? 'ready' : 'failed')
}

const resources = (extra: Record<string, string> = {}) => new ResourceCloudflareCredentialResolver({
  CloudflareAccountId: 'account-id',
  CloudflareApiToken: { value: 'api-token' },
  WorkerSecret: 'resolved-worker-secret',
  ProjectSecret: 'resolved-project-secret',
  ...extra,
})

const config = (overrides: Record<string, unknown> = {}) => ({
  accountId: 'Resource.CloudflareAccountId',
  apiToken: 'Resource.CloudflareApiToken',
  ...overrides,
})

describe('ResourceCloudflareCredentialResolver', () => {
  it('resolves injected Resource.* values without an environment fallback', async () => {
    const resolver = resources()
    await expect(resolver.resolve('Resource.CloudflareApiToken')).resolves.toBe('api-token')
    await expect(resolver.resolve('env:CLOUDFLARE_API_TOKEN')).rejects.toThrow('Unsupported')
    await expect(resolver.resolve('Resource.Missing')).rejects.toThrow('unavailable')
  })
})

describe('CloudflareEnvironmentProvider', () => {
  it('provisions a namespace with per-environment bindings, secrets, ingress, and Containers', async () => {
    const api = new FakeCloudflareApi()
    const wrangler = new FakeWranglerRuntime(api)
    const provider = new CloudflareEnvironmentProvider({
      config: config(),
      credentialResolver: resources(),
      api,
      wrangler,
      now: () => new Date('2026-07-21T10:00:00.000Z'),
      randomId: () => 'abc123',
    })

    const environment = await provider.provision({
      customerId: 'customer-a',
      repository: 'AgentWorkforce/factory',
      ownerId: 'run-142',
      ttl: 60_000,
      stack: {
        workers: [{
          name: 'api',
          script: 'export default { fetch(_request, env) { return Response.json(env) } }',
          bindings: [{ type: 'plain_text', name: 'FEATURE_FLAG', text: 'enabled' }],
          secrets: [{ name: 'API_SECRET', secretRef: 'Resource.WorkerSecret' }],
          endpoint: { name: 'api', path: '/health' },
        }],
        wranglerProjects: [{
          name: 'container-api',
          cwd: '.',
          containerApplications: ['factory-container-api'],
          secrets: [{ name: 'PROJECT_SECRET', secretRef: 'Resource.ProjectSecret' }],
          endpoint: { name: 'container', path: '/' },
        }],
      },
    })

    expect(environment).toMatchObject({
      provider: 'cloudflare',
      dispatchNamespace: 'factory-factory-abc123',
      status: 'ready',
      ttl: 60_000,
      endpoints: {
        api: 'https://factory-factory-abc123-ingress.factory-test.workers.dev/api/health',
        container: 'https://factory-factory-abc123-ingress.factory-test.workers.dev/container-api/',
      },
    })
    const worker = api.workers.get(environment.id)?.get('api')
    expect(worker?.upload.bindings).toEqual(expect.arrayContaining([
      { type: 'plain_text', name: 'FEATURE_FLAG', text: 'enabled' },
      { type: 'plain_text', name: 'FACTORY_ENVIRONMENT_ID', text: environment.id },
      { type: 'plain_text', name: 'FACTORY_OWNER_ID', text: 'run-142' },
      { type: 'plain_text', name: 'FACTORY_CUSTOMER_ID', text: 'customer-a' },
      { type: 'plain_text', name: 'FACTORY_REPOSITORY', text: 'AgentWorkforce/factory' },
    ]))
    expect(worker?.secrets).toEqual({ API_SECRET: 'resolved-worker-secret' })
    expect(api.workers.get(environment.id)?.get(CLOUDFLARE_ENVIRONMENT_METADATA_WORKER)?.upload.bindings)
      .toContainEqual(expect.objectContaining({ name: CLOUDFLARE_ENVIRONMENT_METADATA_BINDING }))
    expect(wrangler.deploy).toHaveBeenCalledWith(expect.objectContaining({
      namespace: environment.id,
      bindings: expect.objectContaining({ FACTORY_ENVIRONMENT_ID: environment.id }),
      secrets: { PROJECT_SECRET: 'resolved-project-secret' },
    }))
    await expect(provider.status(environment.id)).resolves.toBe('ready')
    await expect(provider.endpoints(environment.id)).resolves.toEqual(environment.endpoints)
  })

  it('destroys Container applications, ingress, scripts, and namespace idempotently', async () => {
    const api = new FakeCloudflareApi()
    const wrangler = new FakeWranglerRuntime(api)
    const provider = new CloudflareEnvironmentProvider({
      config: config(), credentialResolver: resources(), api, wrangler, randomId: () => 'destroy1',
    })
    const environment = await provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'run',
      stack: {
        wranglerProjects: [{
          name: 'app', cwd: '.', containerApplications: ['container-app'], endpoint: { name: 'app' },
        }],
      },
    })

    await provider.destroy(environment.id)
    await provider.destroy(environment.id)

    expect(api.namespaces.has(environment.id)).toBe(false)
    expect(api.workers.has(environment.id)).toBe(false)
    expect(api.ingress.size).toBe(0)
    expect(wrangler.applications.size).toBe(0)
    expect(wrangler.deleteContainer).toHaveBeenCalledTimes(1)
    await expect(provider.status(environment.id)).resolves.toBe('destroyed')
  })

  it('reaps expired and orphaned environments while retaining live owners', async () => {
    const api = new FakeCloudflareApi()
    const wrangler = new FakeWranglerRuntime()
    let now = Date.parse('2026-07-21T10:00:00.000Z')
    const liveOwners = new Set(['live-owner'])
    const provider = new CloudflareEnvironmentProvider({
      config: config({ limits: { maxActiveEnvironments: 5 } }),
      credentialResolver: resources(), api, wrangler,
      now: () => new Date(now),
      randomId: (() => {
        const ids = ['expired1', 'orphaned1', 'live1']
        return () => ids.shift() ?? 'fallback'
      })(),
      ownerIsAlive: async (owner) => liveOwners.has(owner),
    })
    const expired = await provider.provision({
      customerId: 'c', repository: 'o/r', ownerId: 'live-owner', ttl: 1_000, stack: { workers: [] },
    })
    const orphaned = await provider.provision({
      customerId: 'c', repository: 'o/r', ownerId: 'dead-owner', ttl: 60_000, stack: { workers: [] },
    })
    const live = await provider.provision({
      customerId: 'c', repository: 'o/r', ownerId: 'live-owner', ttl: 60_000, stack: { workers: [] },
    })
    now += 2_000

    const report = await provider.reap()

    expect(report.reaped).toEqual(expect.arrayContaining([
      { id: expired.id, reason: 'ttl-expired' },
      { id: orphaned.id, reason: 'owner-gone' },
    ]))
    expect(api.namespaces.has(expired.id)).toBe(false)
    expect(api.namespaces.has(orphaned.id)).toBe(false)
    expect(api.namespaces.has(live.id)).toBe(true)
  })

  it('refuses to reap when the namespace UUID no longer matches persisted ownership', async () => {
    const api = new FakeCloudflareApi()
    let now = Date.parse('2026-07-21T10:00:00.000Z')
    const provider = new CloudflareEnvironmentProvider({
      config: config(), credentialResolver: resources(), api, wrangler: new FakeWranglerRuntime(),
      now: () => new Date(now), randomId: () => 'identity1',
    })
    const environment = await provider.provision({
      customerId: 'c', repository: 'o/r', ownerId: 'owner', ttl: 1_000, stack: { workers: [] },
    })
    api.namespaces.set(environment.id, { id: 'replacement-namespace', name: environment.id })
    now += 2_000

    const report = await provider.reap()

    expect(report.reaped).toEqual([])
    expect(report.skipped).toEqual([expect.objectContaining({
      id: environment.id,
      reason: expect.stringContaining('ownership identity does not match'),
    })])
    expect(api.namespaces.has(environment.id)).toBe(true)
  })

  it('cleans a partially-created namespace when deployment fails', async () => {
    const api = new FakeCloudflareApi()
    const upload = vi.spyOn(api, 'uploadDispatchWorker').mockImplementation(async (namespace, worker) => {
      if (worker.name === 'broken') throw new Error('upload failed')
      const workers = api.workers.get(namespace)
      if (!workers) throw new Error('namespace missing')
      workers.set(worker.name, { upload: worker, secrets: {} })
    })
    const provider = new CloudflareEnvironmentProvider({
      config: config(), credentialResolver: resources(), api, wrangler: new FakeWranglerRuntime(),
      randomId: () => 'partial1',
    })

    await expect(provider.provision({
      customerId: 'c', repository: 'o/r', ownerId: 'owner',
      stack: { workers: [{ name: 'broken', script: 'throw new Error()' }] },
    })).rejects.toThrow('upload failed')
    expect(upload).toHaveBeenCalled()
    expect(api.namespaces.size).toBe(0)
  })
})

describe('WranglerCloudflareRuntime', () => {
  const credentials = { accountId: 'account-id', apiToken: 'api-token' }
  const project = {
    name: 'container-worker',
    cwd: '.',
    containerApplications: ['container-app'],
    secrets: [],
  }

  it('deploys into the dispatch namespace and returns exact Container identities', async () => {
    const calls: string[][] = []
    let listCalls = 0
    const runner: WranglerCommandRunner = {
      run: async (args, options) => {
        calls.push(args)
        expect(options.env.CLOUDFLARE_API_TOKEN).toBe('api-token')
        if (args[0] === 'containers' && args[1] === 'list') {
          listCalls += 1
          return {
            stdout: JSON.stringify(listCalls === 1 ? [] : [{ id: 'app-id', name: 'container-app' }]),
            stderr: '',
          }
        }
        return { stdout: '', stderr: '' }
      },
    }
    const runtime = new WranglerCloudflareRuntime({ command: 'wrangler', runner })

    await expect(runtime.deploy({
      namespace: 'factory-environment',
      project,
      bindings: { FACTORY_ENVIRONMENT_ID: 'factory-environment' },
      secrets: {},
      credentials,
    })).resolves.toEqual([{ id: 'app-id', name: 'container-app' }])
    expect(calls).toContainEqual(expect.arrayContaining([
      'deploy', '--name', 'container-worker', '--dispatch-namespace', 'factory-environment',
      '--var', 'FACTORY_ENVIRONMENT_ID:factory-environment', '--containers-rollout', 'immediate',
    ]))
  })

  it('refuses pre-existing Container names and cleans newly-created apps after an ambiguous deploy failure', async () => {
    let mode: 'collision' | 'cleanup' = 'collision'
    let listCalls = 0
    const deleted: string[] = []
    const runner: WranglerCommandRunner = {
      run: async (args) => {
        if (args[0] === 'containers' && args[1] === 'list') {
          listCalls += 1
          if (mode === 'collision') return { stdout: JSON.stringify([{ id: 'existing', name: 'container-app' }]), stderr: '' }
          return {
            stdout: JSON.stringify(listCalls === 1 ? [] : [{ id: 'new-app', name: 'container-app' }]),
            stderr: '',
          }
        }
        if (args[0] === 'deploy') throw new Error('connection closed after upload')
        if (args[0] === 'containers' && args[1] === 'delete') {
          deleted.push(args[2])
          return { stdout: '', stderr: '' }
        }
        throw new Error(`unexpected command ${args.join(' ')}`)
      },
    }
    const runtime = new WranglerCloudflareRuntime({ command: 'wrangler', runner })
    const input = {
      namespace: 'factory-environment', project, bindings: {}, secrets: {}, credentials,
    }

    await expect(runtime.deploy(input)).rejects.toThrow('already exists')
    mode = 'cleanup'
    listCalls = 0
    await expect(runtime.deploy(input)).rejects.toThrow('connection closed after upload')
    expect(deleted).toEqual(['new-app'])
  })
})
