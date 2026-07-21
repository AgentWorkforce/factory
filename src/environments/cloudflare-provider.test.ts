import { describe, expect, it, vi } from 'vitest'

import {
  CLOUDFLARE_ENVIRONMENT_BINDINGS,
  CLOUDFLARE_METADATA_SCRIPT,
  CloudflareApiError,
  CloudflareEnvironmentProvider,
  CloudflareEnvironmentQuotaError,
  CloudflareEnvironmentResourceSchema,
  HttpCloudflareEnvironmentClient,
  cloudflareEnvironmentName,
  type CloudflareDispatchNamespace,
  type CloudflareEnvironmentClient,
  type CloudflareWorkerBinding,
  type UploadCloudflareWorkerInput,
} from './cloudflare-provider'

const resource = CloudflareEnvironmentResourceSchema.parse({
  accountId: '0123456789abcdef0123456789abcdef',
  apiToken: 'resource-secret-token',
  dispatcherUrlTemplate: 'https://verification.example.test/{namespace}/{script}',
})

class FakeCloudflareEnvironmentClient implements CloudflareEnvironmentClient {
  readonly namespaces = new Map<string, CloudflareDispatchNamespace>()
  readonly bindings = new Map<string, CloudflareWorkerBinding[]>()
  readonly uploads: UploadCloudflareWorkerInput[] = []
  readonly deletes: string[] = []
  failUpload = false
  createTrustedNamespace = false

  async listDispatchNamespaces(): Promise<CloudflareDispatchNamespace[]> {
    return [...this.namespaces.values()].map((namespace) => ({ ...namespace }))
  }

  async getDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace | undefined> {
    const namespace = this.namespaces.get(name)
    return namespace ? { ...namespace } : undefined
  }

  async createDispatchNamespace(name: string): Promise<CloudflareDispatchNamespace> {
    if (this.namespaces.has(name)) throw new Error('AlreadyExists')
    const namespace = {
      namespace_name: name,
      namespace_id: `namespace-${name}`,
      trusted_workers: this.createTrustedNamespace,
    }
    this.namespaces.set(name, namespace)
    return { ...namespace }
  }

  async deleteDispatchNamespace(name: string): Promise<void> {
    this.deletes.push(name)
    this.namespaces.delete(name)
    for (const key of [...this.bindings.keys()]) {
      if (key.startsWith(`${name}/`)) this.bindings.delete(key)
    }
  }

  async uploadDispatchWorker(input: UploadCloudflareWorkerInput): Promise<void> {
    if (this.failUpload) throw new Error('upload failed')
    this.uploads.push(structuredClone(input))
    this.bindings.set(`${input.namespace}/${input.scriptName}`, structuredClone(input.bindings))
  }

  async getDispatchWorkerBindings(
    namespace: string,
    scriptName: string,
  ): Promise<CloudflareWorkerBinding[] | undefined> {
    return structuredClone(this.bindings.get(`${namespace}/${scriptName}`))
  }
}

describe('CloudflareEnvironmentProvider', () => {
  it('resolves credentials only from the configured Resource-style object', () => {
    const provider = CloudflareEnvironmentProvider.fromResource({
      config: { resource: 'VerificationInfra' },
      resources: { VerificationInfra: resource },
      client: new FakeCloudflareEnvironmentClient(),
    })

    expect(provider.config.resource).toBe('VerificationInfra')
    expect(() => CloudflareEnvironmentProvider.fromResource({
      config: { resource: 'MissingInfra' },
      resources: {},
    })).toThrow('resource "MissingInfra" is unavailable')
    expect(() => CloudflareEnvironmentResourceSchema.parse({
      accountId: resource.accountId,
      apiToken: '',
    })).toThrow()
  })

  it('creates one untrusted namespace with a durable identity lease and guardrail metadata', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    const provider = new CloudflareEnvironmentProvider({
      resource,
      client,
      config: {
        maxRunCostUsd: 0.5,
        workerLimits: { cpuMs: 75, subrequests: 80 },
        container: { instanceType: 'basic', maxInstances: 3 },
      },
      now: () => new Date('2026-07-21T10:00:00.000Z'),
      randomId: () => 'abc123',
    })

    const environment = await provider.provision({
      customerId: 'customer-a',
      repository: 'AgentWorkforce/factory',
      ownerId: 'run/146',
      ttl: 120_000,
      stack: { containerInstances: 2, runCostBudgetUsd: 0.25 },
    })

    expect(environment).toMatchObject({
      id: 'factory-verification-factory-abc123',
      provider: 'cloudflare',
      dispatchNamespace: 'factory-verification-factory-abc123',
      status: 'ready',
      createdAt: '2026-07-21T10:00:00.000Z',
      ttl: 120_000,
      endpoints: {},
      bindings: {
        'cloudflare.resource': 'FactoryTestInfra',
        'cloudflare.metadataScript': CLOUDFLARE_METADATA_SCRIPT,
        'cloudflare.expiresAt': '2026-07-21T10:02:00.000Z',
        'cloudflare.maxRunCostUsd': '0.25',
        'cloudflare.container.instanceType': 'basic',
        'cloudflare.container.maxInstances': '2',
        'cloudflare.worker.cpuMs': '75',
        'cloudflare.worker.subrequests': '80',
      },
    })
    expect(client.namespaces.get(environment.id)?.trusted_workers).toBe(false)
    expect(client.uploads).toHaveLength(1)
    expect(client.uploads[0]).toMatchObject({
      namespace: environment.id,
      scriptName: CLOUDFLARE_METADATA_SCRIPT,
      compatibilityDate: '2026-07-21',
      limits: { cpu_ms: 75, subrequests: 80 },
    })
    const metadata = plainTextBindings(client.uploads[0].bindings)
    expect(metadata).toMatchObject({
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.environmentId]: environment.id,
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.ownerId]: 'run/146',
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.customerId]: 'customer-a',
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.repository]: 'AgentWorkforce/factory',
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.expiresAt]: '2026-07-21T10:02:00.000Z',
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.maxRunCostUsd]: '0.25',
      [CLOUDFLARE_ENVIRONMENT_BINDINGS.containerMaxInstances]: '2',
    })

    await provider.destroy(environment.id)
    await provider.destroy(environment.id)
    expect(client.deletes).toEqual([environment.id])
    expect(await provider.status(environment.id)).toBe('destroyed')
  })

  it.each([
    {
      label: 'Container instances',
      config: { container: { maxInstances: 2 } },
      spec: { stack: { containerInstances: 3 } },
      message: /exceeds per-environment cap/iu,
    },
    {
      label: 'run cost',
      config: { maxRunCostUsd: 0.1 },
      spec: { stack: { runCostBudgetUsd: 0.11 } },
      message: /exceeds per-run cap/iu,
    },
    {
      label: 'ttl',
      config: { maxTtlMs: 60_000, defaultTtlMs: 60_000 },
      spec: { ttl: 60_001 },
      message: /ttl must be between/iu,
    },
  ])('refuses a $label request before creating Cloudflare resources', async ({ config, spec, message }) => {
    const client = new FakeCloudflareEnvironmentClient()
    const provider = new CloudflareEnvironmentProvider({ resource, client, config })

    await expect(provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'run', ...spec,
    })).rejects.toThrow(message)
    expect(client.namespaces.size).toBe(0)
    expect(client.uploads).toHaveLength(0)
  })

  it('serializes provisioning so concurrent calls cannot pass the max-concurrent cap', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    let random = 0
    const provider = new CloudflareEnvironmentProvider({
      resource,
      client,
      config: { maxConcurrentEnvironments: 1 },
      randomId: () => `run${++random}`,
    })
    const spec = { customerId: 'customer', repository: 'org/repo', ownerId: 'run' }

    const results = await Promise.allSettled([provider.provision(spec), provider.provision(spec)])

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
    const rejected = results.find((result) => result.status === 'rejected') as PromiseRejectedResult
    expect(rejected.reason).toBeInstanceOf(CloudflareEnvironmentQuotaError)
    expect(String(rejected.reason)).toContain('max-concurrent')
    expect(client.namespaces.size).toBe(1)
  })

  it('isolates environments by namespace and refuses teardown after ownership metadata is changed', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    let random = 0
    const provider = new CloudflareEnvironmentProvider({
      resource,
      client,
      config: { maxConcurrentEnvironments: 2 },
      randomId: () => `run${++random}`,
    })
    const first = await provider.provision({
      customerId: 'customer-a', repository: 'org/repo', ownerId: 'owner-a',
    })
    const second = await provider.provision({
      customerId: 'customer-b', repository: 'org/repo', ownerId: 'owner-b',
    })

    expect(first.dispatchNamespace).not.toBe(second.dispatchNamespace)
    expect(plainTextBindings(client.bindings.get(`${first.id}/${CLOUDFLARE_METADATA_SCRIPT}`) ?? []))
      .toMatchObject({ FACTORY_OWNER_ID: 'owner-a' })
    expect(plainTextBindings(client.bindings.get(`${second.id}/${CLOUDFLARE_METADATA_SCRIPT}`) ?? []))
      .toMatchObject({ FACTORY_OWNER_ID: 'owner-b' })

    const firstBindings = client.bindings.get(`${first.id}/${CLOUDFLARE_METADATA_SCRIPT}`)
    const identity = firstBindings?.find((binding) => binding.name === CLOUDFLARE_ENVIRONMENT_BINDINGS.environmentId)
    if (!identity) throw new Error('fixture identity binding is missing')
    identity.text = second.id

    await expect(provider.destroy(first.id)).rejects.toThrow('ownership identity mismatch')
    expect(client.namespaces.has(first.id)).toBe(true)
    expect(client.namespaces.has(second.id)).toBe(true)
  })

  it('cleans up a newly-created namespace if its metadata Worker cannot be uploaded', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    client.failUpload = true
    const provider = new CloudflareEnvironmentProvider({
      resource, client, randomId: () => 'cleanup',
    })

    await expect(provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'owner',
    })).rejects.toThrow('upload failed')
    expect(client.namespaces.size).toBe(0)
    expect(client.deletes).toEqual(['factory-verification-repo-cleanup'])
  })

  it('rejects trusted dispatch namespaces and removes the unsafe allocation', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    client.createTrustedNamespace = true
    const provider = new CloudflareEnvironmentProvider({
      resource, client, randomId: () => 'trusted',
    })

    await expect(provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'owner',
    })).rejects.toThrow('must be untrusted')
    expect(client.namespaces.size).toBe(0)
  })

  it('reaps expired and dead-owner environments while preserving invalid or live leases', async () => {
    const client = new FakeCloudflareEnvironmentClient()
    let now = new Date('2026-07-21T10:00:00.000Z')
    let random = 0
    const provider = new CloudflareEnvironmentProvider({
      resource,
      client,
      config: { maxConcurrentEnvironments: 4, defaultTtlMs: 60_000 },
      now: () => now,
      randomId: () => `reap${++random}`,
      ownerIsAlive: async (ownerId) => ownerId !== 'dead-owner',
    })
    const expired = await provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'live-owner',
    })
    const deadOwner = await provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'dead-owner', ttl: 120_000,
    })
    const live = await provider.provision({
      customerId: 'customer', repository: 'org/repo', ownerId: 'live-owner', ttl: 120_000,
    })
    client.namespaces.set('factory-verification-spoof-invalid', {
      namespace_name: 'factory-verification-spoof-invalid', trusted_workers: false,
    })
    now = new Date('2026-07-21T10:01:01.000Z')

    const report = await provider.reap()

    expect(report.reaped).toEqual(expect.arrayContaining([
      { id: expired.id, reason: 'ttl-expired' },
      { id: deadOwner.id, reason: 'owner-gone' },
    ]))
    expect(report.skipped).toContainEqual({
      id: 'factory-verification-spoof-invalid',
      reason: 'Refusing Cloudflare namespace factory-verification-spoof-invalid: ownership metadata Worker is missing',
    })
    expect(client.namespaces.has(expired.id)).toBe(false)
    expect(client.namespaces.has(deadOwner.id)).toBe(false)
    expect(client.namespaces.has(live.id)).toBe(true)
    expect(client.namespaces.has('factory-verification-spoof-invalid')).toBe(true)
  })
})

describe('HttpCloudflareEnvironmentClient', () => {
  it('uses the documented account dispatch endpoint and bearer resource token', async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = []
    const fetch = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(input), init })
      return new Response(JSON.stringify({
        success: true,
        result: { namespace_name: 'factory-verification-repo-api' },
      }), { status: 200, headers: { 'content-type': 'application/json' } })
    }) as typeof globalThis.fetch
    const client = new HttpCloudflareEnvironmentClient({ resource, fetch })

    await client.createDispatchNamespace('factory-verification-repo-api')

    expect(calls).toHaveLength(1)
    expect(calls[0].url).toBe(
      `https://api.cloudflare.com/client/v4/accounts/${resource.accountId}/workers/dispatch/namespaces`,
    )
    expect(calls[0].init).toMatchObject({
      method: 'POST',
      headers: {
        authorization: 'Bearer resource-secret-token',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ name: 'factory-verification-repo-api' }),
    })
  })

  it('treats GET 404 as absence and does not leak the token in API errors', async () => {
    const notFound = new HttpCloudflareEnvironmentClient({
      resource,
      fetch: vi.fn(async () => new Response(JSON.stringify({ success: false, errors: [] }), {
        status: 404,
      })) as typeof globalThis.fetch,
    })
    expect(await notFound.getDispatchNamespace('missing')).toBeUndefined()

    const failing = new HttpCloudflareEnvironmentClient({
      resource,
      fetch: vi.fn(async () => new Response(JSON.stringify({
        success: false,
        errors: [{ code: 10001, message: 'authentication failed' }],
      }), { status: 403 })) as typeof globalThis.fetch,
    })
    const error = await failing.listDispatchNamespaces().catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(CloudflareApiError)
    expect(String(error)).toContain('authentication failed')
    expect(String(error)).not.toContain(resource.apiToken)
  })
})

describe('cloudflareEnvironmentName', () => {
  it('produces a stable DNS label within the Cloudflare namespace bound', () => {
    const name = cloudflareEnvironmentName(
      'factory-verification',
      `AgentWorkforce/${'Very_Long_Repository_'.repeat(5)}`,
      'A-B_C!123',
    )

    expect(name).toMatch(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u)
    expect(name.length).toBeLessThanOrEqual(63)
    expect(name.endsWith('-abc123')).toBe(true)
  })
})

function plainTextBindings(bindings: CloudflareWorkerBinding[]): Record<string, string> {
  return Object.fromEntries(bindings.flatMap((binding) => (
    binding.type === 'plain_text' && typeof binding.text === 'string'
      ? [[binding.name, binding.text]]
      : []
  )))
}
