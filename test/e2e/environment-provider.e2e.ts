import { randomUUID } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import {
  CloudflareEnvironmentProvider,
  FetchCloudflareApi,
  ResourceCloudflareCredentialResolver,
  WranglerCloudflareRuntime,
  type Environment,
} from '../../src/index.js'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const apiToken = process.env.CLOUDFLARE_API_TOKEN
assert(apiToken, 'CLOUDFLARE_API_TOKEN is required')
const accountId = process.env.CLOUDFLARE_ACCOUNT_ID ?? await resolveSingleAccountId(apiToken)
const suffix = `${process.pid}-${randomUUID().replaceAll('-', '').slice(0, 8)}`.toLowerCase()
const namespacePrefix = `f-e2e-${randomUUID().replaceAll('-', '').slice(0, 6)}`
const primaryRandomId = randomUUID().replaceAll('-', '').slice(0, 8)
const ttlRandomId = randomUUID().replaceAll('-', '').slice(0, 8)
const primaryEnvironmentId = `${namespacePrefix}-factory-${primaryRandomId}`
const containerApplicationName = `${primaryEnvironmentId}-container`
const workerSecret = randomUUID()
const resolver = new ResourceCloudflareCredentialResolver({
  CloudflareAccountId: accountId,
  CloudflareApiToken: apiToken,
  E2EWorkerSecret: workerSecret,
})
const api = new FetchCloudflareApi({ accountId, apiToken })
const wrangler = new WranglerCloudflareRuntime()
const containerFixture = fileURLToPath(new URL('../fixtures/cloudflare-container/', import.meta.url))
const temporary = await mkdtemp(join(tmpdir(), 'factory-cloudflare-e2e-'))
const containerConfig = join(temporary, 'wrangler.json')
await writeFile(containerConfig, JSON.stringify({
  name: 'container-health',
  main: join(containerFixture, 'worker.ts'),
  compatibility_date: new Date().toISOString().slice(0, 10),
  containers: [{
    class_name: 'FactoryE2EContainer',
    name: containerApplicationName,
    image: join(containerFixture, 'Dockerfile'),
    max_instances: 1,
    instance_type: 'lite',
  }],
  durable_objects: {
    bindings: [{ name: 'FACTORY_E2E_CONTAINER', class_name: 'FactoryE2EContainer' }],
  },
  migrations: [{ tag: 'v1', new_sqlite_classes: ['FactoryE2EContainer'] }],
}, null, 2), { mode: 0o600 })
const randomIds = [primaryRandomId, ttlRandomId]
const provider = new CloudflareEnvironmentProvider({
  config: {
    accountId: 'Resource.CloudflareAccountId',
    apiToken: 'Resource.CloudflareApiToken',
    namespacePrefix,
    ttlMs: 60_000,
    minTtlMs: 1_000,
    maxTtlMs: 10 * 60_000,
    limits: { maxActiveEnvironments: 2 },
  },
  credentialResolver: resolver,
  api,
  wrangler,
  randomId: () => randomIds.shift() ?? randomUUID().replaceAll('-', '').slice(0, 8),
})

let environment: Environment | undefined
let expiring: Environment | undefined

try {
  environment = await provider.provision({
    customerId: 'factory-e2e',
    repository: 'AgentWorkforce/factory',
    ownerId: `environment-provider-e2e-${suffix}`,
    ttl: 60_000,
    stack: {
      workers: [{
        name: 'health',
        script: `export default {
  fetch(_request, env) {
    const valid = env.E2E_BINDING === 'binding-ok' &&
      env.E2E_SECRET === ${JSON.stringify(workerSecret)} &&
      env.FACTORY_ENVIRONMENT_ID &&
      env.FACTORY_OWNER_ID === ${JSON.stringify(`environment-provider-e2e-${suffix}`)};
    return new Response(valid ? 'factory-cloudflare-ok' : 'binding-check-failed', {
      status: valid ? 200 : 500,
      headers: { 'content-type': 'text/plain' },
    });
  }
}`,
        bindings: [{ type: 'plain_text', name: 'E2E_BINDING', text: 'binding-ok' }],
        secrets: [{ name: 'E2E_SECRET', secretRef: 'Resource.E2EWorkerSecret' }],
        endpoint: { name: 'health', path: '/' },
      }],
      wranglerProjects: [{
        name: 'container-health',
        cwd: containerFixture,
        configPath: containerConfig,
        containerApplications: [containerApplicationName],
        endpoint: { name: 'container', path: '/' },
      }],
    },
  })

  const namespace = await api.getDispatchNamespace(environment.dispatchNamespace)
  assert(namespace, 'provision() returned before the dispatch namespace existed')
  assert(namespace.id, 'dispatch namespace has no stable namespace UUID')
  assert(namespace.trustedWorkers === false, 'dispatch namespace was not created in untrusted mode')
  const bindings = await api.getDispatchWorkerBindings(environment.id, 'health')
  assert(bindings?.some((binding) => binding.name === 'E2E_BINDING'), 'plain-text binding was not deployed')
  assert(bindings?.some((binding) => binding.name === 'FACTORY_ENVIRONMENT_ID'), 'per-environment identity binding was not deployed')
  assert(bindings?.some((binding) => binding.name === 'E2E_SECRET' && binding.type === 'secret_text'), 'secret binding was not deployed')
  await eventuallyReady(provider, environment.id)
  const endpoints = await provider.endpoints(environment.id)
  assert(endpoints.health === environment.endpoints.health, 'endpoints() did not return the provisioned ingress URL')
  const response = await eventuallyFetch(endpoints.health)
  assert(await response.text() === 'factory-cloudflare-ok', 'reachable Worker did not observe its per-environment binding and secret')
  const containerResponse = await eventuallyFetch(endpoints.container, 5 * 60_000)
  assert(
    await containerResponse.text() === 'factory-cloudflare-container-ok',
    'reachable Container ingress did not return its health response',
  )
  const containerId = environment.bindings[`cloudflare.container.${containerApplicationName}`]
  assert(containerId, 'provision() did not retain the exact Container application identity')
  console.log(`provision/reachable: ${environment.id} ${endpoints.health}`)

  const ingressWorker = new URL(endpoints.health).hostname.split('.')[0]
  await provider.destroy(environment.id)
  assert(!await api.getDispatchNamespace(environment.id), 'destroy() left the dispatch namespace behind')
  assert(!await api.ingressWorkerExists(ingressWorker), 'destroy() left the dispatch ingress Worker behind')
  assert(
    await wrangler.containerStatus(
      { id: containerId, name: containerApplicationName },
      { accountId, apiToken },
    ) === 'failed',
    'destroy() left the Container application behind',
  )
  await provider.destroy(environment.id)
  console.log(`destroy/idempotent: ${environment.id}`)
  environment = undefined

  expiring = await provider.provision({
    customerId: 'factory-e2e',
    repository: 'AgentWorkforce/factory',
    ownerId: `environment-provider-e2e-reaper-${suffix}`,
    ttl: 1_000,
    stack: {
      workers: [{
        name: 'ttl-health',
        script: 'export default { fetch() { return new Response("ttl-ok") } }',
      }],
    },
  })
  assert(await api.getDispatchNamespace(expiring.id), 'short-TTL namespace was not created')
  await new Promise((resolve) => setTimeout(resolve, 1_250))
  const restartedProvider = new CloudflareEnvironmentProvider({
    config: {
      accountId: 'Resource.CloudflareAccountId',
      apiToken: 'Resource.CloudflareApiToken',
      namespacePrefix,
      ttlMs: 60_000,
      minTtlMs: 1_000,
      maxTtlMs: 10 * 60_000,
      limits: { maxActiveEnvironments: 2 },
    },
    credentialResolver: resolver,
    api,
    wrangler,
  })
  const report = await restartedProvider.reap()
  assert(
    report.reaped.some((entry) => entry.id === expiring?.id && entry.reason === 'ttl-expired'),
    `reaper did not report the expired environment: ${JSON.stringify(report)}`,
  )
  assert(!await api.getDispatchNamespace(expiring.id), 'reaper left the expired dispatch namespace behind')
  console.log(`reaper/ttl: ${expiring.id}`)
  expiring = undefined
} finally {
  if (environment) await provider.destroy(environment.id).catch(() => undefined)
  if (expiring) await provider.destroy(expiring.id).catch(() => undefined)
  await rm(temporary, { recursive: true, force: true })
}

async function eventuallyFetch(url: string, timeoutMs = 60_000): Promise<Response> {
  const deadline = Date.now() + timeoutMs
  let lastError: unknown
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url)
      if (response.ok) return response
      lastError = new Error(`HTTP ${response.status}: ${await response.text()}`)
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000))
  }
  throw new Error(`Endpoint ${url} never became reachable: ${String(lastError)}`)
}

async function eventuallyReady(provider: CloudflareEnvironmentProvider, id: string): Promise<void> {
  const deadline = Date.now() + 5 * 60_000
  let lastStatus = await provider.status(id)
  while (Date.now() < deadline) {
    if (lastStatus === 'ready') return
    if (lastStatus === 'failed' || lastStatus === 'destroyed') {
      throw new Error(`Environment ${id} entered terminal status ${lastStatus} before readiness`)
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000))
    lastStatus = await provider.status(id)
  }
  throw new Error(`Environment ${id} never became ready; last status was ${lastStatus}`)
}

async function resolveSingleAccountId(token: string): Promise<string> {
  const response = await fetch('https://api.cloudflare.com/client/v4/accounts?per_page=50', {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error('CLOUDFLARE_ACCOUNT_ID is required when the token cannot list accounts')
  }
  const payload = await response.json() as {
    success?: boolean
    result?: Array<{ id?: string; name?: string }>
  }
  const accounts = payload.result?.filter((account): account is { id: string; name?: string } => Boolean(account.id)) ?? []
  if (accounts.length !== 1) {
    throw new Error(`CLOUDFLARE_ACCOUNT_ID is required when the token can access ${accounts.length} accounts`)
  }
  return accounts[0].id
}
