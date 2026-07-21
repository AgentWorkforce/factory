import { randomUUID } from 'node:crypto'

import {
  CloudflareEnvironmentProvider,
  FetchCloudflareApi,
  ResourceCloudflareCredentialResolver,
  type Environment,
} from '../../src/index.js'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const apiToken = process.env.CLOUDFLARE_API_TOKEN
assert(apiToken, 'CLOUDFLARE_API_TOKEN is required')
const accountId = process.env.CLOUDFLARE_ACCOUNT_ID ?? await resolveSingleAccountId(apiToken)
const suffix = `${process.pid}-${randomUUID().replaceAll('-', '').slice(0, 8)}`.toLowerCase()
const namespacePrefix = `factory-e2e-${suffix}`.slice(0, 24).replace(/-$/u, '')
const workerSecret = randomUUID()
const resolver = new ResourceCloudflareCredentialResolver({
  CloudflareAccountId: accountId,
  CloudflareApiToken: apiToken,
  E2EWorkerSecret: workerSecret,
})
const api = new FetchCloudflareApi({ accountId, apiToken })
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
    },
  })

  const namespace = await api.getDispatchNamespace(environment.dispatchNamespace)
  assert(namespace, 'provision() returned before the dispatch namespace existed')
  assert(namespace.id, 'dispatch namespace has no stable namespace UUID')
  const bindings = await api.getDispatchWorkerBindings(environment.id, 'health')
  assert(bindings?.some((binding) => binding.name === 'E2E_BINDING'), 'plain-text binding was not deployed')
  assert(bindings?.some((binding) => binding.name === 'FACTORY_ENVIRONMENT_ID'), 'per-environment identity binding was not deployed')
  assert(bindings?.some((binding) => binding.name === 'E2E_SECRET' && binding.type === 'secret_text'), 'secret binding was not deployed')
  assert(await provider.status(environment.id) === 'ready', 'status() did not report ready')
  const endpoints = await provider.endpoints(environment.id)
  assert(endpoints.health === environment.endpoints.health, 'endpoints() did not return the provisioned ingress URL')
  const response = await eventuallyFetch(endpoints.health)
  assert(await response.text() === 'factory-cloudflare-ok', 'reachable Worker did not observe its per-environment binding and secret')
  console.log(`provision/reachable: ${environment.id} ${endpoints.health}`)

  const ingressWorker = new URL(endpoints.health).hostname.split('.')[0]
  await provider.destroy(environment.id)
  assert(!await api.getDispatchNamespace(environment.id), 'destroy() left the dispatch namespace behind')
  assert(!await api.ingressWorkerExists(ingressWorker), 'destroy() left the dispatch ingress Worker behind')
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
  const report = await provider.reap()
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
}

async function eventuallyFetch(url: string): Promise<Response> {
  const deadline = Date.now() + 60_000
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
