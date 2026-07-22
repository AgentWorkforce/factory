import { afterEach, describe, expect, it } from 'vitest'
import { Miniflare } from 'miniflare'

const runtimes: Miniflare[] = []

afterEach(async () => {
  await Promise.all(runtimes.splice(0).map(async (runtime) => await runtime.dispose()))
})

describe('Cloudflare environment Worker runtime', () => {
  it('exposes environment bindings and secrets inside workerd', async () => {
    const runtime = new Miniflare({
      modules: true,
      compatibilityDate: '2026-07-22',
      bindings: {
        FACTORY_ENVIRONMENT_ID: 'factory-miniflare-env',
        FACTORY_OWNER_ID: 'run-142',
        FACTORY_CUSTOMER_ID: 'customer-a',
        FACTORY_REPOSITORY: 'AgentWorkforce/factory',
        E2E_SECRET: 'miniflare-secret',
      },
      script: `export default {
        fetch(_request, env) {
          return Response.json({
            environmentId: env.FACTORY_ENVIRONMENT_ID,
            ownerId: env.FACTORY_OWNER_ID,
            customerId: env.FACTORY_CUSTOMER_ID,
            repository: env.FACTORY_REPOSITORY,
            secretVisible: env.E2E_SECRET === 'miniflare-secret',
          });
        },
      }`,
    })
    runtimes.push(runtime)

    const response = await runtime.dispatchFetch('https://factory.invalid/health')

    expect(response.status).toBe(200)
    await expect(response.json()).resolves.toEqual({
      environmentId: 'factory-miniflare-env',
      ownerId: 'run-142',
      customerId: 'customer-a',
      repository: 'AgentWorkforce/factory',
      secretVisible: true,
    })
  })
})
