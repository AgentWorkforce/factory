import { Container, getContainer } from '@cloudflare/containers'

export class FactoryE2EContainer extends Container {
  defaultPort = 8080
  sleepAfter = '2m'
}

export default {
  async fetch(request: Request, env: { FACTORY_E2E_CONTAINER: DurableObjectNamespace }): Promise<Response> {
    return await getContainer(env.FACTORY_E2E_CONTAINER, 'health').fetch(request)
  },
}
