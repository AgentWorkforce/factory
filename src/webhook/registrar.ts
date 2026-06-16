import type { RelayFileClient, WebhookSubscription } from '@relayfile/sdk'

const DEFAULT_PATH_GLOBS = [
  '/linear/issues/**',
  '/slack/channels/**',
  '/github/repos/**',
]

export interface WebhookRegistrarConfig {
  client: RelayFileClient
  workspaceId: string
  receiverUrl: string
  secret: string
  pathGlobs?: string[]
}

export class WebhookRegistrar {
  readonly #config: WebhookRegistrarConfig
  #subscriptionId?: string

  constructor(config: WebhookRegistrarConfig) {
    this.#config = config
  }

  async register(): Promise<void> {
    if (this.#subscriptionId) return

    const pathGlobs = this.#config.pathGlobs ?? DEFAULT_PATH_GLOBS
    const existing = await this.#findExistingSubscription(pathGlobs)
    if (existing) {
      this.#subscriptionId = existing.id
      return
    }

    const subscription = await this.#config.client.registerWebhook({
      workspaceId: this.#config.workspaceId,
      url: this.#config.receiverUrl,
      pathGlobs,
      secret: this.#config.secret,
    })
    this.#subscriptionId = subscription.subscriptionId
  }

  async unregister(): Promise<void> {
    if (!this.#subscriptionId) return
    await this.#config.client.deleteWebhook(
      this.#config.workspaceId,
      this.#subscriptionId,
    )
    this.#subscriptionId = undefined
  }

  get subscriptionId(): string | undefined {
    return this.#subscriptionId
  }

  async #findExistingSubscription(pathGlobs: string[]): Promise<WebhookSubscription | undefined> {
    const subscriptions = await this.#config.client.listWebhooks(this.#config.workspaceId)
    return subscriptions.find((subscription) => (
      subscription.url === this.#config.receiverUrl
      && sameStringSet(subscription.pathGlobs, pathGlobs)
    ))
  }
}

function sameStringSet(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false
  const bSet = new Set(b)
  return a.every((value) => bSet.has(value))
}
