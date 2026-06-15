import { createHmac, timingSafeEqual as cryptoTimingSafeEqual } from 'node:crypto'

export interface WebhookEvent {
  type: string
  path: string
  revision: string
  eventId: string
  contentHash?: string
  provider?: string
  correlationId?: string
  timestamp: string
}

export interface WebhookHandlerConfig {
  secret: string
  onLinearEvent: (event: WebhookEvent) => Promise<void>
  onSlackEvent: (event: WebhookEvent) => Promise<void>
  onGithubEvent: (event: WebhookEvent) => Promise<void>
}

export class WebhookHandler {
  readonly #config: WebhookHandlerConfig
  readonly #seen = new Set<string>()

  constructor(config: WebhookHandlerConfig) {
    this.#config = config
  }

  async handle(request: Request): Promise<Response> {
    const timestamp = request.headers.get('x-relay-timestamp') ?? ''
    const signature = request.headers.get('x-relay-signature') ?? ''
    const headerEventId = request.headers.get('x-relay-event-id') ?? ''
    const rawBody = await request.text()

    const expected = createHmac('sha256', this.#config.secret)
      .update(`${timestamp}.${rawBody}`)
      .digest('hex')
    if (!timingSafeHexEqual(expected, signature)) {
      return new Response('forbidden', { status: 403 })
    }

    let event: WebhookEvent
    try {
      event = JSON.parse(rawBody) as WebhookEvent
    } catch {
      return new Response('bad_request', { status: 400 })
    }

    const eventId = event.eventId || headerEventId
    if (eventId && this.#seen.has(eventId)) {
      return new Response('ok', { status: 200 })
    }

    try {
      if (event.path.startsWith('/linear/issues/')) {
        await this.#config.onLinearEvent(event)
      } else if (event.path.startsWith('/slack/channels/')) {
        await this.#config.onSlackEvent(event)
      } else if (event.path.startsWith('/github/repos/')) {
        await this.#config.onGithubEvent(event)
      }
    } catch {
      return new Response('internal_error', { status: 500 })
    }

    if (eventId) {
      this.#seen.add(eventId)
    }

    return new Response('ok', { status: 200 })
  }
}

function timingSafeHexEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  const bufA = Buffer.from(a, 'hex')
  const bufB = Buffer.from(b, 'hex')
  return bufA.length === bufB.length && cryptoTimingSafeEqual(bufA, bufB)
}
