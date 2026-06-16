import { createHmac } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'

import { WebhookHandler, type WebhookEvent } from './handler'

const SECRET = 'test-webhook-secret'
const TIMESTAMP = '1781548800'

describe('WebhookHandler', () => {
  it('calls the linear handler for a valid linear event', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({ path: '/linear/issues/PROJ-123/metadata.json' })

    const response = await handler.handle(signedRequest(event))

    expect(response.status).toBe(200)
    expect(handlers.onLinearEvent).toHaveBeenCalledWith(event)
    expect(handlers.onSlackEvent).not.toHaveBeenCalled()
    expect(handlers.onGithubEvent).not.toHaveBeenCalled()
  })

  it('rejects invalid HMAC signatures without calling handlers', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({ path: '/linear/issues/PROJ-123/metadata.json' })

    const response = await handler.handle(signedRequest(event, { signature: '0'.repeat(64) }))

    expect(response.status).toBe(403)
    expect(handlers.onLinearEvent).not.toHaveBeenCalled()
    expect(handlers.onSlackEvent).not.toHaveBeenCalled()
    expect(handlers.onGithubEvent).not.toHaveBeenCalled()
  })

  it('dedupes duplicate event IDs without calling handlers again', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({
      eventId: 'evt_duplicate',
      path: '/linear/issues/PROJ-123/metadata.json',
    })

    const firstResponse = await handler.handle(signedRequest(event))
    handlers.onLinearEvent.mockClear()
    const duplicateResponse = await handler.handle(signedRequest(event))

    expect(firstResponse.status).toBe(200)
    expect(duplicateResponse.status).toBe(200)
    expect(handlers.onLinearEvent).not.toHaveBeenCalled()
    expect(handlers.onSlackEvent).not.toHaveBeenCalled()
    expect(handlers.onGithubEvent).not.toHaveBeenCalled()
  })

  it('dedupes by body event ID before falling back to the header event ID', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({
      eventId: 'evt_body',
      path: '/linear/issues/PROJ-123/metadata.json',
    })

    const firstResponse = await handler.handle(signedRequest(event, { headerEventId: 'evt_header_1' }))
    handlers.onLinearEvent.mockClear()
    const duplicateResponse = await handler.handle(signedRequest(event, { headerEventId: 'evt_header_2' }))

    expect(firstResponse.status).toBe(200)
    expect(duplicateResponse.status).toBe(200)
    expect(handlers.onLinearEvent).not.toHaveBeenCalled()
  })

  it('calls the slack handler for a valid slack event', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({ path: '/slack/channels/C123/messages/456.json' })

    const response = await handler.handle(signedRequest(event))

    expect(response.status).toBe(200)
    expect(handlers.onSlackEvent).toHaveBeenCalledWith(event)
    expect(handlers.onLinearEvent).not.toHaveBeenCalled()
    expect(handlers.onGithubEvent).not.toHaveBeenCalled()
  })

  it('calls the github handler for a valid github event', async () => {
    const handlers = createHandlers()
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({ path: '/github/repos/AgentWorkforce/pear/pulls/123.json' })

    const response = await handler.handle(signedRequest(event))

    expect(response.status).toBe(200)
    expect(handlers.onGithubEvent).toHaveBeenCalledWith(event)
    expect(handlers.onLinearEvent).not.toHaveBeenCalled()
    expect(handlers.onSlackEvent).not.toHaveBeenCalled()
  })

  it('returns 500 when a handler throws so delivery can retry', async () => {
    const handlers = createHandlers()
    handlers.onLinearEvent.mockRejectedValueOnce(new Error('temporary failure'))
    const handler = new WebhookHandler({ secret: SECRET, ...handlers })
    const event = webhookEvent({ path: '/linear/issues/PROJ-123/metadata.json' })

    const response = await handler.handle(signedRequest(event))

    expect(response.status).toBe(500)
    expect(handlers.onLinearEvent).toHaveBeenCalledWith(event)
  })
})

function createHandlers() {
  return {
    onLinearEvent: vi.fn(async (_event: WebhookEvent): Promise<void> => {}),
    onSlackEvent: vi.fn(async (_event: WebhookEvent): Promise<void> => {}),
    onGithubEvent: vi.fn(async (_event: WebhookEvent): Promise<void> => {}),
  }
}

function webhookEvent(overrides: Partial<WebhookEvent> = {}): WebhookEvent {
  return {
    type: 'file.updated',
    path: '/linear/issues/PROJ-123/metadata.json',
    revision: 'rev_123',
    eventId: 'evt_123',
    contentHash: 'hash_123',
    provider: 'linear',
    correlationId: 'corr_123',
    timestamp: '2026-06-15T12:00:00.000Z',
    ...overrides,
  }
}

function signedRequest(
  event: WebhookEvent,
  options: { headerEventId?: string; signature?: string } = {},
): Request {
  const rawBody = JSON.stringify(event)
  const signature = options.signature ?? createHmac('sha256', SECRET)
    .update(`${TIMESTAMP}.${rawBody}`)
    .digest('hex')

  return new Request('https://factory.example.test/webhook', {
    method: 'POST',
    headers: {
      'x-relay-timestamp': TIMESTAMP,
      'x-relay-signature': signature,
      'x-relay-event-id': options.headerEventId ?? event.eventId,
    },
    body: rawBody,
  })
}
