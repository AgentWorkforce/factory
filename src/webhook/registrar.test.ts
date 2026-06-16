import type { RelayFileClient, WebhookSubscription } from '@relayfile/sdk'
import { describe, expect, it, vi } from 'vitest'

import { WebhookRegistrar } from './registrar'

describe('WebhookRegistrar', () => {
  it('does not create duplicate subscriptions when registered twice', async () => {
    const client = createClient()
    client.listWebhooks.mockResolvedValue([])
    client.registerWebhook.mockResolvedValue({ subscriptionId: 'sub_123' })
    const registrar = new WebhookRegistrar({
      client: client as unknown as RelayFileClient,
      workspaceId: 'ws_123',
      receiverUrl: 'https://factory.example.test/webhook',
      secret: 'secret',
    })

    await registrar.register()
    await registrar.register()

    expect(registrar.subscriptionId).toBe('sub_123')
    expect(client.listWebhooks).toHaveBeenCalledTimes(1)
    expect(client.registerWebhook).toHaveBeenCalledTimes(1)
  })

  it('reuses an existing matching subscription instead of creating a duplicate', async () => {
    const client = createClient()
    client.listWebhooks.mockResolvedValue([
      webhookSubscription({
        id: 'sub_existing',
        url: 'https://factory.example.test/webhook',
        pathGlobs: ['/github/repos/**', '/linear/issues/**', '/slack/channels/**'],
      }),
    ])
    const registrar = new WebhookRegistrar({
      client: client as unknown as RelayFileClient,
      workspaceId: 'ws_123',
      receiverUrl: 'https://factory.example.test/webhook',
      secret: 'secret',
    })

    await registrar.register()

    expect(registrar.subscriptionId).toBe('sub_existing')
    expect(client.registerWebhook).not.toHaveBeenCalled()
  })
})

function createClient() {
  return {
    listWebhooks: vi.fn(),
    registerWebhook: vi.fn(),
    deleteWebhook: vi.fn(),
  }
}

function webhookSubscription(
  overrides: Partial<WebhookSubscription> = {},
): WebhookSubscription {
  return {
    id: 'sub_123',
    url: 'https://factory.example.test/other',
    pathGlobs: ['/linear/issues/**'],
    createdAt: '2026-06-15T12:00:00.000Z',
    updatedAt: '2026-06-15T12:00:00.000Z',
    health: {
      lastDeliveryAt: null,
      lastSuccessAt: null,
      lastError: null,
      consecutiveFailures: 0,
    },
    ...overrides,
  }
}
