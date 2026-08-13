import { slackClient, telegramClient } from '@relayfile/relay-helpers'

export interface TicketDispatchDelivery {
  slack(input: { channel?: string; dm?: string; text: string }): Promise<void>
  telegram(input: { chatId: string; text: string }): Promise<void>
}

export function createTicketDispatchDelivery(options: {
  mountRoot?: string
} = {}): TicketDispatchDelivery {
  const clientOptions = options.mountRoot ? { mountRoot: options.mountRoot } : undefined

  return {
    async slack({ channel, dm, text }): Promise<void> {
      const client = slackClient(clientOptions)
      await Promise.all([
        ...(channel ? [client.post(channel, text)] : []),
        ...(dm ? [client.dm(dm, text)] : []),
      ])
    },

    async telegram({ chatId, text }): Promise<void> {
      const result = await telegramClient(clientOptions).sendMessage(chatId, text)
      if (!result.ok || !result.messageId) {
        throw new Error(`Telegram delivery to ${chatId} returned no receipt`)
      }
    },
  }
}
