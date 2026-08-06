import { createHash } from 'node:crypto'

import { AgentRelay } from '@agent-relay/sdk'
import type { RelayMessage } from '@agent-relay/sdk'

import type { NotionContractDelivery, NotionContractPublisher } from './notion'

const CONTRACT_CHUNK_CHARACTERS = 6_000
const CONTRACT_BEGIN = '---BEGIN FACTORY NOTION CONTRACT BASE64---'
const CONTRACT_END = '---END FACTORY NOTION CONTRACT BASE64---'

type RelayChannelContractPublisherOptions = {
  workspaceKey: string
  baseUrl?: string
  publisherName?: string
  createRelay?: (options: { workspaceKey: string; baseUrl?: string; agentToken?: string }) => ContractRelay
}

type ContractRelay = Pick<AgentRelay, 'agents' | 'channels' | 'messages' | 'messaging'>

/**
 * Publishes digest-bound Notion bytes to a workspace-private Relay channel.
 * Workers can reconstruct a read-only local mount snapshot on any fleet node
 * without exposing the private page through a public lifecycle issue.
 */
export class RelayChannelNotionContractPublisher implements NotionContractPublisher {
  readonly #workspaceKey: string
  readonly #baseUrl?: string
  readonly #publisherName: string
  readonly #createRelay: NonNullable<RelayChannelContractPublisherOptions['createRelay']>
  readonly #cache = new Map<string, NotionContractDelivery>()
  #workspaceRelay?: ContractRelay
  #agentRelay?: ContractRelay
  #relayReady?: Promise<ContractRelay>

  constructor(options: RelayChannelContractPublisherOptions) {
    this.#workspaceKey = options.workspaceKey
    this.#baseUrl = options.baseUrl
    this.#publisherName = options.publisherName ??
      `factory-notion-intake-${process.pid}-${Date.now().toString(36)}`
    this.#createRelay = options.createRelay ?? ((relayOptions) => new AgentRelay(relayOptions))
  }

  async publish(input: {
    pageId: string
    sourceKey: string
    content: string
    contentDigest: string
  }): Promise<NotionContractDelivery> {
    const observedDigest = createHash('sha256').update(input.content).digest('hex')
    if (observedDigest !== input.contentDigest) {
      throw new Error('Notion contract changed before portable mount publication')
    }
    const cacheKey = `${input.sourceKey}\0${input.contentDigest}`
    const cached = this.#cache.get(cacheKey)
    if (cached) return cached

    const relay = await this.#relay()
    const channel = contractChannelName(input.pageId, input.sourceKey, input.contentDigest)
    try {
      await relay.channels.join(channel)
    } catch (joinError) {
      try {
        await relay.channels.create({
          name: channel,
          topic: `Read-only Notion contract ${input.pageId}`,
        })
      } catch (createError) {
        try {
          await relay.channels.join(channel)
        } catch (finalJoinError) {
          throw new Error(`unable to join or create Notion contract channel ${channel}`, {
            cause: new AggregateError([joinError, createError, finalJoinError]),
          })
        }
      }
    }

    const encoded = Buffer.from(input.content, 'utf8').toString('base64')
    const chunks = splitContract(encoded)
    const markerPrefix = contractMarkerPrefix(input.pageId, input.contentDigest)
    const existing = await listAllMessages(relay, channel)
    const messageIds: string[] = []

    for (let index = 0; index < chunks.length; index += 1) {
      const marker = `${markerPrefix}${index + 1}/${chunks.length}`
      const expectedText = `${marker}\n${CONTRACT_BEGIN}\n${chunks[index]}\n${CONTRACT_END}`
      const idempotencyKey = createHash('sha256')
        .update(`${input.sourceKey}\0${input.contentDigest}\0${index + 1}\0${chunks.length}`)
        .digest('hex')
      const prior = existing.find((message) => message.text.startsWith(`${marker}\n`))
      const message = prior ?? await relay.messages.send({
        channel,
        text: expectedText,
        idempotencyKey: `factory-notion-contract-v1:${idempotencyKey}`,
      })
      if (message.text !== expectedText) {
        throw new Error(`portable Notion contract chunk ${index + 1} does not match its digest-bound marker`)
      }
      messageIds.push(message.id)
    }

    const delivery: NotionContractDelivery = {
      kind: 'relay-channel',
      channel,
      messageIds,
      encoding: 'base64-chunks-v1',
    }
    this.#cache.set(cacheKey, delivery)
    return delivery
  }

  async dispose(): Promise<void> {
    await this.#agentRelay?.messaging.events.disconnect().catch(() => undefined)
    await this.#workspaceRelay?.agents.delete(this.#publisherName).catch(() => undefined)
    this.#agentRelay = undefined
    this.#workspaceRelay = undefined
    this.#relayReady = undefined
  }

  async #relay(): Promise<ContractRelay> {
    if (this.#agentRelay) return this.#agentRelay
    this.#relayReady ??= this.#initializeRelay()
    try {
      return await this.#relayReady
    } catch (error) {
      this.#relayReady = undefined
      throw error
    }
  }

  async #initializeRelay(): Promise<ContractRelay> {
    const options = {
      workspaceKey: this.#workspaceKey,
      ...(this.#baseUrl ? { baseUrl: this.#baseUrl } : {}),
    }
    const workspaceRelay = this.#createRelay(options)
    const registration = await workspaceRelay.agents.register({
      name: this.#publisherName,
      type: 'system',
    })
    this.#workspaceRelay = workspaceRelay
    this.#agentRelay = this.#createRelay({ ...options, agentToken: registration.token })
    return this.#agentRelay
  }
}

export function contractChannelName(pageId: string, sourceKey: string, contentDigest: string): string {
  const sourceSuffix = createHash('sha256').update(sourceKey).digest('hex').slice(0, 8)
  return `factory-notion-${pageId.slice(-8)}-${sourceSuffix}-${contentDigest.slice(0, 10)}`
}

export function contractMarkerPrefix(pageId: string, contentDigest: string): string {
  return `factory-notion-contract-v1:${pageId}:${contentDigest}:part:`
}

function splitContract(encoded: string): string[] {
  const chunks: string[] = []
  for (let offset = 0; offset < encoded.length; offset += CONTRACT_CHUNK_CHARACTERS) {
    chunks.push(encoded.slice(offset, offset + CONTRACT_CHUNK_CHARACTERS))
  }
  return chunks.length > 0 ? chunks : ['']
}

async function listAllMessages(relay: ContractRelay, channel: string): Promise<RelayMessage[]> {
  const messages: RelayMessage[] = []
  let before: string | undefined
  for (let pageNumber = 0; pageNumber < 100; pageNumber += 1) {
    const page = await relay.messages.list(channel, { limit: 100, ...(before ? { before } : {}) })
    messages.push(...page)
    if (page.length < 100) return messages
    const nextBefore = page.at(-1)?.id
    if (!nextBefore || nextBefore === before) {
      throw new Error('portable Notion contract message pagination did not advance')
    }
    before = nextBefore
  }
  throw new Error('portable Notion contract digest channel exceeds the 10,000-message safety limit')
}
