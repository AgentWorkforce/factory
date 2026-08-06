import { createHash } from 'node:crypto'

import { AgentRelay } from '@agent-relay/sdk'
import type { RelayMessage } from '@agent-relay/sdk'
import { z } from 'zod'

import type { NotionIntakeClaim, NotionIntakeClaimStore } from './notion'

const CLAIM_MARKER = '---FACTORY NOTION INTAKE CLAIM V1---'

const claimRecordSchema = z.object({
  version: z.literal(1),
  sourceKey: z.string().min(1),
  digest: z.string().regex(/^[0-9a-f]{64}$/u),
  claimedAt: z.string().datetime(),
}).strict()

type RelayChannelClaimStoreOptions = {
  workspaceKey: string
  baseUrl?: string
  publisherName?: string
  createRelay?: (options: { workspaceKey: string; baseUrl?: string; agentToken?: string }) => ClaimRelay
}

type ClaimRelay = Pick<AgentRelay, 'agents' | 'channels' | 'messages' | 'messaging'>

/**
 * Stores one immutable claim per Notion source key in a workspace-global Relay
 * channel. Channel-name uniqueness is the cross-dispatcher compare-and-set;
 * message idempotency is deliberately not used because it is actor-scoped and
 * expires after a bounded interval.
 */
export class RelayChannelNotionClaimStore implements NotionIntakeClaimStore {
  readonly #workspaceKey: string
  readonly #baseUrl?: string
  readonly #publisherName: string
  readonly #createRelay: NonNullable<RelayChannelClaimStoreOptions['createRelay']>
  #workspaceRelay?: ClaimRelay
  #agentRelay?: ClaimRelay
  #relayReady?: Promise<ClaimRelay>
  #disposed = false

  constructor(options: RelayChannelClaimStoreOptions) {
    this.#workspaceKey = options.workspaceKey
    this.#baseUrl = options.baseUrl
    this.#publisherName = options.publisherName ??
      `factory-notion-claims-${process.pid}-${Date.now().toString(36)}`
    this.#createRelay = options.createRelay ?? ((relayOptions) => new AgentRelay(relayOptions))
  }

  async claim(input: NotionIntakeClaim): Promise<{
    status: 'claimed' | 'existing'
    claim: NotionIntakeClaim
  }> {
    if (this.#disposed) throw new Error('Notion claim store has been disposed')
    const record = claimRecordSchema.parse({ version: 1, ...input })
    const claim = publicClaim(record)
    const relay = await this.#relay()
    const channel = notionClaimChannelName(claim.sourceKey)

    try {
      await relay.channels.create({
        name: channel,
        topic: `Immutable Factory Notion claim ${claim.digest}`,
      })
    } catch (createError) {
      try {
        await relay.channels.join(channel)
      } catch (joinError) {
        throw new Error(`unable to create or observe durable Notion claim ${channel}`, {
          cause: new AggregateError([createError, joinError]),
        })
      }
      return {
        status: 'existing',
        claim: await readExistingClaim(relay, channel, claim.sourceKey),
      }
    }

    const text = renderClaim(record)
    const written = await relay.messages.send({ channel, text })
    if (written.text !== text) {
      throw new Error(`durable Notion claim acknowledgement did not match ${channel}`)
    }
    return { status: 'claimed', claim }
  }

  async get(sourceKey: string): Promise<NotionIntakeClaim | undefined> {
    if (this.#disposed) throw new Error('Notion claim store has been disposed')
    const relay = await this.#relay()
    const channel = notionClaimChannelName(sourceKey)
    try {
      await relay.channels.get(channel)
    } catch (error) {
      if (isMissingChannel(error)) return undefined
      throw error
    }
    await relay.channels.join(channel)
    return await readExistingClaim(relay, channel, sourceKey)
  }

  async dispose(): Promise<void> {
    this.#disposed = true
    await this.#relayReady?.catch(() => undefined)
    await this.#agentRelay?.messaging.events.disconnect().catch(() => undefined)
    await this.#workspaceRelay?.agents.delete(this.#publisherName).catch(() => undefined)
    this.#agentRelay = undefined
    this.#workspaceRelay = undefined
    this.#relayReady = undefined
  }

  async #relay(): Promise<ClaimRelay> {
    if (this.#agentRelay) return this.#agentRelay
    this.#relayReady ??= this.#initializeRelay()
    try {
      return await this.#relayReady
    } catch (error) {
      this.#relayReady = undefined
      throw error
    }
  }

  async #initializeRelay(): Promise<ClaimRelay> {
    const options = {
      workspaceKey: this.#workspaceKey,
      ...(this.#baseUrl ? { baseUrl: this.#baseUrl } : {}),
    }
    const workspaceRelay = this.#createRelay(options)
    const registration = await workspaceRelay.agents.register({
      name: this.#publisherName,
      type: 'system',
    })
    if (this.#disposed) {
      await workspaceRelay.agents.delete(this.#publisherName).catch(() => undefined)
      throw new Error('Notion claim store was disposed during initialization')
    }
    this.#workspaceRelay = workspaceRelay
    this.#agentRelay = this.#createRelay({ ...options, agentToken: registration.token })
    return this.#agentRelay
  }
}

function isMissingChannel(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false
  const record = error as { code?: unknown; status?: unknown }
  return record.code === 'channel_not_found' || record.status === 404
}

export function notionClaimChannelName(sourceKey: string): string {
  const suffix = createHash('sha256').update(sourceKey).digest('hex')
  return `factory-notion-claim-${suffix}`
}

function renderClaim(claim: z.infer<typeof claimRecordSchema>): string {
  return `${CLAIM_MARKER}\n${JSON.stringify(claim)}`
}

function publicClaim(record: z.infer<typeof claimRecordSchema>): NotionIntakeClaim {
  return {
    sourceKey: record.sourceKey,
    digest: record.digest,
    claimedAt: record.claimedAt,
  }
}

async function readExistingClaim(
  relay: ClaimRelay,
  channel: string,
  expectedSourceKey: string,
): Promise<NotionIntakeClaim> {
  const messages = await listAllMessages(relay, channel)
  const records = messages
    .filter((message) => message.text.startsWith(`${CLAIM_MARKER}\n`))
    .map((message) => claimRecordSchema.parse(JSON.parse(message.text.slice(CLAIM_MARKER.length + 1))))
  if (records.length !== 1) {
    throw new Error(
      `durable Notion claim ${channel} has ${records.length} immutable claim records; refusing dispatch`,
    )
  }
  const [claim] = records
  if (claim!.sourceKey !== expectedSourceKey) {
    throw new Error(`durable Notion claim ${channel} does not match its source key`)
  }
  return publicClaim(claim!)
}

async function listAllMessages(relay: ClaimRelay, channel: string): Promise<RelayMessage[]> {
  const messages: RelayMessage[] = []
  let before: string | undefined
  for (let pageNumber = 0; pageNumber < 100; pageNumber += 1) {
    const page = await relay.messages.list(channel, { limit: 100, ...(before ? { before } : {}) })
    messages.push(...page)
    if (page.length < 100) return messages
    const nextBefore = page.at(-1)?.id
    if (!nextBefore || nextBefore === before) {
      throw new Error('durable Notion claim message pagination did not advance')
    }
    before = nextBefore
  }
  throw new Error('durable Notion claim channel exceeds the 10,000-message safety limit')
}
