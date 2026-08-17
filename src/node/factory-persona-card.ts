import { isDeepStrictEqual } from 'node:util'

import { A2aAgentCardSchema, type A2aAgentCard } from '@relaycast/a2a'
import {
  deriveAgentCard,
  isIntent,
  parsePersonaSpec,
  type DeriveAgentCardOptions,
  type PersonaIntent,
  type PersonaSpec,
} from '@agentworkforce/persona-kit/spec'

export interface FactoryPersonaCardInput {
  /** Raw persona.json data or an already parsed PersonaSpec. */
  persona: unknown
  /** Deployed agent origin; A2A RPC is available at `<baseUrl>/a2a/rpc`. */
  baseUrl: string
  version: string
  documentationUrl?: string
  inputModes?: string[]
  outputModes?: string[]
}

export interface AgentCardPublisher {
  publishAgentCard(card: A2aAgentCard): Promise<PublishedAgentCard>
}

export interface PublishedAgentCard {
  name: string
  address: string
  alreadyPublished?: boolean
  certification?: string
}

export interface RelaycastAgentCardPublisherOptions {
  baseUrl: string
  token: string
  fetch?: typeof globalThis.fetch
  timeoutMs?: number
}

/**
 * Derive the card with persona-kit's canonical mapper, then validate it against
 * the shared Relaycast schema before attaching it to a Factory-hosted persona.
 */
export function deriveFactoryPersonaCard(input: FactoryPersonaCardInput): A2aAgentCard {
  const persona = parsePersona(input.persona)
  const options: DeriveAgentCardOptions = {
    baseUrl: input.baseUrl,
    version: input.version,
    ...(input.documentationUrl ? { documentationUrl: input.documentationUrl } : {}),
    ...(input.inputModes ? { inputModes: input.inputModes } : {}),
    ...(input.outputModes ? { outputModes: input.outputModes } : {}),
  }
  return A2aAgentCardSchema.parse(deriveAgentCard(persona, options))
}

/** Publish an inline canonical card through Relaycast's A2A registration seam. */
export class RelaycastAgentCardPublisher implements AgentCardPublisher {
  readonly #baseUrl: string
  readonly #token: string
  readonly #fetch: typeof globalThis.fetch
  readonly #timeoutMs: number

  constructor(options: RelaycastAgentCardPublisherOptions) {
    this.#baseUrl = options.baseUrl.replace(/\/+$/u, '')
    this.#token = requiredText(options.token, 'RelaycastAgentCardPublisher token')
    this.#fetch = options.fetch ?? globalThis.fetch
    this.#timeoutMs = positiveTimeout(options.timeoutMs, 10_000)
  }

  async publishAgentCard(card: A2aAgentCard): Promise<PublishedAgentCard> {
    const canonical = A2aAgentCardSchema.parse(card)
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.#timeoutMs)
    try {
      const response = await this.#fetch(new URL('/v1/a2a/register', `${this.#baseUrl}/`), {
        method: 'POST',
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${this.#token}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ agent_card: canonical }),
        signal: controller.signal,
      })
      const payload = await readJson(response)
      if (response.status === 409) {
        return await this.#resolveExistingCard(canonical, controller.signal)
      }
      if (!response.ok) {
        throw new Error(`Relaycast rejected Factory persona card (${response.status}): ${errorDetail(payload)}`)
      }
      const data = asRecord(asRecord(payload)?.data)
      const address = readString(data, 'relay_name', 'relayName', 'address')
      if (!address) {
        throw new Error('Relaycast accepted the Factory persona card without returning its relay address')
      }
      return {
        name: canonical.name,
        address,
        ...(readString(data, 'certification') ? { certification: readString(data, 'certification') } : {}),
      }
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error(`Timed out publishing Factory persona card after ${this.#timeoutMs}ms`, { cause: error })
      }
      throw error
    } finally {
      clearTimeout(timeout)
    }
  }

  async #resolveExistingCard(
    canonical: A2aAgentCard,
    signal: AbortSignal,
  ): Promise<PublishedAgentCard> {
    const response = await this.#fetch(new URL('/v1/a2a/agents', `${this.#baseUrl}/`), {
      method: 'GET',
      headers: {
        accept: 'application/json',
        authorization: `Bearer ${this.#token}`,
      },
      signal,
    })
    const payload = await readJson(response)
    if (!response.ok) {
      throw new Error(
        `Relaycast reported an existing Factory persona card but its record could not be verified ` +
        `(${response.status}): ${errorDetail(payload)}`,
      )
    }

    const records = directoryRows(payload)
    let sameName = false
    for (const value of records) {
      const record = asRecord(value)
      const parsed = A2aAgentCardSchema.safeParse(record?.agent_card ?? record?.agentCard)
      if (!parsed.success || parsed.data.name !== canonical.name) continue
      sameName = true
      if (!isDeepStrictEqual(parsed.data, canonical)) continue
      const address = readString(record, 'relay_name', 'relayName', 'address')
      if (!address) {
        throw new Error('Relaycast returned the existing Factory persona card without its relay address')
      }
      return { name: canonical.name, address, alreadyPublished: true }
    }

    throw new Error(sameName
      ? `Relaycast already has a different card for Factory persona "${canonical.name}"`
      : `Relaycast reported a conflict but no card for Factory persona "${canonical.name}" was found`)
  }
}

function parsePersona(value: unknown): PersonaSpec {
  const record = asRecord(value)
  const intent = readString(record, 'intent')
  if (!intent || !isIntent(intent)) {
    throw new Error('Factory-hosted persona must declare a valid persona-kit intent')
  }
  return parsePersonaSpec(value, intent as PersonaIntent)
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text()
  if (!text) return undefined
  try {
    return JSON.parse(text) as unknown
  } catch (error) {
    throw new Error('Relaycast card publication returned invalid JSON', { cause: error })
  }
}

function errorDetail(payload: unknown): string {
  const record = asRecord(payload)
  const error = asRecord(record?.error)
  return readString(error, 'message') ?? readString(record, 'message') ?? 'request failed'
}

function directoryRows(payload: unknown): unknown[] {
  if (Array.isArray(payload)) return payload
  const record = asRecord(payload)
  return Array.isArray(record?.data) ? record.data : []
}

function positiveTimeout(value: number | undefined, fallback: number): number {
  return Number.isFinite(value) && (value ?? 0) > 0 ? Math.floor(value as number) : fallback
}

function requiredText(value: string, label: string): string {
  const normalized = value.trim()
  if (!normalized) throw new Error(`${label} must be a non-empty string`)
  return normalized
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function readString(record: Record<string, unknown> | undefined, ...keys: string[]): string | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}
