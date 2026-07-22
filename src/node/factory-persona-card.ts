import { isDeepStrictEqual } from 'node:util'

import { A2aAgentCardSchema, type A2aAgentCard } from '@relaycast/a2a'
import * as personaKitSpec from '@agentworkforce/persona-kit/spec'

import type { PersonaIntent, PersonaSpec } from '@agentworkforce/persona-kit/spec'

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

type DeriveAgentCardOptions = {
  baseUrl: string
  version: string
  documentationUrl?: string
  inputModes?: string[]
  outputModes?: string[]
}
type DeriveAgentCard = (persona: PersonaSpec, options: DeriveAgentCardOptions) => A2aAgentCard
const A2A_TRANSPORT_CAPABILITIES = new Set(['streaming', 'pushNotifications'])

/**
 * Derive and schema-validate the card attached to a Factory-hosted persona.
 *
 * workforce#296 owns the canonical mapper. persona-kit 4.1.34 predates that
 * export, so this release keeps a compatibility mapper behind the same call
 * boundary. As soon as a persona-kit containing deriveAgentCard is installed,
 * it is selected automatically and every result is still parsed by the shared
 * @relaycast/a2a schema.
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
  const upstream = (personaKitSpec as typeof personaKitSpec & { deriveAgentCard?: DeriveAgentCard }).deriveAgentCard
  return A2aAgentCardSchema.parse(
    upstream ? upstream(persona, options) : deriveAgentCardCompatibility(persona, options),
  )
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
  if (!intent || !personaKitSpec.isIntent(intent)) {
    throw new Error('Factory-hosted persona must declare a valid persona-kit intent')
  }
  return personaKitSpec.parsePersonaSpec(value, intent as PersonaIntent)
}

function deriveAgentCardCompatibility(persona: PersonaSpec, options: DeriveAgentCardOptions): A2aAgentCard {
  const integrationTags = Object.keys(persona.integrations ?? {})
  const skills = persona.skills.map((skill) => ({
    id: skill.id,
    name: humanize(skill.id),
    description: skill.description,
    tags: unique([skill.source, ...integrationTags]),
  }))
  for (const [name, value] of Object.entries(persona.capabilities ?? {})) {
    if (!capabilityEnabled(value)) continue
    if (A2A_TRANSPORT_CAPABILITIES.has(name)) continue
    const canonicalName = name === 'pullRequest' ? 'review' : name
    const existing = skills.find((skill) => skill.id === canonicalName)
    if (existing) {
      existing.tags = unique([...(existing.tags ?? []), ...integrationTags])
      continue
    }
    skills.push({
      id: canonicalName,
      name: humanize(canonicalName),
      description: `Persona capability: ${humanize(canonicalName)}`,
      tags: unique(integrationTags),
    })
  }

  if (skills.length === 0) {
    skills.push({
      id: persona.intent,
      name: humanize(persona.intent),
      description: persona.description,
      tags: unique(integrationTags),
    })
  }

  const relayName = typeof persona.relay === 'object' && persona.relay !== null
    ? persona.relay.agentName
    : undefined
  return {
    name: relayName ?? persona.id,
    description: persona.description,
    url: options.baseUrl,
    version: options.version,
    skills,
    capabilities: {
      streaming: capabilityEnabled(persona.capabilities?.streaming),
      pushNotifications: capabilityEnabled(persona.capabilities?.pushNotifications),
    },
    default_input_modes: options.inputModes ?? ['text/plain', 'application/json'],
    default_output_modes: options.outputModes ?? ['text/plain', 'application/json'],
    provider: {
      organization: 'AgentWorkforce',
      persona_id: persona.id,
      intent: persona.intent,
      tags: [...(persona.tags ?? [])],
    },
    ...(options.documentationUrl ? { documentation_url: options.documentationUrl } : {}),
  }
}

function capabilityEnabled(value: unknown): boolean {
  if (value === true) return true
  if (value === false || value === undefined || value === null) return false
  return typeof value === 'object' && !Array.isArray(value) && (value as { enabled?: unknown }).enabled !== false
}

function humanize(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/gu, '$1 $2')
    .replace(/[-_:]+/gu, ' ')
    .replace(/\b\w/gu, (letter) => letter.toUpperCase())
}

function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))]
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
