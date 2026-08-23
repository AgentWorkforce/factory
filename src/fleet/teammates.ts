import { randomUUID } from 'node:crypto'

import { A2aSkillSchema } from '@relaycast/a2a'

import { FleetDeliveryRejectedError, type AgentMessage, type FleetClient, type TeammateAgent, type TeammateQuery } from '../ports/fleet'

export const DEFAULT_RELAYCAST_BASE_URL = 'https://cast.agentrelay.com'
export const DEFAULT_TEAMMATE_DIRECTORY_TIMEOUT_MS = 10_000
export const DEFAULT_ASK_TEAMMATE_TIMEOUT_MS = 30_000

export interface TeammateDirectory {
  discover(query: TeammateQuery): Promise<TeammateAgent[]>
}

export interface RelaycastTeammateDirectoryOptions {
  baseUrl?: string
  token?: string
  fetch?: typeof globalThis.fetch
  timeoutMs?: number
}

/** Relaycast-backed, card-aware teammate discovery. */
export class RelaycastTeammateDirectory implements TeammateDirectory {
  readonly #baseUrl: string
  readonly #token?: string
  readonly #fetch: typeof globalThis.fetch
  readonly #timeoutMs: number

  constructor(options: RelaycastTeammateDirectoryOptions = {}) {
    this.#baseUrl = normalizeBaseUrl(options.baseUrl ?? DEFAULT_RELAYCAST_BASE_URL)
    this.#token = nonEmpty(options.token)
    this.#fetch = options.fetch ?? globalThis.fetch
    this.#timeoutMs = positiveTimeout(options.timeoutMs, DEFAULT_TEAMMATE_DIRECTORY_TIMEOUT_MS)
  }

  async discover(query: TeammateQuery): Promise<TeammateAgent[]> {
    const normalized = normalizeQuery(query)
    const url = new URL('/v1/a2a/directory', `${this.#baseUrl}/`)
    if (normalized.skill) url.searchParams.set('skill', normalized.skill)
    if (normalized.tag) url.searchParams.set('tag', normalized.tag)
    if (normalized.q) url.searchParams.set('q', normalized.q)

    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.#timeoutMs)
    try {
      const response = await this.#fetch(url, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          ...(this.#token ? { authorization: `Bearer ${this.#token}` } : {}),
        },
        signal: controller.signal,
      })
      const payload = await readJson(response)
      if (!response.ok) {
        throw new Error(`Relaycast teammate directory returned ${response.status}: ${errorDetail(payload)}`)
      }

      const rows = directoryRows(payload)
        .map(parseDirectoryEntry)
        .filter((entry): entry is TeammateAgent => Boolean(entry))
        // Keep the client honest even if a server version ignores a filter. This
        // also makes an unknown exact skill/tag deterministically return [].
        .filter((entry) => matchesQuery(entry, normalized))

      const unique = new Map<string, TeammateAgent>()
      for (const entry of rows) unique.set(`${entry.kind}:${entry.address}`, entry)
      return [...unique.values()]
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error(`Timed out querying the Relaycast teammate directory after ${this.#timeoutMs}ms`, { cause: error })
      }
      throw error
    } finally {
      clearTimeout(timeout)
    }
  }
}

export interface AskTeammateInput {
  /** Worker identity that is asking the question. */
  from: string
  question: string
  /** Use an already-discovered target, or provide a query to resolve one. */
  teammate?: TeammateAgent
  skill?: string
  tag?: string
  q?: string
  timeoutMs?: number
}

export interface AskTeammateResult {
  requestId: string
  teammate: TeammateAgent
  reply: AgentMessage
}

/**
 * Claimed teammate pairs per backend.
 *
 * `AgentMessage` carries no echoed request field on ANY backend -- there is no
 * reply-to, and `SendInput.data` is not reflected back -- so a reply can never
 * be attributed to one of two questions to the same teammate. Two devices
 * follow from that:
 *
 * - While an ask is waiting, the pair is claimed. A second ask
 *   is refused rather than resolved with an answer that may not be its own.
 *   The claim set uses every sender identity accepted for the RESOLVED
 *   teammate and is taken after discovery, so aliases and direct asks contend.
 * - After a TIMEOUT a confirmed send stays quarantined for this client. There
 *   is no safe time- or message-based release: the transport places no upper
 *   bound on a late answer, and an unrelated DM cannot be distinguished from
 *   that answer. Only a definitive late delivery failure frees the pair. When
 *   `AgentMessage` grows an observable correlation field, this registry can be
 *   replaced by a requestId check.
 *
 * Scoped per `FleetClient` because two clients in one process address
 * different workspaces, where identical names cannot collide.
 */
type AskClaim = 'waiting' | 'timed-out-uncorrelated'

const inFlightAsks = new WeakMap<FleetClient, Map<string, AskClaim>>()

function claimsFor(fleet: FleetClient): Map<string, AskClaim> {
  let claims = inFlightAsks.get(fleet)
  if (!claims) {
    claims = new Map()
    inFlightAsks.set(fleet, claims)
  }
  return claims
}

/**
 * Resolve a teammate (when needed), deliver a relay DM, and wait for that
 * teammate's reply. The listener is armed before sending so fast replies cannot
 * race the waiter; the whole operation has one bounded deadline.
 *
 * Replies are matched against the identity the backend actually authors as --
 * see `FleetClient.effectiveSender` -- because a backend that cannot represent
 * `from` receives the teammate's reply under its own identity instead.
 */
export async function askTeammate(fleet: FleetClient, input: AskTeammateInput): Promise<AskTeammateResult> {
  const timeoutMs = positiveTimeout(input.timeoutMs, DEFAULT_ASK_TEAMMATE_TIMEOUT_MS)
  const requestId = randomUUID()
  const from = requiredText(input.from, 'askTeammate.from')
  const question = requiredText(input.question, 'askTeammate.question')
  const query = normalizeQuery({ skill: input.skill, tag: input.tag, q: input.q })
  if (!input.teammate && !query.skill && !query.tag && !query.q) {
    throw new Error('askTeammate requires a discovered teammate or a skill/tag/query')
  }
  const deadline = Date.now() + timeoutMs

  return await new Promise<AskTeammateResult>((resolve, reject) => {
    let settled = false
    let deliveryState: 'not-started' | 'pending' | 'confirmed' = 'not-started'
    let unsubscribe = () => {}
    let claimed: string[] = []
    let timedOutClaim: string[] = []
    const release = () => {
      for (const key of claimed) claimsFor(fleet).delete(key)
      claimed = []
    }
    const quarantine = () => {
      for (const key of claimed) claimsFor(fleet).set(key, 'timed-out-uncorrelated')
      timedOutClaim = claimed
      claimed = []
    }
    const releaseTimedOutClaim = () => {
      for (const key of timedOutClaim) claimsFor(fleet).delete(key)
      timedOutClaim = []
    }
    const finish = (result: AskTeammateResult) => {
      if (settled) return
      settled = true
      clearTimeout(timeout)
      unsubscribe()
      release()
      resolve(result)
    }
    const fail = (error: unknown) => {
      if (settled) return
      settled = true
      clearTimeout(timeout)
      unsubscribe()
      release()
      reject(error)
    }
    const timeout = setTimeout(() => {
      if (settled) return
      settled = true
      unsubscribe()
      if (deliveryState === 'not-started') release()
      else quarantine()
      reject(new Error(`Timed out waiting for a reply from a teammate after ${timeoutMs}ms`))
    }, timeoutMs)

    void (async () => {
      // Whoever the teammate will actually reply to. Resolved before the
      // listener is armed, and awaited because a backend may need an auth round
      // trip to know its own name. Falls back to `from` on a backend that
      // carries the requested sender faithfully.
      const replyTarget = (await fleet.effectiveSender?.()) ?? from
      const teammate = input.teammate ?? (await fleet.discoverTeammates(query))[0]
      if (settled) return
      if (!teammate) {
        throw new Error('No teammate matched the requested skill/tag/query')
      }
      if (!fleet.onAgentMessage) {
        throw new Error('This fleet backend cannot observe teammate replies')
      }
      // Claimed against the RESOLVED teammate, so a discovery-based ask
      // contends with an explicit one for the same target. Taken before the
      // listener is armed so two waiters can never both accept one reply.
      const askKeys = teammateClaimKeys(replyTarget, teammate)
      const claims = claimsFor(fleet)
      const existingClaim = askKeys.map((key) => claims.get(key)).find((claim) => claim !== undefined)
      if (existingClaim !== undefined) {
        throw new Error(
          existingClaim === 'waiting'
            ? `askTeammate already has an unanswered question to "${teammate.name}" as "${replyTarget}". ` +
              'A reply carries no correlation id, so it cannot be attributed to one of two open ' +
              'questions; await the first before asking again.'
            : `askTeammate is quarantining "${teammate.name}" for "${replyTarget}" after a timed-out ` +
              'question. Replies carry no correlation id and have no maximum delay, so retry is ' +
              'unsafe for this client until the protocol supplies correlation.',
        )
      }
      for (const key of askKeys) claims.set(key, 'waiting')
      claimed = askKeys
      // `onAgentMessage` can return before the transport is really listening,
      // so wait for observability BEFORE sending -- otherwise a fast reply
      // lands in the gap and is lost.
      unsubscribe = fleet.onAgentMessage((message) => {
        // The listener is armed before transport observability, but nothing
        // received before the send starts can answer this question.
        if (deliveryState === 'not-started') return
        if (!sameAgent(message.from, teammate.address) && !sameAgent(message.from, teammate.name)) return
        if (!sameAgent(message.target, replyTarget)) return
        finish({ requestId, teammate, reply: message })
      })
      const send = {
        to: teammate.address,
        from,
        text: question,
        mode: 'wait' as const,
        data: {
          factoryCapability: 'ask-a-teammate',
          requestId,
          requester: from,
        },
      }
      await fleet.whenMessagesObservable?.()
      if (settled) return
      deliveryState = 'pending'
      try {
        if (fleet.waitForInjected) {
          await fleet.waitForInjected(send, { timeoutMs: Math.max(1, deadline - Date.now()) })
        } else {
          await fleet.sendMessage(send)
        }
        deliveryState = 'confirmed'
      } catch (error) {
        // The timeout may win while delivery confirmation is still pending.
        // Only positive, correlated transport rejection proves no answer can
        // arrive. An ordinary waitForInjected timeout is ambiguous: the send
        // may already have landed, so its uncorrelated pair stays quarantined.
        if (settled && timedOutClaim.length > 0 && error instanceof FleetDeliveryRejectedError) {
          releaseTimedOutClaim()
          return
        }
        throw error
      }
    })().catch(fail)
  })
}

function normalizeQuery(query: TeammateQuery): TeammateQuery {
  return {
    skill: nonEmpty(query.skill),
    tag: nonEmpty(query.tag),
    q: nonEmpty(query.q),
  }
}

function matchesQuery(entry: TeammateAgent, query: TeammateQuery): boolean {
  const skill = normalizedComparable(query.skill)
  if (skill && !entry.skills.some((candidate) =>
    normalizedComparable(candidate.id) === skill || normalizedComparable(candidate.name) === skill)) return false

  const tag = normalizedComparable(query.tag)
  if (tag) {
    const tags = [...entry.tags, ...entry.skills.flatMap((candidate) => candidate.tags ?? [])]
    if (!tags.some((candidate) => normalizedComparable(candidate) === tag)) return false
  }

  // Relaycast also searches aliases that are intentionally not exposed in a
  // directory row (for example, an A2A card name behind its derived ext-* relay
  // identity). Re-applying q here would discard those valid server matches.
  // Exact skill/tag filters are fully represented by the row, so those remain
  // enforced client-side to protect callers from an older unfiltered server.
  return true
}

function parseDirectoryEntry(value: unknown): TeammateAgent | undefined {
  const record = asRecord(value)
  const name = readString(record, 'name')
  const url = readString(record, 'url', 'endpoint_url', 'endpointUrl')
  const kind = readString(record, 'kind')
  if (!name || !url || (kind !== 'native' && kind !== 'a2a')) return undefined
  try {
    new URL(url)
  } catch {
    return undefined
  }

  const rawSkills = Array.isArray(record?.skills) ? record.skills : []
  const skills = rawSkills.flatMap((value) => {
    const candidate = typeof value === 'string'
      ? { id: value, name: value }
      : value
    const parsed = A2aSkillSchema.safeParse(candidate)
    return parsed.success ? [parsed.data] : []
  })
  const tags = readStrings(record, 'tags')
  const address = readString(record, 'address', 'relay_name', 'relayName', 'target') ?? name
  return {
    name,
    ...(readString(record, 'description') ? { description: readString(record, 'description') } : {}),
    skills,
    url,
    kind,
    address,
    tags,
    ...(readString(record, 'status') ? { status: readString(record, 'status') } : {}),
    ...(readString(record, 'certification') ? { certification: readString(record, 'certification') } : {}),
  }
}

function directoryRows(payload: unknown): unknown[] {
  if (Array.isArray(payload)) return payload
  const record = asRecord(payload)
  return Array.isArray(record?.data) ? record.data : []
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text()
  if (!text) return undefined
  try {
    return JSON.parse(text) as unknown
  } catch (error) {
    throw new Error('Relaycast teammate directory returned invalid JSON', { cause: error })
  }
}

function errorDetail(payload: unknown): string {
  const record = asRecord(payload)
  const error = asRecord(record?.error)
  return readString(error, 'message') ?? readString(record, 'message') ?? 'request failed'
}

function positiveTimeout(value: number | undefined, fallback: number): number {
  return Number.isFinite(value) && (value ?? 0) > 0 ? Math.floor(value as number) : fallback
}

function normalizeBaseUrl(value: string): string {
  return value.replace(/\/+$/u, '')
}

function teammateClaimKeys(replyTarget: string, teammate: TeammateAgent): string[] {
  const target = agentKey(replyTarget)
  // Claim every sender identity the listener below accepts. Directory kind is
  // not reply identity, and an external address plus its public name alias can
  // overlap a direct ask even though their primary addresses differ.
  return [...new Set([teammate.address, teammate.name].map(agentKey))]
    .map((sender) => `${target}\u0000${sender}`)
}

function sameAgent(left: string, right: string): boolean {
  return agentKey(left) === agentKey(right)
}

function agentKey(value: string): string {
  return value.replace(/^@/u, '').toLowerCase()
}

function normalizedComparable(value: string | undefined): string {
  return value?.trim().toLowerCase() ?? ''
}

function nonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed || undefined
}

function requiredText(value: string, label: string): string {
  const normalized = nonEmpty(value)
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

function readStrings(record: Record<string, unknown> | undefined, key: string): string[] {
  const value = record?.[key]
  return Array.isArray(value)
    ? [...new Set(value.filter((entry): entry is string => typeof entry === 'string' && Boolean(entry.trim())).map((entry) => entry.trim()))]
    : []
}
