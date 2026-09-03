import { randomUUID } from 'node:crypto'

import type { GithubConnectionRead, GithubIssueLookup } from '../ports'

type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>

export type GithubReadTokenProvider = () => Promise<string> | string

export interface RelayfileGithubConnectionReadConfig {
  workspaceId: string
  baseUrl: string
  tokenProvider: GithubReadTokenProvider
  /** Internal request override for deterministic tests. */
  fetch?: FetchLike
  /** Internal correlation-id override for deterministic tests. */
  correlationIdFactory?: () => string
  /** Internal request timeout override for tests. */
  timeoutMs?: number
}

const DEFAULT_READ_TIMEOUT_MS = 30_000

/**
 * Mount-native GitHub issue read as the connected App actor
 * (relayfile-cloud#159, shipped in relayfile-cloud PR #160). Factory never
 * receives or holds a GitHub credential here: the mount performs the read
 * server-side through its existing Nango-backed App connection, using
 * Factory's ordinary rotating Relayfile workspace bearer token as the
 * transport credential — the same mount-native pattern
 * `RelayfileGithubConnectionWrite` uses for `author: 'app'` writes.
 *
 * Response contract (accepted on factory#228 / relayfile-cloud#159, amended
 * by factory-lead on relayfile-cloud#159):
 * - HTTP 200 carries the existing three-outcome `GithubIssueLookup` body
 *   (`found` / `not-found` / `indeterminate`) verbatim — passed through here
 *   unchanged.
 * - Any other HTTP status (424 absent workspace connection, 503 missing
 *   server credential config, 403 `actor_not_supported`, 502 malformed/failed
 *   provider read, etc.) is a loud, operator-visible configuration failure,
 *   not a lookup outcome. It throws instead of degrading to `indeterminate`:
 *   folding it into `indeterminate` would make a broken credential path
 *   present as "no work found" workspace-wide instead of a diagnosable error.
 * - A failure to reach the mount at all (network error, DNS, or this
 *   client's own request timeout) is genuinely unknown lookup state, distinct
 *   from the mount's own structured error responses above, so it degrades to
 *   `indeterminate` the same way the unauthenticated GitHub API reader
 *   degrades an unclassifiable failure. A failure to resolve the bearer token
 *   itself is a credential-path failure, not a transport one, so it is
 *   resolved outside that degradation and always throws.
 */
export class RelayfileGithubConnectionRead implements GithubConnectionRead {
  readonly #workspaceId: string
  readonly #baseUrl: string
  readonly #tokenProvider: GithubReadTokenProvider
  readonly #fetch: FetchLike
  readonly #correlationIdFactory: () => string
  readonly #timeoutMs: number

  constructor(config: RelayfileGithubConnectionReadConfig) {
    const baseUrl = config.baseUrl.replace(/\/+$/u, '')
    if (new URL(baseUrl).protocol !== 'https:') {
      throw new Error(`Relayfile mount base URL must use https: ${baseUrl}`)
    }
    this.#workspaceId = config.workspaceId
    this.#baseUrl = baseUrl
    this.#tokenProvider = config.tokenProvider
    this.#fetch = config.fetch ?? fetch
    this.#correlationIdFactory = config.correlationIdFactory ?? randomUUID
    this.#timeoutMs = config.timeoutMs ?? DEFAULT_READ_TIMEOUT_MS
  }

  async getIssue(repo: string, number: number): Promise<GithubIssueLookup> {
    const { owner, name } = githubRepoParts(repo)
    if (!Number.isSafeInteger(number) || number <= 0) {
      throw new Error(`GitHub issue number must be a positive integer: ${number}`)
    }

    const url = `${this.#baseUrl}/v1/workspaces/${encodeURIComponent(this.#workspaceId)}` +
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}/issues/${number}/read`

    // Resolving the token is a credential-path operation, not a transport
    // one: a token-provider failure (e.g. no workspace token available) is
    // exactly the kind of loud, operator-visible configuration failure the
    // accepted contract amendment requires — it must not be caught below and
    // degraded to `indeterminate` alongside a genuine network failure.
    const token = await this.#tokenProvider()

    let response: Response
    try {
      response = await this.#fetch(url, {
        method: 'POST',
        signal: AbortSignal.timeout(this.#timeoutMs),
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          authorization: `Bearer ${token}`,
          'x-correlation-id': this.#correlationIdFactory(),
        },
        body: JSON.stringify({ actor: 'app' }),
      })
    } catch (error) {
      return {
        outcome: 'indeterminate',
        reason: `could not reach the Relayfile mount for ${owner}/${name}#${number}: ${errorMessage(error)}`,
      }
    }

    if (response.status !== 200) {
      throw new Error(
        `GitHub App issue read failed for ${owner}/${name}#${number}: HTTP ${response.status}${await errorDetail(response)}`,
      )
    }

    return parseGithubIssueLookup(await response.json(), owner, name, number)
  }
}

const parseGithubIssueLookup = (
  value: unknown,
  owner: string,
  name: string,
  number: number,
): GithubIssueLookup => {
  const body = record(value)
  const outcome = stringValue(body?.outcome)
  if (outcome === 'not-found') return { outcome: 'not-found' }
  if (outcome === 'indeterminate') {
    const reason = stringValue(body?.reason)
    if (!reason) {
      throw new Error(`GitHub App issue read returned indeterminate with no reason for ${owner}/${name}#${number}`)
    }
    return { outcome: 'indeterminate', reason }
  }
  if (outcome === 'found') {
    const issue = record(body?.issue)
    const path = stringValue(issue?.path)
    const issueRepo = stringValue(issue?.repo)
    const issueNumber = numberValue(issue?.number)
    const content = issue?.content
    if (!issue || !path || !issueRepo || issueNumber !== number || content === undefined) {
      throw new Error(`GitHub App issue read returned an incomplete issue record for ${owner}/${name}#${number}`)
    }
    return { outcome: 'found', issue: { repo: issueRepo, number: issueNumber, path, content } }
  }
  throw new Error(`GitHub App issue read returned an unrecognized response for ${owner}/${name}#${number}`)
}

const errorDetail = async (response: Response): Promise<string> => {
  try {
    const body = record(await response.clone().json())
    const code = stringValue(body?.code)
    const message = stringValue(body?.message)
    if (code || message) return ` ${[code, message].filter(Boolean).join(': ')}`
  } catch {
    // Error body is best-effort context; the HTTP status alone is diagnostic.
  }
  return ''
}

const githubRepoParts = (repo: string): { owner: string; name: string } => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0) {
    throw new Error(`GitHub repo must be owner/repo: ${repo}`)
  }
  return { owner, name }
}

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.length > 0 ? value : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined

const errorMessage = (error: unknown): string => (error instanceof Error ? error.message : String(error))
