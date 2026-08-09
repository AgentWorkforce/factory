import type { GithubIssueLookup, GithubConnectionRead } from '../ports'

const GITHUB_API_BASE_URL = 'https://api.github.com'

type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>

export interface GithubApiIssueReadConfig {
  /** Internal request override for deterministic tests. */
  fetch?: FetchLike
}

/**
 * Read-only lookup against GitHub's REST API, unauthenticated.
 *
 * Factory never receives or invokes a GitHub credential here. GitHub
 * mutations continue to use the Relayfile connection writeback path with the
 * app author; this deliberately separate reader only recovers issue facts
 * when the preferred Relayfile projection cannot answer.
 *
 * Because every request is unauthenticated, this reader can only be
 * authoritative about repositories it can independently confirm are public
 * (see #isRepoPublic). GitHub returns an identical 404 for "issue does not
 * exist" and "repository is private, existence hidden from you" — without
 * that independent confirmation a 404 cannot be trusted as absence, so it is
 * reported as `indeterminate` instead of a false `not-found`.
 */
export class GithubApiIssueRead implements GithubConnectionRead {
  readonly #fetch: FetchLike
  readonly #repoIsPublic = new Map<string, boolean>()

  constructor(config: GithubApiIssueReadConfig = {}) {
    this.#fetch = config.fetch ?? fetch
  }

  async getIssue(repo: string, number: number): Promise<GithubIssueLookup> {
    const { owner, name } = githubRepoParts(repo)
    if (!Number.isSafeInteger(number) || number <= 0) {
      throw new Error(`GitHub issue number must be a positive integer: ${number}`)
    }

    const isPublic = await this.#isRepoPublic(owner, name)
    if (isPublic === undefined) {
      return {
        outcome: 'indeterminate',
        reason: `could not confirm ${owner}/${name} is publicly visible without authentication`,
      }
    }

    const response = await this.#fetch(
      `${GITHUB_API_BASE_URL}/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}/issues/${number}`,
      {
        method: 'GET',
        signal: AbortSignal.timeout(30_000),
        headers: {
          accept: 'application/vnd.github+json',
          'user-agent': '@agent-relay/factory',
        },
      },
    )
    if (response.status === 404) {
      // A confirmed-public repo's 404 is a trustworthy miss. Otherwise this
      // 404 is the same one GitHub returns to hide a private repo's
      // existence, and cannot be told apart from a real absence.
      return isPublic
        ? { outcome: 'not-found' }
        : { outcome: 'indeterminate', reason: `${owner}/${name} is not visible without authentication` }
    }
    if (isRateLimitOrAuthStatus(response.status)) {
      return { outcome: 'indeterminate', reason: `GitHub API issue lookup returned HTTP ${response.status}` }
    }
    if (!response.ok) {
      throw new Error(`GitHub API issue lookup failed (HTTP ${response.status})`)
    }

    const issue = record(await response.json())
    if (!issue) {
      throw new Error(`GitHub API issue lookup returned an incomplete issue record for ${repo}#${number}`)
    }
    // GitHub's REST "issues" endpoint also returns pull requests. For Factory
    // issue dispatch a PR occupying the same repository number is an
    // authoritative non-match, not a malformed issue.
    if (issue.pull_request !== undefined) return { outcome: 'not-found' }
    const resolvedNumber = positiveInteger(issue.number)
    const title = stringValue(issue.title)
    const url = stringValue(issue.html_url)
    if (resolvedNumber !== number || !title || !url) {
      throw new Error(`GitHub API issue lookup returned an incomplete issue record for ${repo}#${number}`)
    }

    const labels = Array.isArray(issue.labels) ? issue.labels : []
    const author = stringValue(record(issue.user)?.login)
    const path = githubIssuePath(owner, name, number)
    return {
      outcome: 'found',
      issue: {
        repo: `${owner}/${name}`,
        number,
        path,
        content: {
          provider: 'github',
          objectType: 'issue',
          objectId: stringValue(issue.node_id) ?? String(issue.id ?? `${owner}/${name}#${number}`),
          payload: {
            id: issue.id,
            node_id: issue.node_id,
            number,
            title,
            body: stringValue(issue.body) ?? '',
            state: (stringValue(issue.state) ?? '').toLowerCase(),
            url,
            html_url: url,
            updated_at: stringValue(issue.updated_at),
            labels: labels
              .map((label) => typeof label === 'string' ? { name: label } : { name: stringValue(record(label)?.name) })
              .filter((label): label is { name: string } => Boolean(label.name)),
            ...(author ? { user: { login: author }, author: { login: author } } : {}),
            repository: { name, owner: { login: owner } },
          },
        },
      },
    }
  }

  /**
   * Confirms repository visibility with one unauthenticated GET, cached for
   * the life of this reader. Returns `undefined` — never throws, never
   * caches — for any response that is not a clean 200 or 404, so a rate
   * limit or transient failure on this probe degrades a lookup to
   * `indeterminate` instead of turning it into a hard error or pinning a
   * wrong verdict.
   */
  async #isRepoPublic(owner: string, name: string): Promise<boolean | undefined> {
    const key = `${owner}/${name}`.toLowerCase()
    const cached = this.#repoIsPublic.get(key)
    if (cached !== undefined) return cached

    let response: Response
    try {
      response = await this.#fetch(
        `${GITHUB_API_BASE_URL}/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`,
        {
          method: 'GET',
          signal: AbortSignal.timeout(30_000),
          headers: {
            accept: 'application/vnd.github+json',
            'user-agent': '@agent-relay/factory',
          },
        },
      )
    } catch {
      return undefined
    }
    if (response.status === 200) {
      this.#repoIsPublic.set(key, true)
      return true
    }
    if (response.status === 404) {
      this.#repoIsPublic.set(key, false)
      return false
    }
    return undefined
  }
}

const isRateLimitOrAuthStatus = (status: number): boolean => status === 403 || status === 429

const githubRepoParts = (repo: string): { owner: string; name: string } => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0) {
    throw new Error(`GitHub repo must be owner/repo: ${repo}`)
  }
  return { owner, name }
}

const githubIssuePath = (owner: string, repo: string, number: number): string =>
  `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(repo)}/issues/by-id/${number}.json`

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.length > 0 ? value : undefined

const positiveInteger = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined
