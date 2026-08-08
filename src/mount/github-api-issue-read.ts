import type { GithubConnectionIssue, GithubConnectionRead } from '../ports'

const GITHUB_API_BASE_URL = 'https://api.github.com'

type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>

export interface GithubApiIssueReadConfig {
  /** Internal request override for deterministic tests. */
  fetch?: FetchLike
}

/**
 * Provider-authoritative, read-only lookup against GitHub's REST API.
 *
 * Factory never receives or invokes a GitHub CLI credential here. GitHub
 * mutations continue to use the Relayfile connection writeback path with the
 * app author; this deliberately separate reader only recovers public issue
 * facts when the preferred Relayfile projection cannot answer.
 */
export class GithubApiIssueRead implements GithubConnectionRead {
  readonly #fetch: FetchLike

  constructor(config: GithubApiIssueReadConfig = {}) {
    this.#fetch = config.fetch ?? fetch
  }

  async getIssue(repo: string, number: number): Promise<GithubConnectionIssue | undefined> {
    const { owner, name } = githubRepoParts(repo)
    if (!Number.isSafeInteger(number) || number <= 0) {
      throw new Error(`GitHub issue number must be a positive integer: ${number}`)
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
    if (response.status === 404) return undefined
    if (!response.ok) {
      throw new Error(`GitHub API issue lookup failed (HTTP ${response.status})`)
    }

    const issue = record(await response.json())
    if (!issue) {
      throw new Error(`GitHub API issue lookup returned an incomplete issue record for ${repo}#${number}`)
    }
    const resolvedNumber = positiveInteger(issue.number)
    const title = stringValue(issue.title)
    const url = stringValue(issue.html_url)
    if (resolvedNumber !== number || !title || !url || issue.pull_request !== undefined) {
      throw new Error(`GitHub API issue lookup returned an incomplete issue record for ${repo}#${number}`)
    }

    const labels = Array.isArray(issue.labels) ? issue.labels : []
    const author = stringValue(record(issue.user)?.login)
    const path = githubIssuePath(owner, name, number)
    return {
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
    }
  }
}

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
