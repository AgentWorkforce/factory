import type { GithubConnectionIssue, GithubConnectionRead } from '../ports'

const GITHUB_ISSUE_QUERY = `
  query FactoryIssue($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
      issue(number: $number) {
        id
        number
        title
        body
        state
        url
        updatedAt
        author { login }
        labels(first: 100) { nodes { name } }
      }
    }
  }
`

export interface RelayfileGithubConnectionRequest {
  requestJson(options: {
    operation: string
    method: string
    path: string
    body?: unknown
    timeoutMs?: number
  }): Promise<unknown>
}

export interface RelayfileGithubConnectionReadConfig {
  workspace: RelayfileGithubConnectionRequest
}

/**
 * Read-only GitHub issue lookup through the authenticated Relayfile workspace
 * connection. WorkspaceHandle.requestJson supplies the Relayfile workspace
 * token; Factory never receives or shells out with a provider credential.
 */
export class RelayfileGithubConnectionRead implements GithubConnectionRead {
  readonly #workspace: RelayfileGithubConnectionRequest

  constructor(config: RelayfileGithubConnectionReadConfig) {
    this.#workspace = config.workspace
  }

  async getIssue(repo: string, number: number): Promise<GithubConnectionIssue | undefined> {
    const { owner, name } = githubRepoParts(repo)
    if (!Number.isSafeInteger(number) || number <= 0) {
      throw new Error(`GitHub issue number must be a positive integer: ${number}`)
    }

    const response = record(await this.#workspace.requestJson({
      operation: 'getGithubIssue',
      method: 'POST',
      path: 'api/v1/github/graphql',
      body: {
        query: GITHUB_ISSUE_QUERY,
        variables: { owner, repo: name, number },
      },
      timeoutMs: 30_000,
    }))
    const errors = Array.isArray(response?.errors) ? response.errors : []
    if (errors.length > 0) {
      const codes = errors
        .map((error) => stringValue(record(record(error)?.extensions)?.type) ?? stringValue(record(record(error)?.extensions)?.code))
        .filter((code): code is string => Boolean(code))
      throw new Error(`GitHub API issue lookup failed${codes.length > 0 ? ` (${[...new Set(codes)].join(', ')})` : ''}`)
    }

    const issue = record(record(record(response?.data)?.repository)?.issue)
    if (!issue) return undefined
    const resolvedNumber = positiveInteger(issue.number)
    const title = stringValue(issue.title)
    const url = stringValue(issue.url)
    if (resolvedNumber !== number || !title || !url) {
      throw new Error(`GitHub API issue lookup returned an incomplete record for ${repo}#${number}`)
    }

    const labels = record(issue.labels)?.nodes
    const author = stringValue(record(issue.author)?.login)
    const path = githubIssuePath(owner, name, number)
    return {
      repo: `${owner}/${name}`,
      number,
      path,
      content: {
        provider: 'github',
        objectType: 'issue',
        objectId: stringValue(issue.id) ?? `${owner}/${name}#${number}`,
        payload: {
          id: stringValue(issue.id),
          number,
          title,
          body: stringValue(issue.body) ?? '',
          state: (stringValue(issue.state) ?? '').toLowerCase(),
          url,
          html_url: url,
          updated_at: stringValue(issue.updatedAt),
          labels: Array.isArray(labels)
            ? labels.map((label) => ({ name: stringValue(record(label)?.name) })).filter((label) => Boolean(label.name))
            : [],
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
