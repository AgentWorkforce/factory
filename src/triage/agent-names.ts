import type { LinearIssue } from '../types'

export type FactoryAgentRoleName = 'impl' | 'review' | 'babysit' | 'workflow'

type AgentIssue = Pick<LinearIssue, 'key' | 'path' | 'raw'>

export function agentNameForRole(
  issue: AgentIssue,
  role: FactoryAgentRoleName,
  options: { repo?: string; discriminator?: string } = {},
): string {
  const suffixes: string[] = []
  if (options.discriminator) {
    suffixes.push(sanitizeAgentSlug(options.discriminator))
  }

  if (isGithubNativeIssue(issue)) {
    const repoSlug = githubNativeRepoSlug(issue) ?? repoSlugFromName(options.repo)
    if (repoSlug && !suffixes.some((suffix) => suffix === repoSlug || suffix.endsWith(`-${repoSlug}`))) {
      suffixes.push(repoSlug)
    }
  }

  return [agentBaseName(issue), role, ...suffixes].join('-')
}

export function agentBaseName(issue: Pick<LinearIssue, 'key'>): string {
  const number = issue.key.match(/\d+/u)?.[0] ?? sanitizeAgentSlug(issue.key)
  return `ar-${number}`
}

export function repoSlugFromName(repo: string | undefined): string | undefined {
  if (!repo?.trim()) return undefined
  return sanitizeAgentSlug(repo.split('/').at(-1) ?? repo)
}

export function sanitizeAgentSlug(slug: string): string {
  return slug.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'scope'
}

export function isGithubNativeIssue(issue: Pick<LinearIssue, 'key' | 'path' | 'raw'>): boolean {
  if (!/^\d+$/u.test(issue.key)) return false
  if (issue.path.startsWith('/github/repos/')) return true
  return stringValue(asRecord(issue.raw)?.provider)?.toLowerCase() === 'github'
}

function githubNativeRepoSlug(issue: AgentIssue): string | undefined {
  const wrapper = asRecord(issue.raw)
  const payload = asRecord(wrapper?.payload) ?? wrapper
  const source = asRecord(payload?.source)
  const sourceRepo = stringValue(source?.repo)
  if (sourceRepo) return repoSlugFromName(sourceRepo)

  const repository = asRecord(payload?.repository)
  const repositoryName = stringValue(repository?.name)
  if (repositoryName) return repoSlugFromName(repositoryName)

  const compact = issue.path.match(/^\/github\/repos\/[^/]+__([^/]+)\/issues\//u)
  if (compact?.[1]) return sanitizeAgentSlug(compact[1])
  const nested = issue.path.match(/^\/github\/repos\/[^/]+\/([^/]+)\/issues\//u)
  return nested?.[1] ? sanitizeAgentSlug(nested[1]) : undefined
}

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value.trim() : undefined
