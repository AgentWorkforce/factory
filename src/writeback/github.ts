import type { MountClient } from '../ports'
import type { PrSummary } from '../types'
import { asRecord, wrappedPayload } from './shared'

const repoDir = (repo: string): string => {
  if (repo.includes('__')) {
    return repo
  }

  const [owner, name] = repo.split('/')
  if (!owner || !name) {
    throw new Error(`GitHub repo must be owner/repo or owner__repo: ${repo}`)
  }

  return `${owner}__${name}`
}

const prPath = (repo: string, number: number): string =>
  `/github/repos/${repoDir(repo)}/pulls/by-id/${number}.json`

const checksFromPayload = (payload: Record<string, unknown>): Array<{ name: string; status: string; conclusion?: string }> | undefined => {
  const checks = payload.checks ?? payload.check_runs ?? payload.status_checks
  if (!Array.isArray(checks)) {
    return undefined
  }

  return checks.map((check, index) => {
    const record = asRecord(check) ?? {}
    const name = typeof record.name === 'string'
      ? record.name
      : typeof record.context === 'string'
        ? record.context
        : `check-${index + 1}`
    const status = typeof record.status === 'string'
      ? record.status
      : typeof record.state === 'string'
        ? record.state
        : 'unknown'
    const conclusion = typeof record.conclusion === 'string'
      ? record.conclusion
      : undefined

    return { name, status, conclusion }
  })
}

export const MountGithubRead = (mount: MountClient) => ({
  async getPr(repo: string, number: number): Promise<PrSummary> {
    const { content } = await mount.readFile(prPath(repo, number))
    const payload = wrappedPayload(content)
    const state = typeof payload.state === 'string' ? payload.state : undefined
    const mergeable = typeof payload.mergeable === 'boolean'
      ? payload.mergeable
      : payload.mergeable === 'unknown'
        ? 'unknown'
        : undefined

    return {
      repo,
      number,
      title: typeof payload.title === 'string' ? payload.title : undefined,
      url: typeof payload.url === 'string' ? payload.url : undefined,
      state,
      status: state,
      checks: checksFromPayload(payload),
      mergeable,
    }
  },
})
