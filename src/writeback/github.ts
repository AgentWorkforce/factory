import type { MountClient } from '../ports'
import type { GithubIssueStatus, GithubWriteback } from '../ports/writeback'
import { defaultGhRunner, type GhRunner } from '../github/merge-gate'
import type { LinearIssue, PrSummary } from '../types'
import { asRecord, wrappedPayload } from './shared'

const STATUS_LABELS: Record<GithubIssueStatus, { name: string; color: string; description: string }> = {
  'in-progress': {
    name: 'factory:in-progress',
    color: '1d76db',
    description: 'Factory agents are working on this issue.',
  },
  'human-review': {
    name: 'factory:human-review',
    color: 'fbca04',
    description: 'Factory work is ready for human review.',
  },
}

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

export const MountGithubRead = (mount: MountClient) => ({
  async getPr(repo: string, number: number): Promise<PrSummary> {
    const { content } = await mount.readFile(prPath(repo, number))
    const payload = wrappedPayload(content)

    return {
      repo,
      number: numberValue(payload.number) ?? number,
      title: typeof payload.title === 'string' ? payload.title : undefined,
      url: typeof payload.url === 'string' ? payload.url : undefined,
      state: typeof payload.state === 'string' ? payload.state : undefined,
      headRef: refName(payload.headRef) ?? refName(payload.head) ?? stringValue(payload.head_ref),
      baseRef: refName(payload.baseRef) ?? refName(payload.base) ?? stringValue(payload.base_ref),
      author: refName(payload.author) ?? stringValue(payload.user),
      filesChanged: filesChanged(payload.files_changed ?? payload.filesChanged ?? payload.files),
    }
  },
})

export interface GhCliGithubWritebackConfig {
  runner?: GhRunner
}

/**
 * GitHub issue lifecycle writeback using authenticated `gh` primitives.
 * TODO(issue-52): migrate issue labels/comments to the mounted GitHub
 * connection after the PR publication path has proven stable.
 *
 * Labels are created idempotently before use so a newly-onboarded repository
 * does not need factory status labels to be provisioned by hand.
 */
export class GhCliGithubWriteback implements GithubWriteback {
  readonly #run: GhRunner

  constructor(config: GhCliGithubWritebackConfig = {}) {
    this.#run = config.runner ?? defaultGhRunner
  }

  async postComment(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.#run([
      'issue',
      'comment',
      String(ref.number),
      '--repo',
      ref.repo,
      '--body',
      body,
    ])
  }

  async setStatus(issue: LinearIssue, status: GithubIssueStatus): Promise<void> {
    const ref = githubIssueRef(issue)
    const target = STATUS_LABELS[status]
    const previous = STATUS_LABELS[status === 'in-progress' ? 'human-review' : 'in-progress']
    await this.#run([
      'label',
      'create',
      target.name,
      '--repo',
      ref.repo,
      '--color',
      target.color,
      '--description',
      target.description,
      '--force',
    ])
    const labels = await this.#issueLabels(ref)
    const editArgs = [
      'issue',
      'edit',
      String(ref.number),
      '--repo',
      ref.repo,
    ]
    if (!labels.has(target.name.toLowerCase())) {
      editArgs.push('--add-label', target.name)
    }
    if (labels.has(previous.name.toLowerCase())) {
      editArgs.push('--remove-label', previous.name)
    }
    if (editArgs.length > 5) {
      await this.#run(editArgs)
    }
  }

  async #issueLabels(ref: { repo: string; number: number }): Promise<Set<string>> {
    const result = await this.#run([
      'issue',
      'view',
      String(ref.number),
      '--repo',
      ref.repo,
      '--json',
      'labels',
    ])
    if (!result.stdout.trim()) {
      return new Set()
    }
    const parsed = JSON.parse(result.stdout) as { labels?: Array<{ name?: unknown }> }
    return new Set(
      (parsed.labels ?? [])
        .map((label) => stringValue(label.name)?.toLowerCase())
        .filter((label): label is string => Boolean(label)),
    )
  }

  async closeIssue(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.postComment(issue, body)
    await this.#run([
      'issue',
      'close',
      String(ref.number),
      '--repo',
      ref.repo,
      '--reason',
      'completed',
    ])
  }
}

const githubIssueRef = (issue: LinearIssue): { repo: string; number: number; url: string } => {
  const payload = wrappedPayload(issue.raw)
  const source = asRecord(payload.source)
  const provider = stringValue(source?.provider)?.toLowerCase()
  const owner = stringValue(source?.owner)
  const repoName = stringValue(source?.repo)
  const number = numberValue(source?.number)
  const url = stringValue(source?.url)
  if (provider !== 'github' || !owner || !repoName || !Number.isInteger(number) || (number ?? 0) <= 0 || !url) {
    throw new Error(`GitHub writeback requires a stable GitHub issue source: ${issue.key}`)
  }
  const repo = `${owner}/${repoName}`
  const normalizedUrl = url.toLowerCase()
  const expectedUrlPrefixes = [
    `https://github.com/${repo}/issues/${number}`,
    `https://api.github.com/repos/${repo}/issues/${number}`,
  ].map((candidate) => candidate.toLowerCase())
  if (!expectedUrlPrefixes.some((prefix) => matchesBoundary(normalizedUrl, prefix))) {
    throw new Error(`GitHub writeback source URL does not match ${repo}#${number}`)
  }
  return { repo, number: number!, url }
}

const matchesBoundary = (value: string, prefix: string): boolean => {
  if (!value.startsWith(prefix)) return false
  const next = value[prefix.length]
  return next === undefined || next === '/' || next === '?' || next === '#'
}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' ? value : undefined

const refName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.ref) ?? stringValue(record?.login)
}

const filesChanged = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined
  }

  const files = value
    .map((entry) => typeof entry === 'string' ? entry : stringValue(asRecord(entry)?.path) ?? stringValue(asRecord(entry)?.filename))
    .filter((entry): entry is string => Boolean(entry))
  return files.length > 0 ? files : undefined
}
