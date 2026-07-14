import type { GithubIssueWritebackState, MountClient } from '../ports'
import type { LinearIssue, PrSummary } from '../types'
import { asRecord, stableHash, wrappedPayload } from './shared'

export const GITHUB_IN_PROGRESS_LABEL = 'factory:in-progress'
export const GITHUB_HUMAN_REVIEW_LABEL = 'factory:human-review'

const FACTORY_STATUS_LABELS = new Set([
  GITHUB_IN_PROGRESS_LABEL,
  GITHUB_HUMAN_REVIEW_LABEL,
])

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

export const githubCommentName = (issue: LinearIssue, body: string): string =>
  `factory-${stableHash(`${githubIssueIdentity(issue).owner}/${githubIssueIdentity(issue).repo}#${githubIssueIdentity(issue).number}:${body}`)}`

export const MountGithubWriteback = (mount: MountClient) => ({
  async setState(issue: LinearIssue, state: GithubIssueWritebackState): Promise<void> {
    const identity = githubIssueIdentity(issue)
    const labels = issue.labels.filter((label) => !FACTORY_STATUS_LABELS.has(label.toLowerCase()))
    if (state === 'in-progress') labels.push(GITHUB_IN_PROGRESS_LABEL)
    if (state === 'human-review') labels.push(GITHUB_HUMAN_REVIEW_LABEL)

    const path = githubIssueWritePath(identity)
    await mount.writeFile(path, {
      labels: uniqueLabels(labels),
      state: state === 'done' ? 'closed' : 'open',
    }, { guarded: true })
    await assertWritebackAcked(mount, path)

    issue.labels = uniqueLabels(labels)
    issue.stateId = githubStateId(state)
  },

  async postComment(issue: LinearIssue, body: string): Promise<void> {
    const identity = githubIssueIdentity(issue)
    const name = githubCommentName(issue, body)
    const path = `/github/repos/${identity.owner}/${identity.repo}/issues/${identity.number}/comments/${name}.json`
    await mount.writeFile(path, { body }, { guarded: true })
    await assertWritebackAcked(mount, path)
  },

  async verify(
    issue: LinearIssue,
    expect: { state?: GithubIssueWritebackState; commentName?: string },
  ): Promise<boolean> {
    const identity = githubIssueIdentity(issue)
    if (expect.state) {
      const path = githubIssueWritePath(identity)
      await assertWritebackAcked(mount, path)
      return issue.stateId === githubStateId(expect.state)
    }
    if (expect.commentName) {
      const path = `/github/repos/${identity.owner}/${identity.repo}/issues/${identity.number}/comments/${expect.commentName}.json`
      await assertWritebackAcked(mount, path)
      return true
    }
    return false
  },
})

const githubIssueIdentity = (issue: LinearIssue): { owner: string; repo: string; number: number } => {
  const payload = wrappedPayload(issue.raw)
  const repository = asRecord(payload.repository)
  const owner = stringValue(asRecord(repository?.owner)?.login) ?? stringValue(asRecord(repository?.owner)?.name)
  const repo = stringValue(repository?.name)
  const number = numberValue(payload.number)
  const pathMatch = issue.path.match(/^\/github\/repos\/(?:([^/]+)\/([^/]+)|([^/]+)__([^/]+))\/issues\/(?:by-id\/)?(\d+)/u)
  const resolvedOwner = owner ?? pathMatch?.[1] ?? pathMatch?.[3]
  const resolvedRepo = repo ?? pathMatch?.[2] ?? pathMatch?.[4]
  const resolvedNumber = number ?? (pathMatch?.[5] ? Number(pathMatch[5]) : undefined)
  if (!resolvedOwner || !resolvedRepo || !resolvedNumber) {
    throw new Error(`GitHub issue ${issue.key} is missing owner, repo, or number`)
  }
  return { owner: resolvedOwner, repo: resolvedRepo, number: resolvedNumber }
}

const githubIssueWritePath = (identity: { owner: string; repo: string; number: number }): string =>
  `/github/repos/${identity.owner}/${identity.repo}/issues/${identity.number}.json`

const githubStateId = (state: GithubIssueWritebackState): string => `github:${state}`

const uniqueLabels = (labels: string[]): string[] => {
  const seen = new Set<string>()
  return labels.filter((label) => {
    const normalized = label.trim().toLowerCase()
    if (!normalized || seen.has(normalized)) return false
    seen.add(normalized)
    return true
  })
}

const assertWritebackAcked = async (mount: MountClient, path: string): Promise<void> => {
  const confirmation = await mount.confirmWrite(path, { timeoutMs: 90_000 })
  if (confirmation !== 'acked') {
    throw new Error(`Writeback not acked for ${path}: ${confirmation}`)
  }
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
