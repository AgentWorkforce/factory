import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { promisify } from 'node:util'

import type {
  GithubCommentResult,
  GithubConnectionWrite,
  GithubIssueCommentInput,
  GithubIssueCreateInput,
  GithubIssueCreateResult,
  GithubIssueUpdateInput,
  GithubMergePullRequestInput,
  GithubMergePullRequestResult,
  GithubPublishPullRequestInput,
  GithubPublishPullRequestResult,
  GithubReviewCommentReplyInput,
  MountClient,
} from '../ports'

const execFileAsync = promisify(execFile)
const WRITE_CONFIRM_TIMEOUT_MS = 90_000
const RECEIPT_READ_ATTEMPTS = 5
const RECEIPT_READ_DELAY_MS = 100
const AUTHOR_READ_ATTEMPTS = 60
const AUTHOR_READ_DELAY_MS = 500

export type GitCommandRunner = (args: string[]) => Promise<{ stdout: string; stderr?: string }>

export interface RelayfileGithubConnectionWriteConfig {
  mount: Pick<MountClient, 'confirmWrite' | 'getConfirmedWriteExternalId' | 'getConfirmedWriteFailureReason' | 'listTree' | 'readFile' | 'writeFile'>
  gitRunner?: GitCommandRunner
  receiptReadAttempts?: number
  receiptReadDelayMs?: number
  /** Provider-projection polling for identity proof after an acknowledged write. */
  authorReadAttempts?: number
  authorReadDelayMs?: number
}

/**
 * GitHub PR mutations backed by the workspace's file-native Relayfile
 * connection. The adapter is server-side; Factory only depends on the stable
 * write paths and payload contracts exposed through MountClient.
 */
export class RelayfileGithubConnectionWrite implements GithubConnectionWrite {
  readonly #mount: RelayfileGithubConnectionWriteConfig['mount']
  readonly #git: GitCommandRunner
  readonly #receiptReadAttempts: number
  readonly #receiptReadDelayMs: number
  readonly #authorReadAttempts: number
  readonly #authorReadDelayMs: number
  readonly #writesByPath = new Map<string, Promise<string | undefined>>()
  #draftSequence = 0

  constructor(config: RelayfileGithubConnectionWriteConfig) {
    this.#mount = config.mount
    this.#git = config.gitRunner ?? defaultGitRunner
    this.#receiptReadAttempts = positiveInteger(config.receiptReadAttempts) ?? RECEIPT_READ_ATTEMPTS
    this.#receiptReadDelayMs = nonNegativeInteger(config.receiptReadDelayMs) ?? RECEIPT_READ_DELAY_MS
    this.#authorReadAttempts = positiveInteger(config.authorReadAttempts)
      ?? positiveInteger(config.receiptReadAttempts)
      ?? AUTHOR_READ_ATTEMPTS
    this.#authorReadDelayMs = nonNegativeInteger(config.authorReadDelayMs)
      ?? nonNegativeInteger(config.receiptReadDelayMs)
      ?? AUTHOR_READ_DELAY_MS
  }

  async publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const headRef = input.headRef ?? (input.clonePath
      ? await this.#gitValue(['-C', input.clonePath, 'symbolic-ref', '--short', 'HEAD'], 'current branch')
      : undefined)
    const headSha = input.headSha ?? (input.clonePath && !input.headRef
      ? await this.#gitValue(['-C', input.clonePath, 'rev-parse', 'HEAD'], 'HEAD commit')
      : undefined)
    if (!headRef) {
      throw new Error('GitHub PR publication requires headRef or clonePath')
    }
    if (headRef === input.baseRef) {
      throw new Error(`Refusing to publish GitHub PR with head equal to base branch: ${headRef}`)
    }
    const draftName = githubDraftName(headRef, headSha)
    const repoRoot = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
    const fullHeadRef = `refs/heads/${headRef}`
    // Relayfile's GitHub adapter exposes one guarded draft endpoint for create
    // and a canonical encoded endpoint for update. The draft filename is an
    // adapter contract; the requested branch identity lives in the payload.
    const createRefPath = `${repoRoot}/refs/factory.json`
    const updateRefPath = `${repoRoot}/refs/${encodeURIComponent(fullHeadRef)}.json`

    // A remote implementer already pushed its branch. For the legacy local-clone
    // path, create the branch before opening the PR. Relayfile's canonical encoded
    // ref path updates an existing ref and GitHub rejects it for a new branch.
    if (headSha) {
      try {
        await this.#writeAndConfirm(createRefPath, {
          ref: fullHeadRef,
          sha: headSha,
        })
      } catch (error) {
        if (!isGithubReferenceAlreadyExistsError(error)) throw error
        await this.#writeAndConfirm(updateRefPath, {
          ref: fullHeadRef,
          sha: headSha,
          force: false,
        })
      }
    }

    const pullRequestPath = `${repoRoot}/pull-requests/${draftName}.json`
    const confirmedPullRequestId = await this.#writeAndConfirm(pullRequestPath, {
      title: input.title,
      head: headRef,
      base: input.baseRef,
      body: input.body,
      author: 'app',
    })

    const { number, url } = await this.#readPullRequestReceipt(
      pullRequestPath,
      input.repo,
      confirmedPullRequestId,
    )

    return {
      repo: input.repo,
      number,
      url,
      headRef,
      headSha,
      // The workspace adapter contract explicitly requests app authorship.
      // The concrete bot login is installation-specific and is not included in
      // every acknowledgement receipt, so retain the stable identity label.
      author: 'app',
    }
  }

  async closePullRequest(input: { repo: string; number: number }): Promise<void> {
    const { owner, repo } = githubRepoParts(input.repo)
    if (!Number.isInteger(input.number) || input.number <= 0) {
      throw new Error(`GitHub pull request number must be a positive integer: ${input.number}`)
    }
    await this.#writeAndConfirm(
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/pulls/${input.number}/close.json`,
      {},
    )
  }

  async postIssueComment(input: GithubIssueCommentInput): Promise<GithubCommentResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const number = requirePositiveInteger(input.number, 'issue number')
    const body = requireBody(input.body, 'GitHub issue comment')
    const path = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
      + `/issues/${number}/comments/${this.#draftName(body)}.json`
    const externalId = await this.#writeAndConfirm(path, { body, author: 'app' })
    const commentId = positiveInteger(externalId)
    if (!commentId) {
      throw new Error(`Cannot prove GitHub App authorship for ${input.repo}#${number}: comment receipt has no provider id`)
    }
    await this.#assertAppCommentAuthor({ repo: input.repo, issueNumber: number, commentId })
    return {
      repo: input.repo,
      number,
      commentId,
      author: 'app',
    }
  }

  async replyToReviewComment(input: GithubReviewCommentReplyInput): Promise<GithubCommentResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const number = requirePositiveInteger(input.number, 'pull request number')
    const inReplyTo = requirePositiveInteger(input.inReplyTo, 'review comment id')
    const body = requireBody(input.body, 'GitHub review comment reply')
    // The adapter gives review-thread replies their own create-only namespace;
    // writing under the synced `/comments` projection would be an update.
    const path = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
      + `/pulls/${number}/review-comments/${inReplyTo}/replies/${this.#draftName(`${inReplyTo}:${body}`)}.json`
    const externalId = await this.#writeAndConfirm(path, { body, author: 'app' })
    const commentId = positiveInteger(externalId)
    if (!commentId) {
      throw new Error(`Cannot prove GitHub App authorship for ${input.repo}#${number}: review reply receipt has no provider id`)
    }
    await this.#assertAppCommentAuthor({ repo: input.repo, pullRequestNumber: number, commentId })
    return {
      repo: input.repo,
      number,
      commentId,
      author: 'app',
    }
  }

  async updateIssue(input: GithubIssueUpdateInput): Promise<void> {
    const { owner, repo } = githubRepoParts(input.repo)
    const number = requirePositiveInteger(input.number, 'issue number')
    const payload: Record<string, unknown> = { author: 'app' }
    // GitHub PATCHes the label set wholesale, so an explicit empty array is a
    // meaningful instruction ("clear every label"), not an absent field.
    if (input.labels) payload.labels = [...input.labels]
    if (input.state) payload.state = input.state
    if (input.title !== undefined) payload.title = input.title
    if (input.body !== undefined) payload.body = input.body
    if (Object.keys(payload).length === 1) {
      throw new Error(`GitHub issue update requires at least one mutable field for ${input.repo}#${number}`)
    }
    await this.#writeAndConfirm(
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/issues/${number}.json`,
      payload,
    )
  }

  async createIssue(input: GithubIssueCreateInput): Promise<GithubIssueCreateResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const title = requireBody(input.title, 'GitHub issue')
    const path = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
      + `/issues/${this.#draftName(`${title}\n${input.body}`)}.json`
    const externalId = await this.#writeAndConfirm(path, {
      title,
      body: input.body,
      ...(input.labels?.length ? { labels: [...input.labels] } : {}),
      author: 'app',
    })
    const number = positiveInteger(externalId)
    if (!number) {
      throw new Error(`GitHub issue creation returned an incomplete receipt for ${input.repo}`)
    }
    await this.#assertAppIssueAuthor(input.repo, number)
    return {
      repo: input.repo,
      number,
      url: `https://github.com/${input.repo}/issues/${number}`,
      author: 'app',
    }
  }

  async mergePullRequest(input: GithubMergePullRequestInput): Promise<GithubMergePullRequestResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const number = requirePositiveInteger(input.number, 'pull request number')
    const expectedHeadSha = requireBody(input.expectedHeadSha, 'GitHub merge expected head SHA')
    const sha = await this.#writeAndConfirm(
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/pulls/${number}/merge.json`,
      { method: input.method, sha: expectedHeadSha, author: 'app' },
    )
    return { ...(sha ? { sha } : {}) }
  }

  async #assertAppCommentAuthor(input: {
    repo: string
    issueNumber?: number
    pullRequestNumber?: number
    commentId: number
  }): Promise<void> {
    const author = await this.#readBackAuthor(async () => await readGithubCommentAuthor(this.#mount, input))
    assertGithubAppAuthor(author, `${input.repo} comment ${input.commentId}`)
  }

  async #assertAppIssueAuthor(repo: string, issueNumber: number): Promise<void> {
    const author = await this.#readBackAuthor(async () => await readGithubIssueAuthor(this.#mount, { repo, issueNumber }))
    assertGithubAppAuthor(author, `${repo} issue ${issueNumber}`)
  }

  async #readBackAuthor(
    read: () => Promise<GithubObservedAuthor | undefined>,
  ): Promise<GithubObservedAuthor | undefined> {
    for (let attempt = 0; attempt < this.#authorReadAttempts; attempt += 1) {
      const author = await read()
      if (author) return author
      if (attempt < this.#authorReadAttempts - 1) await delay(this.#authorReadDelayMs)
    }
    return undefined
  }

  /**
   * Draft filename for a create-shaped writeback. The adapter treats a numeric
   * trailing segment as an update to that provider id, so the name must never
   * be all digits. Content-derived plus a per-instance sequence so two
   * identical bodies on the same thread stay distinct writes.
   */
  #draftName(seed: string): string {
    this.#draftSequence += 1
    const hash = createHash('sha256').update(seed).digest('hex').slice(0, 12)
    return `factory-${hash}-${this.#draftSequence}`
  }

  async #gitValue(args: string[], description: string): Promise<string> {
    try {
      const value = (await this.#git(args)).stdout.trim()
      if (value) return value
    } catch (error) {
      throw new Error(`Unable to resolve ${description} for GitHub PR publication: ${errorMessage(error)}`)
    }
    throw new Error(`Unable to resolve ${description} for GitHub PR publication`)
  }

  async #readPullRequestReceipt(
    path: string,
    repo: string,
    confirmedExternalId?: string,
  ): Promise<{ number: number; url: string }> {
    // The cloud mount confirms provider success from the durable operation and
    // retains its externalId. Relayfile may immediately reconcile (rename or
    // remove) the authored draft, so reading that draft is not a reliable way
    // to recover the receipt after an acknowledged create.
    const confirmedNumber = positiveInteger(confirmedExternalId)
    if (confirmedNumber) {
      return {
        number: confirmedNumber,
        url: `https://github.com/${repo}/pull/${confirmedNumber}`,
      }
    }
    for (let attempt = 0; attempt < this.#receiptReadAttempts; attempt += 1) {
      try {
        const receipt = record((await this.#mount.readFile(path)).content)
        const number = positiveInteger(receipt.created)
        const url = stringValue(receipt.url)
        if (number && url) return { number, url }
      } catch {
        // Receipt propagation is eventually consistent after the write ack.
      }
      if (attempt < this.#receiptReadAttempts - 1) {
        await delay(this.#receiptReadDelayMs)
      }
    }
    throw new Error(`GitHub pull request writeback returned an incomplete receipt for ${repo}`)
  }

  async #writeAndConfirm(path: string, content: unknown): Promise<string | undefined> {
    const previous = this.#writesByPath.get(path) ?? Promise.resolve(undefined)
    const current = previous
      .catch(() => undefined)
      .then(async () => await this.#writeAndConfirmUnlocked(path, content))
    this.#writesByPath.set(path, current)
    try {
      return await current
    } finally {
      if (this.#writesByPath.get(path) === current) this.#writesByPath.delete(path)
    }
  }

  async #writeAndConfirmUnlocked(path: string, content: unknown): Promise<string | undefined> {
    await this.#mount.writeFile(path, content, { guarded: true })
    const status = await this.#mount.confirmWrite(path, { timeoutMs: WRITE_CONFIRM_TIMEOUT_MS })
    if (status !== 'acked') {
      const failureReason = status === 'failed'
        ? await this.#mount.getConfirmedWriteFailureReason?.(path)
        : undefined
      throw new Error(`GitHub writeback did not complete for ${path}: ${failureReason ?? status}`)
    }
    return this.#mount.getConfirmedWriteExternalId?.(path)
  }
}

const defaultGitRunner: GitCommandRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('git', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

const githubRepoParts = (value: string): { owner: string; repo: string } => {
  const match = /^([^/]+)\/([^/]+)$/u.exec(value.trim())
  if (!match?.[1] || !match[2]) {
    throw new Error(`GitHub repo must be owner/repo: ${value}`)
  }
  return { owner: match[1], repo: match[2] }
}

const requirePositiveInteger = (value: number, description: string): number => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`GitHub ${description} must be a positive integer: ${value}`)
  }
  return value
}

const requireBody = (value: string, description: string): string => {
  const trimmed = typeof value === 'string' ? value.trim() : ''
  if (!trimmed) throw new Error(`${description} requires a non-empty body`)
  return value
}

/**
 * Observed author of a GitHub comment, read back from the mounted projection.
 *
 * This is the only honest way to answer "who wrote that comment": the write
 * receipt carries the identity Factory *asked* for, while this reads what
 * GitHub actually recorded. Returns undefined when the projection has not
 * caught up, which callers must treat as "unknown", never as "app".
 */
export interface GithubObservedAuthor {
  login?: string
  type?: string
}

export async function readGithubCommentAuthor(
  mount: Pick<MountClient, 'listTree' | 'readFile'>,
  input: { repo: string; issueNumber?: number; pullRequestNumber?: number; commentId: number },
): Promise<GithubObservedAuthor | undefined> {
  const { owner, repo } = githubRepoParts(input.repo)
  const canonicalRoot = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
  const aliasRoot = `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(repo)}`
  const candidates = new Set<string>()
  if (input.issueNumber) {
    for (const root of [canonicalRoot, aliasRoot]) {
      candidates.add(`${root}/issues/${input.issueNumber}/comments/${input.commentId}/meta.json`)
      candidates.add(`${root}/issues/${input.issueNumber}/comments/${input.commentId}.json`)
      for (const path of await listTreeOrEmpty(mount, `${root}/issues/`)) {
        if (issueCommentPathMatches(path, input.issueNumber, input.commentId)) candidates.add(path)
      }
    }
  }
  if (input.pullRequestNumber) {
    for (const root of [canonicalRoot, aliasRoot]) {
      candidates.add(`${root}/pulls/${input.pullRequestNumber}/comments/${input.commentId}.json`)
      candidates.add(`${root}/pulls/${input.pullRequestNumber}/comments/${input.commentId}/meta.json`)
      for (const path of await listTreeOrEmpty(mount, `${root}/pulls/`)) {
        if (pullCommentPathMatches(path, input.pullRequestNumber, input.commentId)) candidates.add(path)
      }
    }
  }
  for (const path of candidates) {
    try {
      const author = githubObservedAuthor((await mount.readFile(path)).content)
      if (author) return author
    } catch {
      // Try the next mounted representation.
    }
  }
  return undefined
}

export async function readGithubIssueAuthor(
  mount: Pick<MountClient, 'listTree' | 'readFile'>,
  input: { repo: string; issueNumber: number },
): Promise<GithubObservedAuthor | undefined> {
  const { owner, repo } = githubRepoParts(input.repo)
  const candidates = new Set<string>()
  for (const root of [
    `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`,
    `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(repo)}`,
  ]) {
    candidates.add(`${root}/issues/${input.issueNumber}/meta.json`)
    candidates.add(`${root}/issues/by-id/${input.issueNumber}.json`)
    for (const path of await listTreeOrEmpty(mount, `${root}/issues/`)) {
      if (issuePathMatches(path, input.issueNumber)) candidates.add(path)
    }
  }
  for (const path of candidates) {
    try {
      const author = githubObservedAuthor((await mount.readFile(path)).content)
      if (author) return author
    } catch {
      // Try the next mounted representation.
    }
  }
  return undefined
}

const listTreeOrEmpty = async (
  mount: Pick<MountClient, 'listTree'>,
  root: string,
): Promise<string[]> => {
  try {
    return await mount.listTree(root)
  } catch {
    return []
  }
}

const issueCommentPathMatches = (path: string, issueNumber: number, commentId: number): boolean =>
  new RegExp(`/issues/${issueNumber}(?:__[^/]+)?/comments/${commentId}(?:\\.json|/(?:meta|metadata)\\.json)$`, 'u').test(path)

const pullCommentPathMatches = (path: string, pullRequestNumber: number, commentId: number): boolean =>
  new RegExp(`/pulls/${pullRequestNumber}(?:__[^/]+)?/(?:comments/${commentId}(?:\\.json|/(?:meta|metadata)\\.json)|reviews/[^/]+/comments/${commentId}(?:\\.json|/(?:meta|metadata)\\.json))$`, 'u').test(path)

const issuePathMatches = (path: string, issueNumber: number): boolean =>
  new RegExp(`/issues/(?:by-id/)?${issueNumber}(?:__[^/]+)?(?:\\.json|/(?:meta|metadata)\\.json)$`, 'u').test(path)

const githubObservedAuthor = (content: unknown): GithubObservedAuthor | undefined => {
  const envelope = record(content)
  const payload = record(envelope.payload)
  const candidates = [payload.author, payload.user, envelope.author, envelope.user]
  for (const candidate of candidates) {
    const actor = record(candidate)
    const login = stringValue(actor.login) ?? (typeof candidate === 'string' ? stringValue(candidate) : undefined)
    const type = stringValue(actor.type) ?? stringValue(actor.__typename)
    if (login || type) return { ...(login ? { login } : {}), ...(type ? { type } : {}) }
  }
  return undefined
}

const assertGithubAppAuthor = (author: GithubObservedAuthor | undefined, subject: string): void => {
  const login = author?.login?.trim()
  const type = author?.type?.trim()
  if (type?.toLowerCase() === 'bot' || login?.toLowerCase().endsWith('[bot]')) return
  const observed = login ?? type ?? 'unavailable'
  throw new Error(`GitHub App authorship check failed for ${subject}: provider recorded ${observed}`)
}

const githubDraftName = (headRef: string, headSha?: string): string => {
  const branch = headRef.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'branch'
  const identity = headSha?.slice(0, 12) ?? 'pushed'
  return `factory-${branch.slice(0, 80)}-${identity}`
}

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.length > 0 ? value : undefined

const positiveInteger = (value: unknown): number | undefined => {
  const number = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isInteger(number) && number > 0 ? number : undefined
}

const nonNegativeInteger = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : undefined

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const errorMessage = (error: unknown): string => {
  if (error instanceof Error) return error.message
  if (error !== null && typeof error === 'object' && 'message' in error) {
    return String((error as { message?: unknown }).message)
  }
  return String(error)
}

const isGithubReferenceAlreadyExistsError = (error: unknown): boolean =>
  /reference already exists/iu.test(errorMessage(error))
