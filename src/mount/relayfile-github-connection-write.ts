import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type {
  GithubConnectionWrite,
  GithubPublishPullRequestInput,
  GithubPublishPullRequestResult,
  MountClient,
} from '../ports'
import {
  FACTORY_CODERABBIT_REVIEW_BODY,
  containsCoderabbitReviewRequest,
  isAllowedFactoryGithubWritebackDraft,
} from '../github/review-request'

const REVIEW_REQUEST_CONFIRM_TIMEOUT_MS = 10_000
const REVIEW_REQUEST_QUERY_PAGE_LIMIT = 100

const execFileAsync = promisify(execFile)
const WRITE_CONFIRM_TIMEOUT_MS = 90_000
const RECEIPT_READ_ATTEMPTS = 5
const RECEIPT_READ_DELAY_MS = 100

export type GitCommandRunner = (args: string[]) => Promise<{ stdout: string; stderr?: string }>

export interface RelayfileGithubConnectionWriteConfig {
  mount: Pick<MountClient, 'confirmWrite' | 'deleteFile' | 'getConfirmedWriteExternalId' | 'getConfirmedWriteFailureReason' | 'listTree' | 'readFile' | 'writeFile'> & {
    createFile: NonNullable<MountClient['createFile']>
    queryFiles(opts: {
      path: string
      provider?: string
      cursor?: string
      limit?: number
    }): Promise<{ paths: string[]; nextCursor: string | null }>
  }
  gitRunner?: GitCommandRunner
  receiptReadAttempts?: number
  receiptReadDelayMs?: number
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
  readonly #writesByPath = new Map<string, Promise<string | undefined>>()
  readonly #reviewRequests = new Map<string, Promise<void>>()

  constructor(config: RelayfileGithubConnectionWriteConfig) {
    this.#mount = config.mount
    this.#git = config.gitRunner ?? defaultGitRunner
    this.#receiptReadAttempts = positiveInteger(config.receiptReadAttempts) ?? RECEIPT_READ_ATTEMPTS
    this.#receiptReadDelayMs = nonNegativeInteger(config.receiptReadDelayMs) ?? RECEIPT_READ_DELAY_MS
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

  async requestPullRequestReview(input: { repo: string; number: number }): Promise<void> {
    const { owner, repo } = githubRepoParts(input.repo)
    if (!Number.isInteger(input.number) || input.number <= 0) {
      throw new Error(`GitHub pull request number must be a positive integer: ${input.number}`)
    }
    const repoRoots = [
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`,
      `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(repo)}`,
    ]
    // The process-local promise handles re-entry here; the Relayfile
    // create-if-absent below arbitrates independent Factory processes. Direct
    // GitHub comments remain outside that store boundary and can still race.
    const requestKey = `${input.repo.toLowerCase()}#${input.number}`
    const existing = this.#reviewRequests.get(requestKey)
    if (existing) return existing
    const request = this.#requestPullRequestReview(input.repo, input.number, repoRoots)
      .catch((error: unknown) => {
        this.#reviewRequests.delete(requestKey)
        throw error
      })
    this.#reviewRequests.set(requestKey, request)
    return request
  }

  async #requestPullRequestReview(expectedRepo: string, number: number, repoRoots: string[]): Promise<void> {
    for (const repoRoot of repoRoots) {
      if (await this.#hasPullRequestReviewRequest(expectedRepo, number, repoRoot)) return
    }
    const commentsRoot = `${repoRoots[0]}/pulls/${number}/comments`
    const path = `${commentsRoot}/factory-coderabbit-review.json`
    const content = { body: FACTORY_CODERABBIT_REVIEW_BODY }
    const result = await this.#mount.createFile(path, content, { guarded: true })
    if (result === 'exists') {
      const existing = (await this.#mount.readFile(path)).content
      if (!isAllowedFactoryGithubWritebackDraft(path, existing)) {
        throw new Error(`GitHub review request create conflicted with unexpected content at ${path}`)
      }
    }
    const status = await this.#mount.confirmWrite(path, {
      timeoutMs: WRITE_CONFIRM_TIMEOUT_MS,
      returnFailed: true,
    })
    if (status !== 'acked') {
      const failureReason = status === 'failed'
        ? await this.#mount.getConfirmedWriteFailureReason?.(path)
        : undefined
      throw new Error(`GitHub writeback did not complete for ${path}: ${failureReason ?? status}`)
    }
  }

  async #hasPullRequestReviewRequest(expectedRepo: string, number: number, repoRoot: string): Promise<boolean> {
    const pullsRoot = `${repoRoot}/pulls`
    const pullDirectoryPattern = new RegExp(
      `^${escapeRegExp(pullsRoot)}/(${number}(?:__[^/]+)?)(?:/|$)`,
      'u',
    )
    const commentRoots = new Set([`${pullsRoot}/${number}/comments`])
    for (const path of await this.#listTreeOrEmpty(`${pullsRoot}/${number}`)) {
      const pullDirectory = pullDirectoryPattern.exec(path)?.[1]
      if (pullDirectory) commentRoots.add(`${pullsRoot}/${pullDirectory}/comments`)
    }
    for (const commentsRoot of commentRoots) {
      const directCommentPattern = new RegExp(`^${escapeRegExp(commentsRoot)}/[^/]+\\.json$`, 'u')
      const nestedCommentPattern = new RegExp(
        `^${escapeRegExp(commentsRoot)}/[^/]+/(?:meta|metadata)\\.json$`,
        'u',
      )
      const directCommentDirectoryPattern = new RegExp(`^${escapeRegExp(commentsRoot)}/[^/.]+$`, 'u')
      const commentPaths = new Set<string>()
      for (const path of await this.#listTreeOrEmpty(commentsRoot)) {
        if (directCommentPattern.test(path) || nestedCommentPattern.test(path)) {
          commentPaths.add(path)
          continue
        }
        if (!directCommentDirectoryPattern.test(path)) continue
        for (const nestedPath of await this.#listTreeOrEmpty(path)) {
          if (nestedCommentPattern.test(nestedPath)) commentPaths.add(nestedPath)
        }
      }
      for (const path of commentPaths) {
        // A listed comment becoming unreadable is indeterminate, not evidence
        // that the request is absent. Propagate so the caller retries the scan.
        const content = record((await this.#mount.readFile(path)).content)
        // A failed provider operation can leave our bare local draft in the
        // tree. Resolve its provider status rather than treating the draft
        // itself as evidence that GitHub received the request.
        if (isAllowedFactoryGithubWritebackDraft(path, content)) {
          const status = await this.#mount.confirmWrite(path, {
            timeoutMs: REVIEW_REQUEST_CONFIRM_TIMEOUT_MS,
            returnFailed: true,
          })
          if (status === 'acked') return true
          if (status === 'failed') {
            await this.#mount.deleteFile(path)
            continue
          }
          throw new Error(`GitHub review request draft has indeterminate provider status for ${path}: ${status}`)
        }
        const payload = record(content.payload)
        const comment = record(payload.comment)
        const rootComment = record(content.comment)
        const body = stringValue(content.body) ?? stringValue(payload.body) ?? stringValue(comment.body)
          ?? stringValue(rootComment.body)
        if (body && containsCoderabbitReviewRequest(body)) return true
      }
    }
    const canonicalCommentsRoot = `${repoRoot}/comments`
    const canonicalDirectPattern = new RegExp(
      `^${escapeRegExp(canonicalCommentsRoot)}/[^/]+\\.json$`,
      'u',
    )
    const canonicalNestedPattern = new RegExp(
      `^${escapeRegExp(canonicalCommentsRoot)}/[^/]+/(?:meta|metadata)\\.json$`,
      'u',
    )
    let cursor: string | undefined
    const visitedCursors = new Set<string>()
    do {
      const page = await this.#mount.queryFiles({
        path: canonicalCommentsRoot,
        provider: 'github',
        cursor,
        limit: REVIEW_REQUEST_QUERY_PAGE_LIMIT,
      })
      for (const path of page.paths) {
        if (!canonicalDirectPattern.test(path) && !canonicalNestedPattern.test(path)) continue
        let content: Record<string, unknown>
        try {
          content = record((await this.#mount.readFile(path)).content)
        } catch (error) {
          // A current-tree query can race provider reconciliation. A path that
          // was deleted after the query is stale evidence, so continue; other
          // read failures remain indeterminate and must be retried by the caller.
          if (isMountPathNotFound(error)) continue
          throw error
        }
        if (!canonicalGithubCommentMatches(content, expectedRepo, number)) continue
        const body = githubCommentBody(content)
        if (body && containsCoderabbitReviewRequest(body)) return true
      }
      cursor = page.nextCursor ?? undefined
      if (cursor && visitedCursors.has(cursor)) {
        throw new Error(`Relayfile file query repeated cursor while scanning ${canonicalCommentsRoot}: ${cursor}`)
      }
      if (cursor) visitedCursors.add(cursor)
    } while (cursor)
    return false
  }

  async #listTreeOrEmpty(prefix: string): Promise<string[]> {
    try {
      return await this.#mount.listTree(prefix)
    } catch (error) {
      if (isMountPathNotFound(error)) return []
      throw error
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

const githubDraftName = (headRef: string, headSha?: string): string => {
  const branch = headRef.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'branch'
  const identity = headSha?.slice(0, 12) ?? 'pushed'
  return `factory-${branch.slice(0, 80)}-${identity}`
}

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const escapeRegExp = (value: string): string =>
  value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')

const isMountPathNotFound = (error: unknown): boolean => {
  const details = record(error)
  const response = record(details.response)
  const status = details.status ?? details.statusCode ?? response.status ?? response.statusCode
  const code = stringValue(details.code)?.toLowerCase()
  return status === 404 || status === '404' || code === 'not_found' || code === 'file_not_found'
}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.length > 0 ? value : undefined

const githubCommentBody = (content: Record<string, unknown>): string | undefined => {
  const payload = record(content.payload)
  const comment = record(payload.comment)
  const rootComment = record(content.comment)
  return stringValue(content.body) ?? stringValue(payload.body) ?? stringValue(comment.body)
    ?? stringValue(rootComment.body)
}

const canonicalGithubCommentMatches = (
  content: Record<string, unknown>,
  expectedRepo: string,
  expectedNumber: number,
): boolean => {
  const payload = record(content.payload)
  const comment = record(payload.comment)
  const repository = record(
    Object.keys(record(payload.repository)).length > 0
      ? payload.repository
      : content.repository,
  )
  const pullRequest = record(
    Object.keys(record(payload.pull_request)).length > 0
      ? payload.pull_request
      : content.pull_request,
  )
  const identities = new Set<string>()
  const numbers = new Set<number>()
  const fullName = stringValue(repository.full_name)
  if (fullName) identities.add(fullName.toLowerCase())
  const owner = stringValue(payload.owner) ?? stringValue(content.owner)
  const repo = stringValue(payload.repo) ?? stringValue(content.repo)
  if (owner || repo) {
    if (!owner || !repo) return false
    identities.add(`${owner}/${repo}`.toLowerCase())
  }
  const directNumber = positiveInteger(pullRequest.number)
  if (directNumber) numbers.add(directNumber)
  for (const value of [
    content.pull_request_url,
    content.html_url,
    payload.pull_request_url,
    payload.html_url,
    comment.pull_request_url,
    comment.html_url,
  ]) {
    const parsed = githubPullRequestFromUrl(stringValue(value))
    if (!parsed) continue
    identities.add(parsed.repo.toLowerCase())
    numbers.add(parsed.number)
  }
  return identities.size === 1 &&
    identities.has(expectedRepo.toLowerCase()) &&
    numbers.size === 1 &&
    numbers.has(expectedNumber)
}

const githubPullRequestFromUrl = (value: string | undefined): { repo: string; number: number } | undefined => {
  if (!value) return undefined
  const match = value.match(
    /^https:\/\/(?:api\.)?github\.com\/(?:repos\/)?([^/]+)\/([^/]+)\/pulls?\/(\d+)(?:[#/?].*)?$/iu,
  )
  const number = positiveInteger(match?.[3])
  return match?.[1] && match[2] && number
    ? { repo: `${match[1]}/${match[2]}`, number }
    : undefined
}

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
