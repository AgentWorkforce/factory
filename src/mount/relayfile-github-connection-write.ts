import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import { postAttestationGrant } from '../github/attestation-grant'
import { factoryGithubIssueCommentDraftName } from '../github/writeback-paths'
import { assertRoutedGithubWritebackPath } from '../github/writeback-routes'
import type {
  GithubConnectionIssueUpdateInput,
  GithubIssueLookup,
  GithubConnectionWrite,
  GithubPublishPullRequestInput,
  GithubPublishPullRequestResult,
  MountClient,
} from '../ports'

const execFileAsync = promisify(execFile)
const WRITE_CONFIRM_TIMEOUT_MS = 90_000
const RECEIPT_READ_ATTEMPTS = 5
const RECEIPT_READ_DELAY_MS = 100

export type GitCommandRunner = (args: string[]) => Promise<{ stdout: string; stderr?: string }>

export interface RelayfileGithubConnectionWriteConfig {
  mount: Pick<MountClient, 'confirmWrite' | 'getConfirmedWriteExternalId' | 'getConfirmedWriteFailureReason' | 'readFile' | 'writeFile'>
  gitRunner?: GitCommandRunner
  receiptReadAttempts?: number
  receiptReadDelayMs?: number
}

/**
 * GitHub mutations backed by the workspace's file-native Relayfile
 * connection. The adapter is server-side; Factory only depends on the stable
 * write paths and payload contracts exposed through MountClient.
 */
export class RelayfileGithubConnectionWrite implements GithubConnectionWrite {
  readonly #mount: RelayfileGithubConnectionWriteConfig['mount']
  readonly #git: GitCommandRunner
  readonly #receiptReadAttempts: number
  readonly #receiptReadDelayMs: number
  readonly #writesByPath = new Map<string, Promise<string | undefined>>()

  constructor(config: RelayfileGithubConnectionWriteConfig) {
    this.#mount = config.mount
    this.#git = config.gitRunner ?? defaultGitRunner
    this.#receiptReadAttempts = positiveInteger(config.receiptReadAttempts) ?? RECEIPT_READ_ATTEMPTS
    this.#receiptReadDelayMs = nonNegativeInteger(config.receiptReadDelayMs) ?? RECEIPT_READ_DELAY_MS
  }

  async getIssue(repo: string, number: number): Promise<GithubIssueLookup> {
    const { owner, repo: name } = githubRepoParts(repo)
    assertPositiveGithubNumber(number, 'issue')
    // Provider projections use the encoded owner__repo canonical tree, while
    // connected write paths use the nested owner/repo tree. Accept both so the
    // authenticated workspace connection remains authoritative across mount
    // layouts and migrations.
    const paths = [
      `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(name)}/issues/by-id/${number}.json`,
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}/issues/by-id/${number}.json`,
    ]
    for (const path of paths) {
      try {
        const { content } = await this.#mount.readFile(path)
        return {
          outcome: 'found',
          issue: { repo: `${owner}/${name}`, number, path, content },
        }
      } catch {
        // Try the alternate canonical layout. If neither is readable, absence
        // and transient sync failure are intentionally indistinguishable.
      }
    }
    return {
      outcome: 'indeterminate',
      reason: `connected GitHub projection did not expose ${owner}/${name}#${number}`,
    }
  }

  async publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const headRef = input.headRef ?? (input.clonePath
      ? await this.#gitValue(['-C', input.clonePath, 'symbolic-ref', '--short', 'HEAD'], 'current branch')
      : undefined)
    if (!headRef) {
      throw new Error('GitHub PR publication requires headRef or clonePath')
    }
    if (input.expectedHeadRef && headRef !== input.expectedHeadRef) {
      throw new Error(
        `Refusing to publish GitHub PR: expected head branch ${input.expectedHeadRef}, found ${headRef}`,
      )
    }
    if (headRef === input.baseRef) {
      throw new Error(`Refusing to publish GitHub PR with head equal to base branch: ${headRef}`)
    }
    const headSha = input.headSha ?? (input.clonePath && !input.headRef
      ? await this.#gitValue(['-C', input.clonePath, 'rev-parse', 'HEAD'], 'HEAD commit')
      : undefined)
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
    } else {
      // Nothing in this process can create the branch: no clone to push from,
      // no sha to point a ref at. A remote implementer is trusted to have
      // pushed it, and when that trust is misplaced GitHub answers a bare
      // `422 Validation Failed` on the PR create - true, but it reads like a
      // payload bug rather than the missing push it actually is (#430).
      // Confirm the ref exists first, so the failure names the real cause.
      await this.#assertHeadRefPushed(owner, repo, fullHeadRef, headRef)
    }

    // Best-effort: record a late attestation grant so the session reference
    // rides through to the attestation ledger. Silently omits when the relay
    // auth env vars are absent (operator key path, no workspace token). This
    // moved here from the retired local-`gh` publisher, which was the only
    // caller before publication became App-only; prefer the per-agent
    // sessionRef over the process-wide env var so concurrent implementers each
    // record their own session.
    const sessionRef = input.sessionRef ?? (process.env.RELAY_ATTEST_SESSION_ID || undefined)
    await postAttestationGrant(input.repo, sessionRef).catch(() => undefined)

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

  async postIssueComment(input: {
    repo: string
    number: number
    body: string
    author: 'app'
  }): Promise<void> {
    const repoRoot = githubRepoRoot(input.repo)
    assertPositiveGithubNumber(input.number, 'issue')
    const body = input.body.trim()
    if (!body) {
      throw new Error('GitHub issue comment body must be a non-empty string')
    }
    if (input.author !== 'app') {
      throw new Error(`Relayfile GitHub issue comments require author "app": ${input.author}`)
    }
    await this.#writeAndConfirm(
      `${repoRoot}/issues/${input.number}/comments/${factoryGithubIssueCommentDraftName(body)}`,
      // `author` selects this connection method, but is not a writable field
      // in Relayfile's GitHub issue-comment schema. The connected App
      // credential is applied server-side.
      { body },
    )
  }

  // `ensureRepositoryLabel` and `mutateIssueLabel` used to live here, authoring
  // `${repoRoot}/labels/<draft>.json` and
  // `${repoRoot}/issues/{n}/labels/<draft>.json`. Relayfile's GitHub adapter
  // routes no label resource at all, so both were refused with `Unsupported
  // GitHub writeback path` and no request ever reached GitHub (#431, #411).
  //
  // They are gone rather than rerouted because neither has a routed
  // equivalent at this layer. The only expression of a label change the
  // adapter accepts is a complete-label-set PATCH on the issue itself —
  // `updateIssue` below — and computing that set needs the issue's current
  // labels, which is `AppGithubWriteback.setStatus`'s job (#434). Repository
  // label provisioning is unnecessary on top of it: GitHub auto-creates an
  // unknown label named in that PATCH.

  async updateIssue(input: GithubConnectionIssueUpdateInput): Promise<void> {
    const repoRoot = githubRepoRoot(input.repo)
    assertPositiveGithubNumber(input.number, 'issue')
    if (input.author !== 'app') {
      throw new Error(`Relayfile GitHub issue updates require author "app": ${input.author}`)
    }
    const labels = input.labels === undefined ? undefined : normalizedGithubLabels(input.labels)
    if (labels === undefined && input.state === undefined) {
      throw new Error('GitHub issue update requires labels and/or state')
    }
    // As with comments, the App identity is enforced by the method contract;
    // the provider's issue schema accepts only its mutable REST fields.
    await this.#writeAndConfirm(
      `${repoRoot}/issues/${input.number}.json`,
      {
        ...(labels === undefined ? {} : { labels }),
        ...(input.state === undefined ? {} : { state: input.state }),
      },
    )
  }

  /**
   * Confirm `refs/heads/<headRef>` actually exists on GitHub before a PR is
   * opened against it (#430).
   *
   * This only runs on the path where no `headSha` was supplied, so nothing
   * above could have created the ref itself - a positive result here means an
   * implementer really did push, not merely that this process pushed for it.
   *
   * Only a CONFIRMED-absent read (`isMountFileNotFound`) is reported as the
   * branch never having been pushed. Every other read failure - a transport
   * blip, an auth hiccup, the projection not yet having caught up with a
   * branch that really was just pushed - is propagated unclassified instead
   * (#453 review, codex + cubic P1): the non-retryable classifier upstream
   * would otherwise abandon a genuinely publishable dispatch on its first bad
   * network moment, rather than letting the existing publish-retry budget
   * recover it. Both of `getIssue`'s canonical layouts (the encoded
   * `owner__repo` provider projection and the nested `owner/repo` writeback
   * tree) are probed for the same reason `getIssue` probes both: a workspace
   * that only exposes one of them must not read as "branch absent".
   */
  async #assertHeadRefPushed(owner: string, repo: string, fullHeadRef: string, headRef: string): Promise<void> {
    const paths = [
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/refs/${encodeURIComponent(fullHeadRef)}.json`,
      `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(repo)}/refs/${encodeURIComponent(fullHeadRef)}.json`,
    ]
    let lastError: unknown
    for (const path of paths) {
      try {
        await this.#mount.readFile(path)
        return
      } catch (error) {
        lastError = error
        if (!isMountFileNotFound(error)) throw error
        // Try the alternate canonical layout before concluding absence.
      }
    }
    throw new Error(
      `Refusing to publish GitHub PR: implementer branch ${headRef} was never pushed to ${owner}/${repo} ` +
      `(refs/heads/${headRef} does not exist: ${errorMessage(lastError)})`,
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
    // Fail on the authoring stack, not 90s later in the confirm. A path the
    // adapter does not route is a Factory bug, never a transient one, but the
    // remote rejection arrives as an ordinary confirmation failure that the
    // durable lifecycle retries indefinitely — which is how #431 stayed
    // invisible while every dispatch silently lost its lifecycle label.
    assertRoutedGithubWritebackPath(path)
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

const githubRepoRoot = (value: string): string => {
  const { owner, repo } = githubRepoParts(value)
  return `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
}

const assertPositiveGithubNumber = (value: number, resource: string): void => {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`GitHub ${resource} number must be a positive integer: ${value}`)
  }
}

const normalizedGithubLabels = (labels: string[]): string[] => {
  const normalized = labels.map((label) => label.trim())
  if (normalized.some((label) => !label)) {
    throw new Error('GitHub issue labels must be non-empty strings')
  }
  return [...new Set(normalized)]
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

/**
 * A CONFIRMED absent read, as opposed to any other read failure. Structured
 * fields first, matching `isMountFileNotFound` in `src/cli/fleet.ts` (kept
 * local because `mount/` is a lower layer than `cli/` and must not depend on
 * it; reuses this file's own `record` coercion rather than a second
 * near-identical helper - #453 review, cubic P3).
 *
 * The message fallback is deliberately narrower than a bare `\b404\b`
 * (#453 review, CodeRabbit P1): a transport error whose text merely CONTAINS
 * "404" - a URI segment like `feature-404`, an unrelated numeric id - would
 * false-positive as a confirmed-absent ref, and that false positive feeds
 * straight into `isNonRetryablePublishError` abandoning a genuinely
 * publishable dispatch. It is also deliberately broader than the single
 * phrase "file not found" (#453 review, cubic P2, on the same line the
 * CodeRabbit fix landed on): a ref/branch/resource-flavored "not found", or
 * the unambiguous compound "404 not found", must still count as confirmed
 * absence, or a REAL unpushed branch stops terminalizing and spends the full
 * retry budget instead.
 */
const isMountFileNotFound = (error: unknown): boolean => {
  const errorRecord = record(error)
  const response = record(errorRecord.response)
  const status = errorRecord.status ?? errorRecord.statusCode ?? response.status ?? response.statusCode
  const code = typeof errorRecord.code === 'string' ? errorRecord.code.toLowerCase() : undefined
  return status === 404 || status === '404' ||
    code === 'not_found' || code === 'file_not_found' ||
    /(?:file|ref(?:erence)?|branch|resource)\s+not\s+found|\b404\s+not\s+found\b/iu.test(errorMessage(error))
}
