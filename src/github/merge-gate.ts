import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { MountClient } from '../ports'
import { wrappedPayload } from '../writeback/shared'
import { localGhMutationAllowed, localGhMutationRefusal, type GithubWriteIdentity } from './gh-identity'

const execFileAsync = promisify(execFile)

export interface GhRunResult {
  stdout: string
  stderr?: string
}

export type GhRunner = (args: string[]) => Promise<GhRunResult>

export interface GithubMergeGateInput {
  repo: string
  number: number
  expectedHeadSha?: string
  /** Exact mounted PR metadata path returned by discovery. */
  path?: string
}

export interface GithubMergeInput {
  repo: string
  number: number
  expectedHeadSha: string
}

/**
 * One `statusCheckRollup` entry, retaining the fields a bare state string
 * throws away. `description` is what lets a vacuous bot check (expired
 * trial, rate limit, unassigned seat) be told apart from a real pass — both
 * report a non-blocking state.
 */
export interface CheckSignal {
  context: string
  state: string
  description?: string
  kind: 'REAL' | 'VACUOUS' | 'BLOCKING'
}

/** A PR review, anchored to the commit it was actually submitted against. */
export interface ReviewAtHead {
  login: string
  state: string
  commitId: string
  bodyLength: number
  inlineCommentsAtHead: number
}

export interface GithubMergeGateVerdict {
  verdict: 'READY' | 'REFUSE'
  ready: boolean
  reason: string
  live: {
    mergeable?: string
    mergeStateStatus?: string
    headRefOid?: string
    reviewDecision?: string
    checkSignals: CheckSignal[]
    /** @deprecated derived from `checkSignals`; retained for one release. */
    checkStates: string[]
  }
}

export interface GithubMergeResult {
  merged: boolean
  reason: string
  stdout?: string
  stderr?: string
}

export interface GithubMergeGate {
  check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict>
  merge(input: GithubMergeInput): Promise<GithubMergeResult>
}

export class GhCliGithubMergeGate implements GithubMergeGate {
  readonly #run: GhRunner
  readonly #identity: GithubWriteIdentity

  /**
   * @param identity the configured `github.identity`. `check` is a read and
   *   ignores it; `merge` mutates GitHub and refuses under exact `app` rather
   *   than squash-merging as the operator's own account. Defaults to `auto`
   *   so a directly-constructed gate keeps its historical behavior.
   */
  constructor(run: GhRunner = defaultGhRunner, identity: GithubWriteIdentity = 'auto') {
    this.#run = run
    this.#identity = identity
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    throw new Error(
      `GitHub merge-gate readiness for ${input.repo}#${input.number} requires mounted PR metadata; ` +
      'the local-gh adapter supports mutations only and Factory will not disguise a missing read capability as REFUSE',
    )
  }

  async merge(input: GithubMergeInput): Promise<GithubMergeResult> {
    // Fail closed before spawning `gh`. A guarded merge run through the local
    // CLI is recorded by GitHub as the operator merging, which is precisely
    // the split audit trail `github.identity: "app"` exists to remove. There
    // is no app-authored merge to fall through to, so refuse and say why.
    if (!localGhMutationAllowed(this.#identity)) {
      return {
        merged: false,
        reason: localGhMutationRefusal(
          `the guarded squash merge of ${input.repo}#${input.number}`,
          'mergePullRequest',
        ),
      }
    }

    try {
      const result = await this.#run([
        'pr',
        'merge',
        String(input.number),
        '--repo',
        input.repo,
        '--squash',
        '--delete-branch',
        '--match-head-commit',
        input.expectedHeadSha,
      ])
      return {
        merged: true,
        reason: `merged ${input.repo}#${input.number} at ${input.expectedHeadSha}`,
        stdout: result.stdout,
        stderr: result.stderr,
      }
    } catch (error) {
      return {
        merged: false,
        reason: `gh guarded merge failed: ${error instanceof Error ? error.message : String(error)}`,
      }
    }
  }
}

export const GithubMergeGate = GhCliGithubMergeGate

/**
 * Reads provider-authoritative merge readiness from the GitHub App projection
 * and delegates the guarded mutation separately. Discovery passes the exact PR
 * path, so this adds one read and never scans the pulls tree.
 */
export class MountedGithubMergeGate implements GithubMergeGate {
  readonly #mount: Pick<MountClient, 'readFile'>
  readonly #mutation: Pick<GithubMergeGate, 'merge'>

  constructor(
    mount: Pick<MountClient, 'readFile'>,
    mutation: Pick<GithubMergeGate, 'merge'> = new GhCliGithubMergeGate(),
  ) {
    this.#mount = mount
    this.#mutation = mutation
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    const path = input.path ?? mountedPullByIdPath(input.repo, input.number)
    let content: unknown
    try {
      content = (await this.#mount.readFile(path)).content
    } catch (error) {
      throw mountedMergeGateCapabilityError(
        input,
        `could not read ${path}: ${error instanceof Error ? error.message : String(error)}`,
      )
    }

    const live = mountedMergeGateFields(content, input)
    return evaluateGithubMergeGate(input, live)
  }

  async merge(input: GithubMergeInput): Promise<GithubMergeResult> {
    return this.#mutation.merge(input)
  }
}

export function evaluateGithubMergeGate(
  input: GithubMergeGateInput,
  live: unknown,
): GithubMergeGateVerdict {
  const record = asRecord(live)
  const mergeable = stringValue(record.mergeable)
  const mergeStateStatus = stringValue(record.mergeStateStatus)
  const headRefOid = stringValue(record.headRefOid)
  const reviewDecision = stringValue(record.reviewDecision)
  const statusCheckRollup = Array.isArray(record.statusCheckRollup) ? record.statusCheckRollup : undefined
  const checkSignals = statusCheckRollup ? checkSignalsFromRollup(statusCheckRollup) : []
  const checkStates = checkSignals.map((signal) => signal.state)
  const reviewsRaw = Array.isArray(record.reviews) ? record.reviews : undefined
  const reviewsAtHead = reviewsRaw ? reviewsFromPayload(reviewsRaw) : []
  const prAuthor = prAuthorLogin(record)

  const refuseWith = (reason: string): GithubMergeGateVerdict =>
    refuse(reason, { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkSignals, checkStates })

  if (!mergeable || !mergeStateStatus || !headRefOid || !reviewDecision || !statusCheckRollup || !reviewsRaw || !prAuthor) {
    return refuseWith('missing required live GitHub merge fields')
  }

  if (mergeable === 'UNKNOWN' || mergeStateStatus === 'UNKNOWN') {
    return refuseWith('GitHub mergeability is still unknown')
  }

  if (input.expectedHeadSha && headRefOid !== input.expectedHeadSha) {
    return refuseWith(`head moved: expected ${input.expectedHeadSha}, live ${headRefOid ?? 'unknown'}`)
  }

  if (mergeable !== 'MERGEABLE') {
    return refuseWith(`mergeable is ${mergeable ?? 'unknown'}`)
  }

  if (mergeStateStatus !== 'CLEAN') {
    return refuseWith(`merge state is ${mergeStateStatus ?? 'unknown'}`)
  }

  if (reviewDecision !== 'APPROVED') {
    return refuseWith(`review decision is ${reviewDecision ?? 'unknown'}`)
  }

  const reviewRefusal = reviewAtHeadRefusal(reviewsAtHead, headRefOid, prAuthor)
  if (reviewRefusal) {
    return refuseWith(reviewRefusal)
  }

  const nonVacuous = checkSignals.filter((signal) => signal.kind !== 'VACUOUS')
  if (nonVacuous.length === 0) {
    const vacuousContexts = checkSignals.map((signal) => signal.context)
    return refuseWith(
      checkSignals.length === 0
        ? 'no successful status checks observed'
        : `no successful status checks observed: ${checkSignals.length} check(s) present but all vacuous (${vacuousContexts.join(', ')})`,
    )
  }

  const blocking = nonVacuous.filter((signal) => signal.kind === 'BLOCKING')
  if (blocking.length > 0) {
    return refuseWith(`checks not merge-ready: ${blocking.map((signal) => signal.state).join(', ')}`)
  }

  const vacuousCount = checkSignals.length - nonVacuous.length
  const realCount = nonVacuous.filter((signal) => signal.kind === 'REAL').length

  return {
    verdict: 'READY',
    ready: true,
    reason: `MERGEABLE+CLEAN with APPROVED review substantiated at head, matching head when supplied, and no blocking checks ` +
      `(${realCount} real, ${vacuousCount} vacuous)`,
    live: { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkSignals, checkStates },
  }
}

export const defaultGhRunner: GhRunner = async (args) => {
  // Mutation-only compatibility runner. Merge-gate reads now come from the
  // mounted App projection. Retire this when `GithubConnectionWrite` exposes
  // a server-side `mergePullRequest` capability so Factory still holds no
  // GitHub credential. Until then `merge` refuses under
  // `github.identity: "app"` rather than merging as the operator (see
  // ./gh-identity). Tracked on AgentWorkforce/factory#221.
  const { stdout, stderr } = await execFileAsync('gh', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

const mountedPullByIdPath = (repo: string, number: number): string => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0 || !Number.isSafeInteger(number) || number <= 0) {
    throw new Error(`Invalid GitHub pull request identity ${repo}#${number}`)
  }
  return `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(name)}/pulls/by-id/${number}.json`
}

const mountedMergeGateFields = (
  content: unknown,
  input: GithubMergeGateInput,
): Record<string, unknown> => {
  let payload: Record<string, unknown>
  try {
    payload = wrappedPayload(content)
  } catch (error) {
    throw mountedMergeGateCapabilityError(
      input,
      `could not parse mounted PR metadata: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
  const explicitNumber = numberValue(payload.number)
  if (explicitNumber !== undefined && explicitNumber !== input.number) {
    throw mountedMergeGateCapabilityError(
      input,
      `mounted PR record number is ${explicitNumber}, expected ${input.number}`,
    )
  }

  const head = asRecord(payload.head)
  const mergeable = normalizeMergeable(payload.mergeable)
  const mergeStateStatus = normalizedString(
    payload.mergeStateStatus ?? payload.merge_state_status ?? payload.mergeable_state,
  )
  const headRefOid = stringValue(payload.headRefOid) ?? stringValue(head.sha)
  const reviewDecision = normalizedString(payload.reviewDecision ?? payload.review_decision)
  const statusCheckRollup = payload.statusCheckRollup ?? payload.status_check_rollup
  const reviews = payload.reviews
  const author = prAuthorLogin(payload)
  const missing = [
    ['mergeable', mergeable],
    ['mergeStateStatus', mergeStateStatus],
    ['headRefOid', headRefOid],
    ['reviewDecision', reviewDecision],
    ['statusCheckRollup', Array.isArray(statusCheckRollup) ? statusCheckRollup : undefined],
    ['reviews', Array.isArray(reviews) ? reviews : undefined],
    ['author', author],
  ].flatMap(([name, value]) => value === undefined ? [name] : [])
  if (missing.length > 0) {
    throw mountedMergeGateCapabilityError(
      input,
      `mounted PR metadata is missing ${missing.join(', ')}`,
    )
  }

  return { mergeable, mergeStateStatus, headRefOid, reviewDecision, statusCheckRollup, reviews, author }
}

const mountedMergeGateCapabilityError = (input: GithubMergeGateInput, detail: string): Error =>
  new Error(
    `GitHub merge-gate capability unavailable for ${input.repo}#${input.number}: ${detail}; ` +
    'Factory requires the authenticated mounted PR projection and does not fall back to local gh',
  )

const normalizeMergeable = (value: unknown): string | undefined => {
  if (typeof value === 'boolean') return value ? 'MERGEABLE' : 'CONFLICTING'
  return normalizedString(value)
}

const normalizedString = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value.trim().toUpperCase() : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined

const refuse = (reason: string, live: GithubMergeGateVerdict['live']): GithubMergeGateVerdict => ({
  verdict: 'REFUSE',
  ready: false,
  reason,
  live,
})

const nonBlockingCheckStates = new Set(['SUCCESS', 'NEUTRAL', 'SKIPPED', 'EXPECTED'])

/**
 * Description-string markers a vacuous bot check reports instead of an actual
 * review: an expired trial, a rate limit, an unassigned seat, a line-count
 * cap. Each one currently lands on a non-blocking rollup state, so without
 * this classifier it is indistinguishable from a real pass. See
 * AgentWorkforce/factory#432.
 */
const VACUOUS_REVIEW_MARKERS: RegExp[] = [
  /full review skipped/i, // Devin: trial expired and no credits remaining
  /review rate limited/i, // CodeRabbit
  /review skipped/i, // CodeRabbit: OSS / draft
  /ai review skipped/i, // cubic: seat unassigned
  /review line limit reached/i, // cubic: monthly cap
  /review cancelled/i, // cubic
  /review not started/i, // cubic: branch rewrite
  /usage limits?/i, // codex
]

/**
 * Every string on a rollup entry that can carry the bot's own account of what
 * it did. A legacy commit status (`/commits/{sha}/status`) says it in
 * `description`; a check run (`/commits/{sha}/check-runs`) says it in the
 * nested `output.title` / `output.summary` — cubic reports
 * `AI review skipped: seat author not assigned` as `output.title`, which a
 * top-level-only read never sees, so the check classifies as a real pass.
 */
const checkDescriptions = (record: Record<string, unknown>): string[] => {
  const output = asRecord(record.output)
  return [
    record.description,
    record.summary,
    record.title,
    record.text,
    output.title,
    output.summary,
    output.text,
  ].flatMap((value) => {
    const text = stringValue(value)?.trim()
    return text ? [text] : []
  })
}

/**
 * GitHub reports the same state in two casings: GraphQL's `statusCheckRollup`
 * uppercases it, REST's `/commits/{sha}/status` and `/commits/{sha}/check-runs`
 * return `success` / `failure` lowercase. Comparing an unnormalized state
 * against an uppercase set made every REST-shaped passing check read as
 * BLOCKING, so the gate refused real greens while reporting
 * `checks not merge-ready: success, success, ...`.
 */
const normalizeCheckState = (state: string): string => state.trim().toUpperCase()

const isVacuousDescription = (text: string): boolean =>
  VACUOUS_REVIEW_MARKERS.some((marker) => marker.test(text))

const classifyCheckKind = (state: string, descriptions: string[]): CheckSignal['kind'] => {
  if (descriptions.some(isVacuousDescription)) {
    return 'VACUOUS'
  }
  return nonBlockingCheckStates.has(normalizeCheckState(state)) ? 'REAL' : 'BLOCKING'
}

export const checkSignalsFromRollup = (value: unknown): CheckSignal[] => {
  if (!Array.isArray(value)) {
    return []
  }

  return value.map((entry) => {
    const record = asRecord(entry)
    const context = stringValue(record.context) ?? stringValue(record.name) ?? 'unknown'
    const raw = stringValue(record.conclusion) ?? stringValue(record.state) ?? stringValue(record.status) ?? 'UNKNOWN'
    const descriptions = checkDescriptions(record)
    return {
      context,
      state: normalizeCheckState(raw),
      // Report the string that made the call. A check run whose `output.title`
      // is a generic "AI review" while the reason sits in `output.summary`
      // would otherwise be surfaced by its title, hiding why it was ruled
      // vacuous in the very message meant to explain the refusal.
      description: descriptions.find(isVacuousDescription) ?? descriptions[0],
      kind: classifyCheckKind(raw, descriptions),
    }
  })
}

const prAuthorLogin = (record: Record<string, unknown>): string | undefined =>
  stringValue(record.author) ??
  stringValue(asRecord(record.author).login) ??
  stringValue(asRecord(record.user).login)

const reviewAuthorLogin = (record: Record<string, unknown>): string | undefined =>
  stringValue(asRecord(record.author).login) ?? stringValue(asRecord(record.user).login) ?? stringValue(record.login)

const reviewCommitId = (record: Record<string, unknown>): string | undefined =>
  stringValue(record.commitId) ??
  stringValue(record.commit_id) ??
  stringValue(asRecord(record.commit).oid) ??
  stringValue(asRecord(record.commit).sha)

const finiteNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

const reviewInlineComments = (record: Record<string, unknown>): number => {
  const direct = finiteNumber(record.inlineComments) ?? finiteNumber(record.inline_comments)
  if (direct !== undefined) return direct
  if (typeof record.comments === 'number') return record.comments
  return finiteNumber(asRecord(record.comments).totalCount) ?? 0
}

/** Parses raw `reviews` entries (GraphQL- or REST-shaped) into `ReviewAtHead`s, dropping any without an identifiable author, commit, or state. */
export const reviewsFromPayload = (value: unknown[]): ReviewAtHead[] =>
  value.flatMap((entry) => {
    const record = asRecord(entry)
    const login = reviewAuthorLogin(record)
    const commitId = reviewCommitId(record)
    const state = stringValue(record.state)
    if (!login || !commitId || !state) return []
    return [{
      login,
      state: state.toUpperCase(),
      commitId,
      bodyLength: (stringValue(record.body) ?? '').length,
      inlineCommentsAtHead: reviewInlineComments(record),
    }]
  })

/**
 * Returns a refusal reason when no review at `head` is substantive, or
 * `undefined` when at least one qualifies. `reviewDecision === 'APPROVED'` is
 * a repo-level rollup unbound to the head commit or to content (relay#1638:
 * a stale-commit approval with an empty body satisfies it); this predicate
 * requires a review actually anchored to `head`, from someone other than the
 * PR author, carrying a body or inline comments.
 */
export const reviewAtHeadRefusal = (
  reviews: ReviewAtHead[],
  head: string,
  author: string | undefined,
): string | undefined => {
  const atHead = reviews.filter((review) => review.commitId === head)
  if (atHead.length === 0) {
    return reviews.length === 0
      ? `no review at head: no reviews exist (head is ${head})`
      : `no review at head: ${reviews.length} review(s) exist, newest pinned to ${reviews[reviews.length - 1]!.commitId} (head is ${head})`
  }

  const fromOthers = atHead.filter((review) => review.login !== author)
  if (fromOthers.length === 0) {
    return 'only review at head is by the PR author'
  }

  const substantive = fromOthers.filter((review) => review.bodyLength > 0 || review.inlineCommentsAtHead > 0)
  if (substantive.length === 0) {
    const vacuous = fromOthers[0]!
    return `review at head has no content: ${vacuous.login} ${vacuous.state} with empty body and no inline comments`
  }

  return undefined
}

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined
