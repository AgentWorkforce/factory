/**
 * The CI half of AgentWorkforce/factory#432, part (c).
 *
 * Factory's merge gate protects Factory's own merges, but most merges are
 * performed by a human reading the PR page, so the "was this actually
 * reviewed?" signal has to exist there too. This module applies the same
 * predicate as `evaluateGithubMergeGate` — it imports it rather than
 * restating it, so the CI check and the gate cannot drift apart.
 *
 * It reads the check evidence from **both** endpoints on purpose:
 *
 * - `GET /commits/{sha}/status` — legacy commit statuses. Devin posts one of
 *   these and never a check run, so it is invisible to `/check-runs`. On the
 *   audited window it was the single largest source of vacuous green.
 * - `GET /commits/{sha}/check-runs` — check runs. cubic posts one of these,
 *   and states its reason in the nested `output.title`.
 *
 * A classifier reading either endpoint alone silently misses half the bots.
 */

import {
  checkSignalsFromRollup,
  reviewAtHeadRefusal,
  reviewsFromPayload,
  type CheckSignal,
  type ReviewAtHead,
} from './merge-gate'

/** Performs one authenticated GitHub REST GET and returns the parsed body. */
export type GithubRestFetch = (path: string) => Promise<unknown>

export interface ReviewAtHeadCheckInput {
  repo: string
  number: number
}

export interface ReviewAtHeadCheckResult {
  ok: boolean
  repo: string
  number: number
  head: string
  author?: string
  /** The refusal reason from the shared predicate, or `undefined` when it passes. */
  reason?: string
  reviewsAtHead: ReviewAtHead[]
  checkSignals: CheckSignal[]
  summary: string
}

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const asArray = (value: unknown): unknown[] => Array.isArray(value) ? value : []

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value : undefined

const splitRepo = (repo: string): { owner: string, name: string } => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0) {
    throw new Error(`Invalid GitHub repository identity ${repo}`)
  }
  return { owner, name }
}

/**
 * Both check sources, merged into the one rollup shape
 * `checkSignalsFromRollup` classifies. Legacy statuses keep their
 * `context`/`description`; check runs keep their `name`/`conclusion` and the
 * nested `output` the classifier reads the vacuous marker out of.
 */
export const rollupFromRestSources = (
  legacyStatuses: unknown,
  checkRuns: unknown,
): unknown[] => [
  ...asArray(legacyStatuses).map((entry) => {
    const status = asRecord(entry)
    return {
      context: status.context,
      state: status.state,
      description: status.description,
    }
  }),
  ...asArray(checkRuns).map((entry) => {
    const run = asRecord(entry)
    return {
      name: run.name,
      status: run.status,
      conclusion: run.conclusion,
      output: run.output,
    }
  }),
]

/**
 * `GET /pulls/{n}/reviews` carries no inline-comment count, so a review whose
 * substance is entirely inline reads as empty-bodied. The predicate in (a)
 * counts `inlineCommentsAtHead`, so the CI check has to supply it or it would
 * be strictly stricter than the merge gate it is meant to mirror. Only
 * comments anchored to `head` count — a comment left on an older commit is
 * exactly the stale review this check exists to reject.
 */
export const withInlineCommentsAtHead = (
  reviews: unknown[],
  comments: unknown[],
  head: string,
): unknown[] => {
  const perReview = new Map<number, number>()
  for (const entry of comments) {
    const comment = asRecord(entry)
    const reviewId = comment.pull_request_review_id
    if (typeof reviewId !== 'number') continue
    if (stringValue(comment.commit_id) !== head) continue
    perReview.set(reviewId, (perReview.get(reviewId) ?? 0) + 1)
  }

  return reviews.map((entry) => {
    const review = asRecord(entry)
    const id = review.id
    const count = typeof id === 'number' ? perReview.get(id) ?? 0 : 0
    return { ...review, inlineComments: count }
  })
}

const describeSignals = (signals: CheckSignal[]): string => {
  const count = (kind: CheckSignal['kind']): number =>
    signals.filter((signal) => signal.kind === kind).length
  const vacuous = signals.filter((signal) => signal.kind === 'VACUOUS')
  const tail = vacuous.length === 0
    ? ''
    : ` — vacuous: ${vacuous.map((signal) => `${signal.context} (${signal.description ?? 'no description'})`).join('; ')}`
  return `${signals.length} check(s): ${count('REAL')} real, ${count('VACUOUS')} vacuous, ${count('BLOCKING')} blocking${tail}`
}

const describeReviews = (reviews: ReviewAtHead[], head: string): string => {
  const atHead = reviews.filter((review) => review.commitId === head)
  if (atHead.length === 0) return 'no review is anchored to head'
  return atHead
    .map((review) => `${review.login} ${review.state} (body ${review.bodyLength}, inline ${review.inlineCommentsAtHead})`)
    .join('; ')
}

/**
 * Reads the PR, its reviews, and both check sources, then applies the shared
 * head-bound review predicate. `ok: false` means the PR would merge with no
 * substantive review of the commit that merges.
 */
export async function evaluateReviewAtHeadCheck(
  input: ReviewAtHeadCheckInput,
  fetch: GithubRestFetch,
): Promise<ReviewAtHeadCheckResult> {
  const { owner, name } = splitRepo(input.repo)
  const base = `repos/${owner}/${name}`

  const pull = asRecord(await fetch(`${base}/pulls/${input.number}`))
  const head = stringValue(asRecord(pull.head).sha)
  if (!head) {
    throw new Error(`GitHub returned no head SHA for ${input.repo}#${input.number}`)
  }
  const author = stringValue(asRecord(pull.user).login)

  const [reviewsRaw, comments, legacy, runs] = await Promise.all([
    fetch(`${base}/pulls/${input.number}/reviews?per_page=100`),
    fetch(`${base}/pulls/${input.number}/comments?per_page=100`),
    fetch(`${base}/commits/${head}/status`),
    fetch(`${base}/commits/${head}/check-runs?per_page=100`),
  ])

  const reviewsAtHead = reviewsFromPayload(
    withInlineCommentsAtHead(asArray(reviewsRaw), asArray(comments), head),
  )
  const checkSignals = checkSignalsFromRollup(
    rollupFromRestSources(asRecord(legacy).statuses, asRecord(runs).check_runs),
  )

  const reason = reviewAtHeadRefusal(reviewsAtHead, head, author)
  const summary = [
    `${input.repo}#${input.number} at ${head}`,
    `reviews at head: ${describeReviews(reviewsAtHead, head)}`,
    describeSignals(checkSignals),
    reason
      ? `REFUSED: ${reason}`
      : 'PASSED: a substantive review from a third party is anchored to head',
  ].join('\n')

  return {
    ok: reason === undefined,
    repo: input.repo,
    number: input.number,
    head,
    author,
    reason,
    reviewsAtHead,
    checkSignals,
    summary,
  }
}

/**
 * Authenticated REST reader built on `fetch`. `GITHUB_TOKEN` is the workflow
 * token; the check reads public metadata only and never needs write scope.
 */
export const githubRestFetch = (token: string, apiUrl = 'https://api.github.com'): GithubRestFetch =>
  async (path) => {
    const response = await globalThis.fetch(`${apiUrl.replace(/\/$/, '')}/${path}`, {
      headers: {
        accept: 'application/vnd.github+json',
        authorization: `Bearer ${token}`,
        'x-github-api-version': '2022-11-28',
      },
    })
    if (!response.ok) {
      throw new Error(`GitHub GET /${path} failed: ${response.status} ${response.statusText}`)
    }
    return response.json()
  }

/**
 * Workflow entry point. Exits non-zero when the PR has no substantive review
 * at head, so the check is red on the PR page rather than only inside Factory.
 */
export async function main(argv: string[]): Promise<number> {
  const [repo, rawNumber] = argv
  const number = Number(rawNumber)
  if (!repo || !Number.isSafeInteger(number) || number <= 0) {
    process.stderr.write('usage: review-at-head-check <owner/repo> <pr-number>\n')
    return 2
  }

  const token = process.env.GITHUB_TOKEN
  if (!token) {
    process.stderr.write('review-at-head-check requires GITHUB_TOKEN\n')
    return 2
  }

  const result = await evaluateReviewAtHeadCheck({ repo, number }, githubRestFetch(token, process.env.GITHUB_API_URL))
  process.stdout.write(`${result.summary}\n`)

  const stepSummary = process.env.GITHUB_STEP_SUMMARY
  if (stepSummary) {
    const { appendFile } = await import('node:fs/promises')
    await appendFile(stepSummary, `## Review at head\n\n\`\`\`\n${result.summary}\n\`\`\`\n`)
  }

  return result.ok ? 0 : 1
}
