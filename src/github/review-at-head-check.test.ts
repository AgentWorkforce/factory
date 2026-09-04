import { describe, expect, it } from 'vitest'

import { evaluateReviewAtHeadCheck, rollupFromRestSources, type GithubRestFetch } from './review-at-head-check'

const HEAD = '82305815d0593ec1061ef7394c7d432156d6f720'
const STALE = '0fb25f08163eb11363f47dc052fbce1e8aa11111'

/**
 * Verbatim from AgentWorkforce/software-garden, head 82305815:
 * `GET /commits/{sha}/status`. Devin and CodeRabbit appear only here — they
 * post legacy commit statuses and are absent from `/check-runs` entirely.
 */
const legacyStatuses = {
  statuses: [
    {
      context: 'Devin Review',
      state: 'success',
      description: 'Full review skipped: trial expired and no credits remaining',
    },
    { context: 'CodeRabbit', state: 'success', description: 'Review completed' },
  ],
}

/** Verbatim from the same head: `GET /commits/{sha}/check-runs`. */
const checkRuns = {
  check_runs: [
    {
      name: 'cubic · AI code reviewer',
      status: 'completed',
      conclusion: 'success',
      output: { title: 'AI review completed', summary: 'AI review completed with 1 review. 0 issues found' },
    },
    { name: 'package', status: 'completed', conclusion: 'success', output: { title: null, summary: '' } },
  ],
}

const pull = (overrides: Record<string, unknown> = {}) => ({
  number: 462,
  head: { sha: HEAD },
  user: { login: 'pr-author' },
  ...overrides,
})

/**
 * Exact-match routing. Prefix matching would let `pulls/462` answer
 * `pulls/462/comments`, so a route this check really depends on could go
 * missing while every test still passed.
 */
const fetcher = (routes: Record<string, unknown>): GithubRestFetch => async (path) => {
  if (!(path in routes)) throw new Error(`unexpected GET /${path}`)
  return routes[path]
}

const PULL = 'repos/AgentWorkforce/software-garden/pulls/462'
const REVIEWS = `${PULL}/reviews?per_page=100`
const COMMENTS = `${PULL}/comments?per_page=100`
const STATUS = `repos/AgentWorkforce/software-garden/commits/${HEAD}/status?per_page=100`
const CHECK_RUNS = `repos/AgentWorkforce/software-garden/commits/${HEAD}/check-runs?per_page=100`

// `collectPages` appends `&page=N`, so a single-page fixture answers page 1.
const routes = (overrides: Record<string, unknown> = {}) => ({
  [PULL]: pull(),
  [`${REVIEWS}&page=1`]: [],
  [`${COMMENTS}&page=1`]: [],
  [`${STATUS}&page=1`]: legacyStatuses,
  [`${CHECK_RUNS}&page=1`]: checkRuns,
  ...overrides,
})

const input = { repo: 'AgentWorkforce/software-garden', number: 462 }

describe('review-at-head CI check (factory#432 part c)', () => {
  it('fails a PR with no review anchored to head', async () => {
    const result = await evaluateReviewAtHeadCheck(input, fetcher(routes()))

    expect(result.ok).toBe(false)
    expect(result.reason).toMatch(/no review at head: no reviews exist/)
    expect(result.summary).toMatch(/REFUSED/)
  })

  it('fails a PR whose only review at head is empty-bodied — the relay#1638 shape', async () => {
    const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
      [`${REVIEWS}&page=1`]: [
        { user: { login: 'coderabbitai[bot]' }, state: 'COMMENTED', commit_id: STALE, body: 'x'.repeat(2575) },
        { user: { login: 'reviewer' }, state: 'APPROVED', commit_id: HEAD, body: '' },
      ],
    })))

    expect(result.ok).toBe(false)
    expect(result.reason).toMatch(/review at head has no content: reviewer APPROVED/)
  })

  it('fails when the only review at head is the PR author', async () => {
    const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
      [`${REVIEWS}&page=1`]: [
        { user: { login: 'pr-author' }, state: 'APPROVED', commit_id: HEAD, body: 'self approve' },
      ],
    })))

    expect(result.ok).toBe(false)
    expect(result.reason).toMatch(/only review at head is by the PR author/)
  })

  it('passes when a third party leaves a substantive review at head', async () => {
    const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
      [`${REVIEWS}&page=1`]: [
        { user: { login: 'cubic-dev-ai[bot]' }, state: 'COMMENTED', commit_id: HEAD, body: 'x'.repeat(513) },
      ],
    })))

    expect(result.ok).toBe(true)
    expect(result.reason).toBeUndefined()
    expect(result.summary).toMatch(/PASSED/)
  })

  describe('Devin posts a legacy commit status, not a check run', () => {
    it('sees the Devin trial-expired status, which /check-runs alone never returns', async () => {
      const result = await evaluateReviewAtHeadCheck(input, fetcher(routes()))

      expect(result.checkSignals).toEqual(expect.arrayContaining([
        expect.objectContaining({ context: 'Devin Review', kind: 'VACUOUS' }),
      ]))
      expect(result.summary).toMatch(/vacuous: Devin Review \(Full review skipped: trial expired/)
    })

    it('reports nothing vacuous when the legacy status source is dropped, proving the endpoint carries it', async () => {
      // The same head read through /check-runs only — which is what a
      // check-runs-only classifier sees. Devin has vanished.
      const signals = rollupFromRestSources({ statuses: [] }, checkRuns.check_runs)

      expect(signals.map((entry) => (entry as { name?: string }).name)).toEqual([
        'cubic · AI code reviewer',
        'package',
      ])
      expect(JSON.stringify(signals)).not.toMatch(/Devin/)
    })

    it('merges both sources into one rollup, preserving each shape', () => {
      const merged = rollupFromRestSources(legacyStatuses.statuses, checkRuns.check_runs)

      expect(merged).toEqual([
        { context: 'Devin Review', state: 'success', description: 'Full review skipped: trial expired and no credits remaining' },
        { context: 'CodeRabbit', state: 'success', description: 'Review completed' },
        expect.objectContaining({ name: 'cubic · AI code reviewer', conclusion: 'success' }),
        expect.objectContaining({ name: 'package', conclusion: 'success' }),
      ])
    })
  })

  describe('inline comments at head', () => {
    const inlineOnlyReview = [
      { id: 9001, user: { login: 'reviewer' }, state: 'COMMENTED', commit_id: HEAD, body: '' },
    ]

    it('passes an empty-bodied review whose substance is inline at head', async () => {
      const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
        [`${REVIEWS}&page=1`]: inlineOnlyReview,
        [`${COMMENTS}&page=1`]: [
          { pull_request_review_id: 9001, commit_id: HEAD, body: 'this branch is never taken' },
          { pull_request_review_id: 9001, commit_id: HEAD, body: 'off-by-one here' },
        ],
      })))

      expect(result.ok).toBe(true)
      expect(result.reviewsAtHead).toEqual([
        expect.objectContaining({ login: 'reviewer', inlineCommentsAtHead: 2 }),
      ])
    })

    it('does not count inline comments left on an older commit', async () => {
      const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
        [`${REVIEWS}&page=1`]: inlineOnlyReview,
        [`${COMMENTS}&page=1`]: [
          { pull_request_review_id: 9001, commit_id: STALE, body: 'reviewed two pushes ago' },
        ],
      })))

      expect(result.ok).toBe(false)
      expect(result.reason).toMatch(/review at head has no content/)
      expect(result.reviewsAtHead).toEqual([
        expect.objectContaining({ inlineCommentsAtHead: 0 }),
      ])
    })

    it('attributes inline comments to the review that carried them', async () => {
      const result = await evaluateReviewAtHeadCheck(input, fetcher(routes({
        [`${REVIEWS}&page=1`]: [
          ...inlineOnlyReview,
          { id: 9002, user: { login: 'other' }, state: 'COMMENTED', commit_id: HEAD, body: '' },
        ],
        [`${COMMENTS}&page=1`]: [
          { pull_request_review_id: 9002, commit_id: HEAD, body: 'only this review has substance' },
        ],
      })))

      expect(result.reviewsAtHead).toEqual([
        expect.objectContaining({ login: 'reviewer', inlineCommentsAtHead: 0 }),
        expect.objectContaining({ login: 'other', inlineCommentsAtHead: 1 }),
      ])
      expect(result.ok).toBe(true)
    })
  })

  it('counts real and vacuous checks separately in the summary', async () => {
    const result = await evaluateReviewAtHeadCheck(input, fetcher(routes()))

    expect(result.summary).toMatch(/4 check\(s\): 3 real, 1 vacuous, 0 blocking/)
  })

  describe('pagination', () => {
    it('finds a review at head that falls on the second page', async () => {
      // 100 stale reviews fill page 1 exactly, so a single `per_page=100`
      // request would see none of the substantive review behind them and
      // refuse a PR that was in fact reviewed.
      const stale = Array.from({ length: 100 }, (_, index) => ({
        id: index + 1,
        user: { login: 'coderabbitai[bot]' },
        state: 'COMMENTED',
        commit_id: STALE,
        body: 'stale',
      }))
      const atHead = { id: 999, user: { login: 'reviewer' }, state: 'APPROVED', commit_id: HEAD, body: 'ship it' }

      const result = await evaluateReviewAtHeadCheck(input, async (path) => {
        if (path.endsWith(`${REVIEWS}&page=1`)) return stale
        if (path.endsWith(`${REVIEWS}&page=2`)) return [atHead]
        if (path.endsWith(`${COMMENTS}&page=1`)) return []
        if (path.startsWith(PULL)) return pull()
        if (path.includes('/status')) return legacyStatuses
        if (path.includes('/check-runs')) return checkRuns
        throw new Error(`unexpected GET /${path}`)
      })

      expect(result.ok).toBe(true)
      expect(result.reviewsAtHead).toHaveLength(101)
    })

    it('collects vacuous legacy statuses across pages rather than truncating at one', async () => {
      const filler = Array.from({ length: 100 }, (_, index) => ({
        context: `ci/shard-${index}`,
        state: 'success',
        description: 'passed',
      }))

      const result = await evaluateReviewAtHeadCheck(input, async (path) => {
        if (path.startsWith(PULL) && !path.includes('/reviews') && !path.includes('/comments')) return pull()
        if (path.startsWith(REVIEWS)) return []
        if (path.startsWith(COMMENTS)) return []
        if (path.includes('/status') && path.endsWith('page=1')) return { statuses: filler }
        if (path.includes('/status') && path.endsWith('page=2')) return { statuses: legacyStatuses.statuses }
        if (path.includes('/check-runs')) return { check_runs: [] }
        throw new Error(`unexpected GET /${path}`)
      })

      expect(result.checkSignals).toHaveLength(102)
      expect(result.summary).toMatch(/vacuous: Devin Review \(Full review skipped/)
    })

    it('stops paging on a short page instead of requesting forever', async () => {
      const seen: string[] = []
      await evaluateReviewAtHeadCheck(input, async (path) => {
        seen.push(path)
        if (path.startsWith(PULL) && !path.includes('/reviews') && !path.includes('/comments')) return pull()
        if (path.startsWith(REVIEWS)) return []
        if (path.startsWith(COMMENTS)) return []
        if (path.includes('/status')) return legacyStatuses
        if (path.includes('/check-runs')) return checkRuns
        throw new Error(`unexpected GET /${path}`)
      })

      expect(seen.filter((path) => path.includes('page=2'))).toEqual([])
    })
  })

  it('fails closed when GitHub returns no PR author, rather than counting a self-review', async () => {
    // Without an author every `review.login !== author` comparison is true, so
    // the PR author's own review would satisfy the third-party requirement.
    await expect(evaluateReviewAtHeadCheck(input, fetcher(routes({
      [PULL]: { number: 462, head: { sha: HEAD }, user: null },
      [`${REVIEWS}&page=1`]: [
        { id: 1, user: { login: 'pr-author' }, state: 'APPROVED', commit_id: HEAD, body: 'self approve' },
      ],
    })))).rejects.toThrow(/no author for/)
  })

  it('refuses to guess when GitHub returns no head SHA', async () => {
    await expect(evaluateReviewAtHeadCheck(input, fetcher(routes({
      [PULL]: { number: 462, head: {} },
    })))).rejects.toThrow(/no head SHA/)
  })

  it('rejects a malformed repository identity rather than building a wrong path', async () => {
    await expect(evaluateReviewAtHeadCheck({ repo: 'software-garden', number: 1 }, fetcher(routes())))
      .rejects.toThrow(/Invalid GitHub repository identity/)
  })
})
