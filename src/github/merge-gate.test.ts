import { describe, expect, it } from 'vitest'

import type { MountClient } from '../ports'
import { GhCliGithubMergeGate, MountedGithubMergeGate, evaluateGithubMergeGate, type GhRunner } from './merge-gate'

const input = {
  repo: 'AgentWorkforce/pear',
  number: 123,
  expectedHeadSha: 'abc123',
}

const live = (overrides: Record<string, unknown> = {}) => ({
  mergeable: 'MERGEABLE',
  mergeStateStatus: 'CLEAN',
  headRefOid: 'abc123',
  reviewDecision: 'APPROVED',
  statusCheckRollup: [
    { name: 'test', conclusion: 'SUCCESS' },
  ],
  author: 'pr-author',
  reviews: [
    { login: 'reviewer', state: 'APPROVED', commitId: 'abc123', body: 'Looks correct, ship it.' },
  ],
  ...overrides,
})

const mountedPath = '/github/repos/AgentWorkforce/pear/pulls/123/metadata.json'
const defaultMountedPath = '/github/repos/AgentWorkforce__pear/pulls/by-id/123.json'

const mountedPull = (overrides: Record<string, unknown> = {}) => ({
  provider: 'github',
  objectType: 'pull_request',
  objectId: '123',
  payload: {
    number: 123,
    ...live(),
    ...overrides,
  },
})

const pullMount = (content: unknown): Pick<MountClient, 'readFile'> => ({
  readFile: async (path) => {
    if (path !== mountedPath) throw new Error(`unexpected mounted PR path ${path}`)
    return { content }
  },
})

const defaultPathMount = (content: unknown): Pick<MountClient, 'readFile'> => ({
  readFile: async (path) => {
    if (path !== defaultMountedPath) throw new Error(`unexpected mounted PR path ${path}`)
    return { content }
  },
})

describe('GithubMergeGate', () => {
  it('returns READY from the exact mounted PR record without invoking local gh', async () => {
    let ghInvoked = false
    const gate = new MountedGithubMergeGate(
      pullMount(mountedPull()),
      new GhCliGithubMergeGate(async () => {
        ghInvoked = true
        throw new Error('local gh must not be used for merge-gate reads')
      }),
    )

    await expect(gate.check({ ...input, path: mountedPath })).resolves.toMatchObject({
      verdict: 'READY',
      ready: true,
    })
    expect(ghInvoked).toBe(false)
  })

  it('derives the canonical by-id mounted path when the caller omits an exact path', async () => {
    const gate = new MountedGithubMergeGate(defaultPathMount(mountedPull()))

    await expect(gate.check(input)).resolves.toMatchObject({ verdict: 'READY', ready: true })
  })

  it('reports malformed mounted JSON as a merge-gate capability error', async () => {
    const gate = new MountedGithubMergeGate(pullMount('{not-json'))

    await expect(gate.check({ ...input, path: mountedPath })).rejects.toThrow(
      /merge-gate capability unavailable.*could not parse mounted PR metadata.*does not fall back to local gh/i,
    )
  })

  it('returns READY for MERGEABLE+CLEAN with neutral, skipped, or expected advisory checks', () => {
    expect(evaluateGithubMergeGate(input, live({
      statusCheckRollup: [
        { name: 'required', conclusion: 'SUCCESS' },
        { name: 'advisory-neutral', conclusion: 'NEUTRAL' },
        { name: 'advisory-skipped', conclusion: 'SKIPPED' },
        { name: 'expected-but-nonblocking', conclusion: 'EXPECTED' },
      ],
    }))).toMatchObject({
      verdict: 'READY',
      ready: true,
    })
  })

  it('refuses when the live head differs from the expected head sha', () => {
    expect(evaluateGithubMergeGate(input, live({ headRefOid: 'different-sha' }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
      reason: expect.stringMatching(/head moved/),
    })
  })

  it('captures the current ready head when no expected head sha is supplied', () => {
    expect(evaluateGithubMergeGate({
      repo: 'AgentWorkforce/pear',
      number: 123,
    }, live({
      headRefOid: 'ready-sha',
      reviews: [
        { login: 'reviewer', state: 'APPROVED', commitId: 'ready-sha', body: 'Looks correct, ship it.' },
      ],
    }))).toMatchObject({
      verdict: 'READY',
      ready: true,
      live: { headRefOid: 'ready-sha' },
    })
  })

  it('refuses stale mount-clean snapshots when live GitHub contradicts readiness', () => {
    const staleMountSnapshot = {
      mergeable: 'MERGEABLE',
      mergeStateStatus: 'CLEAN',
      headRefOid: 'abc123',
      statusCheckRollup: [{ conclusion: 'SUCCESS' }],
    }
    void staleMountSnapshot

    expect(evaluateGithubMergeGate(input, live({
      mergeable: 'CONFLICTING',
      mergeStateStatus: 'UNSTABLE',
      headRefOid: 'def456',
      statusCheckRollup: [{ conclusion: 'FAILURE' }],
    }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
  })

  it('fails closed when mounted GitHub reports UNKNOWN or partial gate metadata', async () => {
    const unknown = new MountedGithubMergeGate(pullMount(mountedPull({
      mergeable: 'UNKNOWN',
      mergeStateStatus: 'UNKNOWN',
    })))
    await expect(unknown.check({ ...input, path: mountedPath })).resolves.toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })

    const partial = new MountedGithubMergeGate(pullMount(mountedPull({
      reviewDecision: undefined,
    })))
    await expect(partial.check({ ...input, path: mountedPath })).rejects.toThrow(
      /capability unavailable.*mounted PR metadata.*reviewDecision.*does not fall back to local gh/i,
    )

    const noReviews = new MountedGithubMergeGate(pullMount(mountedPull({
      reviews: undefined,
    })))
    await expect(noReviews.check({ ...input, path: mountedPath })).rejects.toThrow(
      /capability unavailable.*mounted PR metadata.*\breviews\b.*does not fall back to local gh/i,
    )

    const noAuthor = new MountedGithubMergeGate(pullMount(mountedPull({
      author: undefined,
    })))
    await expect(noAuthor.check({ ...input, path: mountedPath })).rejects.toThrow(
      /capability unavailable.*mounted PR metadata.*author.*does not fall back to local gh/i,
    )
  })

  it('fails loudly when only the local-gh merge adapter is asked to read readiness', async () => {
    let ghInvoked = false
    const gate = new GhCliGithubMergeGate(async () => {
      ghInvoked = true
      return { stdout: JSON.stringify(live()) }
    })

    await expect(gate.check(input)).rejects.toThrow(/requires mounted PR metadata/i)
    expect(ghInvoked).toBe(false)
  })

  it('refuses missing, blocking, pending, or unknown status checks', () => {
    expect(evaluateGithubMergeGate(input, live({ statusCheckRollup: [] }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
    expect(evaluateGithubMergeGate(input, live({ statusCheckRollup: [{ conclusion: 'FAILURE' }] }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
    expect(evaluateGithubMergeGate(input, live({ statusCheckRollup: [{ status: 'IN_PROGRESS' }] }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
    expect(evaluateGithubMergeGate(input, live({ statusCheckRollup: [{ conclusion: 'UNKNOWN' }] }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
    expect(evaluateGithubMergeGate(input, live({ statusCheckRollup: [{ status: 'COMPLETED' }] }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
  })

  it('refuses until the review decision is approved', () => {
    expect(evaluateGithubMergeGate(input, live({ reviewDecision: 'REVIEW_REQUIRED' }))).toMatchObject({
      verdict: 'REFUSE',
      ready: false,
      reason: expect.stringMatching(/review decision/),
    })
  })

  describe('review-at-head predicate (factory#432)', () => {
    it('refuses relay#1638\'s shape: a repo-level APPROVED with every review either stale or empty at head', () => {
      // Reproduction from factory#432: 14 reviews, several substantive but pinned to
      // long-gone commits, and exactly one review at head — an empty-bodied approval.
      // `reviewDecision` still reports APPROVED because it is a repo-level rollup
      // unbound to content or to headRefOid.
      const verdict = evaluateGithubMergeGate(input, live({
        headRefOid: 'abc123',
        reviewDecision: 'APPROVED',
        reviews: [
          { login: 'coderabbitai[bot]', state: 'COMMENTED', commitId: 'stale-1', body: 'x'.repeat(2575) },
          { login: 'cubic-dev-ai[bot]', state: 'COMMENTED', commitId: 'stale-2', body: 'x'.repeat(579) },
          { login: 'reviewer', state: 'COMMENTED', commitId: 'stale-3', body: '' },
          { login: 'reviewer', state: 'APPROVED', commitId: 'abc123', body: '' },
        ],
      }))

      expect(verdict).toMatchObject({
        verdict: 'REFUSE',
        ready: false,
        reason: expect.stringMatching(/review at head has no content: reviewer APPROVED/),
      })
    })

    it('refuses when no review is anchored to the head commit at all', () => {
      expect(evaluateGithubMergeGate(input, live({
        headRefOid: 'abc123',
        reviews: [
          { login: 'reviewer', state: 'APPROVED', commitId: 'old-sha', body: 'looks good' },
        ],
      }))).toMatchObject({
        verdict: 'REFUSE',
        ready: false,
        reason: expect.stringMatching(/no review at head: 1 review\(s\) exist, newest pinned to old-sha \(head is abc123\)/),
      })
    })

    it('refuses when reviews exist but zero of them anchor to head', () => {
      expect(evaluateGithubMergeGate(input, live({ reviews: [] }))).toMatchObject({
        verdict: 'REFUSE',
        ready: false,
        reason: expect.stringMatching(/no review at head: no reviews exist/),
      })
    })

    it('refuses a self-approval: the only review at head is by the PR author', () => {
      expect(evaluateGithubMergeGate(input, live({
        author: 'same-person',
        reviews: [
          { login: 'same-person', state: 'APPROVED', commitId: 'abc123', body: 'looks good to me' },
        ],
      }))).toMatchObject({
        verdict: 'REFUSE',
        ready: false,
        reason: expect.stringMatching(/only review at head is by the PR author/),
      })
    })

    it('accepts a review at head with no body but inline comments as substantive', () => {
      expect(evaluateGithubMergeGate(input, live({
        reviews: [
          { login: 'reviewer', state: 'COMMENTED', commitId: 'abc123', body: '', comments: 3 },
        ],
      }))).toMatchObject({ verdict: 'READY', ready: true })
    })

    it('is READY when a substantive review anchors to head from a third party', () => {
      expect(evaluateGithubMergeGate(input, live())).toMatchObject({
        verdict: 'READY',
        ready: true,
      })
    })
  })

  describe('vacuous check classification (factory#432)', () => {
    it('classifies a Devin trial-expired status as VACUOUS, not REAL, and refuses when it is the only check', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { context: 'Devin', state: 'success', description: 'Full review skipped: trial expired and no credits remaining' },
        ],
      }))

      expect(verdict).toMatchObject({ verdict: 'REFUSE', ready: false })
      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ context: 'Devin', kind: 'VACUOUS' }),
      ])
      expect(verdict.reason).toMatch(/no successful status checks observed/)
    })

    it('classifies CodeRabbit rate-limited and OSS-skip, and cubic seat-unassigned and line-limit, as VACUOUS', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { context: 'coderabbitai', conclusion: 'SUCCESS', description: 'Review rate limited' },
          { context: 'coderabbitai', conclusion: 'SUCCESS', description: 'Review skipped: manual review required for this OSS repository' },
          { context: 'cubic', conclusion: 'SKIPPED', description: 'AI review skipped: seat author not assigned' },
          { context: 'cubic', conclusion: 'NEUTRAL', description: 'AI review line limit reached' },
        ],
      }))

      expect(verdict.live.checkSignals.every((signal) => signal.kind === 'VACUOUS')).toBe(true)
      expect(verdict).toMatchObject({ verdict: 'REFUSE', ready: false })
    })

    it('does not classify a genuine success as VACUOUS', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [{ context: 'ci/test', conclusion: 'SUCCESS', description: 'All tests passed' }],
      }))

      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ context: 'ci/test', kind: 'REAL' }),
      ])
      expect(verdict).toMatchObject({ verdict: 'READY', ready: true })
    })

    it('is READY when a real check accompanies a vacuous one, and reports both counts', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { context: 'ci/test', conclusion: 'SUCCESS' },
          { context: 'Devin', state: 'success', description: 'Full review skipped: trial expired and no credits remaining' },
        ],
      }))

      expect(verdict).toMatchObject({ verdict: 'READY', ready: true })
      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ context: 'ci/test', kind: 'REAL' }),
        expect.objectContaining({ context: 'Devin', kind: 'VACUOUS' }),
      ])
      expect(verdict.reason).toMatch(/1 real, 1 vacuous/)
    })
  })

  // Both of these are shapes GitHub really returns, captured from
  // AgentWorkforce/software-garden. `/commits/{sha}/status` (Devin,
  // CodeRabbit) reports a lowercase state and a top-level `description`;
  // `/commits/{sha}/check-runs` (cubic) reports a lowercase conclusion and
  // puts its account of itself in the nested `output.title`. The classifier
  // has to read both, and the two defects below have to be fixed together:
  // fixing the casing alone promotes an unread nested description straight
  // from BLOCKING to REAL, which is a vacuous green.
  describe('rollup shapes GitHub actually returns (factory#432)', () => {
    it('classifies a cubic check run as VACUOUS when its account is in the nested output.title', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          {
            name: 'cubic · AI code reviewer',
            status: 'completed',
            conclusion: 'success',
            output: { title: 'AI review skipped: seat author not assigned', summary: '' },
          },
        ],
      }))

      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ context: 'cubic · AI code reviewer', kind: 'VACUOUS' }),
      ])
      expect(verdict).toMatchObject({ verdict: 'REFUSE', ready: false })
      expect(verdict.reason).toMatch(/all vacuous/)
    })

    it('reads a vacuous marker out of output.summary as well as output.title', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          {
            name: 'cubic · AI code reviewer',
            conclusion: 'success',
            output: { title: 'AI review', summary: 'AI review line limit reached for this month' },
          },
        ],
      }))

      // The reported description has to be the string that made the call. If
      // the generic `output.title` were reported instead, the refusal message
      // would say "AI review" and hide the reason the check was ruled vacuous.
      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({
          kind: 'VACUOUS',
          description: 'AI review line limit reached for this month',
        }),
      ])
      expect(verdict.reason).toMatch(/all vacuous/)
    })

    it('reports the plain description when nothing is vacuous', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { name: 'package', conclusion: 'success', output: { title: 'Build passed', summary: 'all green' } },
        ],
      }))

      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ kind: 'REAL', description: 'Build passed' }),
      ])
    })

    it('treats a lowercase REST success as a real pass rather than a blocking check', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [{ context: 'ci/build', state: 'success', description: 'Build passed' }],
      }))

      expect(verdict.live.checkSignals).toEqual([
        expect.objectContaining({ context: 'ci/build', state: 'SUCCESS', kind: 'REAL' }),
      ])
      expect(verdict).toMatchObject({ verdict: 'READY', ready: true })
    })

    it('still blocks on a lowercase REST failure', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { context: 'ci/build', state: 'success', description: 'Build passed' },
          { name: 'package', status: 'completed', conclusion: 'failure' },
        ],
      }))

      expect(verdict).toMatchObject({ verdict: 'REFUSE', ready: false })
      expect(verdict.reason).toMatch(/checks not merge-ready: FAILURE/)
    })

    it('classifies a real mixed rollup of legacy statuses and check runs the way GitHub returns it', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          // GET /commits/{sha}/status — legacy commit statuses. Devin is only
          // ever visible here, never in /check-runs.
          { context: 'Devin Review', state: 'success', description: 'Full review skipped: trial expired and no credits remaining' },
          { context: 'CodeRabbit', state: 'success', description: 'Review rate limited' },
          // GET /commits/{sha}/check-runs
          { name: 'package', status: 'completed', conclusion: 'success', output: { title: null, summary: '' } },
          { name: 'cubic · AI code reviewer', status: 'completed', conclusion: 'success', output: { title: 'AI review completed', summary: 'AI review completed with 1 review.' } },
        ],
      }))

      expect(verdict.live.checkSignals.map((signal) => `${signal.context}=${signal.kind}`)).toEqual([
        'Devin Review=VACUOUS',
        'CodeRabbit=VACUOUS',
        'package=REAL',
        'cubic · AI code reviewer=REAL',
      ])
      expect(verdict).toMatchObject({ verdict: 'READY', ready: true })
      expect(verdict.reason).toMatch(/2 real, 2 vacuous/)
    })

    it('refuses when every legacy status is vacuous and no check run accompanies them', () => {
      const verdict = evaluateGithubMergeGate(input, live({
        statusCheckRollup: [
          { context: 'Devin Review', state: 'success', description: 'Full review skipped: trial expired and no credits remaining' },
          { context: 'CodeRabbit', state: 'success', description: 'Review rate limited' },
        ],
      }))

      expect(verdict).toMatchObject({ verdict: 'REFUSE', ready: false })
      expect(verdict.reason).toMatch(/all vacuous \(Devin Review, CodeRabbit\)/)
    })
  })

  it('merges through gh with squash, delete-branch, and match-head-commit', async () => {
    const calls: string[][] = []
    const gate = new GhCliGithubMergeGate(async (args) => {
      calls.push(args)
      return { stdout: 'merged' }
    })

    await expect(gate.merge(input)).resolves.toMatchObject({
      merged: true,
    })

    expect(calls).toEqual([[
      'pr',
      'merge',
      '123',
      '--repo',
      'AgentWorkforce/pear',
      '--squash',
      '--delete-branch',
      '--match-head-commit',
      'abc123',
    ]])
  })

  it('reports guarded merge failure without claiming success', async () => {
    const gate = new GhCliGithubMergeGate(async () => {
      throw new Error('Head commit changed')
    })

    await expect(gate.merge(input)).resolves.toMatchObject({
      merged: false,
      reason: expect.stringMatching(/Head commit changed/),
    })
  })
})
