import { describe, expect, it } from 'vitest'

import { GhCliGithubMergeGate, evaluateGithubMergeGate, type GhRunner } from './merge-gate'

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
  ...overrides,
})

describe('GithubMergeGate', () => {
  it('returns READY only for MERGEABLE+CLEAN, matching head, and no blocking checks', async () => {
    const gate = new GhCliGithubMergeGate(async () => ({ stdout: JSON.stringify(live()) }))

    await expect(gate.check(input)).resolves.toMatchObject({
      verdict: 'READY',
      ready: true,
    })
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
    }, live({ headRefOid: 'ready-sha' }))).toMatchObject({
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

  it('fails closed when gh returns UNKNOWN, errors, or partial output', async () => {
    const unknown = new GhCliGithubMergeGate(async () => ({
      stdout: JSON.stringify(live({ mergeable: 'UNKNOWN', mergeStateStatus: 'UNKNOWN' })),
    }))
    await expect(unknown.check(input)).resolves.toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })

    const errorRunner: GhRunner = async () => {
      throw new Error('gh timed out')
    }
    await expect(new GhCliGithubMergeGate(errorRunner).check(input)).resolves.toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })

    const partial = new GhCliGithubMergeGate(async () => ({
      stdout: JSON.stringify({ mergeable: 'MERGEABLE', mergeStateStatus: 'CLEAN' }),
    }))
    await expect(partial.check(input)).resolves.toMatchObject({
      verdict: 'REFUSE',
      ready: false,
    })
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
