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
