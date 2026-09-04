import { describe, expect, it } from 'vitest'

import { closeProbePr } from './probe-closer'
import type { GithubConnectionWrite, MountClient } from '../ports'

const openProbe = {
  state: 'OPEN',
  headRefName: 'factory-e2e/ar-42-probe',
  title: '[garden-e2e] AR-42 probe',
  body: 'Closes AR-42',
}

const githubWrite = (closes: Array<{ repo: string; number: number }> = []): GithubConnectionWrite => ({
  publishPullRequest: async () => { throw new Error('unexpected publish') },
  closePullRequest: async (input) => { closes.push(input) },
})

const prPath = (number: number): string =>
  `/github/repos/AgentWorkforce__pear/pulls/by-id/${number}.json`

const prMount = (
  prNumber: number,
  content: unknown,
  reads: string[] = [],
): Pick<MountClient, 'readFile'> => ({
  readFile: async (path) => {
    reads.push(path)
    if (path !== prPath(prNumber)) throw new Error(`unexpected mounted PR path ${path}`)
    return { content }
  },
})

describe('closeProbePr', () => {
  it('guards from the mounted PR and closes through the confirmed App write path', async () => {
    const reads: string[] = []
    const closes: Array<{ repo: string; number: number }> = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 123,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(closes),
      mount: prMount(123, { payload: { number: 123, ...openProbe } }, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 123, state: 'CLOSED' })

    expect(reads).toEqual([prPath(123)])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 123 }])
  })

  it('still closes an in-flight soak PR titled with the legacy factory-e2e marker', async () => {
    // Rename transition: probe PRs created before the rename carry the
    // legacy title marker and must remain recoverable.
    const reads: string[] = []
    const closes: Array<{ repo: string; number: number }> = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 125,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(closes),
      mount: prMount(125, { payload: { number: 125, ...openProbe, title: '[factory-e2e] AR-42 probe' } }, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 125, state: 'CLOSED' })

    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 125 }])
  })

  it('refuses a mounted record for a different PR before closing', async () => {
    const reads: string[] = []
    const closes: Array<{ repo: string; number: number }> = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 131,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(closes),
      mount: prMount(131, { payload: { number: 999, ...openProbe } }, reads),
    })).rejects.toThrow(/mounted record.*identifies PR #999/i)

    expect(reads).toEqual([prPath(131)])
    expect(closes).toEqual([])
  })

  it('refuses a non-probe PR before closing', async () => {
    const reads: string[] = []
    const mount = prMount(124, { payload: {
      number: 124,
      state: 'OPEN',
      headRefName: 'feature/real-fix',
      title: 'Fix a real production issue',
      body: 'No synthetic marker here; mentions AR-42 only as context.',
    } }, reads)

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 124,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      mount,
    })).rejects.toThrow(/missing \[garden-e2e\] \(or legacy \[factory-e2e\]\) probe marker/)
    expect(reads).toEqual([prPath(124)])
  })

  it('requires the e2e marker as a title prefix, not only body or branch text', async () => {
    const reads: string[] = []
    const mount = prMount(128, { payload: {
      number: 128,
      state: 'OPEN',
      headRefName: 'factory-e2e/ar-42-probe',
      title: 'AR-42 probe without title marker',
      body: '[factory-e2e] Closes AR-42',
    } }, reads)

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 128,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      mount,
    })).rejects.toThrow(/missing \[garden-e2e\] \(or legacy \[factory-e2e\]\) probe marker/)
    expect(reads).toEqual([prPath(128)])
  })

  it('allows issue-gated callers to close markerless branch-convention PRs', async () => {
    const reads: string[] = []
    const closes: Array<{ repo: string; number: number }> = []
    const markerlessProbe = {
      state: 'OPEN',
      headRefName: 'ar-229-is-positive',
      title: 'Add isPositive util',
      body: '',
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 279,
      expectedIssueKey: 'AR-229',
      requireTitleMarker: false,
      githubWrite: githubWrite(closes),
      mount: prMount(279, { number: 279, ...markerlessProbe }, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(reads).toEqual([prPath(279)])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 279 }])
  })

  it('treats an already-closed probe PR as idempotent success', async () => {
    const reads: string[] = []
    const mount = prMount(279, { payload: {
      number: 279,
      state: 'CLOSED',
      headRefName: 'ar-229-is-positive',
      title: 'Add isPositive util',
      body: '',
    } }, reads)

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 279,
      expectedIssueKey: 'AR-229',
      requireTitleMarker: false,
      githubWrite: githubWrite(),
      mount,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(reads).toEqual([prPath(279)])
  })

  it('refuses a probe that is not tied to the expected issue key before closing', async () => {
    const reads: string[] = []
    const mount = prMount(125, { payload: {
      number: 125,
      ...openProbe,
      body: 'Closes AR-99',
      headRefName: 'factory-e2e/ar-99-probe',
      title: '[factory-e2e] AR-99 probe',
    } }, reads)

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 125,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      mount,
    })).rejects.toThrow(/missing issue key AR-42/)
    expect(reads).toEqual([prPath(125)])
  })

  it('fails closed when workspace close errors and does not claim success', async () => {
    const reads: string[] = []
    const write = githubWrite()
    write.closePullRequest = async () => { throw new Error('workspace close failed') }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 126,
      expectedIssueKey: 'AR-42',
      githubWrite: write,
      mount: prMount(126, { number: 126, ...openProbe }, reads),
    })).rejects.toThrow(/workspace close failed/)
    expect(reads).toEqual([prPath(126)])
  })

  it('does not require an unauthenticated read-back after the App close is confirmed', async () => {
    const reads: string[] = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 127,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      mount: prMount(127, { number: 127, ...openProbe }, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 127, state: 'CLOSED' })
    expect(reads).toEqual([prPath(127)])
  })

  it('reports a clear connection error without reading the mount when GitHub writes are unavailable', async () => {
    const reads: string[] = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 129,
      expectedIssueKey: 'AR-42',
      path: prPath(129),
      mount: prMount(129, { number: 129, ...openProbe }, reads),
    })).rejects.toThrow('GitHub write path not available on this mount — connect GitHub to your workspace')
    expect(reads).toEqual([])
  })

  it('fails loudly when the mounted PR read capability is unavailable', async () => {
    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 130,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      path: prPath(130),
    })).rejects.toThrow(/mounted GitHub PR read path is unavailable/i)
  })
})
