import { describe, expect, it } from 'vitest'

import { closeProbePr } from './probe-closer'
import type { GithubConnectionWrite, MountClient } from '../ports'

const openProbe = {
  state: 'OPEN',
  headRefName: 'factory-e2e/ar-42-probe',
  title: '[factory-e2e] AR-42 probe',
  body: 'Closes AR-42',
}

const githubWrite = (closes: Array<{ repo: string; number: number }> = []): GithubConnectionWrite => ({
  publishPullRequest: async () => { throw new Error('unexpected publish') },
  closePullRequest: async (input) => { closes.push(input) },
})

const prPath = '/github/repos/AgentWorkforce__pear/pulls/by-id/123.json'

const prMount = (
  content: unknown,
  reads: string[] = [],
): Pick<MountClient, 'readFile'> => ({
  readFile: async (path) => {
    reads.push(path)
    if (path !== prPath) throw new Error(`unexpected mounted PR path ${path}`)
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
      path: prPath,
      mount: prMount({ payload: openProbe }, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 123, state: 'CLOSED' })

    expect(reads).toEqual([prPath])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 123 }])
  })

  it('refuses a non-probe PR before closing', async () => {
    const reads: string[] = []
    const mount = prMount({ payload: {
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
      path: prPath,
      mount,
    })).rejects.toThrow(/missing \[factory-e2e\] probe marker/)
    expect(reads).toEqual([prPath])
  })

  it('requires the factory-e2e marker as a title prefix, not only body or branch text', async () => {
    const reads: string[] = []
    const mount = prMount({ payload: {
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
      path: prPath,
      mount,
    })).rejects.toThrow(/missing \[factory-e2e\] probe marker/)
    expect(reads).toEqual([prPath])
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
      path: prPath,
      mount: prMount(markerlessProbe, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(reads).toEqual([prPath])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 279 }])
  })

  it('treats an already-closed probe PR as idempotent success', async () => {
    const reads: string[] = []
    const mount = prMount({ payload: {
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
      path: prPath,
      mount,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(reads).toEqual([prPath])
  })

  it('refuses a probe that is not tied to the expected issue key before closing', async () => {
    const reads: string[] = []
    const mount = prMount({ payload: {
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
      path: prPath,
      mount,
    })).rejects.toThrow(/missing issue key AR-42/)
    expect(reads).toEqual([prPath])
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
      path: prPath,
      mount: prMount(openProbe, reads),
    })).rejects.toThrow(/workspace close failed/)
    expect(reads).toEqual([prPath])
  })

  it('does not require an unauthenticated read-back after the App close is confirmed', async () => {
    const reads: string[] = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 127,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      path: prPath,
      mount: prMount(openProbe, reads),
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 127, state: 'CLOSED' })
    expect(reads).toEqual([prPath])
  })

  it('reports a clear connection error without reading the mount when GitHub writes are unavailable', async () => {
    const reads: string[] = []

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 129,
      expectedIssueKey: 'AR-42',
      path: prPath,
      mount: prMount(openProbe, reads),
    })).rejects.toThrow('GitHub write path not available on this mount — connect GitHub to your workspace')
    expect(reads).toEqual([])
  })

  it('fails loudly when the mounted PR read capability is unavailable', async () => {
    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 130,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      path: prPath,
    })).rejects.toThrow(/mounted GitHub PR read path is unavailable/i)
  })
})
