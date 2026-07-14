import { describe, expect, it } from 'vitest'

import { closeProbePr } from './probe-closer'
import type { GhRunner } from './merge-gate'
import type { GithubConnectionWrite } from '../ports'

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

describe('closeProbePr', () => {
  it('guards, closes, and confirms CLOSED via read-back', async () => {
    const calls: string[][] = []
    const closes: Array<{ repo: string; number: number }> = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(calls.length === 1 ? openProbe : { ...openProbe, state: 'CLOSED' }) }
      }
      throw new Error(`unexpected gh args ${args.join(' ')}`)
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 123,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(closes),
      runner,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 123, state: 'CLOSED' })

    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '123'],
      ['pr', 'view', '123'],
    ])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 123 }])
  })

  it('refuses a non-probe PR before closing', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      return {
        stdout: JSON.stringify({
          state: 'OPEN',
          headRefName: 'feature/real-fix',
          title: 'Fix a real production issue',
          body: 'No synthetic marker here; mentions AR-42 only as context.',
        }),
      }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 124,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      runner,
    })).rejects.toThrow(/missing \[factory-e2e\] probe marker/)
    expect(calls).toHaveLength(1)
    expect(calls[0]?.slice(0, 3)).toEqual(['pr', 'view', '124'])
  })

  it('requires the factory-e2e marker as a title prefix, not only body or branch text', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      return {
        stdout: JSON.stringify({
          state: 'OPEN',
          headRefName: 'factory-e2e/ar-42-probe',
          title: 'AR-42 probe without title marker',
          body: '[factory-e2e] Closes AR-42',
        }),
      }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 128,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      runner,
    })).rejects.toThrow(/missing \[factory-e2e\] probe marker/)
    expect(calls).toHaveLength(1)
  })

  it('allows issue-gated callers to close markerless branch-convention PRs', async () => {
    const calls: string[][] = []
    const closes: Array<{ repo: string; number: number }> = []
    const markerlessProbe = {
      state: 'OPEN',
      headRefName: 'ar-229-is-positive',
      title: 'Add isPositive util',
      body: '',
    }
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(calls.length === 1 ? markerlessProbe : { ...markerlessProbe, state: 'CLOSED' }) }
      }
      throw new Error(`unexpected gh args ${args.join(' ')}`)
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 279,
      expectedIssueKey: 'AR-229',
      requireTitleMarker: false,
      githubWrite: githubWrite(closes),
      runner,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '279'],
      ['pr', 'view', '279'],
    ])
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 279 }])
  })

  it('treats an already-closed probe PR as idempotent success', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      return {
        stdout: JSON.stringify({
          state: 'CLOSED',
          headRefName: 'ar-229-is-positive',
          title: 'Add isPositive util',
          body: '',
        }),
      }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 279,
      expectedIssueKey: 'AR-229',
      requireTitleMarker: false,
      githubWrite: githubWrite(),
      runner,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 279, state: 'CLOSED' })
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '279'],
    ])
  })

  it('refuses a probe that is not tied to the expected issue key before closing', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      return {
        stdout: JSON.stringify({
          ...openProbe,
          body: 'Closes AR-99',
          headRefName: 'factory-e2e/ar-99-probe',
          title: '[factory-e2e] AR-99 probe',
        }),
      }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 125,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      runner,
    })).rejects.toThrow(/missing issue key AR-42/)
    expect(calls).toHaveLength(1)
  })

  it('fails closed when workspace close errors and does not claim success', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(openProbe) }
      }
      throw new Error(`unexpected gh args ${args.join(' ')}`)
    }
    const write = githubWrite()
    write.closePullRequest = async () => { throw new Error('workspace close failed') }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 126,
      expectedIssueKey: 'AR-42',
      githubWrite: write,
      runner,
    })).rejects.toThrow(/workspace close failed/)
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '126'],
    ])
  })

  it('fails closed when read-back is not CLOSED', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(openProbe) }
      }
      return { stdout: '' }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 127,
      expectedIssueKey: 'AR-42',
      githubWrite: githubWrite(),
      runner,
    })).rejects.toThrow(/live state is OPEN/)
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '127'],
      ['pr', 'view', '127'],
    ])
  })

  it('reports a clear connection error without invoking gh when GitHub writes are unavailable', async () => {
    let runnerCalled = false
    const runner: GhRunner = async () => {
      runnerCalled = true
      throw new Error('gh must not be invoked')
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 129,
      expectedIssueKey: 'AR-42',
      runner,
    })).rejects.toThrow('GitHub write path not available on this mount — connect GitHub to your workspace')
    expect(runnerCalled).toBe(false)
  })
})
