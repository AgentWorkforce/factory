import { describe, expect, it } from 'vitest'

import { closeProbePr } from './probe-closer'
import type { GhRunner } from './merge-gate'

const openProbe = {
  state: 'OPEN',
  headRefName: 'factory-e2e/ar-42-probe',
  title: '[factory-e2e] AR-42 probe',
  body: 'Closes AR-42',
}

describe('closeProbePr', () => {
  it('guards, closes, and confirms CLOSED via read-back', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(calls.length === 1 ? openProbe : { ...openProbe, state: 'CLOSED' }) }
      }
      if (args[0] === 'pr' && args[1] === 'close') {
        return { stdout: '' }
      }
      throw new Error(`unexpected gh args ${args.join(' ')}`)
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 123,
      expectedIssueKey: 'AR-42',
      runner,
    })).resolves.toEqual({ repo: 'AgentWorkforce/pear', prNumber: 123, state: 'CLOSED' })

    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '123'],
      ['pr', 'close', '123'],
      ['pr', 'view', '123'],
    ])
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
      runner,
    })).rejects.toThrow(/missing \[factory-e2e\] probe marker/)
    expect(calls).toHaveLength(1)
  })

  it('refuses a probe that is not tied to the expected issue key before closing', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      return { stdout: JSON.stringify({ ...openProbe, body: 'Closes AR-99', title: '[factory-e2e] AR-99 probe' }) }
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 125,
      expectedIssueKey: 'AR-42',
      runner,
    })).rejects.toThrow(/missing issue key AR-42/)
    expect(calls).toHaveLength(1)
  })

  it('fails closed when gh close errors and does not claim success', async () => {
    const calls: string[][] = []
    const runner: GhRunner = async (args) => {
      calls.push(args)
      if (args[0] === 'pr' && args[1] === 'view') {
        return { stdout: JSON.stringify(openProbe) }
      }
      throw new Error('gh close failed')
    }

    await expect(closeProbePr({
      repo: 'AgentWorkforce/pear',
      prNumber: 126,
      expectedIssueKey: 'AR-42',
      runner,
    })).rejects.toThrow(/gh close failed/)
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '126'],
      ['pr', 'close', '126'],
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
      runner,
    })).rejects.toThrow(/live state is OPEN/)
    expect(calls.map((args) => args.slice(0, 3))).toEqual([
      ['pr', 'view', '127'],
      ['pr', 'close', '127'],
      ['pr', 'view', '127'],
    ])
  })
})
