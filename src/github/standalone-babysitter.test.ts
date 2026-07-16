import { describe, expect, it } from 'vitest'

import { FakeMountClient } from '../testing'
import {
  explicitLinkedIssueKey,
  parseStandaloneBabysitTarget,
  readStandalonePullRequest,
  standaloneBabysitterAgentName,
} from './standalone-babysitter'

describe('standalone PR babysitter helpers', () => {
  it('parses a positive PR number and canonical GitHub PR URL', () => {
    expect(parseStandaloneBabysitTarget('10')).toEqual({ prNumber: 10 })
    expect(parseStandaloneBabysitTarget('https://github.com/AgentWorkforce/hoopsheet/pull/10')).toEqual({
      repo: 'AgentWorkforce/hoopsheet',
      prNumber: 10,
      url: 'https://github.com/AgentWorkforce/hoopsheet/pull/10',
    })
  })

  it('rejects non-canonical, encoded, or unsafe PR targets', () => {
    for (const target of [
      '0',
      '9007199254740992',
      'http://github.com/AgentWorkforce/hoopsheet/pull/10',
      'https://user@github.com/AgentWorkforce/hoopsheet/pull/10',
      'https://github.com/AgentWorkforce/hoopsheet/pull/10?diff=split',
      'https://github.com/AgentWorkforce%2Fother/hoopsheet/pull/10',
      'https://github.com/AgentWorkforce/%68oopsheet/pull/10',
    ]) {
      expect(() => parseStandaloneBabysitTarget(target)).toThrow(/factory babysit requires/u)
    }
  })

  it('uses a complete mounted snapshot when gh is unavailable', async () => {
    const mount = new FakeMountClient({
      '/github/repos/AgentWorkforce__hoopsheet/pulls/by-id/10.json': {
        payload: {
          number: 10,
          title: 'Add league subdomain routing',
          body: 'Full acceptance criteria',
          state: 'open',
          draft: false,
          html_url: 'https://github.com/AgentWorkforce/hoopsheet/pull/10',
          head: { ref: 'codex/league-public-sites', sha: 'abc123', repo: { full_name: 'AgentWorkforce/hoopsheet' } },
          base: { ref: 'main' },
        },
      },
    })

    const pr = await readStandalonePullRequest(
      mount,
      { repo: 'AgentWorkforce/hoopsheet', prNumber: 10 },
      async () => { throw new Error('gh unavailable') },
    )

    expect(pr).toMatchObject({
      source: 'mount',
      title: 'Add league subdomain routing',
      state: 'open',
      draft: false,
      headRef: 'codex/league-public-sites',
      headSha: 'abc123',
      baseRef: 'main',
    })
  })

  it('uses live gh metadata to hydrate and supersede an incomplete mounted snapshot', async () => {
    const mount = new FakeMountClient({
      '/github/repos/AgentWorkforce__hoopsheet/pulls/by-id/10.json': {
        payload: { number: 10, state: 'open', head_ref: 'stale-branch' },
      },
    })
    const pr = await readStandalonePullRequest(
      mount,
      { repo: 'AgentWorkforce/hoopsheet', prNumber: 10 },
      async () => ({
        stdout: JSON.stringify({
          number: 10,
          title: 'Live title',
          body: 'Live body',
          state: 'OPEN',
          isDraft: false,
          url: 'https://github.com/AgentWorkforce/hoopsheet/pull/10',
          headRefName: 'codex/league-public-sites',
          headRefOid: 'live-sha',
          baseRefName: 'main',
          headRepository: { nameWithOwner: 'AgentWorkforce/hoopsheet' },
        }),
      }),
    )

    expect(pr).toMatchObject({
      source: 'mount+gh',
      title: 'Live title',
      body: 'Live body',
      headRef: 'codex/league-public-sites',
      headSha: 'live-sha',
    })
  })

  it('preserves mounted identity fields while live gh guard fields override stale mount state', async () => {
    const mount = new FakeMountClient({
      '/github/repos/AgentWorkforce__hoopsheet/pulls/by-id/10.json': {
        payload: {
          number: 10,
          title: 'Mounted title',
          body: 'Mounted body',
          state: 'open',
          draft: false,
          head: { ref: 'feature', sha: 'mounted-sha', repo: { full_name: 'AgentWorkforce/hoopsheet' } },
          base: { ref: 'main' },
        },
      },
    })
    const pr = await readStandalonePullRequest(
      mount,
      { repo: 'AgentWorkforce/hoopsheet', prNumber: 10 },
      async () => ({
        stdout: JSON.stringify({ number: 10, state: 'CLOSED', isDraft: false }),
      }),
    )

    expect(pr).toMatchObject({
      source: 'mount+gh',
      state: 'CLOSED',
      headRef: 'feature',
      headSha: 'mounted-sha',
      headRepo: 'AgentWorkforce/hoopsheet',
      baseRef: 'main',
      crossRepository: false,
    })
  })

  it('preserves the mounted PR body when live metadata has an empty body', async () => {
    const mount = new FakeMountClient({
      '/github/repos/AgentWorkforce__hoopsheet/pulls/by-id/10.json': {
        payload: {
          number: 10,
          title: 'Mounted title',
          body: 'Mounted definition of done',
          state: 'open',
          draft: false,
          head: { ref: 'feature', sha: 'mounted-sha', repo: { full_name: 'AgentWorkforce/hoopsheet' } },
          base: { ref: 'main' },
        },
      },
    })
    const pr = await readStandalonePullRequest(
      mount,
      { repo: 'AgentWorkforce/hoopsheet', prNumber: 10 },
      async () => ({
        stdout: JSON.stringify({
          number: 10,
          title: 'Live title',
          body: '',
          state: 'OPEN',
          isDraft: false,
          headRefName: 'feature',
          headRefOid: 'live-sha',
          baseRefName: 'main',
          headRepository: { nameWithOwner: 'AgentWorkforce/hoopsheet' },
        }),
      }),
    )

    expect(pr.body).toBe('Mounted definition of done')
  })

  it('only selects an unambiguous explicit linked issue', () => {
    expect(explicitLinkedIssueKey('Closes AR-123')).toBe('AR-123')
    expect(explicitLinkedIssueKey('Closes AR-123\nFixes AR-456')).toBeUndefined()
    expect(explicitLinkedIssueKey('Closes AR-123, Closes AR-456')).toBeUndefined()
    expect(explicitLinkedIssueKey('AR-123 appears incidentally')).toBeUndefined()
  })

  it('generates bounded, collision-resistant PR-keyed agent names', () => {
    const first = standaloneBabysitterAgentName(`AgentWorkforce/${'a'.repeat(100)}`, 10)
    const second = standaloneBabysitterAgentName(`AgentWorkforce/${'a'.repeat(99)}b`, 10)
    expect(first.length).toBeLessThanOrEqual(63)
    expect(first).not.toBe(second)
    expect(first).toMatch(/-10-[a-f0-9]{8}-babysit$/u)
  })
})
