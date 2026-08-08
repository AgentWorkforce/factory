import { describe, expect, it, vi } from 'vitest'

import { GithubApiIssueRead } from './github-api-issue-read'

describe('GithubApiIssueRead', () => {
  it('reads an issue directly through the GitHub REST API', async () => {
    const request = vi.fn(async () => new Response(JSON.stringify({
      id: 222,
      node_id: 'I_222',
      number: 222,
      title: '[factory] Restore dispatch',
      body: 'Use the GitHub API as a fallback.',
      state: 'open',
      html_url: 'https://github.example/AgentWorkforce/factory/issues/222',
      updated_at: '2026-08-08T12:00:00Z',
      user: { login: 'factory-app' },
      labels: [{ name: 'factory' }, { name: 'factory-repo' }],
    }), { status: 200 }))
    const reader = new GithubApiIssueRead({ fetch: request })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).resolves.toEqual({
      repo: 'AgentWorkforce/factory',
      number: 222,
      path: '/github/repos/AgentWorkforce__factory/issues/by-id/222.json',
      content: expect.objectContaining({
        provider: 'github',
        objectType: 'issue',
        objectId: 'I_222',
        payload: expect.objectContaining({
          number: 222,
          title: '[factory] Restore dispatch',
          state: 'open',
          labels: [{ name: 'factory' }, { name: 'factory-repo' }],
          repository: { name: 'factory', owner: { login: 'AgentWorkforce' } },
        }),
      }),
    })
    expect(request).toHaveBeenCalledWith(
      'https://api.github.com/repos/AgentWorkforce/factory/issues/222',
      expect.objectContaining({ method: 'GET' }),
    )
  })

  it('returns undefined only for an authoritative 404', async () => {
    const reader = new GithubApiIssueRead({
      fetch: vi.fn(async () => new Response('{}', { status: 404 })),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 999_999)).resolves.toBeUndefined()
  })

  it('surfaces API failures instead of manufacturing absence', async () => {
    const reader = new GithubApiIssueRead({
      fetch: vi.fn(async () => new Response('{}', { status: 403 })),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).rejects.toThrow(
      'GitHub API issue lookup failed (HTTP 403)',
    )
  })

  it('does not treat a pull request returned by the issues endpoint as an issue', async () => {
    const reader = new GithubApiIssueRead({
      fetch: vi.fn(async () => new Response(JSON.stringify({
        id: 222,
        number: 222,
        title: '[factory] PR',
        html_url: 'https://github.example/AgentWorkforce/factory/pull/222',
        pull_request: {},
      }), { status: 200 })),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).rejects.toThrow(
      'returned an incomplete issue record',
    )
  })
})
