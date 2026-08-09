import { describe, expect, it, vi } from 'vitest'

import { GithubApiIssueRead } from './github-api-issue-read'

const fetchByPath = (byPath: Record<string, () => Response>) =>
  vi.fn(async (input: string | URL | Request) => {
    const url = String(input)
    const path = url.replace('https://api.github.com', '')
    const handler = byPath[path]
    if (!handler) throw new Error(`unexpected request: ${url}`)
    return handler()
  })

describe('GithubApiIssueRead', () => {
  it('reads an issue directly through the GitHub REST API for a confirmed-public repo', async () => {
    const request = fetchByPath({
      '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
      '/repos/AgentWorkforce/factory/issues/222': () => new Response(JSON.stringify({
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
      }), { status: 200 }),
    })
    const reader = new GithubApiIssueRead({ fetch: request })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).resolves.toEqual({
      outcome: 'found',
      issue: {
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
      },
    })
    expect(request).toHaveBeenCalledWith(
      'https://api.github.com/repos/AgentWorkforce/factory/issues/222',
      expect.objectContaining({ method: 'GET' }),
    )
  })

  it('returns not-found for a 404 against a repo it has confirmed is public', async () => {
    const reader = new GithubApiIssueRead({
      fetch: fetchByPath({
        '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
        '/repos/AgentWorkforce/factory/issues/999999': () => new Response('{}', { status: 404 }),
      }),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 999_999)).resolves.toEqual({ outcome: 'not-found' })
  })

  it('returns indeterminate — never not-found — for a 404 against a repo it cannot confirm is public', async () => {
    // This is the private-repo case: GitHub returns the same 404 for "issue
    // does not exist" and "repository is private, existence hidden from an
    // unauthenticated caller". Before this reader probed repo visibility, a
    // 404 here collapsed straight to `undefined`, indistinguishable from a
    // confirmed miss on a public repo — RED without the repo-visibility check.
    const reader = new GithubApiIssueRead({
      fetch: fetchByPath({
        '/repos/AgentWorkforce/cloud': () => new Response('{}', { status: 404 }),
        '/repos/AgentWorkforce/cloud/issues/222': () => new Response('{}', { status: 404 }),
      }),
    })

    const result = await reader.getIssue('AgentWorkforce/cloud', 222)
    expect(result.outcome).toBe('indeterminate')
    expect(result).not.toEqual({ outcome: 'not-found' })
  })

  it('caches a confirmed repo-visibility verdict across repeated lookups', async () => {
    const request = fetchByPath({
      '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
      '/repos/AgentWorkforce/factory/issues/1': () => new Response('{}', { status: 404 }),
      '/repos/AgentWorkforce/factory/issues/2': () => new Response('{}', { status: 404 }),
    })
    const reader = new GithubApiIssueRead({ fetch: request })

    await reader.getIssue('AgentWorkforce/factory', 1)
    await reader.getIssue('AgentWorkforce/factory', 2)

    const repoProbeCalls = request.mock.calls.filter(([input]) =>
      String(input) === 'https://api.github.com/repos/AgentWorkforce/factory')
    expect(repoProbeCalls).toHaveLength(1)
  })

  it('does not cache a rate-limited repo-visibility probe, so a later clean answer is trusted', async () => {
    let call = 0
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = String(input)
      if (url === 'https://api.github.com/repos/AgentWorkforce/factory') {
        call += 1
        return call === 1 ? new Response('{}', { status: 429 }) : new Response('{}', { status: 200 })
      }
      return new Response('{}', { status: 404 })
    })
    const reader = new GithubApiIssueRead({ fetch: request })

    const first = await reader.getIssue('AgentWorkforce/factory', 1)
    expect(first).toEqual({
      outcome: 'indeterminate',
      reason: 'could not confirm AgentWorkforce/factory is publicly visible without authentication',
    })

    const second = await reader.getIssue('AgentWorkforce/factory', 2)
    expect(second).toEqual({ outcome: 'not-found' })
  })

  it('degrades a rate-limited issue lookup to indeterminate instead of throwing', async () => {
    const reader = new GithubApiIssueRead({
      fetch: fetchByPath({
        '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
        '/repos/AgentWorkforce/factory/issues/222': () => new Response('{}', { status: 403 }),
      }),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).resolves.toEqual({
      outcome: 'indeterminate',
      reason: 'GitHub API issue lookup returned HTTP 403',
    })
  })

  it('surfaces a genuine API failure instead of manufacturing absence', async () => {
    const reader = new GithubApiIssueRead({
      fetch: fetchByPath({
        '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
        '/repos/AgentWorkforce/factory/issues/222': () => new Response('{}', { status: 500 }),
      }),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).rejects.toThrow(
      'GitHub API issue lookup failed (HTTP 500)',
    )
  })

  it('treats a pull request returned by the issues endpoint as an authoritative issue miss', async () => {
    const reader = new GithubApiIssueRead({
      fetch: fetchByPath({
        '/repos/AgentWorkforce/factory': () => new Response('{}', { status: 200 }),
        '/repos/AgentWorkforce/factory/issues/222': () => new Response(JSON.stringify({
          id: 222,
          number: 222,
          title: '[factory] PR',
          html_url: 'https://github.example/AgentWorkforce/factory/pull/222',
          pull_request: {},
        }), { status: 200 }),
      }),
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).resolves.toEqual({ outcome: 'not-found' })
  })
})
