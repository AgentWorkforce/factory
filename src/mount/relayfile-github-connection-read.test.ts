import { describe, expect, it, vi } from 'vitest'

import { RelayfileGithubConnectionRead } from './relayfile-github-connection-read'

describe('RelayfileGithubConnectionRead', () => {
  it('reads an issue through the Relayfile workspace SDK request surface', async () => {
    const requestJson = vi.fn(async () => ({
      data: {
        repository: {
          issue: {
            id: 'I_222',
            number: 222,
            title: '[factory] Restore dispatch',
            body: 'Use the connected GitHub API as a fallback.',
            state: 'OPEN',
            url: 'https://github.example/AgentWorkforce/factory/issues/222',
            updatedAt: '2026-08-08T12:00:00Z',
            author: { login: 'factory-app' },
            labels: { nodes: [{ name: 'factory' }, { name: 'factory-repo' }] },
          },
        },
      },
    }))
    const reader = new RelayfileGithubConnectionRead({ workspace: { requestJson } })

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
    expect(requestJson).toHaveBeenCalledWith(expect.objectContaining({
      operation: 'getGithubIssue',
      method: 'POST',
      path: 'api/v1/github/graphql',
      body: expect.objectContaining({
        variables: { owner: 'AgentWorkforce', repo: 'factory', number: 222 },
      }),
    }))
  })

  it('returns undefined only for an authoritative empty issue result', async () => {
    const reader = new RelayfileGithubConnectionRead({
      workspace: { requestJson: vi.fn(async () => ({ data: { repository: { issue: null } } })) },
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 999_999)).resolves.toBeUndefined()
  })

  it('surfaces GraphQL failures instead of manufacturing absence', async () => {
    const reader = new RelayfileGithubConnectionRead({
      workspace: {
        requestJson: vi.fn(async () => ({
          data: { repository: { issue: null } },
          errors: [{ extensions: { type: 'FORBIDDEN' } }],
        })),
      },
    })

    await expect(reader.getIssue('AgentWorkforce/factory', 222)).rejects.toThrow(
      'GitHub API issue lookup failed (FORBIDDEN)',
    )
  })
})
