import { describe, expect, it, vi } from 'vitest'

import { RelayfileGithubConnectionRead } from './relayfile-github-connection-read'

const READ_URL = 'https://relayfile.example/v1/workspaces/rw_test/github/repos/AgentWorkforce/private-repo/issues/159/read'

const reader = (fetchImpl: (input: string | URL | Request, init?: RequestInit) => Promise<Response>) =>
  new RelayfileGithubConnectionRead({
    workspaceId: 'rw_test',
    baseUrl: 'https://relayfile.example',
    tokenProvider: async () => 'delegated-relayfile-token',
    fetch: fetchImpl,
    correlationIdFactory: () => 'corr-1',
  })

describe('RelayfileGithubConnectionRead', () => {
  it('resolves a private-repo issue as found through the mount-native app-actor route', async () => {
    const request = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      expect(String(input)).toBe(READ_URL)
      expect(init?.method).toBe('POST')
      expect(JSON.parse(String(init?.body))).toEqual({ actor: 'app' })
      expect(init?.headers).toMatchObject({
        authorization: 'Bearer delegated-relayfile-token',
        'x-correlation-id': 'corr-1',
      })
      return Response.json({
        outcome: 'found',
        issue: {
          repo: 'AgentWorkforce/private-repo',
          number: 159,
          path: '/github/repos/AgentWorkforce__private-repo/issues/by-id/159.json',
          content: {
            provider: 'github',
            objectType: 'issue',
            objectId: 'I_private_159',
            payload: { number: 159, title: 'Private issue', state: 'open' },
          },
        },
      })
    })

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).resolves.toEqual({
      outcome: 'found',
      issue: {
        repo: 'AgentWorkforce/private-repo',
        number: 159,
        path: '/github/repos/AgentWorkforce__private-repo/issues/by-id/159.json',
        content: {
          provider: 'github',
          objectType: 'issue',
          objectId: 'I_private_159',
          payload: { number: 159, title: 'Private issue', state: 'open' },
        },
      },
    })
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('resolves an authoritative not-found from the mount unchanged', async () => {
    const request = vi.fn(async () => Response.json({ outcome: 'not-found' }))

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).resolves.toEqual({ outcome: 'not-found' })
  })

  it('resolves a mount-reported indeterminate unchanged, including its reason', async () => {
    const request = vi.fn(async () => Response.json({
      outcome: 'indeterminate',
      reason: 'GitHub App issue lookup returned HTTP 403',
    }))

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).resolves.toEqual({
      outcome: 'indeterminate',
      reason: 'GitHub App issue lookup returned HTTP 403',
    })
  })

  // Must-not-fire per relayfile-cloud#159's accepted contract amendment: a
  // missing workspace GitHub connection (424, no `outcome`) must never fold
  // into `indeterminate` — that would present a broken credential path as
  // "no work found" workspace-wide instead of a loud, diagnosable failure.
  it('throws loudly on a 424 missing-connection response instead of degrading to indeterminate', async () => {
    const request = vi.fn(async () => Response.json(
      { code: 'github_app_credential_unavailable', message: 'GitHub App credential is unavailable for this workspace', correlationId: 'corr-1' },
      { status: 424 },
    ))

    const result = reader(request).getIssue('AgentWorkforce/private-repo', 159)
    await expect(result).rejects.toThrow(/HTTP 424/)
    await expect(result).rejects.toThrow(/github_app_credential_unavailable/)
  })

  it('throws loudly on a 503 missing-credential-config response instead of degrading to indeterminate', async () => {
    const request = vi.fn(async () => Response.json(
      { code: 'github_app_credential_unavailable', message: 'GitHub App credential service is unavailable', correlationId: 'corr-1' },
      { status: 503 },
    ))

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow(/HTTP 503/)
  })

  it('throws loudly on a 403 actor_not_supported response instead of degrading to indeterminate', async () => {
    const request = vi.fn(async () => Response.json(
      { code: 'actor_not_supported', message: 'declared actor cannot perform GitHub issue reads here', correlationId: 'corr-1' },
      { status: 403 },
    ))

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow(/actor_not_supported/)
  })

  it('surfaces a malformed/failed provider read (502) as a thrown error, not an empty result', async () => {
    const request = vi.fn(async () => Response.json(
      { code: 'github_app_read_failed', message: 'GitHub App issue read failed', correlationId: 'corr-1' },
      { status: 502 },
    ))

    await expect(reader(request).getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow(/HTTP 502/)
  })

  // Must-not-fire: the workspace token provider throws when no token is
  // available (see relayfile-cloud-mount-client.ts's wiring). That is a
  // credential-path failure, not a transport one, and must stay a loud
  // thrown error under the same accepted-amendment reasoning as the 424/503
  // cases above — catching it alongside network errors would let an expired
  // or missing Relayfile token masquerade as `indeterminate`.
  it('propagates a token-provider failure instead of degrading to indeterminate', async () => {
    const request = vi.fn()
    const badReader = new RelayfileGithubConnectionRead({
      workspaceId: 'rw_test',
      baseUrl: 'https://relayfile.example',
      tokenProvider: async () => { throw new Error('no workspace token available') },
      fetch: request,
    })

    await expect(badReader.getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow('no workspace token available')
    expect(request).not.toHaveBeenCalled()
  })

  // Validated lazily, not in the constructor: this reader is built
  // unconditionally inside RelayfileCloudMountClient whenever a base URL is
  // present, alongside file reads, writeback, and subscriptions sharing the
  // same client — an eager throw here would fail the entire mount, not just
  // GitHub reads.
  it('rejects a non-HTTPS mount base URL when a read is attempted, not at construction', async () => {
    const request = vi.fn()
    const insecureReader = new RelayfileGithubConnectionRead({
      workspaceId: 'rw_test',
      baseUrl: 'http://relayfile.example',
      tokenProvider: async () => 'delegated-relayfile-token',
      fetch: request,
    })

    await expect(insecureReader.getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow(/must use https/)
    expect(request).not.toHaveBeenCalled()
  })

  it('rejects a malformed mount base URL with a clear message when a read is attempted', async () => {
    const malformedReader = new RelayfileGithubConnectionRead({
      workspaceId: 'rw_test',
      baseUrl: 'not-a-url',
      tokenProvider: async () => 'delegated-relayfile-token',
    })

    await expect(malformedReader.getIssue('AgentWorkforce/private-repo', 159)).rejects.toThrow(/malformed/)
  })

  it('degrades to indeterminate when the mount itself cannot be reached', async () => {
    const request = vi.fn(async () => { throw new Error('fetch failed: ECONNREFUSED') })

    const result = await reader(request).getIssue('AgentWorkforce/private-repo', 159)
    expect(result.outcome).toBe('indeterminate')
    expect(result).not.toEqual({ outcome: 'not-found' })
    if (result.outcome === 'indeterminate') {
      expect(result.reason).toContain('could not reach the Relayfile mount')
    }
  })

  it('rejects an issue number that is not a positive integer', async () => {
    const request = vi.fn()
    await expect(reader(request).getIssue('AgentWorkforce/private-repo', -1)).rejects.toThrow(
      'GitHub issue number must be a positive integer',
    )
    expect(request).not.toHaveBeenCalled()
  })
})
