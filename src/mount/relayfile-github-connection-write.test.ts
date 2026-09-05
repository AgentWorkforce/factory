import { describe, expect, it, vi } from 'vitest'

import { FakeMountClient } from '../testing'
import { RelayfileGithubConnectionWrite, type GitCommandRunner } from './relayfile-github-connection-write'

const gitRunner = (): GitCommandRunner => vi.fn(async (args) => {
  if (args.includes('symbolic-ref')) return { stdout: 'fix/issue-52\n' }
  if (args.includes('rev-parse')) return { stdout: '1234567890abcdef1234567890abcdef12345678\n' }
  throw new Error(`unexpected git args: ${args.join(' ')}`)
})

const gitRunnerForBranch = (branch: string): GitCommandRunner => vi.fn(async (args) => {
  if (args.includes('symbolic-ref')) return { stdout: `${branch}\n` }
  if (args.includes('rev-parse')) return { stdout: '1234567890abcdef1234567890abcdef12345678\n' }
  throw new Error(`unexpected git args: ${args.join(' ')}`)
})

describe('RelayfileGithubConnectionWrite', () => {
  it('reads a private issue through the authenticated connected projection', async () => {
    const path = '/github/repos/PrivateOrg__private-repo/issues/by-id/42.json'
    const content = { payload: { number: 42, labels: [{ name: 'factory:human-review' }] } }
    const write = new RelayfileGithubConnectionWrite({
      mount: new FakeMountClient({ [path]: content }),
    })

    await expect(write.getIssue('PrivateOrg/private-repo', 42)).resolves.toEqual({
      outcome: 'found',
      issue: { repo: 'PrivateOrg/private-repo', number: 42, path, content },
    })
  })

  it('publishes an already-pushed remote branch without reading an orchestrator-local clone', async () => {
    const pullRequestPath = '/github/repos/AgentWorkforce/factory/pull-requests/factory-factory-ar-85-agentworkforce-factory-pushed.json'
    const refPath = '/github/repos/AgentWorkforce/factory/refs/refs%2Fheads%2Ffactory%2Far-85-agentworkforce-factory.json'
    class ReceiptMount extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, { content: { created: 85, url: 'https://github.com/AgentWorkforce/factory/pull/85' } })
        }
      }
    }
    const mount = new ReceiptMount({
      [refPath]: { ref: 'refs/heads/factory/ar-85-agentworkforce-factory', object: { sha: 'deadbeef' } },
    })
    const git = vi.fn(async () => { throw new Error('remote publication must not inspect local git') })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: git })

    const input = {
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/ar-85-agentworkforce-factory',
      baseRef: 'main',
      title: 'Issue 85',
      body: 'Fixes #85',
    }
    await expect(write.publishPullRequest(input)).resolves.toEqual({
      repo: 'AgentWorkforce/factory',
      number: 85,
      url: 'https://github.com/AgentWorkforce/factory/pull/85',
      headRef: 'factory/ar-85-agentworkforce-factory',
      headSha: undefined,
      author: 'app',
    })
    expect(git).not.toHaveBeenCalled()
    expect(mount.writes).toEqual([{
      path: pullRequestPath,
      content: {
        title: 'Issue 85',
        head: 'factory/ar-85-agentworkforce-factory',
        base: 'main',
        body: 'Fixes #85',
        author: 'app',
      },
    }])

    await expect(write.publishPullRequest(input)).resolves.toMatchObject({ number: 85 })
    expect(mount.writes[1]?.path).toBe(pullRequestPath)
  })

  it('refuses to open a PR against a remote branch that was never pushed (#430)', async () => {
    // Measured production shape: a remote implementer supplies `headRef` and
    // no `headSha`, so nothing in this process created the branch. Against
    // `origin/main` this proceeds straight to the PR create, which GitHub
    // rejects with a bare `422 Validation Failed` that reads like a payload
    // bug rather than naming the actual cause. The fix confirms the ref
    // exists first and names the real cause instead.
    const mount = new FakeMountClient()
    const git = vi.fn(async () => { throw new Error('remote publication must not inspect local git') })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: git })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/ar-902-agentworkforce-factory',
      baseRef: 'main',
      title: 'Issue 902',
      body: 'Fixes #902',
    })).rejects.toThrow(
      'Refusing to publish GitHub PR: implementer branch factory/ar-902-agentworkforce-factory ' +
      'was never pushed to AgentWorkforce/factory (refs/heads/factory/ar-902-agentworkforce-factory does not exist',
    )
    // The real fix, not merely a thrown error: no PR draft is ever authored
    // against a head GitHub has never seen, so no 422 is ever provoked.
    expect(git).not.toHaveBeenCalled()
    expect(mount.writes).toEqual([])
  })

  it('propagates an indeterminate ref read instead of declaring the branch absent (#453 review)', async () => {
    // A transport blip, an auth hiccup, or the projection simply not having
    // caught up with a branch that really was just pushed must NOT read the
    // same as a confirmed-missing ref: doing so hands the upstream
    // non-retryable classifier a false "never pushed" and abandons a
    // genuinely publishable dispatch on its first bad network moment.
    class FlakyMount extends FakeMountClient {
      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        throw Object.assign(new Error('upstream request timed out'), { status: 503 })
      }
    }
    const mount = new FlakyMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/ar-903-agentworkforce-factory',
      baseRef: 'main',
      title: 'Issue 903',
      body: 'Fixes #903',
    })).rejects.toThrow('upstream request timed out')
    expect(mount.writes).toEqual([])
  })

  it('does not treat a bare "404" substring in an unrelated error as a confirmed-absent ref (#453 review)', async () => {
    // CodeRabbit, #453 review: a transport error whose message merely
    // CONTAINS "404" - here, a branch name segment - must not classify as a
    // confirmed-absent ref. Only unambiguous not-found phrasing (or a
    // structured status/code) may.
    class AmbiguousMessageMount extends FakeMountClient {
      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        throw new Error(`request to fetch refs/heads/feature-404 failed: connection reset`)
      }
    }
    const mount = new AmbiguousMessageMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'feature-404',
      baseRef: 'main',
      title: 'Issue 905',
      body: 'Fixes #905',
    })).rejects.toThrow('connection reset')
    expect(mount.writes).toEqual([])
  })

  it.each([
    ['ref not found', 'ref not found'],
    ['branch not found', 'branch not found: refs/heads/factory/ar-906-agentworkforce-factory'],
    ['404 Not Found', '404 Not Found'],
  ])('still recognizes an unambiguous "%s" phrasing as a confirmed-absent ref (#453 review)', async (_label, message) => {
    // cubic, #453 review: the fix for the bare-404 false positive above must
    // not overshoot into missing the real not-found phrasings a provider or
    // transport can actually use - a REAL unpushed branch must still
    // terminalize immediately instead of spending the full retry budget.
    class NotFoundMount extends FakeMountClient {
      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        throw new Error(message)
      }
    }
    const mount = new NotFoundMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/ar-906-agentworkforce-factory',
      baseRef: 'main',
      title: 'Issue 906',
      body: 'Fixes #906',
    })).rejects.toThrow(
      'Refusing to publish GitHub PR: implementer branch factory/ar-906-agentworkforce-factory was never pushed',
    )
  })

  it('confirms the ref through the encoded owner__repo projection when the nested layout 404s (#453 review)', async () => {
    // `getIssue` probes both of Relayfile's canonical layouts because a
    // workspace can expose only one of them. The ref-existence check must do
    // the same, or a workspace on the encoded-only layout would see every
    // remote publish misreported as an unpushed branch.
    const encodedRefPath = '/github/repos/AgentWorkforce__factory/refs/refs%2Fheads%2Ffactory%2Far-904-agentworkforce-factory.json'
    const pullRequestPath = '/github/repos/AgentWorkforce/factory/pull-requests/factory-factory-ar-904-agentworkforce-factory-pushed.json'
    class EncodedOnlyMount extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, { content: { created: 90, url: 'https://github.com/AgentWorkforce/factory/pull/90' } })
        }
      }
    }
    const mount = new EncodedOnlyMount({
      [encodedRefPath]: { ref: 'refs/heads/factory/ar-904-agentworkforce-factory', object: { sha: 'deadbeef' } },
    })
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/ar-904-agentworkforce-factory',
      baseRef: 'main',
      title: 'Issue 904',
      body: 'Fixes #904',
    })).resolves.toMatchObject({ number: 90 })
  })

  it('pushes the current ref before creating a pull request through Relayfile', async () => {
    const draft = 'factory-fix-issue-52-1234567890ab'
    const pullRequestPath = `/github/repos/AgentWorkforce/factory/pull-requests/${draft}.json`
    class ReceiptMount extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, {
            content: {
              created: '64',
              path: '/github/repos/AgentWorkforce/factory/pull-requests/64.json',
              url: 'https://github.com/AgentWorkforce/factory/pull/64',
            },
          })
        }
      }
    }
    const mount = new ReceiptMount()
    const git = gitRunner()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: git })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'feat(factory): use workspace GitHub writes',
      body: 'Fixes #52',
    })).resolves.toEqual({
      repo: 'AgentWorkforce/factory',
      number: 64,
      url: 'https://github.com/AgentWorkforce/factory/pull/64',
      headRef: 'fix/issue-52',
      headSha: '1234567890abcdef1234567890abcdef12345678',
      author: 'app',
    })

    expect(git).toHaveBeenNthCalledWith(1, ['-C', '/work/factory', 'symbolic-ref', '--short', 'HEAD'])
    expect(git).toHaveBeenNthCalledWith(2, ['-C', '/work/factory', 'rev-parse', 'HEAD'])
    expect(mount.writes).toEqual([
      {
        path: '/github/repos/AgentWorkforce/factory/refs/factory.json',
        content: {
          ref: 'refs/heads/fix/issue-52',
          sha: '1234567890abcdef1234567890abcdef12345678',
        },
      },
      {
        path: pullRequestPath,
        content: {
          title: 'feat(factory): use workspace GitHub writes',
          head: 'fix/issue-52',
          base: 'main',
          body: 'Fixes #52',
          author: 'app',
        },
      },
    ])
  })

  it('serializes concurrent create-ref confirmations that share the repository draft path', async () => {
    const createRefPath = '/github/repos/AgentWorkforce/factory/refs/factory.json'
    let releaseFirst!: () => void
    const firstConfirmation = new Promise<void>((resolve) => { releaseFirst = resolve })
    class ConcurrentRefMount extends FakeMountClient {
      createConfirmations = 0

      override async confirmWrite(path: string): Promise<'acked'> {
        if (path === createRefPath && ++this.createConfirmations === 1) await firstConfirmation
        return 'acked'
      }

      async getConfirmedWriteExternalId(path: string): Promise<string | undefined> {
        if (!path.includes('/pull-requests/')) return undefined
        return path.includes('concurrent-one') ? '71' : '72'
      }
    }
    const mount = new ConcurrentRefMount()
    const write = new RelayfileGithubConnectionWrite({ mount })
    const publish = (headRef: string, headSha: string) => write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef,
      headSha,
      baseRef: 'main',
      title: headRef,
      body: 'Concurrent publication proof',
    })

    const first = publish('factory/concurrent-one', '1111111111111111111111111111111111111111')
    await vi.waitFor(() => expect(mount.createConfirmations).toBe(1))
    const second = publish('factory/concurrent-two', '2222222222222222222222222222222222222222')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(mount.writes.filter((entry) => entry.path === createRefPath)).toHaveLength(1)

    releaseFirst()
    await expect(Promise.all([first, second])).resolves.toMatchObject([
      { number: 71 },
      { number: 72 },
    ])
    expect(mount.writes.filter((entry) => entry.path === createRefPath)).toHaveLength(2)
  })

  it('closes a pull request through its exact Relayfile writeback path', async () => {
    const mount = new FakeMountClient()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await write.closePullRequest({ repo: 'AgentWorkforce/factory', number: 63 })

    expect(mount.writes).toEqual([{
      path: '/github/repos/AgentWorkforce/factory/pulls/63/close.json',
      content: {},
    }])
  })

  it('posts deterministic app-authored issue comments and updates through confirmed drafts', async () => {
    const mount = new FakeMountClient()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Factory dispatch for 221',
      author: 'app',
    })
    await write.updateIssue({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['factory', 'bug', 'factory:in-progress', 'bug'],
      author: 'app',
    })

    expect(mount.writes).toEqual([
      {
        path: expect.stringMatching(
          /^\/github\/repos\/AgentWorkforce\/factory\/issues\/221\/comments\/factory-[a-f0-9]{24}\.json$/u,
        ),
        content: { body: 'Factory dispatch for 221' },
      },
      {
        path: '/github/repos/AgentWorkforce/factory/issues/221.json',
        content: {
          labels: ['factory', 'bug', 'factory:in-progress'],
        },
      },
    ])

    await write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Factory dispatch for 221',
      author: 'app',
    })
    expect(mount.writes[2]?.path).toBe(mount.writes[0]?.path)
  })

  it('authors no writeback path Relayfile\'s GitHub adapter refuses', async () => {
    // #431's red-check, and it asserts on the RENDERED PATH rather than on the
    // absence of an exception. The paths this surface used to author for
    // labels — `/labels/<draft>.json` and `/issues/{n}/labels/<draft>.json` —
    // threw nothing locally: `writeFile` accepted them, the mount guard
    // vouched for them, and only the server-side adapter refused, with
    // `Unsupported GitHub writeback path`. A test that watched for a thrown
    // error would have stayed green through the entire outage.
    //
    // Patterns transcribed from the adapter's own route table
    // (relayfile-adapters `packages/github/src/resources.ts`) rather than
    // imported from `GITHUB_WRITEBACK_ROUTES`, so a produced path has to
    // satisfy the adapter's shapes independently of the copy in src. If the
    // two disagree, one of them drifted and this fails.
    const adapterRoutes = [
      /^\/github\/repos\/[^/]+\/[^/]+\/issues(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/issues\/[^/]+\/comments(?:\/[^/]+(?:\.json|\/meta\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/reviews(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pull-requests(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/refs(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/close\.json$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/merge\.json$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/review-comments\/[^/]+\/replies(?:\/[^/]+(?:\.json)?)?$/u,
    ]

    class ReceiptMount extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path.includes('/pull-requests/')) {
          this.files.set(path, { content: { created: 85, url: 'https://github.com/AgentWorkforce/factory/pull/85' } })
        }
      }
    }
    const mount = new ReceiptMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    // Every mutation this surface exposes, driven through one instance.
    await write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/tmp/factory-clone',
      baseRef: 'main',
      title: 'Issue 52',
      body: 'Fixes #52',
    })
    await write.closePullRequest({ repo: 'AgentWorkforce/factory', number: 85 })
    await write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Factory dispatch for 221',
      author: 'app',
    })
    await write.updateIssue({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['factory', 'factory:in-progress'],
      author: 'app',
    })
    await write.updateIssue({
      repo: 'AgentWorkforce/factory',
      number: 221,
      state: 'closed',
      author: 'app',
    })

    const paths = mount.writes.map((entry) => entry.path)
    expect(paths.length).toBeGreaterThan(0)
    expect(paths.filter((path) => !adapterRoutes.some((route) => route.test(path)))).toEqual([])

    // The two writers that authored the unroutable paths are gone, not merely
    // uncalled. Keeping them would leave the next caller one method call away
    // from the same silent failure.
    expect('ensureRepositoryLabel' in write).toBe(false)
    expect('mutateIssueLabel' in write).toBe(false)
  })

  it('refuses an unroutable draft before the mount ever sees it', async () => {
    // The wiring half of the must-not-fire. `assertRoutedGithubWritebackPath`
    // owns the route table and is red-checked against #431's own path in
    // writeback-routes.test.ts; what has to be true HERE is that the producer
    // consults it before `writeFile`, so an unroutable draft is never created
    // and never enters the durable retry that reissued it every 30s.
    //
    // No public method can render an unroutable path any more — that is the
    // fix — so the refusal is provoked by standing in for the route table.
    vi.resetModules()
    vi.doMock('../github/writeback-routes', () => ({
      assertRoutedGithubWritebackPath: (path: string) => {
        throw new Error(`Refusing to author an unroutable GitHub writeback path: ${path}.`)
      },
    }))
    try {
      const { RelayfileGithubConnectionWrite: Isolated } = await import('./relayfile-github-connection-write')
      const mount = new FakeMountClient()
      const write = new Isolated({ mount })

      await expect(write.postIssueComment({
        repo: 'AgentWorkforce/factory',
        number: 221,
        body: 'Factory dispatch for 221',
        author: 'app',
      })).rejects.toThrow(/Refusing to author an unroutable GitHub writeback path/u)
      expect(mount.writes).toEqual([])
    } finally {
      vi.doUnmock('../github/writeback-routes')
      vi.resetModules()
    }
  })

  it('does not report an app issue comment when provider confirmation remains pending', async () => {
    class PendingCommentMount extends FakeMountClient {
      override async confirmWrite(path: string): Promise<'acked' | 'pending'> {
        return path.includes('/comments/') ? 'pending' : 'acked'
      }
    }
    const mount = new PendingCommentMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Unconfirmed comment',
      author: 'app',
    })).rejects.toThrow(/GitHub writeback did not complete .*: pending/u)
  })

  it('does not report an issue update when provider confirmation remains pending', async () => {
    // Was written against `mutateIssueLabel`, whose `/labels/` draft the
    // adapter never routed. The routed expression of a label change is the
    // issue PATCH, so the pending-confirmation contract is pinned there.
    class PendingIssueMount extends FakeMountClient {
      override async confirmWrite(path: string): Promise<'acked' | 'pending'> {
        return path.endsWith('/issues/221.json') ? 'pending' : 'acked'
      }
    }
    const mount = new PendingIssueMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.updateIssue({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['factory', 'factory:in-progress'],
      author: 'app',
    })).rejects.toThrow(/GitHub writeback did not complete .*: pending/u)
  })

  it('fails closed when the provider does not acknowledge a write', async () => {
    const mount = new FakeMountClient()
    const refPath = '/github/repos/AgentWorkforce/factory/refs/factory.json'
    mount.setConfirmWrite(refPath, 'timeout')
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).rejects.toThrow(`GitHub writeback did not complete for ${refPath}: timeout`)
  })

  it('updates an existing branch when retrying local-clone publication', async () => {
    const draft = 'factory-fix-issue-52-1234567890ab'
    const createRefPath = '/github/repos/AgentWorkforce/factory/refs/factory.json'
    const updateRefPath = '/github/repos/AgentWorkforce/factory/refs/refs%2Fheads%2Ffix%2Fissue-52.json'
    const pullRequestPath = `/github/repos/AgentWorkforce/factory/pull-requests/${draft}.json`
    class ExistingRefMount extends FakeMountClient {
      override async confirmWrite(path: string): Promise<'acked' | 'failed'> {
        return path === createRefPath ? 'failed' : 'acked'
      }

      async getConfirmedWriteFailureReason(path: string): Promise<string | undefined> {
        return path === createRefPath
          ? 'GitHub writeback failed with status 422: Reference already exists'
          : undefined
      }

      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, { content: { created: 66, url: 'https://github.com/AgentWorkforce/factory/pull/66' } })
        }
      }
    }
    const mount = new ExistingRefMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).resolves.toMatchObject({ number: 66 })

    expect(mount.writes.slice(0, 2)).toEqual([
      {
        path: createRefPath,
        content: {
          ref: 'refs/heads/fix/issue-52',
          sha: '1234567890abcdef1234567890abcdef12345678',
        },
      },
      {
        path: updateRefPath,
        content: {
          ref: 'refs/heads/fix/issue-52',
          sha: '1234567890abcdef1234567890abcdef12345678',
          force: false,
        },
      },
    ])
  })

  it('recognizes an existing-reference failure from a plain provider error object', async () => {
    const createRefPath = '/github/repos/AgentWorkforce/factory/refs/factory.json'
    const pullRequestPath = '/github/repos/AgentWorkforce/factory/pull-requests/factory-fix-issue-52-1234567890ab.json'
    class PlainErrorMount extends FakeMountClient {
      override async confirmWrite(path: string): Promise<'acked'> {
        if (path === createRefPath) {
          throw { message: 'GitHub writeback failed with status 422: Reference already exists' }
        }
        return 'acked'
      }

      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, { content: { created: 67, url: 'https://github.com/AgentWorkforce/factory/pull/67' } })
        }
      }
    }
    const mount = new PlainErrorMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).resolves.toMatchObject({ number: 67 })
  })

  it('rejects publishing from the base branch before writing a ref', async () => {
    const mount = new FakeMountClient()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunnerForBranch('main') })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).rejects.toThrow('Refusing to publish GitHub PR with head equal to base branch: main')
    expect(mount.writes).toEqual([])
  })

  it('refuses a stale local branch before pushing or opening a pull request', async () => {
    const mount = new FakeMountClient()
    const git = gitRunnerForBranch('factory/3022-chief-org-live-population')
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: git })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/cloud',
      clonePath: '/work/cloud',
      expectedHeadRef: 'factory/3021-agentworkforce-cloud-12345678',
      baseRef: 'main',
      title: '3021: repair deployment objective CI',
      body: 'Fixes #3021',
    })).rejects.toThrow(
      'Refusing to publish GitHub PR: expected head branch factory/3021-agentworkforce-cloud-12345678, found factory/3022-chief-org-live-population',
    )
    expect(git).toHaveBeenCalledTimes(1)
    expect(mount.writes).toEqual([])
  })

  it('retries until the created pull request receipt is visible', async () => {
    const draft = 'factory-fix-issue-52-1234567890ab'
    const pullRequestPath = `/github/repos/AgentWorkforce/factory/pull-requests/${draft}.json`
    class LaggingReceiptMount extends FakeMountClient {
      receiptReads = 0

      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        if (path === pullRequestPath) {
          this.receiptReads += 1
          if (this.receiptReads === 1) return { content: { title: 'draft not rewritten yet' } }
          return {
            content: {
              created: 65,
              path: '/github/repos/AgentWorkforce/factory/pull-requests/65.json',
              url: 'https://github.com/AgentWorkforce/factory/pull/65',
            },
          }
        }
        return super.readFile(path)
      }
    }
    const mount = new LaggingReceiptMount()
    const write = new RelayfileGithubConnectionWrite({
      mount,
      gitRunner: gitRunner(),
      receiptReadAttempts: 3,
      receiptReadDelayMs: 0,
    })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).resolves.toMatchObject({ number: 65 })
    expect(mount.receiptReads).toBe(2)
  })

  it('uses the acknowledged provider id when Relayfile reconciles the create draft', async () => {
    const draft = 'factory-fix-issue-52-1234567890ab'
    const pullRequestPath = `/github/repos/AgentWorkforce/hoopsheet/pull-requests/${draft}.json`
    class ReconciledDraftMount extends FakeMountClient {
      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        if (path === pullRequestPath) throw new Error('draft was reconciled after provider acknowledgement')
        return super.readFile(path)
      }

      override async getConfirmedWriteExternalId(path: string): Promise<string | undefined> {
        return path === pullRequestPath ? '53' : undefined
      }
    }
    const mount = new ReconciledDraftMount()
    const write = new RelayfileGithubConnectionWrite({
      mount,
      gitRunner: gitRunner(),
      receiptReadAttempts: 1,
      receiptReadDelayMs: 0,
    })

    await expect(write.publishPullRequest({
      repo: 'AgentWorkforce/hoopsheet',
      clonePath: '/work/hoopsheet',
      baseRef: 'main',
      title: '52: Bug: Creating a new league does not work',
      body: 'Fixes #52',
    })).resolves.toMatchObject({
      number: 53,
      url: 'https://github.com/AgentWorkforce/hoopsheet/pull/53',
    })
  })
})
