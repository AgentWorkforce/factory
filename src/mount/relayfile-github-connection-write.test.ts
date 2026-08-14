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
  it('publishes an already-pushed remote branch without reading an orchestrator-local clone', async () => {
    const pullRequestPath = '/github/repos/AgentWorkforce/factory/pull-requests/factory-factory-ar-85-agentworkforce-factory-pushed.json'
    class ReceiptMount extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        if (path === pullRequestPath) {
          this.files.set(path, { content: { created: 85, url: 'https://github.com/AgentWorkforce/factory/pull/85' } })
        }
      }
    }
    const mount = new ReceiptMount()
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

  it('proves a babysitter comment was recorded by the app after the real provider write', async () => {
    const providerCommentPath = '/github/repos/AgentWorkforce/factory/issues/221__app-identity/comments/7001/meta.json'
    class AppCommentMount extends FakeMountClient {
      constructor() {
        super({
          [providerCommentPath]: {
            payload: { id: 7001, body: 'Fixed in abc123', user: { login: 'factory-app[bot]', type: 'Bot' } },
          },
        })
      }

      async getConfirmedWriteExternalId(path: string): Promise<string | undefined> {
        return path.includes('/issues/221/comments/') ? '7001' : undefined
      }
    }
    const mount = new AppCommentMount()
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadDelayMs: 0 })

    await expect(write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Fixed in abc123',
    })).resolves.toEqual({
      repo: 'AgentWorkforce/factory',
      number: 221,
      commentId: 7001,
      author: 'app',
    })

    const authoredWrite = mount.writes.find((entry) => entry.path.includes('/issues/221/comments/factory-'))
    expect(authoredWrite?.content).toEqual({ body: 'Fixed in abc123', author: 'app' })
    expect(mount.reads).toContain(providerCommentPath)
  })

  it('rejects a comment receipt when GitHub records the local human instead of the app', async () => {
    class HumanCommentMount extends FakeMountClient {
      constructor() {
        super({
          '/github/repos/AgentWorkforce/factory/issues/221/comments/7002/meta.json': {
            payload: { id: 7002, user: { login: 'local-human', type: 'User' } },
          },
        })
      }

      async getConfirmedWriteExternalId(): Promise<string> {
        return '7002'
      }
    }
    const write = new RelayfileGithubConnectionWrite({ mount: new HumanCommentMount(), receiptReadDelayMs: 0 })

    await expect(write.postIssueComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'This must not inherit the process identity',
    })).rejects.toThrow(/provider recorded local-human/u)
  })

  it('posts review-thread replies through the adapter reply namespace and verifies the bot author', async () => {
    class ReviewReplyMount extends FakeMountClient {
      constructor() {
        super({
          '/github/repos/AgentWorkforce/factory/pulls/221/comments/7100.json': {
            payload: { id: 7100, author: { login: 'factory-app[bot]', type: 'Bot' } },
          },
        })
      }

      async getConfirmedWriteExternalId(): Promise<string> {
        return '7100'
      }
    }
    const mount = new ReviewReplyMount()
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadDelayMs: 0 })

    await expect(write.replyToReviewComment({
      repo: 'AgentWorkforce/factory',
      number: 221,
      inReplyTo: 7099,
      body: 'Addressed in abc123',
    })).resolves.toMatchObject({ commentId: 7100, author: 'app' })
    expect(mount.writes[0]).toMatchObject({
      path: expect.stringMatching(/\/pulls\/221\/review-comments\/7099\/replies\/factory-.+\.json$/u),
      content: { body: 'Addressed in abc123', author: 'app' },
    })
  })

  it('creates Notion lifecycle issues through the app connection and verifies the provider author', async () => {
    class AppIssueMount extends FakeMountClient {
      constructor() {
        super({
          '/github/repos/AgentWorkforce/factory/issues/900__notion-lifecycle/meta.json': {
            payload: { number: 900, user: { login: 'factory-app[bot]', type: 'Bot' } },
          },
        })
      }

      async getConfirmedWriteExternalId(): Promise<string> {
        return '900'
      }
    }
    const mount = new AppIssueMount()
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadDelayMs: 0 })

    await expect(write.createIssue({
      repo: 'AgentWorkforce/factory',
      title: 'Notion lifecycle',
      body: 'Authorized mounted spec',
      labels: ['factory'],
    })).resolves.toMatchObject({ number: 900, author: 'app' })
    expect(mount.writes[0]).toMatchObject({
      path: expect.stringMatching(/\/issues\/factory-.+\.json$/u),
      content: {
        title: 'Notion lifecycle',
        body: 'Authorized mounted spec',
        labels: ['factory'],
        author: 'app',
      },
    })
  })

  it('merges through the Relayfile connection with app authorship and the expected head', async () => {
    class MergeMount extends FakeMountClient {
      async getConfirmedWriteExternalId(): Promise<string> {
        return 'merge-sha'
      }
    }
    const mount = new MergeMount()
    const write = new RelayfileGithubConnectionWrite({ mount })

    await expect(write.mergePullRequest({
      repo: 'AgentWorkforce/factory',
      number: 221,
      expectedHeadSha: 'abc123',
      method: 'squash',
    })).resolves.toEqual({ sha: 'merge-sha' })
    expect(mount.writes).toContainEqual({
      path: '/github/repos/AgentWorkforce/factory/pulls/221/merge.json',
      content: { method: 'squash', sha: 'abc123', author: 'app' },
    })
  })
})

/**
 * Identity is the point of this suite: a comment Factory writes must be
 * authored by the GitHub App, never by whichever human happens to be logged
 * into a `gh` CLI on the host. Each assertion reads the author back from the
 * projection rather than trusting the write receipt.
 */
describe('RelayfileGithubConnectionWrite app-authored comments', () => {
  const repo = 'AgentWorkforce/factory'

  class CommentMount extends FakeMountClient {
    readonly #externalIds = new Map<string, string>()
    #author: unknown

    constructor(author: unknown) {
      super()
      this.#author = author
    }

    override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
      await super.writeFile(path, content, opts)
      this.#externalIds.set(path, '9001')
      // The provider projection materializes the created comment with the
      // identity GitHub actually recorded for it.
      this.files.set(`/github/repos/AgentWorkforce/factory/issues/221/comments/9001/meta.json`, {
        content: { payload: { id: 9001, user: this.#author } },
      })
      this.files.set(`/github/repos/AgentWorkforce/factory/pulls/221/comments/9001.json`, {
        content: { payload: { id: 9001, user: this.#author } },
      })
    }

    override async getConfirmedWriteExternalId(path: string): Promise<string | undefined> {
      return this.#externalIds.get(path)
    }
  }

  const appAuthor = { login: 'agent-relay[bot]', type: 'Bot' }
  const humanAuthor = { login: 'khaliqgant', type: 'User' }

  it('writes an issue comment through the connection and confirms GitHub recorded the app as its author', async () => {
    const mount = new CommentMount(appAuthor)
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadAttempts: 1, receiptReadDelayMs: 0 })

    await expect(write.postIssueComment({ repo, number: 221, body: 'babysitter status' }))
      .resolves.toEqual({ repo, number: 221, commentId: 9001, author: 'app' })

    const comment = mount.writes.find((write) => write.path.includes('/issues/221/comments/'))
    expect(comment?.path).toMatch(
      /^\/github\/repos\/AgentWorkforce\/factory\/issues\/221\/comments\/factory-[0-9a-f]{12}-\d+\.json$/u,
    )
    expect(comment?.content).toMatchObject({ body: 'babysitter status' })
    // No `gh` anywhere: the only side effect is a mount write.
    expect(mount.writes).toHaveLength(1)
  })

  it('refuses to report app authorship when GitHub recorded a human author', async () => {
    const mount = new CommentMount(humanAuthor)
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadAttempts: 1, receiptReadDelayMs: 0 })

    await expect(write.postIssueComment({ repo, number: 221, body: 'babysitter status' }))
      .rejects.toThrow(/authorship check failed.*khaliqgant/u)
  })

  it('refuses to report app authorship when the author cannot be read back at all', async () => {
    const mount = new CommentMount(undefined)
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadAttempts: 1, receiptReadDelayMs: 0 })

    await expect(write.postIssueComment({ repo, number: 221, body: 'babysitter status' }))
      .rejects.toThrow(/authorship check failed.*unavailable/u)
  })

  it('threads a review reply onto the original comment through the connection', async () => {
    const mount = new CommentMount(appAuthor)
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadAttempts: 1, receiptReadDelayMs: 0 })

    await expect(write.replyToReviewComment({ repo, number: 221, inReplyTo: 4242, body: 'fixed in abc123' }))
      .resolves.toMatchObject({ commentId: 9001, author: 'app' })

    expect(mount.writes[0]?.path).toMatch(
      /^\/github\/repos\/AgentWorkforce\/factory\/pulls\/221\/review-comments\/4242\/replies\/factory-[0-9a-f]{12}-\d+\.json$/u,
    )
    // The reply target is carried by the path segment, not the payload: the
    // adapter's replies route derives `in_reply_to` from `.../review-comments/
    // <id>/replies/`, and its payload parser accepts only `body`.
    expect(mount.writes[0]?.content).toMatchObject({ body: 'fixed in abc123' })
  })

  it('gives two identical comment bodies on one thread distinct draft paths', async () => {
    const mount = new CommentMount(appAuthor)
    const write = new RelayfileGithubConnectionWrite({ mount, receiptReadAttempts: 1, receiptReadDelayMs: 0 })

    await write.postIssueComment({ repo, number: 221, body: 'ping' })
    await write.postIssueComment({ repo, number: 221, body: 'ping' })

    expect(new Set(mount.writes.map((write) => write.path)).size).toBe(2)
  })
})
