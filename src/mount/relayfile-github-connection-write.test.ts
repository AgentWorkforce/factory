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
  it('requests CodeRabbit review once through the connected app write path', async () => {
    const mount = new FakeMountClient()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })
    const input = { repo: 'AgentWorkforce/factory', number: 85 }

    await Promise.all([
      write.requestPullRequestReview(input),
      write.requestPullRequestReview(input),
    ])

    const draftPath = '/github/repos/AgentWorkforce/factory/pulls/85/comments/factory-coderabbit-review.json'
    expect(mount.writes).toEqual([{
      path: draftPath,
      content: { body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->' },
    }])

    mount.files.delete(draftPath)
    mount.files.set('/github/repos/AgentWorkforce/factory/pulls/85__title/comments/9001.json', {
      content: {
        payload: {
          comment: {
            body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
          },
        },
      },
    })
    const restarted = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })
    await restarted.requestPullRequestReview(input)
    expect(mount.writes).toHaveLength(1)
  })

  it('treats missing fresh-PR comment trees as empty and posts the first request', async () => {
    class MissingCommentTreesMount extends FakeMountClient {
      override async listTree(): Promise<string[]> {
        throw Object.assign(new Error('not found'), { status: 404 })
      }
    }
    const mount = new MissingCommentTreesMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 85,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([{
      path: '/github/repos/AgentWorkforce/factory/pulls/85/comments/factory-coderabbit-review.json',
      content: { body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->' },
    }])
  })

  it('propagates non-not-found tree scan failures without posting a request', async () => {
    class FailedCommentTreeMount extends FakeMountClient {
      override async listTree(): Promise<string[]> {
        throw Object.assign(new Error('service unavailable'), { status: 503 })
      }
    }
    const mount = new FailedCommentTreeMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 85,
    })).rejects.toThrow('service unavailable')

    expect(mount.writes).toEqual([])
  })

  it('retries a review request after a failed provider confirmation', async () => {
    const draftPath = '/github/repos/AgentWorkforce/factory/pulls/86/comments/factory-coderabbit-review.json'
    class FailedThenAcknowledgedMount extends FakeMountClient {
      confirmationCount = 0

      override async confirmWrite(path: string): Promise<'acked' | 'failed'> {
        if (path !== draftPath) return 'acked'
        this.confirmationCount += 1
        return this.confirmationCount < 3 ? 'failed' : 'acked'
      }
    }
    const mount = new FailedThenAcknowledgedMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })
    const input = { repo: 'AgentWorkforce/factory', number: 86 }

    await expect(write.requestPullRequestReview(input))
      .rejects.toThrow(`GitHub writeback did not complete for ${draftPath}: failed`)

    await expect(write.requestPullRequestReview(input)).resolves.toBeUndefined()
    expect(mount.writes.filter((entry) => entry.path === draftPath)).toHaveLength(2)
    expect(mount.deletes).toEqual([draftPath])
  })

  it('recognizes an acknowledged review draft before provider reconciliation', async () => {
    const draftPath = '/github/repos/AgentWorkforce/factory/pulls/86/comments/factory-coderabbit-review.json'
    const mount = new FakeMountClient({
      [draftPath]: {
        body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 86,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([])
    expect(mount.deletes).toEqual([])
  })

  it('gives restarted cloud operation recovery a realistic bounded confirmation window', async () => {
    const draftPath = '/github/repos/AgentWorkforce/factory/pulls/86/comments/factory-coderabbit-review.json'
    class ConfirmationOptionsMount extends FakeMountClient {
      readonly options: Array<{ timeoutMs?: number; returnFailed?: boolean } | undefined> = []

      override async confirmWrite(
        _path: string,
        opts?: { timeoutMs?: number; returnFailed?: boolean },
      ): Promise<'acked'> {
        this.options.push(opts)
        return 'acked'
      }
    }
    const mount = new ConfirmationOptionsMount({
      [draftPath]: {
        body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 86,
    })).resolves.toBeUndefined()

    expect(mount.options).toEqual([{
      timeoutMs: 10_000,
      returnFailed: true,
    }])
  })

  it('fails closed on an indeterminate review draft instead of posting a duplicate', async () => {
    const draftPath = '/github/repos/AgentWorkforce/factory/pulls/86/comments/factory-coderabbit-review.json'
    const mount = new FakeMountClient({
      [draftPath]: {
        body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
      },
    })
    mount.setConfirmWrite(draftPath, 'timeout')
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 86,
    })).rejects.toThrow(
      `GitHub review request draft has indeterminate provider status for ${draftPath}: timeout`,
    )

    expect(mount.writes).toEqual([])
    expect(mount.deletes).toEqual([])
  })

  it('fails an indeterminate comment scan instead of posting a duplicate request', async () => {
    const stalePath = '/github/repos/AgentWorkforce/factory/pulls/87__renamed/comments/9001.json'
    class RacingMount extends FakeMountClient {
      readonly listPrefixes: string[] = []
      failRead = true

      override async listTree(prefix: string): Promise<string[]> {
        this.listPrefixes.push(prefix)
        if (prefix.endsWith('/pulls')) {
          return ['/github/repos/AgentWorkforce/factory/pulls/87__renamed']
        }
        if (prefix.endsWith('/pulls/87__renamed/comments')) return [stalePath]
        return []
      }

      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        if (path === stalePath && this.failRead) throw new Error('reconciled path moved')
        return {
          content: {
            payload: {
              comment: {
                body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
              },
            },
          },
        }
      }
    }
    const mount = new RacingMount()
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })
    const input = { repo: 'AgentWorkforce/factory', number: 87 }

    await expect(write.requestPullRequestReview(input)).rejects.toThrow('reconciled path moved')
    expect(mount.writes).toEqual([])

    mount.failRead = false
    await expect(write.requestPullRequestReview(input)).resolves.toBeUndefined()
    expect(mount.writes).toEqual([])
    expect(mount.listPrefixes).toEqual([
      '/github/repos/AgentWorkforce/factory/pulls',
      '/github/repos/AgentWorkforce/factory/pulls/87/comments',
      '/github/repos/AgentWorkforce/factory/pulls/87__renamed/comments',
      '/github/repos/AgentWorkforce/factory/pulls',
      '/github/repos/AgentWorkforce/factory/pulls/87/comments',
      '/github/repos/AgentWorkforce/factory/pulls/87__renamed/comments',
    ])
  })

  it('finds a reconciled request in nested comment metadata', async () => {
    const nestedPath =
      '/github/repos/AgentWorkforce/factory/pulls/88__renamed/comments/9002/meta.json'
    const mount = new FakeMountClient({
      [nestedPath]: {
        payload: {
          comment: {
            body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
          },
        },
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 88,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([])
    expect(mount.reads).toEqual([nestedPath])
  })

  it('does not let a marker-only mounted comment suppress the review command', async () => {
    const markerOnlyPath =
      '/github/repos/AgentWorkforce/factory/pulls/88__renamed/comments/9002.json'
    const mount = new FakeMountClient({
      [markerOnlyPath]: {
        body: '<!-- factory-coderabbit-review-request -->',
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 88,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([{
      path: '/github/repos/AgentWorkforce/factory/pulls/88/comments/factory-coderabbit-review.json',
      content: { body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->' },
    }])
  })

  it('finds a reconciled request in the flat repository layout', async () => {
    const flatPath =
      '/github/repos/AgentWorkforce__factory/pulls/89__renamed/comments/9003.json'
    const mount = new FakeMountClient({
      [flatPath]: {
        repository: { full_name: 'AgentWorkforce/factory' },
        pull_request: { number: 89 },
        comment: {
          body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
        },
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 89,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([])
    expect(mount.reads).toEqual([flatPath])
  })

  it('finds a request in the canonical repository-level comment layout after restart', async () => {
    const canonicalPath =
      '/github/repos/AgentWorkforce/factory/comments/9004.json'
    const unrelatedPath =
      '/github/repos/AgentWorkforce/factory/comments/9003.json'
    const mount = new FakeMountClient({
      [unrelatedPath]: {
        repository: { full_name: 'AgentWorkforce/other' },
        pull_request: { number: 90 },
        comment: {
          body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
        },
      },
      [canonicalPath]: {
        repository: { full_name: 'AgentWorkforce/factory' },
        pull_request: { number: 90 },
        comment: {
          body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
        },
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 90,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([])
    expect(mount.reads).toEqual([unrelatedPath, canonicalPath])
  })

  it('does not let another pull request canonical comment suppress a request', async () => {
    const canonicalPath =
      '/github/repos/AgentWorkforce/factory/comments/9006.json'
    const mount = new FakeMountClient({
      [canonicalPath]: {
        repository: { full_name: 'AgentWorkforce/factory' },
        pull_request: { number: 91 },
        comment: {
          body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->',
        },
      },
    })
    const write = new RelayfileGithubConnectionWrite({ mount, gitRunner: gitRunner() })

    await expect(write.requestPullRequestReview({
      repo: 'AgentWorkforce/factory',
      number: 92,
    })).resolves.toBeUndefined()

    expect(mount.writes).toEqual([{
      path: '/github/repos/AgentWorkforce/factory/pulls/92/comments/factory-coderabbit-review.json',
      content: { body: '@coderabbitai review\n<!-- factory-coderabbit-review-request -->' },
    }])
  })

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
})
