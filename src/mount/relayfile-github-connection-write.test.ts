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

  it('provisions and mutates lifecycle labels through unique confirmed operation drafts', async () => {
    const mount = new FakeMountClient()
    const operationIds = [
      '11111111-1111-4111-8111-111111111111',
      '22222222-2222-4222-8222-222222222222',
      '33333333-3333-4333-8333-333333333333',
    ]
    const write = new RelayfileGithubConnectionWrite({
      mount,
      operationIdFactory: () => operationIds.shift()!,
    })

    await write.ensureRepositoryLabel({
      repo: 'AgentWorkforce/factory',
      name: 'factory:in-progress',
      color: '#1D76DB',
      description: 'Factory agents are working on this issue.',
      author: 'app',
    })
    const addReceipt = await write.mutateIssueLabel({
      repo: 'AgentWorkforce/factory',
      number: 221,
      operation: 'add',
      label: 'factory:in-progress',
      author: 'app',
    })
    const removeReceipt = await write.mutateIssueLabel({
      repo: 'AgentWorkforce/factory',
      number: 221,
      operation: 'remove',
      label: 'factory:human-review',
      author: 'app',
    })

    expect(addReceipt).toBe('acknowledged')
    expect(removeReceipt).toBe('acknowledged')

    expect(mount.writes).toEqual([
      {
        path: '/github/repos/AgentWorkforce/factory/labels/factory-11111111-1111-4111-8111-111111111111.json',
        content: {
          name: 'factory:in-progress',
          color: '1d76db',
          description: 'Factory agents are working on this issue.',
        },
      },
      {
        path: '/github/repos/AgentWorkforce/factory/issues/221/labels/factory-22222222-2222-4222-8222-222222222222.json',
        content: { operation: 'add', labels: ['factory:in-progress'] },
      },
      {
        path: '/github/repos/AgentWorkforce/factory/issues/221/labels/factory-33333333-3333-4333-8333-333333333333.json',
        content: { operation: 'remove', label: 'factory:human-review' },
      },
    ])
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

  it('does not report a lifecycle-label operation when provider confirmation remains pending', async () => {
    class PendingLabelMount extends FakeMountClient {
      override async confirmWrite(path: string): Promise<'acked' | 'pending'> {
        return path.includes('/labels/') ? 'pending' : 'acked'
      }
    }
    const mount = new PendingLabelMount()
    const write = new RelayfileGithubConnectionWrite({
      mount,
      operationIdFactory: () => '11111111-1111-4111-8111-111111111111',
    })

    await expect(write.mutateIssueLabel({
      repo: 'AgentWorkforce/factory',
      number: 221,
      operation: 'add',
      label: 'factory:in-progress',
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
