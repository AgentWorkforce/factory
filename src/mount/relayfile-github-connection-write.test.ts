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
      override async confirmWrite(path: string): Promise<'acked'> {
        if (path === createRefPath) {
          throw new Error('GitHub writeback failed with status 422: Reference already exists')
        }
        return 'acked'
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
})
