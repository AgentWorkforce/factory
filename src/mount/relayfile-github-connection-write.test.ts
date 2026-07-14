import { describe, expect, it, vi } from 'vitest'

import { FakeMountClient } from '../testing'
import { RelayfileGithubConnectionWrite, type GitCommandRunner } from './relayfile-github-connection-write'

const gitRunner = (): GitCommandRunner => vi.fn(async (args) => {
  if (args.includes('symbolic-ref')) return { stdout: 'fix/issue-52\n' }
  if (args.includes('rev-parse')) return { stdout: '1234567890abcdef1234567890abcdef12345678\n' }
  throw new Error(`unexpected git args: ${args.join(' ')}`)
})

describe('RelayfileGithubConnectionWrite', () => {
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
        path: `/github/repos/AgentWorkforce/factory/refs/${draft}.json`,
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
    const refPath = '/github/repos/AgentWorkforce/factory/refs/factory-fix-issue-52-1234567890ab.json'
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
})
