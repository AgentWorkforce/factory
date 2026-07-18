import { execFile } from 'node:child_process'
import { mkdtemp, mkdir, rm, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { promisify } from 'node:util'
import { describe, expect, it } from 'vitest'

import { factoryWorktreePath, GitAgentWorktreeManager } from './agent-worktree'

const execFileAsync = promisify(execFile)
const git = async (cwd: string, args: string[]): Promise<string> =>
  (await execFileAsync('git', ['-C', cwd, ...args], { encoding: 'utf8' })).stdout

describe('GitAgentWorktreeManager', () => {
  it('prepares one idempotent issue worktree and removes it after release', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-'))
    const base = join(root, 'PearCheckout')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# pear\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])

      const manager = new GitAgentWorktreeManager()
      const worktreePath = factoryWorktreePath(base, 'AR-33', 'AgentWorkforce/pear', '12345678-rest')
      const worktree = {
        repo: 'AgentWorkforce/pear',
        issueKey: 'AR-33',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/ar-33-pear-12345678',
      }

      await Promise.all([manager.prepare(worktree), manager.prepare(worktree)])

      await expect(git(worktreePath, ['branch', '--show-current']))
        .resolves.toBe('factory/ar-33-pear-12345678\n')
      expect((await git(base, ['worktree', 'list', '--porcelain'])).match(/^worktree /gmu)).toHaveLength(2)

      await manager.cleanup(worktree)

      await expect(stat(worktreePath)).rejects.toMatchObject({ code: 'ENOENT' })
      await expect(stat(join(root, '.factory-worktrees'))).rejects.toMatchObject({ code: 'ENOENT' })
      expect((await git(base, ['worktree', 'list', '--porcelain'])).match(/^worktree /gmu)).toHaveLength(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses cleanup outside the Factory worktree root', async () => {
    const manager = new GitAgentWorktreeManager()
    await expect(manager.cleanup({
      repo: 'AgentWorkforce/pear',
      issueKey: 'AR-33',
      baseClonePath: '/work/pear',
      worktreePath: '/work/pear',
      branch: 'factory/ar-33-pear',
    })).rejects.toThrow(/unsafe Factory worktree path/u)
  })

  it('prunes an unrelated deleted registration before validating an existing checkout', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-prune-'))
    const base = join(root, 'pear')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# pear\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])

      const manager = new GitAgentWorktreeManager()
      const worktreePath = factoryWorktreePath(base, 'AR-34', 'AgentWorkforce/pear', 'abcdefgh-rest')
      const worktree = {
        repo: 'AgentWorkforce/pear',
        issueKey: 'AR-34',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/ar-34-pear-abcdefgh',
      }
      await manager.prepare(worktree)

      const stalePath = join(root, 'deleted-unrelated-worktree')
      await git(base, ['worktree', 'add', '-b', 'unrelated-stale', stalePath, 'main'])
      await rm(stalePath, { recursive: true, force: true })

      await expect(manager.prepare(worktree)).resolves.toBeUndefined()
      expect(await git(worktreePath, ['branch', '--show-current']))
        .toBe('factory/ar-34-pear-abcdefgh\n')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
