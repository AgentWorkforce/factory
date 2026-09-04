import { execFile } from 'node:child_process'
import { mkdtemp, mkdir, rm, stat, symlink, writeFile } from 'node:fs/promises'
import { basename, dirname, join } from 'node:path'
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

  it('refuses a pre-created branch owned by a different issue', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-issue-collision-'))
    const base = join(root, 'CloudCheckout')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# cloud\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])
      await git(base, ['branch', 'factory/3022-chief-org-live-population'])

      const manager = new GitAgentWorktreeManager()
      const worktreePath = factoryWorktreePath(base, '3021', 'AgentWorkforce/cloud', 'collision')
      await expect(manager.prepare({
        repo: 'AgentWorkforce/cloud',
        issueKey: '3021',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/3022-chief-org-live-population',
      })).rejects.toThrow(
        'Refusing Software Garden worktree branch factory/3022-chief-org-live-population: it does not belong to dispatched issue 3021',
      )
      await expect(stat(worktreePath)).rejects.toMatchObject({ code: 'ENOENT' })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses cleanup outside the Software Garden worktree root', async () => {
    const manager = new GitAgentWorktreeManager()
    const unsafe = {
      repo: 'AgentWorkforce/pear',
      issueKey: 'AR-33',
      baseClonePath: '/work/pear',
      worktreePath: '/work/pear',
      branch: 'factory/ar-33-pear',
    }
    await expect(manager.cleanup(unsafe)).rejects.toThrow(/unsafe Software Garden worktree path/u)
    await expect(manager.inspectForCleanup(unsafe)).rejects.toThrow(/unsafe Software Garden worktree path/u)
  })

  it('refuses a Factory-root symlink that resolves to a registered checkout outside the root', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-symlink-safety-'))
    const base = join(root, 'PearCheckout')
    const outside = join(root, 'outside-worktree')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# pear\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])
      const worktreePath = factoryWorktreePath(base, 'AR-34', 'AgentWorkforce/pear', 'escape01')
      await git(base, ['worktree', 'add', '-b', 'factory/ar-34-pear-escape01', outside])
      await mkdir(dirname(worktreePath), { recursive: true })
      await symlink(outside, worktreePath, 'dir')
      const worktree = {
        repo: 'AgentWorkforce/pear',
        issueKey: 'AR-34',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/ar-34-pear-escape01',
      }
      const manager = new GitAgentWorktreeManager()

      await expect(manager.inspectForCleanup(worktree)).rejects.toThrow(/resolved target is/u)
      await expect(manager.cleanup(worktree)).rejects.toThrow(/resolved target is/u)
      await expect(stat(outside)).resolves.toMatchObject({})
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses cleanup when the Factory checkout root itself redirects outside the owned tree', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-root-symlink-safety-'))
    const base = join(root, 'PearCheckout')
    const outsideRoot = join(root, 'outside-root')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# pear\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])
      const worktreePath = factoryWorktreePath(base, 'AR-35', 'AgentWorkforce/pear', 'escape02')
      const expectedRoot = dirname(worktreePath)
      const outside = join(outsideRoot, basename(worktreePath))
      await mkdir(outsideRoot)
      await git(base, ['worktree', 'add', '-b', 'factory/ar-35-pear-escape02', outside])
      await mkdir(dirname(expectedRoot), { recursive: true })
      await symlink(outsideRoot, expectedRoot, 'dir')
      const worktree = {
        repo: 'AgentWorkforce/pear',
        issueKey: 'AR-35',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/ar-35-pear-escape02',
      }
      const manager = new GitAgentWorktreeManager()

      await expect(manager.inspectForCleanup(worktree)).rejects.toThrow(/symbolic-link root/u)
      await expect(manager.cleanup(worktree)).rejects.toThrow(/symbolic-link root/u)
      await expect(stat(outside)).resolves.toMatchObject({})
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses parent cleanup through a symbolic-link Factory container when the checkout is missing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-container-symlink-safety-'))
    const base = join(root, 'PearCheckout')
    const outsideFactory = join(root, 'outside-factory')
    try {
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      const worktreePath = factoryWorktreePath(base, 'AR-36', 'AgentWorkforce/pear', 'escape03')
      const expectedRoot = dirname(worktreePath)
      const factoryContainer = dirname(expectedRoot)
      const outsideCheckout = join(outsideFactory, basename(expectedRoot))
      await mkdir(outsideCheckout, { recursive: true })
      await symlink(outsideFactory, factoryContainer, 'dir')
      const worktree = {
        repo: 'AgentWorkforce/pear',
        issueKey: 'AR-36',
        baseClonePath: base,
        worktreePath,
        branch: 'factory/ar-36-pear-escape03',
      }
      const manager = new GitAgentWorktreeManager()

      await expect(manager.inspectForCleanup(worktree)).rejects.toThrow(/symbolic-link root/u)
      await expect(manager.cleanup(worktree)).rejects.toThrow(/symbolic-link root/u)
      await expect(stat(outsideCheckout)).resolves.toMatchObject({})
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('discovers every run and detects dirty, unpushed, and locked worktrees before cleanup', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-worktree-safety-'))
    const remote = join(root, 'remote.git')
    const base = join(root, 'PearCheckout')
    try {
      await git(root, ['init', '--bare', remote])
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# pear\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])
      await git(base, ['remote', 'add', 'origin', remote])
      await git(base, ['push', '-u', 'origin', 'main'])

      const manager = new GitAgentWorktreeManager()
      const create = async (runId: string) => {
        const worktreePath = factoryWorktreePath(base, 'AR-123', 'AgentWorkforce/pear', runId)
        const worktree = {
          repo: 'AgentWorkforce/pear',
          issueKey: 'AR-123',
          baseClonePath: base,
          worktreePath,
          branch: `factory/ar-123-pear-${runId}`,
        }
        await manager.prepare(worktree)
        return worktree
      }
      const clean = await create('11111111')
      const dirty = await create('22222222')
      const unpushed = await create('33333333')
      const locked = await create('44444444')
      await writeFile(join(dirty.worktreePath, 'dirty.txt'), 'local work\n', 'utf8')
      await writeFile(join(unpushed.worktreePath, 'commit.txt'), 'not pushed\n', 'utf8')
      await git(unpushed.worktreePath, ['add', 'commit.txt'])
      await git(unpushed.worktreePath, ['commit', '-m', 'local only'])
      await git(base, ['worktree', 'lock', locked.worktreePath])

      const discovered = await manager.listWorktrees({ repo: 'AgentWorkforce/pear', baseClonePath: base })
      expect(discovered.map((worktree) => worktree.worktreePath)).toEqual([
        clean.worktreePath,
        dirty.worktreePath,
        unpushed.worktreePath,
        locked.worktreePath,
      ])
      expect(discovered.every((worktree) => worktree.issueKey === 'ar-123')).toBe(true)

      await expect(manager.inspectForCleanup(clean)).resolves.toMatchObject({ retentionReasons: [] })
      await expect(manager.inspectForCleanup(dirty)).resolves.toMatchObject({
        retentionReasons: expect.arrayContaining(['uncommitted changes']),
      })
      await expect(manager.inspectForCleanup(unpushed)).resolves.toMatchObject({
        retentionReasons: expect.arrayContaining(['1 unpushed commit']),
      })
      const lockedInspection = await manager.inspectForCleanup(locked)
      expect(lockedInspection.retentionReasons.some((reason) => reason.startsWith('git lock present'))).toBe(true)

      await manager.cleanup(clean)
      await expect(stat(clean.worktreePath)).rejects.toMatchObject({ code: 'ENOENT' })
      await expect(stat(dirty.worktreePath)).resolves.toMatchObject({})
      await expect(stat(unpushed.worktreePath)).resolves.toMatchObject({})
      await expect(stat(locked.worktreePath)).resolves.toMatchObject({})
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('checks out a verified numeric legacy PR head from origin without synthesizing it from base', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-agent-legacy-pr-worktree-'))
    const remote = join(root, 'remote.git')
    const base = join(root, 'HoopsheetCheckout')
    try {
      await git(root, ['init', '--bare', remote])
      await mkdir(base)
      await git(base, ['init', '-b', 'main'])
      await git(base, ['config', 'user.email', 'factory@example.test'])
      await git(base, ['config', 'user.name', 'Factory Test'])
      await writeFile(join(base, 'README.md'), '# hoopsheet\n', 'utf8')
      await git(base, ['add', 'README.md'])
      await git(base, ['commit', '-m', 'initial'])
      await git(base, ['remote', 'add', 'origin', remote])
      await git(base, ['push', '-u', 'origin', 'main'])
      await git(base, ['switch', '-c', '26-league-schedule-timezone'])
      await writeFile(join(base, 'timezone.md'), 'legacy PR head\n', 'utf8')
      await git(base, ['add', 'timezone.md'])
      await git(base, ['commit', '-m', 'fix schedule timezone'])
      const remoteHead = (await git(base, ['rev-parse', 'HEAD'])).trim()
      await git(base, ['push', 'origin', '26-league-schedule-timezone'])
      await git(base, ['switch', 'main'])
      const manager = new GitAgentWorktreeManager()
      const worktreePath = factoryWorktreePath(base, '26', 'AgentWorkforce/hoopsheet', 'legacy26')
      const worktree = {
        repo: 'AgentWorkforce/hoopsheet',
        issueKey: '26',
        baseClonePath: base,
        worktreePath,
        branch: '26-league-schedule-timezone',
        existingPullRequestBranch: true,
      }
      const manualWorktree = join(root, 'manual-hoopsheet-wt-26')
      await git(base, ['worktree', 'add', manualWorktree, '26-league-schedule-timezone'])

      await expect(manager.prepare(worktree)).rejects.toThrow(
        /already checked out.*Software Garden will not reuse or remove a checkout outside/u,
      )
      expect(await git(manualWorktree, ['branch', '--show-current']))
        .toBe('26-league-schedule-timezone\n')

      await git(base, ['worktree', 'remove', manualWorktree])
      await git(base, ['branch', '-D', '26-league-schedule-timezone'])

      await manager.prepare(worktree)

      expect(await git(worktreePath, ['branch', '--show-current']))
        .toBe('26-league-schedule-timezone\n')
      expect((await git(worktreePath, ['rev-parse', 'HEAD'])).trim()).toBe(remoteHead)
      expect(await git(worktreePath, ['rev-parse', '--abbrev-ref', '@{upstream}']))
        .toBe('origin/26-league-schedule-timezone\n')
      await expect(stat(join(worktreePath, 'timezone.md'))).resolves.toMatchObject({})

      await manager.cleanup(worktree)
      await expect(stat(worktreePath)).rejects.toMatchObject({ code: 'ENOENT' })

      await git(base, ['branch', '-f', '26-league-schedule-timezone', 'main'])
      await expect(manager.prepare(worktree)).rejects.toThrow(/Refusing to overwrite divergent local PR branch/u)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects a numeric legacy branch unless Factory authorized the matching existing PR head', async () => {
    const manager = new GitAgentWorktreeManager()
    const base = '/work/hoopsheet'
    const worktreePath = '/work/.factory-worktrees/hoopsheet/26-hoopsheet-legacy'

    await expect(manager.prepare({
      repo: 'AgentWorkforce/hoopsheet',
      issueKey: '26',
      baseClonePath: base,
      worktreePath,
      branch: '26-league-schedule-timezone',
    })).rejects.toThrow(/unsafe Software Garden worktree branch/u)

    await expect(manager.prepare({
      repo: 'AgentWorkforce/hoopsheet',
      issueKey: '27',
      baseClonePath: base,
      worktreePath,
      branch: '26-league-schedule-timezone',
      existingPullRequestBranch: true,
    })).rejects.toThrow(/unsafe Software Garden worktree branch/u)
  })

  it('ignores an unrelated deleted locked registration when validating an existing checkout', async () => {
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
      await git(base, ['worktree', 'lock', stalePath])
      await rm(stalePath, { recursive: true, force: true })

      await expect(manager.prepare(worktree)).resolves.toBeUndefined()
      await expect(manager.inspectForCleanup(worktree)).resolves.toMatchObject({
        retentionReasons: ['1 unpushed commit'],
      })
      expect(await git(worktreePath, ['branch', '--show-current']))
        .toBe('factory/ar-34-pear-abcdefgh\n')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
