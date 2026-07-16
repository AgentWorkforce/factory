import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, realpath, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { promisify } from 'node:util'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { githubRepoFromRemoteUrl, resolveLocalFactoryConfig } from './local-clone-paths'

const execFileAsync = promisify(execFile)
const tempRoots: string[] = []

afterEach(async () => {
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('resolveLocalFactoryConfig', () => {
  it('infers the git top-level from a nested cwd when the resolved route remote matches', async () => {
    const root = await gitRepo('git@github.com:AgentWorkforce/factory.git')
    const nested = join(root, 'src', 'nested')
    await mkdir(nested, { recursive: true })
    const logger = { info: vi.fn() }

    const config = await resolveLocalFactoryConfig({
      repos: {
        org: 'WrongOrg',
        names: ['factory'],
        overrides: { factory: 'OverrideOrg/factory' },
        byLabel: { factory: 'AgentWorkforce/factory' },
      },
    }, { cwd: nested, logger })

    const checkoutRoot = await realpath(root)
    expect(config.clonePaths).toEqual({ 'AgentWorkforce/factory': checkoutRoot })
    expect(config.repos.clonePaths).toEqual(config.clonePaths)
    expect(logger.info).toHaveBeenCalledOnce()
    expect(logger.info).toHaveBeenCalledWith(`[factory] clonePath inferred from cwd: ${checkoutRoot}`)
  })

  it('infers from a split config only when neither half supplies clone path config', async () => {
    const root = await gitRepo('https://github.com/AgentWorkforce/factory')

    const config = await resolveLocalFactoryConfig({
      workspaceConfig: {
        repos: { org: 'AgentWorkforce', names: ['factory'] },
      },
      nodeConfig: { capabilities: ['spawn:codex'] },
    }, { cwd: root })

    expect(config.clonePaths).toEqual({ 'AgentWorkforce/factory': await realpath(root) })
  })

  it.each([
    'https://github.com/AgentWorkforce/factory.git',
    'https://github.com/agentworkforce/FACTORY/',
    'git@github.com:AgentWorkforce/factory.git',
    'ssh://git@github.com/AgentWorkforce/factory.git',
    'git://github.com/AgentWorkforce/factory.git',
  ])('accepts a matching GitHub remote URL: %s', async (remote) => {
    const root = await gitRepo(remote)

    const config = await resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: root })

    expect(config.clonePaths['AgentWorkforce/factory']).toBe(await realpath(root))
  })

  it('rejects cwd inference when cwd is not a git checkout', async () => {
    const root = await tempDir('factory-not-git-')

    await expect(resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: root })).rejects.toThrow(
      `cwd is not inside a git checkout: ${resolve(root)}; configure cloneRoot or clonePaths explicitly`,
    )
  })

  it('rejects cwd inference when the checkout has no remote', async () => {
    const root = await gitRepo()

    await expect(resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: root })).rejects.toThrow('has no remotes; add a matching GitHub remote')
  })

  it('rejects cwd inference when no GitHub remote matches the resolved repo', async () => {
    const root = await gitRepo('https://github.com/OtherOrg/other.git')

    await expect(resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: root })).rejects.toThrow(
      'cannot infer clonePath for AgentWorkforce/factory: found GitHub remote for OtherOrg/other',
    )
  })

  it('rejects cwd inference when remotes are not parseable GitHub repositories', async () => {
    const root = await gitRepo('https://gitlab.com/AgentWorkforce/factory.git')

    await expect(resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: root })).rejects.toThrow('found no parseable GitHub remote URLs')
  })

  it.each([
    {
      name: 'repos.names is absent',
      input: { repos: { byLabel: { factory: 'AgentWorkforce/factory' } } },
    },
    {
      name: 'repos.names has multiple entries',
      input: { repos: { org: 'AgentWorkforce', names: ['factory', 'cloud'] } },
    },
    {
      name: 'top-level cloneRoot is supplied',
      input: { cloneRoot: '/work', repos: { org: 'AgentWorkforce', names: ['factory'] } },
    },
    {
      name: 'top-level clonePaths is supplied',
      input: { clonePaths: {}, repos: { org: 'AgentWorkforce', names: ['factory'] } },
    },
    {
      name: 'legacy cloneRoot is supplied',
      input: { repos: { org: 'AgentWorkforce', names: ['factory'], cloneRoot: '/work' } },
    },
    {
      name: 'legacy clonePaths is supplied',
      input: { repos: { org: 'AgentWorkforce', names: ['factory'], clonePaths: {} } },
    },
  ])('does not inspect cwd when $name', async ({ input }) => {
    const git = vi.fn(async () => {
      throw new Error('git must not run')
    })

    await resolveLocalFactoryConfig(input, { cwd: '/not/a/repo', git })

    expect(git).not.toHaveBeenCalled()
  })

  it('does not infer when environment-dependent inference is disabled', async () => {
    const git = vi.fn(async () => {
      throw new Error('git must not run')
    })

    const config = await resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: '/not/a/repo', git, inferFromCwd: false })

    expect(config.clonePaths).toEqual({})
    expect(git).not.toHaveBeenCalled()
  })

  it('does not infer from a split config when node-local path config is supplied', async () => {
    const git = vi.fn(async () => {
      throw new Error('git must not run')
    })

    const config = await resolveLocalFactoryConfig({
      workspaceConfig: {
        repos: { org: 'AgentWorkforce', names: ['factory'] },
      },
      nodeConfig: { clonePaths: {} },
    }, { cwd: '/not/a/repo', git })

    expect(config.clonePaths).toEqual({})
    expect(git).not.toHaveBeenCalled()
  })

  it('validates configured paths through git and accepts linked worktrees', async () => {
    const source = await gitRepo('https://github.com/AgentWorkforce/factory.git', true)
    const worktree = await tempDir('factory-worktree-')
    await rm(worktree, { recursive: true, force: true })
    await git(source, ['worktree', 'add', '-b', 'test-worktree', worktree])

    const inferred = await resolveLocalFactoryConfig({
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { cwd: worktree })
    expect(inferred.clonePaths['AgentWorkforce/factory']).toBe(await realpath(worktree))

    const config = await resolveLocalFactoryConfig({
      clonePaths: { 'AgentWorkforce/factory': worktree },
      repos: { org: 'AgentWorkforce', names: ['factory'] },
    }, { validateConfiguredCheckouts: true })

    expect(config.clonePaths['AgentWorkforce/factory']).toBe(worktree)
  })

  it('fails fast for a configured path that is not a checkout root', async () => {
    const root = await gitRepo('https://github.com/AgentWorkforce/factory.git')
    const nested = join(root, 'nested')
    await mkdir(nested)

    await expect(resolveLocalFactoryConfig({
      clonePaths: { 'AgentWorkforce/factory': nested },
      repos: { byLabel: { factory: 'AgentWorkforce/factory' } },
    }, { validateConfiguredCheckouts: true })).rejects.toThrow('points inside a git checkout')
  })

  it('fails fast for missing and non-git configured paths', async () => {
    const root = await tempDir('factory-invalid-checkout-')

    await expect(resolveLocalFactoryConfig({
      clonePaths: { 'AgentWorkforce/factory': join(root, 'missing') },
      repos: {},
    }, { validateConfiguredCheckouts: true })).rejects.toThrow('does not exist')

    await expect(resolveLocalFactoryConfig({
      clonePaths: { 'AgentWorkforce/factory': root },
      repos: {},
    }, { validateConfiguredCheckouts: true })).rejects.toThrow('is not a git checkout')
  })
})

describe('githubRepoFromRemoteUrl', () => {
  it.each([
    ['https://github.com/AgentWorkforce/factory.git', 'AgentWorkforce/factory'],
    ['git@github.com:AgentWorkforce/factory.git', 'AgentWorkforce/factory'],
    ['ssh://git@github.com/AgentWorkforce/factory/', 'AgentWorkforce/factory'],
    ['github.com/AgentWorkforce/factory', 'AgentWorkforce/factory'],
  ])('normalizes %s', (remote, expected) => {
    expect(githubRepoFromRemoteUrl(remote)).toBe(expected)
  })

  it.each([
    'https://gitlab.com/AgentWorkforce/factory.git',
    'git@github.com:AgentWorkforce/factory/extra.git',
    'not-a-remote',
  ])('rejects %s', (remote) => {
    expect(githubRepoFromRemoteUrl(remote)).toBeUndefined()
  })
})

async function gitRepo(remote?: string, commit = false): Promise<string> {
  const root = await tempDir('factory-git-')
  await git(root, ['init'])
  if (remote) await git(root, ['remote', 'add', 'origin', remote])
  if (commit) {
    await git(root, [
      '-c', 'user.name=Factory Test',
      '-c', 'user.email=factory@example.com',
      'commit', '--allow-empty', '-m', 'initial',
    ])
  }
  return root
}

async function tempDir(prefix: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), prefix))
  tempRoots.push(root)
  return root
}

async function git(cwd: string, args: string[]): Promise<void> {
  await execFileAsync('git', ['-C', cwd, ...args])
}
