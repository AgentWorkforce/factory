import { execFile } from 'node:child_process'
import { realpath, stat } from 'node:fs/promises'
import { resolve } from 'node:path'
import { promisify } from 'node:util'

import { loadFactoryConfig, type FactoryConfig } from './schema'

const execFileAsync = promisify(execFile)

export type GitRunner = (cwd: string, args: string[]) => Promise<string>

export interface LocalClonePathOptions {
  cwd?: string
  git?: GitRunner
  inferFromCwd?: boolean
  logger?: { info?(message: string): void }
  validateConfiguredCheckouts?: boolean
}

/**
 * Parse a factory config, then apply the environment-dependent conveniences
 * that are appropriate only for a local CLI run.
 */
export async function resolveLocalFactoryConfig(
  input: unknown,
  options: LocalClonePathOptions = {},
): Promise<FactoryConfig> {
  const config = loadFactoryConfig(input).factoryConfig
  const git = options.git ?? runGit
  let resolved = config

  if (options.inferFromCwd !== false) {
    const repoName = cwdInferenceRepoName(input)
    if (repoName !== undefined) {
      const repo = config.repos.byLabel[repoName]
      if (!repo || !isQualifiedRepo(repo)) {
        throw new Error(
          `[factory] cannot infer clonePath from cwd: resolved route for repos.names[0] must be owner/name (received ${JSON.stringify(repo ?? repoName)})`,
        )
      }

      const checkoutRoot = await gitCheckoutRoot(options.cwd ?? process.cwd(), git, repo)
      await assertMatchingGithubRemote(checkoutRoot, repo, git)
      const clonePaths = { ...config.clonePaths, [repo]: checkoutRoot }
      resolved = {
        ...config,
        clonePaths,
        repos: { ...config.repos, clonePaths },
      }
      options.logger?.info?.(`[factory] clonePath inferred from cwd: ${checkoutRoot}`)
    }
  }

  if (options.validateConfiguredCheckouts) {
    await validateClonePaths(resolved.clonePaths, git)
  }

  return resolved
}

async function gitCheckoutRoot(cwd: string, git: GitRunner, repo: string): Promise<string> {
  let root: string
  try {
    root = (await git(cwd, ['rev-parse', '--show-toplevel'])).trim()
  } catch {
    throw new Error(
      `[factory] cannot infer clonePath for ${repo}: cwd is not inside a git checkout: ${resolve(cwd)}; configure cloneRoot or clonePaths explicitly`,
    )
  }
  if (!root) {
    throw new Error(
      `[factory] cannot infer clonePath for ${repo}: git returned no checkout root for cwd ${resolve(cwd)}; configure cloneRoot or clonePaths explicitly`,
    )
  }
  return resolve(root)
}

async function assertMatchingGithubRemote(checkoutRoot: string, expectedRepo: string, git: GitRunner): Promise<void> {
  const remoteNames = (await git(checkoutRoot, ['remote']))
    .split(/\r?\n/u)
    .map((name) => name.trim())
    .filter(Boolean)

  if (remoteNames.length === 0) {
    throw new Error(
      `[factory] cannot infer clonePath for ${expectedRepo}: git checkout ${checkoutRoot} has no remotes; add a matching GitHub remote or configure cloneRoot/clonePaths explicitly`,
    )
  }

  const remoteUrls: string[] = []
  for (const remote of remoteNames) {
    try {
      remoteUrls.push(...(await git(checkoutRoot, ['remote', 'get-url', '--all', remote]))
        .split(/\r?\n/u)
        .map((url) => url.trim())
        .filter(Boolean))
    } catch {
      // A concurrently removed/broken remote is treated like an unparseable
      // remote below, yielding one actionable config error.
    }
  }

  const parsedRepos = remoteUrls
    .map(githubRepoFromRemoteUrl)
    .filter((repo): repo is string => repo !== undefined)
  if (parsedRepos.some((repo) => repo.toLowerCase() === expectedRepo.toLowerCase())) return

  const found = [...new Set(parsedRepos)]
  const detail = found.length > 0
    ? `found GitHub remote${found.length === 1 ? '' : 's'} for ${found.join(', ')}`
    : 'found no parseable GitHub remote URLs'
  throw new Error(
    `[factory] cannot infer clonePath for ${expectedRepo}: ${detail} in ${checkoutRoot}; add a matching remote or configure cloneRoot/clonePaths explicitly`,
  )
}

export function githubRepoFromRemoteUrl(remoteUrl: string): string | undefined {
  const value = remoteUrl.trim().replace(/^git\+/u, '')
  let host: string
  let pathname: string

  try {
    const url = new URL(value)
    host = url.hostname
    pathname = url.pathname
  } catch {
    const scp = /^(?:[^@/\s]+@)?([^:/\s]+):(.+)$/u.exec(value)
    if (scp) {
      host = scp[1]
      pathname = scp[2]
    } else {
      const hostPath = /^([^/\s]+)\/(.+)$/u.exec(value)
      if (!hostPath) return undefined
      host = hostPath[1]
      pathname = hostPath[2]
    }
  }

  if (!['github.com', 'www.github.com'].includes(host.toLowerCase())) return undefined
  const parts = pathname
    .replace(/^\/+|\/+$/gu, '')
    .replace(/\.git$/iu, '')
    .split('/')
    .filter(Boolean)
  if (parts.length !== 2) return undefined
  return `${parts[0]}/${parts[1]}`
}

async function validateClonePaths(clonePaths: Record<string, string>, git: GitRunner): Promise<void> {
  for (const [repo, clonePath] of Object.entries(clonePaths)) {
    const path = resolve(clonePath)
    let pathStat
    try {
      pathStat = await stat(path)
    } catch {
      throw new Error(`[factory] clonePath for ${repo} does not exist: ${path}`)
    }
    if (!pathStat.isDirectory()) {
      throw new Error(`[factory] clonePath for ${repo} is not a directory: ${path}`)
    }

    let checkoutRoot: string
    try {
      checkoutRoot = (await git(path, ['rev-parse', '--show-toplevel'])).trim()
    } catch {
      throw new Error(`[factory] clonePath for ${repo} is not a git checkout: ${path}`)
    }
    if (!checkoutRoot) {
      throw new Error(`[factory] clonePath for ${repo} is not a git checkout: ${path}`)
    }

    const [realPath, realRoot] = await Promise.all([realpath(path), realpath(resolve(checkoutRoot))])
    if (realPath !== realRoot) {
      throw new Error(
        `[factory] clonePath for ${repo} points inside a git checkout (${path}); configure the checkout root ${resolve(checkoutRoot)}`,
      )
    }
  }
}

async function runGit(cwd: string, args: string[]): Promise<string> {
  const { stdout } = await execFileAsync('git', ['-C', cwd, ...args], {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024,
  })
  return stdout
}

function cwdInferenceRepoName(input: unknown): string | undefined {
  const inputRecord = record(input)
  const factoryInput = record(inputRecord.factoryConfig ?? input)
  if (hasOwn(factoryInput, 'workspaceConfig') || hasOwn(factoryInput, 'nodeConfig')) {
    const workspace = record(factoryInput.workspaceConfig)
    const node = record(factoryInput.nodeConfig)
    const repos = record(workspace.repos)
    if (hasClonePathConfig(workspace) || hasClonePathConfig(node) || hasClonePathConfig(repos)) return undefined
    return singleRepoName(repos.names)
  }

  const repos = record(factoryInput.repos)
  if (hasClonePathConfig(factoryInput) || hasClonePathConfig(repos)) return undefined
  return singleRepoName(repos.names)
}

function hasClonePathConfig(value: Record<string, unknown>): boolean {
  return hasOwn(value, 'cloneRoot') || hasOwn(value, 'clonePaths')
}

function singleRepoName(value: unknown): string | undefined {
  return Array.isArray(value) && value.length === 1 && typeof value[0] === 'string'
    ? value[0]
    : undefined
}

function isQualifiedRepo(repo: string): boolean {
  return /^[^/\s]+\/[^/\s]+$/u.test(repo)
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

function hasOwn(value: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key)
}
