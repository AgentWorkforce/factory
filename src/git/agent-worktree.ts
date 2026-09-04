import { execFile } from 'node:child_process'
import { lstat, mkdir, readdir, realpath, rmdir, stat } from 'node:fs/promises'
import { basename, dirname, join, resolve } from 'node:path'
import { promisify } from 'node:util'

import { factoryBranchBelongsToIssue } from '../issue-key-match'
import type {
  AgentWorktree,
  AgentWorktreeCleanupInspection,
  AgentWorktreeManager,
  AgentWorktreeRepository,
} from '../ports/worktree'

const execFileAsync = promisify(execFile)

export type AgentWorktreeGitRunner = (cwd: string, args: string[]) => Promise<string>

export interface GitAgentWorktreeManagerOptions {
  git?: AgentWorktreeGitRunner
}

/**
 * Owns the isolated checkout used by one issue/repository lifecycle. All roles
 * for that lifecycle share this checkout, while unrelated issues can never
 * switch each other's branches in the configured source clone.
 */
export class GitAgentWorktreeManager implements AgentWorktreeManager {
  readonly #git: AgentWorktreeGitRunner
  readonly #prepares = new Map<string, Promise<void>>()

  constructor(options: GitAgentWorktreeManagerOptions = {}) {
    this.#git = options.git ?? runGit
  }

  async prepare(worktree: AgentWorktree): Promise<void> {
    const key = resolve(worktree.worktreePath)
    const existing = this.#prepares.get(key)
    if (existing) return await existing
    const pending = this.#prepare(worktree)
    this.#prepares.set(key, pending)
    try {
      await pending
    } finally {
      if (this.#prepares.get(key) === pending) this.#prepares.delete(key)
    }
  }

  async #prepare(worktree: AgentWorktree): Promise<void> {
    assertSafeWorktree(worktree)
    // Deleted linked checkouts remain in Git's registry as prunable entries.
    // Remove those before validating an existing healthy Factory checkout so
    // an unrelated stale registration cannot make realpath validation fail.
    await this.#git(worktree.baseClonePath, ['worktree', 'prune'])
    const exists = await pathExists(worktree.worktreePath)
    if (exists) {
      await this.#assertRegisteredCheckout(worktree)
      const branch = (await this.#git(worktree.worktreePath, ['symbolic-ref', '--short', 'HEAD'])).trim()
      if (branch !== worktree.branch) {
        throw new Error(
          `Software Garden worktree branch mismatch for ${worktree.issueKey}: expected ${worktree.branch}, found ${branch}`,
        )
      }
      return
    }

    await mkdir(dirname(worktree.worktreePath), { recursive: true })
    if (worktree.existingPullRequestBranch) {
      const remoteRef = `refs/remotes/origin/${worktree.branch}`
      await this.#git(worktree.baseClonePath, [
        'fetch',
        '--no-tags',
        'origin',
        `+refs/heads/${worktree.branch}:${remoteRef}`,
      ])
      if (await this.#localBranchExists(worktree.baseClonePath, worktree.branch)) {
        const checkedOutAt = await this.#branchCheckoutPath(worktree.baseClonePath, worktree.branch)
        if (checkedOutAt) {
          throw new Error(
            `Refusing to adopt existing PR branch ${worktree.branch}: it is already checked out at ${checkedOutAt}; Software Garden will not reuse or remove a checkout outside ${worktree.worktreePath}`,
          )
        }
        const [localHead, remoteHead] = await Promise.all([
          this.#git(worktree.baseClonePath, ['rev-parse', `refs/heads/${worktree.branch}`]),
          this.#git(worktree.baseClonePath, ['rev-parse', remoteRef]),
        ])
        if (localHead.trim() !== remoteHead.trim()) {
          throw new Error(
            `Refusing to overwrite divergent local PR branch ${worktree.branch} while adopting ${worktree.issueKey}`,
          )
        }
        await this.#git(worktree.baseClonePath, ['worktree', 'add', worktree.worktreePath, worktree.branch])
      } else {
        await this.#git(worktree.baseClonePath, [
          'worktree',
          'add',
          '--track',
          '-b',
          worktree.branch,
          worktree.worktreePath,
          remoteRef,
        ])
      }
    } else if (await this.#localBranchExists(worktree.baseClonePath, worktree.branch)) {
      await this.#git(worktree.baseClonePath, ['worktree', 'add', worktree.worktreePath, worktree.branch])
    } else {
      const baseRef = await this.#defaultBaseRef(worktree.baseClonePath)
      await this.#git(worktree.baseClonePath, [
        'worktree',
        'add',
        '-b',
        worktree.branch,
        worktree.worktreePath,
        baseRef,
      ])
    }
    await this.#assertRegisteredCheckout(worktree)
  }

  async cleanup(worktree: AgentWorktree): Promise<void> {
    await this.#prepares.get(resolve(worktree.worktreePath))
    assertSafeWorktree(worktree)
    await assertNonSymlinkFactoryRoot(factoryWorktreeRoot(worktree.baseClonePath))
    await this.#git(worktree.baseClonePath, ['worktree', 'prune'])
    if (!await pathExists(worktree.worktreePath)) {
      await removeEmptyWorktreeParents(worktree.worktreePath)
      return
    }
    await this.#assertRegisteredCheckout(worktree)
    await this.#git(worktree.baseClonePath, ['worktree', 'remove', '--force', worktree.worktreePath])
    await this.#git(worktree.baseClonePath, ['worktree', 'prune'])
    await removeEmptyWorktreeParents(worktree.worktreePath)
  }

  async listWorktrees(repository: AgentWorktreeRepository): Promise<AgentWorktree[]> {
    const root = factoryWorktreeRoot(repository.baseClonePath)
    let entries
    try {
      entries = await readdir(root, { withFileTypes: true })
    } catch (error) {
      if (errorCode(error) === 'ENOENT') return []
      throw error
    }

    const repoSlug = sanitizeSegment(repository.repo.split('/').at(-1) ?? repository.repo)
    const pattern = new RegExp(`^(.+)-${escapeRegExp(repoSlug)}-(.+)$`, 'u')
    const worktrees: AgentWorktree[] = []
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (!entry.isDirectory()) continue
      const match = entry.name.match(pattern)
      if (!match?.[1]) continue
      const worktreePath = join(root, entry.name)
      // Keep malformed or detached matching directories visible to the caller.
      // Their synthetic unsafe branch makes inspection fail closed and ensures
      // Factory logs the retained path without one bad entry hiding siblings.
      let branch = '__factory-unreadable__'
      try {
        branch = (await this.#git(worktreePath, ['symbolic-ref', '--short', 'HEAD'])).trim()
      } catch {
        // Inspection will reject this candidate before deletion.
      }
      const issueKey = match[1]
      worktrees.push({
        repo: repository.repo,
        issueKey,
        baseClonePath: repository.baseClonePath,
        worktreePath,
        branch,
        ...(isAuthorizedExistingPrBranch({
          repo: repository.repo,
          issueKey,
          baseClonePath: repository.baseClonePath,
          worktreePath,
          branch,
          existingPullRequestBranch: true,
        }) ? { existingPullRequestBranch: true } : {}),
      })
    }
    return worktrees
  }

  async inspectForCleanup(worktree: AgentWorktree): Promise<AgentWorktreeCleanupInspection> {
    assertSafeWorktree(worktree)
    await assertNonSymlinkFactoryRoot(factoryWorktreeRoot(worktree.baseClonePath))
    // Inspection runs before cleanup, so it must clear unrelated deleted
    // registrations itself. Otherwise #assertRegisteredCheckout can fail on
    // realpath() for a stale sibling and retain every healthy candidate in the
    // repository without ever reaching cleanup's existing prune step.
    await this.#git(worktree.baseClonePath, ['worktree', 'prune'])
    if (!await pathExists(worktree.worktreePath)) return { bytes: 0, retentionReasons: [] }
    await this.#assertRegisteredCheckout(worktree)

    const retentionReasons: string[] = []
    const gitDir = resolveGitPath(
      worktree.worktreePath,
      (await this.#git(worktree.worktreePath, ['rev-parse', '--git-dir'])).trim(),
    )
    const commonDir = resolveGitPath(
      worktree.worktreePath,
      (await this.#git(worktree.worktreePath, ['rev-parse', '--git-common-dir'])).trim(),
    )
    const locks = await relevantGitLocks(gitDir, commonDir, worktree.branch)
    if (locks.length > 0) retentionReasons.push(`git lock present (${locks.join(', ')})`)

    const status = await this.#git(worktree.worktreePath, ['status', '--porcelain', '--untracked-files=all'])
    if (status.trim()) retentionReasons.push('uncommitted changes')

    const unpushed = Number.parseInt(
      (await this.#git(worktree.worktreePath, ['rev-list', '--count', 'HEAD', '--not', '--remotes'])).trim(),
      10,
    )
    if (!Number.isFinite(unpushed)) {
      throw new Error(`Unable to determine unpushed commits for ${worktree.worktreePath}`)
    }
    if (unpushed > 0) retentionReasons.push(`${unpushed} unpushed commit${unpushed === 1 ? '' : 's'}`)

    return {
      bytes: await directorySize(worktree.worktreePath),
      retentionReasons,
    }
  }

  async #assertRegisteredCheckout(worktree: AgentWorktree): Promise<void> {
    const wanted = await realpath(worktree.worktreePath)
    const expectedRootPath = factoryWorktreeRoot(worktree.baseClonePath)
    await assertNonSymlinkFactoryRoot(expectedRootPath)
    const expectedRoot = await realpath(expectedRootPath)
    if (wanted === expectedRoot || !wanted.startsWith(`${expectedRoot}/`)) {
      throw new Error(`Refusing Software Garden worktree operation outside ${expectedRoot}: resolved target is ${wanted}`)
    }
    const root = await realpath((await this.#git(worktree.worktreePath, ['rev-parse', '--show-toplevel'])).trim())
    if (root !== wanted) {
      throw new Error(`Refusing Software Garden worktree operation outside ${wanted}: git root is ${root}`)
    }
    const registeredPaths = (await this.#git(worktree.baseClonePath, ['worktree', 'list', '--porcelain']))
      .split(/\r?\n/u)
      .filter((line) => line.startsWith('worktree '))
      .map((line) => line.slice('worktree '.length).trim())
    const registered = (await Promise.allSettled(
      registeredPaths.map(async (path) => await realpath(path)),
    )).flatMap((result) => result.status === 'fulfilled' ? [result.value] : [])
    if (!registered.includes(wanted)) {
      throw new Error(`Refusing Software Garden worktree operation for unregistered checkout ${wanted}`)
    }
  }

  async #localBranchExists(baseClonePath: string, branch: string): Promise<boolean> {
    try {
      await this.#git(baseClonePath, ['show-ref', '--verify', '--quiet', `refs/heads/${branch}`])
      return true
    } catch {
      return false
    }
  }

  async #branchCheckoutPath(baseClonePath: string, branch: string): Promise<string | undefined> {
    const lines = (await this.#git(baseClonePath, ['worktree', 'list', '--porcelain'])).split(/\r?\n/u)
    let path: string | undefined
    for (const line of lines) {
      if (line.startsWith('worktree ')) {
        path = line.slice('worktree '.length).trim()
      } else if (line === `branch refs/heads/${branch}`) {
        return path
      }
    }
    return undefined
  }

  async #defaultBaseRef(baseClonePath: string): Promise<string> {
    try {
      const remoteHead = (await this.#git(baseClonePath, [
        'symbolic-ref',
        '--quiet',
        '--short',
        'refs/remotes/origin/HEAD',
      ])).trim()
      if (remoteHead) return remoteHead
    } catch {
      // Fall through to common default branches and finally the current HEAD.
    }
    for (const candidate of ['origin/main', 'origin/master', 'main', 'master']) {
      try {
        await this.#git(baseClonePath, ['rev-parse', '--verify', '--quiet', candidate])
        return candidate
      } catch {
        // try the next candidate
      }
    }
    return 'HEAD'
  }
}

export function factoryWorktreePath(
  baseClonePath: string,
  issueKey: string,
  repo: string,
  runId: string,
): string {
  const repoSlug = sanitizeSegment(repo.split('/').at(-1) ?? repo)
  const issueSlug = sanitizeSegment(issueKey)
  return join(
    factoryWorktreeRoot(baseClonePath),
    `${issueSlug}-${repoSlug}-${runId.slice(0, 8)}`,
  )
}

export const factoryWorktreeIssueSlug = (issueKey: string): string => sanitizeSegment(issueKey)

const factoryWorktreeRoot = (baseClonePath: string): string => {
  const base = resolve(baseClonePath)
  return resolve(dirname(base), '.factory-worktrees', sanitizeSegment(basename(base)))
}

const sanitizeSegment = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9._-]+/gu, '-').replace(/^-+|-+$/gu, '') || 'worktree'

const assertSafeWorktree = (worktree: AgentWorktree): void => {
  const base = resolve(worktree.baseClonePath)
  const target = resolve(worktree.worktreePath)
  const expectedRoot = factoryWorktreeRoot(base)
  if (target === base || !target.startsWith(`${expectedRoot}/`)) {
    throw new Error(`Refusing unsafe Software Garden worktree path ${target}; expected a child of ${expectedRoot}`)
  }
  if (worktree.branch.startsWith('factory/') && !factoryBranchBelongsToIssue(worktree.branch, worktree.issueKey)) {
    throw new Error(
      `Refusing Software Garden worktree branch ${worktree.branch}: it does not belong to dispatched issue ${worktree.issueKey}`,
    )
  }
  if (!worktree.branch.startsWith('factory/') && !isAuthorizedExistingPrBranch(worktree)) {
    throw new Error(`Refusing unsafe Software Garden worktree branch ${worktree.branch}`)
  }
}

const isAuthorizedExistingPrBranch = (worktree: AgentWorktree): boolean =>
  worktree.existingPullRequestBranch === true &&
  /^\d+$/u.test(worktree.issueKey) &&
  new RegExp(`^${worktree.issueKey}-[A-Za-z0-9][A-Za-z0-9._-]*$`, 'u').test(worktree.branch)

const pathExists = async (path: string): Promise<boolean> => {
  try {
    return (await stat(path)).isDirectory()
  } catch (error) {
    if (errorCode(error) === 'ENOENT') return false
    throw error
  }
}

const resolveGitPath = (cwd: string, path: string): string => resolve(cwd, path)

const relevantGitLocks = async (gitDir: string, commonDir: string, branch: string): Promise<string[]> => {
  const candidates = new Set([
    join(gitDir, 'locked'),
    join(gitDir, 'index.lock'),
    join(gitDir, 'HEAD.lock'),
    join(commonDir, 'packed-refs.lock'),
    join(commonDir, 'config.lock'),
    join(commonDir, 'refs', 'heads', `${branch}.lock`),
  ])
  const locks: string[] = []
  for (const path of candidates) {
    try {
      await lstat(path)
      locks.push(path)
    } catch (error) {
      if (errorCode(error) !== 'ENOENT') throw error
    }
  }
  return locks
}

const directorySize = async (root: string): Promise<number> => {
  const entry = await lstat(root)
  if (!entry.isDirectory()) return entry.size
  let bytes = entry.size
  for (const child of await readdir(root)) {
    bytes += await directorySize(join(root, child))
  }
  return bytes
}

const assertNonSymlinkFactoryRoot = async (expectedRoot: string): Promise<void> => {
  for (const path of [dirname(expectedRoot), expectedRoot]) {
    try {
      if ((await lstat(path)).isSymbolicLink()) {
        throw new Error(`Refusing Software Garden worktree operation through symbolic-link root ${path}`)
      }
    } catch (error) {
      if (errorCode(error) === 'ENOENT') return
      throw error
    }
  }
}

const removeEmptyWorktreeParents = async (worktreePath: string): Promise<void> => {
  const repoRoot = dirname(worktreePath)
  const factoryRoot = dirname(repoRoot)
  await rmdir(repoRoot).catch(ignoreNonEmptyOrMissing)
  await rmdir(factoryRoot).catch(ignoreNonEmptyOrMissing)
}

const ignoreNonEmptyOrMissing = (error: unknown): void => {
  const code = errorCode(error)
  if (code === 'ENOENT' || code === 'ENOTEMPTY' || code === 'EEXIST') return
  throw error
}

const errorCode = (error: unknown): string | undefined =>
  typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : undefined

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')

const runGit: AgentWorktreeGitRunner = async (cwd, args) => {
  const { stdout } = await execFileAsync('git', ['-C', cwd, ...args], {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024,
  })
  return stdout
}
