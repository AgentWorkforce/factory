import { randomUUID } from 'node:crypto'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import lockfile from 'proper-lockfile'

import type { GithubIssueCommentWatchState } from '../ports/state'
import { InMemoryStateStore, type InMemoryStateStoreOptions } from './in-memory-state-store'

type WatchStateDocument = {
  version: 1
  workspaces: Record<string, Record<string, GithubIssueCommentWatchState>>
}

export type FileStateStoreOptions = InMemoryStateStoreOptions & {
  watchStatePath: string
}

export const githubWatchStatePath = (registryPath: string): string =>
  join(dirname(registryPath), 'github-issue-comment-watches.json')

// proper-lockfile refreshes a live writer's lease at half this interval. If a
// process crashes, the next writer can reclaim its lock after this TTL.
const WATCH_STATE_LOCK_STALE_MS = 10_000

/**
 * Keeps the factory's general runtime bookkeeping in memory while persisting
 * GitHub escalation watches atomically so they survive a CLI process restart.
 * Mutations reload under an advisory lock so independent processes merge
 * updates instead of publishing divergent cached documents.
 */
export class FileStateStore extends InMemoryStateStore {
  readonly #watchStatePath: string
  #operation: Promise<void> = Promise.resolve()

  constructor(options: FileStateStoreOptions) {
    super(options)
    this.#watchStatePath = options.watchStatePath
  }

  override async setGithubIssueCommentWatch(
    workspaceId: string,
    key: string,
    watch: GithubIssueCommentWatchState,
  ): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId] ??= {}
        workspace[key] = cloneWatch(watch)
        await this.#persist(document)
      })
    })
  }

  override async listGithubIssueCommentWatches(
    workspaceId: string,
  ): Promise<Array<[string, GithubIssueCommentWatchState]>> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      return Object.entries(document.workspaces[workspaceId] ?? {})
        .map(([key, watch]) => [key, cloneWatch(watch)])
    })
  }

  override async clearGithubIssueCommentWatch(workspaceId: string, key: string): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId]
        if (!workspace || !(key in workspace)) return
        delete workspace[key]
        if (Object.keys(workspace).length === 0) {
          delete document.workspaces[workspaceId]
        }
        await this.#persist(document)
      })
    })
  }

  async #loadFromDisk(): Promise<WatchStateDocument> {
    try {
      const parsed = JSON.parse(await readFile(this.#watchStatePath, 'utf8')) as unknown
      return parseDocument(parsed)
    } catch (error) {
      if (!isMissingFileError(error)) throw error
      return { version: 1, workspaces: {} }
    }
  }

  async #persist(document: WatchStateDocument): Promise<void> {
    const temporaryPath = `${this.#watchStatePath}.${process.pid}.${randomUUID()}.tmp`
    try {
      const handle = await open(temporaryPath, 'wx', 0o600)
      try {
        await handle.writeFile(`${JSON.stringify(document, null, 2)}\n`)
        await handle.sync()
      } finally {
        await handle.close()
      }

      await rename(temporaryPath, this.#watchStatePath)
    } finally {
      await rm(temporaryPath, { force: true })
    }
  }

  async #withMutationLock<T>(operation: () => Promise<T>): Promise<T> {
    await mkdir(dirname(this.#watchStatePath), { recursive: true })
    const release = await lockfile.lock(this.#watchStatePath, {
      realpath: false,
      stale: WATCH_STATE_LOCK_STALE_MS,
      update: WATCH_STATE_LOCK_STALE_MS / 2,
      retries: {
        forever: true,
        factor: 1.2,
        minTimeout: 10,
        maxTimeout: 100,
        randomize: true,
      },
    })
    try {
      return await operation()
    } finally {
      await release()
    }
  }

  async #exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#operation.then(operation, operation)
    this.#operation = result.then(() => undefined, () => undefined)
    return await result
  }
}

const parseDocument = (value: unknown): WatchStateDocument => {
  if (!isRecord(value) || value.version !== 1 || !isRecord(value.workspaces)) {
    throw new Error('Factory GitHub watch state file is invalid')
  }
  return value as WatchStateDocument
}

const cloneWatch = (watch: GithubIssueCommentWatchState): GithubIssueCommentWatchState =>
  structuredClone(watch)

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isMissingFileError = (error: unknown): boolean =>
  isRecord(error) && error.code === 'ENOENT'
