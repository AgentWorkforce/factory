import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

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

/**
 * Keeps the factory's general runtime bookkeeping in memory while persisting
 * GitHub escalation watches atomically so they survive a CLI process restart.
 */
export class FileStateStore extends InMemoryStateStore {
  readonly #watchStatePath: string
  #document?: WatchStateDocument
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
      const document = await this.#load()
      const workspace = document.workspaces[workspaceId] ??= {}
      workspace[key] = cloneWatch(watch)
      await this.#persist(document)
    })
  }

  override async listGithubIssueCommentWatches(
    workspaceId: string,
  ): Promise<Array<[string, GithubIssueCommentWatchState]>> {
    return await this.#exclusive(async () => {
      const document = await this.#load()
      return Object.entries(document.workspaces[workspaceId] ?? {})
        .map(([key, watch]) => [key, cloneWatch(watch)])
    })
  }

  override async clearGithubIssueCommentWatch(workspaceId: string, key: string): Promise<void> {
    await this.#exclusive(async () => {
      const document = await this.#load()
      const workspace = document.workspaces[workspaceId]
      if (!workspace || !(key in workspace)) return
      delete workspace[key]
      if (Object.keys(workspace).length === 0) {
        delete document.workspaces[workspaceId]
      }
      await this.#persist(document)
    })
  }

  async #load(): Promise<WatchStateDocument> {
    if (this.#document) return this.#document
    try {
      const parsed = JSON.parse(await readFile(this.#watchStatePath, 'utf8')) as unknown
      this.#document = parseDocument(parsed)
    } catch (error) {
      if (!isMissingFileError(error)) throw error
      this.#document = { version: 1, workspaces: {} }
    }
    return this.#document
  }

  async #persist(document: WatchStateDocument): Promise<void> {
    await mkdir(dirname(this.#watchStatePath), { recursive: true })
    const temporaryPath = `${this.#watchStatePath}.${process.pid}.${Date.now()}.tmp`
    await writeFile(temporaryPath, `${JSON.stringify(document, null, 2)}\n`, { mode: 0o600 })
    await rename(temporaryPath, this.#watchStatePath)
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
