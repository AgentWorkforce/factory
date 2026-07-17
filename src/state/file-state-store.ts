import { randomUUID } from 'node:crypto'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import lockfile from 'proper-lockfile'

import type {
  BabysitterSessionState,
  ClarificationReply,
  DispatchLifecycle,
  DispatchLifecycleClaim,
  GithubIssueCommentWatchState,
  WaitingClarification,
} from '../ports/state'
import { InMemoryStateStore, type InMemoryStateStoreOptions } from './in-memory-state-store'

type PersistedWorkspaceState = {
  githubIssueCommentWatches: Record<string, GithubIssueCommentWatchState>
  waitingClarifications: Record<string, WaitingClarification>
  babysitterSessions: Record<string, BabysitterSessionState>
  dispatchLifecycles: Record<string, DispatchLifecycle>
}

type WatchStateDocument = {
  version: 3
  workspaces: Record<string, PersistedWorkspaceState>
}

export type FileStateStoreOptions = InMemoryStateStoreOptions & {
  watchStatePath: string
}

export const githubWatchStatePath = (registryPath: string): string =>
  join(dirname(registryPath), 'github-issue-comment-watches.json')

// proper-lockfile refreshes a live writer's lease at half this interval. If a
// process crashes, the next writer can reclaim its lock after this TTL. The
// longer interval trades rare crash-recovery latency for enough headroom that
// serialization, fsync, GC, disk stalls, or a briefly paused process do not
// let a second writer reclaim a lock that is still actively owned.
const WATCH_STATE_LOCK_STALE_MS = 60_000

/**
 * Keeps the factory's general runtime bookkeeping in memory while persisting
 * GitHub escalation watches, parked clarification teams, and exact babysitter
 * PR ownership/pending wakes atomically so they survive a CLI process restart.
 * Mutations reload under an advisory lock so independent processes merge
 * updates instead of publishing divergent cached documents.
 */
export class FileStateStore extends InMemoryStateStore {
  readonly #watchStatePath: string
  readonly #batchSize: number
  #operation: Promise<void> = Promise.resolve()

  constructor(options: FileStateStoreOptions) {
    super(options)
    this.#watchStatePath = options.watchStatePath
    this.#batchSize = options.batchSize
  }

  override async claimDispatchLifecycle(
    workspaceId: string,
    key: string,
    seed: DispatchLifecycle,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DispatchLifecycleClaim> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
      let lifecycle = workspace.dispatchLifecycles[key]
      const created = !lifecycle
      if (!lifecycle) {
        lifecycle = cloneLifecycle(seed)
        if (activeDispatchLifecycleCount(workspace.dispatchLifecycles) >= this.#batchSize) lifecycle.phase = 'queued'
        workspace.dispatchLifecycles[key] = lifecycle
      }
      const terminal = lifecycle.phase === 'complete' || lifecycle.phase === 'abandoned'
      const activeOtherOwner = lifecycle.lease && lifecycle.lease.owner !== owner && lifecycle.lease.leaseUntilMs > nowMs
      if (terminal || activeOtherOwner) {
        return { acquired: false, lifecycle: cloneLifecycle(lifecycle), created }
      }
      const epoch = lifecycle.lease?.owner === owner
        ? lifecycle.lease.epoch
        : (lifecycle.lease?.epoch ?? 0) + 1
      lifecycle.lease = { owner, epoch, leaseUntilMs: nowMs + leaseMs }
      lifecycle.updatedAtMs = nowMs
      await this.#persist(document)
      return {
        acquired: true,
        lifecycle: cloneLifecycle(lifecycle),
        lease: { ...lifecycle.lease },
        created,
      }
    }))
  }

  override async renewDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const lifecycle = document.workspaces[workspaceId]?.dispatchLifecycles[key]
      if (!lifecycle?.lease || lifecycle.lease.owner !== owner || lifecycle.lease.epoch !== epoch) return false
      lifecycle.lease.leaseUntilMs = nowMs + leaseMs
      lifecycle.updatedAtMs = nowMs
      await this.#persist(document)
      return true
    }))
  }

  override async promoteDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      const lifecycle = workspace?.dispatchLifecycles[key]
      if (
        (lifecycle?.phase !== 'queued' && lifecycle?.phase !== 'waiting-for-human') ||
        !lifecycle.lease ||
        lifecycle.lease.owner !== owner ||
        lifecycle.lease.epoch !== epoch ||
        lifecycle.lease.leaseUntilMs <= nowMs ||
        activeDispatchLifecycleCount(workspace!.dispatchLifecycles, key) >= this.#batchSize
      ) return false
      lifecycle.phase = 'dispatching'
      lifecycle.updatedAtMs = nowMs
      await this.#persist(document)
      return true
    }))
  }

  override async releaseDispatchLifecycleLease(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
  ): Promise<void> {
    await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const lease = document.workspaces[workspaceId]?.dispatchLifecycles[key]?.lease
      if (lease?.owner !== owner || lease.epoch !== epoch) return
      lease.leaseUntilMs = Number.MIN_SAFE_INTEGER
      await this.#persist(document)
    }))
  }

  override async saveDispatchLifecycle(
    workspaceId: string,
    key: string,
    owner: string,
    epoch: number,
    nowMs: number,
    lifecycle: DispatchLifecycle,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      const current = workspace?.dispatchLifecycles[key]
      if (!current?.lease || current.lease.owner !== owner || current.lease.epoch !== epoch || current.lease.leaseUntilMs <= nowMs) {
        return false
      }
      const next = cloneLifecycle(lifecycle)
      next.lease = { ...current.lease }
      next.updatedAtMs = nowMs
      workspace!.dispatchLifecycles[key] = next
      await this.#persist(document)
      return true
    }))
  }

  override async getDispatchLifecycle(workspaceId: string, key: string): Promise<DispatchLifecycle | undefined> {
    return await this.#exclusive(async () => {
      const lifecycle = (await this.#loadFromDisk()).workspaces[workspaceId]?.dispatchLifecycles[key]
      return lifecycle ? cloneLifecycle(lifecycle) : undefined
    })
  }

  override async listDispatchLifecycles(workspaceId: string): Promise<Array<[string, DispatchLifecycle]>> {
    return await this.#exclusive(async () => {
      const lifecycles = (await this.#loadFromDisk()).workspaces[workspaceId]?.dispatchLifecycles ?? {}
      return Object.entries(lifecycles).map(([key, lifecycle]) => [key, cloneLifecycle(lifecycle)])
    })
  }

  override async clearDispatchLifecycle(workspaceId: string, key: string): Promise<void> {
    await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      if (!workspace || !(key in workspace.dispatchLifecycles)) return
      delete workspace.dispatchLifecycles[key]
      if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
      await this.#persist(document)
    }))
  }

  override async setGithubIssueCommentWatch(
    workspaceId: string,
    key: string,
    watch: GithubIssueCommentWatchState,
  ): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
        workspace.githubIssueCommentWatches[key] = cloneWatch(watch)
        await this.#persist(document)
      })
    })
  }

  override async listGithubIssueCommentWatches(
    workspaceId: string,
  ): Promise<Array<[string, GithubIssueCommentWatchState]>> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      return Object.entries(document.workspaces[workspaceId]?.githubIssueCommentWatches ?? {})
        .map(([key, watch]) => [key, cloneWatch(watch)])
    })
  }

  override async clearGithubIssueCommentWatch(workspaceId: string, key: string): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId]
        if (!workspace || !(key in workspace.githubIssueCommentWatches)) return
        delete workspace.githubIssueCommentWatches[key]
        if (workspaceIsEmpty(workspace)) {
          delete document.workspaces[workspaceId]
        }
        await this.#persist(document)
      })
    })
  }

  override async reserveWaitingClarification(
    workspaceId: string,
    issueKey: string,
    record: WaitingClarification,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
        if (workspace.waitingClarifications[issueKey]) return false
        workspace.waitingClarifications[issueKey] = cloneClarification(record)
        await this.#persist(document)
        return true
      })
    })
  }

  override async getWaitingClarification(
    workspaceId: string,
    issueKey: string,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
      return record ? cloneClarification(record) : undefined
    })
  }

  override async listWaitingClarifications(
    workspaceId: string,
  ): Promise<Array<[string, WaitingClarification]>> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      return Object.entries(document.workspaces[workspaceId]?.waitingClarifications ?? {})
        .map(([key, record]) => [key, cloneClarification(record)])
    })
  }

  override async claimClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record || record.questionPostedAtMs !== undefined || (
          record.questionDelivery?.owner &&
          nowMs - record.questionDelivery.claimedAtMs < leaseMs
        )) return undefined
        record.questionDelivery = {
          owner,
          claimedAtMs: nowMs,
          attempts: (record.questionDelivery?.attempts ?? 0) + 1,
        }
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async completeClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    postedAtMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (record?.questionDelivery?.owner !== owner) return false
        record.questionPostedAtMs = postedAtMs
        delete record.questionDelivery
        await this.#persist(document)
        return true
      })
    })
  }

  override async renewClarificationQuestionDelivery(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const delivery = document.workspaces[workspaceId]?.waitingClarifications[issueKey]?.questionDelivery
        if (delivery?.owner !== owner) return false
        delivery.claimedAtMs = nowMs
        await this.#persist(document)
        return true
      })
    })
  }

  override async releaseClarificationQuestionDelivery(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    await this.#mutateClarification(workspaceId, issueKey, (record) => {
      if (record.questionDelivery?.owner !== owner) return
      record.questionDelivery.owner = ''
      record.questionDelivery.claimedAtMs = Number.MIN_SAFE_INTEGER
    })
  }

  override async claimClarificationReply(
    workspaceId: string,
    issueKey: string,
    reply: ClarificationReply,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record || (record.questionPostedAtMs === undefined && !record.questionDelivery?.owner) || record.reply) {
          return undefined
        }
        record.reply = { ...reply }
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async markClarificationAgentReleased(
    workspaceId: string,
    issueKey: string,
    agentName: string,
  ): Promise<WaitingClarification | undefined> {
    return await this.#mutateClarification(workspaceId, issueKey, (record) => {
      record.releasedAgents ??= []
      if (!record.releasedAgents.includes(agentName)) record.releasedAgents.push(agentName)
    })
  }

  override async claimClarificationWake(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record?.reply || record.questionPostedAtMs === undefined || record.parkedAtMs === undefined || (record.wake && record.wake.owner !== owner && nowMs - record.wake.claimedAtMs < leaseMs)) {
          return undefined
        }
        record.wake = {
          owner,
          claimedAtMs: nowMs,
          attempts: (record.wake?.attempts ?? 0) + 1,
          injectedAgents: [...(record.wake?.injectedAgents ?? [])],
        }
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async markClarificationParked(
    workspaceId: string,
    issueKey: string,
    parkedAtMs: number,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record) return undefined
        const released = new Set(record.releasedAgents ?? [])
        if (record.agents.some(({ name }) => !released.has(name))) return undefined
        record.parkedAtMs ??= parkedAtMs
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async claimClarificationEscalation(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record || record.reply || record.escalatedAtMs || (
          record.escalation && record.escalation.owner !== owner && nowMs - record.escalation.claimedAtMs < leaseMs
        )) return undefined
        record.escalation = {
          owner,
          claimedAtMs: nowMs,
          attempts: (record.escalation?.attempts ?? 0) + 1,
        }
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async completeClarificationEscalation(
    workspaceId: string,
    issueKey: string,
    owner: string,
    escalatedAtMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (record?.escalation?.owner !== owner) return false
        record.escalatedAtMs = escalatedAtMs
        delete record.escalation
        await this.#persist(document)
        return true
      })
    })
  }

  override async releaseClarificationEscalation(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    await this.#mutateClarification(workspaceId, issueKey, (record) => {
      if (record.escalation?.owner !== owner) return
      record.escalation.owner = ''
      record.escalation.claimedAtMs = Number.MIN_SAFE_INTEGER
    })
  }

  override async renewClarificationWake(
    workspaceId: string,
    issueKey: string,
    owner: string,
    nowMs: number,
  ): Promise<boolean> {
    return await this.#mutateOwnedWake(workspaceId, issueKey, owner, (record) => {
      record.wake!.claimedAtMs = nowMs
    })
  }

  override async markClarificationAgentInjected(
    workspaceId: string,
    issueKey: string,
    owner: string,
    agentName: string,
  ): Promise<boolean> {
    return await this.#mutateOwnedWake(workspaceId, issueKey, owner, (record) => {
      if (!record.wake!.injectedAgents.includes(agentName)) record.wake!.injectedAgents.push(agentName)
    })
  }

  override async completeClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId]
        if (workspace?.waitingClarifications[issueKey]?.wake?.owner !== owner) return false
        delete workspace.waitingClarifications[issueKey]
        if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
        await this.#persist(document)
        return true
      })
    })
  }

  override async releaseClarificationWake(workspaceId: string, issueKey: string, owner: string): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (record?.wake?.owner !== owner) return
        record.wake.owner = ''
        record.wake.claimedAtMs = Number.MIN_SAFE_INTEGER
        await this.#persist(document)
      })
    })
  }

  async #mutateOwnedWake(
    workspaceId: string,
    issueKey: string,
    owner: string,
    mutate: (record: WaitingClarification) => void,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (record?.wake?.owner !== owner) return false
        mutate(record)
        await this.#persist(document)
        return true
      })
    })
  }

  async #mutateClarification(
    workspaceId: string,
    issueKey: string,
    mutate: (record: WaitingClarification) => void,
  ): Promise<WaitingClarification | undefined> {
    return await this.#exclusive(async () => {
      return await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const record = document.workspaces[workspaceId]?.waitingClarifications[issueKey]
        if (!record) return undefined
        mutate(record)
        await this.#persist(document)
        return cloneClarification(record)
      })
    })
  }

  override async clearWaitingClarification(workspaceId: string, issueKey: string): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId]
        if (!workspace || !(issueKey in workspace.waitingClarifications)) return
        delete workspace.waitingClarifications[issueKey]
        if (workspaceIsEmpty(workspace)) {
          delete document.workspaces[workspaceId]
        }
        await this.#persist(document)
      })
    })
  }

  override async setBabysitterSession(
    workspaceId: string,
    issueKey: string,
    session: BabysitterSessionState,
  ): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
        workspace.babysitterSessions[issueKey] = cloneBabysitterSession(session)
        await this.#persist(document)
      })
    })
  }

  override async listBabysitterSessions(
    workspaceId: string,
  ): Promise<Array<[string, BabysitterSessionState]>> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      return Object.entries(document.workspaces[workspaceId]?.babysitterSessions ?? {})
        .map(([key, session]) => [key, cloneBabysitterSession(session)])
    })
  }

  override async clearBabysitterSession(workspaceId: string, issueKey: string): Promise<void> {
    await this.#exclusive(async () => {
      await this.#withMutationLock(async () => {
        const document = await this.#loadFromDisk()
        const workspace = document.workspaces[workspaceId]
        if (!workspace || !(issueKey in workspace.babysitterSessions)) return
        delete workspace.babysitterSessions[issueKey]
        if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
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
      return { version: 3, workspaces: {} }
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
      await syncParentDirectory(this.#watchStatePath)
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
  if (!isRecord(value) || !isRecord(value.workspaces)) {
    throw new Error('Factory GitHub watch state file is invalid')
  }
  if (value.version === 3) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, rawWorkspace] of Object.entries(value.workspaces)) {
      if (!isRecord(rawWorkspace)) throw new Error('Factory GitHub watch state file is invalid')
      const watches = rawWorkspace.githubIssueCommentWatches
      const clarifications = rawWorkspace.waitingClarifications
      const babysitters = rawWorkspace.babysitterSessions
      const lifecycles = rawWorkspace.dispatchLifecycles
      if (
        !isRecord(watches) ||
        !isRecord(clarifications) ||
        (babysitters !== undefined && !isRecord(babysitters)) ||
        (lifecycles !== undefined && !isRecord(lifecycles))
      ) {
        throw new Error('Factory GitHub watch state file is invalid')
      }
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: clarifications as Record<string, WaitingClarification>,
        babysitterSessions: parseBabysitterSessions(babysitters ?? {}),
        dispatchLifecycles: (lifecycles ?? {}) as Record<string, DispatchLifecycle>,
      }
    }
    return { version: 3, workspaces }
  }
  if (value.version === 2) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, rawWorkspace] of Object.entries(value.workspaces)) {
      if (!isRecord(rawWorkspace)) throw new Error('Factory GitHub watch state file is invalid')
      const watches = rawWorkspace.githubIssueCommentWatches
      const clarifications = rawWorkspace.waitingClarifications
      const babysitters = rawWorkspace.babysitterSessions
      if (!isRecord(watches) || !isRecord(clarifications) || (babysitters !== undefined && !isRecord(babysitters))) {
        throw new Error('Factory GitHub watch state file is invalid')
      }
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: clarifications as Record<string, WaitingClarification>,
        babysitterSessions: parseBabysitterSessions(babysitters ?? {}),
        dispatchLifecycles: {},
      }
    }
    return { version: 3, workspaces }
  }
  if (value.version === 1) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, watches] of Object.entries(value.workspaces)) {
      if (!isRecord(watches)) {
        throw new Error('Factory GitHub watch state file is invalid')
      }
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: {},
        babysitterSessions: {},
        dispatchLifecycles: {},
      }
    }
    return { version: 3, workspaces }
  }
  throw new Error('Factory GitHub watch state file is invalid')
}

const cloneWatch = (watch: GithubIssueCommentWatchState): GithubIssueCommentWatchState =>
  structuredClone(watch)

const cloneClarification = (record: WaitingClarification): WaitingClarification =>
  structuredClone(record)

const cloneBabysitterSession = (session: BabysitterSessionState): BabysitterSessionState =>
  structuredClone(session)

const parseBabysitterSessions = (value: Record<string, unknown>): Record<string, BabysitterSessionState> => {
  const sessions: Record<string, BabysitterSessionState> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (!isRecord(candidate) || !isRecord(candidate.issue)) {
      throw new Error('Factory GitHub watch state file is invalid')
    }
    const issue = candidate.issue
    if (
      typeof issue.uuid !== 'string' ||
      typeof issue.key !== 'string' ||
      typeof issue.path !== 'string' ||
      typeof candidate.repo !== 'string' ||
      !Number.isSafeInteger(candidate.prNumber) ||
      (candidate.prNumber as number) < 1 ||
      typeof candidate.agentName !== 'string' ||
      (candidate.path !== undefined && typeof candidate.path !== 'string') ||
      typeof candidate.critical !== 'boolean' ||
      !Array.isArray(candidate.pendingKinds) ||
      !candidate.pendingKinds.every((kind) => typeof kind === 'string')
    ) {
      throw new Error('Factory GitHub watch state file is invalid')
    }
    sessions[key] = {
      issue: { uuid: issue.uuid, key: issue.key, path: issue.path },
      repo: candidate.repo,
      prNumber: candidate.prNumber as number,
      agentName: candidate.agentName,
      ...(candidate.path === undefined ? {} : { path: candidate.path }),
      critical: candidate.critical,
      pendingKinds: [...candidate.pendingKinds] as string[],
    }
  }
  return sessions
}

const cloneLifecycle = (record: DispatchLifecycle): DispatchLifecycle => structuredClone(record)

const activeDispatchLifecycleCount = (lifecycles: Record<string, DispatchLifecycle>, exceptKey?: string): number =>
  Object.entries(lifecycles).filter(([key, lifecycle]) => key !== exceptKey && dispatchLifecycleOccupiesSlot(lifecycle)).length

const dispatchLifecycleOccupiesSlot = (lifecycle: DispatchLifecycle): boolean =>
  lifecycle.phase !== 'queued' &&
  lifecycle.phase !== 'waiting-for-human' &&
  lifecycle.phase !== 'releasing' &&
  lifecycle.phase !== 'complete' &&
  lifecycle.phase !== 'abandoned'

const emptyWorkspaceState = (): PersistedWorkspaceState => ({
  githubIssueCommentWatches: {},
  waitingClarifications: {},
  babysitterSessions: {},
  dispatchLifecycles: {},
})

const workspaceIsEmpty = (workspace: PersistedWorkspaceState): boolean =>
  Object.keys(workspace.githubIssueCommentWatches).length === 0 &&
  Object.keys(workspace.waitingClarifications).length === 0 &&
  Object.keys(workspace.babysitterSessions).length === 0 &&
  Object.keys(workspace.dispatchLifecycles).length === 0

const syncParentDirectory = async (filePath: string): Promise<void> => {
  const handle = await open(dirname(filePath), 'r')
  try {
    await handle.sync()
  } finally {
    await handle.close()
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isMissingFileError = (error: unknown): boolean =>
  isRecord(error) && error.code === 'ENOENT'
