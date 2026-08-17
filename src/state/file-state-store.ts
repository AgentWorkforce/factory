import { randomUUID } from 'node:crypto'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import lockfile from 'proper-lockfile'

import { githubRepositoriesMatch } from '../github/repo-identity'
import type {
  BabysitterGenerationRecord,
  BabysitterSessionState,
  ClarificationReply,
  DispatchLifecycle,
  DispatchLifecycleClaim,
  GithubIssueCommentWatchState,
  ConversationMessage,
  ConversationSessionState,
  DiscoveryCheckpoint,
  DiscoverySweepClaim,
  DiscoverySweepLease,
  DiscoverySweepRenewal,
  DiscoverySweepState,
  WaitingClarification,
} from '../ports/state'
import { InMemoryStateStore, type InMemoryStateStoreOptions } from './in-memory-state-store'
import { matchingGithubLifecycleEntry } from './github-lifecycle-identity'
import type {
  FactoryStateBackend,
  PersistedWorkspaceState,
  WatchStateDocument,
  WatchStateDocumentStore,
} from './document-store'
import { emptyDiscoverySweepState, parseWatchStateDocument } from './watch-state-document'

export type FileStateStoreOptions = InMemoryStateStoreOptions & {
  watchStatePath: string
  /** Injectable for deterministic stale-process lease recovery tests. */
  isProcessAlive?: (pid: number) => boolean
}

export type DocumentStateStoreOptions = InMemoryStateStoreOptions & {
  backend: FactoryStateBackend
  documentStore: WatchStateDocumentStore
  /** Injectable for deterministic stale-process lease recovery tests. */
  isProcessAlive?: (pid: number) => boolean
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
 * GitHub escalation watches, parked clarification teams, exact babysitter PR
 * ownership, and thread-owned conversation turns atomically so they survive a
 * CLI process restart.
 * Mutations reload under an advisory lock so independent processes merge
 * updates instead of publishing divergent cached documents.
 */
export class DocumentStateStore extends InMemoryStateStore {
  readonly backend: FactoryStateBackend
  readonly #documentStore: WatchStateDocumentStore
  readonly #batchSize: number
  readonly #isProcessAlive: (pid: number) => boolean
  #operation: Promise<void> = Promise.resolve()

  constructor(options: DocumentStateStoreOptions) {
    super(options)
    this.backend = options.backend
    this.#documentStore = options.documentStore
    this.#batchSize = options.batchSize
    this.#isProcessAlive = options.isProcessAlive ?? processIsAlive
  }

  async assertReady(): Promise<void> {
    await this.#exclusive(async () => this.#documentStore.assertReady())
  }

  override async claimDiscoverySweep(
    workspaceId: string,
    owner: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepClaim> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
      const state = workspace.discoverySweep
      if (state.backoffUntilMs > nowMs) {
        return { acquired: false, reason: 'backoff', state: cloneDiscoverySweepState(state) }
      }
      const reclaimedLease = state.lease &&
        state.lease.leaseUntilMs > nowMs &&
        discoveryLeaseOwnerIsOrphaned(state.lease.owner, owner, this.#isProcessAlive)
        ? { ...state.lease }
        : undefined
      if (state.lease && state.lease.leaseUntilMs > nowMs && !reclaimedLease) {
        return { acquired: false, reason: 'in-flight', state: cloneDiscoverySweepState(state) }
      }
      const epoch = state.lastEpoch + 1
      state.lastEpoch = epoch
      state.lease = { owner, epoch, leaseUntilMs: nowMs + leaseMs }
      await this.#persist(document)
      return {
        acquired: true,
        state: cloneDiscoverySweepState(state),
        lease: { ...state.lease },
        ...(reclaimedLease ? { reclaimedLease } : {}),
      }
    }))
  }

  override async renewDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    return (await this.renewDiscoverySweepWithDetails(workspaceId, owner, epoch, nowMs, leaseMs)).renewed
  }

  override async renewDiscoverySweepWithDetails(
    workspaceId: string,
    owner: string,
    epoch: number,
    nowMs: number,
    leaseMs: number,
  ): Promise<DiscoverySweepRenewal> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const state = document.workspaces[workspaceId]?.discoverySweep
      if (!state?.lease) return { renewed: false, reason: 'missing' }
      if (!discoveryLeaseMatches(state, owner, epoch)) {
        return { renewed: false, reason: 'contended', observedLease: { ...state.lease } }
      }
      if (state.lease.leaseUntilMs <= nowMs) {
        return { renewed: false, reason: 'expired', observedLease: { ...state.lease } }
      }
      state.lease!.leaseUntilMs = nowMs + leaseMs
      await this.#persist(document)
      return { renewed: true, lease: { ...state.lease } }
    }))
  }

  override async completeDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    checkpoint?: DiscoveryCheckpoint,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const state = document.workspaces[workspaceId]?.discoverySweep
      if (!state || !discoveryLeaseMatches(state, owner, epoch)) return false
      // A missing checkpoint means finalization couldn't get a watermark or
      // change window this cycle (a transient feed hiccup, not "the tree is
      // now empty") — keep the last good checkpoint so the next sweep can
      // still diff from it instead of falling back to a full walk.
      if (checkpoint) state.checkpoint = cloneDiscoveryCheckpoint(checkpoint)
      state.consecutiveOverloads = 0
      state.backoffUntilMs = 0
      delete state.lease
      await this.#persist(document)
      return true
    }))
  }

  override async deferDiscoverySweep(
    workspaceId: string,
    owner: string,
    epoch: number,
    backoffUntilMs: number,
    consecutiveOverloads: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const state = document.workspaces[workspaceId]?.discoverySweep
      if (!state || !discoveryLeaseMatches(state, owner, epoch)) return false
      state.backoffUntilMs = backoffUntilMs
      state.consecutiveOverloads = consecutiveOverloads
      delete state.lease
      await this.#persist(document)
      return true
    }))
  }

  override async releaseDiscoverySweep(workspaceId: string, owner: string, epoch: number): Promise<void> {
    await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const state = document.workspaces[workspaceId]?.discoverySweep
      if (!state || !discoveryLeaseMatches(state, owner, epoch)) return
      delete state.lease
      const workspace = document.workspaces[workspaceId]!
      if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
      await this.#persist(document)
    }))
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
      if (!lifecycle) {
        const matching = matchingGithubLifecycleEntry(Object.entries(workspace.dispatchLifecycles), seed)
        if (matching) [key, lifecycle] = matching
      }
      const created = !lifecycle
      if (!lifecycle) {
        lifecycle = cloneLifecycle(seed)
        if (activeDispatchLifecycleCount(workspace.dispatchLifecycles) >= this.#batchSize) lifecycle.phase = 'queued'
        workspace.dispatchLifecycles[key] = lifecycle
      }
      const terminal = lifecycle.phase === 'complete' || lifecycle.phase === 'abandoned'
      const activeOtherOwner = lifecycle.lease && lifecycle.lease.owner !== owner && lifecycle.lease.leaseUntilMs > nowMs
      if (terminal || activeOtherOwner) {
        return { key, acquired: false, lifecycle: cloneLifecycle(lifecycle), created }
      }
      const epoch = lifecycle.lease?.owner === owner
        ? lifecycle.lease.epoch
        : (lifecycle.lease?.epoch ?? 0) + 1
      lifecycle.lease = { owner, epoch, leaseUntilMs: nowMs + leaseMs }
      lifecycle.updatedAtMs = nowMs
      await this.#persist(document)
      return {
        key,
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

  override async clearQueuedDispatchLifecycle(
    workspaceId: string,
    key: string,
    expectedLease: DispatchLifecycle['lease'],
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      const lifecycle = workspace?.dispatchLifecycles[key]
      if (lifecycle?.phase !== 'queued' || !dispatchLifecycleLeaseMatches(lifecycle.lease, expectedLease)) {
        return false
      }
      delete workspace!.dispatchLifecycles[key]
      if (workspaceIsEmpty(workspace!)) delete document.workspaces[workspaceId]
      await this.#persist(document)
      return true
    }))
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

  override async markRunning(
    workspaceId: string,
    ownershipKey: string,
    agentName: string,
    nowMs: number,
    leaseMs: number,
    options?: { force?: boolean },
  ): Promise<{ generationId: string } | null> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
      const current = workspace.babysitterGenerations[ownershipKey]
      if (current && (
        current.phase !== 'claimed' ||
        current.leaseUntilMs >= nowMs ||
        options?.force !== true
      )) return null

      const generationId = randomUUID()
      workspace.babysitterGenerations[ownershipKey] = {
        generationId,
        agentName,
        claimedAtMs: nowMs,
        leaseUntilMs: nowMs + leaseMs,
        phase: 'claimed',
      }
      await this.#persist(document)
      return { generationId }
    }))
  }

  override async renewBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const current = document.workspaces[workspaceId]?.babysitterGenerations[ownershipKey]
      if (current?.phase !== 'claimed' || current.generationId !== generationId) return false
      current.leaseUntilMs = nowMs + leaseMs
      await this.#persist(document)
      return true
    }))
  }

  override async durableCompletionCas(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const current = document.workspaces[workspaceId]?.babysitterGenerations[ownershipKey]
      if (current?.phase !== 'claimed' || current.generationId !== generationId) return false
      current.phase = 'completed'
      await this.#persist(document)
      return true
    }))
  }

  override async getBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
  ): Promise<BabysitterGenerationRecord | undefined> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      const current = document.workspaces[workspaceId]?.babysitterGenerations[ownershipKey]
      return current ? cloneBabysitterGeneration(current) : undefined
    })
  }

  override async clearBabysitterGeneration(
    workspaceId: string,
    ownershipKey: string,
    generationId: string,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      if (workspace?.babysitterGenerations[ownershipKey]?.generationId !== generationId) return false
      delete workspace.babysitterGenerations[ownershipKey]
      if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
      await this.#persist(document)
      return true
    }))
  }

  override async reserveConversationSession(
    workspaceId: string,
    conversationId: string,
    session: ConversationSessionState,
  ): Promise<boolean> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId] ??= emptyWorkspaceState()
      if (workspace.conversationSessions[conversationId]) return false
      workspace.conversationSessions[conversationId] = cloneConversationSession(session)
      await this.#persist(document)
      return true
    }))
  }

  override async getConversationSession(
    workspaceId: string,
    conversationId: string,
  ): Promise<ConversationSessionState | undefined> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      const session = document.workspaces[workspaceId]?.conversationSessions[conversationId]
      return session ? cloneConversationSession(session) : undefined
    })
  }

  override async listConversationSessions(
    workspaceId: string,
  ): Promise<Array<[string, ConversationSessionState]>> {
    return await this.#exclusive(async () => {
      const document = await this.#loadFromDisk()
      return Object.entries(document.workspaces[workspaceId]?.conversationSessions ?? {})
        .map(([conversationId, session]) => [conversationId, cloneConversationSession(session)])
    })
  }

  override async appendConversationMessage(
    workspaceId: string,
    conversationId: string,
    message: ConversationMessage,
  ): Promise<ConversationSessionState | undefined> {
    return await this.#mutateConversation(workspaceId, conversationId, (session) => {
      if (conversationHasMessage(session, message.id)) return false
      session.processedMessageIds.push(message.id)
      session.pending.push(structuredClone(message))
      return true
    })
  }

  override async claimConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    nowMs: number,
    leaseMs: number,
  ): Promise<ConversationSessionState | undefined> {
    return await this.#mutateConversation(workspaceId, conversationId, (session) => {
      if (session.delivery && session.delivery.claimedAtMs + leaseMs > nowMs) {
        return false
      }
      const attempts = session.delivery?.attempts ?? 0
      if (session.delivery) session.pending.unshift(...session.delivery.messages)
      session.pending.sort(compareConversationMessages)
      if (session.pending.length === 0) {
        session.delivery = undefined
        return false
      }
      session.delivery = {
        claimId,
        owner,
        claimedAtMs: nowMs,
        attempts: attempts + 1,
        messages: session.pending.splice(0),
        agent: {
          name: session.agent.name,
          sessionRef: session.agent.sessionRef,
        },
      }
      return true
    })
  }

  override async renewConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    nowMs: number,
  ): Promise<boolean> {
    const result = await this.#mutateConversation(workspaceId, conversationId, (session) => {
      if (!session.delivery || session.delivery.owner !== owner || session.delivery.claimId !== claimId) return false
      session.delivery.claimedAtMs = nowMs
      return true
    })
    return Boolean(result)
  }

  override async completeConversationTurn(
    workspaceId: string,
    conversationId: string,
    owner: string,
    claimId: string,
    agent: { name: string; sessionRef?: string },
  ): Promise<boolean> {
    const result = await this.#mutateConversation(workspaceId, conversationId, (session) => {
      if (!session.delivery || session.delivery.owner !== owner || session.delivery.claimId !== claimId) return false
      session.history = [...session.history, ...session.delivery.messages].slice(-CONVERSATION_HISTORY_LIMIT)
      if (
        session.agent.name === session.delivery.agent.name &&
        session.agent.sessionRef === session.delivery.agent.sessionRef
      ) {
        session.agent.name = agent.name
        if (agent.sessionRef) session.agent.sessionRef = agent.sessionRef
      }
      session.delivery = undefined
      return true
    })
    return Boolean(result)
  }

  override async releaseConversationTurn(workspaceId: string, conversationId: string, owner: string, claimId: string): Promise<void> {
    await this.#mutateConversation(workspaceId, conversationId, (session) => {
      if (!session.delivery || session.delivery.owner !== owner || session.delivery.claimId !== claimId) return false
      session.pending.unshift(...session.delivery.messages)
      session.pending.sort(compareConversationMessages)
      session.delivery = undefined
      return true
    })
  }

  override async clearConversationSession(workspaceId: string, conversationId: string): Promise<void> {
    await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const workspace = document.workspaces[workspaceId]
      if (!workspace || !workspace.conversationSessions[conversationId]) return
      delete workspace.conversationSessions[conversationId]
      if (workspaceIsEmpty(workspace)) delete document.workspaces[workspaceId]
      await this.#persist(document)
    }))
  }

  override async rebindConversationSession(
    workspaceId: string,
    conversationId: string,
    agent: ConversationSessionState['agent'],
  ): Promise<boolean> {
    const result = await this.#mutateConversation(workspaceId, conversationId, (session) => {
      session.agent = structuredClone(agent)
      return true
    })
    return Boolean(result)
  }

  async #mutateConversation(
    workspaceId: string,
    conversationId: string,
    mutate: (session: ConversationSessionState) => boolean,
  ): Promise<ConversationSessionState | undefined> {
    return await this.#exclusive(async () => this.#withMutationLock(async () => {
      const document = await this.#loadFromDisk()
      const session = document.workspaces[workspaceId]?.conversationSessions[conversationId]
      if (!session || !mutate(session)) return undefined
      await this.#persist(document)
      return cloneConversationSession(session)
    }))
  }

  async #loadFromDisk(): Promise<WatchStateDocument> {
    return await this.#documentStore.read()
  }

  async #persist(document: WatchStateDocument): Promise<void> {
    await this.#documentStore.write(document)
  }

  async #withMutationLock<T>(operation: () => Promise<T>): Promise<T> {
    return await this.#documentStore.runMutation(operation)
  }

  async #exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#operation.then(operation, operation)
    this.#operation = result.then(() => undefined, () => undefined)
    return await result
  }
}

/** Node/file-backed state with the exact persistence and locking semantics used before the document seam. */
export class FileStateStore extends DocumentStateStore {
  constructor(options: FileStateStoreOptions) {
    super({
      backend: 'file',
      batchSize: options.batchSize,
      isProcessAlive: options.isProcessAlive,
      documentStore: new FileWatchStateDocumentStore(options.watchStatePath),
    })
  }
}

class FileWatchStateDocumentStore implements WatchStateDocumentStore {
  readonly #watchStatePath: string

  constructor(watchStatePath: string) {
    this.#watchStatePath = watchStatePath
  }

  async read(): Promise<WatchStateDocument> {
    try {
      const parsed = JSON.parse(await readFile(this.#watchStatePath, 'utf8')) as unknown
      return parseWatchStateDocument(parsed)
    } catch (error) {
      if (!isMissingFileError(error)) throw error
      return { version: 3, workspaces: {} }
    }
  }

  async write(document: WatchStateDocument): Promise<void> {
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

  async runMutation<T>(operation: () => Promise<T>): Promise<T> {
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

  async assertReady(): Promise<void> {
    await this.read()
  }
}

const cloneWatch = (watch: GithubIssueCommentWatchState): GithubIssueCommentWatchState =>
  structuredClone(watch)

const cloneClarification = (record: WaitingClarification): WaitingClarification =>
  structuredClone(record)

const cloneBabysitterSession = (session: BabysitterSessionState): BabysitterSessionState =>
  structuredClone(session)

const cloneBabysitterGeneration = (record: BabysitterGenerationRecord): BabysitterGenerationRecord =>
  structuredClone(record)

const cloneConversationSession = (session: ConversationSessionState): ConversationSessionState =>
  structuredClone(session)

const cloneDiscoveryCheckpoint = (checkpoint: DiscoveryCheckpoint): DiscoveryCheckpoint =>
  structuredClone(checkpoint)

const cloneDiscoverySweepState = (state: DiscoverySweepState): DiscoverySweepState =>
  structuredClone(state)

const discoveryLeaseMatches = (state: DiscoverySweepState, owner: string, epoch: number): boolean =>
  state.lease?.owner === owner && state.lease.epoch === epoch

const discoveryLeaseOwnerIsOrphaned = (
  incumbentOwner: string,
  claimantOwner: string,
  isProcessAlive: (pid: number) => boolean,
): boolean => {
  if (incumbentOwner === claimantOwner) return false
  const incumbentPid = discoveryLeaseOwnerPid(incumbentOwner)
  if (incumbentPid === undefined) return false
  return !isProcessAlive(incumbentPid)
}

const discoveryLeaseOwnerPid = (owner: string): number | undefined => {
  const match = /^(\d+):/u.exec(owner)
  if (!match) return undefined
  const pid = Number(match[1])
  return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined
}

const processIsAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== 'ESRCH'
  }
}

const CONVERSATION_HISTORY_LIMIT = 50

const conversationHasMessage = (session: ConversationSessionState, id: string): boolean =>
  session.processedMessageIds.includes(id) ||
  session.history.some((message) => message.id === id) ||
  session.pending.some((message) => message.id === id) ||
  Boolean(session.delivery?.messages.some((message) => message.id === id))

const compareConversationMessages = (left: ConversationMessage, right: ConversationMessage): number =>
  left.receivedAtMs - right.receivedAtMs ||
  (left.providerSequence ?? left.id).localeCompare(right.providerSequence ?? right.id, undefined, { numeric: true })

const cloneLifecycle = (record: DispatchLifecycle): DispatchLifecycle => structuredClone(record)

const dispatchLifecycleLeaseMatches = (
  current: DispatchLifecycle['lease'],
  expected: DispatchLifecycle['lease'],
): boolean => current === undefined
  ? expected === undefined
  : expected !== undefined && current.owner === expected.owner && current.epoch === expected.epoch

const activeDispatchLifecycleCount = (lifecycles: Record<string, DispatchLifecycle>, exceptKey?: string): number =>
  Object.entries(lifecycles).filter(([key, lifecycle]) => key !== exceptKey && dispatchLifecycleOccupiesSlot(lifecycle)).length

const dispatchLifecycleOccupiesSlot = (lifecycle: DispatchLifecycle): boolean =>
  lifecycle.phase !== 'queued' &&
  lifecycle.phase !== 'waiting-for-human' &&
  lifecycle.phase !== 'releasing' &&
  lifecycle.phase !== 'complete' &&
  lifecycle.phase !== 'abandoned' &&
  !dispatchLifecycleHandedOffToBabysitters(lifecycle)

const dispatchLifecycleHandedOffToBabysitters = (lifecycle: DispatchLifecycle): boolean => {
  const implementerRepos = [...new Set(lifecycle.decision.implementers.map((spec) => spec.repo))]
  if (implementerRepos.length === 0) return false
  const babysitterRepos = lifecycle.agents
    .filter((agent) => agent.tracked.spec.role === 'babysitter')
    .map((agent) => agent.tracked.spec.ownedPullRequest?.repo)
    .filter((repo): repo is string => Boolean(repo))
  return implementerRepos.every((repo) => babysitterRepos.some((ownedRepo) =>
    githubRepositoriesMatch(repo, ownedRepo)))
}

const emptyWorkspaceState = (): PersistedWorkspaceState => ({
  githubIssueCommentWatches: {},
  waitingClarifications: {},
  babysitterSessions: {},
  babysitterGenerations: {},
  conversationSessions: {},
  dispatchLifecycles: {},
  discoverySweep: emptyDiscoverySweepState(),
})

const workspaceIsEmpty = (workspace: PersistedWorkspaceState): boolean =>
  Object.keys(workspace.githubIssueCommentWatches).length === 0 &&
  Object.keys(workspace.waitingClarifications).length === 0 &&
  Object.keys(workspace.babysitterSessions).length === 0 &&
  Object.keys(workspace.babysitterGenerations).length === 0 &&
  Object.keys(workspace.conversationSessions).length === 0 &&
  Object.keys(workspace.dispatchLifecycles).length === 0 &&
  workspace.discoverySweep.checkpoint === undefined &&
  workspace.discoverySweep.lease === undefined &&
  workspace.discoverySweep.backoffUntilMs <= 0 &&
  workspace.discoverySweep.consecutiveOverloads <= 0 &&
  workspace.discoverySweep.lastEpoch <= 0

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
