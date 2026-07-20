import type {
  HostedFactoryIssueRecord,
  HostedFactoryLease,
  HostedFactoryLeaseClaim,
  HostedFactoryStateStore,
} from './types'

type WorkspaceState = {
  lease?: HostedFactoryLease
  issues: Map<string, HostedFactoryIssueRecord>
  issueIdByInvocation: Map<string, string>
}

export type InMemoryHostedFactoryStateStoreOptions = {
  now?: () => number
}

/** Test/local implementation with the same owner+epoch+expiry semantics as the DO store. */
export class InMemoryHostedFactoryStateStore implements HostedFactoryStateStore {
  readonly #now: () => number
  readonly #workspaces = new Map<string, WorkspaceState>()

  constructor(options: InMemoryHostedFactoryStateStoreOptions = {}) {
    this.#now = options.now ?? Date.now
  }

  async claimRunLease(
    workspaceId: string,
    ownerId: string,
    ttlMs: number,
  ): Promise<HostedFactoryLeaseClaim> {
    const workspace = this.#workspace(workspaceId)
    const nowMs = this.#now()
    const current = workspace.lease
    if (current && current.expiresAtMs > nowMs && current.ownerId !== ownerId) {
      return { acquired: false, lease: { ...current } }
    }
    const lease: HostedFactoryLease = {
      workspaceId,
      ownerId,
      // A claim is a new fencing operation even when the caller reuses an
      // owner ID. Renew is the only operation that preserves an epoch.
      epoch: (current?.epoch ?? 0) + 1,
      expiresAtMs: nowMs + validTtl(ttlMs),
    }
    workspace.lease = lease
    return { acquired: true, lease: { ...lease } }
  }

  async renewRunLease(
    lease: HostedFactoryLease,
    ttlMs: number,
  ): Promise<HostedFactoryLease | null> {
    const workspace = this.#workspace(lease.workspaceId)
    const nowMs = this.#now()
    if (!matchesLiveLease(workspace.lease, lease, nowMs)) return null
    const renewed = { ...lease, expiresAtMs: nowMs + validTtl(ttlMs) }
    workspace.lease = renewed
    return { ...renewed }
  }

  async releaseRunLease(lease: HostedFactoryLease): Promise<void> {
    const workspace = this.#workspace(lease.workspaceId)
    if (!matchesLease(workspace.lease, lease)) return
    // Keep the epoch tombstone so a later claimant cannot reuse a stale fence.
    workspace.lease = { ...lease, expiresAtMs: 0 }
  }

  async getIssue(workspaceId: string, issueId: string): Promise<HostedFactoryIssueRecord | null> {
    const record = this.#workspace(workspaceId).issues.get(issueId)
    return record ? cloneRecord(record) : null
  }

  async listIssues(workspaceId: string): Promise<HostedFactoryIssueRecord[]> {
    return [...this.#workspace(workspaceId).issues.values()].map(cloneRecord)
  }

  async findIssueByInvocation(
    workspaceId: string,
    invocationId: string,
  ): Promise<HostedFactoryIssueRecord | null> {
    const workspace = this.#workspace(workspaceId)
    const issueId = workspace.issueIdByInvocation.get(invocationId)
    if (!issueId) return null
    const record = workspace.issues.get(issueId)
    return record ? cloneRecord(record) : null
  }

  async saveIssue(record: HostedFactoryIssueRecord, lease: HostedFactoryLease): Promise<boolean> {
    const workspace = this.#workspace(record.workspaceId)
    if (record.workspaceId !== lease.workspaceId || !matchesLiveLease(workspace.lease, lease, this.#now())) {
      return false
    }
    replaceIssue(workspace, record)
    return true
  }

  #workspace(workspaceId: string): WorkspaceState {
    let workspace = this.#workspaces.get(workspaceId)
    if (!workspace) {
      workspace = { issues: new Map(), issueIdByInvocation: new Map() }
      this.#workspaces.set(workspaceId, workspace)
    }
    return workspace
  }
}

export interface HostedFactoryDurableObjectTransaction {
  get<T>(key: string): Promise<T | undefined>
  put<T>(key: string, value: T): Promise<void>
  delete(key: string): Promise<boolean>
  list<T>(options?: { prefix?: string }): Promise<Map<string, T>>
}

export interface HostedFactoryDurableObjectStorage extends HostedFactoryDurableObjectTransaction {
  transaction<T>(
    closure: (transaction: HostedFactoryDurableObjectTransaction) => Promise<T>,
  ): Promise<T>
}

export type DurableObjectHostedFactoryStateStoreOptions = {
  now?: () => number
  keyPrefix?: string
}

/**
 * Durable-Object-backed hosted state. All lease claims and fenced writes run in
 * storage transactions; storing records without this class's fence checks
 * would reintroduce active/active split brain.
 */
export class DurableObjectHostedFactoryStateStore implements HostedFactoryStateStore {
  readonly #storage: HostedFactoryDurableObjectStorage
  readonly #now: () => number
  readonly #keyPrefix: string

  constructor(
    storage: HostedFactoryDurableObjectStorage,
    options: DurableObjectHostedFactoryStateStoreOptions = {},
  ) {
    this.#storage = storage
    this.#now = options.now ?? Date.now
    this.#keyPrefix = options.keyPrefix ?? 'hosted-factory'
  }

  async claimRunLease(
    workspaceId: string,
    ownerId: string,
    ttlMs: number,
  ): Promise<HostedFactoryLeaseClaim> {
    return await this.#storage.transaction(async (transaction) => {
      const key = this.#leaseKey(workspaceId)
      const current = await transaction.get<HostedFactoryLease>(key)
      const nowMs = this.#now()
      if (current && current.expiresAtMs > nowMs && current.ownerId !== ownerId) {
        return { acquired: false, lease: current }
      }
      const lease: HostedFactoryLease = {
        workspaceId,
        ownerId,
        // A claim is a new fencing operation even when the caller reuses an
        // owner ID. Renew is the only operation that preserves an epoch.
        epoch: (current?.epoch ?? 0) + 1,
        expiresAtMs: nowMs + validTtl(ttlMs),
      }
      await transaction.put(key, lease)
      return { acquired: true, lease }
    })
  }

  async renewRunLease(
    lease: HostedFactoryLease,
    ttlMs: number,
  ): Promise<HostedFactoryLease | null> {
    return await this.#storage.transaction(async (transaction) => {
      const key = this.#leaseKey(lease.workspaceId)
      const current = await transaction.get<HostedFactoryLease>(key)
      const nowMs = this.#now()
      if (!matchesLiveLease(current, lease, nowMs)) return null
      const renewed = { ...lease, expiresAtMs: nowMs + validTtl(ttlMs) }
      await transaction.put(key, renewed)
      return renewed
    })
  }

  async releaseRunLease(lease: HostedFactoryLease): Promise<void> {
    await this.#storage.transaction(async (transaction) => {
      const key = this.#leaseKey(lease.workspaceId)
      const current = await transaction.get<HostedFactoryLease>(key)
      if (matchesLease(current, lease)) {
        await transaction.put(key, { ...lease, expiresAtMs: 0 })
      }
    })
  }

  async getIssue(workspaceId: string, issueId: string): Promise<HostedFactoryIssueRecord | null> {
    return await this.#storage.get<HostedFactoryIssueRecord>(this.#issueKey(workspaceId, issueId)) ?? null
  }

  async listIssues(workspaceId: string): Promise<HostedFactoryIssueRecord[]> {
    return [...(await this.#storage.list<HostedFactoryIssueRecord>({
      prefix: this.#issuePrefix(workspaceId),
    })).values()]
  }

  async findIssueByInvocation(
    workspaceId: string,
    invocationId: string,
  ): Promise<HostedFactoryIssueRecord | null> {
    const issueId = await this.#storage.get<string>(this.#invocationKey(workspaceId, invocationId))
    return issueId ? await this.getIssue(workspaceId, issueId) : null
  }

  async saveIssue(record: HostedFactoryIssueRecord, lease: HostedFactoryLease): Promise<boolean> {
    if (record.workspaceId !== lease.workspaceId) return false
    return await this.#storage.transaction(async (transaction) => {
      const currentLease = await transaction.get<HostedFactoryLease>(this.#leaseKey(record.workspaceId))
      if (!matchesLiveLease(currentLease, lease, this.#now())) return false

      const issueKey = this.#issueKey(record.workspaceId, record.issue.uuid)
      const prior = await transaction.get<HostedFactoryIssueRecord>(issueKey)
      const nextInvocationIds = new Set(record.invocations.map(({ invocationId }) => invocationId))
      for (const invocation of prior?.invocations ?? []) {
        if (!nextInvocationIds.has(invocation.invocationId)) {
          await transaction.delete(this.#invocationKey(record.workspaceId, invocation.invocationId))
        }
      }
      await transaction.put(issueKey, record)
      for (const invocation of record.invocations) {
        await transaction.put(
          this.#invocationKey(record.workspaceId, invocation.invocationId),
          record.issue.uuid,
        )
      }
      return true
    })
  }

  #workspacePrefix(workspaceId: string): string {
    return `${this.#keyPrefix}:workspace:${encodeKey(workspaceId)}:`
  }

  #leaseKey(workspaceId: string): string {
    return `${this.#workspacePrefix(workspaceId)}lease`
  }

  #issuePrefix(workspaceId: string): string {
    return `${this.#workspacePrefix(workspaceId)}issue:`
  }

  #issueKey(workspaceId: string, issueId: string): string {
    return `${this.#issuePrefix(workspaceId)}${encodeKey(issueId)}`
  }

  #invocationKey(workspaceId: string, invocationId: string): string {
    return `${this.#workspacePrefix(workspaceId)}invocation:${encodeKey(invocationId)}`
  }
}

function replaceIssue(workspace: WorkspaceState, record: HostedFactoryIssueRecord): void {
  const prior = workspace.issues.get(record.issue.uuid)
  for (const invocation of prior?.invocations ?? []) {
    workspace.issueIdByInvocation.delete(invocation.invocationId)
  }
  const next = cloneRecord(record)
  workspace.issues.set(record.issue.uuid, next)
  for (const invocation of next.invocations) {
    workspace.issueIdByInvocation.set(invocation.invocationId, record.issue.uuid)
  }
}

function matchesLease(
  current: HostedFactoryLease | undefined,
  expected: HostedFactoryLease,
): boolean {
  return current?.workspaceId === expected.workspaceId &&
    current.ownerId === expected.ownerId &&
    current.epoch === expected.epoch
}

function matchesLiveLease(
  current: HostedFactoryLease | undefined,
  expected: HostedFactoryLease,
  nowMs: number,
): boolean {
  return matchesLease(current, expected) && current!.expiresAtMs > nowMs
}

function validTtl(ttlMs: number): number {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new Error('hosted Factory lease TTL must be greater than zero')
  }
  return Math.trunc(ttlMs)
}

function cloneRecord(record: HostedFactoryIssueRecord): HostedFactoryIssueRecord {
  return structuredClone(record)
}

function encodeKey(value: string): string {
  return encodeURIComponent(value)
}
