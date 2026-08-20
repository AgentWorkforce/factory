import type { AgentSpec, SpawnResult } from '../ports'
import type { DispatchLifecyclePhase } from '../ports/state'
import type { DispatchResult, FactoryDispatchClaimStatus, IssueRef, TriageDecision } from '../types'
import { githubRepositoriesMatch } from '../github/repo-identity'

export interface TrackedAgent {
  spec: AgentSpec
  result?: SpawnResult
  sessionRef?: string
  /** Session lineage already resumed after Relay could not address a babysitter wake. */
  unreachableWakeResumedSessionRef?: string
  /**
   * Set when this placement was deliberately released. A released agent keeps
   * its bookkeeping entry for reporting, but it is no longer a live worker, so
   * it can never suppress a later respawn of the same invocation.
   */
  releasedAtMs?: number
}

export interface InFlightIssue {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  agents: Map<string, TrackedAgent>
  invocationIds: Set<string>
  result?: DispatchResult
  dispatchClaim?: FactoryDispatchClaimStatus
  /** Wall-clock anchor set when the first agent placement succeeds. */
  heldSinceAtMs?: number
  /**
   * Wall-clock anchor set when this record took a `batchSize` slot (#303).
   *
   * The only deadline a lifecycle that never placed an agent has: it holds a
   * slot, `heldSinceAtMs` is never stamped, and nothing else will move it.
   */
  slotHeldSinceAtMs?: number
  /** Latest durable phase, used only for operator-facing held-agent status. */
  lifecyclePhase?: DispatchLifecyclePhase
}

export interface QueuedIssue {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
}

export interface DependencyBlocker {
  /** Canonical repo+number identity used for dependency graph matching. */
  identity: string
  /** Resolved composite issueKey() when the blocker exists in the mounted issue index. */
  key: string
  /** Operator-facing owner/repo#number reference. */
  label: string
}

export interface DependencyAdmission {
  blockers: DependencyBlocker[]
  cycle?: string[]
}

export interface ParkedIssue extends QueuedIssue {
  blockers: DependencyBlocker[]
  cycle?: string[]
  /** Capacity and dependencies are independent admission predicates. */
  capacityBlocked: boolean
}

const stableHash = (input: string): string => {
  let hash = 0x811c9dc5
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }

  return (hash >>> 0).toString(36)
}

export class BatchTracker {
  readonly #limit: number
  readonly #inFlight = new Map<string, InFlightIssue>()
  readonly #queued = new Map<string, QueuedIssue>()
  readonly #parked = new Map<string, ParkedIssue>()
  readonly #invocationIds = new Set<string>()

  constructor(batchSize: number) {
    this.#limit = Math.max(1, Math.min(5, Math.trunc(batchSize)))
  }

  get size(): number {
    return this.#inFlight.size
  }

  get inFlight(): InFlightIssue[] {
    return [...this.#inFlight.values()]
  }

  get queued(): QueuedIssue[] {
    return [...this.#queued.values()]
  }

  get parked(): ParkedIssue[] {
    return [...this.#parked.values()]
  }

  getIssue(issue: IssueRef): InFlightIssue | undefined {
    return this.#inFlight.get(issueKey(issue))
  }

  getIssueByAgent(name: string): InFlightIssue | undefined {
    return this.inFlight.find((record) => record.agents.has(name))
  }

  isInFlight(issue: IssueRef): boolean {
    return this.#inFlight.has(issueKey(issue))
  }

  isQueued(issue: IssueRef): boolean {
    return this.#queued.has(issueKey(issue))
  }

  isParked(issue: IssueRef): boolean {
    return this.#parked.has(issueKey(issue))
  }

  getParked(issue: IssueRef): ParkedIssue | undefined {
    return this.#parked.get(issueKey(issue))
  }

  canStart(): boolean {
    return [...this.#inFlight.values()].filter(dispatchOccupiesImplementationSlot).length < this.#limit
  }

  start(
    decision: TriageDecision,
    dryRun: boolean,
    dependencyAdmission: DependencyAdmission = { blockers: [] },
  ): InFlightIssue | undefined {
    const key = issueKey(decision.issue)
    const existing = this.#inFlight.get(key)
    if (existing) {
      return existing
    }

    if (dependencyAdmission.blockers.length > 0 || (dependencyAdmission.cycle?.length ?? 0) > 0) {
      this.#park(decision, dryRun, dependencyAdmission)
      return undefined
    }

    this.#parked.delete(key)

    if (!this.canStart()) {
      this.queue(decision, dryRun, dependencyAdmission)
      return undefined
    }

    const record: InFlightIssue = {
      issue: decision.issue,
      decision,
      dryRun,
      agents: new Map(),
      invocationIds: new Set(),
    }
    this.#inFlight.set(key, record)
    this.#queued.delete(key)
    return record
  }

  queue(
    decision: TriageDecision,
    dryRun: boolean,
    dependencyAdmission: DependencyAdmission = { blockers: [] },
  ): boolean {
    const key = issueKey(decision.issue)
    if (this.#inFlight.has(key)) {
      return false
    }

    if (dependencyAdmission.blockers.length > 0 || (dependencyAdmission.cycle?.length ?? 0) > 0) {
      return this.#park(decision, dryRun, dependencyAdmission)
    }

    this.#parked.delete(key)
    if (this.#queued.has(key)) return false

    this.#queued.set(key, { issue: decision.issue, decision, dryRun })
    return true
  }

  clearPark(issue: IssueRef): void {
    this.#parked.delete(issueKey(issue))
  }

  complete(issue: IssueRef): QueuedIssue | undefined {
    const key = issueKey(issue)
    const record = this.#inFlight.get(key)
    if (record) {
      for (const invocationId of record.invocationIds) {
        this.#invocationIds.delete(invocationId)
      }
    }
    this.#inFlight.delete(key)
    this.#parked.delete(key)

    if (!this.canStart()) {
      return undefined
    }

    const next = this.#queued.values().next().value as QueuedIssue | undefined
    if (next) {
      this.#queued.delete(issueKey(next.issue))
    }

    return next
  }

  abandon(issue: IssueRef): void {
    const key = issueKey(issue)
    const record = this.#inFlight.get(key)
    if (record) {
      for (const invocationId of record.invocationIds) {
        this.#invocationIds.delete(invocationId)
      }
    }
    this.#inFlight.delete(key)
    this.#queued.delete(key)
    this.#parked.delete(key)
  }

  invocationIdFor(issue: IssueRef, spec: AgentSpec): string {
    return spec.invocationId ?? `factory:${issue.key}:${stableHash(`${issue.uuid}:${spec.role}:${spec.name}:${spec.repo}`)}`
  }

  /**
   * A recorded invocation suppresses a respawn only while a live worker still
   * carries it. `recordPlanned` deliberately records nothing here: an owner
   * that dies between planning and spawn confirmation leaves the invocation
   * unrecorded, so takeover retries it as-is and the roster lookup in
   * `#spawnAgent` adopts the surviving worker instead of duplicating it.
   *
   * A deliberate release is the opposite case. The worker is gone, so the
   * memory of its spawn must not answer for it — otherwise the dispatch gate
   * reports a synthetic success and the state store believes work is running
   * that no process exists for. This derives liveness from the tracked agents
   * rather than trusting the id set, so a release path that drops or marks its
   * agent invalidates the claim without having to remember to say so.
   */
  shouldSpawn(record: InFlightIssue, invocationId: string): boolean {
    if (!record.invocationIds.has(invocationId) && !this.#invocationIds.has(invocationId)) return true
    return !this.#hasLiveInvocation(record, invocationId)
  }

  recordSpawn(record: InFlightIssue, spec: AgentSpec, invocationId: string, result: SpawnResult): void {
    record.invocationIds.add(invocationId)
    this.#invocationIds.add(invocationId)
    record.agents.set(result.name, {
      spec: { ...spec, invocationId },
      result,
      sessionRef: result.sessionRef ?? spec.sessionRef,
    })
  }

  /**
   * Invalidate the spawn memory for an agent that was deliberately released.
   * Returns the invocation id that was released, for callers that log it.
   */
  recordRelease(record: InFlightIssue, agentName: string, releasedAtMs: number): string | undefined {
    const tracked = record.agents.get(agentName)
      ?? [...record.agents.values()].find((candidate) => candidate.result?.name === agentName)
    if (!tracked) return undefined
    tracked.releasedAtMs ??= releasedAtMs
    const invocationId = tracked.spec.invocationId ?? this.invocationIdFor(record.issue, tracked.spec)
    record.invocationIds.delete(invocationId)
    // The tracker-global set is what blocks a different in-flight record from
    // reusing the id, so a release has to clear both or the claim survives the
    // worker in another record's name.
    if (!this.#hasLiveInvocation(record, invocationId)) this.#invocationIds.delete(invocationId)
    return invocationId
  }

  #hasLiveInvocation(record: InFlightIssue, invocationId: string): boolean {
    const records = [record, ...this.#inFlight.values()]
    return records.some((candidate) => [...candidate.agents.values()].some((tracked) =>
      tracked.result !== undefined &&
      tracked.releasedAtMs === undefined &&
      tracked.spec.invocationId === invocationId))
  }

  recordPlanned(record: InFlightIssue, spec: AgentSpec): void {
    // A released placement is not a plan still in flight. Replace it so the
    // retry's own spec is what a takeover reads, instead of the dead
    // generation's spawn result.
    const existing = record.agents.get(spec.name)
    if (existing && existing.releasedAtMs === undefined) return
    record.agents.set(spec.name, { spec: { ...spec }, sessionRef: spec.sessionRef })
  }

  recordDryRun(record: InFlightIssue, spec: AgentSpec, invocationId: string): void {
    record.invocationIds.add(invocationId)
    this.#invocationIds.add(invocationId)
    record.agents.set(spec.name, {
      spec: { ...spec, invocationId },
      result: { name: spec.name, sessionRef: spec.sessionRef },
      sessionRef: spec.sessionRef,
    })
  }

  /** Restore a crash-safe lifecycle before the fleet emits reconciled exits. */
  restore(record: InFlightIssue): InFlightIssue {
    const key = issueKey(record.issue)
    const existing = this.#inFlight.get(key)
    if (existing) return existing
    const restored: InFlightIssue = {
      issue: { ...record.issue },
      decision: structuredClone(record.decision),
      dryRun: record.dryRun,
      agents: new Map([...record.agents].map(([name, tracked]) => [name, {
        spec: structuredClone(tracked.spec),
        result: tracked.result ? { ...tracked.result } : undefined,
        sessionRef: tracked.sessionRef,
        releasedAtMs: tracked.releasedAtMs,
      }])),
      invocationIds: new Set(record.invocationIds),
      result: record.result ? structuredClone(record.result) : undefined,
      heldSinceAtMs: record.heldSinceAtMs,
      slotHeldSinceAtMs: record.slotHeldSinceAtMs,
      lifecyclePhase: record.lifecyclePhase,
    }
    this.#inFlight.set(key, restored)
    this.#parked.delete(key)
    for (const invocationId of restored.invocationIds) this.#invocationIds.add(invocationId)
    return restored
  }

  #park(decision: TriageDecision, dryRun: boolean, admission: DependencyAdmission): boolean {
    const key = issueKey(decision.issue)
    const existing = this.#parked.get(key)
    const parked: ParkedIssue = {
      issue: decision.issue,
      decision,
      dryRun,
      blockers: admission.blockers.map((blocker) => ({ ...blocker })),
      cycle: admission.cycle ? [...admission.cycle] : undefined,
      capacityBlocked: !this.canStart(),
    }
    this.#queued.delete(key)
    this.#parked.set(key, parked)
    if (!existing) return true
    return parkedSignature(existing) !== parkedSignature(parked)
  }
}

export const issueKey = (issue: IssueRef): string => `${issue.key}:${issue.uuid}:${issue.path}`

const parkedSignature = (issue: ParkedIssue): string => JSON.stringify({
  blockers: issue.blockers.map(({ identity, key }) => ({ identity, key })),
  cycle: issue.cycle,
  capacityBlocked: issue.capacityBlocked,
})

/**
 * Batch size limits active implementation work, not PR stewardship. Once each
 * implementer repository has a dedicated babysitter, the durable lifecycle
 * remains addressable for review events without starving new ready issues.
 */
const dispatchOccupiesImplementationSlot = (record: InFlightIssue): boolean => {
  const implementerRepos = [...new Set(record.decision.implementers.map((spec) => spec.repo))]
  if (implementerRepos.length === 0) return true
  const babysitterRepos = [...record.agents.values()]
    .filter((agent) => agent.spec.role === 'babysitter')
    .map((agent) => agent.spec.ownedPullRequest?.repo)
    .filter((repo): repo is string => Boolean(repo))
  return implementerRepos.some((repo) => !babysitterRepos.some((ownedRepo) =>
    githubRepositoriesMatch(repo, ownedRepo)))
}
