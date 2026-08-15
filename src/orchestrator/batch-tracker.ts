import type { AgentSpec, SpawnResult } from '../ports'
import type { DispatchResult, FactoryDispatchClaimStatus, IssueRef, TriageDecision } from '../types'
import { githubRepositoriesMatch } from '../github/repo-identity'

export interface TrackedAgent {
  spec: AgentSpec
  result?: SpawnResult
  sessionRef?: string
  /** Session lineage already resumed after Relay could not address a babysitter wake. */
  unreachableWakeResumedSessionRef?: string
}

export interface InFlightIssue {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  agents: Map<string, TrackedAgent>
  invocationIds: Set<string>
  result?: DispatchResult
  dispatchClaim?: FactoryDispatchClaimStatus
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

  shouldSpawn(record: InFlightIssue, invocationId: string): boolean {
    return !record.invocationIds.has(invocationId) && !this.#invocationIds.has(invocationId)
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

  recordPlanned(record: InFlightIssue, spec: AgentSpec): void {
    if (record.agents.has(spec.name)) return
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
      }])),
      invocationIds: new Set(record.invocationIds),
      result: record.result ? structuredClone(record.result) : undefined,
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
