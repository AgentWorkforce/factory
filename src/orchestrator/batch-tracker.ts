import type { AgentSpec, SpawnResult } from '../ports'
import type { DispatchResult, IssueRef, TriageDecision } from '../types'

export interface TrackedAgent {
  spec: AgentSpec
  result?: SpawnResult
  sessionRef?: string
}

export interface InFlightIssue {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
  agents: Map<string, TrackedAgent>
  invocationIds: Set<string>
  result?: DispatchResult
}

export interface QueuedIssue {
  issue: IssueRef
  decision: TriageDecision
  dryRun: boolean
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

  canStart(): boolean {
    return this.#inFlight.size < this.#limit
  }

  start(decision: TriageDecision, dryRun: boolean): InFlightIssue | undefined {
    const key = issueKey(decision.issue)
    const existing = this.#inFlight.get(key)
    if (existing) {
      return existing
    }

    if (!this.canStart()) {
      this.queue(decision, dryRun)
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

  queue(decision: TriageDecision, dryRun: boolean): boolean {
    const key = issueKey(decision.issue)
    if (this.#inFlight.has(key) || this.#queued.has(key)) {
      return false
    }

    this.#queued.set(key, { issue: decision.issue, decision, dryRun })
    return true
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

    if (!this.canStart()) {
      return undefined
    }

    const next = this.#queued.values().next().value as QueuedIssue | undefined
    if (next) {
      this.#queued.delete(issueKey(next.issue))
    }

    return next
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

  recordDryRun(record: InFlightIssue, spec: AgentSpec, invocationId: string): void {
    record.invocationIds.add(invocationId)
    this.#invocationIds.add(invocationId)
    record.agents.set(spec.name, {
      spec: { ...spec, invocationId },
      result: { name: spec.name, sessionRef: spec.sessionRef },
      sessionRef: spec.sessionRef,
    })
  }
}

export const issueKey = (issue: IssueRef): string => `${issue.key}:${issue.uuid}:${issue.path}`
