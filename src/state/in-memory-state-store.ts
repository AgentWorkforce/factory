import { BatchTracker } from '../orchestrator/batch-tracker'
import type {
  BatchSnapshot,
  CriticalRecord,
  DispatchAttemptState,
  RegistryHandoffAgent,
  StateStore,
} from '../ports/state'

type WorkspaceState = {
  batch: BatchTracker
  criticalMessages: Map<string, CriticalRecord>
  resumedExitKeys: Set<string>
  slackThreadIds: Map<string, string>
  seenAgentQuestionKeys: Set<string>
  seenAgentQuestionOrder: string[]
  dispatchAttempts: Map<string, DispatchAttemptState>
  canonicalIssueStates: Map<string, string>
  dispatchFailureReaperHandoffs: Map<string, RegistryHandoffAgent>
}

export type InMemoryStateStoreOptions = {
  batchSize: number
  agentQuestionDedupeLimit?: number
}

export class InMemoryStateStore implements StateStore {
  readonly #batchSize: number
  readonly #agentQuestionDedupeLimit: number
  readonly #workspaces = new Map<string, WorkspaceState>()

  constructor(options: InMemoryStateStoreOptions) {
    this.#batchSize = options.batchSize
    this.#agentQuestionDedupeLimit = Math.max(1, Math.trunc(options.agentQuestionDedupeLimit ?? 500))
  }

  async getBatch(workspaceId: string): Promise<BatchSnapshot> {
    return this.#workspace(workspaceId).batch
  }

  async recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void> {
    this.#workspace(workspaceId).dispatchAttempts.set(issueKey, { ...attempt })
  }

  async getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined> {
    const attempt = this.#workspace(workspaceId).dispatchAttempts.get(issueKey)
    return attempt ? { ...attempt } : undefined
  }

  async releaseInFlight(workspaceId: string, issueKey: string): Promise<void> {
    const attempt = this.#workspace(workspaceId).dispatchAttempts.get(issueKey)
    if (attempt) {
      attempt.inFlight = false
    }
  }

  async recordCritical(workspaceId: string, key: string, value: CriticalRecord): Promise<void> {
    this.#workspace(workspaceId).criticalMessages.set(key, value)
  }

  async consumeCritical(workspaceId: string, key: string): Promise<CriticalRecord | undefined> {
    const critical = this.#workspace(workspaceId).criticalMessages.get(key)
    if (critical) {
      this.#workspace(workspaceId).criticalMessages.delete(key)
    }
    return critical
  }

  async isResumed(workspaceId: string, exitKey: string): Promise<boolean> {
    return this.#workspace(workspaceId).resumedExitKeys.has(exitKey)
  }

  async markResumed(workspaceId: string, exitKey: string): Promise<void> {
    this.#workspace(workspaceId).resumedExitKeys.add(exitKey)
  }

  async setSlackThread(workspaceId: string, issueKey: string, threadId: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.set(issueKey, threadId)
  }

  async getSlackThread(workspaceId: string, issueKey: string): Promise<string | undefined> {
    return this.#workspace(workspaceId).slackThreadIds.get(issueKey)
  }

  async clearSlackThread(workspaceId: string, issueKey: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.delete(issueKey)
  }

  async clearSlackThreads(workspaceId: string): Promise<void> {
    this.#workspace(workspaceId).slackThreadIds.clear()
  }

  async seenAgentQuestion(workspaceId: string, key: string): Promise<boolean> {
    return this.#workspace(workspaceId).seenAgentQuestionKeys.has(key)
  }

  async markAgentQuestion(workspaceId: string, key: string): Promise<void> {
    this.#rememberAgentQuestion(this.#workspace(workspaceId), key)
  }

  async claimAgentQuestion(workspaceId: string, key: string): Promise<boolean> {
    const state = this.#workspace(workspaceId)
    if (state.seenAgentQuestionKeys.has(key)) {
      return false
    }
    this.#rememberAgentQuestion(state, key)
    return true
  }

  #rememberAgentQuestion(state: WorkspaceState, key: string): void {
    state.seenAgentQuestionKeys.add(key)
    state.seenAgentQuestionOrder.push(key)
    while (state.seenAgentQuestionOrder.length > this.#agentQuestionDedupeLimit) {
      const oldest = state.seenAgentQuestionOrder.shift()
      if (oldest) {
        state.seenAgentQuestionKeys.delete(oldest)
      }
    }
  }

  async recordFailureHandoff(workspaceId: string, key: string, handoff: RegistryHandoffAgent): Promise<void> {
    this.#workspace(workspaceId).dispatchFailureReaperHandoffs.set(key, handoff)
  }

  async getFailureHandoff(workspaceId: string, key: string): Promise<RegistryHandoffAgent | undefined> {
    return this.#workspace(workspaceId).dispatchFailureReaperHandoffs.get(key)
  }

  async listFailureHandoffs(workspaceId: string): Promise<Array<[string, RegistryHandoffAgent]>> {
    return [...this.#workspace(workspaceId).dispatchFailureReaperHandoffs]
  }

  async clearFailureHandoff(workspaceId: string, key: string): Promise<void> {
    this.#workspace(workspaceId).dispatchFailureReaperHandoffs.delete(key)
  }

  async recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void> {
    this.#workspace(workspaceId).canonicalIssueStates.set(key, stateId)
  }

  async getCanonicalState(workspaceId: string, key: string): Promise<string | undefined> {
    return this.#workspace(workspaceId).canonicalIssueStates.get(key)
  }

  #workspace(workspaceId: string): WorkspaceState {
    let state = this.#workspaces.get(workspaceId)
    if (!state) {
      state = {
        batch: new BatchTracker(this.#batchSize),
        criticalMessages: new Map(),
        resumedExitKeys: new Set(),
        slackThreadIds: new Map(),
        seenAgentQuestionKeys: new Set(),
        seenAgentQuestionOrder: [],
        dispatchAttempts: new Map(),
        canonicalIssueStates: new Map(),
        dispatchFailureReaperHandoffs: new Map(),
      }
      this.#workspaces.set(workspaceId, state)
    }
    return state
  }
}
