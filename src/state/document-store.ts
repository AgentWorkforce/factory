import type {
  BabysitterGenerationRecord,
  BabysitterSessionState,
  ConversationSessionState,
  DiscoverySweepState,
  DispatchLifecycle,
  GithubIssueCommentWatchState,
  WaitingClarification,
} from '../ports/state'

export type PersistedWorkspaceState = {
  githubIssueCommentWatches: Record<string, GithubIssueCommentWatchState>
  waitingClarifications: Record<string, WaitingClarification>
  babysitterSessions: Record<string, BabysitterSessionState>
  babysitterGenerations: Record<string, BabysitterGenerationRecord>
  conversationSessions: Record<string, ConversationSessionState>
  dispatchLifecycles: Record<string, DispatchLifecycle>
  discoverySweep: DiscoverySweepState
}

export type WatchStateDocument = {
  version: 3
  workspaces: Record<string, PersistedWorkspaceState>
}

/**
 * Persistence seam for the document-backed StateStore behavior.
 *
 * Implementations must make each runMutation callback serializable with every
 * other writer. A local file implementation can hold a filesystem lock; a
 * remote implementation can use revisioned compare-and-set and retry the pure
 * callback after a conflict.
 */
export interface WatchStateDocumentStore {
  read(): Promise<WatchStateDocument>
  write(document: WatchStateDocument): Promise<void>
  runMutation<T>(operation: () => Promise<T>): Promise<T>
  assertReady(): Promise<void>
}

export class WatchStateDocumentConflictError extends Error {
  constructor(message = 'Factory state document changed concurrently') {
    super(message)
    this.name = 'WatchStateDocumentConflictError'
  }
}
