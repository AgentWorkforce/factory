import type {
  ChangeEvent as RelayFileChangeEvent,
  Subscription as RelayFileSubscription,
} from '@relayfile/sdk'

export type ChangeEvent = RelayFileChangeEvent
export type Subscription = RelayFileSubscription
export type SubscribeOptions = {
  coalesce?: 'none' | 'fire-once'
  coalesceMs?: number
  pathScope?: string[]
  from?: 'now' | 'legacy'
  onCoalesced?: () => void
  onQueueDepth?: (depth: number) => void
}

export interface EventPage {
  events: ChangeEvent[]
  nextCursor?: string | null
}

export interface ProviderSyncStatus {
  provider?: string
  status?: string
  lastEventAt?: string
  lastEventAtMs?: number
  watermarkTs?: string | null
  lagSeconds?: number
  /** Independent health of provider webhook delivery, when reported by the mount. */
  webhookHealthy?: boolean
}

export interface LocalMountOptions {
  /** Alternate identifiers for the same workspace as recorded by the mount. */
  acceptableWorkspaceIds?: readonly string[]
  /** Refresh a stale mount instead of returning a warning. Defaults to true. */
  refreshStaleMount?: boolean
  /** Optional readiness controls for constrained environments and tests. */
  stateWaitTimeoutMs?: number
  stateWaitPollMs?: number
}

export interface GithubPublishPullRequestInput {
  repo: string
  /** Local checkout fallback for internal/local dispatches. */
  clonePath?: string
  /** Exact branch already pushed by a remote implementer. Avoids reading its node-local clone. */
  headRef?: string
  headSha?: string
  baseRef: string
  title: string
  body: string
}

export interface GithubPublishPullRequestResult {
  repo: string
  number: number
  url: string
  headRef: string
  headSha?: string
}

/**
 * GitHub mutations that require the authenticated workspace connection. The
 * concrete mount translates these operations into file-native Relayfile
 * writeback drafts interpreted by the server-side GitHub adapter.
 */
export interface GithubConnectionWrite {
  publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult>
  closePullRequest(input: { repo: string; number: number }): Promise<void>
}

export interface MountClient {
  readonly writebackTransport?: 'relayfile-cloud' | 'test'
  readonly githubWrite?: GithubConnectionWrite
  /** Ensure the SDK-authenticated Relayfile mirror exists below a checkout. */
  ensureLocalMount?(startDir: string, options?: LocalMountOptions): Promise<void>
  /** Stop SDK-owned local mount processes created by this client. */
  dispose?(): Promise<void>
  readFile(path: string): Promise<{ content: unknown; revision?: string }>
  writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void>
  deleteFile(path: string): Promise<void>
  setDefaultAllowedDraftPredicate?(
    predicate: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>,
  ): void
  listTree(prefix: string): Promise<string[]>
  subscribe(globs: string[], onChange: (event: ChangeEvent) => void, opts?: SubscribeOptions): Subscription
  getEvents(opts: { cursor?: string; limit?: number; provider?: string; last?: number }): Promise<EventPage>
  getEventHighWatermark?(opts?: { provider?: string }): Promise<string | undefined>
  getSyncStatus?(provider: string): Promise<ProviderSyncStatus | undefined>
  confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'>
  /** Provider failure detail retained for a completed failed write, when available. */
  getConfirmedWriteFailureReason?(path: string): Promise<string | undefined>
  /** Provider object id returned by the acknowledged write operation, when available. */
  getConfirmedWriteExternalId?(path: string): Promise<string | undefined>
  ensureSubRoot(prefix: string, opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'>
}
