import type {
  ChangeEvent as RelayFileChangeEvent,
  Subscription as RelayFileSubscription,
} from '@relayfile/sdk'
import type { ResourceSubscriptionsClient } from '../subscriptions/resource-subscriptions'

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
  /** Suppress routine stale-refresh progress lines so a caller can summarize them. */
  suppressStaleRefreshLogs?: boolean
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
  /** Provider-confirmed login or identity label used to author the PR. */
  author?: string
}

export interface GithubPullRequestRef {
  repo: string
  number: number
}

export type FactoryIntegrationProvider = 'github' | 'linear'

export interface FactoryIntegrationConnectionStatus {
  ready: boolean
  state?: string
  initialSyncState?: string
}

export interface FactoryIntegrationConnectResult {
  alreadyConnected: boolean
  connectLink: string | null
  connectionId: string
}

/** Relayfile SDK control-plane surface used by the CLI connection preflight. */
export interface FactoryIntegrationConnections {
  getStatus(provider: FactoryIntegrationProvider): Promise<FactoryIntegrationConnectionStatus>
  connect(provider: FactoryIntegrationProvider): Promise<FactoryIntegrationConnectResult>
  waitForConnection(provider: FactoryIntegrationProvider, connectionId: string): Promise<void>
}

/**
 * GitHub mutations that require the authenticated workspace connection. The
 * concrete mount translates these operations into file-native Relayfile
 * writeback drafts interpreted by the server-side GitHub adapter.
 */
export interface GithubConnectionWrite {
  publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult>
  requestPullRequestReview?(input: GithubPullRequestRef): Promise<void>
  closePullRequest(input: { repo: string; number: number }): Promise<void>
}

export interface MountClient {
  readonly writebackTransport?: 'relayfile-cloud' | 'test'
  readonly githubWrite?: GithubConnectionWrite
  /**
   * Optional durable Relayfile resource-subscription API. Its absence means
   * this mount targets a pre-subscription service and consumers retain their
   * established local routing behaviour.
   */
  readonly resourceSubscriptions?: ResourceSubscriptionsClient
  readonly integrationConnections?: FactoryIntegrationConnections
  /** Ensure the SDK-authenticated Relayfile mirror exists below a checkout. */
  ensureLocalMount?(startDir: string, options?: LocalMountOptions): Promise<void>
  /**
   * Whether a local mount is terminally degraded because the cloud session
   * lacks the filesystem scope the mount needs. When true, the mirror is
   * read-denied and cannot recover without re-auth + restart, so consumers must
   * refuse to spawn or resume agents against it. Absent on mounts that cannot
   * report scope health (e.g. the test transport).
   */
  isLocalMountAuthDegraded?(): boolean
  /** Stop SDK-owned local mount processes created by this client. */
  dispose?(): Promise<void>
  readFile(path: string): Promise<{ content: unknown; revision?: string }>
  writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void>
  /**
   * Atomically create a path without replacing an existing record.
   * `exists` means the store observed a revision conflict; callers must still
   * verify that the winner wrote the record they intended.
   */
  createFile?(
    path: string,
    content: unknown,
    opts?: { guarded?: boolean; correlationId?: string },
  ): Promise<'created' | 'exists'>
  deleteFile(path: string): Promise<void>
  setDefaultAllowedDraftPredicate?(
    predicate: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>,
  ): void
  setDefaultAllowedDeletePredicate?(
    predicate: (path: string, content: unknown) => boolean | Promise<boolean>,
  ): void
  listTree(prefix: string): Promise<string[]>
  subscribe(globs: string[], onChange: (event: ChangeEvent) => void, opts?: SubscribeOptions): Subscription
  getEvents(opts: { cursor?: string; limit?: number; provider?: string; last?: number }): Promise<EventPage>
  getEventHighWatermark?(opts?: { provider?: string }): Promise<string | undefined>
  getSyncStatus?(provider: string): Promise<ProviderSyncStatus | undefined>
  confirmWrite(
    path: string,
    opts?: { timeoutMs?: number; returnFailed?: boolean; correlationId?: string },
  ): Promise<'acked' | 'pending' | 'failed' | 'timeout'>
  /** Provider failure detail retained for a completed failed write, when available. */
  getConfirmedWriteFailureReason?(path: string): Promise<string | undefined>
  /** Provider object id returned by the acknowledged write operation, when available. */
  getConfirmedWriteExternalId?(path: string): Promise<string | undefined>
  ensureSubRoot(prefix: string, opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'>
}
