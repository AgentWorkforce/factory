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

export interface LocalMountHealth {
  degraded: boolean
  reason?: string
  localDir?: string
}

export interface GithubPublishPullRequestInput {
  repo: string
  /** Local checkout fallback for internal/local dispatches. */
  clonePath?: string
  /** Exact branch already pushed by a remote implementer. Avoids reading its node-local clone. */
  headRef?: string
  /** Factory-derived branch that must be the PR head before any push or PR mutation occurs. */
  expectedHeadRef?: string
  headSha?: string
  baseRef: string
  title: string
  body: string
  /**
   * Per-agent session reference forwarded to the attestation ledger. Takes
   * precedence over the process-wide RELAY_ATTEST_SESSION_ID env var so that
   * concurrent implementers each record their own session, not a shared one.
   */
  sessionRef?: string
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

/**
 * Provider-authoritative GitHub issue returned by the direct API fallback.
 * `content` intentionally uses the same provider record shape as the
 * Relayfile projection so the existing parser and safety gates stay
 * authoritative.
 */
export interface GithubConnectionIssue {
  repo: string
  number: number
  path: string
  content: unknown
}

/**
 * Outcome of a GitHub issue lookup. A reader that cannot authenticate can
 * only prove absence within what it can see: `not-found` means the reader
 * confirmed the repository is visible and the issue is not in it;
 * `indeterminate` means the reader could not establish that (for example an
 * unauthenticated 404 against a repository it also cannot confirm is
 * public — GitHub returns the same 404 for "does not exist" and "private,
 * hidden from you"). Callers must not treat `indeterminate` as absence.
 */
export type GithubIssueLookup =
  | { outcome: 'found'; issue: GithubConnectionIssue }
  | { outcome: 'not-found' }
  | { outcome: 'indeterminate'; reason: string }

export interface GithubConnectionRead {
  getIssue(repo: string, number: number): Promise<GithubIssueLookup>
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
  /** Authenticated issue read through the same connected GitHub App, when supported. */
  getIssue?(repo: string, number: number): Promise<GithubIssueLookup>
  publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult>
  closePullRequest(input: { repo: string; number: number }): Promise<void>
  /** App-authored issue comment through the workspace GitHub connection. */
  postIssueComment?(input: {
    repo: string
    number: number
    body: string
    author: 'app'
  }): Promise<void>
  /** Idempotently provision a repository label through the connected App. */
  ensureRepositoryLabel?(input: {
    repo: string
    name: string
    color: string
    description: string
    author: 'app'
  }): Promise<void>
  /** Add or remove exactly one issue label without replacing unrelated labels. */
  mutateIssueLabel?(input: {
    repo: string
    number: number
    operation: 'add' | 'remove'
    label: string
    author: 'app'
  }): Promise<GithubConnectionMutationReceipt | void>
  /** App-authored partial issue update through the workspace GitHub connection. */
  updateIssue?(input: GithubConnectionIssueUpdateInput): Promise<void>
}

/** Whether a connected App mutation proves it created the visible change. */
export type GithubConnectionMutationReceipt = 'applied' | 'already-matched' | 'acknowledged'

type GithubConnectionIssueUpdateTarget = {
  repo: string
  number: number
  author: 'app'
}

/** At least one mutable issue field must be present. */
export type GithubConnectionIssueUpdateInput = GithubConnectionIssueUpdateTarget & (
  | { labels: string[]; state?: 'open' | 'closed' }
  | { labels?: never; state: 'open' | 'closed' }
)

export interface MountClient {
  readonly writebackTransport?: 'relayfile-cloud' | 'test'
  readonly githubRead?: GithubConnectionRead
  readonly githubWrite?: GithubConnectionWrite
  /**
   * Optional durable Relayfile resource-subscription API. Its absence means
   * this mount targets a pre-subscription service and consumers retain their
   * established local routing behaviour.
   */
  readonly resourceSubscriptions?: ResourceSubscriptionsClient
  readonly integrationConnections?: FactoryIntegrationConnections
  /** Ensure the SDK-authenticated Relayfile workspace mirror is available. */
  ensureLocalMount?(startDir: string, options?: LocalMountOptions): Promise<void>
  /** The registered workspace mirror root, when the mount can resolve one. */
  getLocalMountRoot?(): string | undefined
  /** Read-only local mirror freshness for `factory status`. */
  getLocalMountHealth?(): LocalMountHealth
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
  writeFile(path: string, content: unknown, opts?: {
    guarded?: boolean
    /** Require this exact source revision and do not retry a conflict. */
    baseRevision?: string
  }): Promise<{ targetRevision: string } | void>
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
