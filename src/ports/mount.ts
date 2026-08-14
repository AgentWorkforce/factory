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

export interface GithubIssueCommentInput {
  repo: string
  /** Issue or pull request number — GitHub issue comments cover both. */
  number: number
  body: string
}

export interface GithubReviewCommentReplyInput {
  repo: string
  /** Pull request number owning the review thread. */
  number: number
  /** Provider id of the review comment being replied to. */
  inReplyTo: number
  body: string
}

/**
 * Receipt for a connection-authored comment. `commentId` is present when the
 * durable write acknowledgement carried the provider id; `author` is the
 * identity Factory requested, not a provider read-back. Callers that must
 * prove the identity read it back from GitHub — see `readGithubCommentAuthor`.
 */
export interface GithubCommentResult {
  repo: string
  number: number
  commentId?: number
  author: 'app'
}

export interface GithubIssueUpdateInput {
  repo: string
  number: number
  /** Replaces the issue's full label set when present. */
  labels?: readonly string[]
  state?: 'open' | 'closed'
  title?: string
  body?: string
}

export interface GithubIssueCreateInput {
  repo: string
  title: string
  body: string
  labels?: readonly string[]
}

export interface GithubIssueCreateResult {
  repo: string
  number: number
  url: string
  author: 'app'
}

export interface GithubMergePullRequestInput {
  repo: string
  number: number
  /** Guard supplied to GitHub so a head move cannot be merged accidentally. */
  expectedHeadSha: string
  method: 'merge' | 'squash' | 'rebase'
}

export interface GithubMergePullRequestResult {
  /** Provider-confirmed merge commit SHA, when returned by the adapter. */
  sha?: string
}

/**
 * GitHub mutations that require the authenticated workspace connection. The
 * concrete mount translates these operations into file-native Relayfile
 * writeback drafts interpreted by the server-side GitHub adapter.
 *
 * Every operation here is authored by the workspace GitHub App installation.
 * That is the point of the port: Factory has exactly one GitHub write
 * identity, and it is not whichever human happens to be logged into a local
 * `gh` CLI. New GitHub mutations belong here, not in a CLI shell-out.
 */
export interface GithubConnectionWrite {
  publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult>
  closePullRequest(input: { repo: string; number: number }): Promise<void>
  /** Comment on an issue or pull request conversation. */
  postIssueComment?(input: GithubIssueCommentInput): Promise<GithubCommentResult>
  /** Reply inside an existing pull request review thread. */
  replyToReviewComment?(input: GithubReviewCommentReplyInput): Promise<GithubCommentResult>
  /** Patch issue fields; `labels` replaces the whole set, so it both adds and removes. */
  updateIssue?(input: GithubIssueUpdateInput): Promise<void>
  createIssue?(input: GithubIssueCreateInput): Promise<GithubIssueCreateResult>
  /** Merge with a provider-enforced expected-head guard. */
  mergePullRequest?(input: GithubMergePullRequestInput): Promise<GithubMergePullRequestResult>
}

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
