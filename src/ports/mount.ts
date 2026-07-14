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
}

export interface GithubPublishPullRequestInput {
  repo: string
  clonePath: string
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
  ensureSubRoot(prefix: string, opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'>
}
