import type { GithubPublishPullRequestInput, GithubPublishPullRequestResult } from './mount'
import type { LinearIssue, PrSummary } from '../types'

export interface LinearWriteback {
  setState(issue: LinearIssue, stateId: string): Promise<void>
  postComment(issue: LinearIssue, body: string): Promise<void>
  createIssue(payload: Record<string, unknown>): Promise<{ path: string }>
  verify(issue: LinearIssue, expect: { stateId?: string; commentName?: string }): Promise<boolean>
}

export interface SlackWriteback {
  postThread(root: { channel: string; text: string }): Promise<{ threadId: string }>
  reply(threadId: string, text: string): Promise<void>
}

export interface GithubRead {
  getPr(repo: string, number: number): Promise<PrSummary>
}

export type GithubIssueStatus = 'ready' | 'in-progress' | 'human-review'

/**
 * Whether a lifecycle call provably changed the provider's effective status.
 * `applied` requires a provider audit event attributed to the writer's actor.
 * `acknowledged` means the provider accepted an idempotent operation but its
 * API did not prove who created the visible transition. `undefined` preserves
 * compatibility with caller-supplied writebacks that predate this receipt.
 */
export type GithubStatusWriteResult = 'applied' | 'already-matched' | 'acknowledged'

/**
 * Whether closing an issue provably created the provider's visible transition.
 * The same conservative receipt semantics as status writes apply: only an
 * `applied` result may establish Factory ownership of the closed state.
 */
export type GithubIssueCloseWriteResult = 'applied' | 'already-matched' | 'acknowledged'

export interface GithubWriteback {
  /** Optional local-user PR publisher, implemented by the default `gh` writeback. */
  publishPullRequest?(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult>
  /** Provider-authoritative fallback when the mounted issue record omits its reporter. */
  getIssueAuthor?(issue: LinearIssue): Promise<string | undefined>
  /** Provider-authoritative lifecycle lookup used before recovering stale mounted labels. */
  getIssueStatus?(issue: LinearIssue): Promise<GithubIssueStatus | undefined>
  postComment(issue: LinearIssue, body: string): Promise<void>
  /** Provider-authoritative lookup used to reconcile ambiguous comment writes. */
  hasCommentMarker?(issue: LinearIssue, marker: string): Promise<boolean>
  setStatus(issue: LinearIssue, status: GithubIssueStatus): Promise<GithubStatusWriteResult | void>
  closeIssue(issue: LinearIssue, body: string): Promise<GithubIssueCloseWriteResult | void>
}
