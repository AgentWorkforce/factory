import type { GithubPublishPullRequestInput, GithubPublishPullRequestResult } from './mount'
import type { LinearIssue, PrSummary } from '../types'

/**
 * Notification that the *state-defining* write has landed.
 *
 * A writeback call does more than change state: it confirms readbacks, clears
 * the previous label, posts comments. The issue becomes observably not-ready
 * the moment the state write resolves, which is well before the call returns —
 * so a caller that needs to know "is this state change mine?" cannot wait for
 * the promise. `onApplied` fires synchronously at that instant, before any
 * confirmation or cleanup await (factory#319).
 */
export interface WritebackApplyHooks {
  onApplied?: () => void
}

export interface LinearWriteback {
  setState(issue: LinearIssue, stateId: string, hooks?: WritebackApplyHooks): Promise<void>
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
  setStatus(issue: LinearIssue, status: GithubIssueStatus, hooks?: WritebackApplyHooks): Promise<void>
  closeIssue(issue: LinearIssue, body: string): Promise<void>
}
