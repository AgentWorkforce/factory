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

export type GithubIssueStatus = 'in-progress' | 'human-review'

export interface GithubWriteback {
  postComment(issue: LinearIssue, body: string): Promise<void>
  /** Provider-authoritative lookup used to reconcile ambiguous comment writes. */
  hasCommentMarker?(issue: LinearIssue, marker: string): Promise<boolean>
  setStatus(issue: LinearIssue, status: GithubIssueStatus): Promise<void>
  /** Remove Factory lifecycle labels after work is safely aborted before implementation starts. */
  clearStatus(issue: LinearIssue): Promise<void>
  closeIssue(issue: LinearIssue, body: string): Promise<void>
}
