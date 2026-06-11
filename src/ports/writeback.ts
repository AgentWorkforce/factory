import type { LinearIssue, PrSummary } from '../types'

export interface LinearWriteback {
  setState(issue: LinearIssue, stateId: string): Promise<void>
  postComment(issue: LinearIssue, body: string): Promise<void>
  verify(issue: LinearIssue, expect: { stateId?: string; commentName?: string }): Promise<boolean>
}

export interface SlackWriteback {
  postThread(root: { channel: string; text: string }): Promise<{ threadId: string }>
  reply(threadId: string, text: string): Promise<void>
}

export interface GithubRead {
  getPr(repo: string, number: number): Promise<PrSummary>
}
