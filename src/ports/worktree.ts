export interface AgentWorktree {
  repo: string
  issueKey: string
  baseClonePath: string
  worktreePath: string
  branch: string
  /** Branch is an already-open, same-repository PR head verified by Factory. */
  existingPullRequestBranch?: boolean
}

export interface AgentWorktreeManager {
  prepare(worktree: AgentWorktree): Promise<void>
  cleanup(worktree: AgentWorktree): Promise<void>
}
