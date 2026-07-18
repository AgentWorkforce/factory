export interface AgentWorktree {
  repo: string
  issueKey: string
  baseClonePath: string
  worktreePath: string
  branch: string
}

export interface AgentWorktreeManager {
  prepare(worktree: AgentWorktree): Promise<void>
  cleanup(worktree: AgentWorktree): Promise<void>
}
