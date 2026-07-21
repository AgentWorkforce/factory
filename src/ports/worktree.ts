export interface AgentWorktree {
  repo: string
  issueKey: string
  baseClonePath: string
  worktreePath: string
  branch: string
  /** Branch is an already-open, same-repository PR head verified by Factory. */
  existingPullRequestBranch?: boolean
}

export interface AgentWorktreeRepository {
  repo: string
  baseClonePath: string
}

export interface AgentWorktreeCleanupInspection {
  /** Approximate on-disk bytes that would be reclaimed by removing the checkout. */
  bytes: number
  /** Empty only when the checkout is safe for an automated sweep to remove. */
  retentionReasons: string[]
}

export interface AgentWorktreeManager {
  prepare(worktree: AgentWorktree): Promise<void>
  cleanup(worktree: AgentWorktree): Promise<void>
  /** Enumerate Factory-owned linked checkouts for one configured repository. */
  listWorktrees(repository: AgentWorktreeRepository): Promise<AgentWorktree[]>
  /** Fail closed before cleanup when local work, unpublished commits, or locks exist. */
  inspectForCleanup(worktree: AgentWorktree): Promise<AgentWorktreeCleanupInspection>
}
