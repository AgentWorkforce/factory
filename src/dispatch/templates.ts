import type { FactoryConfig } from '../config/schema'
import type { AgentSpec } from '../ports/fleet'

export interface TemplateIssue {
  key: string
  title: string
  description: string
}

export interface TemplateRoute {
  repo: string
  clonePath?: string
  rationale?: string
}

export interface TemplatePr {
  number: number
  url?: string
}

export interface RenderAgentTaskInput {
  issue: TemplateIssue
  route: TemplateRoute
  role: AgentSpec['role']
  config: Pick<FactoryConfig, 'mergePolicy' | 'terminalState'>
  reviewerName: string
  implementerNames?: string[]
  /** The already-open PR the babysitter shepherds. Only used for the babysitter role. */
  pr?: TemplatePr
  slackDispatchThread?: {
    channel: string
    threadId: string
  }
}

export function renderAgentTask(input: RenderAgentTaskInput): string {
  const repo = normalizeRepo(input.route.repo)
  const cloneInstruction = input.route.clonePath
    ? `Repo path: ${input.route.clonePath}`
    : `Clone/worktree: clone AgentWorkforce/${repo} and work in your own isolated git worktree before editing.`
  const implementers = input.implementerNames?.length ? input.implementerNames.join(', ') : 'the implementer(s)'

  const header = [
    `GitHub repo: AgentWorkforce/${repo}`,
    cloneInstruction,
    `Linear issue: ${input.issue.key} - ${input.issue.title}`,
    'Full Linear issue description:',
    input.issue.description,
  ]

  const common = [
    ...header,
    '',
    'Create a branch for this issue before editing.',
    'Commit the implementation and tests.',
    'Push the branch to origin.',
    'Open a PR targeting `main` when done.',
    'Use `gh pr create --base main` and report the PR URL.',
    `DM the reviewer \`${input.reviewerName}\` when the PR is ready.`,
    'If blocked and you need human input, DM `factory` with `[factory-needs-input] <your question>` so the factory can relay it to the issue Slack thread.',
    'DM `broker` when fully done.',
    'Do NOT auto-merge.',
    mergePolicyLine(input.config.mergePolicy),
  ]
  const questionInstructions = input.slackDispatchThread
    ? [
        '',
        'If you are blocked or need a human answer mid-task, ask in this issue\'s Slack dispatch thread.',
        `Slack dispatch channel: ${input.slackDispatchThread.channel}`,
        `Slack dispatch thread: ${input.slackDispatchThread.threadId}`,
        'Prefer the injected Agent Relay MCP thread reply tool when it is available.',
        'Fallback: DM `factory` with `[factory-needs-input] <your question>` and continue with safe reversible work while waiting.',
      ]
    : []

  if (input.role === 'babysitter') {
    const prRef = input.pr
      ? `PR #${input.pr.number}${input.pr.url ? ` (${input.pr.url})` : ''}`
      : 'the open PR for this issue'
    const chatLine = input.slackDispatchThread
      ? 'You can also use this issue\'s Slack dispatch thread to discuss the PR with the human (status, trade-offs, open questions) — proactively offer to chat there if it would help.'
      : 'If a human can be reached, proactively offer to discuss the PR (status, trade-offs, open questions) via `[factory-needs-input]`.'
    // Match the prompt to where the issue actually lands so the babysitter is not
    // told to "stop at Human Review" while the factory is configured to finish at
    // Done (and possibly auto-merge under on-green-with-review).
    const humanReview = input.config.terminalState === 'human-review'
    const destination = humanReview ? 'Human Review' : 'Done'
    const jobLine = humanReview
      ? 'Your job: drive this PR to genuinely green and correct against the Linear issue spec above, then hand it to a human for review. Do NOT merge it yourself.'
      : 'Your job: drive this PR to genuinely green and correct against the Linear issue spec above so the factory can finish it. Do NOT merge it yourself.'
    const finishLine = humanReview
      ? 'Do NOT auto-merge; stop at Human Review — a human owns the merge.'
      : input.config.mergePolicy === 'on-green-with-review'
        ? 'Do NOT merge it yourself; the factory runs the guarded merge gate once you signal ready.'
        : 'Do NOT merge it yourself; the factory moves the issue to Done once you signal ready.'
    return [
      ...header,
      '',
      `You are the PR babysitter for ${input.issue.key}. A PR is already open: ${prRef}.`,
      jobLine,
      'Unlike a conservative reviewer, you SHOULD fix things directly and aggressively — you hold the original issue spec as the definition of done, and you have the rest of the dispatched team to draw on.',
      'Read the PR diff, CI checks, and review threads via `.integrations/github/repos`.',
      'Address every review comment for real — make substantive code changes when the feedback calls for it, not just lint/format touch-ups.',
      'Resolve any merge conflicts: rebase onto the base branch and reconcile using judgment anchored in the issue spec; never weaken tests or flip safety defaults just to force a merge.',
      'Fix failing CI — change the code and tests as needed until the checks pass. A red check is not done.',
      `Coordinate the team when it helps: DM the implementer(s) (${implementers}) or the reviewer \`${input.reviewerName}\` to delegate or pull context. Prefer fixing it yourself; loop them in when you are stuck or it is clearly their area.`,
      'Commit and push your fixes to the PR branch.',
      chatLine,
      `When the PR is green — no failing CI, no merge conflicts, every review comment addressed — DM \`factory\` with \`[factory-pr-ready] ${input.issue.key}\` so the factory can move the issue to ${destination}.`,
      'DM `broker` when fully done.',
      finishLine,
      mergePolicyLine(input.config.mergePolicy),
      ...questionInstructions,
    ].join('\n')
  }

  if (input.role === 'reviewer') {
    return [
      ...common,
      ...questionInstructions,
      '',
      `Wait for a DM from the implementer(s): ${implementers}.`,
      'Read the PR diff via `.integrations/github/repos`.',
      'Post review comments via the GitHub writeback path.',
      'DM the implementer with specific feedback if changes needed, or approve if good.',
      'DM `broker` when the review cycle is complete.',
    ].join('\n')
  }

  return [
    ...common,
    ...questionInstructions,
  ].join('\n')
}

export function agentSpecWithRenderedTask(
  spec: Omit<AgentSpec, 'task'> & { task?: string },
  input: RenderAgentTaskInput,
): AgentSpec {
  return {
    ...spec,
    task: renderAgentTask(input),
  }
}

export function mergePolicyLine(policy: FactoryConfig['mergePolicy']): string {
  if (policy === 'on-green-with-review') {
    return 'Merge policy: on-green-with-review - do not merge until checks are green and review approval is present.'
  }

  return 'Merge policy: never - open the PR for human review and approval; never merge it yourself.'
}

function normalizeRepo(repo: string): string {
  return repo.startsWith('AgentWorkforce/') ? repo.slice('AgentWorkforce/'.length) : repo
}
