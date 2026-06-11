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

export interface RenderAgentTaskInput {
  issue: TemplateIssue
  route: TemplateRoute
  role: AgentSpec['role']
  config: Pick<FactoryConfig, 'mergePolicy'>
  reviewerName: string
  implementerNames?: string[]
}

export function renderAgentTask(input: RenderAgentTaskInput): string {
  const repo = normalizeRepo(input.route.repo)
  const cloneInstruction = input.route.clonePath
    ? `Repo path: ${input.route.clonePath}`
    : `Clone/worktree: clone AgentWorkforce/${repo} and work in your own isolated git worktree before editing.`
  const implementers = input.implementerNames?.length ? input.implementerNames.join(', ') : 'the implementer(s)'

  const common = [
    `GitHub repo: AgentWorkforce/${repo}`,
    cloneInstruction,
    `Linear issue: ${input.issue.key} - ${input.issue.title}`,
    'Full Linear issue description:',
    input.issue.description,
    '',
    'Open a PR targeting `main` when done.',
    `DM the reviewer \`${input.reviewerName}\` when the PR is ready.`,
    'DM `broker` when fully done.',
    'Do NOT auto-merge.',
    mergePolicyLine(input.config.mergePolicy),
  ]

  if (input.role === 'reviewer') {
    return [
      ...common,
      '',
      `Wait for a DM from the implementer(s): ${implementers}.`,
      'Read the PR diff via `.integrations/github/repos`.',
      'Post review comments via the GitHub writeback path.',
      'DM the implementer with specific feedback if changes needed, or approve if good.',
      'DM `broker` when the review cycle is complete.',
    ].join('\n')
  }

  return common.join('\n')
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
