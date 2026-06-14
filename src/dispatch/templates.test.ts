import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import { renderAgentTask } from './templates'

const baseConfig = FactoryConfigSchema.parse({
  workspaceId: 'workspace',
  repos: { byLabel: { pear: 'pear' } },
})

const issue = {
  key: 'AR-123',
  title: 'Fix duplicate delivery',
  description: 'Full description with renderer reconnects, broker replay, and acceptance criteria.',
}

describe('renderAgentTask', () => {
  it('renders the required implementer clauses for a single-repo route', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/Users/khaliqgant/Projects/AgentWorkforce/pear' },
      role: 'implementer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
    })

    expect(task).toContain('GitHub repo: AgentWorkforce/pear')
    expect(task).toContain('Repo path: /Users/khaliqgant/Projects/AgentWorkforce/pear')
    expect(task).toContain('Linear issue: AR-123 - Fix duplicate delivery')
    expect(task).toContain(issue.description)
    expect(task).toContain('Create a branch for this issue before editing.')
    expect(task).toContain('Commit the implementation and tests.')
    expect(task).toContain('Push the branch to origin.')
    expect(task).toContain('Open a PR targeting `main` when done.')
    expect(task).toContain('Use `gh pr create --base main` and report the PR URL.')
    expect(task).toContain('DM the reviewer `ar-123-review` when the PR is ready.')
    expect(task).toContain('DM `factory` with `[factory-needs-input] <your question>` so the factory can relay it to the issue Slack thread.')
    expect(task).toContain('DM `broker` when fully done.')
    expect(task).toContain('Do NOT auto-merge.')
    expect(task).toContain('Merge policy: never')
  })

  it('renders reviewer coordination clauses for team dispatch', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'AgentWorkforce/pear', clonePath: '/tmp/pear-team' },
      role: 'reviewer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      implementerNames: ['ar-123-impl-ui', 'ar-123-impl-broker'],
    })

    expect(task).toContain('GitHub repo: AgentWorkforce/pear')
    expect(task).toContain('Wait for a DM from the implementer(s): ar-123-impl-ui, ar-123-impl-broker.')
    expect(task).toContain('Read the PR diff via `.integrations/github/repos`.')
    expect(task).toContain('Post review comments via the GitHub writeback path.')
    expect(task).toContain('DM the implementer with specific feedback if changes needed, or approve if good.')
    expect(task).toContain('DM `broker` when the review cycle is complete.')
  })

  it('renders clone/worktree instructions and on-green merge policy for cross-repo routes', () => {
    const config = FactoryConfigSchema.parse({
      workspaceId: 'workspace',
      repos: { byLabel: { cloud: 'cloud' } },
      mergePolicy: 'on-green-with-review',
    })

    const task = renderAgentTask({
      issue,
      route: { repo: 'cloud' },
      role: 'implementer',
      config,
      reviewerName: 'ar-123-review',
    })

    expect(task).toContain('GitHub repo: AgentWorkforce/cloud')
    expect(task).toContain('Clone/worktree: clone AgentWorkforce/cloud and work in your own isolated git worktree before editing.')
    expect(task).toContain('Merge policy: on-green-with-review')
  })

  it('uses the consistent factory needs-input format across both blocked-input clauses', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'implementer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      slackDispatchThread: { channel: 'C123', threadId: '169.000' },
    })

    // The always-present clause and the Slack-thread fallback both point at
    // `factory` with the [factory-needs-input] marker; no legacy
    // FACTORY_NEEDS_INPUT / DM `broker` blocked-input wording remains.
    expect(task).toContain('If blocked and you need human input, DM `factory` with `[factory-needs-input] <your question>`')
    expect(task).toContain('Fallback: DM `factory` with `[factory-needs-input] <your question>`')
    expect(task).not.toContain('FACTORY_NEEDS_INPUT')
    expect(task).not.toContain('DM `broker` with')
  })
})
