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
    expect(task).toContain('Factory will open the PR targeting `main` through the connected GitHub workspace.')
    expect(task).toContain('Do not run `gh pr create` or require local GitHub CLI authentication.')
    expect(task).toContain('Factory will hand the opened PR to reviewer `ar-123-review`.')
    expect(task).toContain('write to the .integrations mount path so the factory can relay it to the issue thread.')
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
    expect(task).toContain('Read the PR diff via .integrations/github/repos.')
    expect(task).toContain('Post review comments via the GitHub writeback path.')
    expect(task).toContain('DM the implementer with specific feedback if changes needed, or approve if good.')
    expect(task).toContain('DM `broker` when the review cycle is complete.')
  })

  it('renders an aggressive, spec-grounded babysitter task referencing the open PR', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'babysitter',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      implementerNames: ['ar-123-impl'],
      pr: { number: 482, url: 'https://github.com/AgentWorkforce/pear/pull/482' },
      slackDispatchThread: { channel: 'C123', threadId: '170.000', mountRoot: '/work/.integrations' },
    })

    // Carries the original spec (definition of done) and the open PR ref.
    expect(task).toContain('Linear issue: AR-123 - Fix duplicate delivery')
    expect(task).toContain(issue.description)
    expect(task).toContain('PR #482 (https://github.com/AgentWorkforce/pear/pull/482)')
    // Aggressive posture, not the conservative reviewer.
    expect(task).toContain('fix things directly and aggressively')
    expect(task).toContain('Fix failing CI')
    expect(task).toContain('Resolve any merge conflicts')
    expect(task).toContain('Address every review comment for real')
    // Team coordination + readiness signal + guardrail.
    expect(task).toContain('ar-123-impl')
    expect(task).toContain('ar-123-review')
    expect(task).toContain('[factory-pr-ready] AR-123')
    expect(task).toContain('move the issue to Human Review')
    expect(task).toContain('stop at Human Review')
    // It must NOT instruct opening a PR (one already exists).
    expect(task).not.toContain('Open a PR targeting `main` when done.')
    // Human-chat affordance.
    expect(task).toContain('discuss the PR with the human')
  })

  it('adapts the babysitter task to terminalState: done (no Human Review wording)', async () => {
    const doneConfig = FactoryConfigSchema.parse({
      workspaceId: 'workspace',
      repos: { byLabel: { pear: 'pear' } },
      terminalState: 'done',
    })

    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'babysitter',
      config: doneConfig,
      reviewerName: 'ar-123-review',
      pr: { number: 482 },
    })

    expect(task).toContain('move the issue to Done')
    expect(task).not.toContain('Human Review')
    expect(task).toContain('the factory moves the issue to Done once you signal ready')
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

  it('appends integration instructions for an implementer task', () => {
    const instructions = 'Connected integrations:\n- Slack: write messages to .integrations/slack/channels/{id}/messages\n- Linear: write state updates to .integrations/linear/issues/{id}/state'
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'implementer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      integrationInstructions: instructions,
    })

    expect(task).toContain(instructions)
  })

  it('appends integration instructions for a reviewer task', () => {
    const instructions = 'To dispatch an integration action, write a JSON file under the resource path. Do NOT use relay messaging.'
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'reviewer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      integrationInstructions: instructions,
    })

    expect(task).toContain(instructions)
  })

  it('appends integration instructions for a babysitter task', () => {
    const instructions = 'Writeback paths:\n- .integrations/linear/issues/{id}/state'
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'babysitter',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      pr: { number: 482 },
      integrationInstructions: instructions,
    })

    expect(task).toContain(instructions)
  })

  it('omits integration block when integrationInstructions is not provided', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'implementer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
    })

    // Should not add an extra trailing blank line + integration section.
    expect(task).not.toMatch(/\n\n(.integrations|Connected integrations)/m)
  })

  it('directs agent questions to .integrations writeback instead of relay DM', () => {
    const task = renderAgentTask({
      issue,
      route: { repo: 'pear', clonePath: '/tmp/pear' },
      role: 'implementer',
      config: baseConfig,
      reviewerName: 'ar-123-review',
      slackDispatchThread: { channel: 'C123', threadId: '169.000', mountRoot: '/work/.integrations' },
    })

    // The Slack-thread writeback replaces the old relay DM pattern.
    expect(task).toContain('write your question to this issue\'s Slack dispatch thread via the .integrations mount')
    expect(task).toContain('Write path: /work/.integrations/slack/channels/C123/messages/169_000/replies/question.json')
    expect(task).toContain('Write a JSON object with a "text" field')
    expect(task).toContain('Continue with safe reversible work while waiting for a reply.')
    // No relay DM or legacy patterns.
    expect(task).not.toContain('FACTORY_NEEDS_INPUT')
    expect(task).not.toContain('DM `broker` with')
    expect(task).not.toContain('[factory-needs-input]')
  })
})
