import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from './schema'

describe('FactoryConfigSchema', () => {
  it('parses a valid config and applies defaults', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      slack: {
        channel: 'C123',
      },
    })

    expect(parsed.subscription).toEqual({
      teams: [],
      projects: [],
      labels: [],
      assignees: [],
    })
    expect(parsed.repos.byProject).toEqual({})
    expect(parsed.repos.keywordRules).toEqual([])
    expect(parsed.repos.clonePaths).toEqual({})
    expect(parsed.batchSize).toBe(5)
    expect(parsed.models).toEqual({ babysitter: 'sonnet' })
    expect(parsed.babysitter).toEqual({ enabled: false })
    expect(parsed.terminalState).toBe('human-review')
    expect(parsed.stateIds.humanReview).toBeUndefined()
    expect(parsed.loop.registryPath).toBe('/tmp/factory-run/factory-loop-registry.json')
    expect(parsed.loop.maxConsecutiveFailures).toBe(3)
    expect(parsed.slack).toEqual({
      channel: 'C123',
      style: 'threaded-summarized',
      botUserId: 'U0B2596R7EZ',
      staleAfterMs: 10 * 60_000,
    })
    expect(parsed.mergePolicy).toBe('never')
    // No hardcoded state defaults: omitted stateIds resolve to {} and are filled
    // at runtime from linear.states (by name) or explicit stateIds.
    expect(parsed.stateIds).toEqual({})
    expect(parsed.linear).toEqual({ states: {}, statesByTeam: {} })
    expect(parsed.safety).toEqual({
      requireTitlePrefix: '[factory-e2e]',
      requireLabel: 'factory',
      requireTeamKey: 'AR',
    })
    expect(parsed.dryRun).toBe(false)
  })

  it('preserves explicit model overrides', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      models: {
        implementer: 'gpt-5-codex',
        reviewer: 'claude-opus-4-1',
      },
    })

    expect(parsed.models).toMatchObject({
      implementer: 'gpt-5-codex',
      reviewer: 'claude-opus-4-1',
    })
  })

  it('honors explicit babysitter, terminalState, and humanReview config', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      babysitter: { enabled: true },
      terminalState: 'done',
      models: { babysitter: 'claude-sonnet-4-6' },
      stateIds: {
        readyForAgent: 'state-ready',
        agentImplementing: 'state-impl',
        done: 'state-done',
        inPlanning: 'state-plan',
        humanReview: 'state-human-review',
      },
    })

    expect(parsed.babysitter.enabled).toBe(true)
    expect(parsed.terminalState).toBe('done')
    expect(parsed.models.babysitter).toBe('claude-sonnet-4-6')
    expect(parsed.stateIds.humanReview).toBe('state-human-review')
  })

  it('parses dynamic per-team Linear state name mappings', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      linear: {
        states: { readyForAgent: 'Ready for Agent', done: 'Done' },
        statesByTeam: {
          ENG: { readyForAgent: 'To Do', done: 'Shipped' },
        },
      },
    })

    expect(parsed.linear.states.readyForAgent).toBe('Ready for Agent')
    expect(parsed.linear.statesByTeam.ENG).toEqual({ readyForAgent: 'To Do', done: 'Shipped' })
    expect(parsed.stateIds).toEqual({})
  })

  it('derives byLabel, clonePaths, and subscription.labels from a compact repos config', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        org: 'AgentWorkforce',
        cloneRoot: '/work/AgentWorkforce/',
        names: ['pear', 'cloud', 'agentswarm'],
        overrides: { agentswarm: 'AgentWorkforce/AgentSwarm' },
        default: 'pear',
      },
    })

    expect(parsed.repos.byLabel).toEqual({
      pear: 'AgentWorkforce/pear',
      cloud: 'AgentWorkforce/cloud',
      agentswarm: 'AgentWorkforce/AgentSwarm',
    })
    expect(parsed.repos.clonePaths).toEqual({
      'AgentWorkforce/pear': '/work/AgentWorkforce/pear',
      'AgentWorkforce/cloud': '/work/AgentWorkforce/cloud',
      'AgentWorkforce/AgentSwarm': '/work/AgentWorkforce/AgentSwarm',
    })
    // subscription.labels defaults to the repo names
    expect(parsed.subscription.labels).toEqual(['pear', 'cloud', 'agentswarm'])
    expect(parsed.repos.default).toBe('pear')
  })

  it('lets explicit byLabel/clonePaths/labels override the derived ones', () => {
    const parsed = FactoryConfigSchema.parse({
      subscription: { labels: ['pear'] },
      repos: {
        org: 'AgentWorkforce',
        cloneRoot: '/work',
        names: ['pear', 'cloud'],
        byLabel: { cloud: 'Other/cloud-fork' },
        clonePaths: { 'AgentWorkforce/pear': '/custom/pear' },
      },
    })

    expect(parsed.repos.byLabel.cloud).toBe('Other/cloud-fork')
    expect(parsed.repos.byLabel.pear).toBe('AgentWorkforce/pear')
    expect(parsed.repos.clonePaths['AgentWorkforce/pear']).toBe('/custom/pear')
    expect(parsed.repos.clonePaths['Other/cloud-fork']).toBe('/work/cloud-fork')
    // explicit subscription.labels is preserved (not overwritten by names)
    expect(parsed.subscription.labels).toEqual(['pear'])
  })

  it('still accepts the legacy explicit-only repos form', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        byLabel: { pear: 'AgentWorkforce/pear' },
        clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
        default: 'AgentWorkforce/pear',
      },
    })

    expect(parsed.repos.byLabel).toEqual({ pear: 'AgentWorkforce/pear' })
    expect(parsed.repos.clonePaths).toEqual({ 'AgentWorkforce/pear': '/work/pear' })
  })

  it('rejects batch sizes over five', () => {
    expect(() => FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      batchSize: 6,
    })).toThrow()
  })
})
