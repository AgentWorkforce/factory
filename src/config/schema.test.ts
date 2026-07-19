import { describe, expect, it } from 'vitest'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { FactoryConfigSchema, NodeConfigSchema, loadFactoryConfig } from './schema'

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
    // Agent CLI per role defaults to today's behavior: codex implements, claude
    // reviews/babysits — so existing configs are unaffected unless set.
    expect(parsed.agentCapabilities).toEqual({
      implementer: 'spawn:codex',
      reviewer: 'spawn:claude',
      babysitter: 'spawn:claude',
    })
    expect(parsed.babysitter).toEqual({ enabled: false })
    expect(parsed.terminalState).toBe('human-review')
    expect(parsed.stateIds.humanReview).toBeUndefined()
    expect(parsed.loop.registryPath).toBe('/tmp/factory-run/factory-loop-registry.json')
    expect(parsed.loop.maxConsecutiveFailures).toBe(3)
    expect(parsed.reporting).toEqual({
      enabled: true,
      batchSize: 100,
      requestTimeoutMs: 15_000,
    })
    expect(parsed.slack).toEqual({
      channel: 'C123',
      style: 'threaded-summarized',
      botUserId: 'U0B2596R7EZ',
      stakeholderUserIds: [],
      staleAfterMs: 10 * 60_000,
    })
    expect(parsed.mergePolicy).toBe('never')
    // No hardcoded state defaults: omitted stateIds resolve to {} and are filled
    // at runtime from linear.states (by name) or explicit stateIds.
    expect(parsed.stateIds).toEqual({})
    // The factory ships its workflow-state NAME conventions as defaults (so a
    // consumer needn't configure them); statesByTeam stays empty.
    expect(parsed.linear).toEqual({
      states: {
        readyForAgent: 'Ready for Agent',
        agentImplementing: 'Agent Implementing',
        done: 'Done',
        inPlanning: 'In Planning',
        humanReview: 'In Human Review',
      },
      statesByTeam: {},
      teamIds: {},
    })
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

  it('trims and validates an explicit reporting instance name', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: '  Oslo Factory  ' },
    })

    expect(parsed.reporting.instanceName).toBe('Oslo Factory')
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: '   ' },
    })).toThrow()
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: 'x'.repeat(257) },
    })).toThrow()
  })

  it('honors an explicit per-role agent CLI override and rejects unwired capabilities', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      // Swap the implementer CLI to claude to route around a codex-specific
      // failure; reviewer/babysitter fall back to their defaults.
      agentCapabilities: { implementer: 'spawn:claude' },
    })

    expect(parsed.agentCapabilities).toEqual({
      implementer: 'spawn:claude',
      reviewer: 'spawn:claude',
      babysitter: 'spawn:claude',
    })

    // spawn:opencode / spawn:gemini have no capabilityCli mapping yet, so the
    // schema refuses them rather than resolving to an undefined CLI at spawn.
    expect(() => FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/pear' },
      agentCapabilities: { implementer: 'spawn:opencode' },
    })).toThrow()
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

  it('supports an explicit GitHub issue source while leaving source auto-detection enabled by default', () => {
    const auto = FactoryConfigSchema.parse({ repos: { default: 'AgentWorkforce/factory' } })
    const github = FactoryConfigSchema.parse({
      issueSource: 'github',
      repos: { default: 'AgentWorkforce/factory' },
    })

    expect(auto.issueSource).toBeUndefined()
    expect(github.issueSource).toBe('github')
  })

  it('parses dynamic per-team Linear state name mappings', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      linear: {
        states: { readyForAgent: 'Ready for Agent', done: 'Done' },
        teamIds: { AR: 'team-ar' },
        statesByTeam: {
          ENG: { readyForAgent: 'To Do', done: 'Shipped' },
        },
      },
    })

    expect(parsed.linear.states.readyForAgent).toBe('Ready for Agent')
    expect(parsed.linear.teamIds.AR).toBe('team-ar')
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
    expect(parsed.repos.org).toBe('AgentWorkforce')
    expect(parsed.repos.names).toEqual(['pear', 'cloud', 'agentswarm'])
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

  it('expands exact ~ and ~/ in cloneRoot and explicit clonePaths while preserving precedence', () => {
    const parsed = FactoryConfigSchema.parse({
      cloneRoot: '~/top-level',
      clonePaths: { 'AgentWorkforce/pear': '~/top-level-explicit' },
      repos: {
        org: 'AgentWorkforce',
        names: ['pear', 'cloud'],
        cloneRoot: '~/legacy-root',
        clonePaths: {
          'AgentWorkforce/pear': '~/legacy-explicit',
          'AgentWorkforce/cloud': '~',
        },
      },
    })

    expect(parsed.cloneRoot).toBe(join(homedir(), 'top-level'))
    expect(parsed.clonePaths).toEqual({
      'AgentWorkforce/pear': join(homedir(), 'top-level-explicit'),
      'AgentWorkforce/cloud': homedir(),
    })
    expect(parsed.repos.clonePaths).toEqual(parsed.clonePaths)
  })

  it('derives clone paths from an exact ~ legacy cloneRoot', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        org: 'AgentWorkforce',
        names: ['factory'],
        cloneRoot: '~',
      },
    })

    expect(parsed.cloneRoot).toBe(homedir())
    expect(parsed.clonePaths).toEqual({
      'AgentWorkforce/factory': join(homedir(), 'factory'),
    })
  })

  it('expands node-only and split clone paths consistently', () => {
    expect(NodeConfigSchema.parse({
      cloneRoot: '~',
      clonePaths: { 'AgentWorkforce/factory': '~/Projects/factory' },
    })).toMatchObject({
      cloneRoot: homedir(),
      clonePaths: { 'AgentWorkforce/factory': join(homedir(), 'Projects/factory') },
    })

    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { org: 'AgentWorkforce', names: ['factory'] },
      },
      nodeConfig: {
        cloneRoot: '~/Projects/AgentWorkforce',
        clonePaths: { 'AgentWorkforce/factory': '~' },
      },
    })
    expect(loaded.factoryConfig.cloneRoot).toBe(join(homedir(), 'Projects/AgentWorkforce'))
    expect(loaded.factoryConfig.clonePaths).toEqual({ 'AgentWorkforce/factory': homedir() })
    expect(loaded.factoryConfig.repos.clonePaths).toEqual(loaded.factoryConfig.clonePaths)
    expect(loaded.nodeConfig.cloneRoot).toBe(join(homedir(), 'Projects/AgentWorkforce'))
    expect(loaded.nodeConfig.clonePaths).toEqual(loaded.factoryConfig.clonePaths)
  })

  it('does not rewrite embedded tildes', () => {
    const parsed = FactoryConfigSchema.parse({
      cloneRoot: '/work/~shared',
      repos: {
        byLabel: { pear: 'AgentWorkforce/pear' },
        clonePaths: { 'AgentWorkforce/pear': '/work/~shared/pear' },
      },
    })

    expect(parsed.cloneRoot).toBe('/work/~shared')
    expect(parsed.clonePaths['AgentWorkforce/pear']).toBe('/work/~shared/pear')
  })

  it.each([
    { input: { cloneRoot: '~other', repos: {} }, field: 'cloneRoot' },
    { input: { repos: { cloneRoot: '~other/projects' } }, field: 'cloneRoot' },
    {
      input: { repos: { clonePaths: { 'AgentWorkforce/pear': '~other/pear' } } },
      field: 'repos.clonePaths["AgentWorkforce/pear"]',
    },
  ])('rejects unsupported ~user syntax in $field', ({ input, field }) => {
    expect(() => FactoryConfigSchema.parse(input)).toThrow(`${field} does not support ~user expansion`)
  })

  it('rejects ~user syntax even when a higher-precedence value overrides it', () => {
    expect(() => FactoryConfigSchema.parse({
      cloneRoot: '/top-level',
      clonePaths: { 'AgentWorkforce/pear': '/top-level/pear' },
      repos: {
        cloneRoot: '~other/legacy',
        clonePaths: { 'AgentWorkforce/pear': '~other/pear' },
      },
    })).toThrow('repos.cloneRoot does not support ~user expansion')

    expect(() => loadFactoryConfig({
      workspaceConfig: {
        repos: { cloneRoot: '~other/legacy' },
      },
      nodeConfig: { cloneRoot: '/node-root' },
    })).toThrow('workspaceConfig.repos.cloneRoot does not support ~user expansion')
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
