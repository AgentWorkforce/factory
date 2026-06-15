import { describe, expect, it } from 'vitest'

import { LINEAR_STATE_IDS } from '../constants/linear'
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
    expect(parsed.stateIds).toEqual(LINEAR_STATE_IDS)
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
