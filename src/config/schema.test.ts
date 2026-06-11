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
    expect(parsed.models).toEqual({
      implementer: 'codex',
      reviewer: 'claude',
    })
    expect(parsed.slack).toEqual({
      channel: 'C123',
      style: 'threaded-summarized',
    })
    expect(parsed.mergePolicy).toBe('never')
    expect(parsed.stateIds).toEqual(LINEAR_STATE_IDS)
    expect(parsed.dryRun).toBe(false)
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
