import { describe, expect, it, vi } from 'vitest'

import { CostLedger } from './ledger'

describe('CostLedger', () => {
  it('aggregates role/model usage and replaces deterministic agent updates', () => {
    const ledger = new CostLedger()
    ledger.record({
      runId: 'run-1',
      role: 'implementer',
      model: 'openai/gpt-5.4',
      inputTokens: 100,
      outputTokens: 10,
    }, { entryId: 'run-1\0implementer-1' })
    ledger.record({
      runId: 'run-1',
      role: 'implementer',
      model: 'openai/gpt-5.4',
      inputTokens: 200,
      outputTokens: 20,
    }, { entryId: 'run-1\0implementer-1' })
    ledger.record({
      runId: 'run-1',
      role: 'reviewer',
      model: 'anthropic/claude-haiku-4.5',
      inputTokens: 1_000,
      outputTokens: 100,
    }, { entryId: 'run-1\0reviewer-1' })

    expect(ledger.getRunRecords('run-1')).toHaveLength(2)
    expect(ledger.getRunTotal('run-1')).toEqual({
      runId: 'run-1',
      inputTokens: 1_200,
      outputTokens: 120,
      usd: 0.0028,
      byRole: [
        {
          role: 'implementer',
          inputTokens: 200,
          outputTokens: 20,
          usd: 0.0013,
          byModel: [{
            model: 'openai/gpt-5.4',
            inputTokens: 200,
            outputTokens: 20,
            usd: 0.0013,
          }],
        },
        {
          role: 'reviewer',
          inputTokens: 1_000,
          outputTokens: 100,
          usd: 0.0015,
          byModel: [{
            model: 'anthropic/claude-haiku-4.5',
            inputTokens: 1_000,
            outputTokens: 100,
            usd: 0.0015,
          }],
        },
      ],
    })
  })

  it('retains null usage and bounds an unpriced notice to one run/model', () => {
    const ledger = new CostLedger()
    const listener = vi.fn()
    ledger.onUnpricedModel(listener)
    ledger.record({
      runId: 'run-2',
      role: 'triage',
      model: 'provider/unknown',
      inputTokens: 10,
      outputTokens: 5,
    })
    ledger.record({
      runId: 'run-2',
      role: 'reviewer',
      model: 'provider/unknown',
      inputTokens: 20,
      outputTokens: 10,
    })
    ledger.record({
      runId: 'run-2',
      role: 'babysitter',
      model: 'openai/gpt-5.4',
      inputTokens: null,
      outputTokens: null,
    })

    expect(listener).toHaveBeenCalledTimes(1)
    expect(ledger.getRunRecords('run-2')).toContainEqual(expect.objectContaining({
      role: 'babysitter',
      inputTokens: null,
      outputTokens: null,
      usd: null,
    }))
  })
})
