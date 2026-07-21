import { describe, expect, it } from 'vitest'

import { classifyFailure, type FailureRecord } from './failure-classifier'

const failure = (errorKind: string, attempt: number): FailureRecord => ({
  errorKind,
  message: `Failure ${attempt}: ${errorKind}`,
  occurredAtMs: attempt * 1_000,
})

describe('classifyFailure', () => {
  it('classifies identical error kinds at the retry limit as structural', () => {
    expect(classifyFailure([
      failure('authentication', 1),
      failure('authentication', 2),
      failure('authentication', 3),
    ])).toBe('structural')
  })

  it('classifies distinct error kinds at the retry limit as transient', () => {
    expect(classifyFailure([
      failure('timeout', 1),
      failure('rate-limit', 2),
      failure('authentication', 3),
    ])).toBe('transient')
  })

  it('requires unanimity and classifies a two-to-one error-kind split as transient', () => {
    expect(classifyFailure([
      failure('timeout', 1),
      failure('timeout', 2),
      failure('rate-limit', 3),
    ])).toBe('transient')
  })

  it('classifies exactly one attempt as transient', () => {
    expect(classifyFailure([failure('authentication', 1)])).toBe('transient')
  })

  it('classifies an empty history as transient without throwing', () => {
    expect(classifyFailure([])).toBe('transient')
  })
})
