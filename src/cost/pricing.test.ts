import { describe, expect, it, vi } from 'vitest'

import { estimateCostUsd } from './pricing'

describe('estimateCostUsd', () => {
  it('prices known models and reports unknown ids without throwing', () => {
    expect(estimateCostUsd('openai/gpt-5.4', 1_000_000, 100_000)).toBe(6.5)

    const onUnpricedModel = vi.fn(() => { throw new Error('reporter unavailable') })
    expect(() => estimateCostUsd('provider/not-priced', 10, 20, { onUnpricedModel })).not.toThrow()
    expect(estimateCostUsd('provider/not-priced', 10, 20, { onUnpricedModel })).toBeNull()
    expect(onUnpricedModel).toHaveBeenCalledWith('provider/not-priced')

    // Object-prototype names are valid bounded identifiers, but never prices.
    expect(estimateCostUsd('toString', 10, 20, { onUnpricedModel })).toBeNull()
    expect(onUnpricedModel).toHaveBeenCalledWith('toString')
  })
})
