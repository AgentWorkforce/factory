import { describe, expect, it } from 'vitest'

import {
  GARDEN_AUTOMATION_LABEL,
  LEGACY_FACTORY_AUTOMATION_LABEL,
  matchesGardenLabelAlias,
} from './lifecycle-labels'

describe('matchesGardenLabelAlias', () => {
  // The dual read is what keeps an in-flight `factory`-labeled issue
  // dispatchable after the configured default moved to `garden`.
  it('accepts the configured label and its rename alias, in both directions', () => {
    expect(matchesGardenLabelAlias(['factory'], GARDEN_AUTOMATION_LABEL)).toBe(true)
    expect(matchesGardenLabelAlias(['garden'], LEGACY_FACTORY_AUTOMATION_LABEL)).toBe(true)
    expect(matchesGardenLabelAlias(['bug'], GARDEN_AUTOMATION_LABEL)).toBe(false)
  })

  // The set branch used to probe with `has` directly, so a provider-cased label
  // matched through an array and missed through a Set. Callers pass whichever
  // collection they happen to hold; the answer must not depend on that.
  it('normalizes entries the same way for a Set and for an array', () => {
    const provider = ['  Factory  ', 'Bug']
    expect(matchesGardenLabelAlias(provider, GARDEN_AUTOMATION_LABEL)).toBe(true)
    expect(matchesGardenLabelAlias(new Set(provider), GARDEN_AUTOMATION_LABEL)).toBe(true)
    expect(matchesGardenLabelAlias(new Set(['Garden']), LEGACY_FACTORY_AUTOMATION_LABEL)).toBe(true)
  })

  it('refuses an empty configured label rather than matching everything', () => {
    expect(matchesGardenLabelAlias(new Set(['garden']), '   ')).toBe(false)
  })
})
