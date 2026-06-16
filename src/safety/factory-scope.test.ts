import { describe, expect, it } from 'vitest'

import { isInFactoryScope } from './factory-scope'

const issue = (overrides: {
  title?: string
  team?: string
  labels?: string[]
  payloadLabels?: unknown
} = {}) => ({
  title: overrides.title ?? 'Plain Linear issue',
  team: overrides.team ?? 'AR',
  labels: overrides.labels ?? [],
  raw: {
    payload: {
      title: overrides.title ?? 'Plain Linear issue',
      team: { key: overrides.team ?? 'AR' },
      labels: overrides.payloadLabels ?? overrides.labels ?? [],
    },
  },
})

const safety = {
  requireTitlePrefix: '[factory]',
  requireLabel: 'factory',
  requireTeamKey: 'AR',
}

describe('isInFactoryScope', () => {
  it('accepts a Linear issue with only the configured factory label', () => {
    expect(isInFactoryScope(issue({
      labels: ['Factory'],
      payloadLabels: [{ name: 'Factory' }],
    }), safety)).toBe(true)
  })

  it('accepts a Linear issue with only the configured title prefix', () => {
    expect(isInFactoryScope(issue({
      title: '[factory] Title-scoped issue',
    }), safety)).toBe(true)
  })

  it('accepts a Linear issue with both the configured title prefix and label', () => {
    expect(isInFactoryScope(issue({
      title: '[factory] Fully scoped issue',
      labels: ['factory'],
    }), safety)).toBe(true)
  })

  it('rejects a Linear issue with neither the configured title prefix nor label', () => {
    expect(isInFactoryScope(issue(), safety)).toBe(false)
  })

  it('rejects a Linear issue with the factory label on the wrong team', () => {
    expect(isInFactoryScope(issue({
      team: 'ENG',
      labels: ['factory'],
    }), safety)).toBe(false)
  })

  it('treats an empty configured label as title-only scope', () => {
    expect(isInFactoryScope(issue({
      labels: ['factory'],
    }), {
      ...safety,
      requireLabel: '',
    })).toBe(false)
  })

  it('matches raw Linear connection label shapes case-insensitively', () => {
    expect(isInFactoryScope(issue({
      payloadLabels: {
        edges: [{ node: { name: 'FACTORY' } }],
      },
    }), safety)).toBe(true)
  })
})
