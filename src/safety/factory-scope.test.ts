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

  it('treats a null title prefix as label-only scope', () => {
    const labelOnlySafety = {
      ...safety,
      requireTitlePrefix: null,
      requireLabel: 'garden-ready',
    }

    expect(isInFactoryScope(issue({
      title: 'No title marker',
      labels: ['garden-ready'],
    }), labelOnlySafety)).toBe(true)
    expect(isInFactoryScope(issue({
      title: '[factory-e2e] A prefix alone is not enough',
      labels: [],
    }), labelOnlySafety)).toBe(false)
  })

  it('matches raw Linear connection label shapes case-insensitively', () => {
    expect(isInFactoryScope(issue({
      payloadLabels: {
        edges: [{ node: { name: 'FACTORY' } }],
      },
    }), safety)).toBe(true)
  })
})

describe('isInFactoryScope rename transition', () => {
  const gardenSafety = {
    requireTitlePrefix: '[garden-e2e]',
    requireLabel: 'garden',
    requireTeamKey: 'AR',
  }

  it('keeps the legacy factory label acceptable when garden is configured', () => {
    expect(isInFactoryScope(issue({ labels: ['factory'] }), gardenSafety)).toBe(true)
    expect(isInFactoryScope(issue({ labels: ['garden'] }), gardenSafety)).toBe(true)
  })

  it('keeps legacy-titled issues acceptable when the garden prefix is configured', () => {
    expect(isInFactoryScope(issue({ title: '[factory-e2e] Soak issue' }), gardenSafety)).toBe(true)
    expect(isInFactoryScope(issue({ title: '[garden-e2e] Soak issue' }), gardenSafety)).toBe(true)
  })

  it('defaults to the garden prefix and label when no safety is configured', () => {
    expect(isInFactoryScope(issue({ title: '[garden-e2e] Untitled soak' }))).toBe(true)
    expect(isInFactoryScope(issue({ labels: ['garden'] }))).toBe(true)
    expect(isInFactoryScope(issue({ labels: ['factory'] }))).toBe(true)
    expect(isInFactoryScope(issue())).toBe(false)
  })

  it('accepts both mirror title spellings for GitHub mirror payloads only', () => {
    const mirror = (title: string) => issue({ title, labels: [] })
    const withGithubSource = (title: string) => {
      const base = mirror(title)
      return {
        ...base,
        raw: {
          payload: {
            ...base.raw.payload,
            source: { provider: 'github', owner: 'AgentWorkforce', repo: 'pear', number: 9, url: 'https://github.com/AgentWorkforce/pear/issues/9' },
          },
        },
      }
    }
    expect(isInFactoryScope(withGithubSource('[garden] Mirrored issue'), gardenSafety)).toBe(true)
    expect(isInFactoryScope(withGithubSource('[factory] Mirrored issue'), gardenSafety)).toBe(true)
    // A human-authored Linear issue merely titled with the mirror marker is
    // still rejected under the stricter configured prefix.
    expect(isInFactoryScope(mirror('[factory] Human-authored issue'), gardenSafety)).toBe(false)
  })
})
