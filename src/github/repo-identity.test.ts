import { describe, expect, it } from 'vitest'

import { githubRepositoriesMatch } from './repo-identity'

describe('githubRepositoriesMatch', () => {
  it.each([
    ['pear', 'AgentWorkforce/pear'],
    ['AgentWorkforce/pear', 'pear'],
    ['agentworkforce/PEAR.git', 'AgentWorkforce/pear'],
    ['AgentWorkforce__pear', 'AgentWorkforce/pear'],
  ])('matches equivalent bare, canonical, and mounted repo identities: %s / %s', (left, right) => {
    expect(githubRepositoriesMatch(left, right)).toBe(true)
  })

  it.each([
    ['AgentWorkforce/pear', 'someone-else/pear'],
    ['pear', 'AgentWorkforce/factory'],
    ['AgentWorkforce/pear/extra', 'AgentWorkforce/pear'],
  ])('does not conflate distinct repo identities: %s / %s', (left, right) => {
    expect(githubRepositoriesMatch(left, right)).toBe(false)
  })
})
