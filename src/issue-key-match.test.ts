import { describe, expect, it } from 'vitest'

import { containsExplicitIssueReference, containsIssueKey, factoryBranchBelongsToIssue } from './issue-key-match'

describe('issue key matching', () => {
  it('matches dispatch branch conventions without numeric-prefix collisions', () => {
    expect(containsIssueKey('ar-229-is-positive', 'AR-229')).toBe(true)
    expect(containsIssueKey('AR-229: add util', 'AR-229')).toBe(true)
    expect(containsIssueKey('ar-2299-is-positive', 'AR-229')).toBe(false)
    expect(containsIssueKey('ar-229-1-is-positive', 'AR-229')).toBe(false)
    expect(containsIssueKey('ar-22-9-not-229', 'AR-229')).toBe(false)
  })

  it('requires explicit body issue references instead of loose mentions', () => {
    expect(containsExplicitIssueReference('Linear: AR-229', 'AR-229')).toBe(true)
    expect(containsExplicitIssueReference('Closes AR-229', 'AR-229')).toBe(true)
    expect(containsExplicitIssueReference('This merely mentions ar-229 in passing.', 'AR-229')).toBe(false)
  })

  it('does not treat numeric test counts as GitHub issue references', () => {
    expect(containsExplicitIssueReference('tsc, eslint, and 52 tests all pass.', '52')).toBe(false)
    expect(containsExplicitIssueReference('Fixes #52', '52')).toBe(true)
    expect(containsExplicitIssueReference('Issue: 52', '52')).toBe(true)
    expect(containsExplicitIssueReference('https://github.com/AgentWorkforce/hoopsheet/issues/52', '52')).toBe(true)
  })

  it('matches Factory branches only to their dispatched issue key', () => {
    expect(factoryBranchBelongsToIssue('factory/3021-cloud-deployment-fix', '3021')).toBe(true)
    expect(factoryBranchBelongsToIssue('factory/3022-chief-org-live-population', '3021')).toBe(false)
    expect(factoryBranchBelongsToIssue('factory/30210-not-3021', '3021')).toBe(false)
    expect(factoryBranchBelongsToIssue('factory/ar-244-agentworkforce-factory', 'AR-244')).toBe(true)
    expect(factoryBranchBelongsToIssue('feature/ar-244-agentworkforce-factory', 'AR-244')).toBe(false)
  })
})
