import { describe, expect, it } from 'vitest'

import { branchImplementsIssue, containsExplicitIssueReference, containsIssueKey, factoryBranchBelongsToIssue, prBodyDisclaimsClosing, prClosureAuthority } from './issue-key-match'

describe('issue key matching', () => {
  it('matches dispatch branch conventions without numeric-prefix collisions', () => {
    expect(containsIssueKey('ar-229-is-positive', 'AR-229')).toBe(true)
    expect(containsIssueKey('AR-229: add util', 'AR-229')).toBe(true)
    expect(containsIssueKey('ar-2299-is-positive', 'AR-229')).toBe(false)
    expect(containsIssueKey('ar-229-1-is-positive', 'AR-229')).toBe(false)
    expect(containsIssueKey('ar-22-9-not-229', 'AR-229')).toBe(false)
  })

  it('matches Linear team prefixes that embed digits, without a nearby suffix colliding', () => {
    expect(containsIssueKey('core23-456-is-positive', 'CORE23-456')).toBe(true)
    expect(containsIssueKey('CORE23-456: add util', 'CORE23-456')).toBe(true)
    expect(containsIssueKey('core23-4567-is-positive', 'CORE23-456')).toBe(false)
    expect(containsIssueKey('core23-456-1-is-positive', 'CORE23-456')).toBe(false)
  })

  it('keeps a digit-embedded prefix distinct from a pure-alpha key with the same characters', () => {
    // `CORE23-1` and `CORE-231` are the same characters split at a different
    // hyphen, so widening the prefix must not let either claim the other's
    // branch — nor let a longer issue number (`CORE23-12`) absorb `CORE23-1`.
    expect(containsIssueKey('core23-1-is-positive', 'CORE23-1')).toBe(true)
    expect(containsIssueKey('core-231-is-positive', 'CORE23-1')).toBe(false)
    expect(containsIssueKey('core23-12-is-positive', 'CORE23-1')).toBe(false)
    expect(containsIssueKey('core23-1-is-positive', 'CORE-231')).toBe(false)
    expect(factoryBranchBelongsToIssue('factory/core23-1-agentworkforce-factory', 'CORE23-1')).toBe(true)
    expect(factoryBranchBelongsToIssue('factory/core-231-agentworkforce-factory', 'CORE23-1')).toBe(false)
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

describe('pull request closure authority', () => {
  // The two pull requests that wrongly closed the live relayfile-cloud#155
  // outage. Both reference the issue; neither claims to fix it. Verbatim from
  // the incident so a regression reproduces the original failure exactly.
  const DIAGNOSTIC_PR_BODY =
    'Diagnostic for #155. No credential behaviour changes — nothing about token minting, refresh, or connection selection moves here.'
  const REFS_PR_BODY =
    'Refs #155. Does NOT close it — the cause is a connection naming an installation on the wrong account, which no credential refresh can fix.'

  it('refuses to close on the bodies that closed the #155 outage', () => {
    for (const body of [DIAGNOSTIC_PR_BODY, REFS_PR_BODY]) {
      const authority = prClosureAuthority({ body, repo: 'AgentWorkforce/relayfile-cloud' }, '155')
      expect(authority.authorised).toBe(false)
      expect(authority.evidence).toContain('155')
    }
  })

  it('refuses a bare reference and accepts only a closing keyword', () => {
    expect(prClosureAuthority({ body: 'See #155 for background.' }, '155').authorised).toBe(false)
    expect(prClosureAuthority({ body: 'Related to #155.' }, '155').authorised).toBe(false)
    expect(prClosureAuthority({ body: 'Fixes #155' }, '155').authorised).toBe(true)
    expect(prClosureAuthority({ body: 'closes: #155' }, '155').authorised).toBe(true)
    expect(prClosureAuthority({ body: 'Resolved #155' }, '155').authorised).toBe(true)
  })

  // Numeric GitHub keys need the `factory/` prefix (a bare number collides too
  // easily); Linear-keyed branches carry the key itself and never had one.
  it('accepts a Linear implementation branch without a factory/ prefix', () => {
    expect(branchImplementsIssue('ar-410-fix', 'AR-410')).toBe(true)
    expect(branchImplementsIssue('ar-411-fix', 'AR-410')).toBe(false)
    expect(branchImplementsIssue('chore/connection-diagnostic', '155')).toBe(false)
    expect(branchImplementsIssue('155-fix', '155')).toBe(false)
    expect(branchImplementsIssue('factory/155-fix', '155')).toBe(true)
    expect(prClosureAuthority({ headRef: 'ar-410-fix' }, 'AR-410').authorised).toBe(true)
  })

  it('detects an explicit disclaimer without requiring a keyword verdict', () => {
    expect(prBodyDisclaimsClosing({ body: 'Diagnostic for #155.' }, '155')).toBe(true)
    expect(prBodyDisclaimsClosing({ body: 'Refs #155. Does NOT close it.' }, '155')).toBe(true)
    expect(prBodyDisclaimsClosing({ body: 'Linear: AR-411' }, 'AR-411')).toBe(false)
    expect(prBodyDisclaimsClosing({ body: 'Fixes #155' }, '155')).toBe(false)
    // A denial about another issue must not leak onto this one.
    expect(prBodyDisclaimsClosing({ body: 'Fixes #143\nDoes not fix #155.' }, '143')).toBe(false)
  })

  it('reports which source granted authority', () => {
    expect(prClosureAuthority({ body: 'Fixes #155' }, '155').source).toBe('closing-keyword')
    expect(prClosureAuthority({ headRef: 'factory/155-repoint' }, '155').source).toBe('factory-branch')
  })

  // Factory writes `Fixes #N` into every dispatch PR body and cuts a
  // `factory/<key>` branch, so the ordinary completion path keeps both sources.
  it('still authorises Factory\'s own dispatch pull requests', () => {
    expect(prClosureAuthority(
      { headRef: 'factory/143-agentworkforce-factory-f9b704a7', body: 'Verification stack.\n\nFixes #143' },
      '143',
    ).authorised).toBe(true)
    expect(prClosureAuthority({ headRef: 'factory/143-agentworkforce-factory-f9b704a7' }, '143').authorised).toBe(true)
    expect(prClosureAuthority({ body: 'Body only.\n\nFixes AR-244' }, 'AR-244').authorised).toBe(true)
  })

  it('lets an explicit disclaimer override a closing keyword', () => {
    expect(prClosureAuthority({ body: 'Fixes #155 — actually this does not fix #155.' }, '155').authorised).toBe(false)
    expect(prClosureAuthority(
      { headRef: 'factory/155-probe', body: 'Diagnostic for #155.' },
      '155',
    ).authorised).toBe(false)
  })

  // A disclaimer about a different issue must not block a legitimate close,
  // which is why negation is scoped to the referencing line.
  // Reported by codex on #326. A descriptor word is not a denial: these bodies
  // carry a valid closing keyword and merely happen to use the word later on
  // the same line. Treating them as disclaimers would strand ordinary work.
  it('does not read a descriptor word as a denial when a closing keyword is present', () => {
    expect(prClosureAuthority({ body: 'Fixes #155 by adding a diagnostic for timeouts.' }, '155').authorised).toBe(true)
    expect(prClosureAuthority({ body: 'Fixes #155 and lays groundwork for retries.' }, '155').authorised).toBe(true)
    expect(prBodyDisclaimsClosing({ body: 'Fixes #155 by adding a diagnostic for timeouts.' }, '155')).toBe(false)
  })

  // An explicit denial still outranks the keyword — that is a deliberate
  // statement, not an incidental word.
  it('lets an explicit denial outrank a closing keyword', () => {
    expect(prClosureAuthority({ body: 'Fixes #155 — actually this does not fix #155.' }, '155').authorised).toBe(false)
    expect(prBodyDisclaimsClosing({ body: 'Fixes #155 — actually this does not fix #155.' }, '155')).toBe(true)
  })

  it('scopes a disclaimer to the issue it names', () => {
    const body = 'Fixes #143\nThis does not fix #155.'
    expect(prClosureAuthority({ body }, '143').authorised).toBe(true)
    expect(prClosureAuthority({ body }, '155').authorised).toBe(false)
  })

  // Prior art (#268): auto-closures citing a pull request in another
  // repository. A qualified reference only counts for its own repository.
  it('refuses a closing keyword aimed at another repository', () => {
    const body = 'Fixes AgentWorkforce/relayfile-cloud#155'
    expect(prClosureAuthority({ body, repo: 'AgentWorkforce/relayfile-cloud' }, '155').authorised).toBe(true)
    expect(prClosureAuthority({ body, repo: 'AgentWorkforce/factory' }, '155').authorised).toBe(false)
  })

  it('does not let an unrelated number claim authority', () => {
    expect(prClosureAuthority({ body: 'Fixes #1550' }, '155').authorised).toBe(false)
    expect(prClosureAuthority({ body: 'tsc, eslint, and 155 tests all pass.' }, '155').authorised).toBe(false)
  })
})
