import { describe, expect, it } from 'vitest'

import {
  dispatchAgentIdentityKey,
  dispatchIssueIdentity,
  dispatchNotionPageIdentity,
} from './work-unit-identity'

describe('dispatch work-unit identity', () => {
  it('uses one provider-native identity for GitHub Relayfile aliases', () => {
    const compact = {
      uuid: 'AgentWorkforce/factory#244',
      key: '244',
      path: '/github/repos/AgentWorkforce__factory/issues/by-id/244.json',
    }
    const nested = {
      ...compact,
      path: '/github/repos/AgentWorkforce/factory/issues/244__dispatch-reclaim/meta.json',
    }

    expect(dispatchIssueIdentity(compact)).toBe('github:agentworkforce/factory#244')
    expect(dispatchAgentIdentityKey(compact, 'reviewer')).toBe(
      'factory:dispatch:v1:github:agentworkforce/factory#244:reviewer',
    )
    expect(dispatchAgentIdentityKey(nested, 'reviewer')).toBe(dispatchAgentIdentityKey(compact, 'reviewer'))
  })

  it('separates roles and same-number issues from different repositories', () => {
    const factoryIssue = {
      uuid: 'AgentWorkforce/factory#244',
      key: '244',
      path: '/github/repos/AgentWorkforce__factory/issues/by-id/244.json',
    }
    const cloudIssue = {
      uuid: 'AgentWorkforce/cloud#244',
      key: '244',
      path: '/github/repos/AgentWorkforce__cloud/issues/by-id/244.json',
    }

    expect(dispatchAgentIdentityKey(factoryIssue, 'implementer')).not.toBe(
      dispatchAgentIdentityKey(factoryIssue, 'reviewer'),
    )
    expect(dispatchAgentIdentityKey(factoryIssue, 'reviewer')).not.toBe(
      dispatchAgentIdentityKey(cloudIssue, 'reviewer'),
    )
  })

  it('uses the Linear provider UUID instead of its mutable surface path', () => {
    const issue = { uuid: '7f08f5b7-issue-identity', key: 'AR-244', path: '/linear/issues/AR-244.json' }
    expect(dispatchAgentIdentityKey(issue, 'implementer')).toBe(
      'factory:dispatch:v1:linear:7f08f5b7-issue-identity:implementer',
    )
  })

  it('classifies Linear issues by their key shape, not by a /linear/ mount path', () => {
    const issue = { uuid: '7f08f5b7-issue-identity', key: 'AR-244', path: '/some/other/mount/AR-244.json' }
    expect(dispatchIssueIdentity(issue)).toBe('linear:7f08f5b7-issue-identity')
  })

  it('falls back to a generic provider tag for a non-GitHub, non-Linear-shaped key', () => {
    const issue = { uuid: 'opaque-provider-id', key: 'opaque-key', path: '/some/other/mount/opaque-key.json' }
    expect(dispatchIssueIdentity(issue)).toBe('issue:opaque-provider-id')
  })

  it('throws when the provider identity is empty', () => {
    const issue = { uuid: '   ', key: 'opaque-key', path: '/some/other/mount/opaque-key.json' }
    expect(() => dispatchIssueIdentity(issue)).toThrow(/provider identity is empty/u)
  })

  it('uses the provider-native Notion page id without a destination alias', () => {
    expect(dispatchNotionPageIdentity('3B36800C-1C90-801D-B1CF-C8F2E1CFF7CF')).toBe(
      'notion:3b36800c-1c90-801d-b1cf-c8f2e1cff7cf',
    )
    expect(() => dispatchNotionPageIdentity('notion-page:repo:mutable/destination')).toThrow(
      /canonical page id/u,
    )
  })
})
