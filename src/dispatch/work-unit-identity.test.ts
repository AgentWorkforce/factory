import { describe, expect, it } from 'vitest'

import { dispatchAgentIdentityKey, dispatchIssueIdentity } from './work-unit-identity'

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
})
