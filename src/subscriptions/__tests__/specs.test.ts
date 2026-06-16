import { describe, expect, it } from 'vitest'

import {
  canonicalMountPaths,
  deliveryTargetsFor,
  eventPathGlobsForIntegration,
  subscriptionSpecsFor,
  type ConnectedIntegrationLike,
} from '../specs'

describe('subscription specs', () => {
  const linearIntegration: ConnectedIntegrationLike = {
    provider: 'linear',
    integrationId: 'linear-1',
    scope: {
      teams: ['Agent Relay'],
      projects: [],
      labels: ['cloud'],
      assignees: ['kjg@example.invalid'],
      notifyAgents: ['@triage-bot', '#not-agent'],
      notifyChannels: ['factory'],
    },
    mountPaths: ['/linear/issues'],
    downloadHistoricalData: true,
  }

  it('builds Linear subscription specs with targets and scope predicates', () => {
    expect(subscriptionSpecsFor([linearIntegration])).toEqual([
      {
        integrationId: 'linear-1',
        provider: 'linear',
        mountPaths: ['/linear/issues'],
        localMountRoots: [],
        eventPathGlobs: ['/linear/issues/**'],
        watches: [{ glob: '/linear/issues/**', coalesceMs: 750 }],
        targets: { agents: ['triage-bot'], channels: ['#factory'] },
        allowHistoricalReplay: true,
        linearPredicates: {
          teams: ['Agent Relay'],
          projects: [],
          labels: ['cloud'],
          assignees: ['kjg@example.invalid'],
        },
      },
    ])
  })

  it('canonicalizes legacy integration paths and Slack channel aliases', () => {
    const slackIntegration: ConnectedIntegrationLike = {
      provider: 'slack',
      integrationId: 'slack-1',
      scope: { listenDms: true },
      mountPaths: ['/integrations/slack/channels/C123__general'],
    }

    expect(canonicalMountPaths(slackIntegration)).toEqual([
      '/slack/channels/C123',
      '/slack/channels/C123__general',
    ])
    expect(eventPathGlobsForIntegration(slackIntegration)).toEqual([
      '/slack/channels/C123/**',
      '/slack/channels/C123__general/**',
      '/slack/channels/D*/**',
      '/slack/users/*/messages/**',
    ])
  })

  it('dedupes delivery targets across integrations', () => {
    expect(deliveryTargetsFor([
      { ...linearIntegration, scope: { notifyAgents: ['bot', '@bot'], relayChannels: ['factory', '#factory'] } },
    ])).toEqual({ agents: ['bot'], channels: ['#factory'] })
  })
})
