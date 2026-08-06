import { describe, expect, it, vi } from 'vitest'

import { RelayChannelNotionClaimStore, notionClaimChannelName } from './notion-relay-claim'

type StoredMessage = { id: string; text: string }

function fakeRelaySurface(options: { failWrites?: boolean } = {}) {
  const channels = new Map<string, StoredMessage[]>()
  let registrations = 0
  let sends = 0
  const createRelay = vi.fn((relayOptions: { agentToken?: string }) => ({
    agents: {
      register: vi.fn(async () => ({ token: `agent-token-${++registrations}` })),
      delete: vi.fn(async () => undefined),
    },
    channels: {
      get: vi.fn(async (name: string) => {
        if (!channels.has(name)) throw Object.assign(new Error('missing'), { code: 'channel_not_found' })
        return { name }
      }),
      join: vi.fn(async (name: string) => {
        if (!channels.has(name)) throw new Error('channel not found')
      }),
      create: vi.fn(async ({ name }: { name: string }) => {
        if (channels.has(name)) throw Object.assign(new Error('exists'), { code: 'channel_already_exists' })
        channels.set(name, [])
        return { name }
      }),
    },
    messages: {
      list: vi.fn(async (channel: string) => [...(channels.get(channel) ?? [])].reverse()),
      send: vi.fn(async ({ channel, text }: { channel: string; text: string }) => {
        if (options.failWrites) throw new Error('claim write unavailable')
        sends += 1
        const message = { id: `message-${sends}`, text }
        channels.get(channel)?.push(message)
        return message
      }),
    },
    messaging: { events: { disconnect: vi.fn(async () => undefined) } },
    relayOptions,
  }) as never)
  return { channels, createRelay, sendCount: () => sends }
}

const claim = {
  sourceKey: 'notion:3b36800c-1c90-801d-b1cf-c8f2e1cff7cf:repo:agentworkforce/cloud',
  digest: 'a'.repeat(64),
  claimedAt: '2026-08-06T20:00:00.000Z',
}

describe('RelayChannelNotionClaimStore', () => {
  it('uses workspace-global channel uniqueness so different dispatchers observe one claim', async () => {
    const fake = fakeRelaySurface()
    const first = new RelayChannelNotionClaimStore({
      workspaceKey: 'workspace-key',
      publisherName: 'dispatcher-one',
      createRelay: fake.createRelay,
    })
    const second = new RelayChannelNotionClaimStore({
      workspaceKey: 'workspace-key',
      publisherName: 'dispatcher-two',
      createRelay: fake.createRelay,
    })

    const results = await Promise.all([
      first.claim(claim),
      second.claim({ ...claim, claimedAt: '2026-08-06T20:01:00.000Z' }),
    ])
    const winner = results.find((result) => result.status === 'claimed')!
    const observer = results.find((result) => result.status === 'existing')!

    expect(results.map((result) => result.status).sort()).toEqual(['claimed', 'existing'])
    expect(observer.claim).toEqual(winner.claim)
    expect(fake.channels.get(notionClaimChannelName(claim.sourceKey))).toHaveLength(1)
    expect(fake.sendCount()).toBe(1)
  })

  it('reads an existing durable claim without trying to create another', async () => {
    const fake = fakeRelaySurface()
    const first = new RelayChannelNotionClaimStore({ workspaceKey: 'workspace-key', createRelay: fake.createRelay })
    const second = new RelayChannelNotionClaimStore({ workspaceKey: 'workspace-key', createRelay: fake.createRelay })
    await first.claim(claim)

    await expect(second.get(claim.sourceKey)).resolves.toEqual(claim)
    await expect(second.get('notion:missing')).resolves.toBeUndefined()
    expect(fake.sendCount()).toBe(1)
  })

  it('leaves an incomplete durable channel and rejects when the claim record write fails', async () => {
    const fake = fakeRelaySurface({ failWrites: true })
    const first = new RelayChannelNotionClaimStore({ workspaceKey: 'workspace-key', createRelay: fake.createRelay })
    const second = new RelayChannelNotionClaimStore({ workspaceKey: 'workspace-key', createRelay: fake.createRelay })

    await expect(first.claim(claim)).rejects.toThrow('claim write unavailable')
    await expect(second.claim(claim)).rejects.toThrow('has 0 immutable claim records; refusing dispatch')
    expect(fake.channels.has(notionClaimChannelName(claim.sourceKey))).toBe(true)
  })
})
