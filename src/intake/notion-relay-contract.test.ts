import { createHash } from 'node:crypto'

import { describe, expect, it, vi } from 'vitest'

import { RelayChannelNotionContractPublisher, contractChannelName, contractMarkerPrefix } from './notion-relay-contract'

type StoredMessage = { id: string; text: string }

function fakeRelaySurface() {
  const channels = new Map<string, StoredMessage[]>()
  let registrations = 0
  let sends = 0
  let nextMessage = 1
  const createRelay = vi.fn((options: { agentToken?: string }) => ({
    agents: {
      register: vi.fn(async () => ({ token: `agent-token-${++registrations}` })),
      delete: vi.fn(async () => undefined),
    },
    channels: {
      join: vi.fn(async (name: string) => {
        if (!channels.has(name)) throw new Error('channel not found')
      }),
      create: vi.fn(async ({ name }: { name: string }) => {
        if (channels.has(name)) throw new Error('channel already exists')
        channels.set(name, [])
        return { name }
      }),
    },
    messages: {
      list: vi.fn(async (channel: string) => [...(channels.get(channel) ?? [])].reverse()),
      send: vi.fn(async ({ channel, text }: { channel: string; text: string }) => {
        sends += 1
        const message = { id: `message-${nextMessage++}`, text }
        channels.get(channel)?.push(message)
        return message
      }),
    },
    messaging: { events: { disconnect: vi.fn(async () => undefined) } },
    options,
  }) as never)
  return {
    channels,
    createRelay,
    registrationCount: () => registrations,
    sendCount: () => sends,
  }
}

function contractInput(content: string, sourceKey = 'notion:page:repo:agentworkforce/cloud') {
  return {
    pageId: '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf',
    sourceKey,
    content,
    contentDigest: createHash('sha256').update(content).digest('hex'),
  }
}

describe('RelayChannelNotionContractPublisher', () => {
  it('rejects a digest mismatch before creating a Relay client', async () => {
    const fake = fakeRelaySurface()
    const publisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      createRelay: fake.createRelay,
    })

    await expect(publisher.publish({
      ...contractInput('private body'),
      contentDigest: '0'.repeat(64),
    })).rejects.toThrow('changed before portable mount publication')
    expect(fake.createRelay).not.toHaveBeenCalled()
  })

  it('chunks exact bytes and reuses their message ids across publisher identities', async () => {
    const fake = fakeRelaySurface()
    const input = contractInput('private body '.repeat(600))
    const firstPublisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      publisherName: 'publisher-one',
      createRelay: fake.createRelay,
    })
    const first = await firstPublisher.publish(input)
    await firstPublisher.dispose()
    const sendsAfterFirst = fake.sendCount()
    const secondPublisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      publisherName: 'publisher-two',
      createRelay: fake.createRelay,
    })
    const second = await secondPublisher.publish(input)

    expect(first.messageIds.length).toBeGreaterThan(1)
    expect(second).toEqual(first)
    expect(fake.sendCount()).toBe(sendsAfterFirst)
    const byId = new Map((fake.channels.get(first.channel) ?? []).map((message) => [message.id, message.text]))
    const encoded = first.messageIds.map((id) => byId.get(id)?.split('\n')[2]).join('')
    expect(Buffer.from(encoded, 'base64').toString('utf8')).toBe(input.content)
  })

  it('uses a new digest channel and cache entry when one source changes', async () => {
    const fake = fakeRelaySurface()
    const publisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      createRelay: fake.createRelay,
    })
    const first = await publisher.publish(contractInput('first revision'))
    const second = await publisher.publish(contractInput('second revision'))

    expect(second.channel).not.toBe(first.channel)
    expect(second.messageIds).not.toEqual(first.messageIds)
    expect(fake.sendCount()).toBe(2)
  })

  it('rejects an existing digest marker whose payload bytes differ', async () => {
    const fake = fakeRelaySurface()
    const input = contractInput('expected content')
    const channel = contractChannelName(input.pageId, input.sourceKey, input.contentDigest)
    fake.channels.set(channel, [{
      id: 'tampered-message',
      text: `${contractMarkerPrefix(input.pageId, input.contentDigest)}1/1\n` +
        '---BEGIN FACTORY NOTION CONTRACT BASE64---\ndGFtcGVyZWQ=\n---END FACTORY NOTION CONTRACT BASE64---',
    }])
    const publisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      createRelay: fake.createRelay,
    })

    await expect(publisher.publish(input)).rejects.toThrow('does not match its digest-bound marker')
    expect(fake.sendCount()).toBe(0)
  })

  it('shares one in-flight agent registration across overlapping publications', async () => {
    const fake = fakeRelaySurface()
    const publisher = new RelayChannelNotionContractPublisher({
      workspaceKey: 'workspace-key',
      createRelay: fake.createRelay,
    })

    await Promise.all([
      publisher.publish(contractInput('first body', 'notion:first')),
      publisher.publish(contractInput('second body', 'notion:second')),
    ])

    expect(fake.registrationCount()).toBe(1)
  })
})
