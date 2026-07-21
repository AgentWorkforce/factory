import { describe, expect, it, vi } from 'vitest'

import { FakeFleetClient } from '../testing/fakes'
import { askTeammate, RelaycastTeammateDirectory } from './teammates'

const directoryRows = [{
  name: 'infra-agent',
  address: 'infra-agent',
  skills: [{ id: 'infra-watch', name: 'Infra Watch', tags: ['operations'] }],
  tags: ['on-call'],
  url: 'https://relay.example/a2a/rpc',
  kind: 'native',
  status: 'online',
}, {
  name: 'review-agent',
  address: 'review-agent',
  skills: [{ id: 'code-review', name: 'Code Review' }],
  tags: ['quality'],
  url: 'https://review.example/a2a/rpc',
  kind: 'a2a',
  status: 'active',
}]

describe('RelaycastTeammateDirectory', () => {
  it('sends skill/tag filters and rejects unfiltered server rows client-side', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => new Response(JSON.stringify({
      ok: true,
      data: directoryRows,
    }), { status: 200 }))
    const directory = new RelaycastTeammateDirectory({
      baseUrl: 'https://relay.example/',
      token: 'rk_live_test',
      fetch,
    })

    await expect(directory.discover({ skill: 'infra-watch' })).resolves.toEqual([
      expect.objectContaining({ name: 'infra-agent', address: 'infra-agent', kind: 'native' }),
    ])
    await expect(directory.discover({ skill: 'missing' })).resolves.toEqual([])
    await expect(directory.discover({ tag: 'quality' })).resolves.toEqual([
      expect.objectContaining({ name: 'review-agent', kind: 'a2a' }),
    ])

    const firstUrl = fetch.mock.calls[0]?.[0]
    expect(String(firstUrl)).toBe('https://relay.example/v1/a2a/directory?skill=infra-watch')
    expect(fetch.mock.calls[0]?.[1]).toMatchObject({
      headers: expect.objectContaining({ authorization: 'Bearer rk_live_test' }),
    })
  })
})

describe('askTeammate', () => {
  it('discovers a target, sends the question, and resolves only its reply', async () => {
    const fleet = new FakeFleetClient()
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })

    const asked = askTeammate(fleet, {
      from: 'factory-worker',
      question: 'Is the deploy healthy?',
      skill: 'infra-watch',
      timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    fleet.emitAgentMessage({ from: 'someone-else', target: 'factory-worker', body: 'wrong' })
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'different-worker', body: 'also wrong' })
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'All systems green.' })

    await expect(asked).resolves.toMatchObject({
      teammate: { name: 'infra-agent' },
      reply: { from: 'infra-agent', body: 'All systems green.' },
    })
    expect(fleet.messages[0]).toMatchObject({
      to: 'infra-agent',
      from: 'factory-worker',
      text: 'Is the deploy healthy?',
      mode: 'wait',
      data: { factoryCapability: 'ask-a-teammate', requester: 'factory-worker' },
    })
  })

  it('returns a bounded timeout when the teammate never replies', async () => {
    vi.useFakeTimers()
    try {
      const fleet = new FakeFleetClient()
      fleet.teammates.push({
        name: 'silent-agent',
        address: 'silent-agent',
        skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
        tags: [],
        url: 'https://relay.example/a2a/rpc',
        kind: 'native',
      })
      const asked = askTeammate(fleet, {
        from: 'factory-worker',
        question: 'Hello?',
        skill: 'infra-watch',
        timeoutMs: 50,
      })
      const rejected = expect(asked).rejects.toThrow('Timed out waiting for a reply from a teammate after 50ms')
      await vi.advanceTimersByTimeAsync(50)
      await rejected
    } finally {
      vi.useRealTimers()
    }
  })
})
