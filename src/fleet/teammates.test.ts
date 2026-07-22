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
    const fetch = vi.fn<typeof globalThis.fetch>(async (input) => {
      const url = new URL(String(input))
      return new Response(JSON.stringify({
        ok: true,
        // Relaycast may match a registered card alias that is not repeated in
        // the returned row. Preserve that server match for free-text queries.
        data: url.searchParams.has('q') ? [directoryRows[1]] : directoryRows,
      }), { status: 200 })
    })
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
    await expect(directory.discover({ q: 'private-card-alias' })).resolves.toEqual([
      expect.objectContaining({ name: 'review-agent' }),
    ])

    const firstUrl = fetch.mock.calls[0]?.[0]
    expect(String(firstUrl)).toBe('https://relay.example/v1/a2a/directory?skill=infra-watch')
    expect(fetch.mock.calls[0]?.[1]).toMatchObject({
      headers: expect.objectContaining({ authorization: 'Bearer rk_live_test' }),
    })
    expect(String(fetch.mock.calls[3]?.[0])).toBe('https://relay.example/v1/a2a/directory?q=private-card-alias')
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

  it('refuses to choose an arbitrary teammate without a target or discovery query', async () => {
    const fleet = new FakeFleetClient()
    await expect(askTeammate(fleet, {
      from: 'factory-worker',
      question: 'Whoever is first, please answer.',
    })).rejects.toThrow('requires a discovered teammate or a skill/tag/query')
  })
})
