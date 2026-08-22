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

  // A backend that cannot represent `SendInput.from` authors every send as its
  // own identity, so the teammate replies to THAT name and the reply arrives
  // addressed to it -- never to the worker that asked. Matching on the caller's
  // `from` would discard every valid reply and time out. (#178 review, codex P1)
  const relayLike = () => {
    const fleet = new FakeFleetClient()
    Object.assign(fleet, { effectiveSender: () => 'factory-app' })
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    return fleet
  }

  it('matches the reply against the identity the backend actually sends as', async () => {
    const fleet = relayLike()

    const asked = askTeammate(fleet, {
      from: 'factory-worker',
      question: 'Is the deploy healthy?',
      teammate: fleet.teammates[0],
      timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    // Addressed to the authenticated sender, which is what this backend receives.
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-app', body: 'All systems green.' })

    await expect(asked).resolves.toMatchObject({
      reply: { from: 'infra-agent', target: 'factory-app', body: 'All systems green.' },
    })
  })

  it('rejects a second unanswered question to one teammate when replies cannot be correlated', async () => {
    const fleet = relayLike()
    const first = askTeammate(fleet, {
      from: 'factory-worker',
      question: 'Is the deploy healthy?',
      teammate: fleet.teammates[0],
      timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))

    // `data` is dropped by such a backend, so `requestId` never reaches the
    // teammate and a reply cannot be attributed to one of two open questions.
    // Refusing beats resolving the wrong waiter. (#178 review, codex P2)
    await expect(askTeammate(fleet, {
      from: 'factory-worker',
      question: 'And the database?',
      teammate: fleet.teammates[0],
      timeoutMs: 1_000,
    })).rejects.toThrow(/already has an unanswered question to this teammate/u)

    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-app', body: 'All systems green.' })
    await expect(first).resolves.toMatchObject({ reply: { body: 'All systems green.' } })

    // The refused ask never sent, so this is only the second message on the
    // wire -- and the claim is released once the first settles, so the pair is
    // reusable rather than poisoned for the rest of the process.
    const third = askTeammate(fleet, {
      from: 'factory-worker',
      question: 'And the database?',
      teammate: fleet.teammates[0],
      timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(2))
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-app', body: 'Healthy.' })
    await expect(third).resolves.toMatchObject({ reply: { body: 'Healthy.' } })
  })

  it('leaves a faithful backend uncorrelated-guard-free', async () => {
    // FakeFleetClient carries `from`, so effectiveSender() is undefined and two
    // concurrent asks stay legal -- the guard must not punish a correct backend.
    const fleet = new FakeFleetClient()
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    const a = askTeammate(fleet, {
      from: 'worker-a', question: 'q1', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })
    const b = askTeammate(fleet, {
      from: 'worker-b', question: 'q2', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(2))
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'worker-a', body: 'answer-a' })
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'worker-b', body: 'answer-b' })
    await expect(a).resolves.toMatchObject({ reply: { body: 'answer-a' } })
    await expect(b).resolves.toMatchObject({ reply: { body: 'answer-b' } })
  })
})
