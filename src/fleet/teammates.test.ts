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
    Object.assign(fleet, { effectiveSender: async () => 'factory-app' })
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
    })).rejects.toThrow(/already has an unanswered question to "infra-agent" as "factory-app"/u)

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

  it('canonicalizes @ aliases when claiming a requester-teammate pair', async () => {
    const fleet = new FakeFleetClient()
    const teammate = {
      name: 'infra-agent',
      address: '@infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native' as const,
    }
    fleet.teammates.push(teammate)

    const first = askTeammate(fleet, {
      from: '@factory-worker', question: 'q1', teammate, timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))

    await expect(askTeammate(fleet, {
      from: 'factory-worker',
      question: 'q2',
      teammate: { ...teammate, address: 'infra-agent' },
      timeoutMs: 1_000,
    })).rejects.toThrow(/already has an unanswered question to "infra-agent"/u)

    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'answer-1' })
    await expect(first).resolves.toMatchObject({ reply: { body: 'answer-1' } })
  })

  it('keeps distinct requesters to one teammate independent', async () => {
    // Different reply targets are separate keys, so these must not contend.
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

  // `AgentMessage` echoes no request field on ANY backend, so the SAME
  // requester asking one teammate twice is ambiguous even where `from` is
  // carried faithfully. An earlier version of this guard exempted such
  // backends; the exemption was wrong. (#178 review, codex P2 second pass)
  it('refuses a same-requester overlap even on a backend that carries `from`', async () => {
    const fleet = new FakeFleetClient()
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    const first = askTeammate(fleet, {
      from: 'factory-worker', question: 'q1', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    await expect(askTeammate(fleet, {
      from: 'factory-worker', question: 'q2', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })).rejects.toThrow(/already has an unanswered question to "infra-agent"/u)
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'answer-1' })
    await expect(first).resolves.toMatchObject({ reply: { body: 'answer-1' } })
  })

  // Two callers passing the same query resolve to the same teammate, so the
  // claim has to be taken against the RESOLVED target, not against a
  // caller-supplied one. (#178 review, codex P2 second pass)
  it('claims a discovered teammate, so query-based asks contend too', async () => {
    const fleet = new FakeFleetClient()
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    const first = askTeammate(fleet, {
      from: 'factory-worker', question: 'q1', skill: 'infra-watch', timeoutMs: 1_000,
    })
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    await expect(askTeammate(fleet, {
      from: 'factory-worker', question: 'q2', skill: 'infra-watch', timeoutMs: 1_000,
    })).rejects.toThrow(/already has an unanswered question to "infra-agent"/u)
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'answer-1' })
    await expect(first).resolves.toMatchObject({ reply: { body: 'answer-1' } })
  })

  // `onAgentMessage` can return before the transport is really listening, so a
  // reply that lands between registration and connection is lost. The wait
  // must happen before the send. (#178 review, codex P1 third pass)
  it('waits for the message transport to be observable before sending', async () => {
    const fleet = new FakeFleetClient()
    let openTransport = () => {}
    const observable = new Promise<void>((r) => { openTransport = r })
    Object.assign(fleet, { whenMessagesObservable: () => observable })
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    const asked = askTeammate(fleet, {
      from: 'factory-worker', question: 'q', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })
    // Give the async body every chance to run ahead of the gate.
    await Promise.resolve()
    await Promise.resolve()
    expect(fleet.messages).toHaveLength(0)
    openTransport()
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'answered' })
    await expect(asked).resolves.toMatchObject({ reply: { body: 'answered' } })
  })

  // A reply to an abandoned question is indistinguishable from a reply to a
  // fresh one, and an unrelated DM cannot prove which message is the old
  // answer. A confirmed timed-out send therefore stays quarantined for this
  // client until the protocol supplies correlation.
  // (#178 review, codex P2 third pass)
  it('keeps a confirmed timed-out pair quarantined across time and unrelated messages', async () => {
    vi.useFakeTimers()
    try {
      const fleet = new FakeFleetClient()
      fleet.teammates.push({
        name: 'infra-agent',
        address: 'infra-agent',
        skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
        tags: [],
        url: 'https://relay.example/a2a/rpc',
        kind: 'native',
      })
      const first = askTeammate(fleet, {
        from: 'factory-worker', question: 'q1', teammate: fleet.teammates[0], timeoutMs: 20,
      })
      const rejected = expect(first).rejects.toThrow(/Timed out waiting for a reply/u)
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      expect(fleet.messages).toHaveLength(1)
      await vi.advanceTimersByTimeAsync(20)
      await rejected

      // Moving far beyond the former 2 * timeout window MUST NOT reopen the
      // pair while the abandoned response is still outstanding.
      await vi.advanceTimersByTimeAsync(2_000)
      await expect(askTeammate(fleet, {
        from: 'factory-worker', question: 'q2', teammate: fleet.teammates[0], timeoutMs: 20,
      })).rejects.toThrow(/quarantining "infra-agent" for "factory-worker" after a timed-out question/u)

      fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'unrelated proactive DM' })
      await expect(askTeammate(fleet, {
        from: 'factory-worker', question: 'q3', teammate: fleet.teammates[0], timeoutMs: 20,
      })).rejects.toThrow(/quarantining "infra-agent" for "factory-worker" after a timed-out question/u)

      fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'late answer to q1' })
      await expect(askTeammate(fleet, {
        from: 'factory-worker', question: 'q4', teammate: fleet.teammates[0], timeoutMs: 20,
      })).rejects.toThrow(/quarantining "infra-agent" for "factory-worker" after a timed-out question/u)
      expect(fleet.messages).toHaveLength(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('releases a timed-out quarantine after the pending delivery definitively fails', async () => {
    vi.useFakeTimers()
    try {
      let rejectFirstDelivery = (_error: Error) => {}
      let firstDelivery = true
      class LateFailingFleetClient extends FakeFleetClient {
        override async waitForInjected(input: Parameters<FakeFleetClient['waitForInjected']>[0]): Promise<{ eventId: string; targets: string[] }> {
          if (!firstDelivery) return await super.waitForInjected(input)
          firstDelivery = false
          this.messages.push(input)
          return await new Promise((_, reject) => { rejectFirstDelivery = reject })
        }
      }
      const fleet = new LateFailingFleetClient()
      fleet.teammates.push({
        name: 'infra-agent',
        address: 'infra-agent',
        skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
        tags: [],
        url: 'https://relay.example/a2a/rpc',
        kind: 'native',
      })
      const first = askTeammate(fleet, {
        from: 'factory-worker', question: 'q1', teammate: fleet.teammates[0], timeoutMs: 20,
      })
      const rejected = expect(first).rejects.toThrow(/Timed out waiting for a reply/u)
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      expect(fleet.messages).toHaveLength(1)
      await vi.advanceTimersByTimeAsync(20)
      await rejected

      await expect(askTeammate(fleet, {
        from: 'factory-worker', question: 'q2', teammate: fleet.teammates[0], timeoutMs: 1_000,
      })).rejects.toThrow(/quarantining "infra-agent"/u)

      rejectFirstDelivery(new Error('delivery definitively rejected'))
      await Promise.resolve()
      await Promise.resolve()

      const retry = askTeammate(fleet, {
        from: 'factory-worker', question: 'q3', teammate: fleet.teammates[0], timeoutMs: 1_000,
      })
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      expect(fleet.messages).toHaveLength(2)
      fleet.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'answer to q3' })
      await expect(retry).resolves.toMatchObject({ reply: { body: 'answer to q3' } })
    } finally {
      vi.useRealTimers()
    }
  })

  // Two clients address different workspaces; identical names there cannot
  // collide and must not contend. (#178 review, codex P2 third pass)
  it('scopes claims per fleet client', async () => {
    const teammate = {
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native' as const,
    }
    const one = new FakeFleetClient()
    const two = new FakeFleetClient()
    one.teammates.push(teammate)
    two.teammates.push(teammate)

    const a = askTeammate(one, { from: 'factory-worker', question: 'q', teammate, timeoutMs: 1_000 })
    await vi.waitFor(() => expect(one.messages).toHaveLength(1))
    // Same requester, same teammate name, different backend: must be allowed.
    const b = askTeammate(two, { from: 'factory-worker', question: 'q', teammate, timeoutMs: 1_000 })
    await vi.waitFor(() => expect(two.messages).toHaveLength(1))

    one.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'from-one' })
    two.emitAgentMessage({ from: 'infra-agent', target: 'factory-worker', body: 'from-two' })
    await expect(a).resolves.toMatchObject({ reply: { body: 'from-one' } })
    await expect(b).resolves.toMatchObject({ reply: { body: 'from-two' } })
  })

  // A backend whose authenticated identity is only knowable after a round trip
  // must be awaited; matching on a pre-auth guess rejects every reply.
  // (#178 review, codex P1 second pass)
  it('awaits an asynchronously resolved sender before matching replies', async () => {
    const fleet = new FakeFleetClient()
    let resolveIdentity = () => {}
    const identityKnown = new Promise<void>((r) => { resolveIdentity = r })
    Object.assign(fleet, {
      effectiveSender: async () => {
        await identityKnown
        return 'authenticated-name'
      },
    })
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: [],
      url: 'https://relay.example/a2a/rpc',
      kind: 'native',
    })
    const asked = askTeammate(fleet, {
      from: 'configured-guess', question: 'q', teammate: fleet.teammates[0], timeoutMs: 1_000,
    })
    // Nothing may be sent until the real identity is known.
    expect(fleet.messages).toHaveLength(0)
    resolveIdentity()
    await vi.waitFor(() => expect(fleet.messages).toHaveLength(1))
    fleet.emitAgentMessage({ from: 'infra-agent', target: 'authenticated-name', body: 'answered' })
    await expect(asked).resolves.toMatchObject({ reply: { body: 'answered' } })
  })
})
