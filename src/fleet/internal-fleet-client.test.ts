import { describe, expect, it, vi } from 'vitest'

import type { BrokerEvent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'

class FakeHarnessDriverClient implements HarnessDriverClientLike {
  readonly spawned: SpawnPtyInput[] = []
  readonly released: Array<{ name: string; reason?: string }> = []
  readonly sent: SendMessageInput[] = []
  readonly eventListeners = new Set<(event: BrokerEvent) => void>()
  readonly deliveryListeners = new Set<(event: BrokerEvent) => void>()
  readonly exitListeners = new Set<(agent: { name: string; sessionId?: string }) => void>()
  connectEventsCalls = 0

  agents: Array<{ name: string }> = []
  nextSessionRef = 'session-1'

  async spawnPty(input: SpawnPtyInput): Promise<{ name: string; session_ref: string }> {
    this.spawned.push(input)
    this.agents.push({ name: input.name })
    return { name: input.name, session_ref: this.nextSessionRef }
  }

  async release(name: string, reason?: string): Promise<{ name: string }> {
    this.released.push({ name, reason })
    return { name }
  }

  async listAgents(): Promise<Array<{ name: string }>> {
    return this.agents
  }

  async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
    this.sent.push(input)
    return { event_id: `event-${this.sent.length}`, targets: [input.to] }
  }

  connectEvents(): void {
    this.connectEventsCalls += 1
  }

  onEvent(listener: (event: BrokerEvent) => void): () => void {
    this.eventListeners.add(listener)
    return () => {
      this.eventListeners.delete(listener)
    }
  }

  addListener(event: 'agentExited', listener: (agent: { name: string; sessionId?: string }) => void): () => void
  addListener(event: 'deliveryUpdate', listener: (event: BrokerEvent) => void): () => void
  addListener(
    event: 'agentExited' | 'deliveryUpdate',
    listener: ((agent: { name: string; sessionId?: string }) => void) | ((event: BrokerEvent) => void),
  ): () => void {
    if (event === 'agentExited') {
      this.exitListeners.add(listener as (agent: { name: string; sessionId?: string }) => void)
      return () => {
        this.exitListeners.delete(listener as (agent: { name: string; sessionId?: string }) => void)
      }
    }

    this.deliveryListeners.add(listener as (event: BrokerEvent) => void)
    return () => {
      this.deliveryListeners.delete(listener as (event: BrokerEvent) => void)
    }
  }

  emit(event: BrokerEvent): void {
    for (const listener of this.eventListeners) {
      listener(event)
    }
    for (const listener of this.deliveryListeners) {
      listener(event)
    }
  }

  emitAgentExited(agent: { name: string; sessionId?: string }): void {
    for (const listener of this.exitListeners) {
      listener(agent)
    }
  }
}

describe('InternalFleetClient', () => {
  it('maps spawn input to harness spawnPty and returns the broker session ref', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await expect(
      fleet.spawn({
        name: 'ar-1-impl',
        capability: 'spawn:codex',
        node: 'self',
        task: 'do work',
        model: 'gpt-5',
        sessionRef: 'previous-session',
        restartPolicy: { max_restarts: 2 },
        channel: 'factory',
      }),
    ).resolves.toEqual({ name: 'ar-1-impl', sessionRef: 'session-1' })

    expect(harness.spawned).toEqual([
      {
        name: 'ar-1-impl',
        cli: 'codex',
        channels: ['factory'],
        task: 'do work',
        model: 'gpt-5',
        cwd: '/worktree',
        restartPolicy: { max_restarts: 2 },
        continueFrom: 'previous-session',
      },
    ])
  })

  it('maps claude capability and per-spawn cwd', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/default' })

    await fleet.spawn({
      name: 'ar-1-review',
      capability: 'spawn:claude',
      cwd: '/review-worktree',
    })

    expect(harness.spawned[0]).toMatchObject({
      name: 'ar-1-review',
      cli: 'claude',
      cwd: '/review-worktree',
    })
  })

  it('resumes by passing continueFrom to spawnPty', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.nextSessionRef = 'resumed-session'
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await expect(fleet.resume({ name: 'ar-1-impl', sessionRef: 'session-original', node: 'self' })).resolves.toEqual({
      name: 'ar-1-impl',
      sessionRef: 'resumed-session',
    })

    expect(harness.spawned[0]).toMatchObject({
      name: 'ar-1-impl',
      cli: 'codex',
      cwd: '/worktree',
      continueFrom: 'session-original',
    })
  })

  it('resumes with the per-agent capability when provided', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await fleet.resume({
      name: 'ar-1-review',
      sessionRef: 'review-session',
      node: 'self',
      capability: 'spawn:claude',
    })

    expect(harness.spawned[0]).toMatchObject({
      name: 'ar-1-review',
      cli: 'claude',
      cwd: '/worktree',
      continueFrom: 'review-session',
    })
  })

  it('rejects non-self placement for the internal single-node backend', async () => {
    const fleet = new InternalFleetClient({ client: new FakeHarnessDriverClient() })

    await expect(fleet.spawn({ name: 'remote', capability: 'spawn:codex', node: 'node-a' })).rejects.toThrow(
      "only supports node 'self'",
    )
  })

  it('releases and reports roster with the self node capabilities', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [{ name: 'ar-1-impl' }]
    const fleet = new InternalFleetClient({ client: harness })

    await fleet.release('ar-1-impl', 'done')

    expect(harness.released).toEqual([{ name: 'ar-1-impl', reason: 'done' }])
    await expect(fleet.roster()).resolves.toEqual({
      agents: [{ name: 'ar-1-impl' }],
      nodes: [{ name: 'self', capabilities: ['spawn:claude', 'spawn:codex'], live: true }],
    })
  })

  it('sends messages and exposes the broker event id for injected confirmation', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await fleet.sendMessage({ to: 'ar-1-review', from: 'ar-1-impl', text: 'PR ready', data: { pr: 1 } })
    const injected = fleet.waitForInjected({ to: 'broker', text: 'done' }, { timeoutMs: 1000 })
    await Promise.resolve()

    harness.emit({
      kind: 'delivery_injected',
      name: 'broker',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({
      eventId: 'event-2',
      targets: ['broker'],
    })

    expect(harness.sent).toEqual([
      { to: 'ar-1-review', from: 'ar-1-impl', text: 'PR ready', data: { pr: 1 } },
      { to: 'broker', text: 'done', from: undefined, data: undefined },
    ])
  })

  it('starts the harness broker event stream on the connect-backed subscription path', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'broker', text: 'done' }, { timeoutMs: 1000 })
    await Promise.resolve()

    expect(harness.connectEventsCalls).toBe(1)
    fleet.onDeliveryFailed(() => {})
    fleet.onAgentExit(() => {})
    expect(harness.connectEventsCalls).toBe(1)

    harness.emit({
      kind: 'delivery_injected',
      name: 'broker',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
    })

    await expect(injected).resolves.toEqual({
      eventId: 'event-1',
      targets: ['broker'],
    })
  })

  it('rejects waitForInjected on correlated delivery failure', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const injected = fleet.waitForInjected({ to: 'ar-1-review', text: 'PR ready' }, { timeoutMs: 1000 })
    await Promise.resolve()

    harness.emit({
      kind: 'delivery_failed',
      name: 'ar-1-review',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
      reason: 'recipient unavailable',
    })

    await expect(injected).rejects.toThrow('recipient unavailable')
  })

  it('times out waitForInjected when no delivery_injected event arrives', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await expect(fleet.waitForInjected({ to: 'broker', text: 'done' }, { timeoutMs: 1 })).rejects.toThrow(
      'Timed out waiting for delivery_injected for event-1',
    )
  })

  it('surfaces broker delivery failures and agent exits once for duplicate events', () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const deliveryFailures: Array<{ to: string; msgId?: string; reason?: string }> = []
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onDeliveryFailed((info) => deliveryFailures.push(info))
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    const failed = {
      kind: 'message_delivery_failed',
      name: 'broker',
      event_id: 'event-1',
      from: 'ar-1-impl',
      to: 'ar-1-review',
      attempts: 3,
      lastError: 'recipient unavailable',
    } satisfies BrokerEvent
    const exited = {
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'crashed',
    } satisfies BrokerEvent

    harness.emit(failed)
    harness.emit(failed)
    harness.emit(exited)
    harness.emit(exited)

    expect(deliveryFailures).toEqual([{ to: 'ar-1-review', msgId: 'event-1', reason: 'recipient unavailable' }])
    expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'crashed' }])
  })

  it('latches one agent death by name across lagged exit callbacks', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-06-11T00:00:00.000Z'))

    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness })
      const exits: Array<{ name: string; reason?: string }> = []

      fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

      harness.emit({
        kind: 'agent_exited',
        name: 'ar-1-impl',
        code: 1,
        reason: 'pty_closed',
        event_id: 'exit-event-1',
      } as BrokerEvent)
      vi.advanceTimersByTime(10_000)
      harness.emit({
        kind: 'agent_exited',
        name: 'ar-1-impl',
        code: 1,
        reason: 'pty_closed',
        event_id: 'exit-event-2',
      } as BrokerEvent)

      expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'pty_closed' }])
    } finally {
      vi.useRealTimers()
    }
  })

  it('does not suppress exits for different agent names', () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'pty_closed',
      event_id: 'exit-event-1',
    } as BrokerEvent)
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-review',
      code: 1,
      reason: 'pty_closed',
      event_id: 'exit-event-2',
    } as BrokerEvent)

    expect(exits).toEqual([
      { name: 'ar-1-impl', reason: 'pty_closed' },
      { name: 'ar-1-review', reason: 'pty_closed' },
    ])
  })

  it('clears the exit latch when the same agent name is spawned again', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:codex' })
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'first-death',
      event_id: 'exit-event-1',
    } as BrokerEvent)
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'lagged-duplicate',
      event_id: 'exit-event-2',
    } as BrokerEvent)

    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:codex' })
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'second-death',
      event_id: 'exit-event-3',
    } as BrokerEvent)

    expect(exits).toEqual([
      { name: 'ar-1-impl', reason: 'first-death' },
      { name: 'ar-1-impl', reason: 'second-death' },
    ])
  })

  it('clears the exit latch when the same agent name is resumed', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'first-death',
      event_id: 'exit-event-1',
    } as BrokerEvent)
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'lagged-duplicate',
      event_id: 'exit-event-2',
    } as BrokerEvent)

    await fleet.resume({ name: 'ar-1-impl', sessionRef: 'session-original' })
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-1-impl',
      code: 1,
      reason: 'second-death',
      event_id: 'exit-event-3',
    } as BrokerEvent)

    expect(exits).toEqual([
      { name: 'ar-1-impl', reason: 'first-death' },
      { name: 'ar-1-impl', reason: 'second-death' },
    ])
  })

  it('suppresses typed duplicate agent-exit callbacks until the name is spawned again', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    harness.emitAgentExited({ name: 'ar-1-impl', sessionId: 'session-1' })
    harness.emitAgentExited({ name: 'ar-1-impl', sessionId: 'session-2' })

    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:codex' })
    harness.emitAgentExited({ name: 'ar-1-impl', sessionId: 'session-3' })

    expect(exits).toEqual([
      { name: 'ar-1-impl', reason: 'exited' },
      { name: 'ar-1-impl', reason: 'exited' },
    ])
  })

  it('surfaces same-name exits again only after a lifecycle restart, not after time passes', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-06-11T00:00:00.000Z'))

    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness })
      const exits: Array<{ name: string; reason?: string }> = []

      fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

      harness.emit({
        kind: 'agent_exited',
        name: 'ar-1-impl',
        code: 1,
        reason: 'pty_closed',
        event_id: 'exit-event-1',
      } as BrokerEvent)
      vi.advanceTimersByTime(60_000)
      harness.emit({
        kind: 'agent_exited',
        name: 'ar-1-impl',
        code: 1,
        reason: 'pty_closed',
        event_id: 'exit-event-2',
      } as BrokerEvent)
      harness.emit({
        kind: 'agent_exited',
        name: 'ar-1-review',
        code: 1,
        reason: 'pty_closed',
        event_id: 'exit-event-3',
      } as BrokerEvent)

      expect(exits).toEqual([
        { name: 'ar-1-impl', reason: 'pty_closed' },
        { name: 'ar-1-review', reason: 'pty_closed' },
      ])
    } finally {
      vi.useRealTimers()
    }
  })
})
