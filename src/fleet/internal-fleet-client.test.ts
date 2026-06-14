import { describe, expect, it, vi } from 'vitest'

import type { BrokerEvent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'

class FakeHarnessDriverClient implements HarnessDriverClientLike {
  brokerPid: number | undefined
  readonly spawned: SpawnPtyInput[] = []
  readonly released: Array<{ name: string; reason?: string }> = []
  readonly sent: SendMessageInput[] = []
  readonly inputs: Array<{ name: string; data: string }> = []
  readonly eventListeners = new Set<(event: BrokerEvent) => void>()
  readonly deliveryListeners = new Set<(event: BrokerEvent) => void>()
  readonly exitListeners = new Set<(agent: { name: string; sessionId?: string }) => void>()
  connectEventsCalls = 0
  disconnectCalls = 0
  throwOnConnect = false
  partiallyConnected = false

  agents: Array<{ name: string; pid?: number }> = []
  nextSessionRef = 'session-1'
  nextPid: number | undefined

  async spawnPty(input: SpawnPtyInput): Promise<{ name: string; session_ref: string; pid?: number }> {
    this.spawned.push(input)
    this.agents.push({ name: input.name, pid: this.nextPid })
    return { name: input.name, session_ref: this.nextSessionRef }
  }

  async release(name: string, reason?: string): Promise<{ name: string }> {
    this.released.push({ name, reason })
    return { name }
  }

  async listAgents(): Promise<Array<{ name: string; pid?: number }>> {
    return this.agents
  }

  async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets?: string[] }> {
    this.sent.push(input)
    return { event_id: `event-${this.sent.length}`, targets: [input.to] }
  }

  async sendInput(name: string, data: string): Promise<void> {
    this.inputs.push({ name, data })
  }

  connectEvents(): void {
    this.connectEventsCalls += 1
    if (this.throwOnConnect) {
      this.partiallyConnected = true
      throw new Error('connect failed after opening event stream')
    }
  }

  disconnect(): void {
    this.disconnectCalls += 1
    this.partiallyConnected = false
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
    const fleet = new InternalFleetClient({
      client: harness,
      cwd: '/worktree',
      resolveAgentRelayMcpCommand: () => ({ command: '/usr/local/bin/node', args: ['/repo/node_modules/agent-relay/dist/cli/index.js', 'mcp'] }),
    })

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
        harnessConfig: expect.objectContaining({
          runtime: 'pty',
          command: 'codex',
          cwd: '/worktree',
          env: expect.objectContaining({
            RELAY_AGENT_NAME: 'ar-1-impl',
            RELAY_AGENT_TYPE: 'agent',
            RELAY_STRICT_AGENT_NAME: '1',
          }),
          metadata: expect.objectContaining({
            factoryRelayMcp: true,
            relayMcpCommand: '/usr/local/bin/node',
            relayMcpArgs: ['/repo/node_modules/agent-relay/dist/cli/index.js', 'mcp'],
          }),
        }),
      },
    ])
    const config = harness.spawned[0]?.harnessConfig
    expect(config?.runtime).toBe('pty')
    if (config?.runtime !== 'pty') throw new Error('expected pty harness config')
    expect(config.args).toEqual(expect.arrayContaining([
      '--model',
      'gpt-5',
      '--config',
      'mcp_servers.agent-relay.command="/usr/local/bin/node"',
      'mcp_servers.agent-relay.args=["/repo/node_modules/agent-relay/dist/cli/index.js", "mcp"]',
      'do work',
    ]))
    expect(config.args.join('\n')).toContain('"RELAY_AGENT_NAME" = "ar-1-impl"')
  })

  it('injects agent-relay MCP config for Claude factory spawns', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({
      client: harness,
      cwd: '/worktree',
      resolveAgentRelayMcpCommand: () => ({ command: '/usr/local/bin/node', args: ['/repo/node_modules/agent-relay/dist/cli/index.js', 'mcp'] }),
    })

    await fleet.spawn({
      name: 'ar-1-review',
      capability: 'spawn:claude',
      model: 'anthropic/claude-3-5-sonnet',
      task: 'review it',
    })

    const config = harness.spawned[0]?.harnessConfig
    expect(config?.runtime).toBe('pty')
    if (config?.runtime !== 'pty') throw new Error('expected pty harness config')
    expect(config.command).toBe('claude')
    expect(config.args).toEqual(expect.arrayContaining([
      '--model',
      'anthropic/claude-3-5-sonnet',
      '--mcp-config',
      '--strict-mcp-config',
      'review it',
    ]))
    const mcpConfigIndex = config.args.indexOf('--mcp-config')
    const payload = JSON.parse(config.args[mcpConfigIndex + 1]) as {
      mcpServers: Record<string, { command: string; args: string[]; env: Record<string, string> }>
    }
    expect(payload.mcpServers['agent-relay']).toMatchObject({
      type: 'stdio',
      command: '/usr/local/bin/node',
      args: ['/repo/node_modules/agent-relay/dist/cli/index.js', 'mcp'],
      env: {
        RELAY_AGENT_NAME: 'ar-1-review',
        RELAY_AGENT_TYPE: 'agent',
        RELAY_STRICT_AGENT_NAME: '1',
      },
    })
  })

  it('falls back to ordinary spawn when agent-relay MCP cannot be resolved', async () => {
    const harness = new FakeHarnessDriverClient()
    const logger = { warn: vi.fn() }
    const fleet = new InternalFleetClient({
      client: harness,
      cwd: '/worktree',
      logger,
      resolveAgentRelayMcpCommand: () => undefined,
    })

    await fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      task: 'do work',
    })

    expect(harness.spawned[0]).toEqual({
      name: 'ar-1-impl',
      cli: 'codex',
      channels: undefined,
      task: 'do work',
      model: undefined,
      cwd: '/worktree',
      restartPolicy: undefined,
      continueFrom: undefined,
    })
    expect(logger.warn).toHaveBeenCalledWith(
      '[factory-sdk] agent-relay MCP command not found; spawning without MCP injection',
      { agent: 'ar-1-impl', cli: 'codex' },
    )
  })

  it('resolves the default agent-relay CLI mcp command from node_modules', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      task: 'do work',
    })

    const config = harness.spawned[0]!.harnessConfig
    expect(config?.runtime).toBe('pty')
    if (config?.runtime !== 'pty') throw new Error('expected pty harness config')
    const args = config.args
    expect(args).toContain(`mcp_servers.agent-relay.command=${JSON.stringify(process.execPath)}`)
    expect(args.join('\n')).toContain('node_modules/agent-relay/dist/cli/index.js')
    expect(args.join('\n')).toContain('"mcp"')
  })

  it('resolves an agent PID from the broker roster', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [{ name: 'ar-1-impl', pid: 901969 }]
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await expect(fleet.resolveAgentPid('ar-1-impl')).resolves.toEqual({ status: 'found', pid: 901969 })
  })

  it('retries roster PID lookup when broker spawned-list registration lags spawn ack', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      let listCalls = 0
      harness.listAgents = async () => {
        listCalls += 1
        return listCalls === 1 ? [{ name: 'ar-1-impl' }] : [{ name: 'ar-1-impl', pid: 901969 }]
      }
      const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

      const resolved = fleet.resolveAgentPid('ar-1-impl')
      await vi.advanceTimersByTimeAsync(75)

      await expect(resolved).resolves.toEqual({ status: 'found', pid: 901969 })
      expect(listCalls).toBe(2)
    } finally {
      vi.useRealTimers()
    }
  })

  it('distinguishes absent agents from live agents with no PID', async () => {
    vi.useFakeTimers()
    try {
      const absentHarness = new FakeHarnessDriverClient()
      const absentFleet = new InternalFleetClient({ client: absentHarness, cwd: '/worktree' })
      const absent = absentFleet.resolveAgentPid('ar-1-impl')
      await vi.advanceTimersByTimeAsync(150)
      await expect(absent).resolves.toEqual({ status: 'missing' })

      const unresolvedHarness = new FakeHarnessDriverClient()
      unresolvedHarness.agents = [{ name: 'ar-1-impl' }]
      const unresolvedFleet = new InternalFleetClient({ client: unresolvedHarness, cwd: '/worktree' })
      const unresolved = unresolvedFleet.resolveAgentPid('ar-1-impl')
      await vi.advanceTimersByTimeAsync(150)
      await expect(unresolved).resolves.toEqual({ status: 'unresolved' })
    } finally {
      vi.useRealTimers()
    }
  })

  it('surfaces the broker pid as protected process state', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.brokerPid = 68009
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await expect(fleet.protectedPids()).resolves.toEqual([68009])
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

  it('normalizes missing injected targets from the live broker ack', async () => {
    class NoTargetsHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string }> {
        this.sent.push(input)
        return { event_id: `event-${this.sent.length}` }
      }
    }
    const harness = new NoTargetsHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await Promise.resolve()
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
    })

    await expect(injected).resolves.toEqual({
      eventId: 'event-1',
      targets: [],
    })
  })

  it('passes raw PTY input through to the harness client', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await fleet.sendInput('ar-1-impl', '\r')

    expect(harness.inputs).toEqual([{ name: 'ar-1-impl', data: '\r' }])
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

  it('disposes broker subscriptions, pending delivery timers, and the harness connection idempotently', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'broker', text: 'done' }, { timeoutMs: 1000 })
    const rejected = expect(injected).rejects.toThrow('InternalFleetClient disposed')
    const offExit = fleet.onAgentExit(() => {})
    const offDelivery = fleet.onDeliveryFailed(() => {})
    await Promise.resolve()

    expect(harness.connectEventsCalls).toBe(1)
    expect(harness.eventListeners.size).toBe(1)
    expect(harness.deliveryListeners.size).toBe(1)
    expect(harness.exitListeners.size).toBe(1)

    await fleet.dispose()

    expect(harness.disconnectCalls).toBe(1)
    expect(harness.eventListeners.size).toBe(0)
    expect(harness.deliveryListeners.size).toBe(0)
    expect(harness.exitListeners.size).toBe(0)
    await rejected

    offExit()
    offDelivery()
    await fleet.dispose()
    expect(harness.disconnectCalls).toBe(1)
  })

  it('disposes safely after a broker event stream throws during partial connect', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.throwOnConnect = true
    const fleet = new InternalFleetClient({ client: harness })

    expect(() => fleet.onAgentExit(() => {})).toThrow('connect failed after opening event stream')
    expect(harness.partiallyConnected).toBe(true)
    expect(harness.eventListeners.size).toBe(1)
    expect(harness.exitListeners.size).toBe(1)

    await fleet.dispose()
    await fleet.dispose()

    expect(harness.disconnectCalls).toBe(1)
    expect(harness.partiallyConnected).toBe(false)
    expect(harness.eventListeners.size).toBe(0)
    expect(harness.exitListeners.size).toBe(0)
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
