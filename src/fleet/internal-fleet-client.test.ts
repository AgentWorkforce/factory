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
  shutdownCalls = 0
  throwOnRelease = false
  throwOnShutdown = false
  throwOnConnect = false
  partiallyConnected = false

  agents: Array<{ name: string; cli?: string; pid?: number }> = []
  nextSessionRef = 'session-1'
  nextHandleName: string | undefined
  nextPid: number | undefined

  async spawnPty(input: SpawnPtyInput): Promise<{ name: string; session_ref: string; pid?: number }> {
    this.spawned.push(input)
    const name = this.nextHandleName ?? input.name
    this.agents.push({ name, cli: input.cli, pid: this.nextPid })
    return { name, session_ref: this.nextSessionRef }
  }

  async release(name: string, reason?: string): Promise<{ name: string }> {
    this.released.push({ name, reason })
    if (this.throwOnRelease) {
      throw new Error('release failed')
    }
    return { name }
  }

  async listAgents(): Promise<Array<{ name: string; cli?: string; pid?: number }>> {
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

  async shutdown(): Promise<void> {
    this.shutdownCalls += 1
    if (this.throwOnShutdown) {
      throw new Error('shutdown failed')
    }
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
        spawnMode: 'task_exit',
        exitAfterTask: true,
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

  it('threads a resolved workspace key into spawned agent MCP env', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({
      client: harness,
      workspaceKey: 'rk_live_from_broker',
      resolveAgentRelayMcpCommand: () => ({ command: '/usr/local/bin/node', args: ['/repo/node_modules/agent-relay/dist/cli/index.js', 'mcp'] }),
    })

    await fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      task: 'do work',
    })

    const env = harness.spawned[0]?.harnessConfig?.env
    expect(env).toEqual(expect.objectContaining({
      RELAY_WORKSPACE_KEY: 'rk_live_from_broker',
      RELAY_API_KEY: 'rk_live_from_broker',
    }))
    expect(harness.spawned[0]?.harnessConfig?.args.join('\n')).toContain('"RELAY_WORKSPACE_KEY" = "rk_live_from_broker"')
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
      spawnMode: 'task_exit',
      exitAfterTask: true,
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

  it('shuts down the broker on dispose when it owns one (spawned)', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, ownsBroker: true, cwd: '/worktree' })

    await fleet.dispose()

    expect(harness.shutdownCalls).toBe(1)
    expect(harness.disconnectCalls).toBe(0)
  })

  it('preserves a cold-started broker only after infrastructure ownership is relinquished', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, ownsBroker: true, cwd: '/worktree' })

    fleet.preserveInfrastructureOnDispose()
    await fleet.dispose()

    expect(harness.shutdownCalls).toBe(0)
    expect(harness.disconnectCalls).toBe(1)
  })

  it('keeps an owned broker alive until all dispatched agents exit across both broker event shapes', async () => {
    const harness = new FakeHarnessDriverClient()
    const logger = { info: vi.fn() }
    const fleet = new InternalFleetClient({
      client: harness,
      ownsBroker: true,
      ownedBrokerAgentExitTimeoutMs: 1_000,
      cwd: '/worktree',
      logger,
    })

    await fleet.spawn({ name: 'ar-59-impl', capability: 'spawn:codex', task: 'fix issue 59' })
    await fleet.spawn({ name: 'ar-59-review', capability: 'spawn:claude', task: 'review issue 59' })
    const implInjected = fleet.waitForInjected({ to: 'ar-59-impl', text: 'fix issue 59' })
    const reviewInjected = fleet.waitForInjected({ to: 'ar-59-review', text: 'review issue 59' })
    await Promise.resolve()
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-59-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
    })
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-59-review',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })
    await implInjected
    await reviewInjected

    const disposed = fleet.dispose()
    await Promise.resolve()
    expect(harness.shutdownCalls).toBe(0)
    expect(logger.info).toHaveBeenCalledWith(
      '[factory-sdk] waiting for spawned agents to exit before shutting down the owned relay broker',
      {
        agents: ['ar-59-impl', 'ar-59-review'],
        timeoutMs: 1_000,
        remainingCount: 2,
      },
    )

    harness.emit({
      kind: 'agent_exit',
      name: 'ar-59-impl',
      reason: 'task_exit',
    } as BrokerEvent)
    await Promise.resolve()
    expect(harness.shutdownCalls).toBe(0)

    harness.emit({
      kind: 'agent_exited',
      name: 'ar-59-review',
      code: 0,
      reason: 'task_exit',
    } as BrokerEvent)
    await disposed

    expect(harness.shutdownCalls).toBe(1)
    expect(harness.disconnectCalls).toBe(0)
  })

  it('does not delay owned broker shutdown after a factory-stopped release', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, ownsBroker: true })
    await fleet.spawn({ name: 'ar-59-live', capability: 'spawn:codex' })

    await fleet.release('ar-59-live', 'factory-stopped')
    await fleet.dispose()

    expect(harness.released).toEqual([{ name: 'ar-59-live', reason: 'factory-stopped' }])
    expect(harness.shutdownCalls).toBe(1)
  })

  it('does not delay owned broker shutdown after a resumed agent is marked terminal', async () => {
    const harness = new FakeHarnessDriverClient()
    const logger = { info: vi.fn(), warn: vi.fn() }
    const fleet = new InternalFleetClient({
      client: harness,
      ownsBroker: true,
      ownedBrokerAgentExitTimeoutMs: 25,
      logger,
    })
    await fleet.resume({ name: 'ar-59-resumed', sessionRef: 'session-original' })

    const disposed = fleet.dispose()
    await Promise.resolve()
    expect(logger.info).toHaveBeenCalledWith(
      '[factory-sdk] waiting for spawned agents to exit before shutting down the owned relay broker',
      expect.objectContaining({ agents: ['ar-59-resumed'], remainingCount: 1 }),
    )

    fleet.markAgentTerminal('ar-59-resumed', 'resume-already-exists')
    await disposed

    expect(harness.shutdownCalls).toBe(1)
    expect(logger.warn).not.toHaveBeenCalled()
  })

  it('marks terminal idempotently and suppresses a late real exit', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const listener = vi.fn()
    fleet.onAgentExit(listener)
    await fleet.spawn({ name: 'ar-59-terminal', capability: 'spawn:codex' })

    fleet.markAgentTerminal('ar-59-terminal', 'resume-already-exists')
    fleet.markAgentTerminal('ar-59-terminal', 'resume-already-exists')
    harness.emit({
      kind: 'agent_exit',
      name: 'ar-59-terminal',
      reason: 'task_exit',
      event_id: 'late-terminal-exit',
    } as BrokerEvent)

    expect(listener).not.toHaveBeenCalled()
    await fleet.dispose()
  })

  it('treats a failed explicit release as terminal so owned dispose does not wait', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      harness.throwOnRelease = true
      const fleet = new InternalFleetClient({
        client: harness,
        ownsBroker: true,
        ownedBrokerAgentExitTimeoutMs: 1_000,
      })
      await fleet.spawn({ name: 'ar-59-release-fails', capability: 'spawn:codex' })

      await expect(fleet.release('ar-59-release-fails', 'factory-stopped')).rejects.toThrow('release failed')
      const disposed = fleet.dispose()
      await Promise.resolve()

      expect(harness.shutdownCalls).toBe(1)
      await disposed
    } finally {
      vi.useRealTimers()
    }
  })

  it('retries a release rejected as retryable (broker http_500 / internal reply dropped) and succeeds', async () => {
    // Regression for factory#65: on cold-start dispatch completion the broker
    // occasionally drops the release reply and answers http_500 with
    // retryable: true. The transient failure must not reach the orchestrator's
    // final release-failed warning — a bounded retry recovers a graceful release.
    vi.useFakeTimers()
    try {
      class TransientRetryableReleaseHarnessDriverClient extends FakeHarnessDriverClient {
        attempts = 0
        override async release(name: string, reason?: string): Promise<{ name: string }> {
          this.attempts += 1
          this.released.push({ name, reason })
          if (this.attempts < 3) {
            throw Object.assign(new Error('internal reply dropped'), {
              name: 'HarnessDriverProtocolError',
              code: 'http_500',
              status: 500,
              retryable: true,
              data: { error: 'internal reply dropped', success: false },
            })
          }
          return { name }
        }
      }
      const harness = new TransientRetryableReleaseHarnessDriverClient()
      const logger = { warn: vi.fn(), debug: vi.fn(), info: vi.fn() }
      const fleet = new InternalFleetClient({ client: harness, logger })
      await fleet.spawn({ name: 'ar-19-review', capability: 'spawn:codex' })

      const released = fleet.release('ar-19-review', 'issue-done')
      // Two failed attempts, so two backoff sleeps (250ms, then 500ms).
      await vi.advanceTimersByTimeAsync(250 + 500)
      await released

      expect(harness.attempts).toBe(3)
      expect(harness.released).toHaveLength(3)
      expect(logger.warn).toHaveBeenCalledWith(
        '[factory-sdk] release rejected as retryable; retrying',
        expect.objectContaining({ name: 'ar-19-review', attempt: 1, maxAttempts: 3 }),
      )
      expect(logger.warn).toHaveBeenCalledWith(
        '[factory-sdk] release rejected as retryable; retrying',
        expect.objectContaining({ name: 'ar-19-review', attempt: 2, maxAttempts: 3 }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('gives up after bounded retries when the broker keeps dropping the release reply', async () => {
    vi.useFakeTimers()
    try {
      class AlwaysRetryableReleaseHarnessDriverClient extends FakeHarnessDriverClient {
        attempts = 0
        override async release(name: string, reason?: string): Promise<{ name: string }> {
          this.attempts += 1
          this.released.push({ name, reason })
          throw Object.assign(new Error('internal reply dropped'), {
            name: 'HarnessDriverProtocolError',
            code: 'http_500',
            status: 500,
            retryable: true,
            data: { error: 'internal reply dropped', success: false },
          })
        }
      }
      const harness = new AlwaysRetryableReleaseHarnessDriverClient()
      const fleet = new InternalFleetClient({
        client: harness,
        ownsBroker: true,
        ownedBrokerAgentExitTimeoutMs: 1_000,
      })
      await fleet.spawn({ name: 'ar-19-review', capability: 'spawn:codex' })

      const released = fleet.release('ar-19-review', 'issue-done')
      // Observe the rejection before advancing through the final attempt so
      // Vitest never sees an intermediate unhandled rejection.
      const releaseRejected = expect(released).rejects.toThrow('internal reply dropped')
      // Two backoffs (250ms + 500ms) between three attempts.
      await vi.advanceTimersByTimeAsync(250 + 500)
      await releaseRejected
      expect(harness.attempts).toBe(3)

      // The failed release is still terminal for lifecycle bookkeeping — a
      // subsequent dispose() of the owned broker must not wait on this agent.
      const disposed = fleet.dispose()
      await Promise.resolve()
      expect(harness.shutdownCalls).toBe(1)
      await disposed
    } finally {
      vi.useRealTimers()
    }
  })

  it('does not retry release when the broker rejects with a non-retryable error', async () => {
    class NonRetryableReleaseHarnessDriverClient extends FakeHarnessDriverClient {
      attempts = 0
      override async release(name: string, reason?: string): Promise<{ name: string }> {
        this.attempts += 1
        this.released.push({ name, reason })
        throw Object.assign(new Error(`agent '${name}' already exists`), {
          name: 'HarnessDriverProtocolError',
          code: 'http_500',
          status: 500,
          retryable: false,
          data: { error: `agent '${name}' already exists`, success: false },
        })
      }
    }
    const harness = new NonRetryableReleaseHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    await fleet.spawn({ name: 'ar-19-review', capability: 'spawn:codex' })

    await expect(fleet.release('ar-19-review', 'issue-done')).rejects.toThrow("agent 'ar-19-review' already exists")
    expect(harness.attempts).toBe(1)
  })

  it('times out waiting for a hung dispatched agent before shutting down an owned broker', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      const logger = { warn: vi.fn() }
      const fleet = new InternalFleetClient({
        client: harness,
        ownsBroker: true,
        ownedBrokerAgentExitTimeoutMs: 1_000,
        logger,
      })
      await fleet.spawn({ name: 'ar-59-hung', capability: 'spawn:codex' })

      const disposed = fleet.dispose()
      await Promise.resolve()
      expect(harness.shutdownCalls).toBe(0)

      await vi.advanceTimersByTimeAsync(1_000)
      await disposed

      expect(logger.warn).toHaveBeenCalledWith(
        '[factory-sdk] timed out waiting for spawned agents to exit; shutting down the owned relay broker',
        { timeoutMs: 1_000, agents: ['ar-59-hung'] },
      )
      expect(harness.shutdownCalls).toBe(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('defaults the owned broker agent-exit guard to 30 minutes', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness, ownsBroker: true })
      await fleet.spawn({ name: 'ar-59-default-timeout', capability: 'spawn:codex' })

      const disposed = fleet.dispose()
      await vi.advanceTimersByTimeAsync(30 * 60 * 1_000 - 1)
      expect(harness.shutdownCalls).toBe(0)

      await vi.advanceTimersByTimeAsync(1)
      await disposed
      expect(harness.shutdownCalls).toBe(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('only disconnects on dispose when reusing an existing broker (never shuts it down)', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

    await fleet.spawn({ name: 'ar-59-daemon-agent', capability: 'spawn:codex' })

    await fleet.dispose()

    expect(harness.disconnectCalls).toBe(1)
    expect(harness.shutdownCalls).toBe(0)
  })

  it('falls back to disconnect if shutting down an owned broker fails', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.throwOnShutdown = true
    const fleet = new InternalFleetClient({ client: harness, ownsBroker: true, cwd: '/worktree' })

    await expect(fleet.dispose()).resolves.toBeUndefined()
    expect(harness.shutdownCalls).toBe(1)
    expect(harness.disconnectCalls).toBe(1)
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

    await expect(fleet.resume({
      name: 'ar-1-impl',
      sessionRef: 'session-original',
      node: 'self',
      clonePath: '/isolated/ar-1',
      task: 'Continue with the durable human answer.',
    })).resolves.toEqual({
      name: 'ar-1-impl',
      sessionRef: 'resumed-session',
    })

    expect(harness.spawned[0]).toMatchObject({
      name: 'ar-1-impl',
      cli: 'codex',
      cwd: '/isolated/ar-1',
      continueFrom: 'session-original',
      task: 'Continue with the durable human answer.',
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

  it('tracks a resumed agent through exit before shutting down its owned broker', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness, ownsBroker: true })

    await fleet.resume({ name: 'ar-59-resumed', sessionRef: 'session-original' })
    harness.emit({
      kind: 'agent_exited',
      name: 'ar-59-resumed',
      code: 0,
      reason: 'task_exit',
      event_id: 'resume-exit-1',
    } as BrokerEvent)
    await fleet.dispose()

    expect(harness.shutdownCalls).toBe(1)
  })

  it('waits to shut down an owned broker while a resumed agent is still live', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({
      client: harness,
      ownsBroker: true,
      ownedBrokerAgentExitTimeoutMs: 1_000,
    })

    await fleet.resume({ name: 'ar-59-resumed', sessionRef: 'session-original' })
    const disposed = fleet.dispose()
    await Promise.resolve()
    expect(harness.shutdownCalls).toBe(0)

    harness.emit({
      kind: 'agent_exit',
      name: 'ar-59-resumed',
      reason: 'task_exit',
      event_id: 'resume-exit-2',
    } as BrokerEvent)
    await disposed

    expect(harness.shutdownCalls).toBe(1)
  })

  it('tracks a reused broker-returned name and ignores a replayed exit from its old lifetime', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({
      client: harness,
      ownsBroker: true,
      ownedBrokerAgentExitTimeoutMs: 1_000,
    })

    await fleet.spawn({ name: 'worker-1', capability: 'spawn:codex' })
    const oldExit = {
      kind: 'agent_exited',
      name: 'worker-1',
      code: 0,
      reason: 'task_exit',
      event_id: 'worker-1-old-exit',
    } as BrokerEvent
    harness.emit(oldExit)

    harness.nextHandleName = 'worker-1'
    await expect(fleet.spawn({ name: 'worker-request', capability: 'spawn:codex' })).resolves.toMatchObject({
      name: 'worker-1',
    })
    const disposed = fleet.dispose()
    await Promise.resolve()
    expect(harness.shutdownCalls).toBe(0)

    harness.emit(oldExit)
    await Promise.resolve()
    expect(harness.shutdownCalls).toBe(0)

    harness.emit({
      kind: 'agent_exited',
      name: 'worker-1',
      code: 0,
      reason: 'task_exit',
      event_id: 'worker-1-new-exit',
    } as BrokerEvent)
    await disposed

    expect(harness.shutdownCalls).toBe(1)
  })

  it('clears stale readiness and re-sends an injection when a resumed worker becomes ready', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness, cwd: '/worktree' })

      await fleet.resume({ name: 'ar-1-impl', sessionRef: 'session-original', node: 'self' })
      expect(harness.connectEventsCalls).toBe(1)
      harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })

      await fleet.resume({ name: 'ar-1-impl', sessionRef: 'session-resume-again', node: 'self' })
      const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'continue work' }, { timeoutMs: 5000 })
      await Promise.resolve()
      await Promise.resolve()
      expect(harness.sent).toHaveLength(1)

      // The readiness signal from the previous process must not schedule the
      // fallback; only the resumed process becoming ready should re-send.
      await vi.advanceTimersByTimeAsync(1000)
      expect(harness.sent).toHaveLength(1)

      harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })
      expect(harness.sent).toHaveLength(2)
      await Promise.resolve()
      harness.emit({
        kind: 'delivery_injected',
        name: 'ar-1-impl',
        delivery_id: 'delivery-2',
        event_id: 'event-2',
      })

      await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
    } finally {
      vi.useRealTimers()
    }
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
      nodes: [{ name: 'self', capabilities: ['spawn:claude', 'spawn:codex', 'workflow:run'], live: true }],
    })
  })

  it('re-adopts tracked workers and synthesizes exits for workers missing after restart', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [{ name: 'ar-1-impl' }]
    const fleet = new InternalFleetClient({ client: harness })
    const exits: Array<{ name: string; reason?: string }> = []
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    fleet.hydrateTracked([
      { name: 'ar-1-impl', invocationId: 'inv-impl' },
      { name: 'ar-1-review', invocationId: 'inv-review' },
    ])
    await fleet.reconcileTrackedAgents()

    expect(fleet.trackedAgents()).toEqual(new Map([
      ['ar-1-impl', { invocationId: 'inv-impl', node: undefined }],
    ]))
    expect(exits).toEqual([{ name: 'ar-1-review', reason: 'reconciled-missing' }])

    harness.agents = []
    await fleet.reconcileTrackedAgents()
    expect(fleet.trackedAgents().size).toBe(0)
    expect(exits).toEqual([
      { name: 'ar-1-review', reason: 'reconciled-missing' },
      { name: 'ar-1-impl', reason: 'reconciled-missing' },
    ])
  })

  it('filters stale broker rows through canonical presence while granting fresh local registration grace', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [
      { name: 'ar-stale-impl', cli: 'codex' },
      { name: 'ar-live-impl', cli: 'codex' },
    ]
    let nowMs = 1_000_000
    const fleet = new InternalFleetClient({
      client: harness,
      listCanonicalOnlineAgentNames: async () => ['ar-live-impl'],
      now: () => nowMs,
    })
    const exits: string[] = []
    fleet.onAgentExit((name) => exits.push(name))
    fleet.hydrateTracked([
      { name: 'ar-stale-impl', invocationId: 'inv-stale' },
      { name: 'ar-live-impl', invocationId: 'inv-live' },
    ])

    await expect(fleet.roster()).resolves.toEqual({
      agents: [{ name: 'ar-live-impl' }],
      nodes: [{ name: 'self', capabilities: ['spawn:claude', 'spawn:codex', 'workflow:run'], live: true }],
    })
    await fleet.reconcileTrackedAgents()
    expect(exits).toEqual(['ar-stale-impl'])

    await fleet.spawn({ name: 'ar-fresh-impl', capability: 'spawn:codex' })
    await expect(fleet.roster()).resolves.toMatchObject({
      agents: [{ name: 'ar-live-impl' }, { name: 'ar-fresh-impl' }],
    })

    nowMs += 60_000
    await expect(fleet.roster()).resolves.toMatchObject({
      agents: [{ name: 'ar-live-impl' }],
    })
  })

  it('preserves non-MCP and live no-MCP fallback workers when canonical presence is absent', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [
      { name: 'ar-workflow', cli: 'relayflows' },
      { name: 'ar-fallback-live', cli: 'codex', pid: 101 },
      { name: 'ar-stale-dead', cli: 'claude', pid: 102 },
    ]
    const fleet = new InternalFleetClient({
      client: harness,
      listCanonicalOnlineAgentNames: async () => [],
      isProcessAlive: (pid) => pid === 101,
    })

    await expect(fleet.roster()).resolves.toMatchObject({
      agents: [
        { name: 'ar-workflow' },
        { name: 'ar-fallback-live' },
      ],
    })
  })

  it('fails closed when canonical presence is unavailable instead of reviving stale broker rows', async () => {
    const harness = new FakeHarnessDriverClient()
    harness.agents = [{ name: 'ar-stale-impl', cli: 'codex' }]
    const fleet = new InternalFleetClient({
      client: harness,
      listCanonicalOnlineAgentNames: async () => {
        throw new Error('presence unavailable')
      },
    })
    fleet.hydrateTracked([{ name: 'ar-stale-impl', invocationId: 'inv-stale' }])

    await expect(fleet.roster()).rejects.toThrow('presence unavailable')
    await expect(fleet.reconcileTrackedAgents()).rejects.toThrow('presence unavailable')
    expect(fleet.trackedAgents().has('ar-stale-impl')).toBe(true)
  })

  it('clears readiness even when releasing the agent fails', async () => {
    vi.useFakeTimers()
    try {
      class FailingReleaseHarnessDriverClient extends FakeHarnessDriverClient {
        override async release(name: string, reason?: string): Promise<{ name: string }> {
          this.released.push({ name, reason })
          throw new Error('release failed')
        }
      }
      const harness = new FailingReleaseHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness })
      fleet.onAgentExit(() => {})
      harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })

      await expect(fleet.release('ar-1-impl', 'failed cleanup')).rejects.toThrow('release failed')

      const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'new process' }, { timeoutMs: 5000 })
      await Promise.resolve()
      await Promise.resolve()
      expect(harness.sent).toHaveLength(1)
      await vi.advanceTimersByTimeAsync(1000)
      expect(harness.sent).toHaveLength(1)

      harness.emit({
        kind: 'delivery_injected',
        name: 'ar-1-impl',
        delivery_id: 'delivery-1',
        event_id: 'event-1',
      })
      await expect(injected).resolves.toEqual({ eventId: 'event-1', targets: ['ar-1-impl'] })
    } finally {
      vi.useRealTimers()
    }
  })

  it('sends messages and exposes the broker event id for injected confirmation', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await fleet.sendMessage({ to: 'ar-1-review', from: 'ar-1-impl', text: 'PR ready', data: { pr: 1 }, mode: 'wait' })
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
      { to: 'ar-1-review', from: 'ar-1-impl', text: 'PR ready', data: { pr: 1 }, mode: 'wait' },
      { to: 'broker', text: 'done', from: undefined, data: undefined },
    ])
  })

  it('confirms injection by target name when the broker delivery id differs from the send id and worker_ready never fires', async () => {
    // Regression for the cold-start dispatch race: on a real (non-fake) broker,
    // sendMessage returns an inbound-request id (`http_…`) while the broker's
    // delivery_injected event carries its own delivery-tracking id and NEVER the
    // send id. worker_ready is also not observed on a freshly spawned broker.
    // The waiter must still resolve, correlating by the target agent name.
    class MismatchedIdHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        // The id the broker hands back on the send has no relationship to the
        // id it will later report on delivery_injected.
        return { event_id: `http_${this.sent.length}`, targets: [input.to] }
      }
    }
    const harness = new MismatchedIdHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:codex' })
    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 5000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))

    // No worker_ready is ever emitted. The broker reports the injection under a
    // delivery id that does not match any send id, only the target name.
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'broker-delivery-99',
      event_id: 'broker-snowflake-202823817304596480',
    })

    await expect(injected).resolves.toEqual({
      eventId: 'broker-snowflake-202823817304596480',
      targets: ['ar-1-impl'],
    })
  })

  it('confirms a mismatched-id injection that races the sendMessage response', async () => {
    class InjectionDuringSendHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        this.emit({
          kind: 'delivery_injected',
          name: input.to,
          delivery_id: 'broker-delivery-1',
          event_id: 'broker-snowflake-1',
        })
        return { event_id: 'http_1', targets: [input.to] }
      }
    }
    const harness = new InjectionDuringSendHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    await expect(fleet.waitForInjected(
      { to: 'ar-1-impl', text: 'do work' },
      { timeoutMs: 100 },
    )).resolves.toEqual({
      eventId: 'broker-snowflake-1',
      targets: ['ar-1-impl'],
    })
    expect(harness.sent).toHaveLength(1)
  })

  it('re-sends a pending injection when its target registers through a fresh spawn', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))

    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:codex' })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
  })

  it('re-sends an unconfirmed injection when the matching worker becomes ready', async () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))

    harness.emit({ kind: 'worker_ready', name: 'ar-2-impl', runtime: 'pty' })
    await Promise.resolve()
    expect(harness.sent).toHaveLength(1)

    harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))

    // A failure for the superseded first send must not reject the logical
    // waiter while the readiness-triggered re-send can still be acknowledged.
    harness.emit({
      kind: 'delivery_failed',
      name: 'ar-1-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
      reason: 'recipient unavailable',
    })
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
    expect(harness.sent).toEqual([
      { to: 'ar-1-impl', text: 'do work', from: undefined, data: undefined },
      { to: 'ar-1-impl', text: 'do work', from: undefined, data: undefined },
    ])
  })

  it('re-sends when worker_ready races the first sendMessage response', async () => {
    class ReadyDuringSendHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        const eventId = `event-${this.sent.length}`
        if (this.sent.length === 1) {
          this.emit({ kind: 'worker_ready', name: input.to, runtime: 'pty' })
        }
        return { event_id: eventId, targets: [input.to] }
      }
    }
    const harness = new ReadyDuringSendHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
  })

  it('re-sends when delivery failure races initial waiter installation for a ready worker', async () => {
    class FailureDuringSendHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        const eventId = `event-${this.sent.length}`
        if (this.sent.length === 1) {
          this.emit({
            kind: 'delivery_failed',
            name: input.to,
            delivery_id: 'delivery-1',
            event_id: eventId,
            reason: 'recipient unavailable',
          })
        }
        return { event_id: eventId, targets: [input.to] }
      }
    }
    const harness = new FailureDuringSendHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    fleet.onAgentExit(() => {})
    harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
  })

  it('keeps waiting when the first event fails while the readiness re-send is in flight', async () => {
    class BlockingResendHarnessDriverClient extends FakeHarnessDriverClient {
      resolveResend?: (result: { event_id: string; targets: string[] }) => void

      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        if (this.sent.length === 1) {
          return { event_id: 'event-1', targets: [input.to] }
        }
        return await new Promise((resolve) => {
          this.resolveResend = resolve
        })
      }
    }
    const harness = new BlockingResendHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))
    harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))

    harness.emit({
      kind: 'delivery_failed',
      name: 'ar-1-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
      reason: 'recipient unavailable',
    })
    harness.resolveResend?.({ event_id: 'event-2', targets: ['ar-1-impl'] })
    await Promise.resolve()
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
  })

  it('disposes a logical waiter while its readiness re-send response is blocked', async () => {
    class BlockingResendHarnessDriverClient extends FakeHarnessDriverClient {
      resolveResend?: (result: { event_id: string; targets: string[] }) => void

      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        if (this.sent.length === 1) {
          return { event_id: 'event-1', targets: [input.to] }
        }
        return await new Promise((resolve) => {
          this.resolveResend = resolve
        })
      }
    }
    const harness = new BlockingResendHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))
    harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))
    harness.emit({
      kind: 'delivery_failed',
      name: 'ar-1-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
      reason: 'recipient unavailable',
    })

    const rejected = expect(injected).rejects.toThrow('InternalFleetClient disposed')
    await fleet.dispose()
    await rejected

    // A response arriving after disconnect must not resurrect the waiter.
    harness.resolveResend?.({ event_id: 'event-2', targets: ['ar-1-impl'] })
    await Promise.resolve()
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-2',
      event_id: 'event-2',
    })
    await fleet.dispose()

    expect(harness.disconnectCalls).toBe(1)
    expect(harness.eventListeners.size).toBe(0)
    expect(harness.deliveryListeners.size).toBe(0)
  })

  it('warns and keeps the first event viable when the readiness re-send transport fails', async () => {
    const resendError = new Error('resend transport failed')
    class FailingResendHarnessDriverClient extends FakeHarnessDriverClient {
      override async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
        this.sent.push(input)
        if (this.sent.length === 2) {
          throw resendError
        }
        return { event_id: 'event-1', targets: [input.to] }
      }
    }
    const harness = new FailingResendHarnessDriverClient()
    const logger = { warn: vi.fn() }
    const fleet = new InternalFleetClient({ client: harness, logger })

    const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 1000 })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(1))
    harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })
    await vi.waitFor(() => expect(harness.sent).toHaveLength(2))
    await vi.waitFor(() => expect(logger.warn).toHaveBeenCalledWith(
      '[factory-sdk] readiness-triggered resend failed, but original event is still active',
      expect.objectContaining({
        name: 'Error',
        message: 'resend transport failed',
        stack: expect.stringContaining('Error: resend transport failed'),
      }),
    ))
    harness.emit({
      kind: 'delivery_injected',
      name: 'ar-1-impl',
      delivery_id: 'delivery-1',
      event_id: 'event-1',
    })

    await expect(injected).resolves.toEqual({ eventId: 'event-1', targets: ['ar-1-impl'] })
  })

  it('keeps the ready-before-injection fast path to one send when the first send is acknowledged', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness })
      fleet.onAgentExit(() => {})
      harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })

      const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 5000 })
      await Promise.resolve()
      harness.emit({
        kind: 'delivery_injected',
        name: 'ar-1-impl',
        delivery_id: 'delivery-1',
        event_id: 'event-1',
      })

      await expect(injected).resolves.toEqual({ eventId: 'event-1', targets: ['ar-1-impl'] })
      await vi.advanceTimersByTimeAsync(1000)
      expect(harness.sent).toHaveLength(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('re-sends once when readiness precedes an unacknowledged first send', async () => {
    vi.useFakeTimers()
    try {
      const harness = new FakeHarnessDriverClient()
      const fleet = new InternalFleetClient({ client: harness })
      fleet.onAgentExit(() => {})
      harness.emit({ kind: 'worker_ready', name: 'ar-1-impl', runtime: 'pty' })

      const injected = fleet.waitForInjected({ to: 'ar-1-impl', text: 'do work' }, { timeoutMs: 5000 })
      await Promise.resolve()
      expect(harness.sent).toHaveLength(1)

      await vi.advanceTimersByTimeAsync(1000)
      expect(harness.sent).toHaveLength(2)
      harness.emit({
        kind: 'delivery_injected',
        name: 'ar-1-impl',
        delivery_id: 'delivery-2',
        event_id: 'event-2',
      })

      await expect(injected).resolves.toEqual({ eventId: 'event-2', targets: ['ar-1-impl'] })
    } finally {
      vi.useRealTimers()
    }
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

  it('surfaces inbound relay messages once for duplicate events', () => {
    const harness = new FakeHarnessDriverClient()
    const fleet = new InternalFleetClient({ client: harness })
    const messages: Array<{ from: string; target: string; body: string; threadId?: string; eventId?: string }> = []

    fleet.onAgentMessage((message) => messages.push(message))

    const inbound = {
      kind: 'relay_inbound',
      event_id: 'inbound-1',
      from: 'ar-1-impl',
      target: 'broker',
      body: 'FACTORY_NEEDS_INPUT\nQuestion: blocked',
      thread_id: 'thread-1',
    } satisfies BrokerEvent

    harness.emit(inbound)
    harness.emit(inbound)

    expect(messages).toEqual([{
      from: 'ar-1-impl',
      target: 'broker',
      body: 'FACTORY_NEEDS_INPUT\nQuestion: blocked',
      threadId: 'thread-1',
      eventId: 'inbound-1',
    }])
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
