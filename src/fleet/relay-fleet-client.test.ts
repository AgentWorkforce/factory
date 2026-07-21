import { describe, expect, it, vi } from 'vitest'

import { RelayFleetClient, type RelayClientFactoryOptions, type RelayClientLike } from './relay-fleet-client'

import type {
  RelayActionInvocation,
  RelayActionInvocationAck,
  RelayMessage,
  RelayMessaging,
  RelayNode,
  RelaySpawnPlacementInput,
} from '@agent-relay/sdk'

type EventHandler = (...args: unknown[]) => void

function relayMessage(overrides: Partial<RelayMessage> & { text: string }): RelayMessage {
  return {
    id: overrides.id ?? 'msg-1',
    messageId: overrides.id ?? 'msg-1',
    from: overrides.from ?? { name: 'ar-1-impl' },
    ...overrides,
  } as RelayMessage
}

class FakeMessaging {
  readonly placements: RelaySpawnPlacementInput[] = []
  readonly invokes: Array<{ name: string; input?: Record<string, unknown> }> = []
  readonly directs: Array<{ to: string; text: string; mode?: 'wait' | 'steer' }> = []
  readonly channelSends: Array<{ channel: string; text: string; mode?: 'wait' | 'steer' }> = []
  readonly commandRegistrations: Array<{ command: string; handlerAgent: string }> = []
  readonly completedInvocations: Array<{ name: string; invocationId: string; data: Record<string, unknown> }> = []
  readonly handlers = new Map<string, Set<EventHandler>>()
  invocations = new Map<string, RelayActionInvocation[]>()
  placementAck: Partial<RelayActionInvocationAck> & { placement?: { node?: string } } = {}
  agentRows: Array<{ name: string; status?: string; node?: string }> = []
  agentPresenceRows: Array<{ agentId: string; agentName: string; status: 'online' | 'offline' }> | undefined
  nodeRows: Array<Partial<RelayNode> & { name: string }> = []
  directError: Error | undefined
  connected = 0
  disconnected = 0
  nextInvocationId = 0
  readonly agentListFilters: unknown[] = []
  agentPresenceCalls = 0
  meName = 'relay-controller'

  readonly agents = {
    list: async (filter: unknown) => {
      this.agentListFilters.push(filter)
      return this.agentRows as never[]
    },
    presence: async () => {
      this.agentPresenceCalls += 1
      return this.agentPresenceRows ?? this.agentRows.map((agent, index) => ({
        agentId: `agent-${index}`,
        agentName: agent.name,
        status: agent.status === 'online' ? 'online' as const : 'offline' as const,
      }))
    },
    me: async () => ({ name: this.meName }),
  }

  readonly nodes = {
    list: async () => this.nodeRows as RelayNode[],
  }

  readonly messages = {
    send: async (input: { channel: string; text: string; mode?: 'wait' | 'steer' }) => {
      this.channelSends.push(input)
      return relayMessage({ id: `sent-${this.channelSends.length}`, text: input.text, from: { name: 'factory' } })
    },
    direct: async (input: { to: string; text: string; mode?: 'wait' | 'steer' }) => {
      if (this.directError) throw this.directError
      this.directs.push(input)
      return relayMessage({ id: `dm-${this.directs.length}`, text: input.text, from: { name: 'factory' } })
    },
  }

  readonly commands = {
    available: () => true,
    register: async (input: { command: string; handlerAgent: string }) => {
      this.commandRegistrations.push(input)
      return input
    },
    invoke: async (name: string, input?: Record<string, unknown>): Promise<RelayActionInvocationAck> => {
      this.invokes.push({ name, input })
      const invocationId = `inv-${++this.nextInvocationId}`
      return { invocationId, actionName: name, status: this.invocations.has(invocationId) ? 'pending' : 'completed' }
    },
    getInvocation: async (name: string, invocationId: string): Promise<RelayActionInvocation> => {
      const queue = this.invocations.get(invocationId)
      const next = queue?.shift()
      return next ?? { invocationId, actionName: name, status: 'completed', output: {} }
    },
    completeInvocation: async (name: string, invocationId: string, data: Record<string, unknown>) => {
      this.completedInvocations.push({ name, invocationId, data })
      return { invocationId, actionName: name, status: data.error ? 'failed' : 'completed' }
    },
  }

  readonly placement = {
    spawn: async (input: RelaySpawnPlacementInput) => {
      this.placements.push(input)
      const invocationId = this.placementAck.invocationId ?? `inv-${++this.nextInvocationId}`
      return {
        invocationId,
        actionName: 'spawn',
        status: this.placementAck.status ?? (this.invocations.has(invocationId) ? 'pending' : 'completed'),
        dispatchedNodeId: this.placementAck.dispatchedNodeId,
        node: { name: this.placementAck.placement?.node ?? 'node-a' } as RelayNode,
        placement: {
          capability: input.capability,
          node: this.placementAck.placement?.node ?? 'node-a',
          attempts: 1,
          queued: false,
        },
      }
    },
  }

  readonly events = {
    connect: () => {
      this.connected += 1
    },
    disconnect: async () => {
      this.disconnected += 1
    },
    subscribe: () => {},
    unsubscribe: () => {},
    on: (event: string, handler: EventHandler) => {
      const handlers = this.handlers.get(event) ?? new Set()
      handlers.add(handler)
      this.handlers.set(event, handlers)
      return () => handlers.delete(handler)
    },
  }

  emit(event: string, payload: unknown): void {
    for (const handler of this.handlers.get(event) ?? []) {
      handler(payload)
    }
  }

  asMessaging(): RelayMessaging {
    return this as unknown as RelayMessaging
  }
}

const immediateSleep = async () => {}

function createClient(messaging: FakeMessaging, overrides: Record<string, unknown> = {}) {
  return new RelayFleetClient({
    messaging: messaging.asMessaging(),
    sleep: immediateSleep,
    pollIntervalMs: 0,
    ...overrides,
  })
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0))
}

describe('RelayFleetClient', () => {
  it('dispatches spawn through placement with task-exit lifecycle and no self target', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { invocationId: 'inv-1', status: 'pending', placement: { node: 'mac-mini' } }
    messaging.invocations.set('inv-1', [
      { invocationId: 'inv-1', actionName: 'spawn', status: 'dispatched' },
      {
        invocationId: 'inv-1',
        actionName: 'spawn',
        status: 'completed',
        output: { agent_name: 'ar-1-impl', session_ref: 'session-1', pid: 123 },
      },
    ])
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:claude',
      node: 'self',
      repo: 'AgentWorkforce/factory',
      clonePath: '/checkout',
      task: 'do work',
      model: 'opus',
      cwd: '/checkout',
      invocationId: 'factory-inv-1',
      channel: 'wf-factory',
    })).resolves.toEqual({ name: 'ar-1-impl', sessionRef: 'session-1', pid: 123, node: 'mac-mini', locality: 'remote' })

    expect(messaging.placements).toHaveLength(1)
    const placement = messaging.placements[0]!
    expect(placement.capability).toBe('spawn:claude')
    expect(placement.repo).toBe('AgentWorkforce/factory')
    expect(placement).not.toHaveProperty('node')
    expect(placement.input).toEqual({
      name: 'ar-1-impl',
      agent: 'ar-1-impl',
      clone_path: '/checkout',
      clonePath: '/checkout',
      invocationId: 'factory-inv-1',
      task: 'do work',
      model: 'opus',
      cwd: '/checkout',
      channels: ['wf-factory'],
      spawn_mode: 'task_exit',
      exit_after_task: true,
    })
    expect(fleet.trackedAgents().get('ar-1-impl')).toMatchObject({ invocationId: 'inv-1', node: 'mac-mini' })
  })

  it('passes explicit node targets through to placement', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    await fleet.spawn({
      name: 'ar-2-impl',
      capability: 'spawn:codex',
      node: 'mac-mini',
      repo: 'AgentWorkforce/factory',
    })

    expect(messaging.placements[0]?.node).toBe('mac-mini')
    expect(messaging.placements[0]?.repo).toBe('AgentWorkforce/factory')
  })

  it('maps resume onto a placement spawn with session_ref', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    await fleet.resume({
      name: 'ar-3-impl',
      sessionRef: 'session-3',
      node: 'origin-node',
      capability: 'spawn:claude',
      task: 'Continue with the durable human answer.',
    })

    expect(messaging.placements[0]).toMatchObject({ capability: 'spawn:claude', node: 'origin-node' })
    expect(messaging.placements[0]?.input).toMatchObject({
      name: 'ar-3-impl',
      agent: 'ar-3-impl',
      session_ref: 'session-3',
      task: 'Continue with the durable human answer.',
      spawn_mode: 'task_exit',
      exit_after_task: true,
    })
  })

  it('does not request task-exit lifecycle for workflow capabilities', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    await fleet.spawn({
      name: 'ar-5-workflow',
      capability: 'workflow:run',
      workflow: 'workflows/factory/linear-issue.ts',
      inputs: { issue: 'AR-5' },
    })

    expect(messaging.placements[0]?.input).toMatchObject({
      workflow: 'workflows/factory/linear-issue.ts',
      inputs: { issue: 'AR-5' },
    })
    expect(messaging.placements[0]?.input).not.toHaveProperty('spawn_mode')
    expect(messaging.placements[0]?.input).not.toHaveProperty('exit_after_task')
  })

  it('releases through commands.invoke and stops tracking the agent', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    await fleet.spawn({ name: 'ar-4-impl', capability: 'spawn:codex' })
    expect(fleet.trackedAgents().has('ar-4-impl')).toBe(true)

    await fleet.release('ar-4-impl', 'issue-done')

    expect(messaging.invokes).toEqual([
      { name: 'release', input: { name: 'ar-4-impl', agent: 'ar-4-impl', reason: 'issue-done' } },
    ])
    expect(fleet.trackedAgents().has('ar-4-impl')).toBe(false)
  })

  it('fails cleanly when the fleet denies or fails an invocation', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { invocationId: 'inv-denied', status: 'pending' }
    messaging.invocations.set('inv-denied', [
      { invocationId: 'inv-denied', actionName: 'spawn', status: 'denied', error: 'node cannot spawn:codex' },
    ])
    const fleet = createClient(messaging)

    await expect(fleet.spawn({ name: 'ar-7-impl', capability: 'spawn:codex' }))
      .rejects.toThrow(/spawn invocation inv-denied denied: node cannot spawn:codex/)
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('returns the relay agent and node roster', async () => {
    const messaging = new FakeMessaging()
    // An agent-scoped list can leak status-less rows normalized as `unknown`.
    // Canonical presence must be the sole liveness authority.
    messaging.agentRows = [
      { name: 'ar-1-impl', status: 'unknown', node: 'alpha' },
      { name: 'ar-stale-impl', status: 'offline' },
      { name: 'ar-registering-review', status: 'unknown', node: 'beta' },
    ]
    messaging.agentPresenceRows = [
      { agentId: 'agent-1', agentName: 'ar-1-impl', status: 'online' },
      { agentId: 'agent-2', agentName: 'ar-stale-impl', status: 'offline' },
      { agentId: 'agent-3', agentName: 'ar-registering-review', status: 'offline' },
    ]
    messaging.nodeRows = [
      {
        name: 'alpha',
        status: 'online',
        capabilities: [{ name: 'spawn:claude' }, { name: 'workflow:run' }, { name: 'unknown:cap' }],
      },
      { name: 'beta', status: 'offline', live: false, capabilities: [{ name: 'spawn:codex' }] },
    ]
    const fleet = createClient(messaging)

    await expect(fleet.roster()).resolves.toEqual({
      agents: [{ name: 'ar-1-impl', node: 'alpha' }],
      nodes: [
        { name: 'alpha', capabilities: ['spawn:claude', 'workflow:run'], live: true },
        { name: 'beta', capabilities: ['spawn:codex'], live: false },
      ],
    })
    expect(messaging.agentListFilters).toEqual([{ status: 'all' }])
    expect(messaging.agentPresenceCalls).toBe(1)
  })

  it('sends DMs and channel messages through the agent-scoped surface', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    await fleet.sendMessage({ to: 'ar-1-impl', text: 'hello', mode: 'wait' })
    await fleet.sendMessage({ to: '#wf-factory', text: 'update', mode: 'steer' })

    expect(messaging.directs).toEqual([{ to: 'ar-1-impl', text: 'hello', mode: 'wait' }])
    expect(messaging.channelSends).toEqual([{ channel: 'wf-factory', text: 'update', mode: 'steer' }])
  })

  it('confirms injected tasks with the sent message id', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    await expect(fleet.waitForInjected({ to: 'ar-1-impl', text: 'task' })).resolves.toEqual({
      eventId: 'dm-1',
      targets: ['ar-1-impl'],
    })
  })

  it('maps unknown-recipient send errors to the retryable registration-lag shape', async () => {
    const messaging = new FakeMessaging()
    messaging.directError = new Error('Agent not found: ar-1-impl')
    const fleet = createClient(messaging)

    await expect(fleet.waitForInjected({ to: 'ar-1-impl', text: 'task' }))
      .rejects.toThrow(/recipient unavailable: Agent not found: ar-1-impl/)
  })

  it('maps messaging events onto FleetClient callbacks', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    const messages: Array<{ from: string; target: string; body: string; eventId?: string }> = []
    const exits: Array<{ name: string; reason?: string }> = []
    fleet.onAgentMessage((message) => messages.push(message))
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))
    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:claude' })
    await flush()

    messaging.emit('any', {
      type: 'dmReceived',
      message: relayMessage({
        id: 'msg-2',
        text: 'FACTORY_NEEDS_INPUT ar-1-impl',
        from: { name: 'ar-1-impl' },
        target: { kind: 'agent', agentName: 'factory' },
      }),
    })
    messaging.emit('any', {
      type: 'messageCreated',
      channel: 'wf-factory',
      message: relayMessage({ id: 'msg-3', text: 'progress', from: { name: 'ar-1-impl' } }),
    })
    // Self-authored messages must not loop back into the orchestrator.
    messaging.emit('any', {
      type: 'messageCreated',
      channel: 'wf-factory',
      message: relayMessage({ id: 'msg-4', text: 'from us', from: { name: 'factory' } }),
    })
    messaging.emit('any', { type: 'agentOffline', agent: { name: 'ar-1-impl' } })
    messaging.emit('any', { type: 'agentOffline', agent: { name: 'untracked-agent' } })

    expect(messages).toEqual([
      { from: 'ar-1-impl', target: 'factory', body: 'FACTORY_NEEDS_INPUT ar-1-impl', eventId: 'msg-2' },
      { from: 'ar-1-impl', target: 'wf-factory', body: 'progress', eventId: 'msg-3' },
    ])
    expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'offline' }])
    expect(messaging.connected).toBe(1)
    expect(fleet.trackedAgents().has('ar-1-impl')).toBe(false)
  })

  it('routes durable lifecycle actions through the authenticated identity when factory and broker are absent', async () => {
    const messaging = new FakeMessaging()
    messaging.meName = 'relay-controller-7'
    messaging.agentRows = [{ name: 'ar-17-impl', status: 'online' }]
    messaging.invocations.set('lifecycle-17', [{
      invocationId: 'lifecycle-17',
      actionName: 'factory.lifecycle',
      callerName: 'ar-17-impl',
      status: 'invoked',
      input: { kind: 'completed', issueKey: 'AR-17', role: 'implementer' },
    }])
    const fleet = createClient(messaging)
    const signals: unknown[] = []
    fleet.onAgentLifecycleSignal?.((signal) => { signals.push(signal) })
    await fleet.spawn({ name: 'ar-17-impl', capability: 'spawn:codex' })
    await flush()

    messaging.emit('any', {
      type: 'actionInvoked',
      invocationId: 'lifecycle-17',
      actionName: 'factory.lifecycle',
      callerName: 'ar-17-impl',
      handlerAgentId: 'controller-id',
    })

    await vi.waitFor(() => expect(messaging.completedInvocations).toHaveLength(1))
    expect(messaging.commandRegistrations).toEqual([
      expect.objectContaining({ command: 'factory.lifecycle', handlerAgent: 'relay-controller-7' }),
    ])
    expect(messaging.agentRows.map((agent) => agent.name)).not.toEqual(expect.arrayContaining(['factory', 'broker']))
    expect(signals).toEqual([{
      name: 'ar-17-impl',
      kind: 'completed',
      issueKey: 'AR-17',
      role: 'implementer',
      invocationId: 'lifecycle-17',
    }])
    expect(messaging.completedInvocations).toEqual([{
      name: 'factory.lifecycle',
      invocationId: 'lifecycle-17',
      data: { output: { accepted: true } },
    }])
    expect(messaging.directs).toEqual([])
    expect(messaging.channelSends).toEqual([])
  })

  it('hydrates tracked agents for restart recovery', () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)

    fleet.hydrateTracked([{ name: 'ar-9-impl', invocationId: 'inv-9', node: 'mac-mini' }])

    expect(fleet.trackedAgents().get('ar-9-impl')).toMatchObject({ invocationId: 'inv-9', node: 'mac-mini' })
  })

  it('forgets a terminal tracked agent before an intentional replacement', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    const exits: string[] = []
    fleet.onAgentExit((name) => exits.push(name))
    fleet.hydrateTracked([{ name: 'ar-9-babysit', invocationId: 'inv-9' }])

    fleet.markAgentTerminal('ar-9-babysit', 'babysitter-unreachable')
    await fleet.reconcileTrackedAgents()

    expect(fleet.trackedAgents().has('ar-9-babysit')).toBe(false)
    expect(exits).toEqual([])
  })

  it('synthesizes exits for tracked agents that left the roster after the registration grace', async () => {
    const messaging = new FakeMessaging()
    messaging.agentRows = [{ name: 'ar-2-review', status: 'online' }]
    messaging.nodeRows = [{ name: 'mac-mini', status: 'online', capabilities: [] }]
    let nowMs = 1_000_000
    const fleet = createClient(messaging, { now: () => nowMs })
    const exits: Array<{ name: string; reason?: string }> = []
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))
    await fleet.spawn({ name: 'ar-1-impl', capability: 'spawn:claude' })
    fleet.hydrateTracked([{ name: 'ar-2-review', node: 'mac-mini' }])

    // Inside the registration grace a missing spawned agent is not an exit.
    await fleet.reconcileTrackedAgents()
    expect(exits).toEqual([])

    nowMs += 120_000
    await fleet.reconcileTrackedAgents()

    expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'exited' }])
    expect(fleet.trackedAgents().has('ar-1-impl')).toBe(false)
    expect(fleet.trackedAgents().has('ar-2-review')).toBe(true)
  })

  it('synthesizes exits for offline roster rows and dead nodes after their grace windows', async () => {
    const messaging = new FakeMessaging()
    messaging.agentRows = [
      { name: 'ar-1-impl', status: 'offline' },
      { name: 'ar-2-review', status: 'online' },
    ]
    messaging.nodeRows = [{ name: 'mac-mini', status: 'offline', live: false, capabilities: [] }]
    let nowMs = 1_000_000
    const fleet = createClient(messaging, { now: () => nowMs })
    const exits: Array<{ name: string; reason?: string }> = []
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))
    fleet.hydrateTracked([
      { name: 'ar-1-impl', node: 'mac-mini' },
      { name: 'ar-2-review', node: 'mac-mini' },
    ])

    // Hydrated agents carry no registration grace: offline is an exit now. The
    // dead node only starts its grace clock on this sweep.
    await fleet.reconcileTrackedAgents()
    expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'exited' }])

    nowMs += 90_000
    await fleet.reconcileTrackedAgents()

    expect(exits).toEqual([
      { name: 'ar-1-impl', reason: 'exited' },
      { name: 'ar-2-review', reason: 'node-offline' },
    ])
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('clears the node-offline clock when the node comes back', async () => {
    const messaging = new FakeMessaging()
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    messaging.nodeRows = [{ name: 'mac-mini', status: 'offline', live: false, capabilities: [] }]
    let nowMs = 1_000_000
    const fleet = createClient(messaging, { now: () => nowMs })
    const exits: string[] = []
    fleet.onAgentExit((name) => exits.push(name))
    fleet.hydrateTracked([{ name: 'ar-1-impl', node: 'mac-mini' }])

    await fleet.reconcileTrackedAgents()
    messaging.nodeRows = [{ name: 'mac-mini', status: 'online', live: true, capabilities: [] }]
    nowMs += 60_000
    await fleet.reconcileTrackedAgents()
    messaging.nodeRows = [{ name: 'mac-mini', status: 'offline', live: false, capabilities: [] }]
    nowMs += 60_000
    await fleet.reconcileTrackedAgents()

    // The first offline stretch never reached the grace; the clock restarted.
    expect(exits).toEqual([])
    expect(fleet.trackedAgents().has('ar-1-impl')).toBe(true)
  })

  it('runs the exit watcher on an interval while agents are tracked', async () => {
    vi.useFakeTimers()
    try {
      const messaging = new FakeMessaging()
      messaging.agentRows = []
      messaging.nodeRows = []
      const fleet = createClient(messaging, { exitWatchIntervalMs: 1_000, now: () => Date.now() })
      const exits: string[] = []
      fleet.onAgentExit((name) => exits.push(name))
      fleet.hydrateTracked([{ name: 'ar-1-impl' }])

      await vi.advanceTimersByTimeAsync(1_100)

      expect(exits).toEqual(['ar-1-impl'])
    } finally {
      vi.useRealTimers()
    }
  })

  it('registers the factory agent identity from a workspace key on first use', async () => {
    const messaging = new FakeMessaging()
    const created: RelayClientFactoryOptions[] = []
    const registered: Array<{ name: string }> = []
    const bootstrap: RelayClientLike = {
      messaging: {
        agents: {
          registerOrRotate: async (input: { name: string }) => {
            registered.push(input)
            return { id: 'agent-1', name: input.name, token: 'at_live_rotated', status: 'online' }
          },
        },
      } as unknown as RelayMessaging,
    }
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: (options) => {
        created.push(options)
        return options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap
      },
    })

    await fleet.roster()

    expect(registered).toEqual([{ name: 'factory' }])
    expect(created).toEqual([
      { workspaceKey: 'rk_live_test' },
      { workspaceKey: 'rk_live_test', agentToken: 'at_live_rotated' },
    ])
  })

  it('uses a configured at_live_ token without registering', async () => {
    const messaging = new FakeMessaging()
    const created: RelayClientFactoryOptions[] = []
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      agentToken: 'at_live_configured',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: (options) => {
        created.push(options)
        return { messaging: messaging.asMessaging() }
      },
    })

    await fleet.roster()

    expect(created).toEqual([{ workspaceKey: 'rk_live_test', agentToken: 'at_live_configured' }])
  })

  it('constructs without credentials and fails lazily with a helpful error', async () => {
    const fleet = new RelayFleetClient({
      env: {},
      createRelay: () => {
        throw new Error('should not be called')
      },
    })

    await expect(fleet.roster()).rejects.toThrow(/requires a workspace key \(rk_live_…\) or agent token \(at_live_…\)/)
  })

  it('dispose is idempotent and disconnects the event stream', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    fleet.onAgentExit(() => {})
    await flush()

    await fleet.dispose()
    await fleet.dispose()

    expect(messaging.disconnected).toBe(1)
  })
})
