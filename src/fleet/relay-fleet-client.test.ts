import { describe, expect, it, vi } from 'vitest'

import { FactoryAgentRegistrationError, MAX_REGISTRATION_ATTEMPTS, ReadOnlyFleetIdentityError, RelayFleetClient, type RelayClientFactoryOptions, type RelayClientLike } from './relay-fleet-client'
import { runFleetCli } from '../cli/fleet'

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
      const acknowledgedNode = Object.prototype.hasOwnProperty.call(this.placementAck, 'placement')
        ? this.placementAck.placement?.node
        : 'node-a'
      return {
        invocationId,
        actionName: 'spawn',
        status: this.placementAck.status ?? (this.invocations.has(invocationId) ? 'pending' : 'completed'),
        dispatchedNodeId: this.placementAck.dispatchedNodeId,
        node: { name: acknowledgedNode } as RelayNode,
        placement: {
          capability: input.capability,
          node: acknowledgedNode,
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
      identityKey: 'factory:dispatch:v1:github:agentworkforce/factory#1:implementer',
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
      identity_key: 'factory:dispatch:v1:github:agentworkforce/factory#1:implementer',
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
    expect(messaging.invokes).not.toContainEqual(expect.objectContaining({ name: 'release' }))
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

  it('creates and removes previews on the owning node', async () => {
    const messaging = new FakeMessaging()
    const preview = {
      id: 'preview-1',
      provider: 'tailscale-serve',
      namespace: 'factory-test',
      owner: 'AR-129:uuid:/linear/issues/129',
      service: 'factory',
      repo: 'AgentWorkforce/factory',
      url: 'https://mac-mini.tailnet.ts.net:10129/',
      targetPort: 3_000,
      httpsPort: 10_129,
      access: 'tailnet',
      lifetime: 'issue',
      createdAt: '2026-07-20T12:00:00.000Z',
      startCommand: 'npm run dev',
      process: {
        pid: 12_345,
        startTime: 'started-12345',
        cmdline: 'factory-preview preview-1',
        cwd: '/work/factory',
        marker: 'factory-preview-1',
      },
    }
    messaging.placementAck = { invocationId: 'preview-start', status: 'pending', placement: { node: 'mac-mini' } }
    messaging.invocations.set('preview-start', [{
      invocationId: 'preview-start',
      actionName: 'preview:tailscale-serve',
      status: 'completed',
      output: { preview },
    }])
    const fleet = createClient(messaging)

    const reference = await fleet.createPreview({
      namespace: preview.namespace,
      owner: preview.owner,
      issueKey: 'AR-129',
      service: preview.service,
      repo: preview.repo,
      targetPort: preview.targetPort,
      preferredHttpsPort: preview.httpsPort,
      startCommand: preview.startCommand,
      checkoutPath: '/work/factory',
      node: 'self',
    })

    expect(reference).toEqual({ ...preview, node: 'mac-mini' })
    expect(messaging.placements[0]).toMatchObject({
      capability: 'preview:tailscale-serve',
      repo: preview.repo,
      input: {
        operation: 'start',
        namespace: preview.namespace,
        owner: preview.owner,
        issueKey: 'AR-129',
        service: preview.service,
        repo: preview.repo,
        targetPort: preview.targetPort,
        preferredHttpsPort: preview.httpsPort,
        startCommand: preview.startCommand,
        checkoutPath: '/work/factory',
      },
    })
    expect(messaging.placements[0]).not.toHaveProperty('node')

    messaging.placementAck = { invocationId: 'preview-remove', status: 'pending', placement: { node: 'mac-mini' } }
    messaging.invocations.set('preview-remove', [{
      invocationId: 'preview-remove',
      actionName: 'preview:tailscale-serve',
      status: 'completed',
      output: { removed: true },
    }])
    await expect(fleet.removePreview(reference)).resolves.toBe(true)
    expect(messaging.placements[1]).toMatchObject({
      capability: 'preview:tailscale-serve',
      node: 'mac-mini',
      input: { operation: 'remove', preview: reference },
    })
  })

  it.each([
    ['self', { node: 'self' }],
    ['an empty node', { node: '' }],
    ['an absent node', {}],
  ])('fails closed when placement resolves to %s', async (_label, placement) => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { placement }
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
      repo: 'AgentWorkforce/factory',
      task: 'do work',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    expect(fleet.trackedAgents().size).toBe(0)
    expect(messaging.invokes).toContainEqual({
      name: 'release',
      input: {
        name: 'ar-1-impl',
        agent: 'ar-1-impl',
        reason: 'unverified-placement',
      },
    })
  })

  it('rejects the acknowledgement node even when action output synthesizes a named node', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl', node: 'mac-mini' },
    }])
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
      repo: 'AgentWorkforce/factory',
      task: 'do work',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    expect(messaging.invokes).toContainEqual({
      name: 'release',
      input: expect.objectContaining({
        name: 'ar-1-impl',
        reason: 'unverified-placement',
      }),
    })
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('retains and retries cleanup when release of an unverified placement fails', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl', node: 'untrusted-output-node' },
    }])
    messaging.invocations.set('inv-1', [{
      invocationId: 'inv-1',
      actionName: 'release',
      status: 'failed',
      error: 'temporary cleanup failure',
    }])
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
      repo: 'AgentWorkforce/factory',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    expect(fleet.trackedAgents().get('ar-1-impl')).toMatchObject({
      invocationId: 'self-placement',
      pendingReleaseReason: 'unverified-placement',
    })
    expect(fleet.trackedAgents().get('ar-1-impl')).not.toHaveProperty('node')

    messaging.invocations.set('inv-2', [{
      invocationId: 'inv-2',
      actionName: 'release',
      status: 'completed',
      output: {},
    }])
    await fleet.reconcileTrackedAgents()

    expect(messaging.invokes.filter((invoke) => invoke.name === 'release')).toHaveLength(2)
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('drains a pending rejected-placement release before disposal', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl' },
    }])
    messaging.invocations.set('inv-1', [{
      invocationId: 'inv-1',
      actionName: 'release',
      status: 'failed',
      error: 'temporary cleanup failure',
    }])
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    messaging.invocations.set('inv-2', [{
      invocationId: 'inv-2',
      actionName: 'release',
      status: 'completed',
      output: {},
    }])
    await expect(fleet.dispose()).resolves.toBeUndefined()

    expect(messaging.invokes.filter((invoke) => invoke.name === 'release')).toHaveLength(2)
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('shares a pending release retry between reconciliation and disposal', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl' },
    }])
    messaging.invocations.set('inv-1', [{
      invocationId: 'inv-1',
      actionName: 'release',
      status: 'failed',
      error: 'temporary cleanup failure',
    }])
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    let unblockRelease!: () => void
    const releaseBlocked = new Promise<void>((resolve) => {
      unblockRelease = resolve
    })
    const invoke = messaging.commands.invoke.bind(messaging.commands)
    vi.spyOn(messaging.commands, 'invoke').mockImplementation(async (name, input) => {
      if (name !== 'release') return await invoke(name, input)
      messaging.invokes.push({ name, input })
      await releaseBlocked
      return { invocationId: 'shared-release', actionName: name, status: 'completed' }
    })

    const reconciliation = fleet.reconcileTrackedAgents()
    await flush()
    const disposal = fleet.dispose()
    await flush()

    expect(messaging.invokes.filter((candidate) => candidate.name === 'release')).toHaveLength(2)
    unblockRelease()
    await expect(Promise.all([reconciliation, disposal])).resolves.toEqual([undefined, undefined])
    expect(messaging.invokes.filter((candidate) => candidate.name === 'release')).toHaveLength(2)
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('retries pending cleanup during disposal without depending on roster health', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl' },
    }])
    messaging.invocations.set('inv-1', [{
      invocationId: 'inv-1',
      actionName: 'release',
      status: 'failed',
      error: 'temporary cleanup failure',
    }])
    const presence = vi.spyOn(messaging.agents, 'presence')
      .mockRejectedValue(new Error('roster unavailable'))
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    messaging.invocations.set('inv-2', [{
      invocationId: 'inv-2',
      actionName: 'release',
      status: 'completed',
      output: {},
    }])
    await expect(fleet.dispose()).resolves.toBeUndefined()

    expect(presence).not.toHaveBeenCalled()
    expect(messaging.invokes.filter((invoke) => invoke.name === 'release')).toHaveLength(2)
    expect(fleet.trackedAgents().size).toBe(0)
  })

  it('refuses disposal without erasing an unconfirmed rejected-placement release', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'self-placement',
      status: 'pending',
      placement: { node: 'self' },
    }
    messaging.invocations.set('self-placement', [{
      invocationId: 'self-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl' },
    }])
    messaging.invocations.set('inv-1', [{
      invocationId: 'inv-1',
      actionName: 'release',
      status: 'failed',
      error: 'cleanup unavailable',
    }])
    messaging.invocations.set('inv-2', [{
      invocationId: 'inv-2',
      actionName: 'release',
      status: 'failed',
      error: 'cleanup still unavailable',
    }])
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
    })).rejects.toThrow('Relay placement did not prove a named remote node')

    await expect(fleet.dispose()).rejects.toThrow(
      'Refusing to dispose Relay fleet client with unconfirmed worker cleanup: ar-1-impl',
    )
    expect(fleet.trackedAgents().get('ar-1-impl')).toMatchObject({
      pendingReleaseReason: 'unverified-placement',
    })
  })

  it('returns and tracks the normalized acknowledgement node instead of action output', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = {
      invocationId: 'remote-placement',
      status: 'pending',
      placement: { node: ' mac-mini ' },
    }
    messaging.invocations.set('remote-placement', [{
      invocationId: 'remote-placement',
      actionName: 'spawn',
      status: 'completed',
      output: { name: 'ar-1-impl', node: 'wrong-node' },
    }])
    const fleet = createClient(messaging)

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:codex',
      node: 'self',
      repo: 'AgentWorkforce/factory',
    })).resolves.toMatchObject({ node: 'mac-mini' })

    expect(fleet.trackedAgents().get('ar-1-impl')).toMatchObject({ node: 'mac-mini' })
    expect(messaging.invokes).not.toContainEqual(expect.objectContaining({ name: 'release' }))
  })

  it('surfaces a self-placement refusal as a non-zero CLI result', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { placement: { node: 'self' } }
    const fleet = createClient(messaging)
    const stderr: string[] = []

    const code = await runFleetCli([
      'fleet',
      'spawn',
      'spawn:codex',
      '--name',
      'ar-1-impl',
    ], {
      fleet,
      stdout: { write: () => true } as never,
      stderr: { write: (chunk: string | Uint8Array) => {
        stderr.push(String(chunk))
        return true
      } } as never,
    })

    expect(code).toBe(1)
    expect(stderr.join('')).toContain('Relay placement did not prove a named remote node')
  })

  it('sweeps previews on every live preview-capable node', async () => {
    const messaging = new FakeMessaging()
    messaging.nodeRows = [{
      name: 'mac-mini',
      live: true,
      capabilities: [{ name: 'preview:tailscale-serve' }],
    }, {
      name: 'offline-mini',
      live: false,
      capabilities: [{ name: 'preview:tailscale-serve' }],
    }]
    messaging.placementAck = { invocationId: 'preview-sweep', status: 'pending', placement: { node: 'mac-mini' } }
    messaging.invocations.set('preview-sweep', [{
      invocationId: 'preview-sweep',
      actionName: 'preview:tailscale-serve',
      status: 'completed',
      output: {
        reaped: [{
          id: 'preview-orphan',
          provider: 'tailscale-serve',
          namespace: 'factory-test',
          owner: 'owner-orphan',
          service: 'factory',
          repo: 'AgentWorkforce/factory',
          url: 'https://mac-mini.tailnet.ts.net:10129/',
          targetPort: 3_000,
          httpsPort: 10_129,
          access: 'tailnet',
          lifetime: 'issue',
          createdAt: '2026-07-20T12:00:00.000Z',
          startCommand: 'npm run dev',
        }],
        skipped: [{ id: 'preview-mismatch', reason: 'live route identity mismatch' }],
      },
    }])
    const fleet = createClient(messaging)

    await expect(fleet.reapPreviews({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
    })).resolves.toMatchObject({
      reaped: [{ id: 'preview-orphan', node: 'mac-mini' }],
      skipped: [{ id: 'preview-mismatch', reason: 'live route identity mismatch', node: 'mac-mini' }],
    })
    expect(messaging.placements).toEqual([expect.objectContaining({
      capability: 'preview:tailscale-serve',
      node: 'mac-mini',
      input: { operation: 'sweep', namespace: 'factory-test', activeOwners: ['owner-active'] },
    })])
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
        capabilities: [{ name: 'spawn:claude' }, { name: 'workflow:run' }, { name: 'preview:tailscale-serve' }, { name: 'unknown:cap' }],
      },
      { name: 'beta', status: 'offline', live: false, capabilities: [{ name: 'spawn:codex' }] },
    ]
    const fleet = createClient(messaging)

    await expect(fleet.roster()).resolves.toEqual({
      agents: [{ name: 'ar-1-impl', node: 'alpha' }],
      nodes: [
        { name: 'alpha', capabilities: ['spawn:claude', 'workflow:run', 'preview:tailscale-serve'], live: true },
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
          register: async (input: { name: string }) => {
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

  // The must-not-fire half of the test above, at the site that does the write.
  //
  // Registration is the only workspace mutation in this client's bootstrap, so
  // a read-only client that reaches it has already caused the outage: the row
  // exists, the process exits before presence, and the live daemon that boots
  // next under the same name can never reclaim it (factory-cloud#55). Asserting
  // on the register call rather than on `roster()`'s rejection is deliberate —
  // the requirement is "no agent row", not "the call failed".
  it('refuses to register an identity when the client is read-only', async () => {
    const messaging = new FakeMessaging()
    const registered: Array<{ name: string }> = []
    const bootstrap: RelayClientLike = {
      messaging: {
        agents: {
          register: async (input: { name: string }) => {
            registered.push(input)
            return { id: 'agent-1', name: input.name, token: 'at_live_rotated', status: 'online' }
          },
        },
      } as unknown as RelayMessaging,
    }
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      readOnly: true,
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    const outcome = await fleet.roster().then(() => 'resolved' as const, (error: unknown) => error)

    // Asserted first, and on the recorded call rather than on the rejection, so
    // a regression reports the thing that matters: an agent row was created.
    expect(registered).toEqual([])
    expect(outcome).toBeInstanceOf(ReadOnlyFleetIdentityError)
  })

  // A read-only client is not an offline one. Handed an identity it did not
  // have to create, it reads the fleet normally — which is what keeps the mode
  // safe to apply to every read-only command rather than only to `status`.
  it('reads the fleet read-only when an agent token is already supplied', async () => {
    const messaging = new FakeMessaging()
    messaging.agentRows = [{ name: 'ar-1-impl', status: 'online' }]
    const registered: Array<{ name: string }> = []
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      agentToken: 'at_live_supplied',
      readOnly: true,
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: () => ({ messaging: messaging.asMessaging() }),
    })

    await expect(fleet.roster()).resolves.toBeDefined()
    expect(registered).toEqual([])
  })

  // A stub with @relaycast/sdk 8.2.0's REAL semantics: `registerOrRotate` and
  // `registerOrGet` are deprecated aliases that just call `register`, and the
  // engine answers a name it already holds with 409 agent_already_exists.
  // Asserting at this boundary — not on a helper in isolation — is what the
  // last regression got past (factory#316).
  function deprecatedAliasAgents(): {
    agents: Record<string, unknown>
    registerCalls: Array<{ name: string }>
    deprecatedCalls: string[]
    agentStatus: { value: string }
    presenceImpl: { value?: () => unknown }
  } {
    const existing = new Set<string>()
    const registerCalls: Array<{ name: string }> = []
    const deprecatedCalls: string[] = []
    const agentStatus = { value: 'offline' }
    const presenceImpl: { value?: () => unknown } = {}
    const register = async (input: { name: string }) => {
      registerCalls.push(input)
      if (existing.has(input.name)) {
        const conflict = new Error(`Agent "${input.name}" already exists in this workspace`) as Error & {
          code?: string
          rawCode?: string
          statusCode?: number
        }
        conflict.code = 'name_conflict'
        conflict.rawCode = 'agent_already_exists'
        conflict.statusCode = 409
        throw conflict
      }
      existing.add(input.name)
      return { id: 'agent-1', name: input.name, token: 'at_live_minted', status: 'online' }
    }
    const agents = {
      register,
      get: async (name: string) => ({ id: 'agent-1', name, status: agentStatus.value }),
      presence: async () => {
        if (presenceImpl.value) return presenceImpl.value()
        return [{ agentName: 'factory', status: agentStatus.value }]
      },
      // Present, so an optional-chain fallback never fires — the exact shape
      // that made the outage invisible.
      registerOrRotate: async (input: { name: string }) => {
        deprecatedCalls.push('registerOrRotate')
        return register(input)
      },
      registerOrGet: async (input: { name: string }) => {
        deprecatedCalls.push('registerOrGet')
        return register(input)
      },
    }
    return { agents, registerCalls, deprecatedCalls, agentStatus, presenceImpl }
  }

  it('converges when a bootstrap step fails after the agent was already registered', async () => {
    const messaging = new FakeMessaging()
    const { agents, registerCalls } = deprecatedAliasAgents()
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    let agentClientAttempts = 0
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: (options) => {
        if (!options.agentToken) return bootstrap
        agentClientAttempts += 1
        // The first post-registration step fails, which is what clears the
        // memoised messaging promise and drives a second bootstrap.
        if (agentClientAttempts === 1) throw new Error('transient relay client failure')
        return { messaging: messaging.asMessaging() }
      },
    })

    await expect(fleet.roster()).rejects.toThrow(/transient relay client failure/)

    // Against the pre-fix client this re-registers the same name and the engine
    // answers 409 forever. It must reuse the token already minted instead.
    await expect(fleet.roster()).resolves.toBeDefined()
    expect(registerCalls).toEqual([{ name: 'factory' }])
  })

  it('never calls the deprecated registerOrRotate/registerOrGet aliases', async () => {
    const messaging = new FakeMessaging()
    const { agents, deprecatedCalls } = deprecatedAliasAgents()
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    await fleet.roster()

    expect(deprecatedCalls).toEqual([])
  })

  it('fails loudly with a named error when the agent name is already taken', async () => {
    const messaging = new FakeMessaging()
    const { agents, registerCalls } = deprecatedAliasAgents()
    // Someone else already holds the name and we hold no token for it.
    await (agents.register as (input: { name: string }) => Promise<unknown>)({ name: 'factory' })
    registerCalls.length = 0
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      // The workspace key is not accepted for this name, so nothing converges.
      fetch: (async () => new Response(
        JSON.stringify({ ok: false, error: { code: 'agent_recovery_not_authorized', message: 'not authorized' } }),
        { status: 403, headers: { 'content-type': 'application/json' } },
      )) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    // Assert on the error's own identity, not the imported class: against a
    // build that lacks the export the import is `undefined`, and
    // `toThrow(undefined)` would match any error at all.
    const error = await fleet.roster().then(() => undefined, (err: unknown) => err)
    expect(error).toBeInstanceOf(FactoryAgentRegistrationError)
    expect((error as Error).name).toBe('FactoryAgentRegistrationError')
    expect((error as { agentName?: string }).agentName).toBe('factory')
    expect((error as Error).message).toMatch(/could not reclaim/)
    expect(registerCalls.length).toBeGreaterThan(0)
  })

  it('stops retrying registration once the bounded attempt budget is spent', async () => {
    let attempts = 0
    const agents = {
      register: async () => {
        attempts += 1
        throw new Error('relay unavailable')
      },
    }
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      createRelay: () => bootstrap,
    })

    const names: string[] = []
    for (let i = 0; i < MAX_REGISTRATION_ATTEMPTS + 3; i += 1) {
      await fleet.roster().catch((error: unknown) => {
        names.push(error instanceof Error ? error.name : String(error))
      })
    }

    // Transient failures are retried, but only so many times: the budget is
    // what turns an endless retry into one named, actionable failure.
    expect(names).toContain('FactoryAgentRegistrationError')
    expect(attempts).toBe(MAX_REGISTRATION_ATTEMPTS)
  })

  it('reclaims its own dead identity by audited takeover when the name is taken', async () => {
    const messaging = new FakeMessaging()
    const { agents } = deprecatedAliasAgents()
    await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      nodeId: 'node_test_1',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      fetch: (async (url: string, init: { body: string }) => {
        calls.push({ url, body: JSON.parse(init.body) as Record<string, unknown> })
        return new Response(
          JSON.stringify({ ok: true, data: { agent_id: 'agent-1', name: 'factory', token: 'at_live_seized', audit_id: 'aud-1' } }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    await expect(fleet.roster()).resolves.toBeDefined()

    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe('https://cast.agentrelay.com/v1/agents/factory/takeover')
    // Every field is an audit record, so every field must carry meaning.
    expect(calls[0]?.body.expected_agent_id).toBe('agent-1')
    expect(calls[0]?.body.node_id).toBe('node_test_1')
    expect(calls[0]?.body.actor).toBe('factory:factory')
    expect(String(calls[0]?.body.reason)).toMatch(/reclaiming its own workspace identity/)
    expect(String(calls[0]?.body.session_ref)).toMatch(/^factory-bootstrap-/)
  })

  it('refuses to take over an agent that is still online', async () => {
    const messaging = new FakeMessaging()
    const { agents, agentStatus } = deprecatedAliasAgents()
    await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
    agentStatus.value = 'online'
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    let takeovers = 0
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      fetch: (async () => {
        takeovers += 1
        return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } })
      }) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    const error = await fleet.roster().then(() => undefined, (err: unknown) => err)
    expect((error as Error).name).toBe('FactoryAgentRegistrationError')
    expect((error as Error).message).toMatch(/refusing to take over/)
    // The guard must fire BEFORE the seizure, not report it afterwards.
    expect(takeovers).toBe(0)
  })

  it.each(['online', 'active', 'idle', 'blocked', 'waiting', 'unknown'])(
    'refuses takeover for a %s agent, not just an online one',
    async (status) => {
      const messaging = new FakeMessaging()
      const { agents, agentStatus } = deprecatedAliasAgents()
      await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
      agentStatus.value = status
      const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
      let takeovers = 0
      const fleet = new RelayFleetClient({
        workspaceKey: 'rk_live_test',
        env: {},
        sleep: immediateSleep,
        pollIntervalMs: 0,
        fetch: (async () => {
          takeovers += 1
          return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } })
        }) as unknown as typeof globalThis.fetch,
        createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
      })

      const error = await fleet.roster().then(() => undefined, (err: unknown) => err)
      expect((error as Error).name).toBe('FactoryAgentRegistrationError')
      expect(takeovers).toBe(0)
    },
  )

  // A stale `offline` record plus an UNREADABLE presence must never authorise a
  // seizure: the cost of being wrong is stranding a LIVE factory's credential,
  // and the token taken is the one it would have needed to recover.
  //
  // "Unreadable" means exactly that — the request failed, or the body was not a
  // list. A presence list that WAS read and simply omits this agent is the
  // opposite case and is covered by the must-fire tests below; conflating the
  // two is the defect this pair exists to pin.
  it.each([
    ['presence request throws', () => { throw new Error('presence unavailable') }],
    ['presence returns a non-list', () => ({ not: 'a list' })],
    ['presence entry carries no status', () => [{ agentName: 'factory' }]],
    // Omission only means absence if a row FOR this agent would have been
    // recognised. A row we cannot name breaks that: if the SDK renamed the
    // naming field, a LIVE agent's row stops matching and reads as absent.
    ['a presence row carries no agent name', () => [{}]],
    ['presence rows use an unrecognised naming field', () => [{ agent: 'factory', status: 'online' }]],
  ])('fails closed and does not take over when %s', async (_label, impl) => {
    const messaging = new FakeMessaging()
    const { agents, presenceImpl } = deprecatedAliasAgents()
    await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
    // The record still claims offline — only presence can contradict it.
    presenceImpl.value = impl as () => unknown
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    let takeovers = 0
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      fetch: (async () => {
        takeovers += 1
        return new Response(
          JSON.stringify({ ok: true, data: { agent_id: 'agent-1', name: 'factory', token: 'at_live_seized' } }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    const error = await fleet.roster().then(() => undefined, (err: unknown) => err)
    expect((error as Error).name).toBe('FactoryAgentRegistrationError')
    expect((error as Error).message).toMatch(/cannot be confirmed offline/)
    // The refusal must not claim absence from presence — that is the other,
    // now-permitted case, and an operator has to be able to tell them apart.
    expect((error as Error).message).not.toMatch(/does not list this agent/)
    // The seizure must never have been attempted.
    expect(takeovers).toBe(0)
  })

  // The must-fire half of the pair. Absence from a presence list we actually
  // READ is the strongest evidence the engine can give that the agent is
  // offline; refusing to conclude it is what left an orphaned row permanently
  // unreclaimable and gated dispatch (factory-cloud#55). The empty-list arm is
  // the shape a single-agent cloud factory sees, where the dead row is the only
  // agent there is.
  it.each([
    ['presence lists only other agents', () => [{ agentName: 'someone-else', status: 'online' }]],
    ['presence is readable and empty', () => []],
  ])('reclaims the identity when %s', async (_label, impl) => {
    const messaging = new FakeMessaging()
    const { agents, presenceImpl } = deprecatedAliasAgents()
    await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
    presenceImpl.value = impl as () => unknown
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    const calls: Array<{ url: string; body: Record<string, unknown> }> = []
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      nodeId: 'node_test_1',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      fetch: (async (url: string, init: { body: string }) => {
        calls.push({ url, body: JSON.parse(init.body) as Record<string, unknown> })
        return new Response(
          JSON.stringify({ ok: true, data: { agent_id: 'agent-1', name: 'factory', token: 'at_live_seized' } }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    await expect(fleet.roster()).resolves.toBeDefined()

    // Resolving is not enough on its own: the takeover must actually have been
    // issued, for this record, rather than the conflict being routed elsewhere.
    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe('https://cast.agentrelay.com/v1/agents/factory/takeover')
    expect(calls[0]?.body.expected_agent_id).toBe('agent-1')
  })

  // Control arm. Both presence shapes run through ONE harness that differs in
  // nothing but the presence stub, so no setup difference can absorb a swap of
  // the two branches. The `not.toBe` assertion additionally kills the inert
  // fixture — the failure mode where both arms take the same path for a reason
  // unrelated to presence, which is exactly what would let a swapped
  // implementation pass a pair of separately-written tests.
  it('separates readable-and-absent from unreadable presence, and would notice a swap', async () => {
    const run = async (impl: () => unknown): Promise<{ seized: boolean; message: string }> => {
      const messaging = new FakeMessaging()
      const { agents, presenceImpl } = deprecatedAliasAgents()
      await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
      presenceImpl.value = impl
      const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
      let takeovers = 0
      const fleet = new RelayFleetClient({
        workspaceKey: 'rk_live_test',
        env: {},
        sleep: immediateSleep,
        pollIntervalMs: 0,
        fetch: (async () => {
          takeovers += 1
          return new Response(
            JSON.stringify({ ok: true, data: { agent_id: 'agent-1', name: 'factory', token: 'at_live_seized' } }),
            { status: 200, headers: { 'content-type': 'application/json' } },
          )
        }) as unknown as typeof globalThis.fetch,
        createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
      })
      const error = await fleet.roster().then(() => undefined, (err: unknown) => err)
      return { seized: takeovers > 0, message: error instanceof Error ? error.message : '' }
    }

    const absent = await run(() => [{ agentName: 'someone-else', status: 'online' }])
    const unreadable = await run(() => { throw new Error('presence unavailable') })

    expect(absent.seized).toBe(true)
    expect(unreadable.seized).toBe(false)
    // Fails if the harness cannot tell the two apart at all.
    expect(absent.seized).not.toBe(unreadable.seized)
    // And the refusal has to say WHICH of the two it was.
    expect(unreadable.message).toMatch(/presence is unreadable/)
    expect(absent.message).toBe('')
  })

  it('re-reads the agent id and retries once when the identity moved mid-takeover', async () => {
    const messaging = new FakeMessaging()
    const { agents } = deprecatedAliasAgents()
    await (agents.register as (i: { name: string }) => Promise<unknown>)({ name: 'factory' })
    const bootstrap: RelayClientLike = { messaging: { agents } as unknown as RelayMessaging }
    let attempts = 0
    const fleet = new RelayFleetClient({
      workspaceKey: 'rk_live_test',
      env: {},
      sleep: immediateSleep,
      pollIntervalMs: 0,
      fetch: (async () => {
        attempts += 1
        if (attempts === 1) {
          return new Response(
            JSON.stringify({ ok: false, error: { code: 'agent_identity_conflict', message: 'no longer has expected id' } }),
            { status: 409, headers: { 'content-type': 'application/json' } },
          )
        }
        return new Response(
          JSON.stringify({ ok: true, data: { agent_id: 'agent-1', name: 'factory', token: 'at_live_seized' } }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }) as unknown as typeof globalThis.fetch,
      createRelay: (options) => (options.agentToken ? { messaging: messaging.asMessaging() } : bootstrap),
    })

    await expect(fleet.roster()).resolves.toBeDefined()
    expect(attempts).toBe(2)
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

/**
 * #306 — every await on the placement path must be bounded.
 *
 * The 5-minute spawn-ack timeout existed before this suite and could never
 * fire: the deadline was read at the top of the poll loop, around an unbounded
 * `commands.getInvocation`, so a call that never settled never returned control
 * to the check. Production ran a single readiness sweep for 62 minutes against
 * that 5-minute bound with `consecutiveFailures: 0`.
 *
 * Each test below states which half it guards. The must-not-fire cases exist
 * because the cheap way to pass the must-fire cases is an over-eager per-call
 * timeout that kills healthy slow placements — which would trade a hang for an
 * outage.
 */
describe('RelayFleetClient placement deadlines (#306)', () => {
  const never = <T>(): Promise<T> => new Promise<T>(() => {})
  const after = <T>(ms: number, value: T): Promise<T> =>
    new Promise((resolve) => setTimeout(() => resolve(value), ms))

  const spawnInput = () => ({
    name: 'ar-1-impl',
    capability: 'spawn:claude' as const,
    node: 'self',
    repo: 'AgentWorkforce/factory',
    task: 'do work',
  })

  const pending = (invocationId: string): RelayActionInvocation =>
    ({ invocationId, actionName: 'spawn', status: 'dispatched' }) as RelayActionInvocation

  const completed = (invocationId: string): RelayActionInvocation =>
    ({
      invocationId,
      actionName: 'spawn',
      status: 'completed',
      output: { agent_name: 'ar-1-impl', session_ref: 'session-1', pid: 7 },
    }) as RelayActionInvocation

  // MUST-FIRE. The exact production failure: a poll that never settles.
  it('rejects within the ack budget when getInvocation never resolves', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { invocationId: 'inv-1', status: 'pending', placement: { node: 'node-a' } }
    messaging.commands.getInvocation = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 120 })

    const startedAt = Date.now()
    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  // MUST-FIRE. `placement.spawn` runs before any deadline existed at all.
  it('rejects within the ack budget when placement.spawn never resolves', async () => {
    const messaging = new FakeMessaging()
    messaging.placement.spawn = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 120 })

    const startedAt = Date.now()
    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  // MUST-FIRE. `#ensureLifecycleAction` is also awaited before the old deadline.
  it('rejects within the ack budget when lifecycle-action registration never resolves', async () => {
    const messaging = new FakeMessaging()
    messaging.commands.register = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 120 })

    const startedAt = Date.now()
    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  /**
   * MUST-FIRE, and the one that pins requirement 2 specifically.
   *
   * `placement.spawn` completes — slowly, but it completes — and then every
   * poll completes too. No single call is slow enough to trip a per-call
   * timeout on its own. What must end this is one budget spanning the whole
   * operation.
   *
   * Before the fix `#awaitInvocation` computed its own deadline on entry, so
   * the time `placement.spawn` had already burned was free: the operation cost
   * the spawn delay *plus* a fresh full ack budget. The assertion below is
   * what separates a shared budget from a restarted one.
   */
  it('carries one budget across placement.spawn and the poll loop', async () => {
    const messaging = new FakeMessaging()
    const spawnDelayMs = 500
    const budgetMs = 500
    messaging.placement.spawn = async (input) => {
      messaging.placements.push(input)
      return await after(spawnDelayMs, {
        invocationId: 'inv-1',
        actionName: 'spawn',
        status: 'pending',
        node: { name: 'node-a' },
        placement: { capability: input.capability, node: 'node-a', attempts: 1, queued: false },
      })
    }
    messaging.commands.getInvocation = async () => await after(20, pending('inv-1'))
    const fleet = createClient(messaging, {
      spawnAckTimeoutMs: budgetMs,
      sleep: (ms) => after(ms, undefined),
    })

    const startedAt = Date.now()
    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)
    const elapsedMs = Date.now() - startedAt

    // A restarted budget costs spawnDelayMs + budgetMs (~1000ms); one shared
    // budget cannot exceed budgetMs (~500ms). The threshold sits between them.
    expect(elapsedMs).toBeLessThan(spawnDelayMs + budgetMs - 250)
  })

  /**
   * MUST-NOT-FIRE. A healthy but slow placement: several real polls, each
   * taking real time, all comfortably inside the overall budget. This is the
   * test that forbids "fixing" #306 with a timeout short enough to break
   * working spawns.
   */
  it('still succeeds when slow-but-completing polls stay inside the overall budget', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { invocationId: 'inv-1', status: 'pending', placement: { node: 'node-a' } }
    let polls = 0
    messaging.commands.getInvocation = async () => {
      polls += 1
      return await after(40, polls < 4 ? pending('inv-1') : completed('inv-1'))
    }
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 5_000, sleep: (ms) => after(ms, undefined) })

    await expect(fleet.spawn(spawnInput())).resolves.toMatchObject({
      name: 'ar-1-impl',
      sessionRef: 'session-1',
      node: 'node-a',
    })
    expect(polls).toBe(4)
  })

  // MUST-NOT-FIRE. A release that completes normally must not be timed out.
  it('still releases normally when the release invocation completes', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 5_000 })

    await expect(fleet.release('ar-1-impl', 'done')).resolves.toBeUndefined()
    expect(messaging.invokes.map((invoke) => invoke.name)).toContain('release')
  })

  // MUST-FIRE on the release path, which `confirm` cannot reach: `release`
  // goes through `commands.invoke`, not `placement.spawn`.
  it('rejects within the ack budget when a release invocation never settles', async () => {
    const messaging = new FakeMessaging()
    messaging.commands.invoke = async (name: string) => ({
      invocationId: 'inv-rel',
      actionName: name,
      status: 'pending',
    }) as RelayActionInvocationAck
    messaging.commands.getInvocation = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 120 })

    const startedAt = Date.now()
    await expect(fleet.release('ar-1-impl', 'done')).rejects.toThrow(/timed out/i)
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  /**
   * #306 (1) — an ack alone proves the engine accepted the dispatch, not that
   * the node launched anything. Ask the SDK to read the invocation back.
   */
  it('requests placement confirmation with a budget drawn from the ack deadline', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 30_000, pollIntervalMs: 250 })

    await fleet.spawn(spawnInput())

    const placement = messaging.placements[0]!
    expect(placement.confirm).toBe(true)
    expect(placement.confirmPollIntervalMs).toBe(250)
    expect(placement.confirmTimeoutMs).toBeGreaterThan(0)
    expect(placement.confirmTimeoutMs).toBeLessThanOrEqual(30_000)
  })

  /**
   * #307 review (codex, coderabbit, cubic — three independent finds).
   *
   * `reapPreviews` bounded its placement calls but awaited `roster()` outside
   * the budget, and `roster()` is three unbounded reads. `#reapPreviewOrphans`
   * keeps the sweep promise in `#previewSweepInFlight` and schedules the next
   * sweep only from its `.finally()`, so one stalled read stops preview
   * cleanup permanently — the exact hang this change removes elsewhere.
   */
  it('rejects within the ack budget when the preview roster read never resolves', async () => {
    const messaging = new FakeMessaging()
    messaging.nodes.list = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 120 })

    const startedAt = Date.now()
    await expect(fleet.reapPreviews({ namespace: 'ns', activeOwners: [] })).rejects.toThrow(/timed out/i)
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  /**
   * #307 review (cubic P1) — the finding that matters most, because getting it
   * wrong trades an infinite hang for a leaked agent.
   *
   * Relay ACCEPTS the placement, but its response arrives after our local
   * deadline. A worker is live on the fleet and this process has already
   * reported the spawn as failed, so nothing downstream knows to release it.
   * Abandoning the wait must not mean abandoning the worker — the same shape
   * #304 handles one layer up with `LatePlacementReleasedError`.
   */
  it('releases a placement Relay accepts after the local deadline expired', async () => {
    const messaging = new FakeMessaging()
    messaging.placement.spawn = async (input) => {
      messaging.placements.push(input)
      return await after(200, {
        invocationId: 'inv-late',
        actionName: 'spawn',
        status: 'completed',
        node: { name: 'node-a' } as RelayNode,
        placement: { capability: input.capability, node: 'node-a', attempts: 1, queued: false },
      })
    }
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 60 })

    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)

    // The late placement lands after we gave up; the worker must be torn down.
    await vi.waitFor(() => {
      expect(messaging.invokes.filter((invoke) => invoke.name === 'release')).toHaveLength(1)
    }, { timeout: 3_000 })
    const release = messaging.invokes.find((invoke) => invoke.name === 'release')
    expect(release?.input).toMatchObject({ name: 'ar-1-impl', reason: 'late-placement-timeout' })
    // Released cleanly, so nothing is left retaining it.
    expect(fleet.trackedAgents().has('ar-1-impl')).toBe(false)
  })

  /**
   * The certain leak, as opposed to the possible one above: we already hold an
   * ack, so Relay definitely accepted the placement. Running out of budget
   * while polling the invocation must still release the worker.
   */
  it('releases an acked placement when the invocation poll exhausts the budget', async () => {
    const messaging = new FakeMessaging()
    messaging.placementAck = { invocationId: 'inv-1', status: 'pending', placement: { node: 'node-a' } }
    messaging.commands.getInvocation = never
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 100 })

    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)

    const releases = messaging.invokes.filter((invoke) => invoke.name === 'release')
    expect(releases).toHaveLength(1)
    expect(releases[0]?.input).toMatchObject({ name: 'ar-1-impl', reason: 'late-placement-timeout' })
  })

  /**
   * MUST-NOT-FIRE. A placement that genuinely failed launched nothing, so
   * issuing a release would be a spurious teardown against a worker that never
   * existed.
   */
  it('does not release when the abandoned placement ultimately fails', async () => {
    const messaging = new FakeMessaging()
    messaging.placement.spawn = async () => {
      await after(120, undefined)
      throw new Error('placement rejected')
    }
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 60 })

    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)
    await after(300, undefined)

    expect(messaging.invokes.filter((invoke) => invoke.name === 'release')).toEqual([])
    expect(fleet.trackedAgents().has('ar-1-impl')).toBe(false)
  })

  /**
   * #307 review (cubic). The budget must be checked *before* the request is
   * made, not after. Taking an already-started promise meant an exhausted
   * deadline still fired the call — a mutating one here — and then abandoned
   * it, which is how a local timeout orphans a remote spawn.
   *
   * `now` is stepped so the budget is intact through bootstrap and lifecycle
   * registration and spent by the time placement is reached.
   */
  it('does not issue the placement call once the budget is already spent', async () => {
    const messaging = new FakeMessaging()
    let calls = 0
    const now = () => {
      calls += 1
      return calls <= 3 ? 0 : 10_000
    }
    const fleet = createClient(messaging, { spawnAckTimeoutMs: 1_000, now })

    await expect(fleet.spawn(spawnInput())).rejects.toThrow(/timed out/i)

    // The mutating call was never made, so there is no remote spawn to orphan.
    expect(messaging.placements).toEqual([])
  })

  /**
   * A confirmed placement already carries the terminal invocation, so polling
   * for it again would double every spawn's round trips against the same
   * budget.
   */
  it('uses a confirmed invocation instead of polling for it again', async () => {
    const messaging = new FakeMessaging()
    let reads = 0
    messaging.commands.getInvocation = async (name: string, invocationId: string) => {
      reads += 1
      return completed(invocationId)
    }
    const spawnPlacement = messaging.placement.spawn.bind(messaging.placement)
    messaging.placement.spawn = async (input) => ({
      ...(await spawnPlacement(input)),
      status: 'pending',
      confirmation: completed('inv-confirmed'),
    })
    const fleet = createClient(messaging)

    await expect(fleet.spawn(spawnInput())).resolves.toMatchObject({ sessionRef: 'session-1' })
    expect(reads).toBe(0)
  })
})

/**
 * The fleet socket had no status anywhere. `#ensureEventSubscription` starts the
 * subscription with `void ... .catch()` and reported a rejection by calling
 * `#log` only, so a client that registered an agent and then failed to connect
 * was indistinguishable from a healthy one on every surface. These pin the
 * outcome into a field a health surface can publish.
 */
describe('RelayFleetClient fleet connect status', () => {
  it('starts as never-attempted, so an unstarted socket is not reported as healthy', () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    const status = fleet.fleetConnectStatus()
    expect(status.state).toBe('never-attempted')
    expect(status.attempts).toBe(0)
    expect(status.lastError).toBeUndefined()
  })

  it('reports connected only after the dial actually happens', async () => {
    const messaging = new FakeMessaging()
    const fleet = createClient(messaging)
    fleet.onAgentExit(() => {})
    await flush()
    const status = fleet.fleetConnectStatus()
    expect(status.state).toBe('connected')
    expect(status.attempts).toBe(1)
    expect(messaging.connected).toBe(1)
    expect(status.lastConnectedAtMs).toBeTypeOf('number')
  })

  /**
   * THE REGRESSION THIS EXISTS FOR. Before, this produced a `#log` line and
   * nothing else: every status surface still read healthy.
   */
  it('records a failed subscription instead of only logging it', async () => {
    const messaging = new FakeMessaging()
    messaging.commands.available = () => false
    const fleet = createClient(messaging)
    fleet.onAgentExit(() => {})
    await flush()
    const status = fleet.fleetConnectStatus()
    expect(status.state).toBe('failed')
    expect(status.attempts).toBe(1)
    expect(status.lastFailureAtMs).toBeTypeOf('number')
    expect(status.lastError).toBeDefined()
    expect(messaging.connected).toBe(0)
  })

  /**
   * CONTROL. The failure arm above only means something if the SAME harness can
   * produce the other outcome -- otherwise it would pass against a client that
   * reports 'failed' unconditionally.
   */
  it('the same harness yields connected when the gate does not throw', async () => {
    const failing = new FakeMessaging()
    failing.commands.available = () => false
    const failingFleet = createClient(failing)
    failingFleet.onAgentExit(() => {})
    await flush()

    const healthy = new FakeMessaging()
    const healthyFleet = createClient(healthy)
    healthyFleet.onAgentExit(() => {})
    await flush()

    expect(failingFleet.fleetConnectStatus().state).toBe('failed')
    expect(healthyFleet.fleetConnectStatus().state).toBe('connected')
  })

  it('reduces the cause to a name and code, never a transport message', async () => {
    const messaging = new FakeMessaging()
    messaging.commands.available = () => {
      const error = new Error('connect failed to wss://relay.example/socket?token=at_live_abcdef0123456789')
      error.name = 'FleetSocketError'
      ;(error as Error & { code?: string }).code = 'ECONNREFUSED'
      throw error
    }
    const fleet = createClient(messaging)
    fleet.onAgentExit(() => {})
    await flush()
    const lastError = fleet.fleetConnectStatus().lastError ?? ''
    expect(lastError).toBe('FleetSocketError (ECONNREFUSED)')
    expect(lastError).not.toContain('at_live_')
    expect(lastError).not.toContain('wss://')
  })
})
