import { describe, expect, it } from 'vitest'

import { RelayFleetClient, type RelayActionAck, type RelayActionInvocation, type RelayFleetEvent, type RelayFleetTransport } from './relay-fleet-client'

import type { SendInput } from '../ports'

class FakeRelayFleetTransport implements RelayFleetTransport {
  readonly invokes: Array<{ actionName: string; input: Record<string, unknown>; invocationId: string }> = []
  readonly sent: SendInput[] = []
  readonly eventListeners = new Set<(event: RelayFleetEvent) => void>()
  agents: Array<{ name: string }> = []
  nodes: Array<{ name: string; status?: string; live?: boolean; capabilities?: Array<string | { name?: string }> }> = []
  completions = new Map<string, RelayActionInvocation[]>()

  async invokeAction(actionName: string, input: Record<string, unknown>, opts: { invocationId: string }): Promise<RelayActionAck> {
    this.invokes.push({ actionName, input, invocationId: opts.invocationId })
    const first = this.completions.get(opts.invocationId)?.[0]
    return {
      invocationId: opts.invocationId,
      actionName,
      status: first?.status ?? 'pending',
      dispatchedNodeId: first?.dispatchedNodeId,
      input,
    }
  }

  async getInvocation(actionName: string, invocationId: string): Promise<RelayActionInvocation> {
    const queue = this.completions.get(invocationId)
    const next = queue?.shift()
    return next ?? {
      invocationId,
      actionName,
      status: 'completed',
      output: { name: 'fallback-agent' },
    }
  }

  async listAgents(): Promise<Array<{ name: string }>> {
    return this.agents
  }

  async listNodes(): Promise<Array<{ name: string; status?: string; live?: boolean; capabilities?: Array<string | { name?: string }> }>> {
    return this.nodes
  }

  async sendMessage(input: SendInput): Promise<{ eventId?: string; targets?: string[] }> {
    this.sent.push(input)
    return { eventId: `event-${this.sent.length}`, targets: [input.to] }
  }

  onEvent(listener: (event: RelayFleetEvent) => void): () => void {
    this.eventListeners.add(listener)
    return () => {
      this.eventListeners.delete(listener)
    }
  }

  emit(event: RelayFleetEvent): void {
    for (const listener of this.eventListeners) {
      listener(event)
    }
  }
}

const immediateSleep = async () => {}

describe('RelayFleetClient', () => {
  it('invokes fleet spawn with a caller-supplied invocation id and no factory placement target', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('inv-1', [
      { invocationId: 'inv-1', actionName: 'spawn', status: 'pending' },
      { invocationId: 'inv-1', actionName: 'spawn', status: 'dispatched', dispatchedNodeId: 'node-a' },
      {
        invocationId: 'inv-1',
        actionName: 'spawn',
        status: 'completed',
        dispatchedNodeId: 'node-a',
        output: { agent_name: 'ar-1-impl', session_ref: 'session-1', pid: 123 },
      },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await expect(fleet.spawn({
      name: 'ar-1-impl',
      capability: 'spawn:claude',
      node: 'self',
      repo: 'AgentWorkforce/factory',
      clonePath: '/checkout',
      task: 'do work',
      model: 'opus',
      cwd: '/checkout',
      invocationId: 'inv-1',
      channel: 'wf-factory',
    })).resolves.toEqual({ name: 'ar-1-impl', sessionRef: 'session-1', pid: 123 })

    expect(transport.invokes).toEqual([
      {
        actionName: 'spawn',
        invocationId: 'inv-1',
        input: {
          capability: 'spawn:claude',
          name: 'ar-1-impl',
          agent: 'ar-1-impl',
          repo: 'AgentWorkforce/factory',
          clone_path: '/checkout',
          clonePath: '/checkout',
          invocationId: 'inv-1',
          task: 'do work',
          model: 'opus',
          cwd: '/checkout',
          channels: ['wf-factory'],
        },
      },
    ])
  })

  it('passes explicit node targets while leaving ordinary cloud placement to the fleet', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('inv-2', [
      { invocationId: 'inv-2', actionName: 'spawn', status: 'completed', output: { name: 'ar-2-impl' } },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await fleet.spawn({
      name: 'ar-2-impl',
      capability: 'spawn:codex',
      node: 'mac-mini',
      invocationId: 'inv-2',
    })

    expect(transport.invokes[0]?.input.node).toBe('mac-mini')
  })

  it('maps resume to an origin-targeted spawn with session_ref', async () => {
    const transport = new FakeRelayFleetTransport()
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await expect(fleet.resume({
      name: 'ar-3-impl',
      sessionRef: 'session-3',
      node: 'origin-node',
      capability: 'spawn:claude',
    })).resolves.toEqual({ name: 'fallback-agent', sessionRef: 'session-3' })

    expect(transport.invokes).toHaveLength(1)
    expect(transport.invokes[0]).toMatchObject({
      actionName: 'spawn',
      input: {
        capability: 'spawn:claude',
        name: 'ar-3-impl',
        agent: 'ar-3-impl',
        node: 'origin-node',
        session_ref: 'session-3',
      },
    })
  })

  it('omits the local self sentinel from relay resume payloads', async () => {
    const transport = new FakeRelayFleetTransport()
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await fleet.resume({
      name: 'ar-3-impl',
      sessionRef: 'session-3',
      node: 'self',
      capability: 'spawn:codex',
    })

    expect(transport.invokes).toHaveLength(1)
    expect(transport.invokes[0]?.input).toMatchObject({
      capability: 'spawn:codex',
      name: 'ar-3-impl',
      agent: 'ar-3-impl',
      session_ref: 'session-3',
    })
    expect(transport.invokes[0]?.input).not.toHaveProperty('node')
  })

  it('invokes release through the fleet protocol', async () => {
    const transport = new FakeRelayFleetTransport()
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await fleet.release('ar-4-impl', 'issue-done')

    expect(transport.invokes).toHaveLength(1)
    expect(transport.invokes[0]).toMatchObject({
      actionName: 'release',
      input: { name: 'ar-4-impl', agent: 'ar-4-impl', reason: 'issue-done' },
    })
  })

  it('maps workflow:run input into the spawn action contract', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('wf-inv', [
      { invocationId: 'wf-inv', actionName: 'spawn', status: 'completed', output: { name: 'ar-5-workflow' } },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await fleet.spawn({
      name: 'ar-5-workflow',
      capability: 'workflow:run',
      workflow: 'workflows/factory/linear-issue.ts',
      inputs: { issue: 'AR-5' },
      invocationId: 'wf-inv',
    })

    expect(transport.invokes[0]?.input).toMatchObject({
      capability: 'workflow:run',
      workflow: 'workflows/factory/linear-issue.ts',
      inputs: { issue: 'AR-5' },
    })
  })

  it('observes node-loss reschedule on the same invocation id without reinvoking', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('stable-invocation', [
      { invocationId: 'stable-invocation', actionName: 'spawn', status: 'dispatched', dispatchedNodeId: 'alpha' },
      { invocationId: 'stable-invocation', actionName: 'spawn', status: 'pending', dispatchedNodeId: 'beta' },
      { invocationId: 'stable-invocation', actionName: 'spawn', status: 'dispatched', dispatchedNodeId: 'beta' },
      {
        invocationId: 'stable-invocation',
        actionName: 'spawn',
        status: 'completed',
        dispatchedNodeId: 'beta',
        output: { name: 'ar-6-impl' },
      },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await fleet.spawn({ name: 'ar-6-impl', capability: 'spawn:claude', invocationId: 'stable-invocation' })

    expect(transport.invokes.map((invoke) => invoke.invocationId)).toEqual(['stable-invocation'])
  })

  it('fails cleanly when the fleet denies a spawn for a capability mismatch', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('inv-denied', [
      { invocationId: 'inv-denied', actionName: 'spawn', status: 'dispatched', dispatchedNodeId: 'node-a' },
      {
        invocationId: 'inv-denied',
        actionName: 'spawn',
        status: 'denied',
        error: 'node cannot spawn:codex',
      },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await expect(fleet.spawn({
      name: 'ar-7-impl',
      capability: 'spawn:codex',
      invocationId: 'inv-denied',
    })).rejects.toThrow(/spawn invocation inv-denied denied: node cannot spawn:codex/)
  })

  it('fails cleanly when the fleet marks an invocation failed', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.completions.set('inv-failed', [
      {
        invocationId: 'inv-failed',
        actionName: 'spawn',
        status: 'failed',
        error: 'node crashed during spawn',
      },
    ])
    const fleet = new RelayFleetClient({ transport, sleep: immediateSleep, pollIntervalMs: 0 })

    await expect(fleet.spawn({
      name: 'ar-8-impl',
      capability: 'spawn:claude',
      invocationId: 'inv-failed',
    })).rejects.toThrow(/spawn invocation inv-failed failed: node crashed during spawn/)
  })

  it('returns the relay agent and node roster', async () => {
    const transport = new FakeRelayFleetTransport()
    transport.agents = [{ name: 'ar-1-impl' }]
    transport.nodes = [
      { name: 'alpha', status: 'online', capabilities: ['spawn:claude', { name: 'workflow:run' }, 'unknown:cap'] },
      { name: 'beta', status: 'offline', capabilities: ['spawn:codex'] },
    ]
    const fleet = new RelayFleetClient({ transport })

    await expect(fleet.roster()).resolves.toEqual({
      agents: [{ name: 'ar-1-impl' }],
      nodes: [
        { name: 'alpha', capabilities: ['spawn:claude', 'workflow:run'], live: true },
        { name: 'beta', capabilities: ['spawn:codex'], live: false },
      ],
    })
  })

  it('maps relay delivery, message, and exit events to the FleetClient callbacks', () => {
    const transport = new FakeRelayFleetTransport()
    const fleet = new RelayFleetClient({ transport })
    const deliveries: Array<{ to: string; msgId?: string; reason?: string }> = []
    const messages: Array<{ from: string; target: string; body: string; threadId?: string; eventId?: string }> = []
    const exits: Array<{ name: string; reason?: string }> = []

    fleet.onDeliveryFailed((event) => deliveries.push(event))
    fleet.onAgentMessage((event) => messages.push(event))
    fleet.onAgentExit((name, reason) => exits.push({ name, reason }))

    transport.emit({ type: 'delivery.failed', to: 'ar-1-impl', message_id: 'msg-1', reason: 'expired' })
    transport.emit({
      type: 'dm.received',
      message: {
        id: 'msg-2',
        text: 'FACTORY_NEEDS_INPUT ar-1-impl',
        from: { name: 'ar-1-impl' },
        target: { agentName: 'factory' },
      },
    })
    transport.emit({ type: 'agent.status.offline', agentName: 'ar-1-impl', reason: 'completed' })

    expect(deliveries).toEqual([{ to: 'ar-1-impl', msgId: 'msg-1', reason: 'expired' }])
    expect(messages).toEqual([{
      from: 'ar-1-impl',
      target: 'factory',
      body: 'FACTORY_NEEDS_INPUT ar-1-impl',
      eventId: 'msg-2',
    }])
    expect(exits).toEqual([{ name: 'ar-1-impl', reason: 'completed' }])
  })
})
