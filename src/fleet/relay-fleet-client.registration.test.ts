import { describe, expect, it } from 'vitest'
import { RelaycastMessagingClient, type RelaycastMessagingOptions } from '@agent-relay/sdk/messaging'

import { RelayFleetClient } from './relay-fleet-client'

// Exercise the published SDK normalizer: RelayAgent has metadata.fleet, not a
// top-level node field. A fake RelayMessaging returning { node: 'sandbox' }
// bypasses this boundary and cannot reproduce the registration failure.
describe('remote registration through the Relay SDK', () => {
  it.each([
    ['node_id', 'id'],
    ['nodeId', 'nodeId'],
  ])('resolves fleet metadata %s against roster %s without weakening admission', async (agentIdKey, nodeIdKey) => {
    const agent = {
      id: 'worker-1', name: 'worker', status: 'active',
      metadata: { fleet: { [agentIdKey]: 'node_sandbox' } },
    }
    const presence = { agent_id: 'worker-1', agent_name: 'worker', status: 'online' }
    const node = {
      [nodeIdKey]: 'node_sandbox', name: 'sandbox', status: 'online', live: true,
      capabilities: [{ name: 'spawn:codex' }],
    }
    const messaging = new RelaycastMessagingClient({
      relaycast: {
        agents: { list: async () => [agent], presence: async () => [presence] },
        nodes: { list: async () => [node] },
      } as unknown as RelaycastMessagingOptions['relaycast'],
    })
    const fleet = new RelayFleetClient({ messaging, registerLifecycleAction: false })
    const input = { name: 'worker', node: 'sandbox', capability: 'spawn:codex' as const }

    expect((await messaging.agents.list())[0]).not.toHaveProperty('node')
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(true)
    expect((await fleet.roster()).agents).toEqual([{ name: 'worker', node: 'sandbox' }])

    // The same online name on another node must not satisfy this placement.
    await expect(fleet.isAgentRegistered({ ...input, node: 'other-sandbox' })).resolves.toBe(false)
    agent.metadata.fleet[agentIdKey] = 'node_missing'
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(false)
    // A node ID must resolve through the node roster, never by treating it as a name.
    agent.metadata.fleet[agentIdKey] = 'sandbox'
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(false)
    agent.metadata.fleet[agentIdKey] = 'node_sandbox'

    presence.status = 'offline'
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(false)
    presence.status = 'online'
    node.live = false
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(false)
    node.live = true
    node.capabilities = [{ name: 'spawn:claude' }]
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(false)
    node.capabilities = [{ name: 'spawn:codex' }]
    await expect(fleet.isAgentRegistered(input)).resolves.toBe(true)
  })
})
