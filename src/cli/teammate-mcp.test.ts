import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { describe, expect, it } from 'vitest'

import { FakeFleetClient } from '../testing/fakes'
import { createFactoryTeammateMcpServer } from './teammate-mcp'

describe('Factory teammate MCP', () => {
  it('exposes discover and bounded ask to the spawned worker through its injected MCP server', async () => {
    const fleet = new FakeFleetClient()
    fleet.teammates.push({
      name: 'infra-agent',
      address: 'infra-agent',
      description: 'Infrastructure specialist',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: ['operations'],
      kind: 'native',
      url: 'relay://infra-agent',
    })
    fleet.waitForInjected = async (input) => {
      fleet.messages.push(input)
      queueMicrotask(() => fleet.emitAgentMessage({
        from: input.to,
        target: 'ar-139-impl-factory',
        body: 'The deployment heartbeat is frozen.',
      }))
      return { eventId: 'event-1', targets: [input.to] }
    }

    const { server } = createFactoryTeammateMcpServer({
      skipBootstrap: true,
      agentName: 'ar-139-impl-factory',
      agentToken: 'at_live_test',
    }, { fleet })
    const client = new Client({ name: 'factory-teammate-test', version: '1.0.0' })
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)])

    try {
      const listed = await client.listTools()
      expect(listed.tools.map((tool) => tool.name)).toEqual(expect.arrayContaining([
        'discover_teammates',
        'ask_teammate',
        // Positive control: Factory extends rather than replaces Agent Relay.
        'send_dm',
      ]))

      const discovered = await client.callTool({
        name: 'discover_teammates',
        arguments: { skill: 'infra-watch' },
      })
      expect(toolJson(discovered)).toEqual([expect.objectContaining({ name: 'infra-agent' })])

      const asked = await client.callTool({
        name: 'ask_teammate',
        arguments: {
          skill: 'infra-watch',
          question: 'Why is the deployment stuck?',
          timeoutMs: 1_000,
        },
      })
      expect(toolJson(asked)).toMatchObject({
        teammate: { name: 'infra-agent' },
        reply: {
          from: 'infra-agent',
          target: 'ar-139-impl-factory',
          body: 'The deployment heartbeat is frozen.',
        },
      })
      expect(fleet.messages).toEqual([expect.objectContaining({
        from: 'ar-139-impl-factory',
        to: 'infra-agent',
        text: 'Why is the deployment stuck?',
      })])
    } finally {
      await client.close()
      await server.close()
    }
  })
})

function toolJson(result: { content?: unknown }): unknown {
  const content = result.content as Array<{ type: string; text?: string }> | undefined
  const text = content?.find((entry) => entry.type === 'text')?.text
  if (!text) throw new Error('tool returned no text content')
  return JSON.parse(text)
}
