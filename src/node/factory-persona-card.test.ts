import { readFileSync } from 'node:fs'

import { A2aAgentCardSchema } from '@relaycast/a2a'
import { describe, expect, it, vi } from 'vitest'

import { createFactoryNodeDefinition, parseFactoryNodeConfig } from './factory-node'
import { RelaycastAgentCardPublisher } from './factory-persona-card'
import { startFactoryNode } from './factory-node-runtime'

const persona = JSON.parse(readFileSync(
  new URL('../../.agentworkforce/agents/factory-feature-guardian/persona.json', import.meta.url),
  'utf8',
)) as unknown

describe('Factory persona cards', () => {
  it('derives a shared-schema card and publishes it on the node-online edge', async () => {
    const published: unknown[] = []
    const fetch = vi.fn<typeof globalThis.fetch>(async (_url, init) => {
      published.push(JSON.parse(String(init?.body)))
      return new Response(JSON.stringify({
        ok: true,
        data: { relay_name: 'factory-feature-guardian', certification: 'level_1' },
      }), { status: 201 })
    })
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({
        workspaceId: 'workspace-1',
        capabilities: ['spawn:codex'],
        clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
        dryRun: false,
      }),
      name: 'factory-persona-node',
      persona: {
        persona,
        baseUrl: 'https://relay.example',
        version: '1.0.0',
      },
    })
    expect(() => A2aAgentCardSchema.parse(definition.agentCard)).not.toThrow()
    expect(definition.agentCard).toMatchObject({
      name: 'factory-feature-guardian',
      skills: [expect.objectContaining({ id: 'factory-feature-verification' })],
    })

    const publisher = new RelaycastAgentCardPublisher({
      baseUrl: 'https://relay.example',
      token: 'rk_live_test',
      fetch,
    })
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-1', nodeToken: 'nt_live_test' },
      cardPublisher: publisher,
      serve(options) {
        queueMicrotask(() => options.onRegistered?.({
          name: definition.name,
          capabilities: Object.keys(definition.capabilities),
        }))
        return { stop: async () => {}, done: Promise.resolve() }
      },
    })

    await expect(running.cardPublished).resolves.toEqual({
      name: 'factory-feature-guardian',
      address: 'factory-feature-guardian',
      certification: 'level_1',
    })
    expect(published).toEqual([{ agent_card: definition.agentCard }])
  })
})
