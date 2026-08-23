import { readFileSync } from 'node:fs'

import { A2aAgentCardSchema } from '@relaycast/a2a'
import { describe, expect, it, vi } from 'vitest'

import { createFactoryNodeDefinition, parseFactoryNodeConfig } from './factory-node'
import { deriveFactoryPersonaCard, RelaycastAgentCardPublisher } from './factory-persona-card'
import { startFactoryNode } from './factory-node-runtime'

const persona = JSON.parse(readFileSync(
  new URL('../../.agentworkforce/agents/factory-feature-guardian/persona.json', import.meta.url),
  'utf8',
)) as unknown

describe('Factory persona cards', () => {
  it('matches the canonical mapper for runtime flags and an intent fallback skill', () => {
    const cardWithRuntimeFlags = deriveFactoryPersonaCard({
      persona: {
        ...(persona as Record<string, unknown>),
        skills: [],
        capabilities: {
          streaming: true,
          pushNotifications: { enabled: true },
        },
      },
      baseUrl: 'https://agent.example',
      version: '1.2.3',
    })

    expect(cardWithRuntimeFlags).toMatchObject({
      skills: [{
        id: 'relay-orchestrator',
        name: 'Relay Orchestrator',
      }],
      capabilities: {
        streaming: true,
        pushNotifications: true,
      },
    })

    const cardWithoutDeclaredSkills = deriveFactoryPersonaCard({
      persona: {
        ...(persona as Record<string, unknown>),
        skills: [],
        capabilities: {},
      },
      baseUrl: 'https://agent.example',
      version: '1.2.3',
    })
    expect(cardWithoutDeclaredSkills.skills).toEqual([expect.objectContaining({
      id: 'relay-orchestrator',
      name: 'Relay Orchestrator',
    })])
  })

  it('derives a shared-schema card and publishes it on the node-online edge', async () => {
    const published: unknown[] = []
    const fetch = vi.fn<typeof globalThis.fetch>(async (_url, init) => {
      published.push(JSON.parse(String(init?.body)))
      return new Response(JSON.stringify({
        ok: true,
        data: { relay_name: 'ext-factory-feature-guardian-a1b2c3d4', certification: 'level_1' },
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
        return { stop: async () => {}, done: new Promise(() => {}) }
      },
    })

    await expect(running.cardPublished).resolves.toEqual({
      name: 'factory-feature-guardian',
      address: 'ext-factory-feature-guardian-a1b2c3d4',
      certification: 'level_1',
    })
    expect(published).toEqual([{ agent_card: definition.agentCard }])
  })

  it('retries a transient card-publication failure on the next registration edge', async () => {
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
    let registration: ((info: { name: string; capabilities: string[] }) => void) | undefined
    let attempts = 0
    const warn = vi.fn()
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-1', nodeToken: 'nt_live_test' },
      cardPublisher: {
        async publishAgentCard() {
          attempts += 1
          if (attempts === 1) throw new Error('temporary directory outage')
          return { name: 'factory-feature-guardian', address: 'ext-factory-feature-guardian-retry' }
        },
      },
      warn,
      serve(options) {
        registration = options.onRegistered
        return { stop: async () => {}, done: new Promise(() => {}) }
      },
    })
    const info = { name: definition.name, capabilities: Object.keys(definition.capabilities) }

    registration?.(info)
    await vi.waitFor(() => expect(attempts).toBe(1))
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('next node registration will retry'))

    registration?.(info)
    await expect(running.cardPublished).resolves.toEqual({
      name: 'factory-feature-guardian',
      address: 'ext-factory-feature-guardian-retry',
    })
    expect(attempts).toBe(2)

    // A later reconnect after success must not publish a duplicate card.
    registration?.(info)
    await Promise.resolve()
    expect(attempts).toBe(2)
  })

  it('preserves a registration edge that arrives while publication is in flight', async () => {
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({ capabilities: ['spawn:codex'] }),
      persona: { persona, baseUrl: 'https://relay.example', version: '1.0.0' },
    })
    let registration: ((info: { name: string; capabilities: string[] }) => void) | undefined
    let rejectFirst!: (reason: unknown) => void
    let attempts = 0
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-1', nodeToken: 'nt_live_test' },
      cardPublisher: {
        publishAgentCard() {
          attempts += 1
          if (attempts === 1) {
            return new Promise((_resolve, reject) => { rejectFirst = reject })
          }
          return Promise.resolve({
            name: 'factory-feature-guardian',
            address: 'ext-factory-feature-guardian-overlap',
          })
        },
      },
      serve(options) {
        registration = options.onRegistered
        return { stop: async () => {}, done: new Promise(() => {}) }
      },
    })
    const info = { name: definition.name, capabilities: Object.keys(definition.capabilities) }

    registration?.(info)
    await vi.waitFor(() => expect(attempts).toBe(1))
    registration?.(info)
    rejectFirst(new Error('first registration lost its connection'))

    await expect(running.cardPublished).resolves.toEqual({
      name: 'factory-feature-guardian',
      address: 'ext-factory-feature-guardian-overlap',
    })
    expect(attempts).toBe(2)
  })

  it('rejects card publication when node startup terminates before registration', async () => {
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({ capabilities: ['spawn:codex'] }),
      persona: { persona, baseUrl: 'https://relay.example', version: '1.0.0' },
    })
    let rejectDone!: (reason: unknown) => void
    const done = new Promise<void>((_resolve, reject) => { rejectDone = reject })
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-1', nodeToken: 'nt_live_test' },
      cardPublisher: { publishAgentCard: vi.fn() },
      serve() {
        return { stop: async () => {}, done }
      },
    })
    const startupError = new Error('registration credentials rejected')

    rejectDone(startupError)

    await expect(running.cardPublished).rejects.toBe(startupError)
  })

  it('rejects card publication on a pre-registration stop even if done has not settled', async () => {
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({ capabilities: ['spawn:codex'] }),
      persona: { persona, baseUrl: 'https://relay.example', version: '1.0.0' },
    })
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-1', nodeToken: 'nt_live_test' },
      cardPublisher: { publishAgentCard: vi.fn() },
      serve() {
        return { stop: async () => {}, done: new Promise(() => {}) }
      },
    })

    await running.stop()

    await expect(running.cardPublished).rejects.toThrow(/stopped before persona card publication/)
  })

  it('verifies an idempotent registration conflict and returns the existing relay address', async () => {
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({
        workspaceId: 'workspace-1',
        capabilities: ['spawn:codex'],
        clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
        dryRun: false,
      }),
      persona: {
        persona,
        baseUrl: 'https://agent.example',
        version: '1.0.0',
      },
    })
    const fetch = vi.fn<typeof globalThis.fetch>(async (_url, init) => {
      if (init?.method === 'POST') {
        return new Response(JSON.stringify({
          ok: false,
          error: { code: 'a2a_agent_already_exists', message: 'already exists' },
        }), { status: 409 })
      }
      return new Response(JSON.stringify({
        ok: true,
        data: [{
          relay_name: 'ext-factory-feature-guardian-a1b2c3d4',
          agent_card: definition.agentCard,
        }],
      }), { status: 200 })
    })
    const publisher = new RelaycastAgentCardPublisher({
      baseUrl: 'https://relay.example',
      token: 'rk_live_test',
      fetch,
    })

    await expect(publisher.publishAgentCard(definition.agentCard!)).resolves.toEqual({
      name: 'factory-feature-guardian',
      address: 'ext-factory-feature-guardian-a1b2c3d4',
      alreadyPublished: true,
    })
    expect(fetch.mock.calls.map(([url]) => String(url))).toEqual([
      'https://relay.example/v1/a2a/register',
      'https://relay.example/v1/a2a/agents',
    ])
  })
})
