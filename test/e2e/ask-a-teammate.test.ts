import { readFileSync } from 'node:fs'
import { createServer, type Server } from 'node:http'

import { A2aAgentCardSchema } from '@relaycast/a2a'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { BrokerEvent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import { ensureRelayBroker } from '../../src/fleet/ensure-relay-broker'
import { InternalFleetClient, type HarnessDriverClientLike } from '../../src/fleet/internal-fleet-client'
import { RelayFleetClient } from '../../src/fleet/relay-fleet-client'
import { askTeammate, RelaycastTeammateDirectory } from '../../src/fleet/teammates'
import { createFactoryNodeDefinition, parseFactoryNodeConfig } from '../../src/node/factory-node'
import { RelaycastAgentCardPublisher } from '../../src/node/factory-persona-card'
import { startFactoryNode } from '../../src/node/factory-node-runtime'

type DirectoryRow = {
  name: string
  description?: string
  address: string
  skills: Array<{ id?: string; name: string; description?: string; tags?: string[] }>
  tags: string[]
  url: string
  kind: 'native' | 'a2a'
  status: string
}

const openServers: Server[] = []
afterEach(async () => {
  await Promise.all(openServers.splice(0).map((server) => new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve())
  })))
})

describe('discover -> ask -> reply', () => {
  it('discovers one skilled teammate, completes a relay round trip, and publishes a hosted persona card', async () => {
    const directoryRows: DirectoryRow[] = [{
      name: 'infra-agent',
      description: 'Watches production infrastructure.',
      address: 'infra-agent',
      skills: [{ id: 'infra-watch', name: 'Infra Watch' }],
      tags: ['operations'],
      url: 'http://relay.local/a2a/rpc',
      kind: 'native',
      status: 'online',
    }, {
      name: 'review-agent',
      description: 'Reviews code changes.',
      address: 'review-agent',
      skills: [{ id: 'code-review', name: 'Code Review' }],
      tags: ['quality'],
      url: 'http://relay.local/a2a/rpc',
      kind: 'native',
      status: 'online',
    }]
    const directoryRequests: URL[] = []
    const server = createServer(async (request, response) => {
      const origin = `http://${request.headers.host}`
      const url = new URL(request.url ?? '/', origin)
      if (request.method === 'GET' && url.pathname === '/v1/a2a/directory') {
        directoryRequests.push(url)
        // Deliberately return every row. Factory must enforce exact skill/tag
        // filtering too, otherwise this E2E returns both cards and fails.
        json(response, 200, { ok: true, data: directoryRows })
        return
      }
      if (request.method === 'POST' && url.pathname === '/v1/a2a/register') {
        const body = JSON.parse(await requestBody(request)) as { agent_card?: unknown }
        const card = A2aAgentCardSchema.parse(body.agent_card)
        const relayName = `ext-${card.name}-a1b2c3d4`
        directoryRows.push({
          name: card.name,
          address: relayName,
          skills: card.skills,
          tags: Array.isArray(card.provider?.tags)
            ? card.provider.tags.filter((value): value is string => typeof value === 'string')
            : [],
          url: card.url,
          kind: 'a2a',
          status: 'active',
        })
        json(response, 201, {
          ok: true,
          data: { relay_name: relayName, certification: 'level_1' },
        })
        return
      }
      json(response, 404, { ok: false })
    })
    openServers.push(server)
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (!address || typeof address === 'string') throw new Error('mock directory did not bind')
    const baseUrl = `http://127.0.0.1:${address.port}`

    const discoveryFleet = new RelayFleetClient({
      baseUrl,
      workspaceKey: 'rk_live_test',
      directoryFetch: fetch,
    })
    const infra = await discoveryFleet.discoverTeammates({ skill: 'infra-watch' })
    expect(infra).toEqual([expect.objectContaining({
      name: 'infra-agent',
      description: 'Watches production infrastructure.',
      address: 'infra-agent',
      skills: [expect.objectContaining({ id: 'infra-watch' })],
    })])
    await expect(discoveryFleet.discoverTeammates({ skill: 'unknown-skill' })).resolves.toEqual([])
    expect(directoryRequests[0]?.searchParams.get('skill')).toBe('infra-watch')

    const broker = new InProcessRelayBroker(['factory-worker', 'infra-agent'])
    const brokerHandle = await ensureRelayBroker({
      connect: () => { throw new Error('no existing broker') },
      spawn: async () => broker,
      env: {},
      resolveWorkspaceKey: () => undefined,
    })
    expect(brokerHandle.started).toBe(true)
    const workerFleet = new InternalFleetClient({
      client: brokerHandle.client,
      ownsBroker: brokerHandle.started,
      teammateDirectory: new RelaycastTeammateDirectory({
        baseUrl,
        token: 'rk_live_test',
      }),
    })
    const teammateFleet = new InternalFleetClient({
      client: brokerHandle.client,
    })
    const cannedReply = 'The deploy is healthy; no intervention is needed.'
    const questions: string[] = []
    const stopStub = teammateFleet.onAgentMessage((message) => {
      if (message.from !== 'factory-worker' || message.target !== 'infra-agent') return
      questions.push(message.body)
      void teammateFleet.sendMessage({
        from: 'infra-agent',
        to: 'factory-worker',
        text: cannedReply,
      })
    })

    const roundTrip = await askTeammate(workerFleet, {
      from: 'factory-worker',
      question: 'Is the deploy healthy?',
      skill: 'infra-watch',
      timeoutMs: 1_000,
    })
    expect(questions).toEqual(['Is the deploy healthy?'])
    expect(roundTrip.teammate.name).toBe('infra-agent')
    expect(roundTrip.reply).toMatchObject({
      from: 'infra-agent',
      target: 'factory-worker',
      body: cannedReply,
    })

    const persona = JSON.parse(readFileSync(
      new URL('../../.agentworkforce/agents/factory-feature-guardian/persona.json', import.meta.url),
      'utf8',
    )) as unknown
    const definition = createFactoryNodeDefinition({
      config: parseFactoryNodeConfig({
        workspaceId: 'workspace-test',
        capabilities: ['spawn:codex'],
        clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
        dryRun: false,
      }),
      name: 'factory-persona-node',
      persona: {
        persona,
        baseUrl,
        version: 'e2e',
      },
    })
    expect(() => A2aAgentCardSchema.parse(definition.agentCard)).not.toThrow()
    const running = startFactoryNode({
      definition,
      connection: { nodeId: 'node-test', nodeToken: 'nt_live_test' },
      cardPublisher: new RelaycastAgentCardPublisher({
        baseUrl,
        token: 'rk_live_test',
      }),
      serve(options) {
        queueMicrotask(() => options.onRegistered?.({
          name: definition.name,
          capabilities: Object.keys(definition.capabilities),
        }))
        return { stop: async () => {}, done: Promise.resolve() }
      },
    })
    await expect(running.cardPublished).resolves.toMatchObject({
      name: 'factory-feature-guardian',
    })
    await expect(discoveryFleet.discoverTeammates({
      skill: 'factory-feature-verification',
    })).resolves.toEqual([expect.objectContaining({
      name: 'factory-feature-guardian',
      address: 'ext-factory-feature-guardian-a1b2c3d4',
      kind: 'a2a',
    })])

    stopStub()
    await teammateFleet.dispose()
    await workerFleet.dispose()
    expect(broker.shutdownCalls).toBe(1)
    await discoveryFleet.dispose()
  })
})

class InProcessRelayBroker implements HarnessDriverClientLike {
  readonly brokerPid = process.pid
  readonly #agents: Array<{ name: string; cli?: string; pid?: number }>
  readonly #events = new Set<(event: BrokerEvent) => void>()
  readonly #deliveryEvents = new Set<(event: BrokerEvent) => void>()
  readonly #exitEvents = new Set<(agent: { name: string; sessionId?: string }) => void>()
  #sequence = 0
  shutdownCalls = 0

  constructor(agentNames: string[]) {
    this.#agents = agentNames.map((name) => ({ name, cli: 'stub', pid: process.pid }))
  }

  async spawnPty(input: SpawnPtyInput) {
    this.#agents.push({ name: input.name, cli: input.cli, pid: process.pid })
    return { name: input.name, session_ref: `session-${input.name}`, pid: process.pid }
  }

  async release(name: string): Promise<{ name: string }> {
    const index = this.#agents.findIndex((agent) => agent.name === name)
    if (index >= 0) this.#agents.splice(index, 1)
    return { name }
  }

  async listAgents() {
    return this.#agents.map((agent) => ({ ...agent }))
  }

  async sendMessage(input: SendMessageInput): Promise<{ event_id: string; targets: string[] }> {
    if (!this.#agents.some((agent) => agent.name === input.to)) {
      throw new Error(`unknown recipient ${input.to}`)
    }
    const eventId = `event-${++this.#sequence}`
    queueMicrotask(() => {
      this.#emit({
        kind: 'delivery_injected',
        name: input.to,
        delivery_id: `delivery-${this.#sequence}`,
        event_id: eventId,
      })
      this.#emit({
        kind: 'relay_inbound',
        event_id: `inbound-${this.#sequence}`,
        from: input.from ?? 'factory',
        target: input.to,
        body: input.text,
      })
    })
    return { event_id: eventId, targets: [input.to] }
  }

  async sendInput(): Promise<void> {}
  connectEvents(): void {}
  disconnect(): void {}
  async shutdown(): Promise<void> { this.shutdownCalls += 1 }

  onEvent(listener: (event: BrokerEvent) => void): () => void {
    this.#events.add(listener)
    return () => this.#events.delete(listener)
  }

  addListener(event: 'agentExited', listener: (agent: { name: string; sessionId?: string }) => void): () => void
  addListener(event: 'deliveryUpdate', listener: (event: BrokerEvent) => void): () => void
  addListener(
    event: 'agentExited' | 'deliveryUpdate',
    listener: ((agent: { name: string; sessionId?: string }) => void) | ((event: BrokerEvent) => void),
  ): () => void {
    if (event === 'agentExited') {
      const exitListener = listener as (agent: { name: string; sessionId?: string }) => void
      this.#exitEvents.add(exitListener)
      return () => this.#exitEvents.delete(exitListener)
    }
    const deliveryListener = listener as (event: BrokerEvent) => void
    this.#deliveryEvents.add(deliveryListener)
    return () => this.#deliveryEvents.delete(deliveryListener)
  }

  #emit(event: BrokerEvent): void {
    for (const listener of this.#events) listener(event)
    for (const listener of this.#deliveryEvents) listener(event)
  }
}

function requestBody(request: import('node:http').IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk) => { body += chunk })
    request.on('end', () => resolve(body))
    request.on('error', reject)
  })
}

function json(response: import('node:http').ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'content-type': 'application/json' })
  response.end(JSON.stringify(body))
}
