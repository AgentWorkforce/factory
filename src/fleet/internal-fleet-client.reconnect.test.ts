import { afterEach, describe, expect, it } from 'vitest'
import { createServer, type Server } from 'node:http'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { AddressInfo } from 'node:net'

import { HarnessDriverClient } from '@agent-relay/harness-driver'

import { InternalFleetClient, type HarnessDriverClientLike } from './internal-fleet-client'

// A stand-in for the relay broker's HTTP surface. Only the endpoints the fleet
// client actually reaches are implemented: `/api/spawned` (listAgents, which is
// what `roster()` is built on) and `/api/send` (sendMessage).
class FakeBroker {
  #server: Server
  readonly apiKey: string
  readonly agents: string[]
  readonly requests: Array<{ path: string; apiKey?: string }> = []
  status = 200

  private constructor(server: Server, apiKey: string, agents: string[]) {
    this.#server = server
    this.apiKey = apiKey
    this.agents = agents
  }

  static async start(input: { apiKey: string; agents: string[] }): Promise<FakeBroker> {
    const server = createServer()
    const broker = new FakeBroker(server, input.apiKey, input.agents)
    server.on('request', (req, res) => {
      const path = (req.url ?? '').split('?')[0]!
      broker.requests.push({ path, apiKey: firstHeader(req.headers['x-api-key']) })
      if (broker.status !== 200) {
        res.writeHead(broker.status, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ error: 'broker rejected the request' }))
        return
      }
      if (path === '/api/spawned') {
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ agents: broker.agents.map((name) => ({ name, cli: 'codex', pid: process.pid })) }))
        return
      }
      if (path === '/api/send') {
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ event_id: `event-from-${broker.apiKey}`, targets: ['worker'] }))
        return
      }
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'not found' }))
    })
    // Port 0 mirrors AGENT_RELAY_BROKER_PORT=0: the OS hands out a fresh
    // ephemeral port, which is exactly what makes a restart change the URL.
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    return broker
  }

  get url(): string {
    return `http://127.0.0.1:${(this.#server.address() as AddressInfo).port}`
  }

  async stop(): Promise<void> {
    this.#server.closeAllConnections?.()
    await new Promise<void>((resolve) => this.#server.close(() => resolve()))
  }
}

function firstHeader(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value
}

// `brokerPid` is a getter on the real client and is only populated by spawn().
// Overlay it so a connect()ed client can stand in for a spawned one, keeping
// every method bound to the real instance so its private state still works.
function asSpawnedClient(client: HarnessDriverClient, brokerPid: number): HarnessDriverClientLike {
  return new Proxy(client, {
    get(target, property) {
      if (property === 'brokerPid') return brokerPid
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  }) as unknown as HarnessDriverClientLike
}

describe('InternalFleetClient broker rebind recovery', () => {
  const cleanup: Array<() => Promise<void> | void> = []

  afterEach(async () => {
    for (const dispose of cleanup.splice(0).reverse()) {
      await dispose()
    }
  })

  function connectionFile(): string {
    const dir = mkdtempSync(join(tmpdir(), 'factory-connection-'))
    cleanup.push(() => rmSync(dir, { recursive: true, force: true }))
    return join(dir, 'connection.json')
  }

  function writeConnection(path: string, broker: FakeBroker): void {
    // `pid` must be a live process or HarnessDriverClient.connect() rejects the
    // file as stale. The test process stands in for the broker process.
    writeFileSync(path, JSON.stringify({ url: broker.url, api_key: broker.apiKey, pid: process.pid }))
  }

  async function startBroker(input: { apiKey: string; agents: string[] }): Promise<FakeBroker> {
    const broker = await FakeBroker.start(input)
    cleanup.push(() => broker.stop())
    return broker
  }

  it('picks up a rebound broker port from a rewritten connection.json', async () => {
    const connectionPath = connectionFile()
    const first = await startBroker({ apiKey: 'key-first', agents: ['worker-a'] })
    writeConnection(connectionPath, first)

    const fleet = new InternalFleetClient({ connectionPath })
    cleanup.push(() => fleet.dispose())

    // Boot-time roster works: the daemon captured a port that is alive.
    await expect(fleet.roster()).resolves.toMatchObject({ agents: [{ name: 'worker-a' }] })

    // The node restarts. The old port dies, the broker rebinds a NEW ephemeral
    // port and rewrites connection.json — precisely the observed sequence.
    const deadUrl = first.url
    await first.stop()
    const second = await startBroker({ apiKey: 'key-second', agents: ['worker-b'] })
    expect(second.url).not.toBe(deadUrl)
    writeConnection(connectionPath, second)

    const roster = await fleet.roster()
    expect(roster.agents).toEqual([{ name: 'worker-b' }])
    // It reached the NEW broker with the NEW api key, not a cached credential.
    expect(second.requests).toEqual([{ path: '/api/spawned', apiKey: 'key-second' }])
  })

  it('surfaces an error, and never reconnects, when the broker is simply down', async () => {
    const connectionPath = connectionFile()
    const broker = await startBroker({ apiKey: 'key-only', agents: ['worker-a'] })
    writeConnection(connectionPath, broker)

    let connectCalls = 0
    const fleet = new InternalFleetClient({
      connectionPath,
      connect: (options) => {
        connectCalls += 1
        return HarnessDriverClient.connect(options)
      },
    })
    cleanup.push(() => fleet.dispose())
    await expect(fleet.roster()).resolves.toMatchObject({ agents: [{ name: 'worker-a' }] })
    expect(connectCalls).toBe(1)

    // The broker dies without anything rewriting connection.json.
    await broker.stop()

    for (let attempt = 0; attempt < 3; attempt += 1) {
      await expect(fleet.roster()).rejects.toThrow(/fetch failed/)
    }
    // The connection file still names the broker we are attached to, so there
    // is nothing to reconnect to: no reconnect was attempted on any of the
    // three failures. A down broker fails; it does not spin.
    expect(connectCalls).toBe(1)
  })

  it('attributes a transport failure to the base URL it actually attempted', async () => {
    const connectionPath = connectionFile()
    const broker = await startBroker({ apiKey: 'key-only', agents: [] })
    writeConnection(connectionPath, broker)

    const fleet = new InternalFleetClient({ connectionPath })
    cleanup.push(() => fleet.dispose())
    await fleet.roster()
    const deadUrl = broker.url
    await broker.stop()

    await expect(fleet.roster()).rejects.toThrow(new RegExp(`broker at ${deadUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`))
  })

  it('does not reconnect when a reachable broker rejects the call', async () => {
    const connectionPath = connectionFile()
    const broker = await startBroker({ apiKey: 'key-only', agents: [] })
    writeConnection(connectionPath, broker)

    let connectCalls = 0
    const fleet = new InternalFleetClient({
      connectionPath,
      connect: (options) => {
        connectCalls += 1
        return HarnessDriverClient.connect(options)
      },
    })
    cleanup.push(() => fleet.dispose())
    await fleet.roster()

    broker.status = 500
    await expect(fleet.roster()).rejects.toThrow()
    // The broker answered — it is reachable, the connection file is unchanged,
    // and a 500 is not a reason to go looking for a different broker.
    expect(connectCalls).toBe(1)
  })

  it('does not abandon a broker it spawned while that broker is still alive', async () => {
    const connectionPath = connectionFile()
    const spawned = await startBroker({ apiKey: 'key-spawned', agents: ['worker-a'] })
    writeConnection(connectionPath, spawned)

    let connectCalls = 0
    const fleet = new InternalFleetClient({
      connectionPath,
      ownsBroker: true,
      // Stands in for the broker child this client spawned. It reports alive.
      client: asSpawnedClient(HarnessDriverClient.connect({ connectionPath }), process.pid),
      isProcessAlive: () => true,
      connect: (options) => {
        connectCalls += 1
        return HarnessDriverClient.connect(options)
      },
    })
    cleanup.push(() => fleet.dispose())
    await fleet.roster()

    // Something else's broker takes over the connection file while ours is
    // still running and merely unreachable for this call.
    await spawned.stop()
    const other = await startBroker({ apiKey: 'key-other', agents: ['worker-b'] })
    writeConnection(connectionPath, other)

    await expect(fleet.roster()).rejects.toThrow(/fetch failed/)
    // Switching would orphan the broker we are responsible for shutting down.
    expect(connectCalls).toBe(0)
    expect(other.requests).toHaveLength(0)
  })

  it('adopts the rebound broker once the broker it spawned is gone', async () => {
    const connectionPath = connectionFile()
    const spawned = await startBroker({ apiKey: 'key-spawned', agents: ['worker-a'] })
    writeConnection(connectionPath, spawned)

    const fleet = new InternalFleetClient({
      connectionPath,
      ownsBroker: true,
      client: asSpawnedClient(HarnessDriverClient.connect({ connectionPath }), process.pid),
      isProcessAlive: () => false,
    })
    cleanup.push(() => fleet.dispose())
    await fleet.roster()

    await spawned.stop()
    const replacement = await startBroker({ apiKey: 'key-replacement', agents: ['worker-b'] })
    writeConnection(connectionPath, replacement)

    await expect(fleet.roster()).resolves.toMatchObject({ agents: [{ name: 'worker-b' }] })
  })

  it('re-points a non-idempotent send at the rebound broker without replaying it', async () => {
    const connectionPath = connectionFile()
    const first = await startBroker({ apiKey: 'key-first', agents: ['worker'] })
    writeConnection(connectionPath, first)

    const fleet = new InternalFleetClient({ connectionPath })
    cleanup.push(() => fleet.dispose())
    await fleet.sendMessage({ to: 'worker', text: 'hello' })

    await first.stop()
    const second = await startBroker({ apiKey: 'key-second', agents: ['worker'] })
    writeConnection(connectionPath, second)

    // The send that straddles the rebind still fails — replaying a message the
    // dead broker may have accepted is not safe. The transport is repaired, so
    // the caller's next attempt lands.
    await expect(fleet.sendMessage({ to: 'worker', text: 'hello' })).rejects.toThrow()
    expect(second.requests).toHaveLength(0)

    await fleet.sendMessage({ to: 'worker', text: 'hello' })
    expect(second.requests.map((request) => request.path)).toEqual(['/api/send'])
  })
})
