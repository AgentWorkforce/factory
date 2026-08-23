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

// A client whose calls stay pending until the test settles them, so the window
// between one call reconnecting and another failing on the client it retired
// can be driven deterministically.
class ControllableClient implements HarnessDriverClientLike {
  readonly pending: Array<{ resolve: () => void; reject: (error: unknown) => void }> = []
  disconnectCalls = 0

  constructor(readonly baseUrl: string, private readonly agents: string[]) {}

  listAgents(): Promise<Array<{ name: string; cli?: string; pid?: number }>> {
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve: () => resolve(this.agents.map((name) => ({ name }))), reject })
    })
  }

  async spawnPty(): Promise<{ name: string; session_ref: string }> {
    throw new Error('not used')
  }

  async release(name: string): Promise<{ name: string }> {
    return { name }
  }

  async sendMessage(): Promise<{ event_id: string }> {
    return { event_id: 'event' }
  }

  async sendInput(): Promise<void> {}

  disconnect(): void {
    this.disconnectCalls += 1
  }
}

const flush = async (): Promise<void> => {
  for (let tick = 0; tick < 5; tick += 1) await new Promise((resolve) => setImmediate(resolve))
}

const transportFailure = (): Error =>
  Object.assign(new TypeError('fetch failed'), {
    cause: Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' }),
  })

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

  function writeRawConnection(path: string, url: string, apiKey: string): void {
    writeFileSync(path, JSON.stringify({ url, api_key: apiKey, pid: process.pid }))
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

  it('recovers every concurrent read, including the ones that lost the reconnect race', async () => {
    const connectionPath = connectionFile()
    const first = await startBroker({ apiKey: 'key-first', agents: ['worker-a'] })
    writeConnection(connectionPath, first)

    const fleet = new InternalFleetClient({ connectionPath })
    cleanup.push(() => fleet.dispose())
    await fleet.roster()

    await first.stop()
    const second = await startBroker({ apiKey: 'key-second', agents: ['worker-b'] })
    writeConnection(connectionPath, second)

    // Factory takes rosters concurrently (orchestrator/factory.ts:3296, :8740).
    // Only one of these can win the reconnect; the others fail on the client it
    // retired, and must ride the replacement rather than surfacing a stale
    // error from a broker that no longer exists.
    const rosters = await Promise.all([fleet.roster(), fleet.roster(), fleet.roster()])
    for (const roster of rosters) {
      expect(roster.agents).toEqual([{ name: 'worker-b' }])
    }
    // Exactly one reconnect, not one per in-flight call.
    expect(second.requests).toHaveLength(3)
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

  it('does not retry a superseded call once dispose() has begun', async () => {
    const connectionPath = connectionFile()
    writeRawConnection(connectionPath, 'http://127.0.0.1:1', 'key-first')
    const retired = new ControllableClient('http://127.0.0.1:1', ['worker-a'])
    const replacement = new ControllableClient('http://127.0.0.1:2', ['worker-b'])

    const fleet = new InternalFleetClient({
      connectionPath,
      client: retired,
      connect: () => replacement,
    })

    // Two rosters in flight against the same client, the way factory.ts:3296
    // issues them.
    const loser = fleet.roster()
    // Attach the expectation now: the rejection lands well before the assertion
    // point below, and an unhandled rejection in between would be reported as a
    // suite error.
    const loserRejects = expect(loser).rejects.toThrow(/fetch failed/)
    const winner = fleet.roster()
    await flush()
    expect(retired.pending).toHaveLength(2)

    // The broker rebinds. The second call fails first and wins the reconnect.
    writeRawConnection(connectionPath, 'http://127.0.0.1:2', 'key-second')
    retired.pending[1]!.reject(transportFailure())
    await flush()
    expect(replacement.pending).toHaveLength(1)
    replacement.pending[0]!.resolve()
    await expect(winner).resolves.toMatchObject({ agents: [{ name: 'worker-b' }] })

    // Factory shuts down, and only then does the first call fail on the client
    // the reconnect retired.
    await fleet.dispose()
    retired.pending[0]!.reject(transportFailure())
    await flush()

    // dispose() means stop touching the broker. The retry must not be issued —
    // and because the replacement's calls never settle here, issuing it would
    // also leave the caller hanging instead of failing.
    expect(replacement.pending).toHaveLength(1)
    await loserRejects
  })

  it('attributes a failed retry to the broker that received it, not a later one', async () => {
    const connectionPath = connectionFile()
    writeRawConnection(connectionPath, 'http://127.0.0.1:1', 'key-1')
    const first = new ControllableClient('http://127.0.0.1:1', [])
    const second = new ControllableClient('http://127.0.0.1:2', [])
    const third = new ControllableClient('http://127.0.0.1:3', [])
    const replacements = [second, third]

    const fleet = new InternalFleetClient({
      connectionPath,
      client: first,
      connect: () => replacements.shift()!,
    })

    // A fails on `first`, reconnects to `second`, and its retry is now in
    // flight against `second`.
    const retried = fleet.roster()
    const retriedRejects = expect(retried).rejects.toThrow(/broker at http:\/\/127\.0\.0\.1:2/)
    await flush()
    writeRawConnection(connectionPath, 'http://127.0.0.1:2', 'key-2')
    first.pending[0]!.reject(transportFailure())
    await flush()
    expect(second.pending).toHaveLength(1)

    // While that retry is still open, a second caller rebinds again, so
    // `#client` moves on to `third` underneath it.
    const mover = fleet.roster()
    const moverRejects = expect(mover).rejects.toThrow(/fetch failed/)
    await flush()
    writeRawConnection(connectionPath, 'http://127.0.0.1:3', 'key-3')
    second.pending[1]!.reject(transportFailure())
    await flush()
    expect(third.pending).toHaveLength(1)

    // Only now does the retry fail. It was answered by `second`, so naming
    // `third` would point a future diagnosis at a broker that never saw it.
    second.pending[0]!.reject(transportFailure())
    await retriedRejects
    third.pending[0]!.reject(transportFailure())
    await moverRejects
  })

  it('re-points a non-idempotent send at the rebound broker without replaying it', async () => {
    const connectionPath = connectionFile()
    const first = await startBroker({ apiKey: 'key-first', agents: ['worker'] })
    writeConnection(connectionPath, first)

    const fleet = new InternalFleetClient({ connectionPath })
    cleanup.push(() => fleet.dispose())
    const streamIdentityBeforeRebind = await fleet.messageStreamIdentity()
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
    await expect(fleet.messageStreamIdentity()).resolves.toBe(streamIdentityBeforeRebind)
  })
})
