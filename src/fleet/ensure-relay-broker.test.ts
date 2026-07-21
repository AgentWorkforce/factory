import { describe, expect, it, vi } from 'vitest'

import { ensureRelayBroker } from './ensure-relay-broker'
import type { HarnessDriverClientLike } from './internal-fleet-client'

const noStoredWorkspaceKey = () => undefined

const fakeClient = (tag: string): HarnessDriverClientLike => ({
  // tag lets a test assert which client (connected vs spawned) came back.
  brokerPid: tag === 'connected' ? 111 : 222,
  async spawnPty(input) {
    return { name: input.name, sessionId: 'session' }
  },
  async release(name) {
    return { name }
  },
  async listAgents() {
    return []
  },
  async sendMessage(input) {
    return { event_id: 'event', targets: [input.to] }
  },
  async sendInput() {},
})

describe('ensureRelayBroker', () => {
  it('reuses the already-running broker when connect succeeds (never spawns, not owned)', async () => {
    const connected = fakeClient('connected')
    const spawn = vi.fn(async () => fakeClient('spawned'))
    const connect = vi.fn(() => connected)

    const handle = await ensureRelayBroker({ cwd: '/work', connectionPath: '/work/conn.json', connect, spawn })

    expect(handle.client).toBe(connected)
    expect(handle.started).toBe(false)
    expect(connect).toHaveBeenCalledWith({ cwd: '/work', connectionPath: '/work/conn.json' })
    expect(spawn).not.toHaveBeenCalled()
  })

  it('starts a broker when none is running (connect throws) and marks it owned', async () => {
    const spawned = fakeClient('spawned')
    const connect = vi.fn(() => {
      throw new Error('No running broker found')
    })
    const spawn = vi.fn(async () => spawned)

    const handle = await ensureRelayBroker({ cwd: '/work', connect, spawn, env: {}, resolveWorkspaceKey: noStoredWorkspaceKey })

    expect(handle.client).toBe(spawned)
    expect(handle.started).toBe(true)
    expect(handle.workspaceKey).toBeUndefined()
    expect(spawn).toHaveBeenCalledWith({ cwd: '/work', workspaceKey: undefined })
  })

  it('persists an auto-started broker in the explicit relay state directory', async () => {
    const spawned = fakeClient('spawned')
    const spawn = vi.fn(async () => spawned)
    const stateDir = '/work/isolated-relay-state'

    const handle = await ensureRelayBroker({
      cwd: '/work',
      connect: () => { throw new Error('No running broker found') },
      spawn,
      env: { AGENT_RELAY_STATE_DIR: stateDir },
      resolveWorkspaceKey: noStoredWorkspaceKey,
    })

    expect(handle.client).toBe(spawned)
    expect(handle.started).toBe(true)
    expect(spawn).toHaveBeenCalledWith({
      cwd: '/work',
      workspaceKey: undefined,
      binaryArgs: { persist: true, stateDir },
    })
  })

  it('surfaces the connect error without spawning when autoStart is false', async () => {
    const connect = vi.fn(() => {
      throw new Error('No running broker found')
    })
    const spawn = vi.fn(async () => fakeClient('spawned'))

    await expect(ensureRelayBroker({ autoStart: false, connect, spawn })).rejects.toThrow('No running broker found')
    expect(spawn).not.toHaveBeenCalled()
  })

  it('logs which path it took', async () => {
    const info = vi.fn()
    await ensureRelayBroker({ connect: () => fakeClient('connected'), spawn: async () => fakeClient('spawned'), logger: { info } })
    expect(info).toHaveBeenCalledWith('[factory] reusing the relay broker that is already running')

    info.mockClear()
    await ensureRelayBroker({
      connect: () => {
        throw new Error('boom')
      },
      spawn: async () => fakeClient('spawned'),
      logger: { info },
      env: {},
      resolveWorkspaceKey: noStoredWorkspaceKey,
    })
    expect(info).toHaveBeenCalledWith('[factory] no relay broker running; starting one', { reason: 'boom', joiningWorkspace: false })
  })

  it('threads a workspace key (env or option) into spawn so the broker JOINS', async () => {
    const spawn = vi.fn(async () => fakeClient('spawned'))
    const envHandle = await ensureRelayBroker({ connect: () => { throw new Error('no broker') }, spawn, env: { RELAY_WORKSPACE_KEY: 'rk_live_test' } })
    expect(envHandle.workspaceKey).toBe('rk_live_test')
    expect(spawn).toHaveBeenCalledWith(expect.objectContaining({ workspaceKey: 'rk_live_test' }))

    spawn.mockClear()
    const explicitHandle = await ensureRelayBroker({ connect: () => { throw new Error('no broker') }, spawn, workspaceKey: 'rk_live_explicit', env: {} })
    expect(explicitHandle.workspaceKey).toBe('rk_live_explicit')
    expect(spawn).toHaveBeenCalledWith(expect.objectContaining({ workspaceKey: 'rk_live_explicit' }))
  })

  it('uses the active Agent Relay workspace key when env is empty', async () => {
    const spawn = vi.fn(async () => fakeClient('spawned'))
    const handle = await ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn,
      env: {},
      resolveWorkspaceKey: () => 'rk_live_stored',
    })

    expect(handle.workspaceKey).toBe('rk_live_stored')
    expect(spawn).toHaveBeenCalledWith(expect.objectContaining({ workspaceKey: 'rk_live_stored' }))
  })

  it('returns the resolved workspace key when reusing an existing broker', async () => {
    const connected = fakeClient('connected')
    const handle = await ensureRelayBroker({
      connect: () => connected,
      spawn: async () => fakeClient('spawned'),
      env: {},
      resolveWorkspaceKey: () => 'rk_live_reused',
    })

    expect(handle.client).toBe(connected)
    expect(handle.started).toBe(false)
    expect(handle.workspaceKey).toBe('rk_live_reused')
  })

  it('waits for a spawned broker cloud node before returning it', async () => {
    const client = fakeClient('spawned')
    const getStatus = vi.fn()
      .mockResolvedValueOnce({ node_delivery: { connected: false } })
      .mockResolvedValueOnce({ node_delivery: { connected: true } })
    client.getStatus = getStatus

    const handle = await ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn: async () => client,
      env: { RELAY_WORKSPACE_KEY: 'rk_live_test' },
      sleep: async () => {},
    })

    expect(handle.client).toBe(client)
    expect(handle.started).toBe(true)
    expect(getStatus).toHaveBeenCalledTimes(2)
  })

  it('spawns at the project root selected by the connection boundary', async () => {
    const spawn = vi.fn(async () => fakeClient('spawned'))

    await ensureRelayBroker({
      cwd: '/work/packages/sdk',
      connectionPath: '/work/.agentworkforce/relay/connection.json',
      connect: () => { throw new Error('no broker') },
      spawn,
      env: { RELAY_WORKSPACE_KEY: 'rk_live_test' },
    })

    expect(spawn).toHaveBeenCalledWith({
      cwd: '/work',
      workspaceKey: 'rk_live_test',
      binaryArgs: {
        persist: true,
        stateDir: '/work/.agentworkforce/relay',
      },
    })
  })

  it('accepts a legacy or malformed empty broker status defensively', async () => {
    const client = fakeClient('connected')
    client.getStatus = vi.fn(async () => undefined)

    await expect(ensureRelayBroker({ connect: () => client }))
      .resolves.toMatchObject({ client, started: false })
  })

  it('does not start a competing broker when a reachable broker has no cloud delivery', async () => {
    const client = fakeClient('connected')
    client.getStatus = vi.fn(async () => ({ node_delivery: { connected: false } }))
    client.disconnect = vi.fn()
    const spawn = vi.fn(async () => fakeClient('spawned'))

    await expect(ensureRelayBroker({
      connect: () => client,
      spawn,
      nodeDeliveryTimeoutMs: 0,
    })).rejects.toThrow(/cloud node delivery/u)

    expect(spawn).not.toHaveBeenCalled()
    expect(client.disconnect).toHaveBeenCalledOnce()
  })

  it('shuts down a broker it started when cloud delivery never becomes ready', async () => {
    const client = fakeClient('spawned')
    client.getStatus = vi.fn(async () => ({ node_delivery: { connected: false } }))
    client.shutdown = vi.fn(async () => {})

    await expect(ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn: async () => client,
      env: { RELAY_WORKSPACE_KEY: 'rk_live_test' },
      nodeDeliveryTimeoutMs: 0,
    })).rejects.toThrow(/cloud node delivery/u)

    expect(client.shutdown).toHaveBeenCalledOnce()
  })

  it('preserves the node-delivery error when spawned-broker cleanup also fails', async () => {
    const client = fakeClient('spawned')
    client.getStatus = vi.fn(async () => ({ node_delivery: { connected: false } }))
    client.shutdown = vi.fn(async () => { throw new Error('cleanup failed') })
    const warn = vi.fn()

    await expect(ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn: async () => client,
      env: { RELAY_WORKSPACE_KEY: 'rk_live_test' },
      nodeDeliveryTimeoutMs: 0,
      logger: { warn },
    })).rejects.toThrow(/cloud node delivery/u)

    expect(warn).toHaveBeenCalledWith(
      '[factory] failed to shut down the spawned relay broker during cleanup',
      { errorClass: 'Error' },
    )
  })

  it('fails with actionable guidance when there is no broker and no workspace key', async () => {
    await expect(ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn: async () => { throw new Error('insert into workspaces failed') },
      env: {},
      resolveWorkspaceKey: noStoredWorkspaceKey,
    })).rejects.toThrow(/RELAY_WORKSPACE_KEY/u)
  })
})
