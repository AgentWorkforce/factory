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

  it('fails with actionable guidance when there is no broker and no workspace key', async () => {
    await expect(ensureRelayBroker({
      connect: () => { throw new Error('no broker') },
      spawn: async () => { throw new Error('insert into workspaces failed') },
      env: {},
      resolveWorkspaceKey: noStoredWorkspaceKey,
    })).rejects.toThrow(/RELAY_WORKSPACE_KEY/u)
  })
})
