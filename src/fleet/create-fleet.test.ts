import { afterEach, describe, expect, it, vi } from 'vitest'

import { createFleet } from './create-fleet'
import { InternalFleetClient } from './internal-fleet-client'
import { RelayFleetClient } from './relay-fleet-client'
import type { HarnessDriverClientLike } from './internal-fleet-client'

const fakeHarness: HarnessDriverClientLike = {
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
}

describe('createFleet', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('defaults to the internal backend', () => {
    expect(createFleet(undefined, { harnessClient: fakeHarness })).toBeInstanceOf(InternalFleetClient)
  })

  it('returns the internal backend explicitly', () => {
    expect(createFleet({ backend: 'internal' }, { harnessClient: fakeHarness })).toBeInstanceOf(InternalFleetClient)
  })

  it('forwards the owned-broker agent exit timeout to the internal client', async () => {
    vi.useFakeTimers()
    try {
      vi.stubEnv('FACTORY_AGENT_EXIT_TIMEOUT_MS', '60')
      const shutdown = vi.fn()
      const harness = { ...fakeHarness, shutdown }
      const fleet = createFleet(
        { backend: 'internal' },
        { harnessClient: harness, ownsBroker: true, ownedBrokerAgentExitTimeoutMs: 25 },
      )
      await fleet.spawn({ name: 'ar-59-hung', capability: 'spawn:codex' })

      const disposed = fleet.dispose()
      await vi.advanceTimersByTimeAsync(24)
      expect(shutdown).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(1)
      await disposed
      expect(shutdown).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('uses the agent exit timeout environment override for direct createFleet callers', async () => {
    vi.useFakeTimers()
    try {
      vi.stubEnv('FACTORY_AGENT_EXIT_TIMEOUT_MS', '60000')
      const shutdown = vi.fn()
      const harness = { ...fakeHarness, shutdown }
      const fleet = createFleet(
        { backend: 'internal' },
        { harnessClient: harness, ownsBroker: true },
      )
      await fleet.spawn({ name: 'ar-59-env-timeout', capability: 'spawn:codex' })

      const disposed = fleet.dispose()
      await vi.advanceTimersByTimeAsync(59_999)
      expect(shutdown).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(1)
      await disposed
      expect(shutdown).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('constructs the relay fleet client without throwing even when no token env is configured', () => {
    // Strip every credential the transport would read so the test does not
    // depend on ambient host env (e.g. a developer's RELAY_API_KEY).
    vi.stubEnv('RELAY_AGENT_TOKEN', '')
    vi.stubEnv('RELAY_WORKSPACE_KEY', '')
    vi.stubEnv('RELAY_API_KEY', '')

    let fleet: RelayFleetClient | undefined
    expect(() => {
      fleet = createFleet({ backend: 'relay' }) as RelayFleetClient
    }).not.toThrow()
    expect(fleet).toBeInstanceOf(RelayFleetClient)
  })

  it('surfaces the relay token error on first transport use, not at construction', async () => {
    vi.stubEnv('RELAY_AGENT_TOKEN', '')
    vi.stubEnv('RELAY_WORKSPACE_KEY', '')
    vi.stubEnv('RELAY_API_KEY', '')

    const fleet = createFleet({ backend: 'relay' })

    await expect(fleet.roster()).rejects.toThrow(/requires agentToken or workspaceKey/)
  })
})
