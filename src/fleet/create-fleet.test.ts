import { afterEach, describe, expect, it, vi } from 'vitest'

import { createFleet } from './create-fleet'
import { FactoryConfigSchema } from '../config/schema'
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

const baseConfig = {
  workspaceId: 'ws_create_fleet_test',
  repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
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
    let fleet: RelayFleetClient | undefined
    expect(() => {
      // A hermetic env keeps the test independent of ambient host credentials
      // (a developer's RELAY_WORKSPACE_KEY or cloud workspace store).
      fleet = createFleet({ backend: 'relay' }, { env: {} }) as RelayFleetClient
    }).not.toThrow()
    expect(fleet).toBeInstanceOf(RelayFleetClient)
  })

  it('surfaces the relay credential error on first use, not at construction', async () => {
    const fleet = createFleet({ backend: 'relay' }, { env: {} })

    await expect(fleet.roster()).rejects.toThrow(/requires a workspace key \(rk_live_…\) or agent token \(at_live_…\)/)
  })

  // The identity the factory registers as, threaded config -> createFleet ->
  // RelayFleetClient. Both directions are asserted from a really-parsed config
  // so the schema, the option, and the client default stay in one story.
  describe('relay workspace identity', () => {
    const relayFleetFor = (raw: Record<string, unknown>): RelayFleetClient => {
      const config = FactoryConfigSchema.parse({ ...baseConfig, ...raw })
      return createFleet(
        { backend: 'relay', relayAgentName: config.relay.agentName },
        { env: {} },
      ) as RelayFleetClient
    }

    it('registers under the agent name the config supplies', () => {
      expect(relayFleetFor({ relay: { agentName: 'factory-cloud' } }).agentName).toBe('factory-cloud')
    })

    // Guards the upgrade path, not the feature: a deployment that never sets
    // `relay.agentName` must keep registering the exact name it registers
    // today. A silent identity change here strands a live deployment the same
    // way an unconfigurable identity did.
    it('keeps the built-in `factory` identity when the config omits an agent name', () => {
      const config = FactoryConfigSchema.parse(baseConfig)

      expect(config.relay.agentName).toBeUndefined()
      expect(relayFleetFor({}).agentName).toBe('factory')
    })
  })
})
