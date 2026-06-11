import { describe, expect, it } from 'vitest'

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
}

describe('createFleet', () => {
  it('defaults to the internal backend', () => {
    expect(createFleet(undefined, { harnessClient: fakeHarness })).toBeInstanceOf(InternalFleetClient)
  })

  it('returns the internal backend explicitly', () => {
    expect(createFleet({ backend: 'internal' }, { harnessClient: fakeHarness })).toBeInstanceOf(InternalFleetClient)
  })

  it('returns the relay seam stub for the relay backend', async () => {
    const fleet = createFleet({ backend: 'relay' })

    expect(fleet).toBeInstanceOf(RelayFleetClient)
    await expect(fleet.roster()).rejects.toThrow('relay#1056')
  })
})
