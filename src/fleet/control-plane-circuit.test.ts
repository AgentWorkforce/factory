import { describe, expect, it, vi } from 'vitest'

import type { RosterEntry } from '../ports/fleet'
import { FakeFleetClient } from '../testing/fakes'
import {
  FleetControlPlaneCircuit,
  FleetControlPlaneCircuitOpenError,
  guardFleetControlPlane,
  isFleetControlPlaneFailure,
} from './control-plane-circuit'

const roster: RosterEntry = { agents: [], nodes: [] }

describe('FleetControlPlaneCircuit', () => {
  it('opens after bounded roster failures and rejects further probes without calling the broker', async () => {
    let now = 1_000
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: 5,
      failureThreshold: 2,
      resetTimeoutMs: 60_000,
      now: () => now,
    })
    const never = vi.fn(() => new Promise<RosterEntry>(() => undefined))

    await expect(circuit.probe(never)).rejects.toMatchObject({ name: 'TimeoutError' })
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })
    await expect(circuit.probe(never)).rejects.toMatchObject({ name: 'TimeoutError' })
    expect(circuit.status()).toMatchObject({ state: 'open', consecutiveFailures: 2, retryAtMs: 61_000 })

    await expect(circuit.probe(never)).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(never).toHaveBeenCalledTimes(2)
    now += 60_000
    await expect(circuit.probe(async () => roster)).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('coalesces concurrent roster probes', async () => {
    let resolveProbe: ((value: RosterEntry) => void) | undefined
    const call = vi.fn(() => new Promise<RosterEntry>((resolve) => { resolveProbe = resolve }))
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })
    const first = circuit.probe(call)
    const second = circuit.probe(call)
    await Promise.resolve()
    resolveProbe?.(roster)
    await expect(Promise.all([first, second])).resolves.toEqual([roster, roster])
    expect(call).toHaveBeenCalledTimes(1)
  })

  it('blocks new spawns while open without force-timing an accepted mutation', async () => {
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 5, failureThreshold: 1, resetTimeoutMs: 60_000 })
    const fleet = new FakeFleetClient()
    const guarded = guardFleetControlPlane(fleet, circuit)
    circuit.recordFailure(new Error('broker unavailable'))

    await expect(guarded.spawn({
      name: 'blocked-worker',
      capability: 'spawn:codex',
    })).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(fleet.spawns).toEqual([])
  })

  it('recognizes timeout and transport failures without classifying domain errors', () => {
    expect(isFleetControlPlaneFailure(Object.assign(new Error('connect failed'), { code: 'ECONNREFUSED' }))).toBe(true)
    expect(isFleetControlPlaneFailure(Object.assign(new Error('aborted'), { name: 'AbortError' }))).toBe(true)
    expect(isFleetControlPlaneFailure(new Error('agent already exists'))).toBe(false)

    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 1, resetTimeoutMs: 1_000 })
    circuit.recordFailure(Object.assign(new Error('https://broker.invalid?token=must-not-leak'), { code: 'ECONNREFUSED' }))
    expect(circuit.status().lastError).toBe('Error (ECONNREFUSED)')
  })
})
