import { afterEach, describe, expect, it, vi } from 'vitest'

import type { RosterEntry } from '../ports/fleet'
import { FakeFleetClient } from '../testing/fakes'
import {
  DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
  DEFAULT_FLEET_CONTROL_ADMISSION_LEASE_MS,
  DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
  DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
  FleetControlPlaneCircuit,
  FleetControlPlaneCircuitOpenError,
  describeControlPlaneError,
  guardFleetControlPlane,
  isFleetControlPlaneFailure,
} from './control-plane-circuit'

const roster: RosterEntry = { agents: [], nodes: [] }

describe('describeControlPlaneError', () => {
  it.each([
    'request failed for https://relay.invalid/events?credential=example-secret',
    'authorization failed for Bearer example.invalid.token',
    'could not connect to postgres://example-user:example-password@db.invalid/factory',
  ])('redacts transport text from an Error message', (message) => {
    const error = new Error(message)
    error.name = 'FleetTransportError'

    expect(describeControlPlaneError(error)).toBe('FleetTransportError')
  })

  it('appends a well-formed error code', () => {
    const error = Object.assign(new Error('transport details'), { code: 'ECONNREFUSED_42' })

    expect(describeControlPlaneError(error)).toBe('Error (ECONNREFUSED_42)')
  })

  it.each([
    'connection_refused',
    'CONNECTION-REFUSED',
    'A'.repeat(81),
  ])('drops a malformed error code', (code) => {
    const error = Object.assign(new Error('transport details'), { code })

    expect(describeControlPlaneError(error)).toBe('Error')
  })
})

describe('FleetControlPlaneCircuit', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('MUST FIRE: two 5s roster timeouts open for 60s and only a successful half-open probe closes', async () => {
    vi.useFakeTimers()
    let now = 1_000
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
      now: () => now,
    })
    const never = vi.fn(() => new Promise<RosterEntry>(() => undefined))

    const first = circuit.probe(never)
    const firstFailure = expect(first).rejects.toMatchObject({
      name: 'TimeoutError',
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
    })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS - 1)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
    await vi.advanceTimersByTimeAsync(1)
    await firstFailure
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    // The threshold-crossing failure rejects as the open transition, not as
    // the bare timeout that caused it (factory#292); the timeout stays on
    // `cause` so the diagnostic is not lost.
    const second = circuit.probe(never)
    const secondFailure = expect(second).rejects.toMatchObject({
      name: 'FleetControlPlaneCircuitOpenError',
      cause: expect.objectContaining({ name: 'TimeoutError' }),
    })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS)
    await secondFailure
    expect(circuit.status()).toMatchObject({ state: 'open', consecutiveFailures: 2, retryAtMs: 61_000 })

    await expect(circuit.probe(never)).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(never).toHaveBeenCalledTimes(2)
    now += DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS - 1
    expect(circuit.status().state).toBe('open')
    await expect(circuit.probe(never)).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(never).toHaveBeenCalledTimes(2)

    now += 1
    expect(circuit.status().state).toBe('half-open')
    await expect(circuit.probe(async () => roster)).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('MUST NOT FIRE: a slow 4.999s probe and one isolated 5s failure leave the circuit closed', async () => {
    vi.useFakeTimers()
    let resolveProbe: ((value: RosterEntry) => void) | undefined
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
    })
    const slow = vi.fn(() => new Promise<RosterEntry>((resolve) => { resolveProbe = resolve }))

    const slowProbe = circuit.probe(slow)
    await Promise.resolve()
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS - 1)
    resolveProbe?.(roster)
    await expect(slowProbe).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })

    const isolated = circuit.probe(() => new Promise<RosterEntry>(() => undefined))
    const isolatedFailure = expect(isolated).rejects.toMatchObject({ name: 'TimeoutError' })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS)
    await isolatedFailure
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    await expect(circuit.probe(async () => roster)).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  // factory#292: the failure that trips the threshold IS the open transition,
  // but it arrives as an ordinary transport error. A caller that classifies by
  // error type — a dispatcher deciding whether one work unit's failure should
  // abort the whole pass — cannot tell the two apart unless probe() names it.
  it('MUST FIRE: the failure that trips the threshold rejects as circuit-open, keeping the cause', async () => {
    const transport = Object.assign(new TypeError('fetch failed'), { code: 'ECONNREFUSED' })
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 1, resetTimeoutMs: 1_000 })

    const rejection = await circuit.probe(async () => { throw transport }).catch((error: unknown) => error)

    expect(rejection).toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect((rejection as { code?: unknown }).code).toBe('FACTORY_FLEET_CONTROL_CIRCUIT_OPEN')
    expect((rejection as { cause?: unknown }).cause).toBe(transport)
    expect(circuit.status()).toMatchObject({ state: 'open', consecutiveFailures: 1 })
  })

  // MUST NOT FIRE control for the above: a failure that leaves the circuit
  // closed is one unit's problem and must keep its own identity, or a caller
  // would treat every transient fault as a global pause.
  it('MUST NOT FIRE: a failure below the threshold still rejects with the original error', async () => {
    const transport = Object.assign(new TypeError('fetch failed'), { code: 'ECONNREFUSED' })
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })

    const rejection = await circuit.probe(async () => { throw transport }).catch((error: unknown) => error)

    expect(rejection).toBe(transport)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })
  })

  it('coalesces a rejected probe, rejects every waiter, and does not cache the failure', async () => {
    const sharedError = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' })
    let rejectProbe: ((error: Error) => void) | undefined
    const call = vi.fn()
      .mockImplementationOnce(() => new Promise<RosterEntry>((_resolve, reject) => { rejectProbe = reject }))
      .mockResolvedValueOnce(roster)
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })
    const first = circuit.probe(call)
    const second = circuit.probe(call)
    await Promise.resolve()
    rejectProbe?.(sharedError)

    const results = await Promise.allSettled([first, second])
    expect(results).toEqual([
      { status: 'rejected', reason: sharedError },
      { status: 'rejected', reason: sharedError },
    ])
    expect(call).toHaveBeenCalledTimes(1)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    await expect(circuit.probe(call)).resolves.toEqual(roster)
    expect(call).toHaveBeenCalledTimes(2)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('MUST FIRE: concurrent mutation faults cannot let a pre-open roster probe bypass or close the circuit', async () => {
    let now = 1_000
    let rejectSpawn: ((error: Error) => void) | undefined
    let rejectResume: ((error: Error) => void) | undefined
    let resolveStaleProbe: ((value: RosterEntry) => void) | undefined
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
    const spawn = vi.spyOn(fleet, 'spawn')
      .mockImplementation(() => new Promise((_resolve, reject) => { rejectSpawn = reject }))
    const resume = vi.spyOn(fleet, 'resume')
      .mockImplementation(() => new Promise((_resolve, reject) => { rejectResume = reject }))
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
      now: () => now,
    })
    const guarded = guardFleetControlPlane(fleet, circuit)

    const spawning = guarded.spawn({ name: 'worker-1', capability: 'spawn:codex' })
    await vi.waitFor(() => { expect(spawn).toHaveBeenCalledTimes(1) })
    const resuming = guarded.resume({ name: 'worker-2', sessionRef: 'session-2' })
    await vi.waitFor(() => { expect(resume).toHaveBeenCalledTimes(1) })

    rosterProbe.mockImplementationOnce(() => new Promise<RosterEntry>((resolve) => { resolveStaleProbe = resolve }))
    const staleProbe = guarded.roster()
    await vi.waitFor(() => { expect(resolveStaleProbe).toBeTypeOf('function') })

    rejectSpawn?.(new Error('Timed out waiting for spawn invocation inv-spawn to complete (last status: pending)'))
    rejectResume?.(new Error('Timed out waiting for resume invocation inv-resume to complete (last status: pending)'))
    await Promise.allSettled([spawning, resuming])
    const openedBeforeStaleProbeSettled = circuit.status()

    let laterSettled = false
    let laterError: unknown
    const laterProbe = guarded.roster().then(
      (result) => {
        laterSettled = true
        return result
      },
      (error: unknown) => {
        laterSettled = true
        laterError = error
        throw error
      },
    )
    void laterProbe.catch(() => undefined)
    await vi.waitFor(() => { expect(laterSettled).toBe(true) })
    const laterFailedFast = laterSettled && laterError instanceof FleetControlPlaneCircuitOpenError

    now += DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS
    resolveStaleProbe?.(roster)
    const outcomes = await Promise.allSettled([staleProbe, laterProbe])

    expect(openedBeforeStaleProbeSettled).toMatchObject({
      state: 'open',
      consecutiveFailures: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      retryAtMs: 61_000,
    })
    expect(laterFailedFast).toBe(true)
    expect(outcomes.map((outcome) => outcome.status)).toEqual(['rejected', 'rejected'])
    expect(circuit.status()).toMatchObject({ state: 'half-open', consecutiveFailures: 2 })

    rosterProbe.mockResolvedValueOnce(roster)
    await expect(guarded.roster()).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('does not start a mutation until the shared admission probe has completed', async () => {
    let resolveProbe: ((value: RosterEntry) => void) | undefined
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
      .mockImplementation(() => new Promise<RosterEntry>((resolve) => { resolveProbe = resolve }))
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })
    const guarded = guardFleetControlPlane(fleet, circuit)

    const spawning = guarded.spawn({ name: 'pending-worker', capability: 'spawn:codex' })
    await Promise.resolve()
    expect(rosterProbe).toHaveBeenCalledTimes(1)
    expect(fleet.spawns).toEqual([])

    resolveProbe?.(roster)
    await expect(spawning).resolves.toMatchObject({ name: 'pending-worker' })
    expect(fleet.spawns).toHaveLength(1)
  })

  it('reuses a successful placement as admission evidence for the adjacent team member', async () => {
    let now = 1_000
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
      .mockResolvedValueOnce(roster)
      // This is the production failure shape: the first worker placed, then a
      // repeated fleet-node inventory exceeded the 5s roster boundary before
      // the reviewer could start.
      .mockImplementationOnce(() => new Promise<RosterEntry>(() => undefined))
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
      now: () => now,
    })
    const guarded = guardFleetControlPlane(fleet, circuit, { now: () => now })

    await expect(guarded.spawn({ name: 'implementer', capability: 'spawn:codex' }))
      .resolves.toMatchObject({ name: 'implementer' })
    await expect(guarded.spawn({ name: 'reviewer', capability: 'spawn:codex' }))
      .resolves.toMatchObject({ name: 'reviewer' })

    expect(rosterProbe).toHaveBeenCalledTimes(1)
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['implementer', 'reviewer'])
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })

    // The lease is deliberately finite. Once it expires, the very same next
    // mutation must run its own roster admission and surface the stalled read.
    vi.useFakeTimers()
    now += DEFAULT_FLEET_CONTROL_ADMISSION_LEASE_MS + 1
    const expired = guarded.spawn({ name: 'later-worker', capability: 'spawn:codex' })
    const expiredFailure = expect(expired).rejects.toMatchObject({ name: 'TimeoutError' })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS)
    await expiredFailure
    expect(rosterProbe).toHaveBeenCalledTimes(2)
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['implementer', 'reviewer'])
  })

  it('invalidates recent admission evidence when a direct roster read fails', async () => {
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
      .mockResolvedValueOnce(roster)
      .mockRejectedValueOnce(new Error('broker unavailable'))
      .mockResolvedValueOnce(roster)
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })
    const guarded = guardFleetControlPlane(fleet, circuit)

    await expect(guarded.spawn({ name: 'first-worker', capability: 'spawn:codex' })).resolves.toBeDefined()
    await expect(guarded.roster()).rejects.toThrow('broker unavailable')
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    await expect(guarded.spawn({ name: 'second-worker', capability: 'spawn:codex' })).resolves.toBeDefined()
    expect(rosterProbe).toHaveBeenCalledTimes(3)
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['first-worker', 'second-worker'])
  })

  it('does not let unrelated successful roster reads extend the admission lease', async () => {
    let now = 1_000
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster').mockResolvedValue(roster)
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 2, resetTimeoutMs: 1_000 })
    const guarded = guardFleetControlPlane(fleet, circuit, { admissionLeaseMs: 100, now: () => now })

    await expect(guarded.spawn({ name: 'first-worker', capability: 'spawn:codex' })).resolves.toBeDefined()
    now += 99
    await expect(guarded.roster()).resolves.toEqual(roster)
    now += 2
    await expect(guarded.spawn({ name: 'second-worker', capability: 'spawn:codex' })).resolves.toBeDefined()

    expect(rosterProbe).toHaveBeenCalledTimes(3)
    expect(fleet.spawns.map((spawn) => spawn.name)).toEqual(['first-worker', 'second-worker'])
  })

  it('lets a half-open mutation perform the roster probe that recovers the circuit', async () => {
    let now = 1_000
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster').mockResolvedValue(roster)
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: 100,
      failureThreshold: 2,
      resetTimeoutMs: 1_000,
      now: () => now,
    })
    const guarded = guardFleetControlPlane(fleet, circuit, { now: () => now })
    circuit.recordFailure(new Error('broker unavailable'))
    circuit.recordFailure(new Error('broker unavailable'))
    now += 1_000
    expect(circuit.status()).toMatchObject({ state: 'half-open', consecutiveFailures: 2 })

    await expect(guarded.spawn({ name: 'recovery-worker', capability: 'spawn:codex' }))
      .resolves.toMatchObject({ name: 'recovery-worker' })
    expect(rosterProbe).toHaveBeenCalledTimes(1)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('does not join a stale pre-open roster request during half-open recovery', async () => {
    let now = 1_000
    let resolveStaleProbe: ((value: RosterEntry) => void) | undefined
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
      .mockImplementationOnce(() => new Promise<RosterEntry>((resolve) => { resolveStaleProbe = resolve }))
      .mockResolvedValueOnce(roster)
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: 1_000,
      failureThreshold: 2,
      resetTimeoutMs: 100,
      now: () => now,
    })
    const guarded = guardFleetControlPlane(fleet, circuit, { now: () => now })

    const staleProbe = guarded.roster()
    await vi.waitFor(() => { expect(resolveStaleProbe).toBeTypeOf('function') })
    circuit.recordFailure(new Error('broker unavailable'))
    circuit.recordFailure(new Error('broker unavailable'))
    now += 100

    const recovering = guarded.spawn({ name: 'recovery-worker', capability: 'spawn:codex' })
    await vi.waitFor(() => { expect(rosterProbe).toHaveBeenCalledTimes(2) })
    await expect(recovering).resolves.toMatchObject({ name: 'recovery-worker' })
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })

    resolveStaleProbe?.(roster)
    await expect(staleProbe).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
  })

  it('does not let a stale pre-open rejection reopen a recovered circuit', async () => {
    let now = 1_000
    let rejectStaleProbe: ((reason: Error) => void) | undefined
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: 1_000,
      failureThreshold: 1,
      resetTimeoutMs: 100,
      now: () => now,
    })

    const staleProbe = circuit.probe(() => new Promise<RosterEntry>((_resolve, reject) => {
      rejectStaleProbe = reject
    }))
    await vi.waitFor(() => { expect(rejectStaleProbe).toBeTypeOf('function') })
    circuit.recordFailure(new Error('broker unavailable'))
    now += 100

    await expect(circuit.probe(async () => roster)).resolves.toEqual(roster)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })

    rejectStaleProbe?.(new Error('stale broker timeout'))
    await expect(staleProbe).rejects.toThrow('stale broker timeout')
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })

  it('MUST FIRE at mutation admission: two wedged rosters open the circuit and the next spawn fails fast', async () => {
    vi.useFakeTimers()
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
      .mockImplementation(() => new Promise<RosterEntry>(() => undefined))
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
    })
    const guarded = guardFleetControlPlane(fleet, circuit)

    const first = guarded.spawn({ name: 'worker-1', capability: 'spawn:codex' })
    const firstFailure = expect(first).rejects.toMatchObject({ name: 'TimeoutError' })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS)
    await firstFailure
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    // As above: the admission probe that opens the circuit reports the
    // transition, keeping the timeout as `cause`.
    const second = guarded.spawn({ name: 'worker-2', capability: 'spawn:codex' })
    const secondFailure = expect(second).rejects.toMatchObject({
      name: 'FleetControlPlaneCircuitOpenError',
      cause: expect.objectContaining({ name: 'TimeoutError' }),
    })
    await vi.advanceTimersByTimeAsync(DEFAULT_FLEET_ROSTER_TIMEOUT_MS)
    await secondFailure
    expect(circuit.status()).toMatchObject({ state: 'open', consecutiveFailures: 2 })

    await expect(guarded.spawn({ name: 'worker-3', capability: 'spawn:codex' }))
      .rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(rosterProbe).toHaveBeenCalledTimes(2)
    expect(fleet.spawns).toEqual([])
  })

  it('blocks spawn and resume while open without calling roster or either mutation', async () => {
    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 5, failureThreshold: 2, resetTimeoutMs: 60_000 })
    const fleet = new FakeFleetClient()
    const rosterProbe = vi.spyOn(fleet, 'roster')
    const guarded = guardFleetControlPlane(fleet, circuit)
    await expect(circuit.probe(async () => { throw new Error('broker unavailable') })).rejects.toThrow()
    await expect(circuit.probe(async () => { throw new Error('broker unavailable') })).rejects.toThrow()

    await expect(guarded.spawn({
      name: 'blocked-worker',
      capability: 'spawn:codex',
    })).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    await expect(guarded.resume({
      name: 'blocked-worker',
      sessionRef: 'session-blocked',
    })).rejects.toBeInstanceOf(FleetControlPlaneCircuitOpenError)
    expect(rosterProbe).not.toHaveBeenCalled()
    expect(fleet.spawns).toEqual([])
    expect(fleet.resumes).toEqual([])
  })

  it('recognizes and sanitizes transport failures without classifying domain errors', async () => {
    expect(isFleetControlPlaneFailure(Object.assign(new Error('connect failed'), { code: 'ECONNREFUSED' }))).toBe(true)
    expect(isFleetControlPlaneFailure(Object.assign(new Error('aborted'), { name: 'AbortError' }))).toBe(true)
    expect(isFleetControlPlaneFailure(
      new Error('Timed out waiting for spawn invocation inv-1 to complete (last status: pending)'),
    )).toBe(true)
    expect(isFleetControlPlaneFailure(new Error('agent already exists'))).toBe(false)

    const circuit = new FleetControlPlaneCircuit({ timeoutMs: 100, failureThreshold: 1, resetTimeoutMs: 1_000 })
    await expect(circuit.probe(async () => {
      throw Object.assign(new Error('https://broker.invalid?token=must-not-leak'), { code: 'ECONNREFUSED' })
    })).rejects.toThrow()
    expect(circuit.status().lastError).toBe('Error (ECONNREFUSED)')
  })

  it('MUST NOT FIRE: one isolated mutation fault stays closed and domain errors are not counted', async () => {
    const fleet = new FakeFleetClient()
    const transportError = new Error('Timed out waiting for spawn invocation inv-1 to complete (last status: pending)')
    const domainError = new Error('agent already exists')
    vi.spyOn(fleet, 'spawn').mockRejectedValueOnce(transportError)
    vi.spyOn(fleet, 'resume').mockRejectedValueOnce(domainError)
    const circuit = new FleetControlPlaneCircuit({
      timeoutMs: DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
      failureThreshold: DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
      resetTimeoutMs: DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
    })
    const guarded = guardFleetControlPlane(fleet, circuit)

    await expect(guarded.spawn({ name: 'worker-1', capability: 'spawn:codex' })).rejects.toBe(transportError)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 1 })

    await expect(guarded.resume({ name: 'worker-1', sessionRef: 'session-1' })).rejects.toBe(domainError)
    expect(circuit.status()).toMatchObject({ state: 'closed', consecutiveFailures: 0 })
  })
})
