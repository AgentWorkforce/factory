import type { FleetClient, RosterEntry, SpawnInput, SpawnResult } from '../ports/fleet'

export const DEFAULT_FLEET_ROSTER_TIMEOUT_MS = 5_000
export const DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD = 2
export const DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS = 60_000

export type FleetControlPlaneState = 'closed' | 'open' | 'half-open'

export interface FleetControlPlaneStatus {
  state: FleetControlPlaneState
  consecutiveFailures: number
  timeoutMs: number
  failureThreshold: number
  resetTimeoutMs: number
  lastFailureAtMs?: number
  retryAtMs?: number
  lastError?: string
}

export interface FleetControlPlaneCircuitOptions {
  timeoutMs: number
  failureThreshold: number
  resetTimeoutMs: number
  now?: () => number
}

export class FleetControlPlaneTimeoutError extends Error {
  readonly code = 'FACTORY_FLEET_CONTROL_TIMEOUT'

  constructor(readonly timeoutMs: number) {
    super(`fleet control-plane roster probe timed out after ${timeoutMs}ms`)
    this.name = 'TimeoutError'
  }
}

export class FleetControlPlaneCircuitOpenError extends Error {
  readonly code = 'FACTORY_FLEET_CONTROL_CIRCUIT_OPEN'

  constructor(readonly retryAtMs: number, readonly state: 'open' | 'half-open' = 'open') {
    super(state === 'open'
      ? `fleet control-plane circuit is open until ${new Date(retryAtMs).toISOString()}`
      : 'fleet control-plane circuit requires a successful roster probe before dispatch')
    this.name = 'FleetControlPlaneCircuitOpenError'
  }
}

/**
 * Bounds the read-only roster probe and prevents new worker mutations after
 * repeated control-plane failures. Mutating operations are deliberately not
 * raced against a local timer: abandoning a spawn after its side effect has
 * reached the broker would create an ambiguous orphan.
 */
export class FleetControlPlaneCircuit {
  readonly #timeoutMs: number
  readonly #failureThreshold: number
  readonly #resetTimeoutMs: number
  readonly #now: () => number
  #consecutiveFailures = 0
  #lastFailureAtMs?: number
  #retryAtMs?: number
  #lastError?: string
  #probeInFlight?: Promise<RosterEntry>

  constructor(options: FleetControlPlaneCircuitOptions) {
    this.#timeoutMs = options.timeoutMs
    this.#failureThreshold = options.failureThreshold
    this.#resetTimeoutMs = options.resetTimeoutMs
    this.#now = options.now ?? Date.now
  }

  /** Returns the current admission state without performing broker I/O. */
  status(): FleetControlPlaneStatus {
    const state: FleetControlPlaneState = this.#consecutiveFailures < this.#failureThreshold
      ? 'closed'
      : this.#retryAtMs !== undefined && this.#now() < this.#retryAtMs
        ? 'open'
        : 'half-open'
    return {
      state,
      consecutiveFailures: this.#consecutiveFailures,
      timeoutMs: this.#timeoutMs,
      failureThreshold: this.#failureThreshold,
      resetTimeoutMs: this.#resetTimeoutMs,
      ...(this.#lastFailureAtMs === undefined ? {} : { lastFailureAtMs: this.#lastFailureAtMs }),
      ...(this.#retryAtMs === undefined ? {} : { retryAtMs: this.#retryAtMs }),
      ...(this.#lastError === undefined ? {} : { lastError: this.#lastError }),
    }
  }

  /** Runs or joins one bounded roster request, recording only its outcome. */
  async probe(roster: () => Promise<RosterEntry>): Promise<RosterEntry> {
    const status = this.status()
    if (status.state === 'open') {
      throw new FleetControlPlaneCircuitOpenError(status.retryAtMs!)
    }
    if (this.#probeInFlight) return this.#probeInFlight

    const probe = withTimeout(roster, this.#timeoutMs)
      .then((result) => {
        this.#recordSuccess()
        return result
      })
      .catch((error: unknown) => {
        this.#recordFailure(error)
        throw error
      })
      .finally(() => {
        if (this.#probeInFlight === probe) this.#probeInFlight = undefined
      })
    this.#probeInFlight = probe
    return probe
  }

  /** Rejects mutations until an open or half-open circuit has recovered. */
  assertMutationAllowed(): void {
    const status = this.status()
    if (status.state === 'closed') return
    throw new FleetControlPlaneCircuitOpenError(status.retryAtMs ?? this.#now(), status.state)
  }

  #recordFailure(error: unknown): void {
    const now = this.#now()
    const wasOpen = this.#retryAtMs !== undefined && now < this.#retryAtMs
    this.#lastFailureAtMs = now
    this.#lastError = describeControlPlaneError(error)
    if (wasOpen) return
    this.#consecutiveFailures += 1
    if (this.#consecutiveFailures >= this.#failureThreshold) {
      this.#retryAtMs = now + this.#resetTimeoutMs
    }
  }

  #recordSuccess(): void {
    this.#consecutiveFailures = 0
    this.#retryAtMs = undefined
    this.#lastError = undefined
  }
}

/**
 * Gates roster, spawn, and resume through the read-only roster control path.
 * Mutation rejections deliberately do not affect circuit state: a transport
 * error can arrive after the remote side effect committed, so only a fresh,
 * bounded roster probe is allowed to decide subsequent admission.
 */
export function guardFleetControlPlane(
  fleet: FleetClient,
  circuit: FleetControlPlaneCircuit,
): FleetClient {
  const guardedMutation = async <T>(operation: () => Promise<T>): Promise<T> => {
    // A fresh Factory instance starts with a closed circuit, so checking state
    // alone would let resume/cold-start paths that did not run discovery bypass
    // admission. Probe the same roster path before every spawn/resume.
    // Concurrent mutations coalesce onto one in-flight probe, and an open
    // circuit rejects here without calling either roster or the mutation.
    await circuit.probe(() => fleet.roster())
    circuit.assertMutationAllowed()
    // Do not catch or classify operation failures here. Their remote outcome
    // can be ambiguous; the next mutation must run a new roster admission probe.
    return await operation()
  }

  return new Proxy(fleet, {
    get(target, property) {
      if (property === 'roster') {
        return (): Promise<RosterEntry> => circuit.probe(() => target.roster())
      }
      if (property === 'spawn') {
        return (input: SpawnInput): Promise<SpawnResult> => guardedMutation(() => target.spawn(input))
      }
      if (property === 'resume') {
        return (input: Parameters<FleetClient['resume']>[0]): Promise<SpawnResult> =>
          guardedMutation(() => target.resume(input))
      }
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  }) as FleetClient
}

/** Redacts arbitrary transport text before circuit state becomes observable. */
function describeControlPlaneError(error: unknown): string {
  if (!(error instanceof Error)) return 'unknown control-plane failure'
  const code = (error as Error & { code?: unknown }).code
  const safeCode = typeof code === 'string' && /^[A-Z0-9_]{1,80}$/u.test(code) ? ` (${code})` : ''
  // This value is exposed through `factory status`; do not persist arbitrary
  // transport messages because they may contain URLs or credential material.
  return `${error.name || 'Error'}${safeCode}`
}

/** Applies the local roster deadline without imposing a timeout on mutations. */
function withTimeout<T>(operation: () => Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false
    const timer = setTimeout(() => {
      if (settled) return
      settled = true
      reject(new FleetControlPlaneTimeoutError(timeoutMs))
    }, timeoutMs)

    Promise.resolve()
      .then(operation)
      .then(
        (value) => {
          if (settled) return
          settled = true
          clearTimeout(timer)
          resolve(value)
        },
        (error: unknown) => {
          if (settled) return
          settled = true
          clearTimeout(timer)
          reject(error)
        },
      )
  })
}
