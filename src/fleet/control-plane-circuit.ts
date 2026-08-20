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
  // A roster request that predates an open transition is not a valid
  // half-open recovery probe, even if it resolves after the cooldown.
  #openGeneration = 0
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

    const openGeneration = this.#openGeneration
    const probe = withTimeout(roster, this.#timeoutMs)
      .catch((error: unknown) => {
        this.recordFailure(error)
        // The failure that trips the threshold IS the open transition, but the
        // transport error it arrives as says nothing about that. Callers that
        // saw only the original error could not tell "one roster request
        // failed" from "dispatch is now globally paused" — factory#292 —
        // without re-reading status() after every rejection. Name the
        // transition here, the same way the two branches above already do, and
        // keep the original as `cause` for diagnostics.
        const settled = this.status()
        if (settled.state === 'closed') throw error
        const opened = new FleetControlPlaneCircuitOpenError(
          settled.retryAtMs ?? this.#now(),
          settled.state,
        )
        ;(opened as Error & { cause?: unknown }).cause = error
        throw opened
      })
      .then((result) => {
        const settledStatus = this.status()
        // Mutation failures can open the circuit while this read is pending.
        // Never let that stale result satisfy waiters or reset circuit state.
        if (settledStatus.state === 'open' || openGeneration !== this.#openGeneration) {
          throw new FleetControlPlaneCircuitOpenError(
            settledStatus.retryAtMs ?? this.#now(),
            settledStatus.state === 'open' ? 'open' : 'half-open',
          )
        }
        this.#recordSuccess()
        return result
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

  recordFailure(error: unknown): void {
    const now = this.#now()
    const wasOpen = this.#retryAtMs !== undefined && now < this.#retryAtMs
    this.#lastFailureAtMs = now
    this.#lastError = describeControlPlaneError(error)
    if (wasOpen) return
    this.#consecutiveFailures += 1
    if (this.#consecutiveFailures >= this.#failureThreshold) {
      this.#retryAtMs = now + this.#resetTimeoutMs
      this.#openGeneration += 1
    }
  }

  #recordSuccess(): void {
    this.#consecutiveFailures = 0
    this.#retryAtMs = undefined
    this.#lastError = undefined
  }
}

/**
 * Gates spawn and resume with a bounded read-only roster admission. Transport
 * failures from admitted mutations also count toward the circuit without
 * abandoning or timing the mutation; domain rejections remain uncounted.
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
    try {
      return await operation()
    } catch (error) {
      if (isFleetControlPlaneFailure(error)) circuit.recordFailure(error)
      throw error
    }
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

export function isFleetControlPlaneFailure(error: unknown): boolean {
  if (!(error instanceof Error)) return false
  const candidate = error as Error & { code?: unknown }
  if (error.name === 'TimeoutError' || error.name === 'AbortError') return true
  if (typeof candidate.code === 'string' && [
    'ECONNREFUSED',
    'ECONNRESET',
    'EPIPE',
    'ETIMEDOUT',
    'UND_ERR_CONNECT_TIMEOUT',
  ].includes(candidate.code)) return true
  return /(?:operation\s+timed\s+out|timed\s+out\s+waiting\s+for\b.*\binvocation\b.*\bto\s+complete|operation\s+was\s+aborted|no\s+running\s+broker|broker\s+unavailable|socket\s+hang\s+up)/iu
    .test(error.message)
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
