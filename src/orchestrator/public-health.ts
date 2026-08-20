import { telemetryErrorClassName } from '../observability/error-class.js'
import type {
  FactoryEventListenerStatus,
  FactoryLoopHeartbeat,
  FactoryPublicEventListenerHealth,
  FactoryPublicHealth,
  FactoryPublicReadinessReconcileHealth,
  FactoryPublicSubsystemState,
  FactoryReadinessReconcileStatus,
} from '../types'

/**
 * Public health projection (#295).
 *
 * A deployed Factory's only unauthenticated surface is the container's
 * `/healthz`. Until now it carried subsystem *state strings* and nothing else,
 * so an operator could see `degraded` without learning how badly, since when,
 * or what class of failure — and could not see a wedged sweep at all, because
 * a hang writes no state. The fields that answer those questions live in the
 * loop heartbeat next to `lastError`, which is free text and must not be
 * published.
 *
 * This module is that boundary. It builds the public record **by
 * construction**: every field is named here, every number is coerced, every
 * string is either a closed enum or passes the shared telemetry allowlist.
 * Nothing is spread, so a field added upstream — or written by an older or
 * hostile producer — cannot reach the public surface by default.
 */
export const FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION = 1

/** Default readiness-reconcile cadence, mirrored from the live daemon. */
export const DEFAULT_READINESS_RECONCILE_INTERVAL_MS = 60_000

/**
 * How many missed passes make an in-flight sweep `stalled`.
 *
 * A cold container legitimately spends minutes in its first pass — #36
 * measured a post-boot reconcile at 61 minutes while the Relayfile mirror
 * hydrated — so a small multiple would cry wolf on every boot. Ten missed
 * passes (ten minutes at the default cadence) is past any warm-path sweep,
 * and `inFlightMs`/`missedPasses`/`lastCompletedAtMs` ship alongside so a
 * reader can still tell "first pass since boot, still hydrating" from "was
 * fine for hours, then stopped".
 */
export const READINESS_RECONCILE_STALL_INTERVALS = 10

/** Heartbeat age past which the whole record is treated as unknown, not green. */
export const DEFAULT_PUBLIC_HEALTH_STALE_MS = 60_000

const READINESS_RECONCILE_STATES: readonly FactoryPublicSubsystemState[] = [
  'not-running',
  'healthy',
  'retrying',
  'degraded',
  'stalled',
]

const EVENT_LISTENER_STATES: readonly FactoryEventListenerStatus['state'][] = [
  'not-listening',
  'starting',
  'subscribed',
  'polling',
]

/** Subsystems whose degradation stops issues from being dispatched. */
const DISPATCH_GATING_SUBSYSTEMS = ['readinessReconcile', 'eventListener'] as const

const finiteNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

const counter = (value: unknown): number => {
  const parsed = finiteNumber(value)
  return parsed !== undefined && parsed >= 0 ? Math.floor(parsed) : 0
}

const plainRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const optionalNumber = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = finiteNumber(value)
  return parsed === undefined ? {} : { [key]: parsed } as Partial<Record<K, number>>
}

/** Control characters stripped, length bounded: this text can reach a terminal. */
const boundedText = (value: string): string =>
  value.replace(/[\u0000-\u001F\u007F]+/gu, ' ').trim().slice(0, 300)

const enumValue = <T extends string>(value: unknown, allowed: readonly T[]): T | 'unknown' =>
  typeof value === 'string' && (allowed as readonly string[]).includes(value) ? value as T : 'unknown'

/**
 * How long the current pass has been running, or `undefined` when none is.
 *
 * A sweep that hangs takes neither the success nor the failure path, so no
 * field is written while it is stuck. The only evidence is that its start
 * timestamp is newer than both settle timestamps — which is why the *relative
 * order* of these three numbers, not any state string, is the signal.
 */
export function readinessReconcileInFlightMs(
  status: Pick<
    FactoryReadinessReconcileStatus,
    'lastStartedAtMs' | 'lastCompletedAtMs' | 'lastFailureAtMs'
  >,
  nowMs: number,
): number | undefined {
  const startedAtMs = finiteNumber(status.lastStartedAtMs)
  if (startedAtMs === undefined) return undefined
  const settledAtMs = Math.max(
    finiteNumber(status.lastCompletedAtMs) ?? Number.NEGATIVE_INFINITY,
    finiteNumber(status.lastFailureAtMs) ?? Number.NEGATIVE_INFINITY,
  )
  if (settledAtMs >= startedAtMs) return undefined
  return Math.max(0, nowMs - startedAtMs)
}

/**
 * The state a reader should believe, which is not always the one on record.
 *
 * `state` as persisted is last-write-wins over the last *settled* pass, so it
 * reports `healthy` for as long as a hang lasts. This re-derives it against
 * the clock: an in-flight pass past the stall threshold is `stalled` no matter
 * what the last completed pass said.
 */
export function derivedReadinessReconcileState(
  status: Pick<
    FactoryReadinessReconcileStatus,
    'state' | 'intervalMs' | 'lastStartedAtMs' | 'lastCompletedAtMs' | 'lastFailureAtMs'
  >,
  nowMs: number,
): FactoryPublicSubsystemState | 'unknown' {
  const reported = enumValue(status.state, READINESS_RECONCILE_STATES)
  // A daemon that is not running has no pass in flight; its own state wins.
  if (reported === 'not-running') return reported
  const inFlightMs = readinessReconcileInFlightMs(status, nowMs)
  if (inFlightMs === undefined) return reported
  const intervalMs = finiteNumber(status.intervalMs) ?? DEFAULT_READINESS_RECONCILE_INTERVAL_MS
  return inFlightMs > intervalMs * READINESS_RECONCILE_STALL_INTERVALS ? 'stalled' : reported
}

function readinessReconcileHealth(
  status: FactoryReadinessReconcileStatus,
  nowMs: number,
): FactoryPublicReadinessReconcileHealth {
  const intervalMs = finiteNumber(status.intervalMs)
  const inFlightMs = readinessReconcileInFlightMs(status, nowMs)
  const cadenceMs = intervalMs ?? DEFAULT_READINESS_RECONCILE_INTERVAL_MS
  return {
    state: derivedReadinessReconcileState(status, nowMs),
    consecutiveFailures: counter(status.consecutiveFailures),
    failureThreshold: counter(status.failureThreshold),
    ...(intervalMs !== undefined ? { intervalMs } : {}),
    ...(finiteNumber(status.lastDurationMs) !== undefined
      ? { lastDurationMs: finiteNumber(status.lastDurationMs) }
      : {}),
    ...(finiteNumber(status.lastStartedAtMs) !== undefined
      ? { lastStartedAtMs: finiteNumber(status.lastStartedAtMs) }
      : {}),
    ...(finiteNumber(status.lastCompletedAtMs) !== undefined
      ? { lastCompletedAtMs: finiteNumber(status.lastCompletedAtMs) }
      : {}),
    ...(finiteNumber(status.lastFailureAtMs) !== undefined
      ? { lastFailureAtMs: finiteNumber(status.lastFailureAtMs) }
      : {}),
    ...(inFlightMs !== undefined
      ? { inFlightMs, missedPasses: Math.floor(inFlightMs / cadenceMs) }
      : {}),
    // `lastError` itself never crosses. Its class does, through the same
    // allowlist that guards IterationReport.skipped[].reason — and a record
    // that carries an error but no admissible class still says so.
    ...(status.lastErrorClass !== undefined || status.lastError !== undefined
      ? { lastErrorClass: telemetryErrorClassName(status.lastErrorClass) }
      : {}),
  }
}

/**
 * Project a loop heartbeat into the record safe to serve unauthenticated.
 *
 * `nowMs` must come from the *writer's* clock, not a remote reader's: every
 * derived duration here is a difference against timestamps the daemon
 * produced, and comparing them to a laptop's clock would report skew as
 * stall. Readers get `ageMs` instead and can bound the staleness themselves.
 */
export function publicHealthFromHeartbeat(
  heartbeat: FactoryLoopHeartbeat | undefined,
  opts: { nowMs?: number; staleMs?: number } = {},
): FactoryPublicHealth {
  const nowMs = opts.nowMs ?? Date.now()
  const staleMs = opts.staleMs ?? DEFAULT_PUBLIC_HEALTH_STALE_MS
  if (!heartbeat) {
    return {
      schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
      ok: false,
      status: 'unknown',
      stale: true,
      reason: 'heartbeat missing',
      degradedSubsystems: [],
    }
  }

  const updatedAtMs = finiteNumber(heartbeat.updatedAtMs)
  const ageMs = updatedAtMs === undefined ? undefined : Math.max(0, nowMs - updatedAtMs)
  const stale = ageMs === undefined || ageMs > staleMs
  const loopStatus = enumValue(heartbeat.status, ['running', 'idle', 'stopping'] as const)

  const readinessReconcile = heartbeat.readinessReconcile
    ? readinessReconcileHealth(heartbeat.readinessReconcile, nowMs)
    : undefined
  const eventListener: FactoryPublicEventListenerHealth | undefined = heartbeat.eventListener
    // Only the state. `reason` is assembled free text and stays behind the
    // authenticated surface.
    ? { state: enumValue(heartbeat.eventListener.state, EVENT_LISTENER_STATES) }
    : undefined

  const degradedSubsystems = DISPATCH_GATING_SUBSYSTEMS.filter((name) => {
    if (name === 'readinessReconcile') {
      return readinessReconcile !== undefined &&
        readinessReconcile.state !== 'healthy' &&
        readinessReconcile.state !== 'not-running'
    }
    return eventListener !== undefined && eventListener.state === 'not-listening'
  })

  // Deliberate split (#295, deliverable 2).
  //
  // `ok` answers "is this process alive", because that is the question the
  // platform asks: the container ping endpoint is `/healthz`, and a non-200
  // there recycles the container. Recycling a wedged Factory destroys the
  // in-memory evidence of the wedge and restarts the cold-start hydration
  // that #36 measured at 61 minutes, so a dispatch-gating degradation must
  // not be able to reach into container lifecycle.
  //
  // `status` is the amber a liveness bit cannot express. No platform reads
  // it, so a monitor can alert on `status !== "ok"` — or on
  // `degradedSubsystems` being non-empty — and get the signal that was
  // missing during the outage, with no restart-loop risk.
  const ok = !stale && loopStatus !== 'stopping' && loopStatus !== 'unknown'
  const status = !ok ? 'unknown' : degradedSubsystems.length > 0 ? 'degraded' : 'ok'
  const reason = stale
    ? 'heartbeat stale'
    : loopStatus === 'stopping'
      ? 'loop stopping'
      : degradedSubsystems.length > 0
        ? `dispatch-gating subsystem not healthy: ${degradedSubsystems.join(', ')}`
        : undefined

  return {
    schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
    ok,
    status,
    stale,
    ...(updatedAtMs !== undefined ? { updatedAtMs } : {}),
    ...(ageMs !== undefined ? { ageMs } : {}),
    loopStatus,
    degradedSubsystems: [...degradedSubsystems],
    ...(reason ? { reason } : {}),
    ...(readinessReconcile ? { readinessReconcile } : {}),
    ...(eventListener ? { eventListener } : {}),
  }
}

/**
 * Re-read a health record that arrived over the wire.
 *
 * A reader of a remote `/healthz` holds parsed JSON from a process it does
 * not control and that may be several versions behind. Running it back
 * through the same coercions the writer used means a caller can rely on the
 * shape — and means an unrecognised state or a hostile string cannot reach a
 * terminal or a downstream report just because it arrived over HTTP.
 *
 * The derived fields are read, not recomputed: they were derived against the
 * writer's clock, and a reader's clock would report skew as stall.
 *
 * Returns `undefined` when the record is absent, which is how a caller tells
 * "instance predates the diagnostics block" from "instance is unhealthy".
 */
export function normalizePublicHealth(value: unknown): FactoryPublicHealth | undefined {
  const record = plainRecord(value)
  if (!record) return undefined
  const readiness = plainRecord(record.readinessReconcile)
  const listener = plainRecord(record.eventListener)
  const degradedSubsystems = Array.isArray(record.degradedSubsystems)
    ? DISPATCH_GATING_SUBSYSTEMS.filter((name) => (record.degradedSubsystems as unknown[]).includes(name))
    : []
  const status = enumValue(record.status, ['ok', 'degraded'] as const)
  return {
    schemaVersion: finiteNumber(record.schemaVersion) ?? FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
    ok: record.ok === true,
    status,
    stale: record.stale === true,
    ...optionalNumber('updatedAtMs', record.updatedAtMs),
    ...optionalNumber('ageMs', record.ageMs),
    loopStatus: enumValue(record.loopStatus, ['running', 'idle', 'stopping'] as const),
    degradedSubsystems: [...degradedSubsystems],
    ...(typeof record.reason === 'string'
      ? { reason: boundedText(record.reason) }
      : {}),
    ...(readiness
      ? {
          readinessReconcile: {
            state: enumValue(readiness.state, READINESS_RECONCILE_STATES),
            consecutiveFailures: counter(readiness.consecutiveFailures),
            failureThreshold: counter(readiness.failureThreshold),
            ...optionalNumber('intervalMs', readiness.intervalMs),
            ...optionalNumber('lastDurationMs', readiness.lastDurationMs),
            ...optionalNumber('lastStartedAtMs', readiness.lastStartedAtMs),
            ...optionalNumber('lastCompletedAtMs', readiness.lastCompletedAtMs),
            ...optionalNumber('lastFailureAtMs', readiness.lastFailureAtMs),
            ...optionalNumber('inFlightMs', readiness.inFlightMs),
            ...optionalNumber('missedPasses', readiness.missedPasses),
            ...(readiness.lastErrorClass !== undefined
              ? { lastErrorClass: telemetryErrorClassName(readiness.lastErrorClass) }
              : {}),
          },
        }
      : {}),
    ...(listener ? { eventListener: { state: enumValue(listener.state, EVENT_LISTENER_STATES) } } : {}),
  }
}
