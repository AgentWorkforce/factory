import { telemetryErrorClassName } from '../observability/error-class.js'
import type { FleetControlPlaneStatus } from '../fleet/control-plane-circuit'
import type {
  FactoryEventListenerStatus,
  FactoryLoopHeartbeat,
  FactoryPublicEventListenerHealth,
  FactoryPublicFleetControlPlaneHealth,
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

const FLEET_CONTROL_PLANE_STATES: readonly FleetControlPlaneStatus['state'][] = [
  'closed',
  'open',
  'half-open',
]

/** Subsystems whose degradation stops issues from being dispatched. */
const DISPATCH_GATING_SUBSYSTEMS = ['readinessReconcile', 'eventListener', 'fleetControlPlane'] as const

const finiteNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

/** A cadence is a denominator: zero or negative would make every derived ratio nonsense. */
const positiveNumber = (value: unknown): number | undefined => {
  const parsed = finiteNumber(value)
  return parsed !== undefined && parsed > 0 ? parsed : undefined
}

const counter = (value: unknown): number => {
  const parsed = finiteNumber(value)
  return parsed !== undefined && parsed >= 0 ? Math.floor(parsed) : 0
}

/**
 * The widest instant `Date` can represent (ECMA-262 time-value limit).
 *
 * Review follow-up on #300 (P2, codex): a finite number is not a valid date.
 * `new Date(1e300).toISOString()` throws, and these numbers arrive from a
 * remote process — so a hostile or corrupted record could abort a renderer
 * that was asked to explain an outage. A timestamp outside the range is
 * dropped rather than published.
 */
const MAX_TIME_VALUE_MS = 8.64e15

const timestamp = (value: unknown): number | undefined => {
  const parsed = finiteNumber(value)
  return parsed !== undefined && Math.abs(parsed) <= MAX_TIME_VALUE_MS ? parsed : undefined
}

const optionalTimestamp = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = timestamp(value)
  return parsed === undefined ? {} : { [key]: parsed } as Partial<Record<K, number>>
}

const plainRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const optionalNumber = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = finiteNumber(value)
  return parsed === undefined ? {} : { [key]: parsed } as Partial<Record<K, number>>
}

/**
 * A duration that cannot be negative, dropped rather than republished.
 *
 * The writer never emits these; a remote process on another version might
 * (#300 review, CodeRabbit). Dropping the field keeps the promise this
 * module's doc comment makes to its callers about the shape they get.
 */
const optionalDuration = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = finiteNumber(value)
  return parsed === undefined || parsed < 0 ? {} : { [key]: parsed } as Partial<Record<K, number>>
}

/** A count of whole passes: fractions are not a thing an operator can read. */
const optionalCount = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = finiteNumber(value)
  return parsed === undefined || parsed < 0
    ? {}
    : { [key]: Math.floor(parsed) } as Partial<Record<K, number>>
}

const optionalPositive = <K extends string>(key: K, value: unknown): Partial<Record<K, number>> => {
  const parsed = positiveNumber(value)
  return parsed === undefined ? {} : { [key]: parsed } as Partial<Record<K, number>>
}

/** Control characters stripped, length bounded: this text can reach a terminal. */
const boundedText = (value: string): string =>
  // C0 and C1 alike (#300 review, P2, cubic): some terminals interpret the
  // C1 range as escape introducers.
  value.replace(/[\u0000-\u001F\u007F-\u009F]+/gu, ' ').trim().slice(0, 300)

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
  const startedAtMs = timestamp(status.lastStartedAtMs)
  if (startedAtMs === undefined) return undefined
  const settledAtMs = Math.max(
    timestamp(status.lastCompletedAtMs) ?? Number.NEGATIVE_INFINITY,
    timestamp(status.lastFailureAtMs) ?? Number.NEGATIVE_INFINITY,
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
  const intervalMs = positiveNumber(status.intervalMs) ?? DEFAULT_READINESS_RECONCILE_INTERVAL_MS
  return inFlightMs > intervalMs * READINESS_RECONCILE_STALL_INTERVALS ? 'stalled' : reported
}

function readinessReconcileHealth(
  status: FactoryReadinessReconcileStatus,
  nowMs: number,
): FactoryPublicReadinessReconcileHealth {
  // Review follow-up on #300 (P2, cubic): a recorded `intervalMs: 0` made
  // every in-flight pass instantly stalled and `missedPasses` Infinity, which
  // JSON renders as null. An unusable cadence falls back to the default and is
  // not republished as though it were real.
  const intervalMs = positiveNumber(status.intervalMs)
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
    ...optionalTimestamp('lastStartedAtMs', status.lastStartedAtMs),
    ...optionalTimestamp('lastCompletedAtMs', status.lastCompletedAtMs),
    ...optionalTimestamp('lastFailureAtMs', status.lastFailureAtMs),
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

function fleetControlPlaneHealth(
  status: FleetControlPlaneStatus,
): FactoryPublicFleetControlPlaneHealth {
  return {
    state: enumValue(status.state, FLEET_CONTROL_PLANE_STATES),
    consecutiveFailures: counter(status.consecutiveFailures),
    failureThreshold: counter(status.failureThreshold),
    ...optionalTimestamp('lastFailureAtMs', status.lastFailureAtMs),
    ...optionalTimestamp('retryAtMs', status.retryAtMs),
    // `lastError` stays behind /evidence: a roster probe failure names the
    // broker socket path.
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

  const updatedAtMs = timestamp(heartbeat.updatedAtMs)
  const ageMs = updatedAtMs === undefined ? undefined : Math.max(0, nowMs - updatedAtMs)
  const stale = ageMs === undefined || ageMs > staleMs
  const loopStatus = enumValue(heartbeat.status, ['running', 'idle', 'stopping'] as const)

  const readinessReconcile = heartbeat.readinessReconcile
    ? readinessReconcileHealth(heartbeat.readinessReconcile, nowMs)
    : undefined
  const fleetControlPlane = heartbeat.fleetControlPlane
    ? fleetControlPlaneHealth(heartbeat.fleetControlPlane)
    : undefined
  const eventListener: FactoryPublicEventListenerHealth | undefined = heartbeat.eventListener
    // Only the state. `reason` is assembled free text and stays behind the
    // authenticated surface.
    ? { state: enumValue(heartbeat.eventListener.state, EVENT_LISTENER_STATES) }
    : undefined

  // A daemon that is not running a readiness loop is not a live dispatcher —
  // a bounded `factory loop` reports `not-running` here and is not supposed to
  // hold a subscription. Only a live instance's listener is dispatch-gating.
  const liveDispatcher = readinessReconcile !== undefined && readinessReconcile.state !== 'not-running'
  const degradedSubsystems = DISPATCH_GATING_SUBSYSTEMS.filter((name) => {
    if (name === 'readinessReconcile') {
      return readinessReconcile !== undefined &&
        readinessReconcile.state !== 'healthy' &&
        readinessReconcile.state !== 'not-running'
    }
    if (name === 'fleetControlPlane') {
      // An open circuit fails every spawn fast; half-open is one probe away
      // from either. Both mean dispatch is not admitting work normally.
      return fleetControlPlane !== undefined && fleetControlPlane.state !== 'closed'
    }
    // Review follow-up on #300 (P2, codex): `starting` is what a live daemon
    // reports before `#startLiveSubscription` installs the subscription. No
    // listener is registered, and startup can be lengthy, so anything short of
    // a registered subscription or an active poll is amber — not green.
    return liveDispatcher &&
      eventListener !== undefined &&
      eventListener.state !== 'subscribed' &&
      eventListener.state !== 'polling'
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
    ...(fleetControlPlane ? { fleetControlPlane } : {}),
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
  const fleet = plainRecord(record.fleetControlPlane)
  const degradedSubsystems = Array.isArray(record.degradedSubsystems)
    ? DISPATCH_GATING_SUBSYSTEMS.filter((name) => (record.degradedSubsystems as unknown[]).includes(name))
    : []
  const status = enumValue(record.status, ['ok', 'degraded'] as const)
  return {
    schemaVersion: finiteNumber(record.schemaVersion) ?? FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
    ok: record.ok === true,
    status,
    stale: record.stale === true,
    ...optionalTimestamp('updatedAtMs', record.updatedAtMs),
    ...optionalDuration('ageMs', record.ageMs),
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
            ...optionalPositive('intervalMs', readiness.intervalMs),
            ...optionalDuration('lastDurationMs', readiness.lastDurationMs),
            ...optionalTimestamp('lastStartedAtMs', readiness.lastStartedAtMs),
            ...optionalTimestamp('lastCompletedAtMs', readiness.lastCompletedAtMs),
            ...optionalTimestamp('lastFailureAtMs', readiness.lastFailureAtMs),
            ...optionalDuration('inFlightMs', readiness.inFlightMs),
            ...optionalCount('missedPasses', readiness.missedPasses),
            ...(readiness.lastErrorClass !== undefined
              ? { lastErrorClass: telemetryErrorClassName(readiness.lastErrorClass) }
              : {}),
          },
        }
      : {}),
    ...(listener ? { eventListener: { state: enumValue(listener.state, EVENT_LISTENER_STATES) } } : {}),
    ...(fleet
      ? {
          fleetControlPlane: {
            state: enumValue(fleet.state, FLEET_CONTROL_PLANE_STATES),
            consecutiveFailures: counter(fleet.consecutiveFailures),
            failureThreshold: counter(fleet.failureThreshold),
            ...optionalTimestamp('lastFailureAtMs', fleet.lastFailureAtMs),
            ...optionalTimestamp('retryAtMs', fleet.retryAtMs),
          },
        }
      : {}),
  }
}
