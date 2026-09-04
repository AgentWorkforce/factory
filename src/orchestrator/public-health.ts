import { createHash, randomBytes } from 'node:crypto'
import { telemetryErrorClassName } from '../observability/error-class.js'
import type { FleetControlPlaneStatus } from '../fleet/control-plane-circuit'
import type { FleetConnectStatus } from '../ports/fleet'
import {
  DEFAULT_AGENT_HOLD_TIMEOUT_MS,
  DEFAULT_AGENTLESS_HOLD_TIMEOUT_MS,
  DEFAULT_CAPACITY_WAIT_WARN_MS,
} from '../config/schema'
import {
  FACTORY_SWEEP_SKIP_REASON_CODES,
  factorySweepSkipReasonCode,
} from './sweep-skip-reason'
import type { FactorySweepSkipReasonCode } from './sweep-skip-reason'
import {
  FACTORY_DISPATCH_FAILURE_REASON_CODES,
  factoryDispatchFailureReasonCode,
} from './dispatch-failure-reason'
import type { FactoryDispatchFailureReasonCode } from './dispatch-failure-reason'
import {
  FULL_COMMIT_SHA,
  PUBLISHABLE_VERSION,
  readBuildIdentity,
  UNKNOWN_BUILD_FIELD,
} from './build-identity'
import type {
  FactoryDispatchCapacityStatus,
  FactoryEventListenerStatus,
  FactoryLoopHeartbeat,
  FactoryPublicBuildIdentity,
  FactoryPublicDispatchCapacityHealth,
  FactoryPublicDispatchSlotOccupant,
  FactoryPublicEventListenerHealth,
  FactoryPublicFleetConnectHealth,
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

const FLEET_CONNECT_STATES: readonly FleetConnectStatus['state'][] = [
  'never-attempted',
  'connecting',
  'dialed',
  'connected',
  'failed',
]

const FLEET_CONTROL_PLANE_STATES: readonly FleetControlPlaneStatus['state'][] = [
  'closed',
  'open',
  'half-open',
]

/** Subsystems whose degradation stops issues from being dispatched. */
const DISPATCH_GATING_SUBSYSTEMS = [
  'readinessReconcile',
  'eventListener',
  'fleetControlPlane',
  // #303. A full batch stops dispatch exactly as hard as a failing sweep, and
  // is the only one of the four that fails without anything throwing: nothing
  // increments a failure counter, nothing writes `lastError`, and the wait
  // logged once and went quiet. It belongs on this list because "why is
  // nothing being dispatched" is the question the list exists to answer.
  'dispatchCapacity',
] as const

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

/**
 * The last sweep's skip breakdown, rebuilt key by key (#355).
 *
 * Numbers only, and the keys come from this module's own copy of the
 * vocabulary rather than from the record: a producer on another version — or a
 * corrupted one — could otherwise put an arbitrary string on an
 * unauthenticated surface simply by using it as an object key, which is the
 * one thing every other field here is careful not to allow. An unknown key's
 * count is folded into `other` rather than dropped, so the parts still sum to
 * `skipped`.
 */
const skipReasonCounts = (
  value: unknown,
): Partial<Record<FactorySweepSkipReasonCode, number>> | undefined => {
  const record = plainRecord(value)
  if (!record) return undefined
  const counts: Partial<Record<FactorySweepSkipReasonCode, number>> = {}
  for (const [key, raw] of Object.entries(record)) {
    const parsed = finiteNumber(raw)
    if (parsed === undefined || parsed < 0) continue
    const floored = Math.floor(parsed)
    if (floored === 0) continue
    const code = factorySweepSkipReasonCode(key)
    counts[code] = (counts[code] ?? 0) + floored
  }
  // Emitted in vocabulary order so two samples of the same surface diff
  // cleanly, and dropped entirely when empty: `skipped` already carries the
  // total, so an empty breakdown states nothing the reader did not have.
  const ordered = FACTORY_SWEEP_SKIP_REASON_CODES.filter((code) => counts[code] !== undefined)
  if (ordered.length === 0) return undefined
  return Object.fromEntries(ordered.map((code) => [code, counts[code] as number]))
}

/**
 * The last sweep's dispatch-failure breakdown, rebuilt key by key (#355).
 *
 * Identical discipline to `skipReasonCounts` and for the identical reason: the
 * keys come from this module's own copy of the vocabulary, never from the
 * record, so a producer on another version cannot put an arbitrary string onto
 * an unauthenticated surface by using it as an object key. Unknown keys fold
 * into `other` rather than being dropped, so the parts still sum to
 * `dispatchFailures`.
 */
const dispatchFailureReasonCounts = (
  value: unknown,
): Partial<Record<FactoryDispatchFailureReasonCode, number>> | undefined => {
  const record = plainRecord(value)
  if (!record) return undefined
  const counts: Partial<Record<FactoryDispatchFailureReasonCode, number>> = {}
  for (const [key, raw] of Object.entries(record)) {
    const parsed = finiteNumber(raw)
    if (parsed === undefined || parsed < 0) continue
    const floored = Math.floor(parsed)
    if (floored === 0) continue
    const code = factoryDispatchFailureReasonCode(key)
    counts[code] = (counts[code] ?? 0) + floored
  }
  const ordered = FACTORY_DISPATCH_FAILURE_REASON_CODES.filter((code) => counts[code] !== undefined)
  if (ordered.length === 0) return undefined
  return Object.fromEntries(ordered.map((code) => [code, counts[code] as number]))
}

/**
 * The last sweep's tree reads and how many were empty, as an all-or-nothing
 * pair (#351 follow-up; #363 review, codex P1).
 *
 * Validated as a pair because it is only readable as one. `emptyTreeReads`
 * alone fires on a healthy sweep — discovery lists two path forms per repo and
 * only one exists — and `treeReads` alone says nothing about what came back.
 * So half a pair is dropped rather than published: a lone number here invites
 * exactly the wrong reading ("some reads were empty, so the mount is sick").
 *
 * `emptyTreeReads > treeReads` is arithmetically impossible and means the
 * producer is not one we understand; both numbers go rather than publishing a
 * ratio a reader would take at face value.
 *
 * Independent of the `candidates` trio in the direction that matters, for the
 * same reason `dispatchFailures` is: a producer that publishes the trio and has
 * never heard of this pair keeps its trio. Not the other way round — a rejected
 * trio takes the pair with it, at `sweepOutcome`'s early return, because the
 * pair is only ever read against `candidates` and a ratio with nothing to
 * compare it to is not the signal.
 */
const treeReadOutcome = (
  status: { treeReads?: unknown; emptyTreeReads?: unknown },
): Partial<Pick<FactoryPublicReadinessReconcileHealth, 'treeReads' | 'emptyTreeReads'>> => {
  const treeReads = optionalCount('treeReads', status.treeReads)
  const emptyTreeReads = optionalCount('emptyTreeReads', status.emptyTreeReads)
  if (treeReads.treeReads === undefined || emptyTreeReads.emptyTreeReads === undefined) return {}
  if (emptyTreeReads.emptyTreeReads > treeReads.treeReads) return {}
  return { ...treeReads, ...emptyTreeReads }
}

/**
 * The last enumerating sweep's arithmetic, published (#355).
 *
 * Deliberately NOT `counter()`: that coerces an absent field to `0`, which
 * would make a daemon that has never enumerated a sweep indistinguishable from
 * one that enumerated a sweep and found nothing. Those are the two halves of
 * the split this block exists to make, so the three fields travel together —
 * all present, or none — and a zero is published as a zero.
 */
const sweepOutcome = (
  // Deliberately `unknown` per field rather than the status type: the same
  // code serves the writer, which holds a real status, and the reader, which
  // holds parsed JSON from a process it does not control. Casting the latter
  // into the former to share the function would be the one unchecked
  // assumption on a path whose whole job is not making any.
  status: {
    candidates?: unknown
    dispatched?: unknown
    skipped?: unknown
    skipReasons?: unknown
    dispatchFailures?: unknown
    dispatchFailureReasons?: unknown
    treeReads?: unknown
    emptyTreeReads?: unknown
    discoveryDeferred?: unknown
    lastEnumeratedAtMs?: unknown
    enumerationCountsInvalid?: unknown
  },
): Partial<Pick<
  FactoryPublicReadinessReconcileHealth,
  | 'candidates'
  | 'dispatched'
  | 'skipped'
  | 'skipReasons'
  | 'dispatchFailures'
  | 'dispatchFailureReasons'
  | 'treeReads'
  | 'emptyTreeReads'
  | 'discoveryDeferred'
  | 'lastEnumeratedAtMs'
  | 'enumerationCountsInvalid'
>> => {
  // Independent of the trio (#358 review, CodeRabbit): the counts describe the
  // last sweep that ENUMERATED, and this describes the most recent pass. A
  // daemon whose first pass deferred has no counts and still has to say why,
  // and one that deferred after a real sweep publishes both — which is the
  // pairing that tells a reader the numbers are from an earlier pass.
  const deferred = status.discoveryDeferred === 'sweep-in-flight'
    ? { discoveryDeferred: 'sweep-in-flight' as const }
    : {}
  const candidates = optionalCount('candidates', status.candidates)
  const dispatched = optionalCount('dispatched', status.dispatched)
  const skipped = optionalCount('skipped', status.skipped)
  const suppliedCounts = status.enumerationCountsInvalid === true ||
    status.candidates !== undefined ||
    status.dispatched !== undefined ||
    status.skipped !== undefined
  // A record carrying only some of the three is a producer we do not
  // understand; publishing the fragment would invite exactly the arithmetic
  // ("candidates minus dispatched") that the missing field makes wrong.
  if (candidates.candidates === undefined ||
      dispatched.dispatched === undefined ||
      skipped.skipped === undefined) {
    return {
      ...deferred,
      ...(suppliedCounts ? { enumerationCountsInvalid: true as const } : {}),
    }
  }
  const skipReasons = skipReasonCounts(status.skipReasons)
  // Deliberately NOT joined to the all-or-nothing trio above. A daemon on
  // 0.1.72 publishes the trio and knows nothing about this field, and requiring
  // it would drop that producer's whole sweep block — deleting the counters
  // that are currently the only view of the outage. Independently optional, and
  // `optionalCount` keeps a zero a zero: absent means "no completed sweep, or a
  // producer without the field", `0` means "a sweep completed and no dispatch
  // it attempted failed". Those are the two facts a bucket count cannot tell
  // apart, which is why this number exists next to the breakdown.
  const dispatchFailures = optionalCount('dispatchFailures', status.dispatchFailures)
  const dispatchFailureReasons = dispatchFailureReasonCounts(status.dispatchFailureReasons)
  return {
    ...candidates,
    ...dispatched,
    ...skipped,
    ...(skipReasons ? { skipReasons } : {}),
    ...dispatchFailures,
    // A breakdown with no total is an orphan: a reader cannot check that the
    // parts sum, which is the one integrity check this surface offers.
    ...(dispatchFailures.dispatchFailures !== undefined && dispatchFailureReasons
      ? { dispatchFailureReasons }
      : {}),
    // The half of the outage a timeout cannot catch, carried the rest of the
    // way to the unauthenticated surface (#363 review, codex P1). Independently
    // optional, like `dispatchFailures`: a 0.1.73 daemon publishes the trio and
    // knows nothing about this pair, and requiring it would drop that
    // producer's whole sweep block.
    ...treeReadOutcome(status),
    // Part of the same atomic snapshot as the counts: it is what dates them,
    // and without it retained counts have no freshness a reader can recover
    // (#359 review).
    ...optionalTimestamp('lastEnumeratedAtMs', status.lastEnumeratedAtMs),
    ...deferred,
  }
}

const DISPATCH_CAPACITY_STATES: readonly FactoryPublicDispatchCapacityHealth['state'][] = [
  'healthy',
  'waiting',
  'stalled',
]

/**
 * Capacity state from the numbers that produced it.
 *
 * Used by the writer, and as the fallback when a record arrives carrying an
 * unrecognised state string. Falling back to `healthy` there would hide the
 * exact condition this block exists to report, and unlike the readiness
 * derivations this one needs no clock — `longestWaitMs` is a duration the
 * writer already measured.
 */
const deriveDispatchCapacityState = (
  waiting: number,
  longestWaitMs: number | undefined,
  warnMs: number,
  agentlessOccupants = 0,
  occupiedOccupants = 0,
): FactoryPublicDispatchCapacityHealth['state'] =>
  // An occupant past its own reap deadline is stalled capacity, and it is
  // stalled whether or not anything is queued behind it (#315, #419). This
  // block already computed that fact for the agentless shape and then dropped
  // it on the floor: with `waiting === 0` the state read `healthy` while half
  // of a two-slot batch was consumed by a row nothing could move — a monitor
  // staying green through the exact condition it exists to catch. #419 is the
  // same failure for the OTHER shape: a slot whose placed agent has gone
  // offline and outlasted `agentHoldTimeoutMs`. Both are total dispatch stops
  // and both must degrade the same way.
  agentlessOccupants > 0 || occupiedOccupants > 0
    ? 'stalled'
    : waiting === 0
      ? 'healthy'
      : longestWaitMs !== undefined && longestWaitMs > warnMs
        ? 'stalled'
        : 'waiting'

const dispatchCapacityState = (
  value: unknown,
  waiting: number,
  longestWaitMs: number | undefined,
  warnMs: number,
  agentlessOccupants = 0,
  occupiedOccupants = 0,
): FactoryPublicDispatchCapacityHealth['state'] =>
  // A record that already names the wedge cannot be re-published as `healthy`
  // on the strength of its own stale state string.
  agentlessOccupants > 0 || occupiedOccupants > 0
    ? 'stalled'
    : typeof value === 'string' && (DISPATCH_CAPACITY_STATES as readonly string[]).includes(value)
      ? value as FactoryPublicDispatchCapacityHealth['state']
      : deriveDispatchCapacityState(waiting, longestWaitMs, warnMs, agentlessOccupants, occupiedOccupants)

/**
 * Build identity, coerced for the unauthenticated surface (#446).
 *
 * Used on BOTH sides of the wire — by the writer, over the values
 * `build-identity.ts` read off this process's own artifact, and by
 * `normalizePublicHealth` over a record that arrived as JSON from a remote
 * process running a version this one has never seen. The remote case is why
 * the writer's own values go through it too: one coercion, one set of rules,
 * no second copy of the judgement to drift.
 *
 * Anything that is not a full 40-hex commit or a bounded version token becomes
 * `unknown`. An abbreviated SHA is rejected on purpose — it is not what was
 * stamped, and a reader who pastes it into `git show` and gets a hit has
 * learned nothing about which artifact answered.
 */
const buildIdentity = (value: unknown): FactoryPublicBuildIdentity | undefined => {
  const parsed = plainRecord(value)
  if (!parsed) return undefined
  const version = parsed.version
  const commit = parsed.commit
  return {
    version: typeof version === 'string' && PUBLISHABLE_VERSION.test(version)
      ? version
      : UNKNOWN_BUILD_FIELD,
    commit: typeof commit === 'string' && FULL_COMMIT_SHA.test(commit)
      ? commit
      : UNKNOWN_BUILD_FIELD,
  }
}

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
    'inFlightSinceMs' | 'lastStartedAtMs' | 'lastCompletedAtMs' | 'lastFailureAtMs'
  >,
  nowMs: number,
): number | undefined {
  // A daemon that publishes `inFlightSinceMs` knows what is still running and
  // does not need the inference below. It is strictly better: a sweep whose
  // wait ended on the #296 deadline writes a settle timestamp while its
  // `runOnce()` keeps running, so timestamp order alone would call that stuck
  // pass "not in flight" — the exact blindness this projection exists to cure.
  const inFlightSinceMs = timestamp(status.inFlightSinceMs)
  if (inFlightSinceMs !== undefined) return Math.max(0, nowMs - inFlightSinceMs)
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
    'state' | 'intervalMs' | 'inFlightSinceMs' | 'lastStartedAtMs' | 'lastCompletedAtMs' | 'lastFailureAtMs'
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
    // Republished only when positive, for the same reason `intervalMs` is: a
    // recorded `0` means "no bound configured", and echoing it as though it
    // were a real deadline would read as an instant one.
    ...optionalPositive('timeoutMs', status.timeoutMs),
    ...optionalPositive('sweepBudgetMs', status.sweepBudgetMs),
    ...(finiteNumber(status.lastDurationMs) !== undefined
      ? { lastDurationMs: finiteNumber(status.lastDurationMs) }
      : {}),
    ...optionalTimestamp('lastStartedAtMs', status.lastStartedAtMs),
    ...optionalTimestamp('lastCompletedAtMs', status.lastCompletedAtMs),
    ...optionalTimestamp('lastFailureAtMs', status.lastFailureAtMs),
    ...(inFlightMs !== undefined
      ? { inFlightMs, missedPasses: Math.floor(inFlightMs / cadenceMs) }
      : {}),
    ...sweepOutcome(status),
    // `lastError` itself never crosses. Its class does, through the same
    // allowlist that guards IterationReport.skipped[].reason — and a record
    // that carries an error but no admissible class still says so.
    ...(status.lastErrorClass !== undefined || status.lastError !== undefined
      ? { lastErrorClass: telemetryErrorClassName(status.lastErrorClass) }
      : {}),
  }
}

/**
 * Occupied slots that will not free themselves.
 *
 * NOT "has no agent yet". `BatchTracker#recordPlanned` writes a spec before
 * the spawn returns, so every healthy dispatch is agent-less for as long as
 * its placement takes — minutes for a cloud spawn — and on a single-slot batch
 * that is nearly always. Counting that would make the wedge signature read 1
 * continuously on a batch that is working, which is worse than not having the
 * field (#303 review, cubic). The condition no healthy dispatch reaches is
 * *never placed and already past the deadline that should have reaped it*.
 *
 * Defensive throughout: this runs inside the heartbeat writer, where a throw
 * costs the whole diagnostics block, and the record may come from an older or
 * corrupted producer (#303 review, cubic).
 */
function countAgentlessOccupants(occupants: unknown, reapMs: number): number {
  if (!Array.isArray(occupants)) return 0
  return occupants.filter((entry) => {
    const occupant = plainRecord(entry)
    if (!occupant) return false
    const slotHeldForMs = finiteNumber(occupant.slotHeldForMs)
    // A producer that sends neither field cannot answer the question, and
    // guessing "wedged" from an absence is how a false alarm gets published.
    // `>=`, not `>`: the reaper skips only while `nowMs < dueAtMs`, so it
    // reaps at exactly the deadline. A diagnostic that disagrees with the
    // mechanism it reports on — even on one boundary instant — is the failure
    // mode this whole PR exists to close (#303 review, cubic).
    return finiteNumber(occupant.placedAgents) === 0 &&
      slotHeldForMs !== undefined &&
      slotHeldForMs >= reapMs
  }).length
}

/**
 * Occupied slots whose placement has outlasted `agentHoldTimeoutMs` (#419).
 *
 * The other wedge shape. `countAgentlessOccupants` catches a slot that never
 * placed an agent; this one catches a slot that DID and whose team then went
 * offline or exceeded any plausible run duration. Requires `placedAgents > 0`
 * (an absent value or 0 is the agentless shape, counted elsewhere) and
 * anchors on `heldForMs` — the clock the reaper actually uses — not
 * `slotHeldForMs`, so a placement that took hours to succeed does not carry
 * its own spawn window into the deadline. Same `>=` boundary as the
 * agentless count, for the same reason: the reaper skips only while
 * `nowMs < dueAtMs`.
 */
function countOccupiedOccupants(occupants: unknown, reapMs: number): number {
  if (!Array.isArray(occupants)) return 0
  return occupants.filter((entry) => {
    const occupant = plainRecord(entry)
    if (!occupant) return false
    const placedAgents = finiteNumber(occupant.placedAgents)
    const heldForMs = finiteNumber(occupant.heldForMs)
    return placedAgents !== undefined &&
      placedAgents > 0 &&
      heldForMs !== undefined &&
      heldForMs >= reapMs
  }).length
}

/**
 * Boot-scoped salt for occupant ids (#315).
 *
 * Issue keys carry project and repository names, so they cannot be published —
 * but a bare digest of a low-entropy key like `AR-315` is recoverable from a
 * word list, which would defeat the redaction it is meant to respect. Salting
 * per process makes the id meaningful only by comparison, which is all the
 * caller needs: two samples from one boot can be matched against each other,
 * and nothing can be matched back to an issue.
 */
const OCCUPANT_ID_SALT = randomBytes(16).toString('hex')

// The position is the fallback identity, not the primary one: a corrupted
// producer that sent no issue key would otherwise hash the empty string for
// every such row, and two distinct occupants sharing one id reads as a single
// stuck slot — the precise misreading this field exists to prevent. Occupants
// are emitted in a stable key order, so the index is a usable last resort.
const occupantId = (issue: unknown, index: number): string =>
  createHash('sha256')
    .update(`${OCCUPANT_ID_SALT}:${typeof issue === 'string' && issue ? issue : `#${index}`}`)
    .digest('hex')
    .slice(0, 12)

/**
 * Occupied slots as identities with ages, redacted (#315).
 *
 * The count above answers "is something wedged". It cannot answer "is it the
 * SAME something", which is the question that separates a stuck slot from
 * reap-and-reacquire churn, and answering it by sampling a count takes as long
 * as the operator is willing to watch. A monotonically growing `slotHeldForMs`
 * against a stable `id` settles it in one request.
 *
 * Defensive throughout: this runs inside the heartbeat writer, where a throw
 * costs the whole diagnostics block (#303 review, cubic).
 */
function publicOccupants(
  occupants: unknown,
  reapMs: number,
  occupiedReapMs: number,
): FactoryPublicDispatchSlotOccupant[] {
  if (!Array.isArray(occupants)) return []
  return occupants.flatMap((entry, index) => {
    const occupant = plainRecord(entry)
    if (!occupant) return []
    // An ABSENT `placedAgents` is not a reported zero (#318 review,
    // CodeRabbit). `countAgentlessOccupants` compares
    // `finiteNumber(placedAgents) === 0`, which is false when the field is
    // missing, precisely so that a producer who cannot answer the question is
    // not read as answering "wedged". Defaulting to 0 here would have made the
    // two disagree inside one payload — and worse, the reader folds
    // `pastReapDeadline` into its wedge count, so an occupant that merely
    // omitted the field would have forced `status: 'degraded'`: the false
    // alarm the #303 comment exists to prevent.
    const reportedPlacedAgents = finiteNumber(occupant.placedAgents)
    const slotHeldForMs = finiteNumber(occupant.slotHeldForMs)
    const heldForMs = finiteNumber(occupant.heldForMs)
    const pastReapDeadline = reportedPlacedAgents === 0 &&
      slotHeldForMs !== undefined &&
      slotHeldForMs >= reapMs
    // #419: the occupied-past-deadline sibling. Same discipline as
    // `pastReapDeadline` — the flag reflects a real reap condition, so an
    // ABSENT `placedAgents` cannot answer the question and neither can an
    // absent `heldForMs`. Anchors on `heldForMs` (the placement clock the
    // reaper uses) so a slot-hold that included a slow spawn is not confused
    // with a placement that outran its deadline.
    const pastOccupiedDeadline = reportedPlacedAgents !== undefined &&
      reportedPlacedAgents > 0 &&
      heldForMs !== undefined &&
      heldForMs >= occupiedReapMs
    return [{
      // A record re-projected from an already-public one carries its id
      // forward; only the writer, which holds the issue key, mints one.
      id: typeof occupant.id === 'string' ? occupant.id : occupantId(occupant.issue, index),
      // Omitted rather than defaulted: publishing `0` for a count nobody
      // reported states a fact the producer never gave us.
      ...(reportedPlacedAgents === undefined ? {} : { placedAgents: reportedPlacedAgents }),
      ...optionalDuration('slotHeldForMs', slotHeldForMs),
      ...optionalDuration('heldForMs', heldForMs),
      ...(pastReapDeadline ? { pastReapDeadline } : {}),
      ...(pastOccupiedDeadline ? { pastOccupiedDeadline } : {}),
    }]
  })
}

/**
 * Batch occupancy, redacted (#303).
 *
 * Issue keys stay behind the authenticated surface — they carry customer
 * project and repository names — so the public record carries counts and
 * durations only. `agentlessOccupants` is the wedge signature: a slot held by
 * a lifecycle that never placed an agent cannot make progress on its own.
 */
function dispatchCapacityHealth(
  status: FactoryDispatchCapacityStatus,
): FactoryPublicDispatchCapacityHealth {
  const waiting = counter(status.waiting)
  const longestWaitMs = finiteNumber(status.longestWaitMs)
  const warnMs = positiveNumber(status.waitWarnMs) ?? DEFAULT_CAPACITY_WAIT_WARN_MS
  const reapMs = positiveNumber(status.agentlessHoldTimeoutMs) ?? DEFAULT_AGENTLESS_HOLD_TIMEOUT_MS
  const occupiedReapMs = positiveNumber(status.agentHoldTimeoutMs) ?? DEFAULT_AGENT_HOLD_TIMEOUT_MS
  const agentlessOccupants = countAgentlessOccupants(status.occupants, reapMs)
  const occupiedOccupants = countOccupiedOccupants(status.occupants, occupiedReapMs)
  const occupants = publicOccupants(status.occupants, reapMs, occupiedReapMs)
  return {
    state: deriveDispatchCapacityState(
      waiting,
      longestWaitMs,
      warnMs,
      agentlessOccupants,
      occupiedOccupants,
    ),
    batchSize: counter(status.batchSize),
    active: counter(status.active),
    waiting,
    waitWarnMs: warnMs,
    agentlessHoldTimeoutMs: reapMs,
    agentHoldTimeoutMs: occupiedReapMs,
    ...optionalDuration('longestWaitMs', longestWaitMs),
    ...(agentlessOccupants > 0 ? { agentlessOccupants } : {}),
    ...(occupiedOccupants > 0 ? { occupiedOccupants } : {}),
    ...(occupants.length > 0 ? { occupants } : {}),
  }
}

/**
 * Project the fleet socket for the UNAUTHENTICATED surface.
 *
 * Deliberately NOT added to DISPATCH_GATING_SUBSYSTEMS. A failed socket does not
 * itself stop dispatch -- `roster()` runs over HTTP -- and listing it there would
 * flip `ok` on a live deployment and hand the container-replacement logic a new
 * reason to cycle. Publishing the fact is the goal; changing what `ok` means is a
 * separate decision belonging to whoever owns dispatch behaviour.
 */
function fleetConnectHealth(status: FleetConnectStatus): FactoryPublicFleetConnectHealth {
  const attempts = counter(status.attempts)
  return {
    state: enumValue(status.state, FLEET_CONNECT_STATES),
    ...(attempts !== undefined ? { attempts } : {}),
    ...optionalTimestamp('lastAttemptAtMs', status.lastAttemptAtMs),
    ...optionalTimestamp('lastDialedAtMs', status.lastDialedAtMs),
    ...optionalTimestamp('firstEventAtMs', status.firstEventAtMs),
    ...optionalTimestamp('lastConnectedAtMs', status.lastConnectedAtMs),
    ...optionalTimestamp('lastFailureAtMs', status.lastFailureAtMs),
    // `lastError` stays behind /evidence, exactly as it does for the circuit.
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
      // Still published: this process knows what it is even when it has no
      // heartbeat to describe, and "no heartbeat, and here is the build that
      // has none" is strictly more diagnosable than "no heartbeat" (#446).
      build: buildIdentity(readBuildIdentity()),
    }
  }

  const updatedAtMs = timestamp(heartbeat.updatedAtMs)
  const ageMs = updatedAtMs === undefined ? undefined : Math.max(0, nowMs - updatedAtMs)
  const stale = ageMs === undefined || ageMs > staleMs
  const loopStatus = enumValue(heartbeat.status, ['running', 'idle', 'stopping'] as const)

  const readinessReconcile = heartbeat.readinessReconcile
    ? readinessReconcileHealth(heartbeat.readinessReconcile, nowMs)
    : undefined
  const fleetConnect = heartbeat.fleetConnect ? fleetConnectHealth(heartbeat.fleetConnect) : undefined
  const fleetControlPlane = heartbeat.fleetControlPlane
    ? fleetControlPlaneHealth(heartbeat.fleetControlPlane)
    : undefined
  const dispatchCapacity = heartbeat.dispatchCapacity
    ? dispatchCapacityHealth(heartbeat.dispatchCapacity)
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
    if (name === 'dispatchCapacity') {
      // `waiting` alone is ordinary backpressure and stays green: a batch is
      // supposed to fill up. Only a wait past the configured threshold — which
      // a deployment running multi-hour issues should raise rather than
      // silence — is a degradation.
      return dispatchCapacity !== undefined && dispatchCapacity.state === 'stalled'
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
    ...(fleetConnect ? { fleetConnect } : {}),
    ...(dispatchCapacity ? { dispatchCapacity } : {}),
    // Identity, not health, and unconditional (#446). It is published even on
    // the heartbeat-missing path above and even when both halves read
    // `unknown`, because "which build is answering" is a question about the
    // PROCESS serving the record, not about the record — and the moment it is
    // most worth asking is the moment the daemon looks wrong.
    build: buildIdentity(readBuildIdentity()),
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
  const fleetConnect = plainRecord(record.fleetConnect)
  const capacity = plainRecord(record.dispatchCapacity)
  const build = buildIdentity(record.build)
  // Re-derive the wedge from the occupants the record carries rather than
  // trusting its own `agentlessOccupants`: a producer that published the
  // occupants but predates the count still has to project as stalled (#315).
  const capacityReapMs = positiveNumber(capacity?.agentlessHoldTimeoutMs)
    ?? DEFAULT_AGENTLESS_HOLD_TIMEOUT_MS
  const capacityOccupiedReapMs = positiveNumber(capacity?.agentHoldTimeoutMs)
    ?? DEFAULT_AGENT_HOLD_TIMEOUT_MS
  const capacityOccupants = publicOccupants(
    capacity?.occupants,
    capacityReapMs,
    capacityOccupiedReapMs,
  )
  const capacityAgentlessOccupants = Math.max(
    counter(capacity?.agentlessOccupants),
    capacityOccupants.filter((occupant) => occupant.pastReapDeadline).length,
  )
  // #419: mirror the agentless-derivation for the OCCUPIED shape. A producer
  // that publishes occupants without the count would otherwise let a
  // 13.5-hour placed-agent wedge project `dispatchCapacity.state: 'stalled'`
  // under `status: 'ok'` — the same stays-green failure #315 closed for the
  // agentless shape, one deadline over.
  const capacityOccupiedOccupants = Math.max(
    counter(capacity?.occupiedOccupants),
    capacityOccupants.filter((occupant) => occupant.pastOccupiedDeadline).length,
  )
  const capacityWedged = capacityAgentlessOccupants > 0 || capacityOccupiedOccupants > 0
  const recordDegraded = Array.isArray(record.degradedSubsystems)
    ? DISPATCH_GATING_SUBSYSTEMS.filter((name) => (record.degradedSubsystems as unknown[]).includes(name))
    : []
  // The wedge has to reach the TOP-LEVEL signal too, not just the nested state
  // (#318 review, codex). A record whose producer published occupants without
  // the count would otherwise project `dispatchCapacity.state: 'stalled'`
  // underneath `status: 'ok'` and an empty `degradedSubsystems` — the same
  // stays-green failure this change exists to close, reappearing one layer up,
  // where every documented consumer actually reads it.
  const degradedSubsystems = capacityWedged && !recordDegraded.includes('dispatchCapacity')
    ? DISPATCH_GATING_SUBSYSTEMS.filter((name) => recordDegraded.includes(name) || name === 'dispatchCapacity')
    : recordDegraded
  const status = capacityWedged
    ? 'degraded' as const
    : enumValue(record.status, ['ok', 'degraded'] as const)
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
            ...optionalPositive('timeoutMs', readiness.timeoutMs),
            ...optionalPositive('sweepBudgetMs', readiness.sweepBudgetMs),
            ...optionalDuration('lastDurationMs', readiness.lastDurationMs),
            ...optionalTimestamp('lastStartedAtMs', readiness.lastStartedAtMs),
            ...optionalTimestamp('lastCompletedAtMs', readiness.lastCompletedAtMs),
            ...optionalTimestamp('lastFailureAtMs', readiness.lastFailureAtMs),
            ...optionalDuration('inFlightMs', readiness.inFlightMs),
            ...optionalCount('missedPasses', readiness.missedPasses),
            ...sweepOutcome(readiness),
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
    ...(fleetConnect
      ? {
          fleetConnect: {
            state: enumValue(fleetConnect.state, FLEET_CONNECT_STATES),
            ...optionalCount('attempts', fleetConnect.attempts),
            ...optionalTimestamp('lastAttemptAtMs', fleetConnect.lastAttemptAtMs),
            ...optionalTimestamp('lastDialedAtMs', fleetConnect.lastDialedAtMs),
            ...optionalTimestamp('firstEventAtMs', fleetConnect.firstEventAtMs),
            ...optionalTimestamp('lastConnectedAtMs', fleetConnect.lastConnectedAtMs),
            ...optionalTimestamp('lastFailureAtMs', fleetConnect.lastFailureAtMs),
            // Never retain `lastError` from a remote unauthenticated record.
          },
        }
      : {}),
    // Absent stays ABSENT here (#446). A producer older than this change
    // published no identity at all, and coercing that into
    // `{version: 'unknown', commit: 'unknown'}` would claim the remote
    // ANSWERED the question — erasing the very distinction ("is that instance
    // even new enough to tell me?") this field exists to make.
    ...(build ? { build } : {}),
    ...(capacity
      ? {
          dispatchCapacity: {
            state: dispatchCapacityState(
              capacity.state,
              counter(capacity.waiting),
              optionalDuration('longestWaitMs', capacity.longestWaitMs).longestWaitMs,
              positiveNumber(capacity.waitWarnMs) ?? DEFAULT_CAPACITY_WAIT_WARN_MS,
              capacityAgentlessOccupants,
              capacityOccupiedOccupants,
            ),
            batchSize: counter(capacity.batchSize),
            active: counter(capacity.active),
            waiting: counter(capacity.waiting),
            waitWarnMs: positiveNumber(capacity.waitWarnMs) ?? DEFAULT_CAPACITY_WAIT_WARN_MS,
            agentlessHoldTimeoutMs: positiveNumber(capacity.agentlessHoldTimeoutMs)
              ?? DEFAULT_AGENTLESS_HOLD_TIMEOUT_MS,
            agentHoldTimeoutMs: capacityOccupiedReapMs,
            ...optionalDuration('longestWaitMs', capacity.longestWaitMs),
            ...(capacityAgentlessOccupants > 0 ? { agentlessOccupants: capacityAgentlessOccupants } : {}),
            ...(capacityOccupiedOccupants > 0 ? { occupiedOccupants: capacityOccupiedOccupants } : {}),
            ...(capacityOccupants.length > 0 ? { occupants: capacityOccupants } : {}),
          },
        }
      : {}),
  }
}
