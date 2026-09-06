import { telemetryErrorClass } from '../observability/error-class.js'
import { normalizePublicHealth } from '../orchestrator/public-health.js'
import type { FactoryPublicHealth, FactoryPublicReadinessReconcileHealth } from '../types'

/**
 * `factory diagnose --deployed <url>` (#295).
 *
 * The command a lane brief can name. Every other route to a deployed
 * Factory's health is local-only (`factory status`, `factory loop-status`),
 * Worker-scope (`wrangler tail`), blocked by network shape (the container is
 * private-networked with no sshd), or gated behind a per-deploy credential
 * that no longer exists. This pulls the answer through the Worker, which is
 * the only path that survives that shape.
 *
 * It works with no credential at all: `/healthz` is unauthenticated, and
 * since #295 it carries the failure count, the error class and the in-flight
 * age. A token, when the operator has one, adds `/evidence` — the free-text
 * `lastError` that must not be on the public surface.
 */
export interface DeployedEvidenceSummary {
  fetched: boolean
  httpStatus?: number
  /** Why the evidence surface was not read. */
  reason?: string
  phase?: string
  lastError?: string
  fleetConnectLastError?: string
  consecutiveFailures?: number
}

export interface DeployedLegacyHealth {
  phase?: string
  factoryProcess?: string
  heartbeatStatus?: string
  heartbeatUpdatedAt?: string
  readinessReconcile?: string
  eventListener?: string
}

export interface DeployedFactoryDiagnosis {
  url: string
  reachable: boolean
  httpStatus?: number
  /**
   * The instance's own liveness verdict, computed against ITS clock.
   *
   * The daemon stamps the health block when it writes the heartbeat, so the
   * block's `ageMs` is 0 and `stale` false *in the file*. If the daemon dies
   * and the container keeps serving that file, the block stays green forever.
   * The container recomputes liveness from `updatedAtMs` on every request, so
   * its verdict is the fresh one and it wins (#300 review, P1).
   *
   * `false` only when the INSTANCE answered no — a 503 health body, or
   * `ok: false`. A 404, 401 or 502 from a proxy in front of it is not the
   * instance speaking at all, and leaves this `undefined` (#300 review,
   * CodeRabbit).
   */
  live?: boolean
  /** The endpoint answered, but not with anything this command can read. */
  unreadable?: boolean
  /** Allowlisted class of a transport failure; the message is not reported. */
  errorClass?: string
  dispatching: boolean
  verdict: string
  health?: FactoryPublicHealth
  /**
   * The Worker answered without probing the container.
   *
   * In event-driven short-sleep mode `/healthz` terminates at the Worker on
   * purpose, so anonymous polling cannot wake the container and defeat
   * scale-to-zero. That answer is Worker liveness and says nothing about
   * Factory (factory-cloud#40 review).
   */
  workerOnly?: boolean
  /**
   * The container's own bootstrap phase, when it reports one.
   *
   * `booting`/`rendering-config`/`preflight` answer `ok: false` exactly like a
   * wedged instance does, and telling someone three minutes into a boot that
   * their Factory process is gone sends them to the wrong problem.
   */
  phase?: string
  /** Present when the instance predates the `/healthz` diagnostics block. */
  legacy?: DeployedLegacyHealth
  evidence?: DeployedEvidenceSummary
}

export interface DiagnoseDeployedOptions {
  url: string
  token?: string
  timeoutMs?: number
  fetch?: typeof fetch
}

const DEFAULT_TIMEOUT_MS = 10_000

/** Container bootstrap phases: `ok: false` here means "not yet", not "wedged". */
const BOOT_PHASES = new Set(['booting', 'rendering-config', 'preflight'])
const MAX_EVIDENCE_TEXT = 2_000

const endpoint = (base: string, path: string): string => `${base.replace(/\/+$/u, '')}${path}`

/**
 * Strip control characters before anything remote reaches a terminal.
 *
 * `/evidence` is authenticated and returns the operator's own instance, but
 * its `lastError` is still dependency-controlled text, and a terminal
 * interprets escape sequences.
 */
const forTerminal = (value: string): string =>
  // C0 and C1 alike (#300 review, P2, cubic): some terminals treat the C1
  // range as escape introducers, so stripping only C0 is not enough.
  value.replace(/[\u0000-\u001F\u007F-\u009F]+/gu, ' ').trim().slice(0, MAX_EVIDENCE_TEXT)

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const asText = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? forTerminal(value) : undefined

const asCount = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

async function getJson(
  fetchImpl: typeof fetch,
  url: string,
  opts: { token?: string; timeoutMs: number },
): Promise<{ status: number; body: unknown }> {
  const response = await fetchImpl(url, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      ...(opts.token ? { authorization: `Bearer ${opts.token}` } : {}),
    },
    signal: AbortSignal.timeout(opts.timeoutMs),
  })
  let body: unknown
  try {
    body = await response.json()
  } catch {
    body = undefined
  }
  return { status: response.status, body }
}

function legacyHealth(body: Record<string, unknown>): DeployedLegacyHealth {
  const heartbeat = asRecord(body.heartbeat)
  return {
    ...(asText(body.phase) ? { phase: asText(body.phase) } : {}),
    ...(asText(body.factoryProcess) ? { factoryProcess: asText(body.factoryProcess) } : {}),
    ...(asText(heartbeat.status) ? { heartbeatStatus: asText(heartbeat.status) } : {}),
    ...(asText(heartbeat.updatedAt) ? { heartbeatUpdatedAt: asText(heartbeat.updatedAt) } : {}),
    ...(asText(heartbeat.readinessReconcile)
      ? { readinessReconcile: asText(heartbeat.readinessReconcile) }
      : {}),
    ...(asText(heartbeat.eventListener) ? { eventListener: asText(heartbeat.eventListener) } : {}),
  }
}

function verdictFor(diagnosis: Omit<DeployedFactoryDiagnosis, 'verdict' | 'dispatching'>): {
  dispatching: boolean
  verdict: string
} {
  if (!diagnosis.reachable) {
    return {
      dispatching: false,
      verdict: `unreachable: ${diagnosis.url} did not answer (${diagnosis.errorClass ?? 'no response'})`,
    }
  }
  // Before any subsystem reading: a snapshot served by a container whose
  // daemon has stopped updating it describes a process that is no longer
  // there. The instance already said so.
  if (diagnosis.live === false) {
    if (diagnosis.phase !== undefined && BOOT_PHASES.has(diagnosis.phase)) {
      return {
        dispatching: false,
        verdict:
          `not dispatching yet: the instance is still starting (phase ${diagnosis.phase}). ` +
          'A cold container hydrates its Relayfile mirror before the first pass, which #36 measured ' +
          'at up to 61 minutes; re-run this in a few minutes before treating it as wedged.',
      }
    }
    return {
      dispatching: false,
      verdict:
        `not dispatching: the instance reports itself not live (HTTP ${diagnosis.httpStatus ?? '?'}` +
        `${diagnosis.phase ? `, phase ${diagnosis.phase}` : ''}). ` +
        'Its loop heartbeat is stale or the Factory process is gone, so any health block it still ' +
        'serves describes the last write, not the present.',
    }
  }
  if (diagnosis.unreadable) {
    return {
      dispatching: false,
      verdict:
        `cannot tell: the endpoint answered HTTP ${diagnosis.httpStatus ?? '?'} and carried no Factory ` +
        'health. Something other than the instance may be answering this URL — a gateway, an auth ' +
        'proxy, or a load balancer. Check the URL, then pass --token to read /evidence.',
    }
  }
  if (diagnosis.workerOnly) {
    return {
      dispatching: false,
      verdict:
        'cannot tell: this deployment answers /healthz at the Worker without probing the container ' +
        '(event-driven short-sleep), so the response is Worker liveness and carries no Factory ' +
        'health. Pass --token to read /evidence, which does reach the container.',
    }
  }
  const health = diagnosis.health
  if (!health) {
    const legacy = diagnosis.legacy ?? {}
    if (!legacy.readinessReconcile && !legacy.eventListener) {
      return {
        dispatching: false,
        verdict:
          'cannot tell: this response carried no Factory health at all — no diagnostics block and ' +
          'no subsystem state strings. Pass --token to read /evidence.',
      }
    }
    return {
      dispatching: false,
      verdict:
        'cannot tell: this instance predates the /healthz diagnostics block (#295), so it publishes ' +
        `state strings only — readinessReconcile=${legacy.readinessReconcile ?? 'unknown'}, ` +
        `eventListener=${legacy.eventListener ?? 'unknown'}. ` +
        'Upgrade the deployed Factory, or pass --token to read /evidence.',
    }
  }
  if (health.stale || health.loopStatus === 'stopping') {
    return {
      dispatching: false,
      verdict: `not dispatching: ${health.reason ?? 'the loop heartbeat is not current'}`,
    }
  }
  const readiness = health.readinessReconcile
  if (readiness?.state === 'stalled') {
    const missed = readiness.missedPasses ?? 0
    return {
      dispatching: false,
      verdict:
        `not dispatching: the readiness sweep is stalled — one pass has been in flight for ` +
        `${formatDuration(readiness.inFlightMs)} (${missed} missed passes at ` +
        `${formatDuration(readiness.intervalMs)} cadence). The loop only re-arms when a sweep ` +
        'settles, so a hung pass stops dispatch permanently.',
    }
  }
  if (readiness && (readiness.state === 'degraded' || readiness.state === 'retrying')) {
    return {
      dispatching: false,
      verdict:
        `not dispatching: readinessReconcile is ${readiness.state} after ` +
        `${readiness.consecutiveFailures} consecutive failures ` +
        `(threshold ${readiness.failureThreshold}), last failure class ` +
        `${readiness.lastErrorClass ?? 'unknown'}. ` +
        'Pass --token to read the message at /evidence.',
    }
  }
  if (health.eventListener?.state === 'not-listening') {
    return {
      dispatching: false,
      verdict: 'not dispatching: the daemon is not listening for Relayfile events.',
    }
  }
  // #303. A wedged batch is the one dispatch-gating condition that fails
  // without failing: nothing throws, no counter moves, and every other line in
  // this report reads green while no issue can be promoted out of `queued`.
  const capacity = health.dispatchCapacity
  if (capacity?.state === 'stalled') {
    const agentless = capacity.agentlessOccupants ?? 0
    return {
      dispatching: false,
      verdict:
        // `longestWaitMs` is the oldest *queue* wait, not how long the slots
        // have been held; saying otherwise sends an operator looking for an
        // occupant that has been there that long (#303 review, cubic).
        `not dispatching: ${capacity.waiting} issue(s) have been waiting for batch capacity for up ` +
        `to ${formatDuration(capacity.longestWaitMs)}, with ${capacity.active}/${capacity.batchSize} ` +
        'slot(s) occupied' +
        (agentless > 0
          ? `. ${agentless} occupied slot(s) never placed an agent and are past the ` +
            `${formatDuration(capacity.agentlessHoldTimeoutMs)} reap deadline, so they cannot finish ` +
            'on their own'
          : '') +
        '. Pass --token to read /evidence for the issues holding the slots.',
    }
  }
  if (health.status === 'unknown') {
    return {
      dispatching: false,
      verdict:
        'cannot tell: the health block did not report a status this version understands. ' +
        'Pass --token to read /evidence.',
    }
  }
  if (health.status !== 'ok') {
    return { dispatching: false, verdict: `not dispatching: ${health.reason ?? 'a subsystem is degraded'}` }
  }
  // Review follow-up on #300 (P1, cubic). An empty `degradedSubsystems` on a
  // block that never reported the readiness sweep is an absence of evidence,
  // not evidence of health.
  if (!readiness || readiness.state === 'unknown') {
    return {
      dispatching: false,
      verdict:
        'cannot tell: the health block carries no readiness-reconcile state, so nothing here says ' +
        'whether discovery is running. Pass --token to read /evidence.',
    }
  }
  if (readiness.state === 'not-running') {
    return {
      dispatching: false,
      verdict:
        'not dispatching: the readiness loop is not running — this instance is not a live daemon.',
    }
  }
  // #355. Everything above is a subsystem verdict, and every one of them read
  // green through a total dispatch outage: a healthy sub-second sweep declined
  // seven eligible issues and this line still said "dispatching". The last
  // sweep's own arithmetic is the only thing on this surface that can
  // contradict that, so it goes in the sentence rather than three lines below
  // it.
  return {
    dispatching: true,
    verdict:
      'dispatching: readinessReconcile is healthy' +
      (readiness?.intervalMs ? ` on a ${formatDuration(readiness.intervalMs)} cadence` : '') +
      `, and the event listener is ${health.eventListener?.state ?? 'unknown'}.` +
      ` Last enumerating sweep: ${formatSweepOutcome(readiness)}.`,
  }
}

/**
 * The last enumerating sweep, as a phrase that keeps zero and absent apart (#355).
 *
 * `candidates: 0` says discovery pulled nothing and the bug is upstream of
 * eligibility; no `candidates` at all says this instance has not enumerated a
 * sweep — or predates the counters — and says nothing about either. Rendering
 * both as `0` would recreate the ambiguity the field exists to remove.
 */
export function formatSweepOutcome(
  readiness: FactoryPublicReadinessReconcileHealth | undefined,
): string {
  if (readiness?.enumerationCountsInvalid) {
    return 'not attributable (the report supplied an incomplete or invalid count snapshot; ' +
      'whether an earlier pass enumerated is unknown)'
  }
  if (!readiness || readiness.candidates === undefined) {
    // A deferred pass publishes the marker with no counts. It proves only why
    // the LATEST pass did not enumerate: an earlier startup attempt may have
    // failed before it could publish any outcome (#359 review).
    return readiness?.discoveryDeferred
      ? 'nothing has enumerated successfully yet — the most recent pass deferred ' +
        'to another process holding the discovery lease'
      : 'not reported (no sweep has enumerated, or this instance predates the counters)'
  }
  if (readiness.discoveryDeferred && readiness.lastEnumeratedAtMs === undefined) {
    // The immediately preceding producer overwrote its trio with zeroes on a
    // deferral and had no enumeration timestamp. Those values cannot be
    // attributed to a real provider read, so label the record as legacy/unknown
    // instead of presenting it under "Last enumerating sweep" (#359 review).
    return 'not attributable (legacy deferred report has counts without an enumeration timestamp; ' +
      'the most recent pass deferred to another process holding the discovery lease)'
  }
  return `${readiness.candidates} candidate(s), ${readiness.dispatched ?? 0} dispatched, ` +
    `${readiness.skipped ?? 0} skipped` +
    formatTreeReadReading(readiness) +
    (readiness.discoveryDeferred
      // Name the instant, not just "an earlier pass" (#359 review): retained
      // counts sit beside an ever-fresh `lastCompletedAtMs`, so without this
      // an operator cannot tell a measurement one interval old from one days
      // old, and the staleness is the whole reason to say anything at all.
      ? `${readiness.lastEnumeratedAtMs === undefined
          ? ''
          : ` measured ${formatInstant(readiness.lastEnumeratedAtMs)}`} — the most recent pass deferred ` +
        'to another process holding the discovery lease and enumerated nothing'
      : '')
}

/**
 * The tree-read pair, turned into the sentence an operator needs (#363 review).
 *
 * The numbers reaching `/healthz` is half the fix; the other half is that
 * `candidates: 0, treeReads: 3, emptyTreeReads: 3` and
 * `candidates: 0, treeReads: 3, emptyTreeReads: 1` mean opposite things and
 * nothing on this surface said which was which. The first is a mount serving
 * nothing at all — dispatch is dead upstream of eligibility; the second is a
 * workspace with no ready work.
 *
 * Silent about a zero `treeReads`: a sweep that issued no enumerating read
 * (deferred, or shed) has nothing to report here, and a ratio over zero reads
 * is not a fact about the mount.
 */
export function formatTreeReadReading(
  readiness: FactoryPublicReadinessReconcileHealth | undefined,
): string {
  const treeReads = readiness?.treeReads
  const emptyTreeReads = readiness?.emptyTreeReads
  if (treeReads === undefined || emptyTreeReads === undefined || treeReads === 0) return ''
  return emptyTreeReads === treeReads
    ? ` — every one of ${treeReads} tree read(s) came back empty: the mount served nothing at all,` +
      ' so a zero candidate count here is not evidence the workspace is empty'
    : ` (${emptyTreeReads}/${treeReads} tree read(s) empty — the mount served content)`
}

/** The skip breakdown, ordered as the record carries it. */
export function formatSkipReasons(counts: Record<string, number>): string {
  const entries = Object.entries(counts).filter(([, count]) => count > 0)
  return entries.length === 0 ? '—' : entries.map(([code, count]) => `${code}=${count}`).join(', ')
}

export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return 'unknown'
  if (ms < 1_000) return `${ms}ms`
  const seconds = Math.floor(ms / 1_000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
}

/** Read a deployed instance's public health, plus `/evidence` when a token is held. */
export async function diagnoseDeployedFactory(
  options: DiagnoseDeployedOptions,
): Promise<DeployedFactoryDiagnosis> {
  const fetchImpl = options.fetch ?? fetch
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const url = options.url.replace(/\/+$/u, '')

  let base: Omit<DeployedFactoryDiagnosis, 'verdict' | 'dispatching'>
  try {
    const health = await getJson(fetchImpl, endpoint(url, '/healthz'), { timeoutMs })
    const body = asRecord(health.body)
    // The container serves the daemon's block inside its heartbeat projection;
    // accept a top-level copy too, so a proxy that hoists it still works.
    const published = normalizePublicHealth(body.health ?? asRecord(body.heartbeat).health)
    const workerOnly = body.eventDrivenSleep === true || body.container === 'not-probed'
    // A container answering its own health says `ok`; the 503 path is the
    // service-level negative. Anything else that answers on this URL — a
    // gateway 404, an auth proxy 401, a load balancer 502 — never asked the
    // container, and cannot support a statement about Factory.
    // The container's health response always carries a top-level `ok`. A
    // status code alone is not enough: a gateway can answer 200 with an error
    // page or 503 with its own, and neither asked the container (#300 review,
    // cubic).
    const instanceAnswered = typeof body.ok === 'boolean'
    base = {
      url,
      reachable: true,
      httpStatus: health.status,
      ...(instanceAnswered
        ? { live: health.status === 200 && body.ok !== false }
        : { unreadable: true }),
      ...(asText(body.phase) ? { phase: asText(body.phase) } : {}),
      ...(workerOnly ? { workerOnly: true } : {}),
      ...(published ? { health: published } : { legacy: legacyHealth(body) }),
    }
  } catch (error) {
    base = { url, reachable: false, errorClass: telemetryErrorClass(error) }
  }

  if (base.reachable) {
    base = { ...base, evidence: await readEvidence(fetchImpl, url, options.token, timeoutMs) }
  }

  return { ...base, ...verdictFor(base) }
}

async function readEvidence(
  fetchImpl: typeof fetch,
  url: string,
  token: string | undefined,
  timeoutMs: number,
): Promise<DeployedEvidenceSummary> {
  if (!token) {
    return {
      fetched: false,
      reason:
        'no operator token supplied — pass --token or set FACTORY_EVIDENCE_TOKEN to read the ' +
        'free-text lastError at /evidence',
    }
  }
  try {
    const evidence = await getJson(fetchImpl, endpoint(url, '/evidence'), { token, timeoutMs })
    if (evidence.status !== 200) {
      // Only 401/403 are statements about the credential. A 404 means this
      // deployment exposes no /evidence route and a 5xx means the endpoint
      // failed — sending someone to rotate a working token for either is the
      // wrong-problem failure again (#300 review, CodeRabbit).
      const reason = evidence.status === 401 || evidence.status === 403
        ? `/evidence answered HTTP ${evidence.status}; the token was not accepted`
        : evidence.status === 404
          ? `/evidence answered HTTP 404; this deployment exposes no /evidence route (the token is not the problem)`
          : `/evidence request failed with HTTP ${evidence.status}; the endpoint errored (the token is not the problem)`
      return { fetched: false, httpStatus: evidence.status, reason }
    }
    const body = asRecord(evidence.body)
    const readiness = asRecord(body.readinessReconcile)
    const fleetConnect = asRecord(body.fleetConnect)
    return {
      fetched: true,
      httpStatus: evidence.status,
      ...(asText(body.phase) ? { phase: asText(body.phase) } : {}),
      ...(asText(readiness.lastError) ? { lastError: asText(readiness.lastError) } : {}),
      ...(asText(fleetConnect.lastError)
        ? { fleetConnectLastError: asText(fleetConnect.lastError) }
        : {}),
      ...(asCount(readiness.consecutiveFailures) !== undefined
        ? { consecutiveFailures: asCount(readiness.consecutiveFailures) }
        : {}),
    }
  } catch (error) {
    return { fetched: false, reason: `/evidence request failed (${telemetryErrorClass(error)})` }
  }
}

/** Human-readable rendering; `--json` prints the diagnosis object instead. */
export function renderDeployedDiagnosis(diagnosis: DeployedFactoryDiagnosis): string {
  const lines: string[] = [`factory diagnose — ${diagnosis.url}`]
  lines.push(
    `  reachable            : ${diagnosis.reachable ? `yes (HTTP ${diagnosis.httpStatus ?? '?'})` : `no (${diagnosis.errorClass ?? 'no response'})`}`,
  )

  if (diagnosis.phase !== undefined) {
    lines.push(`  phase                : ${diagnosis.phase}`)
  }
  if (diagnosis.live === false) {
    lines.push('  instance liveness    : NOT LIVE (the instance\'s own verdict, on its own clock)')
  }

  const health = diagnosis.health
  if (health) {
    // First, because it is the first question of every outage: is the fix I
    // merged actually running? (#446). An instance older than that change
    // publishes no `build` at all, and saying so is the answer — not a gap.
    lines.push(
      health.build
        ? `  build                : ${health.build.version} @ ${health.build.commit}`
        : '  build                : not reported (instance predates the build identity field)',
    )
    lines.push(`  liveness (ok)        : ${health.ok} (as of the last heartbeat write)`)
    lines.push(`  status               : ${health.status}`)
    lines.push(
      `  loop                 : ${health.loopStatus ?? 'unknown'}, heartbeat ${formatDuration(health.ageMs)} old${health.stale ? ' (STALE)' : ''}`,
    )
    if (health.degradedSubsystems.length > 0) {
      lines.push(`  degraded subsystems  : ${health.degradedSubsystems.join(', ')}`)
    }
    const readiness = health.readinessReconcile
    if (readiness) {
      lines.push('  readinessReconcile:')
      lines.push(`    state              : ${readiness.state}`)
      lines.push(
        `    consecutiveFailures: ${readiness.consecutiveFailures} (threshold ${readiness.failureThreshold})`,
      )
      lines.push(`    lastErrorClass     : ${readiness.lastErrorClass ?? '—'}`)
      lines.push(`    cadence            : ${formatDuration(readiness.intervalMs)}`)
      if (readiness.inFlightMs !== undefined) {
        lines.push(
          `    pass in flight     : ${formatDuration(readiness.inFlightMs)} (${readiness.missedPasses ?? 0} missed passes)`,
        )
      }
      lines.push(`    lastStartedAt      : ${formatInstant(readiness.lastStartedAtMs)}`)
      lines.push(`    lastCompletedAt    : ${formatInstant(readiness.lastCompletedAtMs)}`)
      lines.push(`    lastFailureAt      : ${formatInstant(readiness.lastFailureAtMs)}`)
      // #355. `candidates === 0` and an absent `candidates` are opposite
      // diagnoses, so the renderer must not collapse them into one dash.
      lines.push(`    last sweep         : ${formatSweepOutcome(readiness)}`)
      lines.push(`    lastEnumeratedAt   : ${formatInstant(readiness.lastEnumeratedAtMs)}`)
      if (readiness.skipReasons) {
        lines.push(`    skip reasons       : ${formatSkipReasons(readiness.skipReasons)}`)
      }
      // Rendered whenever the producer reports it, zero included: `skipReasons`
      // drops a zero bucket, so without this line "no dispatch failed" and "this
      // daemon cannot tell you" look identical to a reader (#355).
      if (readiness.dispatchFailures !== undefined) {
        lines.push(
          `    dispatch failures  : ${readiness.dispatchFailures}${
            readiness.dispatchFailureReasons
              ? ` (${formatSkipReasons(readiness.dispatchFailureReasons)})`
              : ''
          }`,
        )
      }
      // Rendered whenever the producer reports the pair, zeroes included: the
      // raw numbers are what an operator diffs across two samples, and the
      // reading beside them is what they act on (#363 review).
      if (readiness.treeReads !== undefined && readiness.emptyTreeReads !== undefined) {
        lines.push(
          `    tree reads         : ${readiness.treeReads} served, ${readiness.emptyTreeReads} empty` +
            (readiness.treeReads > 0 && readiness.emptyTreeReads === readiness.treeReads
              ? ' — SILENT MOUNT: it served nothing at all'
              : ''),
        )
      }
    }
    const capacity = health.dispatchCapacity
    if (capacity) {
      lines.push('  dispatchCapacity:')
      lines.push(`    state              : ${capacity.state}`)
      lines.push(`    slots              : ${capacity.active}/${capacity.batchSize} occupied`)
      lines.push(`    waiting            : ${capacity.waiting} issue(s)`)
      lines.push(
        `    longest queue wait : ${formatDuration(capacity.longestWaitMs)} (warn past ${formatDuration(capacity.waitWarnMs)})`,
      )
      if (capacity.agentlessOccupants !== undefined) {
        lines.push(
          `    unreaped wedges    : ${capacity.agentlessOccupants} occupied slot(s) never placed an ` +
          `agent and are past the ${formatDuration(capacity.agentlessHoldTimeoutMs)} reap deadline`,
        )
      }
    }
    const fleetConnect = health.fleetConnect
    if (fleetConnect) {
      lines.push('  fleetConnect:')
      lines.push(`    state              : ${fleetConnect.state}`)
      if (fleetConnect.state === 'dialed') {
        lines.push('    confirmation       : unconfirmed — the SDK accepted connect(), but no stream event has been observed; a healthy silent workspace may remain dialed')
      }
      lines.push(`    attempts           : ${fleetConnect.attempts ?? '—'}`)
      lines.push(`    lastAttemptAt      : ${formatInstant(fleetConnect.lastAttemptAtMs)}`)
      lines.push(`    lastDialedAt       : ${formatInstant(fleetConnect.lastDialedAtMs)}`)
      lines.push(`    firstEventAt       : ${formatInstant(fleetConnect.firstEventAtMs)}`)
      lines.push(`    lastConnectedAt    : ${formatInstant(fleetConnect.lastConnectedAtMs)}`)
      lines.push(`    lastFailureAt      : ${formatInstant(fleetConnect.lastFailureAtMs)}`)
    }
    lines.push(`  eventListener        : ${health.eventListener?.state ?? 'unknown'}`)
  } else if (diagnosis.unreadable) {
    lines.push('  health block         : none — this response carried no Factory health')
  } else if (diagnosis.workerOnly) {
    // Not an old instance — an unprobed one. Saying "predates #295" here would
    // send an operator to upgrade a Factory that is fine.
    lines.push('  health block         : not requested — the Worker answered without probing the container')
    lines.push('  mode                 : event-driven short-sleep')
  } else if (diagnosis.legacy?.readinessReconcile || diagnosis.legacy?.eventListener) {
    lines.push('  health block         : absent — state strings only (instance predates #295)')
    lines.push(`  readinessReconcile   : ${diagnosis.legacy.readinessReconcile ?? 'unknown'}`)
    lines.push(`  eventListener        : ${diagnosis.legacy.eventListener ?? 'unknown'}`)
  } else {
    // No block and no state strings: the response carried no Factory health at
    // all. That is a statement about this response, not about the instance.
    lines.push('  health block         : absent, and no subsystem state strings in the response')
  }

  const evidence = diagnosis.evidence
  if (evidence) {
    lines.push(
      `  evidence             : ${evidence.fetched ? `read (HTTP ${evidence.httpStatus ?? 200})` : `not read — ${evidence.reason ?? 'unavailable'}`}`,
    )
    if (evidence.lastError) lines.push(`    lastError          : ${evidence.lastError}`)
    if (evidence.fleetConnectLastError) {
      lines.push(`    fleetConnect error : ${evidence.fleetConnectLastError}`)
    }
  }

  lines.push('')
  lines.push(`verdict: ${diagnosis.verdict}`)
  return `${lines.join('\n')}\n`
}

const formatInstant = (ms: number | undefined): string => {
  if (ms === undefined) return '—'
  // Belt and braces with `normalizePublicHealth`'s range check: a renderer
  // asked to explain an outage must never be the thing that throws.
  const instant = new Date(ms)
  return Number.isNaN(instant.getTime()) ? 'unknown' : instant.toISOString()
}
