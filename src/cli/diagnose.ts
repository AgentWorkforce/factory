import { telemetryErrorClass } from '../observability/error-class.js'
import { normalizePublicHealth } from '../orchestrator/public-health.js'
import type { FactoryPublicHealth } from '../types'

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
   */
  live?: boolean
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
  return {
    dispatching: true,
    verdict:
      'dispatching: readinessReconcile is healthy' +
      (readiness?.intervalMs ? ` on a ${formatDuration(readiness.intervalMs)} cadence` : '') +
      `, and the event listener is ${health.eventListener?.state ?? 'unknown'}.`,
  }
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
    base = {
      url,
      reachable: true,
      httpStatus: health.status,
      live: health.status === 200 && body.ok !== false,
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
      return {
        fetched: false,
        httpStatus: evidence.status,
        reason: `/evidence answered HTTP ${evidence.status}; the token was not accepted`,
      }
    }
    const body = asRecord(evidence.body)
    const readiness = asRecord(body.readinessReconcile)
    return {
      fetched: true,
      httpStatus: evidence.status,
      ...(asText(body.phase) ? { phase: asText(body.phase) } : {}),
      ...(asText(readiness.lastError) ? { lastError: asText(readiness.lastError) } : {}),
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
    }
    lines.push(`  eventListener        : ${health.eventListener?.state ?? 'unknown'}`)
  } else if (diagnosis.legacy) {
    lines.push('  health block         : absent — state strings only (instance predates #295)')
    lines.push(`  phase                : ${diagnosis.legacy.phase ?? 'unknown'}`)
    lines.push(`  readinessReconcile   : ${diagnosis.legacy.readinessReconcile ?? 'unknown'}`)
    lines.push(`  eventListener        : ${diagnosis.legacy.eventListener ?? 'unknown'}`)
  }

  const evidence = diagnosis.evidence
  if (evidence) {
    lines.push(
      `  evidence             : ${evidence.fetched ? `read (HTTP ${evidence.httpStatus ?? 200})` : `not read — ${evidence.reason ?? 'unavailable'}`}`,
    )
    if (evidence.lastError) lines.push(`    lastError          : ${evidence.lastError}`)
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
