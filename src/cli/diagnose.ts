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
  /** Allowlisted class of a transport failure; the message is not reported. */
  errorClass?: string
  dispatching: boolean
  verdict: string
  health?: FactoryPublicHealth
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
  value.replace(/[\u0000-\u001F\u007F]+/gu, ' ').trim().slice(0, MAX_EVIDENCE_TEXT)

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
    base = {
      url,
      reachable: true,
      httpStatus: health.status,
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

  const health = diagnosis.health
  if (health) {
    lines.push(`  liveness (ok)        : ${health.ok}`)
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

const formatInstant = (ms: number | undefined): string =>
  ms === undefined ? '—' : new Date(ms).toISOString()
