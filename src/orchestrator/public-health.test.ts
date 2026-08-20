import { describe, expect, it } from 'vitest'

import {
  FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
  READINESS_RECONCILE_STALL_INTERVALS,
  publicHealthFromHeartbeat,
} from './public-health'
import type { FactoryLoopHeartbeat } from '../types'

const BOOT_MS = 1_787_224_000_000

function heartbeat(overrides: Partial<FactoryLoopHeartbeat> = {}): FactoryLoopHeartbeat {
  return {
    pid: 42,
    status: 'running',
    iteration: 0,
    maxIterations: 0,
    updatedAt: new Date(BOOT_MS).toISOString(),
    updatedAtMs: BOOT_MS,
    eventListener: { state: 'subscribed' },
    readinessReconcile: {
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      intervalMs: 60_000,
      lastStartedAtMs: BOOT_MS - 30_000,
      lastCompletedAtMs: BOOT_MS - 29_000,
      lastDurationMs: 1_000,
    },
    ...overrides,
  }
}

describe('publicHealthFromHeartbeat (#295)', () => {
  it('carries the failure count and an allowlisted error class', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'degraded',
          consecutiveFailures: 8,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: BOOT_MS - 30_000,
          lastCompletedAtMs: BOOT_MS - 600_000,
          lastFailureAtMs: BOOT_MS - 29_000,
          lastError: 'Refusing to dispatch AR-241: dispatch lifecycle is already terminal',
          lastErrorClass: 'TypeError',
        },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.schemaVersion).toBe(FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION)
    expect(health.readinessReconcile).toMatchObject({
      state: 'degraded',
      consecutiveFailures: 8,
      failureThreshold: 3,
      lastErrorClass: 'TypeError',
    })
    expect(health.status).toBe('degraded')
    expect(health.degradedSubsystems).toEqual(['readinessReconcile'])
  })

  // MUST-NOT-FIRE. `lastError` is a free-text, dependency-controlled string
  // that already carries provider text, filesystem paths and URLs. It is
  // readable at /evidence behind a bearer token; nothing derived from it may
  // reach the unauthenticated health surface except the allowlisted class.
  it('keeps provider text, filesystem paths, URLs and tokens off the public surface', () => {
    const hostile =
      'ENOENT: no such file or directory, open ' +
      "'/srv/agent-workforce/.relay/workspace-key' while POSTing " +
      'https://relay.internal.example.com/v1/workspaces/ws_9f2?token=sk-live-abcdef0123456789'
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'degraded',
          consecutiveFailures: 7,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: BOOT_MS - 30_000,
          lastFailureAtMs: BOOT_MS - 29_000,
          lastError: hostile,
          // A writer that puts free text where the class belongs must not be
          // able to smuggle it through either.
          lastErrorClass: hostile,
        },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    const rendered = JSON.stringify(health)
    expect(rendered).not.toContain('/srv/agent-workforce')
    expect(rendered).not.toContain('.relay/workspace-key')
    expect(rendered).not.toContain('https://')
    expect(rendered).not.toContain('relay.internal.example.com')
    expect(rendered).not.toContain('sk-live-abcdef0123456789')
    expect(rendered).not.toContain('ENOENT')
    // The operator still learns that the subsystem is failing and how often.
    expect(health.readinessReconcile?.consecutiveFailures).toBe(7)
    expect(health.readinessReconcile?.lastErrorClass).toBe('Error')
  })

  it('drops the free-text reason from the event-listener state', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        eventListener: { state: 'not-listening', reason: 'mount /srv/agent-workforce is unavailable' },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.eventListener).toEqual({ state: 'not-listening' })
    expect(JSON.stringify(health)).not.toContain('/srv/agent-workforce')
    expect(health.degradedSubsystems).toContain('eventListener')
  })

  // The observed 2026-08-20 case: every state string reads green while the
  // sweep that started at 11:16:35Z has neither completed nor failed. The
  // relative order of the two timestamps is the entire signal.
  it('derives stalled from lastStarted > lastCompleted past the stall threshold', () => {
    const startedAtMs = BOOT_MS - 77 * 60_000
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'healthy',
          consecutiveFailures: 0,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: startedAtMs,
          lastCompletedAtMs: startedAtMs - 60_003,
        },
      }),
      { nowMs: BOOT_MS },
    )

    expect(health.readinessReconcile).toMatchObject({
      state: 'stalled',
      inFlightMs: 77 * 60_000,
      missedPasses: 77,
    })
    expect(health.status).toBe('degraded')
    expect(health.degradedSubsystems).toEqual(['readinessReconcile'])
  })

  it('does not call a pass in flight for less than the stall threshold stalled', () => {
    const startedAtMs = BOOT_MS - (READINESS_RECONCILE_STALL_INTERVALS - 1) * 60_000
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'healthy',
          consecutiveFailures: 0,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: startedAtMs,
          lastCompletedAtMs: startedAtMs - 1_000,
        },
      }),
      { nowMs: BOOT_MS },
    )

    expect(health.readinessReconcile?.state).toBe('healthy')
    expect(health.readinessReconcile?.inFlightMs).toBe((READINESS_RECONCILE_STALL_INTERVALS - 1) * 60_000)
    expect(health.status).toBe('ok')
  })

  it('reports no in-flight pass when the last pass completed after it started', () => {
    const health = publicHealthFromHeartbeat(heartbeat(), { nowMs: BOOT_MS })

    expect(health.readinessReconcile?.inFlightMs).toBeUndefined()
    expect(health.readinessReconcile?.state).toBe('healthy')
    expect(health.ok).toBe(true)
    expect(health.status).toBe('ok')
  })

  // Deliverable (2). `ok` is the container ping verdict, and a 503 there
  // recycles the container — which destroys the evidence and restarts the
  // cold-start hydration. The amber goes in `status`, which no platform
  // interprets, so a monitor can alert on it without causing a restart loop.
  it('keeps ok true for a live process while status goes amber', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'degraded',
          consecutiveFailures: 8,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: BOOT_MS - 30_000,
          lastFailureAtMs: BOOT_MS - 29_000,
        },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.ok).toBe(true)
    expect(health.status).toBe('degraded')
  })

  it('reports a missing heartbeat as unknown rather than healthy', () => {
    const health = publicHealthFromHeartbeat(undefined, { nowMs: BOOT_MS })

    expect(health).toMatchObject({ ok: false, status: 'unknown', stale: true })
    expect(health.readinessReconcile).toBeUndefined()
  })

  it('reports a stale heartbeat as not ok', () => {
    const health = publicHealthFromHeartbeat(heartbeat(), { nowMs: BOOT_MS + 120_000, staleMs: 60_000 })

    expect(health).toMatchObject({ ok: false, status: 'unknown', stale: true, ageMs: 120_000 })
  })

  it('coerces hostile non-numeric counters instead of passing them through', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: '/srv/agent-workforce' as never,
          consecutiveFailures: '7; DROP TABLE' as never,
          failureThreshold: Number.NaN,
          lastStartedAtMs: 'https://example.com' as never,
        },
      }),
      { nowMs: BOOT_MS },
    )

    const rendered = JSON.stringify(health)
    expect(rendered).not.toContain('/srv/agent-workforce')
    expect(rendered).not.toContain('DROP TABLE')
    expect(rendered).not.toContain('https://')
    expect(health.readinessReconcile).toMatchObject({ state: 'unknown', consecutiveFailures: 0 })
  })
})
