import { describe, expect, it } from 'vitest'

import {
  FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
  READINESS_RECONCILE_STALL_INTERVALS,
  normalizePublicHealth,
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

describe('dispatch capacity health (#303)', () => {
  const capacity = (overrides: Partial<NonNullable<FactoryLoopHeartbeat['dispatchCapacity']>> = {}) => heartbeat({
    dispatchCapacity: {
      batchSize: 1,
      active: 1,
      waiting: 3,
      waitWarnMs: 30 * 60_000,
      agentlessHoldTimeoutMs: 30 * 60_000,
      longestWaitMs: 6 * 60 * 60_000,
      // `recordPlanned` wrote a spec and the spawn never returned, so the row
      // reports an agent and no placement — the shape the projection must not
      // mistake for a healthy occupant (#303 review).
      occupants: [{
        issue: 'AR-303',
        phase: 'dispatching',
        agents: 1,
        placedAgents: 0,
        slotHeldForMs: 13 * 60 * 60_000,
      }],
      waitingIssues: ['AR-304', 'AR-305', 'AR-306'],
      ...overrides,
    },
  })

  it('reports a long capacity wait as a dispatch-gating degradation', () => {
    const health = publicHealthFromHeartbeat(capacity(), { nowMs: BOOT_MS + 1_000 })

    expect(health.dispatchCapacity).toEqual({
      state: 'stalled',
      batchSize: 1,
      active: 1,
      waiting: 3,
      waitWarnMs: 30 * 60_000,
      agentlessHoldTimeoutMs: 30 * 60_000,
      longestWaitMs: 6 * 60 * 60_000,
      agentlessOccupants: 1,
      // #315: the count alone cannot separate one stuck occupant from
      // reap-and-reacquire churn. The age can, and the id lets two samples be
      // compared without ever naming the issue.
      occupants: [{
        id: expect.stringMatching(/^[0-9a-f]{12}$/),
        placedAgents: 0,
        slotHeldForMs: 13 * 60 * 60_000,
        pastReapDeadline: true,
      }],
    })
    expect(health.degradedSubsystems).toContain('dispatchCapacity')
    expect(health.status).toBe('degraded')
    // Liveness must not move: recycling the container would destroy the
    // evidence of the wedge and carry the durable lock into the replacement.
    expect(health.ok).toBe(true)
  })

  // The wedge signature must not fire on a dispatch that is merely mid-spawn.
  // `recordPlanned` writes the spec first, so every healthy dispatch has zero
  // placements until its spawn returns — minutes, for a cloud placement.
  it('does not count a dispatch still inside its spawn window as a wedge', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        occupants: [{
          issue: 'AR-307',
          phase: 'dispatching',
          agents: 1,
          placedAgents: 0,
          slotHeldForMs: 90_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.agentlessOccupants).toBeUndefined()
    // The wait itself is still reported: capacity is the outage signal here,
    // and only the "cannot finish on its own" claim is withheld.
    expect(health.dispatchCapacity?.state).toBe('stalled')
  })

  // The reaper skips only while `nowMs < dueAtMs`, so it reaps AT the
  // deadline. A strict `>` here would make the diagnostic disagree with the
  // mechanism it reports on for that instant.
  it('counts a never-placed slot that reached its reap deadline exactly', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        occupants: [{
          issue: 'AR-303',
          phase: 'dispatching',
          agents: 1,
          placedAgents: 0,
          slotHeldForMs: 30 * 60_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.agentlessHoldTimeoutMs).toBe(30 * 60_000)
    expect(health.dispatchCapacity?.agentlessOccupants).toBe(1)
  })

  it('survives a corrupted occupants collection rather than dropping the block', () => {
    for (const occupants of ['not-an-array', null, [null], [42], [{ placedAgents: 0 }]]) {
      const health = publicHealthFromHeartbeat(
        capacity({ occupants: occupants as never }),
        { nowMs: BOOT_MS + 1_000 },
      )

      expect(health.dispatchCapacity).toMatchObject({ state: 'stalled', waiting: 3 })
      expect(health.dispatchCapacity?.agentlessOccupants).toBeUndefined()
    }
  })


  // #315: the monitor stayed green through the exact condition it exists to
  // catch. `agentlessOccupants` was computed and then dropped: with nothing
  // queued behind the wedge, `waiting === 0` short-circuited the state to
  // `healthy` while half a two-slot batch was held by a row past its own reap
  // deadline. Nothing is waiting *because* dispatch is down — the moment it
  // resumes, that is a halved batch running into a backlog.
  it('does not report healthy while an occupant is past its own reap deadline', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        batchSize: 2,
        active: 1,
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [{
          issue: 'AR-315',
          phase: 'running',
          agents: 0,
          placedAgents: 0,
          slotHeldForMs: 40 * 60_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.agentlessOccupants).toBe(1)
    expect(health.dispatchCapacity?.state).not.toBe('healthy')
    expect(health.dispatchCapacity?.state).toBe('stalled')
    expect(health.degradedSubsystems).toContain('dispatchCapacity')
  })

  // #315: a count cannot distinguish one stuck occupant from reap-and-reacquire
  // churn — both read 1 on every sample. A monotonically growing age against a
  // stable id settles it in one request instead of 34.
  it('publishes a stable identity and age per occupant, without the issue key', () => {
    const sample = (slotHeldForMs: number) => publicHealthFromHeartbeat(
      capacity({
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [
          { issue: 'AR-315', phase: 'running', agents: 0, placedAgents: 0, slotHeldForMs },
          { issue: 'AR-318', phase: 'running', agents: 2, placedAgents: 2, slotHeldForMs: 5_000 },
        ],
      }),
      { nowMs: BOOT_MS + 1_000 },
    ).dispatchCapacity

    const first = sample(40 * 60_000)
    const second = sample(41 * 60_000)

    // Same occupant across two samples: the id holds, the age advances. That is
    // the reading a count cannot give.
    expect(first?.occupants?.[0]?.id).toBe(second?.occupants?.[0]?.id)
    expect(second?.occupants?.[0]?.slotHeldForMs).toBeGreaterThan(first?.occupants?.[0]?.slotHeldForMs ?? 0)
    expect(first?.occupants?.[0]?.pastReapDeadline).toBe(true)

    // Distinct occupants stay distinguishable, and the healthy one is not
    // mislabelled as a wedge.
    expect(first?.occupants?.[1]?.id).not.toBe(first?.occupants?.[0]?.id)
    expect(first?.occupants?.[1]?.pastReapDeadline).toBeUndefined()
    expect(first?.occupants?.[1]?.placedAgents).toBe(2)

    // The redaction #303 established still holds: no issue key on the wire.
    expect(JSON.stringify(first)).not.toContain('AR-315')
    expect(JSON.stringify(first)).not.toContain('AR-318')
  })

  // #315: a record that carries the wedge in its occupants cannot be
  // re-published as healthy on the strength of its own stale state string.
  it('will not launder a stale healthy state over a wedged occupant', () => {
    const normalized = normalizePublicHealth({
      ...publicHealthFromHeartbeat(capacity({ waiting: 0, longestWaitMs: undefined }), { nowMs: BOOT_MS + 1_000 }),
      dispatchCapacity: {
        state: 'healthy',
        batchSize: 2,
        active: 1,
        waiting: 0,
        waitWarnMs: 30 * 60_000,
        agentlessHoldTimeoutMs: 30 * 60_000,
        occupants: [{ id: 'abcdef123456', placedAgents: 0, slotHeldForMs: 40 * 60_000 }],
      },
    })

    expect(normalized.dispatchCapacity?.state).toBe('stalled')
    expect(normalized.dispatchCapacity?.occupants?.[0]?.id).toBe('abcdef123456')
    expect(normalized.dispatchCapacity?.occupants?.[0]?.pastReapDeadline).toBe(true)
  })

  // Two occupants that both arrive without an issue key must not collapse onto
  // one id: distinct rows sharing an identity read as a single stuck slot,
  // which is the exact misreading this field exists to prevent (#315).
  it('keeps occupants distinguishable when the producer sent no issue key', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        occupants: [
          { phase: 'running', agents: 0, placedAgents: 0, slotHeldForMs: 40 * 60_000 },
          { phase: 'running', agents: 0, placedAgents: 0, slotHeldForMs: 50 * 60_000 },
        ] as never,
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    const ids = health.dispatchCapacity?.occupants?.map((occupant) => occupant.id) ?? []
    expect(ids).toHaveLength(2)
    expect(new Set(ids).size).toBe(2)
  })

  it('keeps issue keys behind the authenticated surface', () => {
    const health = publicHealthFromHeartbeat(capacity(), { nowMs: BOOT_MS + 1_000 })

    expect(JSON.stringify(health)).not.toContain('AR-30')
  })

  it('treats an ordinary full batch as healthy backpressure', () => {
    const health = publicHealthFromHeartbeat(
      // The shared fixture carries a wedged occupant, which is anything but
      // ordinary — spell out a placed one so this really is the healthy case
      // it claims to be (#315).
      capacity({
        longestWaitMs: 60_000,
        occupants: [{
          issue: 'AR-303',
          phase: 'running',
          agents: 2,
          placedAgents: 2,
          slotHeldForMs: 60_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.state).toBe('waiting')
    expect(health.degradedSubsystems).not.toContain('dispatchCapacity')
    expect(health.status).toBe('ok')
  })

  it('re-derives the state when a remote record carries an unrecognised one', () => {
    const normalized = normalizePublicHealth({
      ...publicHealthFromHeartbeat(capacity(), { nowMs: BOOT_MS + 1_000 }),
      dispatchCapacity: {
        state: 'catastrophically-fine',
        batchSize: 1,
        active: 1,
        waiting: 3,
        waitWarnMs: 30 * 60_000,
        agentlessHoldTimeoutMs: 30 * 60_000,
        longestWaitMs: 6 * 60 * 60_000,
      },
    })

    expect(normalized?.dispatchCapacity?.state).toBe('stalled')
  })

  it('omits the block entirely for an instance that predates it', () => {
    const health = publicHealthFromHeartbeat(heartbeat(), { nowMs: BOOT_MS + 1_000 })

    expect(health.dispatchCapacity).toBeUndefined()
    expect(health.degradedSubsystems).not.toContain('dispatchCapacity')
  })
})

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
  // Review follow-up on #300 (P2, codex). `starting` is the state a live
  // daemon reports before `#startLiveSubscription` installs the subscription:
  // no listener is registered, so reporting green would be the same false
  // green this issue exists to remove. Startup can be lengthy.
  it('treats a listener that is still starting as not yet dispatch-capable', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({ eventListener: { state: 'starting' } }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.degradedSubsystems).toContain('eventListener')
    expect(health.status).toBe('degraded')
    // Still alive — this is amber during startup, not a dead process.
    expect(health.ok).toBe(true)
  })

  it('does not fault the listener on an instance that is not running live', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        eventListener: { state: 'not-listening', reason: 'factory mode is dispatch-owner' },
        readinessReconcile: { state: 'not-running', consecutiveFailures: 0, failureThreshold: 3 },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    // A bounded `factory loop` is not supposed to be listening; only a live
    // daemon's silence is a fault.
    expect(health.degradedSubsystems).toEqual([])
    expect(health.status).toBe('ok')
  })

  // Review follow-up on #300 (P2, codex). A finite number is not a valid date:
  // `new Date(1e300).toISOString()` throws, and a remote record reaches a
  // renderer that would abort the whole diagnosis.
  it('drops timestamps outside the representable Date range', () => {
    const health = normalizePublicHealth({
      schemaVersion: 1,
      ok: true,
      status: 'ok',
      stale: false,
      updatedAtMs: 1e300,
      loopStatus: 'running',
      degradedSubsystems: [],
      readinessReconcile: {
        state: 'healthy',
        consecutiveFailures: 0,
        failureThreshold: 3,
        lastStartedAtMs: 1e300,
        lastCompletedAtMs: -1e300,
        lastFailureAtMs: Number.MAX_VALUE,
        intervalMs: 60_000,
      },
    })

    expect(health?.updatedAtMs).toBeUndefined()
    expect(health?.readinessReconcile?.lastStartedAtMs).toBeUndefined()
    expect(health?.readinessReconcile?.lastCompletedAtMs).toBeUndefined()
    expect(health?.readinessReconcile?.lastFailureAtMs).toBeUndefined()
    // Durations are not dates and stay as they are.
    expect(health?.readinessReconcile?.intervalMs).toBe(60_000)
  })

  it('drops an out-of-range timestamp written into the heartbeat itself', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'healthy',
          consecutiveFailures: 0,
          failureThreshold: 3,
          intervalMs: 60_000,
          lastStartedAtMs: 1e300,
        },
      }),
      { nowMs: BOOT_MS },
    )

    expect(health.readinessReconcile?.lastStartedAtMs).toBeUndefined()
    expect(health.readinessReconcile?.inFlightMs).toBeUndefined()
  })
  // Review follow-up on #300 (P1, cubic). An open fleet control-plane circuit
  // rejects every spawn and resume, so dispatch is gated just as hard as by a
  // failing readiness sweep — and the health record said nothing about it.
  it('reports an open fleet control-plane circuit as dispatch-gating', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        fleetControlPlane: {
          state: 'open',
          consecutiveFailures: 4,
          timeoutMs: 10_000,
          failureThreshold: 3,
          resetTimeoutMs: 30_000,
          lastFailureAtMs: BOOT_MS - 5_000,
          retryAtMs: BOOT_MS + 25_000,
          lastError: 'roster probe failed: connect ECONNREFUSED /run/relay/broker.sock',
        },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.degradedSubsystems).toContain('fleetControlPlane')
    expect(health.status).toBe('degraded')
    expect(health.fleetControlPlane).toMatchObject({
      state: 'open',
      consecutiveFailures: 4,
      retryAtMs: BOOT_MS + 25_000,
    })
    // MUST-NOT-FIRE: the circuit's lastError is free text with a socket path.
    const rendered = JSON.stringify(health)
    expect(rendered).not.toContain('/run/relay/broker.sock')
    expect(rendered).not.toContain('ECONNREFUSED')
  })

  it('does not fault a closed fleet control-plane circuit', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        fleetControlPlane: {
          state: 'closed',
          consecutiveFailures: 0,
          timeoutMs: 10_000,
          failureThreshold: 3,
          resetTimeoutMs: 30_000,
        },
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.degradedSubsystems).toEqual([])
    expect(health.fleetControlPlane).toMatchObject({ state: 'closed' })
  })

  // Review follow-up on #300 (P2, cubic). A zero cadence made every in-flight
  // pass instantly stalled and `missedPasses` Infinity, which JSON renders as
  // null — a broken record about a working sweep.
  it('falls back to the default cadence when the recorded interval is not positive', () => {
    const health = publicHealthFromHeartbeat(
      heartbeat({
        readinessReconcile: {
          state: 'healthy',
          consecutiveFailures: 0,
          failureThreshold: 3,
          intervalMs: 0,
          lastStartedAtMs: BOOT_MS - 120_000,
          lastCompletedAtMs: BOOT_MS - 180_000,
        },
      }),
      { nowMs: BOOT_MS },
    )

    expect(health.readinessReconcile?.state).toBe('healthy')
    expect(health.readinessReconcile?.missedPasses).toBe(2)
    expect(Number.isFinite(health.readinessReconcile?.missedPasses ?? 0)).toBe(true)
    expect(health.readinessReconcile?.intervalMs).toBeUndefined()
  })
  // Review follow-up on #300 (Minor, CodeRabbit). The writer refuses to publish
  // a non-positive cadence; a reader that accepts one from a remote process
  // undoes that guarantee for everyone downstream of it.
  it('re-applies the writer cadence and sign invariants when reading a remote record', () => {
    const health = normalizePublicHealth({
      schemaVersion: 1,
      ok: true,
      status: 'degraded',
      stale: false,
      loopStatus: 'running',
      degradedSubsystems: ['readinessReconcile'],
      readinessReconcile: {
        state: 'stalled',
        consecutiveFailures: 3,
        failureThreshold: 3,
        intervalMs: 0,
        inFlightMs: -5_000,
        missedPasses: -12,
        lastDurationMs: -1,
      },
    })

    expect(health?.readinessReconcile?.intervalMs).toBeUndefined()
    expect(health?.readinessReconcile?.inFlightMs).toBeUndefined()
    expect(health?.readinessReconcile?.missedPasses).toBeUndefined()
    expect(health?.readinessReconcile?.lastDurationMs).toBeUndefined()
    // The states and counters still come through: dropping a bad duration must
    // not cost the operator the signal.
    expect(health?.readinessReconcile).toMatchObject({ state: 'stalled', consecutiveFailures: 3 })
  })
  // Review follow-up on #300 (P2, cubic). "1.5 missed passes" is not a thing.
  it('reports missed passes as a whole number', () => {
    const health = normalizePublicHealth({
      schemaVersion: 1,
      ok: true,
      status: 'degraded',
      stale: false,
      loopStatus: 'running',
      degradedSubsystems: ['readinessReconcile'],
      readinessReconcile: {
        state: 'stalled',
        consecutiveFailures: 0,
        failureThreshold: 3,
        intervalMs: 60_000,
        inFlightMs: 90_000,
        missedPasses: 1.5,
      },
    })

    expect(health?.readinessReconcile?.missedPasses).toBe(1)
    // A duration is genuinely fractional; only the count is not.
    expect(health?.readinessReconcile?.inFlightMs).toBe(90_000)
  })
})

