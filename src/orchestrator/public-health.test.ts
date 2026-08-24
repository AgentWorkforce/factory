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
    // The wedge has to reach the top-level signal, not just the nested state
    // (#318 review, codex): a `stalled` capacity under `status: 'ok'` with an
    // empty `degradedSubsystems` is the stays-green defect one layer up, and
    // the top level is what every documented consumer reads.
    expect(normalized.dispatchCapacity?.agentlessOccupants).toBe(1)
    expect(normalized.degradedSubsystems).toContain('dispatchCapacity')
    expect(normalized.status).toBe('degraded')
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

  // #318 review (CodeRabbit): an occupant that OMITS `placedAgents` must not be
  // read as a reported zero. `countAgentlessOccupants` already refuses to guess
  // from an absence; the per-occupant projection has to agree, or one payload
  // contradicts itself — and because the reader folds `pastReapDeadline` into
  // its wedge count, a mere omission would have published `status: 'degraded'`.
  it('does not read an absent placedAgents as a reported zero', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [{ issue: 'AR-319', phase: 'running', agents: 1, slotHeldForMs: 40 * 60_000 }] as never,
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    // All three readings of the same payload agree that nothing is claimed.
    expect(health.dispatchCapacity?.agentlessOccupants).toBeUndefined()
    expect(health.dispatchCapacity?.occupants?.[0]?.pastReapDeadline).toBeUndefined()
    expect(health.dispatchCapacity?.occupants?.[0]?.placedAgents).toBeUndefined()
    expect(health.dispatchCapacity?.state).toBe('healthy')
    expect(health.degradedSubsystems).not.toContain('dispatchCapacity')

    // And the same record survives a round trip without acquiring a wedge.
    const normalized = normalizePublicHealth(health)
    expect(normalized?.dispatchCapacity?.state).toBe('healthy')
    expect(normalized?.status).toBe('ok')
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


/**
 * The fleet event socket is the dial that makes Factory's own agent `online`.
 * It had no status on any surface, and readers substituted `eventListener` --
 * which is the orchestrator's ISSUE subscription, a different subsystem. That
 * conflation is why a Factory that registered an agent and never connected read
 * as healthy everywhere.
 */
describe('fleet connect health', () => {
  const withConnect = (
    overrides: Partial<NonNullable<FactoryLoopHeartbeat['fleetConnect']>> = {},
  ): FactoryLoopHeartbeat =>
    heartbeat({
      fleetConnect: {
        state: 'failed',
        attempts: 1,
        lastAttemptAtMs: BOOT_MS - 5_000,
        lastFailureAtMs: BOOT_MS - 4_000,
        lastError: 'FactoryAgentRegistrationError',
        ...overrides,
      },
    })

  it('publishes the socket state unauthenticated', () => {
    const health = publicHealthFromHeartbeat(withConnect({
      lastDialedAtMs: BOOT_MS - 4_500,
      firstEventAtMs: BOOT_MS - 4_250,
    }), { nowMs: BOOT_MS })
    expect(health.fleetConnect?.state).toBe('failed')
    expect(health.fleetConnect?.attempts).toBe(1)
    expect(health.fleetConnect?.lastDialedAtMs).toBe(BOOT_MS - 4_500)
    expect(health.fleetConnect?.firstEventAtMs).toBe(BOOT_MS - 4_250)
  })

  /** `lastError` stays behind /evidence, exactly as it does for the circuit. */
  it('never leaks the cause to the unauthenticated surface', () => {
    const health = publicHealthFromHeartbeat(withConnect(), { nowMs: BOOT_MS })
    expect(JSON.stringify(health.fleetConnect)).not.toContain('FactoryAgentRegistrationError')
    expect(Object.hasOwn(health.fleetConnect ?? {}, 'lastError')).toBe(false)
  })

  /**
   * Deliberately NOT dispatch-gating. Listing it would flip `ok` on a live
   * deployment and hand container replacement a new reason to cycle -- a
   * behaviour change well beyond publishing the fact.
   */
  it('does not change what ok means', () => {
    const health = publicHealthFromHeartbeat(withConnect(), { nowMs: BOOT_MS })
    expect(health.degradedSubsystems).not.toContain('fleetConnect')
    expect(health.ok).toBe(true)
  })

  /** CONTROL: absent stays absent rather than being invented as healthy. */
  it('omits the block entirely when the backend has no socket', () => {
    const health = publicHealthFromHeartbeat(heartbeat(), { nowMs: BOOT_MS })
    expect(health.fleetConnect).toBeUndefined()
  })

  it('retains a failed socket record through normalization without retaining lastError', () => {
    const published = publicHealthFromHeartbeat(withConnect({
      lastDialedAtMs: BOOT_MS - 4_500,
      firstEventAtMs: BOOT_MS - 4_250,
    }), { nowMs: BOOT_MS })
    const normalized = normalizePublicHealth({
      ...published,
      fleetConnect: {
        ...published.fleetConnect,
        lastError: 'connect failed to wss://relay.example?token=secret',
      },
    })

    expect(normalized?.fleetConnect).toEqual(published.fleetConnect)
    expect(Object.hasOwn(normalized?.fleetConnect ?? {}, 'lastError')).toBe(false)
  })
})

describe('sweep counters on the public surface (#355)', () => {
  const swept = (
    overrides: Partial<NonNullable<FactoryLoopHeartbeat['readinessReconcile']>> = {},
  ) => publicHealthFromHeartbeat(
    heartbeat({
      readinessReconcile: {
        state: 'healthy',
        consecutiveFailures: 0,
        failureThreshold: 3,
        intervalMs: 60_000,
        lastStartedAtMs: BOOT_MS - 30_000,
        lastCompletedAtMs: BOOT_MS - 29_000,
        lastDurationMs: 1_000,
        ...overrides,
      },
    }),
    { nowMs: BOOT_MS + 1_000 },
  ).readinessReconcile

  it('publishes a completed sweep that found nothing as zero, and one that never ran as absent', () => {
    const ran = swept({ candidates: 0, dispatched: 0, skipped: 0 })
    expect(ran?.candidates).toBe(0)
    expect(Object.hasOwn(ran ?? {}, 'candidates')).toBe(true)

    const neverRan = swept()
    expect(Object.hasOwn(neverRan ?? {}, 'candidates')).toBe(false)
    expect(Object.hasOwn(neverRan ?? {}, 'dispatched')).toBe(false)
    expect(Object.hasOwn(neverRan ?? {}, 'skipped')).toBe(false)
  })

  // A record carrying one of the three and not the others is a producer this
  // version does not understand. Publishing the fragment would invite
  // "candidates minus dispatched" arithmetic that the missing field makes
  // wrong, so the group travels whole or not at all.
  it('drops a partial trio rather than publishing a misleading fragment', () => {
    expect(swept({ candidates: 4 })).toMatchObject({ enumerationCountsInvalid: true })
    expect(Object.hasOwn(swept({ candidates: 4 }) ?? {}, 'candidates')).toBe(false)
    expect(Object.hasOwn(swept({ candidates: 4, dispatched: 1 }) ?? {}, 'candidates')).toBe(false)
    expect(swept({ candidates: 4, dispatched: 1, skipped: 3 })).toMatchObject({
      candidates: 4,
      dispatched: 1,
      skipped: 3,
    })
  })

  it('distinguishes a rejected deferred count snapshot from a genuine count-free deferral', () => {
    const rejected = swept({
      candidates: 4,
      dispatched: 'invalid' as unknown as number,
      discoveryDeferred: 'sweep-in-flight',
    })
    expect(rejected).toMatchObject({
      discoveryDeferred: 'sweep-in-flight',
      enumerationCountsInvalid: true,
    })
    expect(Object.hasOwn(rejected ?? {}, 'candidates')).toBe(false)

    const normalizedAgain = normalizePublicHealth({
      schemaVersion: 1,
      ok: true,
      status: 'ok',
      stale: false,
      loopStatus: 'running',
      degradedSubsystems: [],
      readinessReconcile: rejected,
    })
    expect(normalizedAgain?.readinessReconcile).toMatchObject({
      discoveryDeferred: 'sweep-in-flight',
      enumerationCountsInvalid: true,
    })
  })

  it('names the deferred sweep, so a zero from a held lease is not read as an empty provider', () => {
    expect(swept({ candidates: 0, dispatched: 0, skipped: 0, discoveryDeferred: 'sweep-in-flight' }))
      .toMatchObject({ candidates: 0, discoveryDeferred: 'sweep-in-flight' })
    // Independent of the trio (#358 review). A daemon whose first pass deferred
    // has no counts to publish and still has to say why, so dropping the marker
    // with the counts would leave the only surface silent about it.
    const noCounts = swept({ discoveryDeferred: 'sweep-in-flight' })
    expect(noCounts?.discoveryDeferred).toBe('sweep-in-flight')
    expect(Object.hasOwn(noCounts ?? {}, 'candidates')).toBe(false)
    // Only the one value the vocabulary has.
    expect(swept({
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      discoveryDeferred: 'whatever the producer felt like' as 'sweep-in-flight',
    })?.discoveryDeferred).toBeUndefined()
  })

  // MUST-NOT-FIRE. `skipReasons` is the only field here whose *keys* come from
  // a remote record, and an object key is as publishable as a value: a
  // producer on another version could otherwise put an issue key or a
  // filesystem path onto the unauthenticated surface by using it as one.
  it('rebuilds the skip breakdown from its own vocabulary, so no remote key can cross', () => {
    const readiness = swept({
      candidates: 9,
      dispatched: 0,
      skipped: 9,
      skipReasons: {
        'out-of-scope': 4,
        // Not in the vocabulary, and carrying exactly what must never publish.
        ["AR-350 /linear/issues/AR-350__uuid.json"]: 3,
        ['dispatch-terminal']: 2,
      } as Record<string, number>,
    })

    expect(JSON.stringify(readiness)).not.toContain('AR-350')
    expect(JSON.stringify(readiness)).not.toContain('/linear/issues')
    // Folded into `other`, not dropped: the parts still sum to `skipped`, so a
    // reader comparing them does not conclude the counter is broken.
    expect(readiness?.skipReasons).toEqual({ 'out-of-scope': 4, 'dispatch-terminal': 2, other: 3 })
    expect(Object.values(readiness?.skipReasons ?? {}).reduce((sum, n) => sum + n, 0))
      .toBe(readiness?.skipped)
  })

  it('drops counts a reader cannot use, and the breakdown entirely when it is empty', () => {
    expect(swept({
      candidates: 1,
      dispatched: 0,
      skipped: 1,
      skipReasons: {
        'out-of-scope': Number.NaN,
        'dispatch-backoff': -3,
        'not-ready': 0,
      } as Record<string, number>,
    })?.skipReasons).toBeUndefined()
    expect(swept({
      candidates: 1,
      dispatched: 0,
      skipped: 1,
      skipReasons: { 'not-ready': 1.9 } as Record<string, number>,
    })?.skipReasons).toEqual({ 'not-ready': 1 })
  })

  it('re-reads its own published record without turning a zero back into an absence', () => {
    const published = swept({ candidates: 0, dispatched: 0, skipped: 0 })
    const reread = normalizePublicHealth({
      schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: published,
    })
    expect(reread?.readinessReconcile).toMatchObject({ candidates: 0, dispatched: 0, skipped: 0 })
    expect(Object.hasOwn(reread?.readinessReconcile ?? {}, 'candidates')).toBe(true)
  })

  it('applies the same key rebuild to a record that arrived over the wire', () => {
    const reread = normalizePublicHealth({
      schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: {
        state: 'healthy',
        consecutiveFailures: 0,
        failureThreshold: 3,
        candidates: 7,
        dispatched: 0,
        skipped: 7,
        skipReasons: { '/srv/agent-workforce/.relay/workspace-key': 7 },
      },
    })
    expect(JSON.stringify(reread)).not.toContain('workspace-key')
    expect(reread?.readinessReconcile?.skipReasons).toEqual({ other: 7 })
  })
})
