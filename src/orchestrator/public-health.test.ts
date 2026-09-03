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
      agentHoldTimeoutMs: 4 * 60 * 60_000,
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
      agentHoldTimeoutMs: 4 * 60 * 60_000,
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


  // #419 must-fire: an occupant with a placed agent held past
  // `agentHoldTimeoutMs` degrades the subsystem and reaches the top-level
  // status. Before #419 the state derivation only checked the agentless
  // shape, so a slot whose placed agent had gone offline (or whose run
  // exceeded any plausible duration) held its slot indefinitely while the
  // subsystem still read `healthy`. The deployed daemon at 2026-08-31T11:03Z
  // exhibited exactly this: two occupants with `placedAgents: 1` held their
  // slots 13.5 hours (27× `agentlessHoldTimeoutMs`, 3.4× the default
  // `agentHoldTimeoutMs`) while `dispatchCapacity.state` read `healthy` and
  // no `degradedSubsystems` was published.
  it('degrades when a placed-agent occupant is past agentHoldTimeoutMs (#419)', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        batchSize: 2,
        active: 2,
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [
          // The exact observed shape: agent placed, slot held 13.5h.
          {
            issue: 'AR-418',
            phase: 'dispatching',
            agents: 1,
            placedAgents: 1,
            heldForMs: 13 * 60 * 60_000 + 30 * 60_000,
            slotHeldForMs: 13 * 60 * 60_000 + 30 * 60_000,
          },
          {
            issue: 'AR-417',
            phase: 'dispatching',
            agents: 1,
            placedAgents: 1,
            heldForMs: 13 * 60 * 60_000 + 30 * 60_000,
            slotHeldForMs: 13 * 60 * 60_000 + 30 * 60_000,
          },
        ],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    // The wedge shape is a distinct count from the agentless shape: an
    // operator reading /healthz has to know which reaper it points at.
    expect(health.dispatchCapacity?.occupiedOccupants).toBe(2)
    expect(health.dispatchCapacity?.agentlessOccupants).toBeUndefined()
    // Per-occupant flag agrees with the aggregate count: a payload that
    // contradicts itself here republishes the very defect this test covers.
    expect(health.dispatchCapacity?.occupants?.every((occupant) => occupant.pastOccupiedDeadline)).toBe(true)
    // The whole point: the state must not read healthy any more. `stalled`
    // gates dispatch exactly as hard as a failing sweep.
    expect(health.dispatchCapacity?.state).toBe('stalled')
    expect(health.degradedSubsystems).toContain('dispatchCapacity')
    expect(health.status).toBe('degraded')
    // Liveness never moves: recycling the container destroys the evidence of
    // the wedge and re-imports the durable lock into the replacement, which
    // is what turned this bug into a 13.5-hour outage rather than a
    // 30-minute one (the #303 doc-comment discipline).
    expect(health.ok).toBe(true)
    // The bound has to be visible next to `agentlessHoldTimeoutMs`, or the
    // reader cannot tell which reaper is late.
    expect(health.dispatchCapacity?.agentHoldTimeoutMs).toBe(4 * 60 * 60_000)
  })

  // #419 boundary: the reaper skips only while `nowMs < dueAtMs`, so it
  // reaps AT the deadline. `pastOccupiedDeadline` uses `>=` for the same
  // reason `pastReapDeadline` does — a diagnostic that regressed to `>`
  // would disagree with the mechanism it reports on for exactly one
  // millisecond, which is the failure mode this whole PR closes. The
  // agentless shape pins this instant with its own test; the placed shape
  // pins it here.
  it('counts a placed-agent slot that reached agentHoldTimeoutMs exactly (#419)', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        batchSize: 2,
        active: 2,
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [{
          issue: 'AR-501',
          phase: 'running',
          agents: 1,
          placedAgents: 1,
          heldForMs: 4 * 60 * 60_000,
          slotHeldForMs: 4 * 60 * 60_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.occupiedOccupants).toBe(1)
    expect(health.dispatchCapacity?.occupants?.[0]?.pastOccupiedDeadline).toBe(true)
    expect(health.dispatchCapacity?.state).toBe('stalled')
    expect(health.degradedSubsystems).toContain('dispatchCapacity')
  })

  // #419 must-not-fire: a placed-agent occupant INSIDE `agentHoldTimeoutMs`
  // is left alone. The reaper skips only while `nowMs < dueAtMs`, so a
  // diagnostic that fired one second earlier would disagree with the
  // mechanism it reports on for an entire second — the exact failure mode
  // #303 review closed for the agentless side.
  it('leaves a placed-agent occupant within agentHoldTimeoutMs untouched (#419)', () => {
    const health = publicHealthFromHeartbeat(
      capacity({
        batchSize: 2,
        active: 2,
        waiting: 0,
        longestWaitMs: undefined,
        waitingIssues: [],
        occupants: [{
          issue: 'AR-500',
          phase: 'running',
          agents: 1,
          placedAgents: 1,
          // One millisecond before the deadline. `heldForMs` is the clock
          // the reaper anchors on; `slotHeldForMs` is deliberately larger
          // (a slow spawn) to prove the diagnostic reads the same clock.
          heldForMs: 4 * 60 * 60_000 - 1,
          slotHeldForMs: 5 * 60 * 60_000,
        }],
      }),
      { nowMs: BOOT_MS + 1_000 },
    )

    expect(health.dispatchCapacity?.occupiedOccupants).toBeUndefined()
    expect(health.dispatchCapacity?.occupants?.[0]?.pastOccupiedDeadline).toBeUndefined()
    // A genuinely healthy run is not degraded: the whole point of this
    // must-not-fire pair is that the new bound must not read fresh dispatch
    // as a wedge.
    expect(health.dispatchCapacity?.state).toBe('healthy')
    expect(health.degradedSubsystems).not.toContain('dispatchCapacity')
    expect(health.status).toBe('ok')
  })

  // #419: a producer that publishes occupants without the
  // `occupiedOccupants` count still projects the wedge, mirroring the
  // #318 discipline for the agentless shape. Without this, a rolled-back
  // reader would let a 13.5-hour placed-agent occupant slip through with
  // `state: 'stalled'` under `status: 'ok'`.
  it('normalizes a placed-agent wedge over the wire without the aggregate count (#419)', () => {
    const normalized = normalizePublicHealth({
      ...publicHealthFromHeartbeat(
        capacity({ waiting: 0, longestWaitMs: undefined }),
        { nowMs: BOOT_MS + 1_000 },
      ),
      dispatchCapacity: {
        state: 'healthy',
        batchSize: 2,
        active: 2,
        waiting: 0,
        waitWarnMs: 30 * 60_000,
        agentlessHoldTimeoutMs: 30 * 60_000,
        agentHoldTimeoutMs: 4 * 60 * 60_000,
        occupants: [{
          id: 'deadbeef1234',
          placedAgents: 1,
          heldForMs: 13 * 60 * 60_000,
          slotHeldForMs: 13 * 60 * 60_000,
        }],
      },
    })

    expect(normalized?.dispatchCapacity?.state).toBe('stalled')
    expect(normalized?.dispatchCapacity?.occupants?.[0]?.pastOccupiedDeadline).toBe(true)
    expect(normalized?.dispatchCapacity?.occupiedOccupants).toBe(1)
    expect(normalized?.degradedSubsystems).toContain('dispatchCapacity')
    expect(normalized?.status).toBe('degraded')
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

  /**
   * The stanza published `intervalMs` — a scheduler tick that cannot preempt
   * anything — and neither of the two deadlines that can. A reader watching
   * `inFlightMs` climb 1:1 with wall clock beside it had no field that could
   * distinguish "bounded, but the bound is 90 minutes away" from "nothing will
   * ever stop this", and the second reading has now been reported twice off
   * this exact stanza. Publishing the bounds is what makes them falsifiable.
   */
  it('publishes the deadlines that can preempt a sweep, not just the cadence that cannot', () => {
    const health = publicHealthFromHeartbeat(heartbeat({
      readinessReconcile: {
        state: 'healthy',
        consecutiveFailures: 0,
        failureThreshold: 3,
        intervalMs: 60_000,
        timeoutMs: 5_400_000,
        sweepBudgetMs: 5_400_000,
        lastStartedAtMs: BOOT_MS - 268_232,
        inFlightSinceMs: BOOT_MS - 268_232,
      },
    }), { nowMs: BOOT_MS })

    expect(health.readinessReconcile?.timeoutMs).toBe(5_400_000)
    expect(health.readinessReconcile?.sweepBudgetMs).toBe(5_400_000)
    // The production reading that produced the misdiagnosis: a pass 268s old on
    // a 60s cadence. `missedPasses` says how far in, the budget says how far to
    // go, and the two together are what `intervalMs` alone could not say.
    expect(health.readinessReconcile?.missedPasses).toBe(4)
  })

  /**
   * The trivially wrong version of the change above publishes a zero for a
   * daemon that recorded no bound, which reads as an instant deadline rather
   * than an unknown one. Absent and zero are different facts here for the same
   * reason they are for `candidates` (#355).
   */
  it('omits an unset or non-positive bound instead of publishing it as zero', () => {
    const health = normalizePublicHealth({
      schemaVersion: 1,
      ok: true,
      status: 'ok',
      stale: false,
      loopStatus: 'running',
      degradedSubsystems: [],
      readinessReconcile: {
        state: 'healthy',
        consecutiveFailures: 0,
        failureThreshold: 3,
        intervalMs: 60_000,
        timeoutMs: 0,
        sweepBudgetMs: -1,
      },
    })

    expect(health?.readinessReconcile?.timeoutMs).toBeUndefined()
    expect(health?.readinessReconcile?.sweepBudgetMs).toBeUndefined()
    // And an instance that predates the fields at all still projects cleanly.
    const legacy = publicHealthFromHeartbeat(heartbeat(), { nowMs: BOOT_MS })
    expect(legacy.readinessReconcile?.timeoutMs).toBeUndefined()
    expect(legacy.readinessReconcile?.sweepBudgetMs).toBeUndefined()
    expect(legacy.readinessReconcile?.state).toBe('healthy')
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

  // The measurement this whole block exists for: the live container publishes
  // `skipReasons: { 'dispatch-failed': 5 }` on every sweep, with the breaker
  // closed and readiness healthy, and that bucket is a count with no cause
  // attached. The daemon knows the cause; it writes it to stdout, which does
  // not reach the deployed operator.
  it('breaks the dispatch-failed bucket down by cause, and the parts sum to its total', () => {
    const readiness = swept({
      candidates: 27,
      dispatched: 0,
      skipped: 27,
      skipReasons: { 'not-ready': 21, 'parked-dependency': 1, 'dispatch-failed': 5 },
      dispatchFailures: 5,
      dispatchFailureReasons: { 'spawn-ack-timeout': 3, 'unclassified-dispatch': 2 },
    })

    expect(readiness?.dispatchFailures).toBe(5)
    expect(readiness?.dispatchFailureReasons)
      .toEqual({ 'spawn-ack-timeout': 3, 'unclassified-dispatch': 2 })
    expect(Object.values(readiness?.dispatchFailureReasons ?? {}).reduce((sum, n) => sum + n, 0))
      .toBe(readiness?.dispatchFailures)
    // ...and to the bucket it refines, which is the integrity check a reader
    // holding both numbers can actually run.
    expect(readiness?.dispatchFailures).toBe(readiness?.skipReasons?.['dispatch-failed'])
  })

  // #410/#412 follow-up. The stale-terminal reconcile is silent by design — it
  // answers every uncertainty by leaving the row alone — so from outside, "the
  // repair is firing", "its reads are throwing and being swallowed", "it is
  // losing a race" and "its preconditions are never met" all look identical:
  // `lifecycle-terminal` simply persists. These three counts are what separate
  // them, which is why they are published whole or not at all.
  it('publishes the stale-terminal reconcile outcome as a whole group', () => {
    const readiness = swept({
      candidates: 20,
      dispatched: 0,
      skipped: 20,
      staleTerminalReopens: { cleared: 0, conflicts: 0, failures: 0 },
    })

    // An all-zero group is a DIAGNOSIS, not an absence: the reconcile ran and
    // its preconditions were never met. Coercing it away would delete exactly
    // the reading that says the refused rows are not the shape it targets.
    expect(readiness?.staleTerminalReopens).toEqual({ cleared: 0, conflicts: 0, failures: 0 })
  })

  it('drops the stale-terminal group rather than publishing a partial one', () => {
    // A reader seeing `cleared` without `failures` would take the missing field
    // for a zero and call a silently-erroring repair healthy.
    // Every case asserts the SWEEP BLOCK SURVIVES as well as the group being
    // dropped. Without that, these assertions also pass when `swept()` returns
    // `undefined` and the whole `readinessReconcile` block is gone — which
    // would be a far worse bug than the one under test (coderabbitai, #444).
    for (const malformed of [
      // Each member missing in turn, not just one.
      { conflicts: 0, failures: 0 },
      { cleared: 8, failures: 0 },
      { cleared: 8, conflicts: 0 },
      // ...and each member invalid in turn.
      { cleared: -1, conflicts: 0, failures: 0 },
      { cleared: 8, conflicts: -1, failures: 0 },
      { cleared: 8, conflicts: 0, failures: -1 },
      { cleared: 1.5, conflicts: 0, failures: 0 },
      { cleared: 8, conflicts: 1.5, failures: 0 },
      { cleared: 8, conflicts: 0, failures: Number.NaN },
      { cleared: '8', conflicts: 0, failures: 0 },
      { cleared: 8, conflicts: null, failures: 0 },
      null,
      'nope',
      42,
    ]) {
      const rejected = swept({
        candidates: 20,
        dispatched: 0,
        skipped: 20,
        staleTerminalReopens: malformed,
      })
      expect(Object.hasOwn(rejected ?? {}, 'staleTerminalReopens')).toBe(false)
      expect(rejected?.candidates).toBe(20)
      expect(rejected?.dispatched).toBe(0)
      expect(rejected?.skipped).toBe(20)
    }
  })

  // #444 review, chatgpt-codex-connector P1 and coderabbitai. The group is
  // cumulative and independent of any sweep's arithmetic, so it must survive
  // the enumeration gate — a sweep that never completes publishes no trio and
  // would otherwise take the early return with the reconcile's outcome inside
  // it. That is precisely the wedged-sweep outage this field exists to
  // diagnose, so gating it on the trio would hide it exactly when it is needed.
  it('publishes the stale-terminal group even when no enumerating sweep completed', () => {
    const noSweep = swept({ staleTerminalReopens: { cleared: 0, conflicts: 0, failures: 4 } })
    expect(noSweep?.staleTerminalReopens).toEqual({ cleared: 0, conflicts: 0, failures: 4 })
    // ...and it does not fabricate a sweep that never happened.
    expect(Object.hasOwn(noSweep ?? {}, 'candidates')).toBe(false)

    // Same on the invalid-trio path, which returns `enumerationCountsInvalid`.
    const brokenTrio = swept({ candidates: 20, staleTerminalReopens: { cleared: 8, conflicts: 0, failures: 0 } })
    expect(brokenTrio?.staleTerminalReopens).toEqual({ cleared: 8, conflicts: 0, failures: 0 })
    expect(brokenTrio?.enumerationCountsInvalid).toBe(true)
  })

  it('keeps the sweep block when a producer does not know the stale-terminal group', () => {
    // Independently optional, for the reason `dispatchFailures` is: an older
    // daemon publishes the trio and nothing else, and requiring this field
    // would drop its whole sweep block — deleting the counters that are
    // currently the only view of the outage.
    const older = swept({ candidates: 20, dispatched: 0, skipped: 20 })
    // The WHOLE trio, not just `candidates`: asserting one field passes even if
    // the other two were dropped for this producer (coderabbitai, #444).
    expect(older?.candidates).toBe(20)
    expect(older?.dispatched).toBe(0)
    expect(older?.skipped).toBe(20)
    expect(Object.hasOwn(older ?? {}, 'staleTerminalReopens')).toBe(false)
  })

  // THE CONTROL. `skipReasons` omits zero counts, so on that field alone
  // "every dispatch succeeded" is the same absence as "this producer has never
  // heard of dispatch failures" — and 0.1.72 is in production right now being
  // exactly the second thing. `dispatchFailures` is the field that separates
  // them, and it only does that if nothing coerces its absence to a zero or
  // its zero to an absence.
  it('keeps a zero, an absence and a producer without the field three different readings', () => {
    const noneFailed = swept({
      candidates: 4,
      dispatched: 4,
      skipped: 0,
      dispatchFailures: 0,
    })
    expect(noneFailed?.dispatchFailures).toBe(0)
    expect(Object.hasOwn(noneFailed ?? {}, 'dispatchFailures')).toBe(true)
    // No breakdown, because there is nothing to break down — and the total
    // still says so out loud.
    expect(Object.hasOwn(noneFailed ?? {}, 'dispatchFailureReasons')).toBe(false)

    // A 0.1.72 daemon: the trio it does publish must survive intact, or this
    // change would delete the counters that are currently the only view of the
    // outage.
    const olderProducer = swept({ candidates: 4, dispatched: 4, skipped: 0 })
    expect(olderProducer).toMatchObject({ candidates: 4, dispatched: 4, skipped: 0 })
    expect(Object.hasOwn(olderProducer ?? {}, 'dispatchFailures')).toBe(false)

    // No sweep has completed at all.
    const neverRan = swept()
    expect(Object.hasOwn(neverRan ?? {}, 'dispatchFailures')).toBe(false)
  })

  it('re-reads its own published zero without turning it back into an absence', () => {
    const published = swept({ candidates: 4, dispatched: 4, skipped: 0, dispatchFailures: 0 })
    const reread = normalizePublicHealth({
      schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: published,
    })
    expect(reread?.readinessReconcile?.dispatchFailures).toBe(0)
    expect(Object.hasOwn(reread?.readinessReconcile ?? {}, 'dispatchFailures')).toBe(true)
  })

  // MUST-NOT-FIRE, the same leak vector #358 closed one level up: the
  // breakdown's *keys* arrive from a remote record, and an object key is as
  // publishable as a value.
  it('rebuilds the dispatch-failure breakdown from its own vocabulary', () => {
    const readiness = swept({
      candidates: 9,
      dispatched: 0,
      skipped: 9,
      dispatchFailures: 9,
      dispatchFailureReasons: {
        'spawn-ack-timeout': 4,
        // Not in the vocabulary, and carrying exactly what must never publish.
        ['AR-350 /linear/issues/AR-350__uuid.json']: 3,
        ['Error: connect ECONNREFUSED 10.0.0.4:443']: 2,
      } as Record<string, number>,
    })

    expect(JSON.stringify(readiness)).not.toContain('AR-350')
    expect(JSON.stringify(readiness)).not.toContain('/linear/issues')
    expect(JSON.stringify(readiness)).not.toContain('ECONNREFUSED')
    // Folded into `other`, not dropped, so the parts still sum to the total and
    // a reader comparing them detects a newer producer rather than a broken
    // counter.
    expect(readiness?.dispatchFailureReasons).toEqual({ 'spawn-ack-timeout': 4, other: 5 })
    expect(Object.values(readiness?.dispatchFailureReasons ?? {}).reduce((sum, n) => sum + n, 0))
      .toBe(9)
  })

  it('drops counts a reader cannot use, and a breakdown that has lost its total', () => {
    expect(swept({
      candidates: 1,
      dispatched: 0,
      skipped: 1,
      dispatchFailures: 1,
      dispatchFailureReasons: {
        'spawn-ack-timeout': Number.NaN,
        'timed-out': -3,
        'live-state-changed': 0,
      } as Record<string, number>,
    })?.dispatchFailureReasons).toBeUndefined()

    // A breakdown with no total is an orphan: nothing to check the parts
    // against, which is the one integrity check this surface offers.
    const orphaned = swept({
      candidates: 1,
      dispatched: 0,
      skipped: 1,
      dispatchFailureReasons: { 'timed-out': 1 } as Record<string, number>,
    })
    expect(Object.hasOwn(orphaned ?? {}, 'dispatchFailureReasons')).toBe(false)
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
        dispatchFailures: 7,
        dispatchFailureReasons: { '/srv/agent-workforce/.relay/workspace-key': 7 },
      },
    })
    expect(JSON.stringify(reread)).not.toContain('workspace-key')
    expect(reread?.readinessReconcile?.dispatchFailureReasons).toEqual({ other: 7 })
  })

  // MUST-FIRE (#363 review, codex P1). The tree-read pair reached the internal
  // `status()` object and stopped there: `sweepOutcome()` did not read it and
  // `normalizePublicHealth()` therefore stripped it, so the one surface a
  // deployed operator can actually reach — the unauthenticated `/healthz`
  // route behind `factory diagnose --deployed` — still could not tell a mount
  // serving nothing from a workspace with nothing ready. That is the whole
  // fault the pair was added for, on the only instance where it is met.
  it('carries the tree-read pair onto the unauthenticated surface, and back off it', () => {
    const silentMount = swept({
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: 3,
      emptyTreeReads: 3,
    })
    expect(silentMount?.treeReads).toBe(3)
    expect(silentMount?.emptyTreeReads).toBe(3)

    // And survives the round trip a remote reader makes: the container serves
    // this block inside its heartbeat and `diagnose --deployed` re-parses it.
    const reread = normalizePublicHealth({
      schemaVersion: FACTORY_PUBLIC_HEALTH_SCHEMA_VERSION,
      ok: true,
      status: 'ok',
      stale: false,
      degradedSubsystems: [],
      readinessReconcile: silentMount,
    })
    expect(reread?.readinessReconcile?.treeReads).toBe(3)
    expect(reread?.readinessReconcile?.emptyTreeReads).toBe(3)

    // The reading that separates the two zero-candidate diagnoses survives too.
    const emptyWorkspace = swept({
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: 3,
      emptyTreeReads: 1,
    })
    expect(emptyWorkspace?.emptyTreeReads).toBeLessThan(emptyWorkspace?.treeReads ?? 0)
  })

  // MUST-NOT-FIRE. Half a pair is worse than no pair: `emptyTreeReads` alone
  // reads as "the mount is sick" on a sweep where an empty read is ordinary,
  // and `treeReads` alone says nothing about what came back. A zero pair is a
  // real measurement and must NOT collapse to an absence, and a producer that
  // has never heard of the fields must keep the trio it does publish.
  it('publishes the pair whole, or not at all', () => {
    // A sweep that issued no enumerating read at all: zeroes, not absences.
    const noReads = swept({ candidates: 0, dispatched: 0, skipped: 0, treeReads: 0, emptyTreeReads: 0 })
    expect(noReads?.treeReads).toBe(0)
    expect(Object.hasOwn(noReads ?? {}, 'emptyTreeReads')).toBe(true)

    // Half a pair from a producer we do not understand: both go.
    const halfPair = swept({ candidates: 0, dispatched: 0, skipped: 0, treeReads: 4 })
    expect(Object.hasOwn(halfPair ?? {}, 'treeReads')).toBe(false)
    expect(Object.hasOwn(halfPair ?? {}, 'emptyTreeReads')).toBe(false)
    // ...and the trio it DID publish is untouched, which is the whole reason
    // this pair is not joined to it.
    expect(halfPair).toMatchObject({ candidates: 0, dispatched: 0, skipped: 0 })

    // Arithmetically impossible: more empty reads than reads. Publishing the
    // ratio anyway would hand a reader a fabricated silent-mount verdict.
    const impossible = swept({ candidates: 0, dispatched: 0, skipped: 0, treeReads: 2, emptyTreeReads: 3 })
    expect(Object.hasOwn(impossible ?? {}, 'treeReads')).toBe(false)
    expect(Object.hasOwn(impossible ?? {}, 'emptyTreeReads')).toBe(false)

    // Unusable numbers, and a producer that predates the pair entirely.
    const rubbish = swept({
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: Number.NaN as unknown as number,
      emptyTreeReads: -1,
    })
    expect(Object.hasOwn(rubbish ?? {}, 'treeReads')).toBe(false)
    const olderProducer = swept({ candidates: 4, dispatched: 4, skipped: 0 })
    expect(Object.hasOwn(olderProducer ?? {}, 'treeReads')).toBe(false)
    expect(olderProducer).toMatchObject({ candidates: 4, dispatched: 4, skipped: 0 })
  })
})
