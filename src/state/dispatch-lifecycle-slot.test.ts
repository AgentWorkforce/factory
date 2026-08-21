import { describe, expect, it } from 'vitest'

import { dispatchLifecycleOccupiesSlot, stampDispatchLifecycleSlot } from './dispatch-lifecycle-slot'
import type { DispatchLifecycle, DispatchLifecycleAgent } from '../ports/state'

const issue = { uuid: 'uuid-1', key: 'AR-1', path: '/linear/issues/AR-1__uuid-1.json' }

const implementer = {
  name: 'ar-1-impl-pear',
  role: 'implementer' as const,
  capability: 'spawn:codex' as const,
  repo: 'AgentWorkforce/pear',
  task: 'implement',
}

const babysitter = (releasedAtMs?: number): DispatchLifecycleAgent => ({
  name: 'ar-1-babysit',
  ...(releasedAtMs !== undefined ? { releasedAtMs } : {}),
  tracked: {
    spec: {
      name: 'ar-1-babysit',
      role: 'babysitter' as const,
      capability: 'spawn:codex' as const,
      repo: 'AgentWorkforce/pear',
      task: 'babysit',
      ownedPullRequest: { repo: 'AgentWorkforce/pear', number: 7, path: '/github/pr/7' },
    },
    result: { name: 'ar-1-babysit' },
  },
})

const lifecycle = (overrides: Partial<DispatchLifecycle> = {}): DispatchLifecycle => ({
  runId: 'run-1',
  issue,
  decision: {
    issue,
    implementers: [implementer],
    reviewer: { name: 'ar-1-review', role: 'reviewer', capability: 'spawn:codex', repo: 'AgentWorkforce/pear', task: 'review' },
    routes: [],
    confidence: 'high',
    rationale: 'test',
  } as unknown as DispatchLifecycle['decision'],
  dryRun: false,
  phase: 'running',
  agents: [],
  invocationIds: [],
  updatedAtMs: 0,
  ...overrides,
})

describe('dispatch batch-slot accounting (#303)', () => {
  it('stops counting a lifecycle whose PR is babysat', () => {
    expect(dispatchLifecycleOccupiesSlot(lifecycle({ agents: [babysitter()] }))).toBe(false)
  })

  // A released babysitter is not babysitting. Letting one satisfy the handoff
  // drops the lifecycle out of `batchSize` accounting while nothing is watching
  // its PR — admission over-subscribes and the reaper stops bounding a row it
  // still needs to bound.
  it('keeps counting a lifecycle whose only babysitter has been released', () => {
    expect(dispatchLifecycleOccupiesSlot(lifecycle({ agents: [babysitter(1_000)] }))).toBe(true)
  })

  it('reads the release stamp from the tracked agent too', () => {
    const released = babysitter()
    released.tracked = { ...released.tracked, releasedAtMs: 1_000 }

    expect(dispatchLifecycleOccupiesSlot(lifecycle({ agents: [released] }))).toBe(true)
  })

  it('resumes the handoff once a live babysitter replaces the released one', () => {
    const replacement = babysitter()
    replacement.name = 'ar-1-babysit-2'

    expect(dispatchLifecycleOccupiesSlot(lifecycle({ agents: [babysitter(1_000), replacement] }))).toBe(false)
  })

  it('clears the slot anchor when a row stops occupying a slot', () => {
    const held = lifecycle({ slotHeldSinceAtMs: 500 })
    stampDispatchLifecycleSlot(held, held, 900)
    expect(held.slotHeldSinceAtMs).toBe(500)

    const releasing = lifecycle({ phase: 'releasing', slotHeldSinceAtMs: 500 })
    stampDispatchLifecycleSlot(releasing, releasing, 900)
    expect(releasing.slotHeldSinceAtMs).toBeUndefined()
  })

  it('carries the anchor forward from the stored row rather than restamping', () => {
    const next = lifecycle()
    stampDispatchLifecycleSlot(next, lifecycle({ slotHeldSinceAtMs: 500 }), 9_000)

    expect(next.slotHeldSinceAtMs).toBe(500)
  })
})
