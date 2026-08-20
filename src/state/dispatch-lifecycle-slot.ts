import { githubRepositoriesMatch } from '../github/repo-identity'
import type { DispatchLifecycle, DispatchLifecyclePhase } from '../ports/state'

/**
 * Batch-slot accounting for durable dispatch lifecycles.
 *
 * `dispatch.batchSize` is enforced by counting the rows in a slot-occupying
 * phase, so this predicate is what decides whether another issue can ever be
 * promoted out of `queued`. It used to be copy-pasted into both state stores;
 * #303 added a second reader (the reaper's never-placed deadline), and three
 * copies of a predicate that gates all dispatch is one too many.
 */

/** Phases whose lifecycle counts against `dispatch.batchSize`. */
export const dispatchPhaseOccupiesSlot = (phase: DispatchLifecyclePhase | undefined): boolean =>
  phase !== undefined &&
  phase !== 'queued' &&
  phase !== 'waiting-for-human' &&
  phase !== 'releasing' &&
  phase !== 'complete' &&
  phase !== 'abandoned'

export const dispatchLifecycleHandedOffToBabysitters = (lifecycle: DispatchLifecycle): boolean => {
  const implementerRepos = [...new Set(lifecycle.decision.implementers.map((spec) => spec.repo))]
  if (implementerRepos.length === 0) return false
  const babysitterRepos = lifecycle.agents
    .filter((agent) => agent.tracked.spec.role === 'babysitter')
    .map((agent) => agent.tracked.spec.ownedPullRequest?.repo)
    .filter((repo): repo is string => Boolean(repo))
  return implementerRepos.every((repo) => babysitterRepos.some((ownedRepo) =>
    githubRepositoriesMatch(repo, ownedRepo)))
}

export const dispatchLifecycleOccupiesSlot = (lifecycle: DispatchLifecycle): boolean =>
  dispatchPhaseOccupiesSlot(lifecycle.phase) && !dispatchLifecycleHandedOffToBabysitters(lifecycle)

/**
 * Stamp the wall-clock instant this row took its batch slot (#303).
 *
 * `updatedAtMs` cannot serve as this clock: `renewDispatchLifecycle` bumps it
 * every `DISPATCH_LIFECYCLE_RENEW_MS`, so a permanently wedged row looks
 * freshly touched forever. `heldSinceAtMs` cannot either — it is only set once
 * a placement succeeds, and the whole point of the defect is a row that never
 * got one. This is the only anchor a never-placed occupant has, so it is
 * carried forward across saves and cleared the moment the row stops occupying
 * a slot.
 */
export const stampDispatchLifecycleSlot = (
  lifecycle: DispatchLifecycle,
  previous: DispatchLifecycle | undefined,
  nowMs: number,
): void => {
  if (!dispatchLifecycleOccupiesSlot(lifecycle)) {
    delete lifecycle.slotHeldSinceAtMs
    return
  }
  lifecycle.slotHeldSinceAtMs ??= previous?.slotHeldSinceAtMs ?? nowMs
}
