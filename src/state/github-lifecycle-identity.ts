import type { DispatchLifecycle } from '../ports/state'

/**
 * Stable GitHub issue identity shared by lifecycle stores.
 *
 * Relayfile can surface the same issue through a slugged meta.json record and
 * a compact by-id alias. Lifecycle keys historically include the source path,
 * so stores must compare the logical GitHub identity before creating a new row.
 */
export const githubLifecycleIdentity = (
  lifecycle: Pick<DispatchLifecycle, 'issue'>,
): string | undefined => {
  const path = lifecycle?.issue?.path
  if (!path) return undefined
  const match = path.match(
    /^\/github\/repos\/(?:([^/]+)\/([^/]+)|([A-Za-z0-9-]+)__([^/]+))\/issues\/(?:(?:by-id\/)?(\d+)\.json|(\d+)(?:__[^/]*)?\/(?:meta|metadata)\.json)$/u,
  )
  const owner = match?.[1] ?? match?.[3]
  const repo = match?.[2] ?? match?.[4]
  const number = Number(match?.[5] ?? match?.[6])
  if (!owner || !repo || !Number.isSafeInteger(number) || number <= 0) return undefined
  return `${owner.toLowerCase()}/${repo.toLowerCase()}#${number}`
}

export const matchingGithubLifecycleEntry = <T extends DispatchLifecycle>(
  lifecycles: Iterable<[string, T]>,
  seed: Pick<DispatchLifecycle, 'issue'>,
): [string, T] | undefined => {
  const identity = githubLifecycleIdentity(seed)
  if (!identity) return undefined
  let best: [string, T] | undefined
  for (const entry of lifecycles) {
    if (githubLifecycleIdentity(entry[1]) !== identity) continue
    if (!best || compareLifecycleCandidates(entry, best) < 0) best = entry
  }
  return best
}

const compareLifecycleCandidates = (
  [leftKey, left]: [string, DispatchLifecycle],
  [rightKey, right]: [string, DispatchLifecycle],
): number => lifecycleRank(left) - lifecycleRank(right) ||
  (right.updatedAtMs ?? 0) - (left.updatedAtMs ?? 0) ||
  leftKey.localeCompare(rightKey)

const lifecycleRank = (lifecycle: DispatchLifecycle): number => {
  if (lifecycle.phase !== 'complete' && lifecycle.phase !== 'abandoned' && lifecycle.phase !== 'queued') return 0
  if (lifecycle.phase === 'queued') return 1
  return 2
}
