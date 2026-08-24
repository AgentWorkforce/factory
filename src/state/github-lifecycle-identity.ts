import type { DispatchLifecycle } from '../ports/state'

/**
 * Stable GitHub issue identity derived from a Relayfile sense path.
 *
 * Relayfile can surface the same issue through a slugged meta.json record and
 * a compact by-id alias; both resolve here to one identity. This is the
 * path-derived input to `dispatchIssueIdentity`, which prefers a structurally
 * declared origin when the surface is a mirror.
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
