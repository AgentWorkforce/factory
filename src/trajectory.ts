export type TrajectoryWorkUnitSurface = 'linear' | 'github' | 'factory'

export interface TrajectoryPointer {
  workUnitId: string
  workUnitSurface: TrajectoryWorkUnitSurface
  sessionRef?: string
}

export const MISSING_TRAJECTORY_SESSION_REF = 'missing'

const TRAJECTORY_POINTER_PATTERN =
  /<!-- trajectory: work_unit_id=([^\s>]+) work_unit_surface=(linear|github|factory) session_ref=([^\s>]+) -->/gu

const PLACEHOLDER_SESSION_REFS = /^(?:missing|none|null|placeholder|unknown)(?:[-_:].*)?$/iu
const SAFE_SESSION_REF = /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/u

/**
 * Accept only opaque, comment-safe identifiers and reject the known family of
 * placeholder values. Resolution itself remains access-controlled by the
 * relay-side session service; Factory never reads session contents.
 */
export function resolvableTrajectorySessionRef(value: string | undefined): string | undefined {
  const normalized = value?.trim()
  if (!normalized || !SAFE_SESSION_REF.test(normalized) || PLACEHOLDER_SESSION_REFS.test(normalized)) {
    return undefined
  }
  return normalized
}

export function renderTrajectoryPointer(pointer: TrajectoryPointer): string {
  const sessionRef = resolvableTrajectorySessionRef(pointer.sessionRef) ?? MISSING_TRAJECTORY_SESSION_REF
  return `<!-- trajectory: work_unit_id=${pointer.workUnitId} work_unit_surface=${pointer.workUnitSurface} session_ref=${sessionRef} -->`
}

/** Returns the one unambiguous, resolvable trajectory reference in a PR body. */
export function trajectorySessionRefFromBody(body: string): string | undefined {
  const refs = new Set<string>()
  for (const match of body.matchAll(TRAJECTORY_POINTER_PATTERN)) {
    const sessionRef = resolvableTrajectorySessionRef(match[3])
    if (sessionRef) refs.add(sessionRef)
  }
  return refs.size === 1 ? [...refs][0] : undefined
}

/** Remove inherited or untrusted pointers before Factory appends its canonical one. */
export function stripTrajectoryPointers(body: string): string {
  return body.replace(TRAJECTORY_POINTER_PATTERN, '').replace(/\n{3,}/gu, '\n\n').trim()
}
