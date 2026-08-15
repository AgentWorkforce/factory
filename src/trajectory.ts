export type TrajectoryWorkUnitSurface = 'linear' | 'github' | 'factory'

export interface TrajectoryPointer {
  workUnitId: string
  workUnitSurface: TrajectoryWorkUnitSurface
  sessionRef?: string
}

export const MISSING_TRAJECTORY_SESSION_REF = 'missing'

const TRAJECTORY_POINTER_PATTERN =
  /<!-- trajectory: work_unit_id=([^\s>]+) work_unit_surface=(linear|github|factory) session_ref=([^\s>]+) -->/gu

const AI_HIST_SESSION_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu
const NIL_SESSION_UUID = '00000000-0000-0000-0000-000000000000'

/**
 * The accepted value comes only from a real session-id source at dispatch
 * admission. Requiring its canonical UUID representation prevents placeholder,
 * agent-name, and ticket-derived strings from masquerading as coverage without
 * asking Factory to resolve or read the access-controlled session contents.
 */
export function resolvableTrajectorySessionRef(value: string | undefined): string | undefined {
  const normalized = value?.trim()
  if (!normalized || !AI_HIST_SESSION_UUID.test(normalized) || normalized.toLowerCase() === NIL_SESSION_UUID) {
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
