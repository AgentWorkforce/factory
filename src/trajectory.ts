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
const POINTER_TOKEN = /^[^\s>]+$/u

/**
 * Accept only the opaque UUID emitted by Relay for an ai-hist session. Factory
 * does not resolve it or infer current replay availability; the authenticated
 * replay resolver owns that live, retention-aware decision.
 */
export function canonicalTrajectorySessionRef(value: string | undefined): string | undefined {
  const normalized = value?.trim()
  if (!normalized || !AI_HIST_SESSION_UUID.test(normalized) || normalized.toLowerCase() === NIL_SESSION_UUID) {
    return undefined
  }
  return normalized
}

export function renderTrajectoryPointer(pointer: TrajectoryPointer): string {
  if (!POINTER_TOKEN.test(pointer.workUnitId)) {
    throw new Error(`Trajectory work unit id must be a comment-safe token: ${pointer.workUnitId}`)
  }
  const sessionRef = canonicalTrajectorySessionRef(pointer.sessionRef) ?? MISSING_TRAJECTORY_SESSION_REF
  return `<!-- trajectory: work_unit_id=${pointer.workUnitId} work_unit_surface=${pointer.workUnitSurface} session_ref=${sessionRef} -->`
}

/**
 * Returns one unambiguous resolver input pointer. Parsing proves identity
 * shape, not live replay availability; clients must resolve workspace retention.
 */
export function trajectoryPointerFromBody(body: string): Required<TrajectoryPointer> | undefined {
  const pointers = new Map<string, Required<TrajectoryPointer>>()
  for (const match of body.matchAll(TRAJECTORY_POINTER_PATTERN)) {
    const sessionRef = canonicalTrajectorySessionRef(match[3])
    const workUnitId = match[1]
    const workUnitSurface = match[2] as TrajectoryWorkUnitSurface | undefined
    if (!sessionRef || !workUnitId || !workUnitSurface) continue
    const pointer = { workUnitId, workUnitSurface, sessionRef }
    pointers.set(`${workUnitId}:${workUnitSurface}:${sessionRef}`, pointer)
  }
  return pointers.size === 1 ? [...pointers.values()][0] : undefined
}

export function trajectorySessionRefFromBody(body: string): string | undefined {
  return trajectoryPointerFromBody(body)?.sessionRef
}

/** Remove inherited pointers before Factory appends its single canonical one. */
export function stripTrajectoryPointers(body: string): string {
  return body.replace(TRAJECTORY_POINTER_PATTERN, '').replace(/\n{3,}/gu, '\n\n').trim()
}
