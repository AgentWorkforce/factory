import { readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

// Relayfile's poll mirror reconciles every 30 seconds by default.  Three
// intervals allow one missed poll and ordinary filesystem jitter, while still
// making a stalled projection visible within 90 seconds rather than hours.
export const RELAYFILE_SYNC_INTERVAL_MS = 30 * 1000
export const STALE_RECONCILE_INTERVALS = 3
export const STALE_RECONCILE_MS = RELAYFILE_SYNC_INTERVAL_MS * STALE_RECONCILE_INTERVALS

/** Use the registered mirror cadence when available; fall back to Relayfile's default. */
export function staleReconcileMs(intervalMs: unknown): number {
  const registeredIntervalMs = typeof intervalMs === 'number' &&
    Number.isFinite(intervalMs) &&
    intervalMs >= 1_000
    ? Math.floor(intervalMs)
    : RELAYFILE_SYNC_INTERVAL_MS
  return registeredIntervalMs * STALE_RECONCILE_INTERVALS
}

type MountState = {
  workspaceId?: unknown
  lastReconcileAt?: unknown
  // Relayfile writes the active poll cadence in state.json. It is part of the
  // mount's liveness contract, so the stale threshold must follow it rather
  // than assuming the default cadence for every registered mirror.
  intervalMs?: unknown
  // The mount process pid. Older mounts wrote a top-level `pid`; SDK-launched
  // mounts record it under `daemon.pid` instead. Either may be absent.
  pid?: unknown
  daemon?: { pid?: unknown }
}

type MountPidState = {
  pid?: unknown
  workspaceId?: unknown
  localDir?: unknown
}

export function coercePid(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 ? value : undefined
}

/** Read-only liveness probe used to distinguish an attached daemon from a mount Factory owns. */
export function isMountProcessRunning(pid: number | undefined): boolean {
  if (pid === undefined) return false
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return typeof error === 'object' && error !== null && 'code' in error && error.code === 'EPERM'
  }
}

function readMountProcessPid(
  stateFilePath: string,
  acceptedWorkspaceIds: ReadonlySet<string>,
): number | undefined {
  let raw: string
  try {
    raw = readFileSync(join(dirname(stateFilePath), 'mount.pid'), 'utf8')
  } catch {
    return undefined
  }

  try {
    const parsed = JSON.parse(raw) as MountPidState | number
    if (typeof parsed === 'number') return coercePid(parsed)
    if (typeof parsed !== 'object' || parsed === null) return undefined
    const workspaceId = typeof parsed.workspaceId === 'string' ? parsed.workspaceId : undefined
    if (workspaceId && !acceptedWorkspaceIds.has(workspaceId)) return undefined
    const localDir = typeof parsed.localDir === 'string' ? parsed.localDir : undefined
    const expectedLocalDir = dirname(dirname(stateFilePath))
    if (localDir && resolve(localDir) !== resolve(expectedLocalDir)) return undefined
    return coercePid(parsed.pid)
  } catch {
    const legacyPid = Number(raw.trim())
    return coercePid(legacyPid)
  }
}

export function checkMountStaleness(
  stateFilePath: string,
  workspaceId: string,
  // Alternate identifiers that name the SAME workspace (e.g. the cloud-side
  // UUID for a `rw_` handle). The local mount records the cloud UUID in its
  // state.json, so without these aliases a handle-vs-UUID comparison would
  // report a false "workspace mismatch".
  acceptableWorkspaceIds: readonly string[] = [],
): { stale: boolean; reason?: string; pid?: number } {
  let parsed: MountState
  try {
    parsed = JSON.parse(readFileSync(stateFilePath, 'utf8')) as MountState
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT') {
      return { stale: false }
    }
    return { stale: true, reason: `mount state is unreadable: ${error instanceof Error ? error.message : String(error)}` }
  }

  if (typeof parsed !== 'object' || parsed === null) {
    return { stale: true, reason: 'mount state is unreadable: expected object' }
  }

  const registeredWorkspaceId = typeof parsed.workspaceId === 'string' ? parsed.workspaceId : undefined
  const accepted = new Set([workspaceId, ...acceptableWorkspaceIds])
  if (registeredWorkspaceId === undefined || !accepted.has(registeredWorkspaceId)) {
    return {
      stale: true,
      reason: `workspace mismatch: registered=${registeredWorkspaceId ?? 'unknown'} expected=${[...accepted].join('|')}`,
    }
  }

  // Prefer the top-level pid; fall back to the SDK-launched mount's daemon.pid.
  const pid = coercePid(parsed.pid) ??
    coercePid(parsed.daemon?.pid) ??
    readMountProcessPid(stateFilePath, accepted)

  const lastReconcileAt = typeof parsed.lastReconcileAt === 'string'
    ? Date.parse(parsed.lastReconcileAt)
    : NaN
  if (!Number.isFinite(lastReconcileAt)) {
    return { stale: true, reason: 'last reconcile timestamp is missing', pid }
  }

  const ageMs = Date.now() - lastReconcileAt
  if (ageMs > staleReconcileMs(parsed.intervalMs)) {
    return {
      stale: true,
      reason: `last reconcile ${Math.floor(ageMs / 60000)}m ago`,
      pid,
    }
  }

  if (pid === undefined) {
    // Neither a top-level pid nor daemon.pid was recorded. The fresh
    // lastReconcileAt validated above is itself proof the mount is live and
    // reconciling, so fall back to that rather than declaring a healthy mount
    // stale and forcing a spurious refresh (which previously tore down the live
    // mount and then failed to re-spawn).
    return { stale: false }
  }

  if (!isMountProcessRunning(pid)) {
    return { stale: true, reason: `mount process (pid ${pid}) is not running`, pid }
  }

  return { stale: false, pid }
}
