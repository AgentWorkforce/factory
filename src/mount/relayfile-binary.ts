import { readFileSync } from 'node:fs'

// Relayfile's poll mirror reconciles every 30 seconds by default.  Three
// intervals allow one missed poll and ordinary filesystem jitter, while still
// making a stalled projection visible within 90 seconds rather than hours.
export const RELAYFILE_SYNC_INTERVAL_MS = 30 * 1000
export const STALE_RECONCILE_INTERVALS = 3
export const STALE_RECONCILE_MS = RELAYFILE_SYNC_INTERVAL_MS * STALE_RECONCILE_INTERVALS

type MountState = {
  workspaceId?: unknown
  lastReconcileAt?: unknown
  // The mount process pid. Older mounts wrote a top-level `pid`; SDK-launched
  // mounts record it under `daemon.pid` instead. Either may be absent.
  pid?: unknown
  daemon?: { pid?: unknown }
}

export function coercePid(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 ? value : undefined
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
  const pid = coercePid(parsed.pid) ?? coercePid(parsed.daemon?.pid)

  const lastReconcileAt = typeof parsed.lastReconcileAt === 'string'
    ? Date.parse(parsed.lastReconcileAt)
    : NaN
  if (!Number.isFinite(lastReconcileAt)) {
    return { stale: true, reason: 'last reconcile timestamp is missing', pid }
  }

  const ageMs = Date.now() - lastReconcileAt
  if (ageMs > STALE_RECONCILE_MS) {
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

  try {
    process.kill(pid, 0)
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'EPERM') {
      return { stale: false, pid }
    }
    return { stale: true, reason: `mount process (pid ${pid}) is not running`, pid }
  }

  return { stale: false, pid }
}
