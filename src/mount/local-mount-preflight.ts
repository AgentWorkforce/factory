import { readFile } from 'node:fs/promises'
import { join } from 'node:path'

import type { LocalMountOptions } from '../ports'
import { checkMountStaleness, coercePid } from './relayfile-binary'

const STATE_FILE = '.integrations/.relay/state.json'

// How long to wait for a freshly-spawned mount to write a valid state.json
// (workspace match + a live pid). A CLI mount over a large `.integrations` tree
// can take well over 10s to complete its FIRST reconcile (early cycles hit
// `context deadline exceeded` while it materializes the tree), so a 10s wait
// reported a spurious "did not become ready" on a mount that was simply still
// bootstrapping. 60s covers that without blocking startup indefinitely. Callers
// can still override via `stateWaitTimeoutMs`.
const DEFAULT_STATE_READY_TIMEOUT_MS = 60_000

export interface EnsureLocalMountOptions extends LocalMountOptions {
  /**
   * Starts an authenticated mount through the Relayfile SDK. Credential minting,
   * binary resolution, and launch details deliberately stay outside this
   * filesystem-only preflight.
   */
  startMount: () => Promise<void>
}

export async function ensureLocalMount(
  workspaceId: string,
  startDir: string,
  options: EnsureLocalMountOptions,
): Promise<void> {
  const stateFilePath = join(startDir, STATE_FILE)

  if (!(await isMountStatePresent(stateFilePath))) {
    await options.startMount()
    await waitForStateFile(
      stateFilePath,
      workspaceId,
      options.stateWaitTimeoutMs,
      options.stateWaitPollMs,
      options.acceptableWorkspaceIds,
    )
    return
  }

  const staleness = checkMountStaleness(stateFilePath, workspaceId, options.acceptableWorkspaceIds)
  if (!staleness.stale) return

  const suffix = staleness.reason !== undefined ? ` (${staleness.reason})` : ''
  const manualHint = 'Restart Factory after restoring the Agent Relay Cloud session'

  if (options.refreshStaleMount === false) {
    process.stderr.write(`[factory] local mount is stale${suffix}; writeback may not propagate. ${manualHint}\n`)
    return
  }

  // Self-heal through the same SDK-authenticated launch path used for first
  // start, rather than silently shipping writebacks into a stale mirror.
  process.stderr.write(`[factory] local mount is stale${suffix}; refreshing\n`)
  try {
    await options.startMount()
    await waitForStateFile(
      stateFilePath,
      workspaceId,
      options.stateWaitTimeoutMs,
      options.stateWaitPollMs,
      options.acceptableWorkspaceIds,
    )
    process.stderr.write('[factory] local mount refreshed\n')
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error)
    process.stderr.write(`[factory] local mount is stale${suffix} and auto-refresh failed (${reason}); writeback may not propagate. ${manualHint}\n`)
  }
}

async function isMountStatePresent(stateFilePath: string): Promise<boolean> {
  try {
    const raw = await readFile(stateFilePath, 'utf8')
    JSON.parse(raw)
    return true
  } catch {
    return false
  }
}

async function waitForStateFile(
  stateFilePath: string,
  workspaceId: string,
  timeoutMs = DEFAULT_STATE_READY_TIMEOUT_MS,
  pollMs = 200,
  acceptableWorkspaceIds: readonly string[] = [],
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastInvalidReason = 'state file was not created'
  while (Date.now() < deadline) {
    try {
      const raw = await readFile(stateFilePath, 'utf8')
      const state = JSON.parse(raw) as unknown
      if (isValidMountState(state, workspaceId, acceptableWorkspaceIds)) {
        const staleness = checkMountStaleness(stateFilePath, workspaceId, acceptableWorkspaceIds)
        if (!staleness.stale) return
        lastInvalidReason = staleness.reason ?? 'mount state is stale'
      } else {
        lastInvalidReason = `state file is malformed or for another workspace: ${stateFilePath}`
      }
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT') {
        lastInvalidReason = 'state file was not created'
      } else {
        lastInvalidReason = error instanceof Error ? error.message : String(error)
      }
      // not yet present
    }
    await sleep(pollMs)
  }
  throw new Error(`[factory] relayfile mount did not become ready within ${timeoutMs}ms (${lastInvalidReason})`)
}

function isValidMountState(
  value: unknown,
  workspaceId: string,
  acceptableWorkspaceIds: readonly string[] = [],
): boolean {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false
  const state = value as { workspaceId?: unknown; lastReconcileAt?: unknown; pid?: unknown; daemon?: { pid?: unknown } }
  const accepted = new Set([workspaceId, ...acceptableWorkspaceIds])
  // An SDK-launched mount records its pid under `daemon.pid`, not necessarily
  // the top-level `pid` — accept either, matching
  // checkMountStaleness (else a freshly-started CLI mount is never confirmed ready).
  const pid = coercePid(state.pid) ?? coercePid(state.daemon?.pid)
  return typeof state.workspaceId === 'string' && accepted.has(state.workspaceId) &&
    typeof state.lastReconcileAt === 'string' &&
    Number.isFinite(Date.parse(state.lastReconcileAt)) &&
    pid !== undefined
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
