import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { spawn } from 'node:child_process'

import { checkMountStaleness, coercePid, resolveRelayfileCli, resolveRelayfileMountBinary } from './relayfile-binary'

const STATE_FILE = '.integrations/.relay/state.json'

interface EnsureLocalMountOptions {
  stateWaitTimeoutMs?: number
  stateWaitPollMs?: number
  // Alternate identifiers for the same workspace (e.g. the cloud UUID for a
  // `rw_` handle). Passed through to the staleness check so a handle-vs-UUID
  // state.json does not register as a spurious mismatch.
  acceptableWorkspaceIds?: readonly string[]
  // When true (default), a stale mount is auto-refreshed (re-spawned) instead of
  // merely warned about. A standalone `relayfile start` mount has no supervisor,
  // so it can silently stop reconciling — and the factory would then ship
  // writebacks into a mirror that never propagates them. Set false to restore
  // warn-only behavior.
  refreshStaleMount?: boolean
}

export async function ensureLocalMount(
  workspaceId: string,
  startDir: string,
  options: EnsureLocalMountOptions = {},
): Promise<void> {
  const stateFilePath = join(startDir, STATE_FILE)

  if (!(await isMountStatePresent(stateFilePath))) {
    await spawnMount(workspaceId, startDir)
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
  const manualHint = `Run: relayfile stop && relayfile start ${workspaceId} .integrations --background`

  if (options.refreshStaleMount === false) {
    process.stderr.write(`[factory] local mount is stale${suffix}; writeback may not propagate. ${manualHint}\n`)
    return
  }

  // Self-heal: re-spawn the mount so writebacks propagate, rather than silently
  // shipping them into a stale mirror. spawnMount runs the relayfile-mount
  // binary with --rehome, which re-establishes the mount in place.
  process.stderr.write(`[factory] local mount is stale${suffix}; refreshing\n`)
  try {
    await spawnMount(workspaceId, startDir)
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

async function spawnMount(workspaceId: string, startDir: string): Promise<void> {
  // Prefer the relayfile CLI: it resolves workspace credentials itself (the raw
  // relayfile-mount binary requires a --token the factory has no clean way to
  // supply) and bundles an up-to-date mount, so the factory can self-start the
  // writeback mount unattended. Fall back to the raw binary only when no CLI is
  // available.
  const cli = resolveRelayfileCli()
  if (cli) {
    await spawnMountViaCli(cli, workspaceId, startDir)
    return
  }
  await spawnMountViaRawBinary(workspaceId, startDir)
}

async function spawnMountViaCli(cli: string, workspaceId: string, startDir: string): Promise<void> {
  // Best-effort stop first so a stale/dead mount registration does not make
  // `start` reject (matches the documented `relayfile stop && relayfile start`
  // recovery). A no-op when nothing is mounted here.
  await runRelayfile(cli, ['stop'], startDir, workspaceId).catch(() => {})
  await runRelayfile(cli, ['start', workspaceId, '.integrations', '--background'], startDir, workspaceId)
}

async function spawnMountViaRawBinary(workspaceId: string, startDir: string): Promise<void> {
  // Search from the deployment dir (where factory.config.json + the bundled
  // @relayfile/mount live), not this module's install location.
  const binaryPath = resolveRelayfileMountBinary(startDir)
  await runRelayfile(binaryPath, ['start', workspaceId, '.integrations', '--background', '--rehome'], startDir, workspaceId)
}

function runRelayfile(command: string, args: string[], startDir: string, workspaceId: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: startDir,
      stdio: ['ignore', 'ignore', 'pipe'],
    })

    const stderrChunks: Buffer[] = []
    if (child.stderr) {
      child.stderr.on('data', (chunk: Buffer) => stderrChunks.push(chunk))
    }

    child.on('close', (code) => {
      const stderr = Buffer.concat(stderrChunks).toString('utf8')
      if (isAuthError(stderr)) {
        reject(new Error(`[factory] relayfile mount not authorized — run: relayfile workspace join ${workspaceId} --write`))
        return
      }
      if (code !== 0) {
        reject(new Error(`[factory] relayfile mount ${args[0]} failed (exit ${code ?? 'null'}): ${stderr.trim()}`))
        return
      }
      resolve()
    })

    child.on('error', (err: Error) => {
      reject(new Error(`[factory] relayfile mount ${args[0]} error: ${err.message}`))
    })
  })
}

async function waitForStateFile(
  stateFilePath: string,
  workspaceId: string,
  timeoutMs = 10_000,
  pollMs = 200,
  acceptableWorkspaceIds: readonly string[] = [],
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastInvalidReason = 'state file was not created'
  while (Date.now() < deadline) {
    try {
      const raw = await readFile(stateFilePath, 'utf8')
      const state = JSON.parse(raw) as unknown
      if (isValidMountState(state, workspaceId, acceptableWorkspaceIds)) return
      lastInvalidReason = `state file is malformed or for another workspace: ${stateFilePath}`
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
  // A CLI-daemonized mount (`relayfile start --background`) records its pid under
  // `daemon.pid`, not the top-level `pid` — accept either, matching
  // checkMountStaleness (else a freshly-started CLI mount is never confirmed ready).
  const pid = coercePid(state.pid) ?? coercePid(state.daemon?.pid)
  return typeof state.workspaceId === 'string' && accepted.has(state.workspaceId) &&
    typeof state.lastReconcileAt === 'string' &&
    Number.isFinite(Date.parse(state.lastReconcileAt)) &&
    pid !== undefined
}

const isAuthError = (stderr: string): boolean =>
  stderr.includes('workspace join') ||
  stderr.includes('unauthorized') ||
  stderr.includes('no credentials')

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
