import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { spawn } from 'node:child_process'

import { checkMountStaleness, resolveRelayfileMountBinary } from './relayfile-binary'

const STATE_FILE = '.integrations/.relay/state.json'

interface EnsureLocalMountOptions {
  stateWaitTimeoutMs?: number
  stateWaitPollMs?: number
  // Alternate identifiers for the same workspace (e.g. the cloud UUID for a
  // `rw_` handle). Passed through to the staleness check so a handle-vs-UUID
  // state.json does not register as a spurious mismatch.
  acceptableWorkspaceIds?: readonly string[]
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
  if (staleness.stale) {
    const suffix = staleness.reason !== undefined ? ` (${staleness.reason})` : ''
    process.stderr.write(
      `[factory] local mount is stale${suffix}; writeback may not propagate. Run: relayfile stop && relayfile start ${workspaceId} .integrations --background\n`,
    )
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
  const binaryPath = resolveRelayfileMountBinary()

  return new Promise<void>((resolve, reject) => {
    const child = spawn(binaryPath, ['start', workspaceId, '.integrations', '--background', '--rehome'], {
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
        reject(new Error(`[factory] relayfile mount start failed (exit ${code ?? 'null'}): ${stderr.trim()}`))
        return
      }
      resolve()
    })

    child.on('error', (err: Error) => {
      reject(new Error(`[factory] relayfile mount start error: ${err.message}`))
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
  const state = value as { workspaceId?: unknown; lastReconcileAt?: unknown; pid?: unknown }
  const accepted = new Set([workspaceId, ...acceptableWorkspaceIds])
  return typeof state.workspaceId === 'string' && accepted.has(state.workspaceId) &&
    typeof state.lastReconcileAt === 'string' &&
    Number.isFinite(Date.parse(state.lastReconcileAt)) &&
    typeof state.pid === 'number' &&
    Number.isInteger(state.pid) &&
    state.pid > 0
}

const isAuthError = (stderr: string): boolean =>
  stderr.includes('workspace join') ||
  stderr.includes('unauthorized') ||
  stderr.includes('no credentials')

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
