import { accessSync, constants, readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

const STALE_RECONCILE_MS = 15 * 60 * 1000
const NOT_FOUND_ERROR = '[factory] relayfile-mount binary not found. Install dependencies or run: npm run relayfile-mount:install'

type MountState = {
  workspaceId?: unknown
  lastReconcileAt?: unknown
  pid?: unknown
}

function canExecute(filePath: string | undefined): filePath is string {
  if (!filePath) return false
  try {
    accessSync(filePath, constants.X_OK)
    return true
  } catch {
    return false
  }
}

function devArchSuffix(): string {
  if (process.platform === 'darwin') {
    return process.arch === 'arm64' ? 'darwin-arm64' : 'darwin-amd64'
  }
  if (process.platform === 'linux') {
    return process.arch === 'arm64' ? 'linux-arm64' : 'linux-amd64'
  }
  return `${process.platform}-${process.arch}`
}

function binaryName(): string {
  return process.platform === 'win32' ? 'relayfile-mount.exe' : 'relayfile-mount'
}

function optionalPackageArchSuffix(): string {
  return `${process.platform}-${process.arch}`
}

function findPearRoot(startDir: string): string | null {
  let current = resolve(startDir)
  for (;;) {
    try {
      accessSync(join(current, 'factory.config.json'), constants.R_OK)
      return current
    } catch {
      const parent = dirname(current)
      if (parent === current) return null
      current = parent
    }
  }
}

function devRelayfileCandidates(pearRoot: string): string[] {
  const distRoot = resolve(process.env.RELAYFILE_DIST_DIR ?? join(pearRoot, '..', 'relayfile', 'dist'))
  const suffix = devArchSuffix()
  return [
    join(distRoot, `relayfile-mount-${suffix}`),
    join(distRoot, suffix, binaryName()),
  ]
}

// @relayfile/sdk carries relayfile-mount through per-platform optional
// dependencies. Prefer that bundled binary so factory start does not depend on
// a global relayfile install or a manually populated repo bin/ directory.
function optionalPackageCandidates(pearRoot: string): string[] {
  const suffix = optionalPackageArchSuffix()
  return [
    join(pearRoot, 'node_modules', '@relayfile', `mount-${suffix}`, 'bin', binaryName()),
    join(pearRoot, 'node_modules', '@relayfile', 'sdk', 'node_modules', '@relayfile', `mount-${suffix}`, 'bin', binaryName()),
  ]
}

export function resolveRelayfileMountBinary(startDir = __dirname): string {
  if (process.env.RELAYFILE_MOUNT_BIN) {
    const explicitBinary = resolve(process.env.RELAYFILE_MOUNT_BIN)
    if (canExecute(explicitBinary)) return explicitBinary
    throw new Error(NOT_FOUND_ERROR)
  }

  const pearRoot = findPearRoot(startDir)
  const candidates = pearRoot
    ? [
        ...optionalPackageCandidates(pearRoot),
        join(pearRoot, 'bin', binaryName()),
        ...devRelayfileCandidates(pearRoot),
      ]
    : []

  for (const candidate of candidates) {
    if (canExecute(candidate)) return resolve(candidate)
  }

  throw new Error(NOT_FOUND_ERROR)
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

  const pid = typeof parsed.pid === 'number' && Number.isInteger(parsed.pid) && parsed.pid > 0
    ? parsed.pid
    : undefined

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
    return { stale: true, reason: 'mount process pid is missing' }
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
