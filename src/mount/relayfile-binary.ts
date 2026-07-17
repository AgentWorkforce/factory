import { accessSync, constants, readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

// ESM has no __dirname; derive this module's directory so the resolver can find
// dependencies installed with the factory package.
const MODULE_DIR = dirname(fileURLToPath(import.meta.url))

const STALE_RECONCILE_MS = 15 * 60 * 1000

type MountState = {
  workspaceId?: unknown
  lastReconcileAt?: unknown
  // The mount process pid. Older mounts wrote a top-level `pid`; the
  // CLI-daemonized mount (`relayfile start --background`) records it under
  // `daemon.pid` instead. Either may be absent.
  pid?: unknown
  daemon?: { pid?: unknown }
}

export function coercePid(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 ? value : undefined
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
function optionalPackageCandidates(packageRoot: string): string[] {
  const suffix = optionalPackageArchSuffix()
  return [
    join(packageRoot, 'node_modules', '@relayfile', `mount-${suffix}`, 'bin', binaryName()),
    join(packageRoot, 'node_modules', '@relayfile', 'sdk', 'node_modules', '@relayfile', `mount-${suffix}`, 'bin', binaryName()),
  ]
}

// npm can place optional dependencies inside factory's own node_modules (a
// global install), beside factory in a shared node_modules (npx/hoisting), or
// below @relayfile/sdk (strict/nested installs). Walk upward from this module so
// all of those layouts are found without depending on the target workspace.
function installedOptionalPackageCandidates(installDir: string): string[] {
  const candidates: string[] = []
  let current = resolve(installDir)
  for (;;) {
    candidates.push(...optionalPackageCandidates(current))
    const parent = dirname(current)
    if (parent === current) return candidates
    current = parent
  }
}

function notFoundError(): string {
  return `[factory] relayfile-mount binary not found (missing @relayfile/mount-${optionalPackageArchSuffix()}). Reinstall @agent-relay/factory without --omit=optional`
}

export function resolveRelayfileMountBinary(startDir = process.cwd(), installDir = MODULE_DIR): string {
  if (process.env.RELAYFILE_MOUNT_BIN) {
    const explicitBinary = resolve(process.env.RELAYFILE_MOUNT_BIN)
    if (canExecute(explicitBinary)) return explicitBinary
    throw new Error(notFoundError())
  }

  const pearRoot = findPearRoot(startDir)
  const candidates = [
    ...installedOptionalPackageCandidates(installDir),
    ...(pearRoot
      ? [
        ...optionalPackageCandidates(pearRoot),
        join(pearRoot, 'bin', binaryName()),
        ...devRelayfileCandidates(pearRoot),
      ]
      : []),
  ]

  for (const candidate of candidates) {
    if (canExecute(candidate)) return resolve(candidate)
  }

  throw new Error(notFoundError())
}

function cliName(): string {
  return process.platform === 'win32' ? 'relayfile.exe' : 'relayfile'
}

// The `relayfile` CLI (as opposed to the raw relayfile-mount binary) resolves
// workspace credentials itself and bundles an up-to-date mount, so the factory
// can start/refresh the writeback mount without plumbing a token — and without
// depending on a possibly-stale @relayfile/mount-* node_modules binary. Prefer
// it; fall back to the raw binary only when no CLI is on PATH.
export function resolveRelayfileCli(): string | undefined {
  const explicit = process.env.RELAYFILE_BIN
  if (explicit) return canExecute(explicit) ? resolve(explicit) : undefined

  const exe = cliName()
  const separator = process.platform === 'win32' ? ';' : ':'
  for (const dir of (process.env.PATH ?? '').split(separator)) {
    if (!dir) continue
    const candidate = join(dir, exe)
    if (canExecute(candidate)) return resolve(candidate)
  }
  return undefined
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

  // Prefer the top-level pid; fall back to the CLI-daemonized mount's daemon.pid.
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
