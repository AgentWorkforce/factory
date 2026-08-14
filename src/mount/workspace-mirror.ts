import { readdirSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { isAbsolute, join, resolve } from 'node:path'

type RecordValue = Record<string, unknown>

export interface RegisteredWorkspaceMirror {
  localDir: string
  source: 'workspace-registry' | 'mount-state'
}

/**
 * Resolve the one local mirror that Relayfile has already associated with a
 * workspace.  Relayfile versions have stored this association in both the
 * workspace registry and the private mount-state directory, so accept either
 * format.  This is deliberately read-only: a missing registration must never
 * cause Factory to re-home a user's mirror.
 */
export function resolveRegisteredWorkspaceMirror(
  workspaceIds: readonly string[],
  homeDir = homedir(),
): RegisteredWorkspaceMirror | undefined {
  const accepted = new Set(workspaceIds.filter((id) => id.trim().length > 0))
  if (accepted.size === 0) return undefined

  const registryMirror = readWorkspaceRegistry(join(homeDir, '.relayfile', 'workspaces.json'), accepted, homeDir)
  if (registryMirror) return { localDir: registryMirror, source: 'workspace-registry' }

  return readMountStateDirectory(join(homeDir, '.relayfile-mount-state'), accepted, homeDir)
}

function readWorkspaceRegistry(path: string, accepted: ReadonlySet<string>, homeDir: string): string | undefined {
  let payload: unknown
  try {
    payload = JSON.parse(readFileSync(path, 'utf8')) as unknown
  } catch {
    return undefined
  }

  const mirrors = new Set<string>()
  for (const record of workspaceRecords(payload)) {
    // Relayfile's registry records both the stable workspace id and its
    // operator-facing name. Callers may legitimately hold either identifier,
    // so match every recorded alias instead of preferring `id` and making a
    // name lookup miss. This remains a read-only lookup: no-match never adds a
    // stub record to workspaces.json.
    const workspaceAliases = ['id', 'workspaceId', 'workspace', 'name']
      .map((key) => stringField(record, key))
      .filter((value): value is string => value !== undefined)
    if (!workspaceAliases.some((alias) => accepted.has(alias))) continue
    const localDir = stringField(record, 'localDir') ?? stringField(record, 'localRoot') ?? stringField(record, 'mirrorDir')
    if (localDir) mirrors.add(resolveRegisteredLocalDir(homeDir, localDir))
  }
  // Workspace aliases can appear as separate records. Like mount state, do
  // not choose one by JSON ordering if they disagree about the mirror root.
  return mirrors.size === 1 ? [...mirrors][0] : undefined
}

function workspaceRecords(payload: unknown): RecordValue[] {
  if (Array.isArray(payload)) return payload.filter(isRecord)
  if (!isRecord(payload)) return []
  const workspaces = payload.workspaces
  if (Array.isArray(workspaces)) return workspaces.filter(isRecord)
  if (!isRecord(workspaces)) return []
  return Object.entries(workspaces)
    .filter((entry): entry is [string, RecordValue] => isRecord(entry[1]))
    .map(([id, record]) => ({ id, ...record }))
}

function readMountStateDirectory(
  stateRoot: string,
  accepted: ReadonlySet<string>,
  homeDir: string,
): RegisteredWorkspaceMirror | undefined {
  let entries: string[]
  try {
    entries = readdirSync(stateRoot)
  } catch {
    return undefined
  }

  const mirrors = new Set<string>()
  for (const entry of entries) {
    try {
      const state = JSON.parse(readFileSync(join(stateRoot, entry, 'state.json'), 'utf8')) as unknown
      if (!isRecord(state) || !accepted.has(stringField(state, 'workspaceId') ?? '')) continue
      const localDir = stringField(state, 'localDir') ?? stringField(state, 'localRoot')
      if (localDir) mirrors.add(resolveRegisteredLocalDir(homeDir, localDir))
    } catch {
      // A partially-written or retired mount state is not a registration.
    }
  }

  // Relayfile admits one mirror per workspace. Treat contradictory local state
  // as unavailable instead of guessing which directory to refresh.
  if (mirrors.size !== 1) return undefined
  return { localDir: [...mirrors][0]!, source: 'mount-state' }
}

/** Registry values are stored under this user's Relayfile home; preserve that base for relative legacy entries. */
function resolveRegisteredLocalDir(homeDir: string, localDir: string): string {
  return isAbsolute(localDir) ? resolve(localDir) : resolve(homeDir, localDir)
}

function stringField(record: RecordValue, key: string): string | undefined {
  const value = record[key]
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined
}

function isRecord(value: unknown): value is RecordValue {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
