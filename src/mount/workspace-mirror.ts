import { readdirSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'

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

  const registryMirror = readWorkspaceRegistry(join(homeDir, '.relayfile', 'workspaces.json'), accepted)
  if (registryMirror) return { localDir: registryMirror, source: 'workspace-registry' }

  return readMountStateDirectory(join(homeDir, '.relayfile-mount-state'), accepted)
}

function readWorkspaceRegistry(path: string, accepted: ReadonlySet<string>): string | undefined {
  let payload: unknown
  try {
    payload = JSON.parse(readFileSync(path, 'utf8')) as unknown
  } catch {
    return undefined
  }

  const mirrors = new Set<string>()
  for (const record of workspaceRecords(payload)) {
    const workspaceId = stringField(record, 'id') ?? stringField(record, 'workspaceId') ?? stringField(record, 'workspace')
    if (!workspaceId || !accepted.has(workspaceId)) continue
    const localDir = stringField(record, 'localDir') ?? stringField(record, 'localRoot') ?? stringField(record, 'mirrorDir')
    if (localDir) mirrors.add(resolve(localDir))
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
      if (localDir) mirrors.add(resolve(localDir))
    } catch {
      // A partially-written or retired mount state is not a registration.
    }
  }

  // Relayfile admits one mirror per workspace. Treat contradictory local state
  // as unavailable instead of guessing which directory to refresh.
  if (mirrors.size !== 1) return undefined
  return { localDir: [...mirrors][0]!, source: 'mount-state' }
}

function stringField(record: RecordValue, key: string): string | undefined {
  const value = record[key]
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined
}

function isRecord(value: unknown): value is RecordValue {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
