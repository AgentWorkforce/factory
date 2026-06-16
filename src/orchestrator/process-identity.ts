import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { FactoryInFlightRegistryProcess } from '../types'

const execFileAsync = promisify(execFile)

export type ProcessIdentity = Pick<FactoryInFlightRegistryProcess, 'pid' | 'cmdline' | 'startTime'>

export type AgentProcessLookupResult =
  | { status: 'found'; identity: ProcessIdentity }
  | { status: 'missing' }
  | { status: 'ambiguous' }

export interface AgentProcessFinderOptions {
  protectedPids?: number[]
}

export type AgentProcessFinder = (agentName: string, opts?: AgentProcessFinderOptions) => Promise<AgentProcessLookupResult>

export interface AgentProcessLookupOptions {
  listPidsByCommand?: (pattern: string) => Promise<number[]>
  readProcessIdentity?: (pid: number) => Promise<ProcessIdentity | undefined>
  readParentPid?: (pid: number) => Promise<number | undefined>
  protectedPids?: number[]
}

export async function readProcessIdentity(pid: number): Promise<ProcessIdentity | undefined> {
  try {
    const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'lstart=', '-o', 'command='])
    const line = stdout.trim()
    if (!line) return undefined
    const match = line.match(/^(\S+\s+\S+\s+\d+\s+\d+:\d+:\d+\s+\d+)\s+([\s\S]+)$/u)
    if (!match) return undefined
    return { pid, startTime: match[1]!, cmdline: match[2]! }
  } catch {
    return undefined
  }
}

export async function findAgentProcessByName(
  agentName: string,
  opts: AgentProcessLookupOptions = {},
): Promise<AgentProcessLookupResult> {
  const readIdentity = opts.readProcessIdentity ?? readProcessIdentity
  const readParent = opts.readParentPid ?? readParentPid
  const protectedPids = new Set(opts.protectedPids ?? [])
  const pids = await (opts.listPidsByCommand ?? listPidsByCommand)(agentNameCommandPattern(agentName))
  const matches: ProcessIdentity[] = []
  for (const pid of [...new Set(pids)].sort((a, b) => a - b)) {
    if (protectedPids.has(pid)) {
      continue
    }
    const identity = await readIdentity(pid)
    if (identity && agentCommandLineMatches(identity.cmdline, agentName)) {
      matches.push(identity)
    }
  }
  if (matches.length === 0) return { status: 'missing' }
  const root = await coherentAgentRoot(matches, readParent)
  return root ? { status: 'found', identity: root } : { status: 'ambiguous' }
}

function agentNameCommandPattern(agentName: string): string {
  return `(^|[[:space:]])--agent-name[[:space:]=]+${escapeExtendedRegex(agentName)}([[:space:]]|$)`
}

function agentCommandLineMatches(cmdline: string, agentName: string): boolean {
  const escaped = escapeRegExp(agentName)
  return new RegExp(`(^|\\s)--agent-name(\\s+|=)${escaped}(\\s|$)`, 'u').test(cmdline)
}

async function coherentAgentRoot(
  matches: ProcessIdentity[],
  readParent: (pid: number) => Promise<number | undefined>,
): Promise<ProcessIdentity | undefined> {
  if (matches.length === 1) return matches[0]
  const byPid = new Map(matches.map((identity) => [identity.pid, identity]))
  const roots: ProcessIdentity[] = []
  for (const identity of matches) {
    if (!await hasMatchedAncestor(identity.pid, byPid, readParent)) {
      roots.push(identity)
    }
  }
  if (roots.length !== 1) return undefined
  const root = roots[0]!
  for (const identity of matches) {
    if (identity.pid === root.pid) continue
    if (!await reachesRoot(identity.pid, root.pid, readParent)) {
      return undefined
    }
  }
  return root
}

async function hasMatchedAncestor(
  pid: number,
  matches: Map<number, ProcessIdentity>,
  readParent: (pid: number) => Promise<number | undefined>,
): Promise<boolean> {
  let current = pid
  const seen = new Set<number>([pid])
  for (;;) {
    const parent = await readParent(current)
    if (!parent || seen.has(parent)) return false
    if (matches.has(parent)) return true
    seen.add(parent)
    current = parent
  }
}

async function reachesRoot(
  pid: number,
  rootPid: number,
  readParent: (pid: number) => Promise<number | undefined>,
): Promise<boolean> {
  let current = pid
  const seen = new Set<number>([pid])
  for (;;) {
    const parent = await readParent(current)
    if (!parent || seen.has(parent)) return false
    if (parent === rootPid) return true
    seen.add(parent)
    current = parent
  }
}

async function listPidsByCommand(pattern: string): Promise<number[]> {
  try {
    const { stdout } = await execFileAsync('pgrep', ['-f', pattern])
    return parsePidList(stdout)
  } catch (error) {
    return parsePidList((error as { stdout?: string }).stdout)
  }
}

async function readParentPid(pid: number): Promise<number | undefined> {
  try {
    const { stdout } = await execFileAsync('ps', ['-o', 'ppid=', '-p', String(pid)])
    const parent = Number(stdout.trim())
    return Number.isInteger(parent) && parent > 0 ? parent : undefined
  } catch {
    return undefined
  }
}

const parsePidList = (stdout: string | Buffer | undefined): number[] => {
  const text = Buffer.isBuffer(stdout) ? stdout.toString('utf8') : stdout ?? ''
  return text
    .split(/\s+/u)
    .map((value) => Number(value))
    .filter((pid) => Number.isInteger(pid) && pid > 0)
}

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')

const escapeExtendedRegex = (value: string): string => value.replace(/[.[\]()*+?{}|^$\\]/gu, '\\$&')
