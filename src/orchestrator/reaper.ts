import { readFile } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import { parseJsonContent } from '../writeback/shared'
import type { Clock, FleetClient, Logger } from '../ports'
import type { FactoryInFlightRegistry, FactoryInFlightRegistryAgent, FactoryInFlightRegistryProcess } from '../types'
import { checkFactoryLoopLiveness, readFactoryLoopHeartbeat } from './factory'
import { findAgentProcessByName, readProcessIdentity, type AgentProcessFinder, type ProcessIdentity } from './process-identity'

const execFileAsync = promisify(execFile)

export interface FactoryReaperOptions {
  heartbeatPath: string
  registryPath: string
  staleMs: number
  termGraceMs?: number
  nowMs?: number
  clock?: Clock
  logger?: Logger
  kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readChildPids?: (pid: number) => Promise<number[]>
  readParentPid?: (pid: number) => Promise<number | undefined>
  readProcessIdentity?: (pid: number) => Promise<ProcessIdentity | undefined>
  processFinder?: AgentProcessFinder
  fleet?: Pick<FleetClient, 'protectedPids' | 'resolveAgentPid'>
}


export interface FactoryReaperReport {
  stale: boolean
  reason?: string
  reaped: Array<{ pid: number; signals: Array<NodeJS.Signals> }>
  skipped: Array<{ pid?: number; reason: string }>
}

export interface TerminatePidsOptions {
  termGraceMs?: number
  sleep?: (ms: number) => Promise<void>
  kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readChildPids?: (pid: number) => Promise<number[]>
  readParentPid?: (pid: number) => Promise<number | undefined>
  protectedPids?: number[]
}

export interface TerminatePidsReport {
  terminated: Array<{ pid: number; signals: Array<NodeJS.Signals> }>
  skipped: Array<{ pid: number; reason: string }>
}

export async function readFactoryInFlightRegistry(path: string): Promise<FactoryInFlightRegistry | undefined> {
  try {
    return parseJsonContent(await readFile(path, 'utf8')) as FactoryInFlightRegistry
  } catch {
    return undefined
  }
}

export async function terminatePids(pids: number[], opts: TerminatePidsOptions = {}): Promise<TerminatePidsReport> {
  const kill = opts.kill ?? process.kill
  const sleep = opts.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)))
  const termGraceMs = opts.termGraceMs ?? 1_000
  const terminated: TerminatePidsReport['terminated'] = []
  const skipped: TerminatePidsReport['skipped'] = []
  const protectedPids = await protectedPidSet(opts.protectedPids ?? [], opts.readParentPid ?? readParentPid)
  const orderedPids = await pidTreePostOrder(pids, opts.readChildPids ?? readChildPids, protectedPids)

  for (const pid of orderedPids) {
    if (protectedPids.has(pid)) {
      skipped.push({ pid, reason: 'protected pid' })
      continue
    }
    if (!isPidLive(pid, kill)) {
      skipped.push({ pid, reason: 'pid not running' })
      continue
    }

    const signals: Array<NodeJS.Signals> = []
    try {
      kill(pid, 'SIGTERM')
      signals.push('SIGTERM')
      if (termGraceMs > 0) {
        await sleep(termGraceMs)
      }
      if (isPidLive(pid, kill)) {
        kill(pid, 'SIGKILL')
        signals.push('SIGKILL')
      }
      terminated.push({ pid, signals })
    } catch (error) {
      skipped.push({ pid, reason: error instanceof Error ? error.message : String(error) })
    }
  }

  return { terminated, skipped }
}

async function protectedPidSet(
  explicitPids: number[],
  readParent: (pid: number) => Promise<number | undefined>,
): Promise<Set<number>> {
  const protectedPids = new Set<number>()
  for (const pid of explicitPids) {
    if (Number.isInteger(pid) && pid > 0) protectedPids.add(pid)
  }
  let current: number | undefined = process.pid
  while (current && current > 0 && !protectedPids.has(current)) {
    protectedPids.add(current)
    try {
      current = await readParent(current)
    } catch {
      break
    }
  }
  return protectedPids
}

async function pidTreePostOrder(
  roots: number[],
  readChildren: (pid: number) => Promise<number[]>,
  protectedPids = new Set<number>(),
): Promise<number[]> {
  const ordered: number[] = []
  const seen = new Set<number>()
  const visit = async (pid: number): Promise<void> => {
    if (!Number.isInteger(pid) || pid <= 0 || seen.has(pid)) return
    seen.add(pid)
    if (protectedPids.has(pid)) {
      ordered.push(pid)
      return
    }
    let children: number[] = []
    try {
      children = await readChildren(pid)
    } catch {
      children = []
    }
    for (const child of children.sort((a, b) => a - b)) {
      await visit(child)
    }
    ordered.push(pid)
  }

  for (const pid of [...new Set(roots)].sort((a, b) => a - b)) {
    await visit(pid)
  }
  return ordered
}

async function readChildPids(pid: number): Promise<number[]> {
  try {
    const { stdout } = await execFileAsync('pgrep', ['-P', String(pid)])
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

export async function reapFactoryOrphansOnce(opts: FactoryReaperOptions): Promise<FactoryReaperReport> {
  const heartbeat = await readFactoryLoopHeartbeat(opts.heartbeatPath)
  const liveness = checkFactoryLoopLiveness(heartbeat, { nowMs: opts.nowMs ?? opts.clock?.now(), staleMs: opts.staleMs })
  if (!liveness.stale) {
    return { stale: false, reason: liveness.reason, reaped: [], skipped: [] }
  }

  const registry = await readFactoryInFlightRegistry(opts.registryPath)
  if (!registry) {
    return { stale: true, reason: 'registry missing', reaped: [], skipped: [{ reason: 'registry missing' }] }
  }

  const termGraceMs = opts.termGraceMs ?? 1_000
  const reaped: FactoryReaperReport['reaped'] = []
  const skipped: FactoryReaperReport['skipped'] = []
  const kill = opts.kill ?? process.kill
  const protectedPids = await reaperProtectedPids(opts)
  const processes = await registryProcesses(registry, opts, skipped, protectedPids)

  for (const processInfo of processes) {
    const { pid } = processInfo
    if (!isPidLive(pid, kill)) {
      skipped.push({ pid, reason: 'pid not running' })
      continue
    }
    const current = await (opts.readProcessIdentity ?? readProcessIdentity)(pid)
    if (!current || !processIdentityMatches(processInfo, current)) {
      skipped.push({ pid, reason: 'pid identity mismatch' })
      continue
    }

    const report = await terminatePids([pid], {
      kill,
      sleep: opts.clock?.sleep,
      termGraceMs,
      protectedPids,
      readChildPids: opts.readChildPids,
      readParentPid: opts.readParentPid,
    })
    for (const entry of report.terminated) {
      reaped.push(entry)
      opts.logger?.warn?.('[factory-reaper] reaped orphaned factory pid', { pid: entry.pid, signals: entry.signals })
    }
    for (const entry of report.skipped) {
      skipped.push(entry)
    }
  }

  return { stale: true, reason: liveness.reason, reaped, skipped }
}

export class FactoryReaper {
  readonly #opts: FactoryReaperOptions & { intervalMs: number }
  #timer: ReturnType<typeof setTimeout> | undefined
  #running = false
  #stopped = false

  constructor(opts: FactoryReaperOptions & { intervalMs?: number }) {
    this.#opts = { ...opts, intervalMs: opts.intervalMs ?? 5_000 }
  }

  start(): void {
    if (this.#timer || this.#stopped) return
    this.#schedule(0)
  }

  async stop(): Promise<void> {
    this.#stopped = true
    if (this.#timer) {
      clearTimeout(this.#timer)
      this.#timer = undefined
    }
  }

  #schedule(delayMs: number): void {
    this.#timer = setTimeout(() => {
      this.#timer = undefined
      void this.#tick()
    }, delayMs)
  }

  async #tick(): Promise<void> {
    if (this.#stopped || this.#running) return
    this.#running = true
    try {
      await reapFactoryOrphansOnce(this.#opts)
    } finally {
      this.#running = false
      if (!this.#stopped) this.#schedule(this.#opts.intervalMs)
    }
  }
}

async function reaperProtectedPids(opts: FactoryReaperOptions): Promise<number[]> {
  try {
    return await opts.fleet?.protectedPids?.() ?? []
  } catch (error) {
    opts.logger?.warn?.('[factory-reaper] failed to resolve protected fleet pids', error)
    return []
  }
}

async function registryProcesses(
  registry: FactoryInFlightRegistry,
  opts: FactoryReaperOptions,
  skipped: FactoryReaperReport['skipped'],
  protectedPids: number[],
): Promise<FactoryInFlightRegistryProcess[]> {
  const processes = new Map<number, FactoryInFlightRegistryProcess>()
  for (const agent of registry.agents ?? []) {
    for (const processInfo of agent.processes ?? []) {
      if (Number.isInteger(processInfo.pid) && processInfo.pid > 0) {
        processes.set(processInfo.pid, processInfo)
      }
    }
    if (!agentHasPersistedPid(agent)) {
      const resolved = await resolveRegistryAgentProcess(agent, opts, skipped, protectedPids)
      if (resolved) {
        processes.set(resolved.pid, resolved)
      }
    }
  }
  return [...processes.values()].sort((a, b) => a.pid - b.pid)
}

const agentHasPersistedPid = (agent: FactoryInFlightRegistryAgent): boolean =>
  [...(agent.processes ?? []), ...(agent.pids ?? []).map((pid) => ({ pid }))]
    .some((entry) => Number.isInteger(entry.pid) && entry.pid > 0)

async function resolveRegistryAgentProcess(
  agent: FactoryInFlightRegistryAgent,
  opts: FactoryReaperOptions,
  skipped: FactoryReaperReport['skipped'],
  protectedPids: number[],
): Promise<FactoryInFlightRegistryProcess | undefined> {
  const scanned = await scanRegistryAgentProcess(agent, opts, protectedPids)
  if (scanned.status === 'found') {
    return scanned.process
  }
  if (scanned.status === 'ambiguous') {
    skipped.push({ reason: `pid ambiguous for ${agent.name}` })
    return undefined
  }
  if (scanned.status === 'identity-mismatch') {
    skipped.push({ pid: scanned.pid, reason: 'pid identity mismatch' })
    return undefined
  }

  const resolution = await opts.fleet?.resolveAgentPid?.(agent.name)
  if (!resolution) {
    skipped.push({ reason: `pid missing for ${agent.name}` })
    return undefined
  }
  if (resolution.status !== 'found') {
    skipped.push({ reason: resolution.status === 'unresolved' ? `pid missing for ${agent.name}` : `pid ${resolution.status} for ${agent.name}` })
    return undefined
  }
  const identity = await (opts.readProcessIdentity ?? readProcessIdentity)(resolution.pid)
  if (!identity || !identity.cmdline.includes(agent.name)) {
    skipped.push({ pid: resolution.pid, reason: 'pid identity mismatch' })
    return undefined
  }
  return { ...identity, agentName: agent.name }
}

type RegistryAgentScan =
  | { status: 'found'; process: FactoryInFlightRegistryProcess }
  | { status: 'missing' }
  | { status: 'ambiguous' }
  | { status: 'identity-mismatch'; pid: number }

async function scanRegistryAgentProcess(
  agent: FactoryInFlightRegistryAgent,
  opts: FactoryReaperOptions,
  protectedPids: number[],
): Promise<RegistryAgentScan> {
  const lookup = await (opts.processFinder ?? ((agentName) => findAgentProcessByName(agentName, {
    readProcessIdentity: opts.readProcessIdentity,
    readParentPid: opts.readParentPid,
    protectedPids,
  })))(agent.name, { protectedPids })
  if (lookup.status === 'found') {
    if (!lookup.identity.cmdline.includes(agent.name)) {
      return { status: 'identity-mismatch', pid: lookup.identity.pid }
    }
    return { status: 'found', process: { ...lookup.identity, agentName: agent.name } }
  }
  return { status: lookup.status }
}

const isPidLive = (pid: number, kill: (pid: number, signal?: NodeJS.Signals | 0) => boolean): boolean => {
  try {
    kill(pid, 0)
    return true
  } catch {
    return false
  }
}

const processIdentityMatches = (
  expected: FactoryInFlightRegistryProcess,
  current: ProcessIdentity,
): boolean =>
  current.pid === expected.pid &&
  current.startTime === expected.startTime &&
  current.cmdline === expected.cmdline &&
  current.cmdline.includes(expected.agentName)
