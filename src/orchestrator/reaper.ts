import { readFile } from 'node:fs/promises'

import { parseJsonContent } from '../writeback/shared'
import type { Clock, Logger } from '../ports'
import type { FactoryInFlightRegistry, FactoryInFlightRegistryProcess } from '../types'
import { checkFactoryLoopLiveness, readFactoryLoopHeartbeat } from './factory'
import { readProcessIdentity, type ProcessIdentity } from './process-identity'

export interface FactoryReaperOptions {
  heartbeatPath: string
  registryPath: string
  staleMs: number
  termGraceMs?: number
  nowMs?: number
  clock?: Clock
  logger?: Logger
  kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean
  readProcessIdentity?: (pid: number) => Promise<ProcessIdentity | undefined>
}


export interface FactoryReaperReport {
  stale: boolean
  reason?: string
  reaped: Array<{ pid: number; signals: Array<NodeJS.Signals> }>
  skipped: Array<{ pid?: number; reason: string }>
}

export async function readFactoryInFlightRegistry(path: string): Promise<FactoryInFlightRegistry | undefined> {
  try {
    return parseJsonContent(await readFile(path, 'utf8')) as FactoryInFlightRegistry
  } catch {
    return undefined
  }
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

  const kill = opts.kill ?? process.kill
  const sleep = opts.clock?.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)))
  const termGraceMs = opts.termGraceMs ?? 1_000
  const processes = registryProcesses(registry)
  const reaped: FactoryReaperReport['reaped'] = []
  const skipped: FactoryReaperReport['skipped'] = []

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
      reaped.push({ pid, signals })
      opts.logger?.warn?.('[factory-reaper] reaped orphaned factory pid', { pid, signals })
    } catch (error) {
      skipped.push({ pid, reason: error instanceof Error ? error.message : String(error) })
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

const registryProcesses = (registry: FactoryInFlightRegistry): FactoryInFlightRegistryProcess[] => {
  const processes = new Map<number, FactoryInFlightRegistryProcess>()
  for (const agent of registry.agents ?? []) {
    for (const processInfo of agent.processes ?? []) {
      if (Number.isInteger(processInfo.pid) && processInfo.pid > 0) {
        processes.set(processInfo.pid, processInfo)
      }
    }
  }
  return [...processes.values()].sort((a, b) => a.pid - b.pid)
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
