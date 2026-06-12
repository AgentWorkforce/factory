import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it, vi } from 'vitest'

import type { FactoryInFlightRegistry, FactoryLoopHeartbeat } from '../types'
import { FactoryReaper, reapFactoryOrphansOnce } from './reaper'

const writeJson = async (path: string, value: unknown): Promise<void> => {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, 'utf8')
}

const heartbeat = (updatedAtMs: number): FactoryLoopHeartbeat => ({
  pid: 100,
  status: 'running',
  iteration: 1,
  maxIterations: 3,
  updatedAt: new Date(updatedAtMs).toISOString(),
  updatedAtMs,
})

const registry = (): FactoryInFlightRegistry => ({
  pid: 100,
  updatedAt: new Date(1_000).toISOString(),
  updatedAtMs: 1_000,
  agents: [{
    name: 'ar-1-impl',
    role: 'implementer',
    issue: { key: 'AR-1', uuid: 'uuid-1', path: '/linear/issues/AR-1__uuid-1.json' },
    pids: [111],
    processes: [{ pid: 111, agentName: 'ar-1-impl', startTime: 'started-111', cmdline: 'node ar-1-impl worker' }],
  }],
})

describe('factory reaper', () => {
  it('reaps only registry-confirmed factory PIDs when the heartbeat is stale', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, registry())
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const live = new Set([111, 222])

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          return true
        },
        readProcessIdentity: async (pid) => ({ pid, startTime: 'started-111', cmdline: 'node ar-1-impl worker' }),
      })

      expect(report.reaped).toEqual([{ pid: 111, signals: ['SIGTERM', 'SIGKILL'] }])
      expect(killed).toEqual([
        { pid: 111, signal: 0 },
        { pid: 111, signal: 'SIGTERM' },
        { pid: 111, signal: 0 },
        { pid: 111, signal: 'SIGKILL' },
      ])
      expect(killed.some((entry) => entry.pid === 222)).toBe(false)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not kill a recycled PID whose current identity no longer matches the registry', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-reuse-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, registry())
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
        readProcessIdentity: async (pid) => ({ pid, startTime: 'foreign-start', cmdline: 'node foreign-worker' }),
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([{ pid: 111, reason: 'pid identity mismatch' }])
      expect(killed).toEqual([{ pid: 111, signal: 0 }])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not reap while the heartbeat is fresh', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-fresh-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(2_500))
      await writeJson(registryPath, registry())
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_000,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
      })

      expect(report).toEqual({ stale: false, reason: undefined, reaped: [], skipped: [] })
      expect(killed).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('stops its watchdog timer cleanly', async () => {
    vi.useFakeTimers()
    try {
      const reaper = new FactoryReaper({
        heartbeatPath: '/tmp/missing-heartbeat.json',
        registryPath: '/tmp/missing-registry.json',
        staleMs: 1_000,
        intervalMs: 100,
      })

      reaper.start()
      await reaper.stop()
      await vi.advanceTimersByTimeAsync(500)

      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })
})
