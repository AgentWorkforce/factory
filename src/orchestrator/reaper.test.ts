import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it, vi } from 'vitest'

import type { FactoryInFlightRegistry, FactoryLoopHeartbeat } from '../types'
import { FactoryReaper, reapFactoryOrphansOnce, terminatePids } from './reaper'
import type { FleetClient } from '../ports'

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

  it('reaps stale stopping heartbeats so wedged teardown is still detected', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-stale-stopping-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, { ...heartbeat(1_000), status: 'stopping' })
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
        readProcessIdentity: async (pid) => ({ pid, startTime: 'started-111', cmdline: 'node ar-1-impl worker' }),
      })

      expect(report).toMatchObject({
        stale: true,
        reason: 'heartbeat stale',
        reaped: [{ pid: 111, signals: ['SIGTERM', 'SIGKILL'] }],
      })
      expect(killed.some((entry) => entry.signal === 'SIGTERM')).toBe(true)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('terminates a whole process tree before killing the parent root', async () => {
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const brokerParentPid = 68009
    const live = new Set([brokerParentPid, 901969, 901970, 901971, 901972])
    const children = new Map<number, number[]>([
      [901969, [901970, 901971, brokerParentPid]],
      [901970, [901972]],
    ])

    const report = await terminatePids([901969], {
      termGraceMs: 0,
      protectedPids: [brokerParentPid],
      readChildPids: async (pid) => children.get(pid) ?? [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') live.delete(pid)
        return true
      },
    })

    expect(report.terminated.map((entry) => entry.pid)).toEqual([901972, 901970, 901971, 901969])
    expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid)).toEqual([
      901972,
      901970,
      901971,
      901969,
    ])
    expect(killed.some((entry) => entry.pid === brokerParentPid)).toBe(false)
    expect(live).toEqual(new Set([brokerParentPid]))
  })

  it('continues terminating later PIDs when an earlier PID is already gone', async () => {
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
    const live = new Set([222])

    const report = await terminatePids([111, 222], {
      termGraceMs: 0,
      readChildPids: async () => [],
      kill: (pid, signal) => {
        killed.push({ pid, signal })
        if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
        if (signal === 'SIGKILL') live.delete(pid)
        return true
      },
    })

    expect(report.skipped).toEqual([{ pid: 111, reason: 'pid not running' }])
    expect(report.terminated).toEqual([{ pid: 222, signals: ['SIGTERM', 'SIGKILL'] }])
    expect(killed).toEqual([
      { pid: 111, signal: 0 },
      { pid: 222, signal: 0 },
      { pid: 222, signal: 'SIGTERM' },
      { pid: 222, signal: 0 },
      { pid: 222, signal: 'SIGKILL' },
    ])
    expect(live).toEqual(new Set())
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

  it('resolves and reaps registry agents that only persisted a name before factory crash', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-9-impl',
          role: 'implementer',
          issue: { key: 'AR-9', uuid: 'uuid-9', path: '/linear/issues/AR-9__uuid-9.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const live = new Set([9090])
      const fleet: Pick<FleetClient, 'resolveAgentPid'> = {
        resolveAgentPid: async (name) => name === 'ar-9-impl'
          ? { status: 'found', pid: 9090 }
          : { status: 'missing' },
      }

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          return true
        },
        readProcessIdentity: async (pid) => ({ pid, startTime: 'started-9090', cmdline: 'node ar-9-impl worker' }),
      })

      expect(report.reaped).toEqual([{ pid: 9090, signals: ['SIGTERM', 'SIGKILL'] }])
      expect(killed).toEqual([
        { pid: 9090, signal: 0 },
        { pid: 9090, signal: 0 },
        { pid: 9090, signal: 'SIGTERM' },
        { pid: 9090, signal: 0 },
        { pid: 9090, signal: 'SIGKILL' },
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('uses the anchored launcher root before a broker-resolved worker child for name-only registry agents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-launcher-primary-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-15-impl',
          role: 'implementer',
          issue: { key: 'AR-15', uuid: 'uuid-15', path: '/linear/issues/AR-15__uuid-15.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const live = new Set([15_015, 15_016])

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet: { resolveAgentPid: async () => ({ status: 'found', pid: 15_016 }) },
        processFinder: async () => {
          return { status: 'found', identity: { pid: 15_015, startTime: 'launcher', cmdline: 'node --agent-name ar-15-impl launcher' } }
        },
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          return true
        },
        readProcessIdentity: async (pid) => ({
          pid,
          startTime: pid === 15_015 ? 'launcher' : 'worker',
          cmdline: `node --agent-name ar-15-impl ${pid === 15_015 ? 'launcher' : 'worker'}`,
        }),
        readChildPids: async (pid) => pid === 15_015 ? [15_016] : [],
      })

      expect(report.reaped).toEqual([
        { pid: 15_016, signals: ['SIGTERM', 'SIGKILL'] },
        { pid: 15_015, signals: ['SIGTERM', 'SIGKILL'] },
      ])
      expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid)).toEqual([15_016, 15_015])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('falls back to a ps-discovered process when a name-only registry agent has unresolved broker PID', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-ps-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-12-impl',
          role: 'implementer',
          issue: { key: 'AR-12', uuid: 'uuid-12', path: '/linear/issues/AR-12__uuid-12.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const live = new Set([12_012])
      const fleet: Pick<FleetClient, 'resolveAgentPid'> = {
        resolveAgentPid: async () => ({ status: 'unresolved' }),
      }

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet,
        processFinder: async () => ({
          status: 'found',
          identity: { pid: 12_012, startTime: 'started-12012', cmdline: 'node --agent-name ar-12-impl worker' },
        }),
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          if (!live.has(pid)) throw Object.assign(new Error('not running'), { code: 'ESRCH' })
          return true
        },
        readChildPids: async () => [],
        readParentPid: async () => undefined,
        readProcessIdentity: async (pid) => ({ pid, startTime: 'started-12012', cmdline: 'node --agent-name ar-12-impl worker' }),
      })

      expect(report.reaped).toEqual([{ pid: 12_012, signals: ['SIGTERM', 'SIGKILL'] }])
      expect(killed.filter((entry) => entry.signal === 'SIGTERM').map((entry) => entry.pid)).toEqual([12_012])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('skips a name-only registry agent when broker PID and ps fallback are both missing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-missing-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-13-impl',
          role: 'implementer',
          issue: { key: 'AR-13', uuid: 'uuid-13', path: '/linear/issues/AR-13__uuid-13.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet: { resolveAgentPid: async () => ({ status: 'unresolved' }) },
        processFinder: async () => ({ status: 'missing' }),
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([{ reason: 'pid missing for ar-13-impl' }])
      expect(killed).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects a ps-discovered fallback process whose identity does not match the agent', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-ps-foreign-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-14-impl',
          role: 'implementer',
          issue: { key: 'AR-14', uuid: 'uuid-14', path: '/linear/issues/AR-14__uuid-14.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet: { resolveAgentPid: async () => ({ status: 'unresolved' }) },
        processFinder: async () => ({
          status: 'found',
          identity: { pid: 14_014, startTime: 'foreign-start', cmdline: 'node --agent-name foreign worker' },
        }),
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([{ pid: 14_014, reason: 'pid identity mismatch' }])
      expect(killed).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not reap a name-only registry agent when the resolved PID identity does not match', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-reuse-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-10-impl',
          role: 'implementer',
          issue: { key: 'AR-10', uuid: 'uuid-10', path: '/linear/issues/AR-10__uuid-10.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const fleet: Pick<FleetClient, 'resolveAgentPid'> = {
        resolveAgentPid: async (name) => name === 'ar-10-impl'
          ? { status: 'found', pid: 10_010 }
          : { status: 'missing' },
      }

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
        readProcessIdentity: async (pid) => ({ pid, startTime: 'foreign-start', cmdline: 'node foreign-worker' }),
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([{ pid: 10_010, reason: 'pid identity mismatch' }])
      expect(killed).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not signal a name-only registry agent that resolves to a protected broker PID', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-reaper-name-only-protected-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      await writeJson(heartbeatPath, heartbeat(1_000))
      await writeJson(registryPath, {
        pid: 100,
        updatedAt: new Date(1_000).toISOString(),
        updatedAtMs: 1_000,
        agents: [{
          name: 'ar-11-impl',
          role: 'implementer',
          issue: { key: 'AR-11', uuid: 'uuid-11', path: '/linear/issues/AR-11__uuid-11.json' },
          pids: [],
          processes: [],
        }],
      } satisfies FactoryInFlightRegistry)
      const brokerPid = 68_009
      const brokerChildPid = 990_099
      const killed: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = []
      const fleet: Pick<FleetClient, 'protectedPids' | 'resolveAgentPid'> = {
        protectedPids: async () => [brokerPid],
        resolveAgentPid: async (name) => name === 'ar-11-impl'
          ? { status: 'found', pid: brokerPid }
          : { status: 'missing' },
      }

      const report = await reapFactoryOrphansOnce({
        heartbeatPath,
        registryPath,
        staleMs: 1_000,
        nowMs: 3_001,
        termGraceMs: 0,
        fleet,
        kill: (pid, signal) => {
          killed.push({ pid, signal })
          return true
        },
        readChildPids: async (pid) => pid === brokerPid ? [brokerChildPid] : [],
        readParentPid: async () => undefined,
        readProcessIdentity: async (pid) => ({ pid, startTime: 'broker-start', cmdline: `node ar-11-impl broker ${pid}` }),
      })

      expect(report.reaped).toEqual([])
      expect(report.skipped).toEqual([{ pid: brokerPid, reason: 'protected pid' }])
      expect(killed).toEqual([
        { pid: brokerPid, signal: 0 },
      ])
      expect(killed.some((entry) => entry.pid === brokerChildPid)).toBe(false)
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
