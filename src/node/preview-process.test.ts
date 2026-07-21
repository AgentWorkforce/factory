import { createHash } from 'node:crypto'

import { describe, expect, it, vi } from 'vitest'

import type { AgentProcessLookupResult, ProcessIdentity } from '../orchestrator/process-identity'
import type { TerminatePidsOptions, TerminatePidsReport } from '../orchestrator/reaper'
import {
  PreviewProcessSupervisor,
  previewProcessMarker,
  type PreviewProcessIdentity,
  type PreviewProcessSpawnOptions,
} from './preview-process'

const cwd = '/work/issue checkout'
const command = 'npm run dev -- --host 127.0.0.1'

function wrapperCommandLine(marker: string, expectedCwd = cwd, expectedCommand = command): string {
  const encodedCwd = Buffer.from(expectedCwd, 'utf8').toString('base64url')
  const commandHash = sha256(expectedCommand)
  return `/bin/sh -c wrapper --agent-name ${marker} --factory-preview-cwd ${encodedCwd} --factory-preview-command ${commandHash} ${expectedCommand}`
}

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex')
}

function persistedIdentity(overrides: Partial<PreviewProcessIdentity> = {}): PreviewProcessIdentity {
  const marker = overrides.marker ?? previewProcessMarker('preview-1')
  return {
    pid: 4_201,
    startTime: 'Mon Jul 21 12:00:00 2026',
    cmdline: wrapperCommandLine(marker),
    cwd,
    marker,
    ...overrides,
  }
}

describe('PreviewProcessSupervisor', () => {
  it('starts and unrefs a detached marked wrapper with PORT and exact identity', async () => {
    const spawned: Array<{ file: string; args: string[]; options: PreviewProcessSpawnOptions }> = []
    const unref = vi.fn()
    const readProcessIdentity = vi.fn(async (pid: number): Promise<ProcessIdentity | undefined> => {
      const marker = spawned[0]?.args[3]
      return marker
        ? { pid, startTime: 'started-4201', cmdline: wrapperCommandLine(marker) }
        : undefined
    })
    const supervisor = new PreviewProcessSupervisor({
      env: { PATH: '/test/bin' },
      findProcess: async () => ({ status: 'missing' }),
      readProcessIdentity,
      spawn: (file, args, options) => {
        spawned.push({ file, args, options })
        return { pid: 4_201, unref }
      },
    })

    const identity = await supervisor.start({ id: 'preview-1', command, cwd, port: 4_173 })

    expect(identity).toEqual({
      pid: 4_201,
      startTime: 'started-4201',
      cmdline: wrapperCommandLine(previewProcessMarker('preview-1')),
      cwd,
      marker: previewProcessMarker('preview-1'),
    })
    expect(unref).toHaveBeenCalledOnce()
    expect(spawned).toHaveLength(1)
    expect(spawned[0]).toMatchObject({
      file: '/bin/sh',
      options: {
        cwd,
        detached: true,
        env: { PATH: '/test/bin', PORT: '4173' },
        stdio: 'ignore',
      },
    })
    expect(spawned[0]!.args).toEqual(expect.arrayContaining([
      '--agent-name',
      previewProcessMarker('preview-1'),
      '--factory-preview-cwd',
      Buffer.from(cwd).toString('base64url'),
      '--factory-preview-command',
      sha256(command),
      command,
    ]))
  })

  it('recovers one exact wrapper by deterministic marker without spawning again', async () => {
    const marker = previewProcessMarker('crash-gap-preview')
    const recovered = persistedIdentity({
      pid: 7_701,
      marker,
      cmdline: wrapperCommandLine(marker),
    })
    const spawn = vi.fn()
    const findProcess = vi.fn(async (name: string): Promise<AgentProcessLookupResult> => {
      expect(name).toBe(marker)
      return {
        status: 'found',
        identity: {
          pid: recovered.pid,
          startTime: recovered.startTime,
          cmdline: recovered.cmdline,
        },
      }
    })
    const supervisor = new PreviewProcessSupervisor({ findProcess, spawn })

    await expect(supervisor.find('crash-gap-preview')).resolves.toEqual(recovered)
    await expect(supervisor.start({ id: 'crash-gap-preview', command, cwd, port: 3_000 })).resolves.toEqual(recovered)
    expect(spawn).not.toHaveBeenCalled()
  })

  it('fails closed when recovery finds multiple marked process trees', async () => {
    const supervisor = new PreviewProcessSupervisor({
      findProcess: async () => ({ status: 'ambiguous' }),
    })

    await expect(supervisor.find('preview-1')).rejects.toThrow('matched multiple process trees')
  })

  it('checks persisted start time and command line before reporting a process running', async () => {
    const identity = persistedIdentity()
    let current: ProcessIdentity | undefined = {
      pid: identity.pid,
      startTime: identity.startTime,
      cmdline: identity.cmdline,
    }
    const supervisor = new PreviewProcessSupervisor({
      findProcess: async () => ({ status: 'missing' }),
      readProcessIdentity: async () => current,
    })

    await expect(supervisor.isRunning(identity)).resolves.toBe(true)
    current = { ...current, startTime: 'recycled-start' }
    await expect(supervisor.isRunning(identity)).resolves.toBe(false)
    current = { pid: identity.pid, startTime: identity.startTime, cmdline: 'node foreign-worker' }
    await expect(supervisor.isRunning(identity)).resolves.toBe(false)
  })

  it('terminates the identity-checked wrapper tree through terminatePids', async () => {
    const identity = persistedIdentity()
    const terminateOptions: TerminatePidsOptions = { termGraceMs: 0, protectedPids: [99] }
    const terminate = vi.fn(async (): Promise<TerminatePidsReport> => ({
      terminated: [{ pid: identity.pid, signals: ['SIGTERM'] }],
      skipped: [],
    }))
    const supervisor = new PreviewProcessSupervisor({
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: identity.startTime,
        cmdline: identity.cmdline,
      }),
      terminatePids: terminate,
      terminateOptions,
    })

    await expect(supervisor.stop(identity)).resolves.toBe(true)
    expect(terminate).toHaveBeenCalledWith([identity.pid], terminateOptions)
  })

  it('refuses to terminate a recycled PID whose identity no longer matches', async () => {
    const identity = persistedIdentity()
    const terminate = vi.fn()
    const supervisor = new PreviewProcessSupervisor({
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: 'new-process-start',
        cmdline: 'node --agent-name foreign-worker',
      }),
      terminatePids: terminate,
    })

    await expect(supervisor.stop(identity)).resolves.toBe(false)
    expect(terminate).not.toHaveBeenCalled()
  })

  it('rejects invalid launch inputs before process discovery or spawn', async () => {
    const findProcess = vi.fn()
    const spawn = vi.fn()
    const supervisor = new PreviewProcessSupervisor({ findProcess, spawn })

    await expect(supervisor.start({ id: '', command, cwd, port: 3_000 })).rejects.toThrow('id must be non-empty')
    await expect(supervisor.start({ id: 'preview-1', command: ' ', cwd, port: 3_000 })).rejects.toThrow('command must be non-empty')
    await expect(supervisor.start({ id: 'preview-1', command, cwd, port: 70_000 })).rejects.toThrow('between 1 and 65535')
    expect(findProcess).not.toHaveBeenCalled()
    expect(spawn).not.toHaveBeenCalled()
  })
})
