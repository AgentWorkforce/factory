import { createHash } from 'node:crypto'
import { mkdirSync, mkdtempSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

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

function fakeLinuxProc(listenerPid: number, parentPid: number, port: number): string {
  const root = mkdtempSync(join(tmpdir(), 'factory-preview-proc-'))
  mkdirSync(join(root, 'net'), { recursive: true })
  mkdirSync(join(root, String(listenerPid), 'fd'), { recursive: true })
  const hexPort = port.toString(16).toUpperCase().padStart(4, '0')
  const header = '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n'
  const listener = `   0: 0100007F:${hexPort} 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 12345 1\n`
  writeFileSync(join(root, 'net', 'tcp'), header + listener)
  writeFileSync(join(root, 'net', 'tcp6'), header)
  writeFileSync(join(root, String(listenerPid), 'status'), `Name:\tpreview\nPPid:\t${parentPid}\n`)
  symlinkSync('socket:[12345]', join(root, String(listenerPid), 'fd', '7'))
  return root
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
      env: {
        PATH: '/test/bin',
        HOME: '/home/factory',
        LC_ALL: 'C',
        RELAY_API_KEY: 'must-not-reach-preview',
        APP_SECRET: 'must-not-reach-preview',
      },
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
        env: {
          PATH: '/test/bin',
          HOME: '/home/factory',
          LC_ALL: 'C',
          PORT: '4173',
        },
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

  it('confirms a Linux listener owned by a descendant of the exact wrapper', async () => {
    const identity = persistedIdentity()
    const supervisor = new PreviewProcessSupervisor({
      platform: 'linux',
      procRoot: fakeLinuxProc(4_202, identity.pid, 3_000),
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: identity.startTime,
        cmdline: identity.cmdline,
      }),
    })

    await expect(supervisor.listenerOwnership(identity, 3_000)).resolves.toBe('owned')
  })

  it('rejects a Linux listener whose process tree is unrelated to the wrapper', async () => {
    const identity = persistedIdentity()
    const supervisor = new PreviewProcessSupervisor({
      platform: 'linux',
      procRoot: fakeLinuxProc(9_001, 1, 3_000),
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: identity.startTime,
        cmdline: identity.cmdline,
      }),
    })

    await expect(supervisor.listenerOwnership(identity, 3_000)).resolves.toBe('unrelated')
  })

  it('resolves macOS lsof listeners and verifies their parent chain', async () => {
    const identity = persistedIdentity()
    const runListenerCommand = vi.fn(async (file: string, args: string[]) => {
      if (file === '/usr/sbin/lsof') return 'p4202\n'
      if (file === '/bin/ps' && args.at(-1) === '4202') return `${identity.pid}\n`
      throw new Error(`unexpected listener command: ${file} ${args.join(' ')}`)
    })
    const supervisor = new PreviewProcessSupervisor({
      platform: 'darwin',
      runListenerCommand,
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: identity.startTime,
        cmdline: identity.cmdline,
      }),
    })

    await expect(supervisor.listenerOwnership(identity, 3_000)).resolves.toBe('owned')
    expect(runListenerCommand).toHaveBeenCalledWith('/usr/sbin/lsof', [
      '-nP', '-a', '-iTCP:3000', '-sTCP:LISTEN', '-Fp',
    ])
  })

  it('fails closed when listener ownership cannot be determined', async () => {
    const identity = persistedIdentity()
    const supervisor = new PreviewProcessSupervisor({
      resolveListenerPids: async () => undefined,
      readProcessIdentity: async () => ({
        pid: identity.pid,
        startTime: identity.startTime,
        cmdline: identity.cmdline,
      }),
    })

    await expect(supervisor.listenerOwnership(identity, 3_000)).resolves.toBe('indeterminate')
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

  it('terminates the PID returned by spawn when its exact identity never becomes observable', async () => {
    const terminateOptions: TerminatePidsOptions = { termGraceMs: 0 }
    const terminate = vi.fn(async (): Promise<TerminatePidsReport> => ({
      terminated: [{ pid: 7_777, signals: ['SIGTERM'] }],
      skipped: [],
    }))
    const supervisor = new PreviewProcessSupervisor({
      identityTimeoutMs: 0,
      findProcess: async () => ({ status: 'missing' }),
      readProcessIdentity: async () => undefined,
      spawn: () => ({ pid: 7_777, unref: vi.fn() }),
      terminatePids: terminate,
      terminateOptions,
    })

    await expect(supervisor.start({ id: 'unobservable', command, cwd, port: 3_000 }))
      .rejects.toThrow('exact identity was not observable and was terminated')
    expect(terminate).toHaveBeenCalledWith([7_777], terminateOptions)
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
