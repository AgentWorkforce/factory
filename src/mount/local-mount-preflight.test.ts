import { EventEmitter } from 'node:events'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ensureLocalMount } from './local-mount-preflight'
import { resolveRelayfileCli } from './relayfile-binary'

const spawnMock = vi.hoisted(() => vi.fn())

vi.mock('node:child_process', () => ({
  spawn: spawnMock,
}))

// Default the CLI resolver OFF so the existing tests exercise the raw-binary
// path deterministically (a real `relayfile` on PATH would otherwise be picked).
// CLI-path tests opt in by overriding this mock.
vi.mock('./relayfile-binary', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./relayfile-binary')>()
  return { ...actual, resolveRelayfileCli: vi.fn(() => undefined) }
})

const resolveCliMock = vi.mocked(resolveRelayfileCli)

const originalRelayfileMountBin = process.env.RELAYFILE_MOUNT_BIN

afterEach(() => {
  if (originalRelayfileMountBin === undefined) {
    delete process.env.RELAYFILE_MOUNT_BIN
  } else {
    process.env.RELAYFILE_MOUNT_BIN = originalRelayfileMountBin
  }
  spawnMock.mockReset()
  resolveCliMock.mockReset()
  resolveCliMock.mockReturnValue(undefined)
})

async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'local-mount-preflight-test-'))
  try {
    return await fn(dir)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}

async function installFakeBinary(dir: string): Promise<void> {
  const binary = join(dir, 'relayfile-mount')
  await writeFile(binary, '#!/bin/sh\n', 'utf8')
  await chmod(binary, 0o755)
  process.env.RELAYFILE_MOUNT_BIN = binary
}

function mockSuccessfulSpawn(onClose?: () => Promise<void>): void {
  spawnMock.mockImplementation(() => {
    const child = new EventEmitter() as EventEmitter & { stderr: EventEmitter }
    child.stderr = new EventEmitter()
    setTimeout(() => {
      void Promise.resolve(onClose?.()).then(() => child.emit('close', 0), () => child.emit('close', 1))
    }, 0)
    return child
  })
}

describe('ensureLocalMount', () => {
  it('waits for a well-formed state file after spawning the mount', async () => {
    await withTempDir(async (dir) => {
      await installFakeBinary(dir)
      const stateDir = join(dir, '.integrations', '.relay')
      const statePath = join(stateDir, 'state.json')
      mockSuccessfulSpawn(async () => {
        await mkdir(stateDir, { recursive: true })
        await writeFile(statePath, JSON.stringify({
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
          pid: process.pid,
        }), 'utf8')
      })

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
      expect(spawnMock).toHaveBeenCalledWith(process.env.RELAYFILE_MOUNT_BIN, [
        'start',
        'rw_test',
        '.integrations',
        '--background',
        '--rehome',
      ], {
        cwd: dir,
        stdio: ['ignore', 'ignore', 'pipe'],
      })
    })
  })

  it('auto-refreshes a stale mount by re-spawning so writebacks propagate', async () => {
    await withTempDir(async (dir) => {
      await installFakeBinary(dir)
      const stateDir = join(dir, '.integrations', '.relay')
      const statePath = join(stateDir, 'state.json')
      await mkdir(stateDir, { recursive: true })
      // stale: last reconcile well past the staleness threshold.
      await writeFile(statePath, JSON.stringify({
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      }), 'utf8')
      mockSuccessfulSpawn(async () => {
        await writeFile(statePath, JSON.stringify({
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
          pid: process.pid,
        }), 'utf8')
      })

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
      expect(spawnMock).toHaveBeenCalled()
    })
  })

  it('warns without re-spawning a stale mount when refreshStaleMount is false', async () => {
    await withTempDir(async (dir) => {
      await installFakeBinary(dir)
      const stateDir = join(dir, '.integrations', '.relay')
      await mkdir(stateDir, { recursive: true })
      await writeFile(join(stateDir, 'state.json'), JSON.stringify({
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      }), 'utf8')
      mockSuccessfulSpawn()

      await expect(ensureLocalMount('rw_test', dir, { refreshStaleMount: false })).resolves.toBeUndefined()
      expect(spawnMock).not.toHaveBeenCalled()
    })
  })

  it('prefers the relayfile CLI when available: stop then start, no token plumbing', async () => {
    await withTempDir(async (dir) => {
      const cli = join(dir, 'relayfile')
      await writeFile(cli, '#!/bin/sh\n', 'utf8')
      await chmod(cli, 0o755)
      resolveCliMock.mockReturnValue(cli)
      const stateDir = join(dir, '.integrations', '.relay')
      const statePath = join(stateDir, 'state.json')

      // stop -> exit 0 (no state); start -> writes a fresh state then exit 0.
      spawnMock.mockImplementation((_cmd: string, args: string[]) => {
        const child = new EventEmitter() as EventEmitter & { stderr: EventEmitter }
        child.stderr = new EventEmitter()
        const isStart = args[0] === 'start'
        setTimeout(() => {
          const finish = async (): Promise<void> => {
            if (isStart) {
              await mkdir(stateDir, { recursive: true })
              await writeFile(statePath, JSON.stringify({
                workspaceId: 'rw_test',
                lastReconcileAt: new Date().toISOString(),
                pid: process.pid,
              }), 'utf8')
            }
          }
          void finish().then(() => child.emit('close', 0), () => child.emit('close', 1))
        }, 0)
        return child
      })

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()

      // raw relayfile-mount binary must NOT be used when the CLI is present.
      expect(spawnMock).toHaveBeenCalledWith(cli, ['stop'], expect.objectContaining({ cwd: dir }))
      expect(spawnMock).toHaveBeenCalledWith(
        cli,
        ['start', 'rw_test', '.integrations', '--background'],
        expect.objectContaining({ cwd: dir }),
      )
      // no --rehome (that's a raw-binary flag) and no RELAYFILE_MOUNT_BIN reliance.
      const startCall = spawnMock.mock.calls.find((c) => c[1]?.[0] === 'start')
      expect(startCall?.[1]).not.toContain('--rehome')
    })
  })

  it('tolerates a failing best-effort stop and still starts via the CLI', async () => {
    await withTempDir(async (dir) => {
      const cli = join(dir, 'relayfile')
      await writeFile(cli, '#!/bin/sh\n', 'utf8')
      await chmod(cli, 0o755)
      resolveCliMock.mockReturnValue(cli)
      const stateDir = join(dir, '.integrations', '.relay')
      const statePath = join(stateDir, 'state.json')

      spawnMock.mockImplementation((_cmd: string, args: string[]) => {
        const child = new EventEmitter() as EventEmitter & { stderr: EventEmitter }
        child.stderr = new EventEmitter()
        const isStart = args[0] === 'start'
        setTimeout(() => {
          if (!isStart) {
            // stop fails (nothing mounted) — must be swallowed.
            child.stderr.emit('data', Buffer.from('no mount running'))
            child.emit('close', 1)
            return
          }
          void mkdir(stateDir, { recursive: true })
            .then(() => writeFile(statePath, JSON.stringify({
              workspaceId: 'rw_test',
              lastReconcileAt: new Date().toISOString(),
              pid: process.pid,
            }), 'utf8'))
            .then(() => child.emit('close', 0), () => child.emit('close', 1))
        }, 0)
        return child
      })

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
      expect(spawnMock).toHaveBeenCalledWith(
        cli,
        ['start', 'rw_test', '.integrations', '--background'],
        expect.objectContaining({ cwd: dir }),
      )
    })
  })

  it('confirms a CLI mount that records its pid under daemon.pid (not top-level pid)', async () => {
    await withTempDir(async (dir) => {
      const cli = join(dir, 'relayfile')
      await writeFile(cli, '#!/bin/sh\n', 'utf8')
      await chmod(cli, 0o755)
      resolveCliMock.mockReturnValue(cli)
      const stateDir = join(dir, '.integrations', '.relay')
      const statePath = join(stateDir, 'state.json')

      spawnMock.mockImplementation((_cmd: string, args: string[]) => {
        const child = new EventEmitter() as EventEmitter & { stderr: EventEmitter }
        child.stderr = new EventEmitter()
        const isStart = args[0] === 'start'
        setTimeout(() => {
          const finish = async (): Promise<void> => {
            if (isStart) {
              await mkdir(stateDir, { recursive: true })
              // CLI-daemonized mount: pid lives under daemon.pid, no top-level pid.
              await writeFile(statePath, JSON.stringify({
                workspaceId: 'rw_test',
                lastReconcileAt: new Date().toISOString(),
                daemon: { pid: process.pid },
              }), 'utf8')
            }
          }
          void finish().then(() => child.emit('close', 0), () => child.emit('close', 1))
        }, 0)
        return child
      })

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
    })
  })

  it('rejects a malformed state file instead of silently continuing', async () => {
    await withTempDir(async (dir) => {
      await installFakeBinary(dir)
      const stateDir = join(dir, '.integrations', '.relay')
      await mkdir(stateDir, { recursive: true })
      await writeFile(join(stateDir, 'state.json'), '{not-json', 'utf8')
      mockSuccessfulSpawn()

      await expect(ensureLocalMount('rw_test', dir, {
        stateWaitTimeoutMs: 5,
        stateWaitPollMs: 1,
      })).rejects.toThrow(/relayfile mount did not become ready/u)
    })
  })
})
