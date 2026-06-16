import { EventEmitter } from 'node:events'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ensureLocalMount } from './local-mount-preflight'

const spawnMock = vi.hoisted(() => vi.fn())

vi.mock('node:child_process', () => ({
  spawn: spawnMock,
}))

const originalRelayfileMountBin = process.env.RELAYFILE_MOUNT_BIN

afterEach(() => {
  if (originalRelayfileMountBin === undefined) {
    delete process.env.RELAYFILE_MOUNT_BIN
  } else {
    process.env.RELAYFILE_MOUNT_BIN = originalRelayfileMountBin
  }
  spawnMock.mockReset()
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
