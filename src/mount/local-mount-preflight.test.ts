import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ensureLocalMount } from './local-mount-preflight'

afterEach(() => {
  vi.restoreAllMocks()
})

async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'local-mount-preflight-test-'))
  try {
    return await fn(dir)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}

async function writeMountState(
  dir: string,
  state: { workspaceId: string; lastReconcileAt: string; pid?: number; daemon?: { pid: number } },
): Promise<void> {
  const stateDir = join(dir, '.integrations', '.relay')
  await mkdir(stateDir, { recursive: true })
  await writeFile(join(stateDir, 'state.json'), JSON.stringify(state), 'utf8')
}

describe('ensureLocalMount', () => {
  it('starts through the injected SDK mount path and waits for a valid state file', async () => {
    await withTempDir(async (dir) => {
      const startMount = vi.fn(async () => {
        await writeMountState(dir, {
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
          pid: process.pid,
        })
      })

      await expect(ensureLocalMount('rw_test', dir, {
        startMount,
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
      expect(startMount).toHaveBeenCalledTimes(1)
    })
  })

  it('accepts the cloud UUID alias emitted by an SDK mount session', async () => {
    await withTempDir(async (dir) => {
      const startMount = vi.fn(async () => {
        await writeMountState(dir, {
          workspaceId: 'cloud-uuid',
          lastReconcileAt: new Date().toISOString(),
          daemon: { pid: process.pid },
        })
      })

      await expect(ensureLocalMount('rw_test', dir, {
        startMount,
        acceptableWorkspaceIds: ['cloud-uuid'],
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
    })
  })

  it('auto-refreshes a stale mount through the SDK callback', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      })
      const startMount = vi.fn(async () => {
        await writeMountState(dir, {
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
          pid: process.pid,
        })
      })

      await expect(ensureLocalMount('rw_test', dir, {
        startMount,
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()
      expect(startMount).toHaveBeenCalledTimes(1)
    })
  })

  it('does not restart a healthy existing mount', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      })
      const startMount = vi.fn(async () => {})

      await expect(ensureLocalMount('rw_test', dir, { startMount })).resolves.toBeUndefined()
      expect(startMount).not.toHaveBeenCalled()
    })
  })

  it('warns without restarting a stale mount when refreshStaleMount is false', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      })
      const startMount = vi.fn(async () => {})

      await expect(ensureLocalMount('rw_test', dir, {
        startMount,
        refreshStaleMount: false,
      })).resolves.toBeUndefined()
      expect(startMount).not.toHaveBeenCalled()
    })
  })

  it('rejects when the SDK start returns without materializing mount state', async () => {
    await withTempDir(async (dir) => {
      await expect(ensureLocalMount('rw_test', dir, {
        startMount: vi.fn(async () => {}),
        stateWaitTimeoutMs: 5,
        stateWaitPollMs: 1,
      })).rejects.toThrow(/relayfile mount did not become ready/u)
    })
  })
})
