import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { checkMountStaleness } from './relayfile-binary'

afterEach(() => {
  vi.restoreAllMocks()
})

async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'relayfile-binary-test-'))
  try {
    return await fn(dir)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}

async function writeState(
  dir: string,
  state: { workspaceId?: string; lastReconcileAt?: string; pid?: number; daemon?: { pid?: number } },
): Promise<string> {
  const statePath = join(dir, 'state.json')
  await writeFile(statePath, JSON.stringify(state), 'utf8')
  return statePath
}

describe('checkMountStaleness', () => {
  it('leaves missing state non-stale so the caller can decide whether to start', async () => {
    await withTempDir(async (dir) => {
      expect(checkMountStaleness(join(dir, 'missing.json'), 'rw_test')).toEqual({ stale: false })
    })
  })

  it('marks a registered state for another workspace stale', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_other',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({
        stale: true,
        reason: 'workspace mismatch: registered=rw_other expected=rw_test',
      })
    })
  })

  it('accepts a registered id that matches an alternate workspace alias (handle vs cloud UUID)', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: '50587328-441d-4acb-b8f3-dbe1b3c5de99',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      })

      expect(
        checkMountStaleness(statePath, 'rw_7ccfea89', ['50587328-441d-4acb-b8f3-dbe1b3c5de99']),
      ).toEqual({ stale: false, pid: process.pid })
    })
  })

  it('still reports a mismatch when the registered id matches neither the workspace nor an alias', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_other',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      })

      expect(checkMountStaleness(statePath, 'rw_test', ['rw_alias'])).toEqual({
        stale: true,
        reason: 'workspace mismatch: registered=rw_other expected=rw_test|rw_alias',
      })
    })
  })

  it('marks an old reconcile timestamp stale before checking process liveness', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 16 * 60 * 1000).toISOString(),
        pid: process.pid,
      })

      const result = checkMountStaleness(statePath, 'rw_test')
      expect(result.stale).toBe(true)
      expect(result.reason).toMatch(/^last reconcile 16m ago$/u)
      expect(result.pid).toBe(process.pid)
    })
  })

  it('marks a dead mount process stale', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        pid: 12345,
      })
      vi.spyOn(process, 'kill').mockImplementation(() => {
        throw Object.assign(new Error('not found'), { code: 'ESRCH' })
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({
        stale: true,
        reason: 'mount process (pid 12345) is not running',
        pid: 12345,
      })
    })
  })

  it('treats a fresh pid-less state as healthy', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        // no pid: a fresh reconcile is still proof of liveness
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({ stale: false })
    })
  })

  it('uses daemon.pid for liveness when the top-level pid is absent', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        daemon: { pid: process.pid },
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({ stale: false, pid: process.pid })
    })
  })

  it('marks a stale mount via a dead daemon.pid even within the reconcile window', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        daemon: { pid: 12345 },
      })
      vi.spyOn(process, 'kill').mockImplementation(() => {
        throw Object.assign(new Error('not found'), { code: 'ESRCH' })
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({
        stale: true,
        reason: 'mount process (pid 12345) is not running',
        pid: 12345,
      })
    })
  })

  it('still marks a pid-less state stale when the reconcile timestamp is old', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 16 * 60 * 1000).toISOString(),
      })

      const result = checkMountStaleness(statePath, 'rw_test')
      expect(result.stale).toBe(true)
      expect(result.reason).toMatch(/^last reconcile 16m ago$/u)
    })
  })

  it('returns non-stale with the mount pid when state is fresh and the process is alive', async () => {
    await withTempDir(async (dir) => {
      const statePath = await writeState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      })

      expect(checkMountStaleness(statePath, 'rw_test')).toEqual({ stale: false, pid: process.pid })
    })
  })
})
