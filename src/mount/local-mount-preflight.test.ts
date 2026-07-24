import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ensureLocalMount } from './local-mount-preflight'
import { MountAuthScopeError } from './mount-auth-error'

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

  it('accepts a fresh SDK mount state that intentionally omits a daemon pid', async () => {
    await withTempDir(async (dir) => {
      const startMount = vi.fn(async () => {
        await writeMountState(dir, {
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
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

  it('can suppress routine stale-refresh progress for caller-level summaries', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      })
      const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)

      await expect(ensureLocalMount('rw_test', dir, {
        startMount: async () => writeMountState(dir, {
          workspaceId: 'rw_test',
          lastReconcileAt: new Date().toISOString(),
          pid: process.pid,
        }),
        suppressStaleRefreshLogs: true,
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()

      expect(stderr).not.toHaveBeenCalledWith(expect.stringContaining('local mount is stale'))
      expect(stderr).not.toHaveBeenCalledWith(expect.stringContaining('local mount refreshed'))
    })
  })

  it('does not report a stale state file as refreshed when the SDK start leaves it unchanged', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      })
      const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)

      await expect(ensureLocalMount('rw_test', dir, {
        startMount: vi.fn(async () => {}),
        stateWaitTimeoutMs: 5,
        stateWaitPollMs: 1,
      })).resolves.toBeUndefined()

      expect(stderr).toHaveBeenCalledWith(expect.stringContaining('auto-refresh failed'))
      expect(stderr).not.toHaveBeenCalledWith(expect.stringContaining('local mount refreshed'))
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

  it('does not restart a fresh pidless existing mount', async () => {
    await withTempDir(async (dir) => {
      await writeMountState(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
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

  const SCOPE_403 = {
    kind: 'bootstrap_stalled',
    code: 'bootstrap_stall_cycle_limit',
    message: 'http 403 forbidden: missing required scope: fs:read',
  }

  async function writeMountStateWithError(
    dir: string,
    state: { workspaceId: string; lastReconcileAt: string; pid?: number },
    lastError: unknown,
  ): Promise<void> {
    const stateDir = join(dir, '.integrations', '.relay')
    await mkdir(stateDir, { recursive: true })
    await writeFile(join(stateDir, 'state.json'), JSON.stringify({ ...state, lastError }), 'utf8')
  }

  it('throws a terminal MountAuthScopeError for a stale mount with a scope-403, without refreshing', async () => {
    await withTempDir(async (dir) => {
      await writeMountStateWithError(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
        pid: process.pid,
      }, SCOPE_403)
      const startMount = vi.fn(async () => {})

      await expect(ensureLocalMount('rw_test', dir, { startMount }))
        .rejects.toThrow(MountAuthScopeError)
      // A re-launch would 403 again — the preflight must not attempt it.
      expect(startMount).not.toHaveBeenCalled()
    })
  })

  it('surfaces the remediation but does not tear down a still-reconciling mount that reports a scope-403', async () => {
    await withTempDir(async (dir) => {
      await writeMountStateWithError(dir, {
        workspaceId: 'rw_test',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      }, SCOPE_403)
      const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)
      const startMount = vi.fn(async () => {})

      await expect(ensureLocalMount('rw_test', dir, { startMount })).resolves.toBeUndefined()
      expect(startMount).not.toHaveBeenCalled()
      expect(stderr).toHaveBeenCalledWith(expect.stringContaining('lacks the filesystem scope'))
    })
  })

  it('escalates a first-ever bootstrap that 403s into a terminal MountAuthScopeError', async () => {
    await withTempDir(async (dir) => {
      // No state file at start; the bootstrap "succeeds" but writes a stale
      // state recording the scope 403 and never becomes ready.
      const startMount = vi.fn(async () => {
        await writeMountStateWithError(dir, {
          workspaceId: 'rw_test',
          lastReconcileAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
          pid: process.pid,
        }, SCOPE_403)
      })

      await expect(ensureLocalMount('rw_test', dir, {
        startMount,
        stateWaitTimeoutMs: 5,
        stateWaitPollMs: 1,
      })).rejects.toThrow(MountAuthScopeError)
    })
  })
})
