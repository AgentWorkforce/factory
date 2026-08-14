import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

import { resolveRegisteredWorkspaceMirror } from './workspace-mirror'

async function withTempHome<T>(fn: (home: string) => Promise<T>): Promise<T> {
  const home = await mkdtemp(join(tmpdir(), 'factory-workspace-mirror-'))
  try {
    return await fn(home)
  } finally {
    await rm(home, { recursive: true, force: true })
  }
}

describe('resolveRegisteredWorkspaceMirror', () => {
  it('uses the workspace registration rather than a caller checkout path', async () => {
    await withTempHome(async (home) => {
      const mirror = join(home, 'shared', '.integrations')
      await mkdir(join(home, '.relayfile'), { recursive: true })
      await writeFile(join(home, '.relayfile', 'workspaces.json'), JSON.stringify({
        workspaces: [{ id: 'rw_shared', localDir: mirror }],
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_shared'], home)).toEqual({
        localDir: mirror,
        source: 'workspace-registry',
      })
    })
  })

  it('resolves a registered workspace by its operator-facing name', async () => {
    await withTempHome(async (home) => {
      const mirror = join(home, 'chief', '.integrations')
      await mkdir(join(home, '.relayfile'), { recursive: true })
      await writeFile(join(home, '.relayfile', 'workspaces.json'), JSON.stringify({
        workspaces: [{ id: 'rw_7ccfea89', name: 'default', localDir: mirror }],
      }))

      expect(resolveRegisteredWorkspaceMirror(['default'], home)).toEqual({
        localDir: mirror,
        source: 'workspace-registry',
      })
      expect(resolveRegisteredWorkspaceMirror(['rw_7ccfea89'], home)).toEqual({
        localDir: mirror,
        source: 'workspace-registry',
      })
    })
  })

  it('leaves the workspace registry byte-for-byte unchanged on lookup miss', async () => {
    await withTempHome(async (home) => {
      const registryPath = join(home, '.relayfile', 'workspaces.json')
      await mkdir(join(home, '.relayfile'), { recursive: true })
      await writeFile(registryPath, JSON.stringify({
        workspaces: [{ id: 'rw_real', name: 'default', localDir: join(home, 'chief', '.integrations') }],
      }, null, 2))
      const before = await readFile(registryPath, 'utf8')

      expect(resolveRegisteredWorkspaceMirror(['rw_missing'], home)).toBeUndefined()

      expect(await readFile(registryPath, 'utf8')).toBe(before)
    })
  })

  it('anchors a legacy relative workspace registry root to the Relayfile home', async () => {
    await withTempHome(async (home) => {
      await mkdir(join(home, '.relayfile'), { recursive: true })
      await writeFile(join(home, '.relayfile', 'workspaces.json'), JSON.stringify({
        workspaces: [{ id: 'rw_shared', localDir: '.integrations' }],
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_shared'], home)).toEqual({
        localDir: join(home, '.integrations'),
        source: 'workspace-registry',
      })
    })
  })

  it('falls back to the Relayfile private mount registration', async () => {
    await withTempHome(async (home) => {
      const mirror = join(home, 'registered', '.integrations')
      const stateDir = join(home, '.relayfile-mount-state', 'mount-1')
      await mkdir(stateDir, { recursive: true })
      await writeFile(join(stateDir, 'state.json'), JSON.stringify({
        workspaceId: 'cloud-workspace-id',
        localRoot: mirror,
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_handle', 'cloud-workspace-id'], home)).toEqual({
        localDir: mirror,
        source: 'mount-state',
      })
    })
  })

  it('anchors a legacy relative mount-state root to the Relayfile home', async () => {
    await withTempHome(async (home) => {
      const stateDir = join(home, '.relayfile-mount-state', 'mount-1')
      await mkdir(stateDir, { recursive: true })
      await writeFile(join(stateDir, 'state.json'), JSON.stringify({
        workspaceId: 'rw_shared',
        localRoot: 'registered/.integrations',
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_shared'], home)).toEqual({
        localDir: join(home, 'registered', '.integrations'),
        source: 'mount-state',
      })
    })
  })

  it('refuses ambiguous workspace-registry roots across accepted aliases', async () => {
    await withTempHome(async (home) => {
      await mkdir(join(home, '.relayfile'), { recursive: true })
      await writeFile(join(home, '.relayfile', 'workspaces.json'), JSON.stringify({
        workspaces: [
          { id: 'rw_handle', localDir: join(home, 'first', '.integrations') },
          { id: 'cloud-workspace-id', localDir: join(home, 'second', '.integrations') },
        ],
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_handle', 'cloud-workspace-id'], home)).toBeUndefined()
    })
  })

  it('refuses to guess when stale local state names more than one mirror', async () => {
    await withTempHome(async (home) => {
      const stateRoot = join(home, '.relayfile-mount-state')
      await Promise.all(['one', 'two'].map(async (name) => {
        const stateDir = join(stateRoot, name)
        await mkdir(stateDir, { recursive: true })
        await writeFile(join(stateDir, 'state.json'), JSON.stringify({
          workspaceId: 'rw_shared',
          localRoot: join(home, name, '.integrations'),
        }))
      }))

      expect(resolveRegisteredWorkspaceMirror(['rw_shared'], home)).toBeUndefined()
    })
  })
})
