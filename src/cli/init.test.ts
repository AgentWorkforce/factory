import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

import { FakeMountClient } from '../testing'
import { initializeFactory } from './init'

describe('factory init', () => {
  it('creates a GitHub-native config for the repository in the checkout', async () => {
    const cwd = await mkdtemp(join(tmpdir(), 'factory-init-'))
    try {
      const output = buffer()
      const result = await initializeFactory({
        cwd,
        stdout: output,
        stderr: buffer(),
        gitRemote: async () => 'git@github.com:Acme/widgets.git',
        commandExists: async () => true,
        resolveWorkspace: async () => ({ workspaceId: 'rw_widgets', cloudWorkspaceId: 'workspace-uuid' }),
        ensureLocalMount: async () => {},
        cloudMountFromConfig: async () => new FakeMountClient(),
      })

      expect(result).toEqual({ configPath: join(cwd, 'factory.config.json'), repo: 'Acme/widgets', workspaceId: 'rw_widgets' })
      expect(JSON.parse(await readFile(result.configPath, 'utf8'))).toEqual({
        workspaceId: 'rw_widgets',
        issueSource: 'github',
        repos: {
          org: 'Acme',
          names: ['widgets'],
          default: 'Acme/widgets',
          clonePaths: { 'Acme/widgets': cwd },
        },
      })
      expect(output.text()).toContain('Add the `garden` label')
    } finally {
      await rm(cwd, { recursive: true, force: true })
    }
  })

  it('does not write a config when a dependency or GitHub connection is missing', async () => {
    const cwd = await mkdtemp(join(tmpdir(), 'factory-init-missing-'))
    try {
      await expect(initializeFactory({
        cwd,
        repo: 'Acme/widgets',
        commandExists: async (command) => command === 'relayfile',
        stderr: buffer(),
      })).rejects.toThrow('Missing required dependency')

      await expect(readFile(join(cwd, 'factory.config.json'), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' })

      await expect(initializeFactory({
        cwd,
        repo: 'Acme/widgets',
        commandExists: async () => true,
        resolveWorkspace: async () => ({ workspaceId: 'rw_widgets' }),
        ensureLocalMount: async () => {},
        cloudMountFromConfig: async () => {
          const mount = new FakeMountClient()
          mount.setSubRoot('/github/repos/Acme/widgets', 'absent')
          return mount
        },
        stderr: buffer(),
      })).rejects.toThrow('GitHub connection')
      await expect(readFile(join(cwd, 'factory.config.json'), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' })
    } finally {
      await rm(cwd, { recursive: true, force: true })
    }
  })

  it('will not replace an existing config', async () => {
    const cwd = await mkdtemp(join(tmpdir(), 'factory-init-existing-'))
    try {
      const path = join(cwd, 'factory.config.json')
      await writeFile(path, '{"keep":true}\n')
      await expect(initializeFactory({
        cwd,
        repo: 'Acme/widgets',
        commandExists: async () => true,
        resolveWorkspace: async () => ({ workspaceId: 'rw_widgets' }),
      })).rejects.toThrow('Refusing to overwrite')
      expect(await readFile(path, 'utf8')).toBe('{"keep":true}\n')
    } finally {
      await rm(cwd, { recursive: true, force: true })
    }
  })
})

function buffer(): Pick<NodeJS.WriteStream, 'write'> & { text: () => string } {
  let contents = ''
  return {
    write: (chunk: string | Uint8Array) => {
      contents += String(chunk)
      return true
    },
    text: () => contents,
  }
}
