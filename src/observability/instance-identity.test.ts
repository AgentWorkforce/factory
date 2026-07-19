import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { loadOrCreateFactoryInstanceId } from './instance-identity'

describe('loadOrCreateFactoryInstanceId', () => {
  it('persists one opaque identity across starts and concurrent creators', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-instance-id-'))
    try {
      const path = join(root, 'state', 'instance-id')
      const ids = await Promise.all(Array.from({ length: 4 }, () => loadOrCreateFactoryInstanceId(path)))

      expect(new Set(ids).size).toBe(1)
      expect((await readFile(path, 'utf8')).trim()).toBe(ids[0])
      expect(ids[0]).toMatch(/^[0-9a-f-]{36}$/u)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects a corrupt identity instead of deriving a machine fingerprint', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-instance-id-corrupt-'))
    try {
      const path = join(root, 'instance-id')
      await writeFile(path, 'hostname-user-path\n')

      await expect(loadOrCreateFactoryInstanceId(path)).rejects.toThrow('identity file is invalid')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reads a valid existing identity without requiring directory writes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-instance-id-fast-path-'))
    const path = join(root, 'instance-id')
    const id = '123e4567-e89b-42d3-a456-426614174000'
    try {
      await writeFile(path, `${id}\n`)
      await chmod(root, 0o500)

      await expect(loadOrCreateFactoryInstanceId(path)).resolves.toBe(id)
    } finally {
      await chmod(root, 0o700).catch(() => undefined)
      await rm(root, { recursive: true, force: true })
    }
  })
})
