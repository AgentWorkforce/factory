import { randomUUID } from 'node:crypto'
import { link, mkdir, open, readFile, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu

/**
 * Return one opaque identity for this Factory installation. The create-only
 * file makes concurrent starts converge without deriving identity from a
 * hostname, username, local path, or other machine fingerprint.
 */
export async function loadOrCreateFactoryInstanceId(path: string): Promise<string> {
  try {
    const existing = (await readFile(path, 'utf8')).trim()
    if (!UUID.test(existing)) throw new Error('Factory instance identity file is invalid')
    return existing
  } catch (error) {
    if (!isMissingFileError(error)) throw error
  }

  await mkdir(dirname(path), { recursive: true, mode: 0o700 })
  const candidatePath = `${path}.${randomUUID()}.tmp`
  let candidateExists = false
  try {
    const handle = await open(candidatePath, 'wx', 0o600)
    candidateExists = true
    const id = randomUUID()
    try {
      await handle.writeFile(`${id}\n`, 'utf8')
      await handle.sync()
    } finally {
      await handle.close()
    }
    try {
      await link(candidatePath, path)
      return id
    } catch (error) {
      if (!isAlreadyExistsError(error)) throw error
    }
  } catch (error) {
    if (!isAlreadyExistsError(error)) throw error
  } finally {
    if (candidateExists) await unlink(candidatePath).catch(() => undefined)
  }

  const id = (await readFile(path, 'utf8')).trim()
  if (!UUID.test(id)) throw new Error('Factory instance identity file is invalid')
  return id
}

const isAlreadyExistsError = (error: unknown): boolean =>
  Boolean(error && typeof error === 'object' && 'code' in error && error.code === 'EEXIST')

const isMissingFileError = (error: unknown): boolean =>
  Boolean(error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT')
