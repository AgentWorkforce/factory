import { readdir, readFile } from 'node:fs/promises'
import { relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const SDK_ROOT = resolve(fileURLToPath(new URL('..', import.meta.url)))
const ALLOWED_WRITEBACK_CALLSITES = new Set([
  'writeback/linear.ts',
  'writeback/slack.ts',
])

describe('mount.writeFile callsite invariant', () => {
  it('keeps draft-creating writes behind guarded writeback modules', async () => {
    const offenders: string[] = []
    for (const file of await sourceFiles(SDK_ROOT)) {
      const rel = relative(SDK_ROOT, file).split('\\').join('/')
      if (rel.endsWith('.test.ts') || rel.startsWith('__tests__/')) continue
      const source = await readFile(file, 'utf8')
      if (!source.includes('mount.writeFile(')) continue
      if (!ALLOWED_WRITEBACK_CALLSITES.has(rel)) offenders.push(rel)
    }

    expect(offenders).toEqual([])
  })
})

async function sourceFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true })
  const files: string[] = []
  for (const entry of entries) {
    const path = resolve(dir, entry.name)
    if (entry.isDirectory()) {
      files.push(...await sourceFiles(path))
    } else if (entry.isFile() && path.endsWith('.ts')) {
      files.push(path)
    }
  }
  return files
}
