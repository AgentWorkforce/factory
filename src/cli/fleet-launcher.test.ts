import { readFile } from 'node:fs/promises'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

const launcherPath = join(import.meta.dirname, '..', '..', 'bin', 'fleet.mjs')

describe('fleet.mjs launcher', () => {
  it('externalizes package dependencies and keys the cache on launcher changes', async () => {
    const source = await readFile(launcherPath, 'utf8')

    expect(source).toContain("packages: 'external'")
    expect(source).toContain("join(repoRoot, 'node_modules', '.cache', 'pear-factory-sdk')")
    expect(source).toContain('const launcher = fileURLToPath(import.meta.url)')
    expect(source).toContain('statSync(launcher).mtimeMs')
  })
})
