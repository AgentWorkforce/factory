import { readdir, readFile } from 'node:fs/promises'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const sourceRoot = join(dirname(fileURLToPath(import.meta.url)), '..')
const importSpecifierPattern =
  /(?:import|export)\s+(?:type\s+)?(?:[^'"]*?\s+from\s+)?['"]([^'"]+)['"]|import\s*\(\s*['"]([^'"]+)['"]\s*\)/g

async function walk(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true })
  const files = await Promise.all(entries.map(async (entry) => {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) {
      return walk(path)
    }
    return path.endsWith('.ts') || path.endsWith('.tsx') ? [path] : []
  }))

  return files.flat()
}

describe('factory-sdk imports', () => {
  it('does not import electron or src/main modules', async () => {
    const violations: string[] = []

    for (const file of await walk(sourceRoot)) {
      const source = await readFile(file, 'utf8')
      for (const match of source.matchAll(importSpecifierPattern)) {
        const specifier = match[1] ?? match[2] ?? ''
        if (specifier.includes('electron') || specifier.includes('src/main')) {
          violations.push(`${relative(sourceRoot, file)} imports ${specifier}`)
        }
      }
    }

    expect(violations).toEqual([])
  })
})
