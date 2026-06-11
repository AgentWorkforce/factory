#!/usr/bin/env node
import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const here = dirname(fileURLToPath(import.meta.url))
const packageRoot = join(here, '..')
const repoRoot = join(packageRoot, '..', '..')
const entry = join(packageRoot, 'src', 'cli', 'fleet.ts')
const cacheDir = join(tmpdir(), 'pear-factory-sdk')
const hash = createHash('sha256')
  .update(entry)
  .update(String(statSync(entry).mtimeMs))
  .digest('hex')
  .slice(0, 16)
const outfile = join(cacheDir, `fleet-${hash}.mjs`)

mkdirSync(cacheDir, { recursive: true })

if (!existsSync(outfile)) {
  // The SDK package is type=commonjs, so Node strip-types loads .ts as CJS and rejects ESM imports.
  // Bundle the thin CLI entry locally instead; esbuild is declared in this package for that launcher path.
  const { buildSync } = require('esbuild')
  buildSync({
    entryPoints: [entry],
    outfile,
    bundle: true,
    platform: 'node',
    format: 'esm',
    banner: { js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);" },
    absWorkingDir: repoRoot,
    logLevel: 'silent',
  })
}

const mod = await import(pathToFileURL(outfile).href)
await mod.main(process.argv.slice(2))
