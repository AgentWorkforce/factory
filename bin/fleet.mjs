#!/usr/bin/env node
import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, readFileSync } from 'node:fs'
import { dirname, isAbsolute, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const here = dirname(fileURLToPath(import.meta.url))
const packageRoot = join(here, '..')
const repoRoot = join(packageRoot, '..', '..')
const entry = join(packageRoot, 'src', 'cli', 'fleet.ts')
const launcher = fileURLToPath(import.meta.url)
const cacheDir = join(repoRoot, 'node_modules', '.cache', 'pear-factory-sdk')
const rawArgs = process.argv.slice(2)
const forceRebuild = process.env.FLEET_FORCE_BUILD === '1' || rawArgs.includes('--rebuild')
const cliArgs = rawArgs.filter((arg) => arg !== '--rebuild')

mkdirSync(cacheDir, { recursive: true })

// The SDK package is type=commonjs, so Node strip-types loads .ts as CJS and rejects ESM imports.
// Bundle the thin CLI entry locally instead; esbuild is declared in this package for that launcher path.
const { buildSync } = require('esbuild')
const buildOptions = {
  entryPoints: [entry],
  bundle: true,
  packages: 'external',
  platform: 'node',
  format: 'esm',
  banner: { js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);" },
  absWorkingDir: repoRoot,
  logLevel: 'silent',
}
const metafileBuild = buildSync({
  ...buildOptions,
  write: false,
  metafile: true,
})
const hash = createHash('sha256')
hash.update(launcher)
hash.update(readFileSync(launcher))
for (const input of Object.keys(metafileBuild.metafile.inputs).sort()) {
  const inputPath = isAbsolute(input) ? input : join(repoRoot, input)
  hash.update(input)
  hash.update(readFileSync(inputPath))
}
const outfile = join(cacheDir, `fleet-${hash.digest('hex').slice(0, 16)}.mjs`)

if (forceRebuild || !existsSync(outfile)) {
  buildSync({
    ...buildOptions,
    outfile,
  })
}

const mod = await import(pathToFileURL(outfile).href)
await mod.main(cliArgs)
