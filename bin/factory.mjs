#!/usr/bin/env node
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
const here = dirname(fileURLToPath(import.meta.url))
const entry = join(here, '..', 'dist', 'cli', 'fleet.js')
const mod = await import(pathToFileURL(entry).href)
await mod.main(process.argv.slice(2))
