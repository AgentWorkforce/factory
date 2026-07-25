import { appendFile, readFile } from 'node:fs/promises'

import { BenchmarkResultSchema, type BenchmarkResult } from './schema'

/** Reads every recorded result, tolerating a not-yet-created results file (a fresh benchmark run). */
export async function loadResults(path: string): Promise<BenchmarkResult[]> {
  let raw: string
  try {
    raw = await readFile(path, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw error
  }

  return raw
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => BenchmarkResultSchema.parse(JSON.parse(line)))
}

/**
 * Appends one result as a JSONL line. Append-only by design: the runner is
 * meant to be safe to kill and resume, and a JSONL file survives a crash
 * mid-write far better than a single JSON array file would.
 */
export async function appendResult(path: string, result: BenchmarkResult): Promise<void> {
  BenchmarkResultSchema.parse(result) // fail fast on a malformed result instead of writing bad data
  await appendFile(path, `${JSON.stringify(result)}\n`)
}
