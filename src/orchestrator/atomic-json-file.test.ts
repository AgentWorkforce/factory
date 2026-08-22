import { describe, expect, it } from 'vitest'
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { writeJsonFileAtomically } from './atomic-json-file'
import type { FactoryLoopHeartbeat } from '../types'

/**
 * Concurrent reader/writer instrument for #323.
 *
 * This is the measurement that found the bug, bounded so it cannot become a
 * slow flake: a writer publishes `writes` documents back to back while a
 * reader reads the same path in a tight loop and parses every read. A torn
 * read is a read that lands between the truncate and the last byte, so it
 * fails to parse — which is exactly how the production readers see it, since
 * `readFactoryLoopHeartbeat` and `readFactoryInFlightRegistry` both collapse a
 * parse failure into `undefined`.
 */
async function measureTornReads(
  path: string,
  document: (iteration: number) => unknown,
  write: (path: string, document: unknown) => Promise<void>,
  opts: { writes: number; stopAtFirstTear?: boolean },
): Promise<{ reads: number; torn: number; invalid: number }> {
  await write(path, document(0))

  let reads = 0
  let torn = 0
  let invalid = 0
  let done = false

  const writer = (async () => {
    for (let iteration = 1; iteration <= opts.writes; iteration++) {
      if (done) break
      await write(path, document(iteration))
    }
    done = true
  })()

  const reader = (async () => {
    while (!done) {
      reads++
      let parsed: unknown
      try {
        parsed = JSON.parse(await readFile(path, 'utf8'))
      } catch {
        // ENOENT counts too: a reader that finds no file at all is the same
        // "daemon looks dead" outcome, and an atomic publish must never
        // expose that gap either.
        torn++
        if (opts.stopAtFirstTear) done = true
        continue
      }
      // A document that parses but is missing the fields the consumers read is
      // just as broken as one that does not parse.
      const record = parsed as Partial<FactoryLoopHeartbeat>
      if (typeof record?.pid !== 'number' || typeof record?.updatedAtMs !== 'number' || !record?.status) {
        invalid++
      }
    }
  })()

  await Promise.all([writer, reader])
  return { reads, torn, invalid }
}

/**
 * Sized like the real thing. The live heartbeat carries the `/healthz` public
 * health projection, the event-listener, readiness-reconcile, dispatch-capacity
 * and fleet-control-plane blocks — a few KB of pretty-printed JSON, which is
 * what makes the truncate-then-write window so easy to hit.
 */
const heartbeatDocument = (iteration: number): FactoryLoopHeartbeat => ({
  pid: process.pid,
  status: 'running',
  iteration,
  maxIterations: 1_000,
  updatedAt: new Date(1_700_000_000_000 + iteration).toISOString(),
  updatedAtMs: 1_700_000_000_000 + iteration,
  registryPath: '/tmp/factory-run/factory-in-flight.json',
  eventListener: {
    state: 'listening',
    reason: 'live daemon heartbeat is active',
    subscriptions: Array.from({ length: 24 }, (_, index) => `subscription-${index}-${'x'.repeat(48)}`),
  },
} as unknown as FactoryLoopHeartbeat)

describe('writeJsonFileAtomically (#323)', () => {
  it('publishes the identical bytes the non-atomic write published', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-atomic-shape-'))
    try {
      const document = heartbeatDocument(7)
      const atomicPath = join(root, 'atomic.json')
      const legacyPath = join(root, 'legacy.json')

      await writeJsonFileAtomically(atomicPath, document)
      // Byte-for-byte what `#writeLoopHeartbeat` wrote before #323.
      await writeFile(legacyPath, `${JSON.stringify(document, null, 2)}\n`, 'utf8')

      expect(await readFile(atomicPath, 'utf8')).toBe(await readFile(legacyPath, 'utf8'))
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('leaves no temp file behind, and keeps the temp file in the target directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-atomic-temp-'))
    try {
      const path = join(root, 'nested', 'factory-loop-heartbeat.json')
      await writeJsonFileAtomically(path, heartbeatDocument(1))
      await writeJsonFileAtomically(path, heartbeatDocument(2))

      // Same-directory temp is what makes the rename atomic: a cross-filesystem
      // rename is not. Anything left in the target directory other than the
      // target itself means the temp escaped or survived.
      expect(await readdir(join(root, 'nested'))).toEqual(['factory-loop-heartbeat.json'])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('the instrument detects torn reads against the non-atomic write it replaced', async () => {
    // Sensitivity control. Without this, a green result above would not
    // distinguish "the fix works" from "the instrument cannot see tearing on
    // this platform". Stops at the first tear, so the common case is a handful
    // of writes and the bound is only reached if nothing tears at all.
    const root = await mkdtemp(join(tmpdir(), 'factory-atomic-control-'))
    try {
      const path = join(root, 'factory-loop-heartbeat.json')
      const result = await measureTornReads(
        path,
        heartbeatDocument,
        async (target, document) => {
          await writeFile(target, `${JSON.stringify(document, null, 2)}\n`, 'utf8')
        },
        { writes: 2_000, stopAtFirstTear: true },
      )

      expect(result.torn).toBeGreaterThan(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('produces zero torn reads under the same concurrent load', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-atomic-'))
    try {
      const path = join(root, 'factory-loop-heartbeat.json')
      const result = await measureTornReads(path, heartbeatDocument, writeJsonFileAtomically, { writes: 150 })

      // The reader must have actually raced the writer, or zero torn reads
      // would prove nothing.
      expect(result.reads).toBeGreaterThan(50)
      expect(result.torn).toBe(0)
      expect(result.invalid).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
