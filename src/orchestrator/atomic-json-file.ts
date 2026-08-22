import { randomUUID } from 'node:crypto'
import { mkdir, open, rename, rm } from 'node:fs/promises'
import { dirname } from 'node:path'

/**
 * Atomically publish a JSON document to `path` (#323).
 *
 * `fs.writeFile` opens with `w`, which truncates first and then writes. A
 * reader that opens the file inside that window sees a partially-written
 * document, and every reader of the daemon's state files collapses a parse
 * failure into `undefined` — so a torn read is indistinguishable from *the
 * file does not exist*. Measured at ~28-30% of concurrent reads. The crash
 * reaper then sees a healthy daemon as dead, and `/healthz`, which the
 * deployed container serves straight out of the loop heartbeat, blips with
 * nothing logged to explain it.
 *
 * Writing a temp file and renaming over the target gives the write an atomic
 * publication point: a reader observes either the whole previous document or
 * the whole new one, never a splice of the two.
 *
 * The temp file MUST live in the same directory as the target. `rename` is
 * only atomic within a single filesystem, so a temp file in the OS temp dir
 * would silently reintroduce the race wherever the target sits on another
 * mount — exactly the deployed-container case this is meant to fix.
 *
 * No mode is passed, deliberately: these files are read by out-of-process
 * consumers (`/healthz`, the crash reaper, `factory fleet loop-status`), so
 * the published file keeps the permissions `writeFile` gave it rather than
 * the tighter 0o600 used for the secret-bearing outbox.
 */
export async function writeJsonFileAtomically(path: string, document: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  // Same directory as the target, so the rename below stays within one
  // filesystem. pid + uuid keeps two writers (and two processes) from
  // colliding on the temp name.
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`
  try {
    const handle = await open(temporaryPath, 'wx')
    try {
      await handle.writeFile(`${JSON.stringify(document, null, 2)}\n`)
      await handle.sync()
    } finally {
      await handle.close()
    }
    await rename(temporaryPath, path)
  } finally {
    // A successful rename leaves nothing behind; this clears the temp file on
    // any failure path so a crashed write cannot litter the run directory.
    await rm(temporaryPath, { force: true })
  }
}
