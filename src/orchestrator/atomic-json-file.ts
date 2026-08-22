import { randomUUID } from 'node:crypto'
import { mkdir, open, rename, rm, stat } from 'node:fs/promises'
import { dirname } from 'node:path'

/**
 * Atomically publish a JSON document to `path` (#323).
 *
 * `fs.writeFile` opens with `w`, which truncates first and then writes. A
 * reader that opens the file inside that window sees a partially-written
 * document, and every reader of the daemon's state files collapses a parse
 * failure into `undefined` — so a torn read is indistinguishable from *the
 * file does not exist*. The crash reaper then sees a healthy daemon as dead,
 * and `/healthz`, which the deployed container serves straight out of the loop
 * heartbeat, blips with nothing logged to explain it.
 *
 * A tight reader/writer instrument put this at ~28-30% of reads, but that
 * figure describes the instrument — it reads in a loop with no delay. The real
 * rate for any consumer depends on its read cadence against the daemon's write
 * cadence, which is not measured. The mechanism is the point; the number is
 * not transferable to `/healthz` or to anything else.
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
 * Permissions are carried across explicitly. `writeFile` truncated the
 * existing inode and so kept whatever mode it already had; `rename` publishes
 * a *new* inode, which would otherwise take `0o666 & ~umask`. That difference
 * is not academic: these files are read by out-of-process consumers
 * (`/healthz`, the crash reaper, `factory fleet loop-status`), so a daemon
 * restarted under a tighter umask could silently lock a differently-UID reader
 * out of every subsequent update, and a deliberately restricted file could be
 * widened. Carrying the destination's mode keeps this change to atomicity
 * alone. No mode is passed for a first publication, deliberately: that
 * reproduces what `writeFile` did when creating the file, rather than the
 * tighter 0o600 used for the secret-bearing outbox.
 *
 * Not preserved: ACLs and extended attributes, which Node cannot portably
 * copy. A consumer relying on those rather than on the mode bits needs the
 * destination provisioned by something other than this writer.
 */
export async function writeJsonFileAtomically(path: string, document: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  // Same directory as the target, so the rename below stays within one
  // filesystem. pid + uuid keeps two writers (and two processes) from
  // colliding on the temp name.
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`
  // Read before writing: the destination's mode is what the previous writer
  // published under, and it is what out-of-process readers were granted.
  // Absent means first publication, which is the only case where the umask
  // default is the correct answer. Any other stat failure is not ours to
  // swallow — proceeding would publish under the wrong permissions.
  let destinationMode: number | undefined
  try {
    destinationMode = (await stat(path)).mode & 0o7777
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
  }
  try {
    const handle = await open(temporaryPath, 'wx')
    try {
      await handle.writeFile(`${JSON.stringify(document, null, 2)}\n`)
      await handle.sync()
      if (destinationMode !== undefined) {
        // chmod the handle, not the path: the temp name is ours alone, but
        // fchmod cannot be raced by anything that recreates it.
        await handle.chmod(destinationMode)
      }
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
