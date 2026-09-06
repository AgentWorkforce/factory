#!/usr/bin/env node

import { lstat, readdir, readFile, readlink } from 'node:fs/promises'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

/**
 * The one file in the payload that is EXPECTED to differ between two builds of
 * the same code (#446).
 *
 * `dist/build-info.json` records which commit produced the artifact. That is
 * the point of it — it is what lets `/healthz` answer "is the fix I merged
 * actually running?" without an argument from boot times.
 *
 * It also breaks the release-recovery path in `.github/workflows/publish.yml`
 * if compared byte-for-byte. That path exists because the 0.1.58 incident left
 * a release tag on an EQUIVALENT ORPHANED COMMIT: it rebuilds the tagged commit
 * and proves its payload matches what npm holds. A stamped commit makes two
 * distinct commits differ by construction, so that proof could never succeed
 * again — the recovery would report a payload mismatch that is really a
 * provenance difference it was designed to tolerate.
 *
 * So this file is compared as a build stamp rather than as bytes: every field
 * except `commit` must match exactly, both sides must parse, and a differing
 * commit is reported as a NOTE naming both SHAs.
 *
 * The note states a fact about the STAMP and never a verdict about the payload
 * (#468 review, P2, codex). This function has seen one file; whether the rest
 * of the tree matches is decided by a traversal that has not finished yet, so
 * wording like "same code" would print immediately before `package.json:
 * content differs` and certify an equivalence the comparison went on to
 * refute. The verdict belongs to the caller — the empty/non-empty
 * `differences` list, and the process exit code. Notes are still printed when
 * the comparison FAILS, because a release-recovery failure is exactly when a
 * reader needs to know which two builds were being compared.
 */
const BUILD_STAMP_PATH = 'dist/build-info.json'
const BUILD_STAMP_PROVENANCE_FIELD = 'commit'
/**
 * The exemption is granted to a COMMIT, not to a string in the commit slot.
 *
 * Kept identical to `scripts/write-build-info.mjs` and
 * `src/orchestrator/build-identity.ts`: the generator emits nothing but a full
 * 40-hex SHA, so anything else in a packed payload is corruption, not
 * provenance. Accepting it would let the release-recovery and
 * final-consistency steps in `publish.yml` certify an artifact whose runtime
 * loader will report `commit: "unknown"` — the silent lie this whole change
 * exists to remove, re-entering through the check that is supposed to catch it.
 */
const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/u

export async function comparePackageTrees(leftRoot, rightRoot, { notes } = {}) {
  const differences = []
  await compareEntry(leftRoot, rightRoot, '.', differences, notes)
  return differences
}

/**
 * @returns `true` when THIS FILE's difference is provenance and may be
 * tolerated, `false` when it is a payload mismatch. It says nothing about the
 * rest of the tree — that verdict is the traversal's.
 */
function compareBuildStamps(leftBytes, rightBytes, relativePath, differences, notes) {
  let left
  let right
  try {
    left = JSON.parse(leftBytes.toString('utf8'))
    right = JSON.parse(rightBytes.toString('utf8'))
  } catch {
    // An unparseable stamp gets no exemption: it is not a build stamp.
    differences.push(`${relativePath}: content differs (unparseable build stamp)`)
    return false
  }
  const plain = (value) => typeof value === 'object' && value !== null && !Array.isArray(value)
  if (!plain(left) || !plain(right)) {
    differences.push(`${relativePath}: content differs (build stamp is not an object)`)
    return false
  }
  const keys = [...new Set([...Object.keys(left), ...Object.keys(right)])].sort()
  for (const key of keys) {
    if (key === BUILD_STAMP_PROVENANCE_FIELD) continue
    if (JSON.stringify(left[key]) !== JSON.stringify(right[key])) {
      differences.push(`${relativePath}: build stamp field ${key} differs`)
      return false
    }
  }
  const leftCommit = left[BUILD_STAMP_PROVENANCE_FIELD]
  const rightCommit = right[BUILD_STAMP_PROVENANCE_FIELD]
  for (const [side, commit] of [['left', leftCommit], ['right', rightCommit]]) {
    if (typeof commit !== 'string' || !FULL_COMMIT_SHA.test(commit)) {
      differences.push(
        `${relativePath}: ${side} build stamp carries no full commit SHA (${JSON.stringify(commit)})`,
      )
      return false
    }
  }
  if (leftCommit !== rightCommit) {
    notes?.push(
      `${relativePath}: build commit differs (${leftCommit} vs ${rightCommit}); ` +
        'exempt from byte comparison',
    )
  }
  return true
}

async function compareEntry(leftPath, rightPath, relativePath, differences, notes) {
  const [left, right] = await Promise.all([
    lstat(leftPath).catch(() => undefined),
    lstat(rightPath).catch(() => undefined),
  ])
  if (!left || !right) {
    differences.push(`${relativePath}: missing from ${left ? 'right' : 'left'} tree`)
    return
  }

  const leftType = fileType(left)
  const rightType = fileType(right)
  if (leftType !== rightType) {
    differences.push(`${relativePath}: type ${leftType} != ${rightType}`)
    return
  }

  // npm tarballs preserve the special permission bits as well as rwx bits.
  // Include setuid, setgid, and sticky so recovery cannot accept a payload
  // whose effective package metadata differs from the registry artifact.
  const leftMode = left.mode & 0o7777
  const rightMode = right.mode & 0o7777
  if (leftMode !== rightMode) {
    differences.push(
      `${relativePath}: mode ${leftMode.toString(8)} != ${rightMode.toString(8)}`,
    )
  }

  if (left.isDirectory()) {
    const [leftNames, rightNames] = await Promise.all([readdir(leftPath), readdir(rightPath)])
    const names = [...new Set([...leftNames, ...rightNames])].sort()
    for (const name of names) {
      await compareEntry(
        join(leftPath, name),
        join(rightPath, name),
        relativePath === '.' ? name : `${relativePath}/${name}`,
        differences,
        notes,
      )
    }
  } else if (left.isSymbolicLink()) {
    const [leftTarget, rightTarget] = await Promise.all([readlink(leftPath), readlink(rightPath)])
    if (leftTarget !== rightTarget) {
      differences.push(`${relativePath}: symlink ${leftTarget} != ${rightTarget}`)
    }
  } else if (left.isFile()) {
    const [leftBytes, rightBytes] = await Promise.all([readFile(leftPath), readFile(rightPath)])
    if (relativePath === BUILD_STAMP_PATH) {
      // Checked even when the two are byte-identical (#468 review, P1, cubic).
      // A short-circuit on equal bytes would exempt a stamp that is malformed
      // on BOTH sides, so release recovery could still certify an artifact
      // whose runtime loader reports `commit: "unknown"`. Equality is not
      // validity: this file gets validated, not merely diffed.
      compareBuildStamps(leftBytes, rightBytes, relativePath, differences, notes)
      return
    }
    if (leftBytes.equals(rightBytes)) return
    differences.push(`${relativePath}: content differs`)
  }
}

function fileType(stat) {
  if (stat.isDirectory()) return 'directory'
  if (stat.isFile()) return 'file'
  if (stat.isSymbolicLink()) return 'symlink'
  return 'other'
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [, , leftRoot, rightRoot] = process.argv
  if (!leftRoot || !rightRoot) {
    console.error('usage: compare-package-trees.mjs LEFT_TREE RIGHT_TREE')
    process.exitCode = 2
  } else {
    const notes = []
    const differences = await comparePackageTrees(leftRoot, rightRoot, { notes })
    // Printed whether or not the comparison passes: when it passes, this is the
    // provenance the caller came for; when it fails, it says which two builds
    // failed to match.
    for (const note of notes) console.error(`note: ${note}`)
    if (differences.length > 0) {
      for (const difference of differences) console.error(difference)
      process.exitCode = 1
    }
  }
}
