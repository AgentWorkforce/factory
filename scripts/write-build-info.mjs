#!/usr/bin/env node

/**
 * Stamp the commit this build was produced from into `dist/build-info.json` (#446).
 *
 * Factory runs in a container that installs `@agent-relay/factory@<version>`
 * from npm at IMAGE BUILD time, and the container rolls roughly hourly, so
 * "which build is up" changes without anyone deploying. Until now the only
 * thing an operator could read back from outside the container was
 * `heartbeat.startedAt`, which dates the BOOT, not the CODE — so "is the fix I
 * merged actually running?" was answered by comparing a boot time against a
 * merge time and inferring a deploy happened in between.
 *
 * The published package already carries its version in `package.json`. The
 * commit is the half that has never existed in the artifact, so it is written
 * here, at build time, from the checkout being built.
 *
 * Deliberately NOT written here: the version. `npm version` runs AFTER `npm run
 * build` in `.github/workflows/publish.yml`, so a version captured at build
 * time is the pre-bump one. `package.json` is in the package `files` list and
 * is rewritten by that bump, which makes it the only field that cannot go
 * stale — the runtime loader reads the version from there and the commit from
 * here, and neither is copied into the other.
 *
 * Fails loudly with no SHA rather than emitting a placeholder. A build stamp
 * that says `unknown` where a commit belongs is indistinguishable from a real
 * answer at the point it is read back during an outage; a build that stops is
 * not. Every context that runs `npm run build` — CI, the publish workflow, the
 * release-recovery worktree rebuild, a developer checkout — has a resolvable
 * HEAD, and a consumer installing the published package never runs the build at
 * all (there is no `prepare` script; `dist/` ships prebuilt).
 *
 * A DIRTY working tree still stamps HEAD, which is knowingly approximate for a
 * developer build and exact for every build that can reach an operator: CI and
 * the publish workflow build a fresh checkout, and the release already binds
 * the packed payload to the commit under test twice over
 * (`scripts/verify-packed-e2e.mjs`, `scripts/verify-release-payload.sh`).
 * Refusing to build on a dirty tree would break the ordinary edit-build loop to
 * guard an artifact that is never published.
 */

import { execFileSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

export const BUILD_INFO_SCHEMA_VERSION = 1
export const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/u

/**
 * The checkout wins; `GITHUB_SHA` is only the fallback.
 *
 * This ordering is load-bearing, not stylistic. On a `pull_request` event
 * `GITHUB_SHA` is GitHub's SYNTHETIC MERGE COMMIT — a commit that exists only
 * inside that run and is not the code being reviewed. `ci.yml` already checks
 * out `pull_request.head.sha` precisely to avoid it. Stamping `GITHUB_SHA`
 * would therefore label every PR build with a SHA an operator cannot find in
 * the repository, which is the failure mode this whole field exists to remove.
 *
 * `git rev-parse HEAD` is the commit that produced the `dist/` being stamped,
 * in every context that runs the build: CI, the publish workflow, the
 * release-recovery worktree rebuild, and a developer checkout. `GITHUB_SHA`
 * remains as a fallback for a hypothetical builder with no `git` on PATH — it
 * is still an answer bound to the run, where a placeholder is bound to nothing.
 */
export function resolveBuildCommit({ env = process.env, cwd = process.cwd() } = {}) {
  let fromGit
  try {
    fromGit = execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim()
  } catch {
    fromGit = undefined
  }
  if (fromGit && FULL_COMMIT_SHA.test(fromGit)) return fromGit
  const fromEnv = env.GITHUB_SHA?.trim()
  if (fromEnv && FULL_COMMIT_SHA.test(fromEnv)) return fromEnv
  throw new Error(
    'cannot stamp the build: no full commit SHA from `git rev-parse HEAD` or GITHUB_SHA. ' +
      'Refusing to write a placeholder — a build stamp that lies about its commit is worse ' +
      'than no build at all (#446).',
  )
}

export function writeBuildInfo({ outPath, env = process.env, cwd = process.cwd() } = {}) {
  const commit = resolveBuildCommit({ env, cwd })
  const document = { schemaVersion: BUILD_INFO_SCHEMA_VERSION, commit }
  mkdirSync(dirname(outPath), { recursive: true })
  writeFileSync(outPath, `${JSON.stringify(document, null, 2)}\n`)
  return document
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const root = resolve(import.meta.dirname, '..')
  const outPath = process.argv[2]
    ? resolve(process.argv[2])
    : join(root, 'dist', 'build-info.json')
  const { commit } = writeBuildInfo({ outPath, cwd: root })
  console.log(`ok: stamped ${outPath} with commit ${commit}`)
}
