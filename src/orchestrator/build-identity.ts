import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { FactoryPublicBuildIdentity } from '../types'

/**
 * Which build is running (#446).
 *
 * A deployed Factory is a container that installed `@agent-relay/factory@<v>`
 * from npm when its IMAGE was built, and the container rolls roughly hourly, so
 * the running build changes without anyone deploying. Before this, the only
 * thing `/healthz` published about identity was `heartbeat.startedAt` — the
 * time of the BOOT, not of the CODE. "Is the fix I merged actually running?"
 * was therefore answered by argument: compare `startedAt` against a merge time
 * and infer a deploy must have happened in between. That inference is sound
 * only if a deploy did happen, which the endpoint also could not say.
 *
 * Two facts answer it directly, and they come from two different files on
 * purpose:
 *
 * - `version` — the running package's own `package.json`. `package.json` ships
 *   inside the tarball (it is in the `files` list) and is the file `npm version`
 *   rewrites during a release, so it is the only place the published version
 *   cannot go stale.
 * - `commit` — `dist/build-info.json`, written by `scripts/write-build-info.mjs`
 *   as part of `npm run build`. The build runs BEFORE `npm version` in the
 *   publish workflow, which is exactly why the version is not copied into it.
 *
 * ## Nothing here is synthesised
 *
 * Both fields report the literal string `unknown` when the artifact does not
 * carry them, and never a plausible substitute. An absent build stamp is not an
 * empty runtime value: a package published before this change carries no
 * `dist/build-info.json` at all, and running from a source checkout carries no
 * `dist/` at all. In both cases the honest answer is that this build cannot say
 * which commit it is — and an operator reading `commit: "unknown"` during an
 * outage learns something true, where a synthesised SHA (this process's HEAD,
 * the last release tag, an empty string) would send them to the wrong diff.
 *
 * The BUILD refuses to produce a stamp it cannot fill (see
 * `scripts/write-build-info.mjs`), so `unknown` reaching a released artifact
 * means "older than #446", not "the stamp failed silently".
 *
 * A malformed or hostile stamp is treated as absent rather than passed through.
 * This value is published on the unauthenticated `/healthz` surface, so it
 * obeys the same rule as everything else that crosses that boundary
 * (`public-health.ts`): closed shapes, bounded lengths, nothing spread.
 */

/** What both fields report when the running artifact does not carry them. */
export const UNKNOWN_BUILD_FIELD = 'unknown'

/** A full Git object ID. An abbreviated SHA is rejected: it is not what was stamped. */
export const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/u

/**
 * npm's own version grammar is wider than semver (it permits build metadata and
 * prerelease tags), so this bounds the CHARACTER SET and the LENGTH rather than
 * trying to re-implement the grammar. It exists because the value is served
 * unauthenticated: a `package.json` this process did not write must not be able
 * to put control characters or a kilobyte of text on `/healthz`.
 */
export const PUBLISHABLE_VERSION = /^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$/u

/** Where this module sits inside the package, in both `src/` and `dist/`. */
const moduleDir = dirname(fileURLToPath(import.meta.url))
/** `dist/orchestrator/build-identity.js` -> `dist/`; `src/orchestrator/...` -> `src/`. */
const buildRoot = join(moduleDir, '..')
/** ...and one level further is the package root in both layouts. */
const packageRoot = join(buildRoot, '..')

const readJson = (path: string): unknown => {
  try {
    return JSON.parse(readFileSync(path, 'utf8')) as unknown
  } catch {
    return undefined
  }
}

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined

/** The published version of the running package, or `unknown`. */
export function readPackageVersion(root = packageRoot): string {
  const parsed = record(readJson(join(root, 'package.json')))
  const version = parsed?.version
  return typeof version === 'string' && PUBLISHABLE_VERSION.test(version)
    ? version
    : UNKNOWN_BUILD_FIELD
}

/**
 * The commit this build was produced from, or `unknown`.
 *
 * Note what is NOT here: no fallback to `git rev-parse`. The daemon runs in a
 * container that has no checkout, and on a developer machine the checkout's
 * HEAD is not the commit the running `dist/` was built from — it is whatever
 * the developer has since checked out. Reading it would produce the most
 * convincing possible wrong answer.
 */
export function readBuildCommit(root = buildRoot): string {
  const parsed = record(readJson(join(root, 'build-info.json')))
  const commit = parsed?.commit
  return typeof commit === 'string' && FULL_COMMIT_SHA.test(commit)
    ? commit
    : UNKNOWN_BUILD_FIELD
}

/**
 * Read the running build's identity, uncached.
 *
 * `readBuildIdentity` is what callers want; this exists so tests can point the
 * loader at a fixture package root without defeating the cache.
 */
export function loadBuildIdentity(
  roots: { packageRoot?: string; buildRoot?: string } = {},
): FactoryPublicBuildIdentity {
  return {
    version: readPackageVersion(roots.packageRoot ?? packageRoot),
    commit: readBuildCommit(roots.buildRoot ?? buildRoot),
  }
}

let cached: FactoryPublicBuildIdentity | undefined

/**
 * The running build's identity, read once per process.
 *
 * Cached because it is stamped into every heartbeat write and cannot change
 * while the process lives — the files it reads are baked into the image. It
 * also must never throw: this sits on the heartbeat write path, which is what
 * the crash reaper and `/healthz` read to decide the daemon is alive, so a
 * failure to identify the build must never be able to make a live daemon look
 * dead. Every read here is already total, returning `unknown` instead of
 * raising; the try/catch is the belt to that braces.
 */
export function readBuildIdentity(): FactoryPublicBuildIdentity {
  if (cached) return cached
  try {
    cached = loadBuildIdentity()
  } catch {
    cached = { version: UNKNOWN_BUILD_FIELD, commit: UNKNOWN_BUILD_FIELD }
  }
  return cached
}
