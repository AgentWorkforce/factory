import { describe, expect, it } from 'vitest'
import { execFileSync, spawnSync } from 'node:child_process'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import {
  FULL_COMMIT_SHA,
  loadBuildIdentity,
  readBuildCommit,
  readBuildIdentity,
  readPackageVersion,
  UNKNOWN_BUILD_FIELD,
} from './build-identity'
import { publicHealthFromHeartbeat, normalizePublicHealth } from './public-health'

/**
 * #446. `/healthz` could not say which build was answering, so "is the fix I
 * merged actually running?" was settled by comparing `heartbeat.startedAt`
 * against a merge time and inferring a deploy in between.
 *
 * The point of contention in a fix like this is not whether a field appears —
 * it is whether the field is *load-bearing*. A version string that is really a
 * constant compiled into the source, or a commit synthesised from whatever git
 * happens to be reachable at read time, would show up on `/healthz` looking
 * exactly like the real thing and send an operator to the wrong diff. So these
 * tests are about provenance, not presence:
 *
 * - the commit is whatever `scripts/write-build-info.mjs` actually wrote, and
 *   changes when the artifact changes;
 * - the version is whatever the running package's own `package.json` says;
 * - a MISSING stamp reports `unknown` and does not fall back to this
 *   process's HEAD, which is the most convincing wrong answer available;
 * - a malformed stamp is refused rather than republished.
 */

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..')
const generator = join(repoRoot, 'scripts', 'write-build-info.mjs')

const headSha = execFileSync('git', ['rev-parse', 'HEAD'], {
  cwd: repoRoot,
  encoding: 'utf8',
}).trim()

const withTempRoot = async (run: (root: string) => Promise<void>): Promise<void> => {
  const root = await mkdtemp(join(tmpdir(), 'factory-build-identity-'))
  try {
    await run(root)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
}

/** Run the real build stamper the way `npm run build` runs it. */
const stamp = (outPath: string, env: NodeJS.ProcessEnv = {}): string =>
  execFileSync(process.execPath, [generator, outPath], {
    cwd: repoRoot,
    encoding: 'utf8',
    // A clean env, so an inherited GITHUB_SHA from a CI runner cannot decide
    // what this test is measuring.
    env: { PATH: process.env.PATH ?? '', ...env },
  })

describe('build identity comes from the build artifact (#446)', () => {
  it('reports the commit the real generator stamped, not a constant', async () => {
    await withTempRoot(async (root) => {
      stamp(join(root, 'build-info.json'))

      // The generator resolved a commit from the checkout it was pointed at,
      // and the loader read back exactly that. Not a literal in either file:
      // change the checkout and both move together.
      expect(readBuildCommit(root)).toBe(headSha)
      expect(headSha).toMatch(FULL_COMMIT_SHA)
    })
  })

  it('stamps the checkout it built, not GitHub\'s synthetic merge commit', async () => {
    await withTempRoot(async (root) => {
      // On a `pull_request` event `GITHUB_SHA` is a merge commit that exists
      // only inside that run — `ci.yml` checks out `pull_request.head.sha`
      // specifically to avoid it. If the stamp preferred the environment, every
      // PR build would be labelled with a SHA an operator cannot find in the
      // repository: the exact failure this field exists to remove.
      const syntheticMergeCommit = 'f'.repeat(40)
      stamp(join(root, 'build-info.json'), { GITHUB_SHA: syntheticMergeCommit })

      expect(readBuildCommit(root)).toBe(headSha)
      expect(readBuildCommit(root)).not.toBe(syntheticMergeCommit)
    })
  })

  it('refuses to build rather than stamp a placeholder it cannot fill', async () => {
    await withTempRoot(async (root) => {
      // No GITHUB_SHA and no reachable `git`: the generator has nothing true to
      // say. It must fail the BUILD rather than emit a stand-in — a stamp
      // reading `unknown` is indistinguishable from a real answer at the moment
      // it is read back during an outage, and it would be baked into a released
      // image, where nothing downstream could ever tell the two apart.
      const outPath = join(root, 'build-info.json')
      const emptyPath = join(root, 'no-tools')
      await mkdir(emptyPath)
      const run = spawnSync(process.execPath, [generator, outPath], {
        cwd: repoRoot,
        encoding: 'utf8',
        env: { PATH: emptyPath },
      })

      // Non-zero for THIS reason — not because node failed to start on a
      // stripped PATH, which would pass a bare "it threw" assertion while
      // proving nothing about the refusal.
      expect(run.status).not.toBe(0)
      expect(run.stderr).toMatch(/no full commit SHA from `git rev-parse HEAD`/u)
      expect(run.stderr).toMatch(/Refusing to write a placeholder/u)

      // And it wrote nothing: a half-stamped artifact is not a lesser failure.
      await expect(readFile(outPath, 'utf8')).rejects.toThrow()
    })
  })

  it('names the missing input rather than failing opaquely', async () => {
    await withTempRoot(async (root) => {
      const { resolveBuildCommit } = (await import(
        pathToFileURL(generator).href
      )) as { resolveBuildCommit: (opts: { env: NodeJS.ProcessEnv; cwd: string }) => string }

      // With no git repository to read, a blank, whitespace-only or abbreviated
      // GITHUB_SHA is not an answer either — none of these may be stamped as
      // though it were a commit.
      for (const notASha of [undefined, '', '   ', 'HEAD', headSha.slice(0, 12)]) {
        expect(() =>
          resolveBuildCommit({
            env: notASha === undefined ? {} : { GITHUB_SHA: notASha },
            cwd: root,
          }),
        ).toThrow(/commit SHA/u)
      }

      // The environment is the FALLBACK, and it does still answer when there is
      // no checkout to read.
      expect(resolveBuildCommit({ env: { GITHUB_SHA: headSha }, cwd: root })).toBe(headSha)
      // …but never ahead of the checkout: pointed at this repository, the
      // checkout wins over a different SHA in the environment.
      expect(resolveBuildCommit({ env: { GITHUB_SHA: 'f'.repeat(40) }, cwd: repoRoot }))
        .toBe(headSha)
    })
  })

  it('reports `unknown` — never this process\'s HEAD — when the artifact carries no stamp', async () => {
    await withTempRoot(async (root) => {
      // A package published before #446 has no `dist/build-info.json`, and a
      // source checkout has no `dist/` at all. Both must say so.
      expect(readBuildCommit(root)).toBe(UNKNOWN_BUILD_FIELD)
      // The failure this guards: `git rev-parse HEAD` at READ time is the
      // developer's current checkout, not the commit `dist/` was built from.
      expect(readBuildCommit(root)).not.toBe(headSha)
    })
  })

  it('refuses a malformed or hostile stamp instead of republishing it', async () => {
    await withTempRoot(async (root) => {
      const path = join(root, 'build-info.json')
      // `/healthz` is unauthenticated. Everything crossing it is a closed shape.
      for (const junk of [
        '{"schemaVersion":1,"commit":"HEAD"}',
        // An abbreviated SHA is not what was stamped: a reader who pastes it
        // into `git show` and gets a hit has learned nothing about the artifact.
        `{"schemaVersion":1,"commit":"${headSha.slice(0, 12)}"}`,
        `{"schemaVersion":1,"commit":"${headSha.toUpperCase()}"}`,
        '{"schemaVersion":1,"commit":{"toString":"pwn"}}',
        '{"schemaVersion":1}',
        'not json at all',
        '[]',
      ]) {
        await writeFile(path, junk)
        expect(readBuildCommit(root)).toBe(UNKNOWN_BUILD_FIELD)
      }
    })
  })

  it('reads the version out of the running package, not out of the source', async () => {
    // Whatever `package.json` says today — including after `npm version`
    // rewrites it during a release, which is why the version is NOT copied
    // into the build stamp.
    const declared = JSON.parse(
      await readFile(join(repoRoot, 'package.json'), 'utf8'),
    ) as { version: string }

    expect(readPackageVersion(repoRoot)).toBe(declared.version)

    await withTempRoot(async (root) => {
      await writeFile(
        join(root, 'package.json'),
        JSON.stringify({ name: '@agent-relay/factory', version: '9.9.9-rc.1' }),
      )
      expect(readPackageVersion(root)).toBe('9.9.9-rc.1')
      expect(readPackageVersion(root)).not.toBe(declared.version)
    })
  })

  it('reports `unknown` for a version that is absent or not publishable', async () => {
    await withTempRoot(async (root) => {
      expect(readPackageVersion(root)).toBe(UNKNOWN_BUILD_FIELD)
      for (const junk of [
        JSON.stringify({ name: '@agent-relay/factory' }),
        JSON.stringify({ version: 42 }),
        JSON.stringify({ version: `0.1.86\n${'x'.repeat(4_000)}` }),
        JSON.stringify({ version: '' }),
      ]) {
        await writeFile(join(root, 'package.json'), junk)
        expect(readPackageVersion(root)).toBe(UNKNOWN_BUILD_FIELD)
      }
    })
  })

  it('composes both halves independently, so one unknown does not hide the other', async () => {
    await withTempRoot(async (root) => {
      await writeFile(
        join(root, 'package.json'),
        JSON.stringify({ name: '@agent-relay/factory', version: '0.1.99' }),
      )
      // Version known, commit not: exactly the shape a pre-#446 release has,
      // and it still names the npm version an operator can resolve to a tag.
      expect(loadBuildIdentity({ packageRoot: root, buildRoot: root })).toEqual({
        version: '0.1.99',
        commit: UNKNOWN_BUILD_FIELD,
      })

      stamp(join(root, 'build-info.json'))
      expect(loadBuildIdentity({ packageRoot: root, buildRoot: root })).toEqual({
        version: '0.1.99',
        commit: headSha,
      })
    })
  })
})

describe('the build identity reaches the unauthenticated surface (#446)', () => {
  it('rides in the health block the container passes through to /healthz', () => {
    const published = publicHealthFromHeartbeat({
      status: 'running',
      pid: process.pid,
      updatedAt: new Date().toISOString(),
      updatedAtMs: Date.now(),
      workspaceId: 'factory-build-identity',
    } as never, { nowMs: Date.now() })

    // Same values the loader read off this artifact — not a re-derivation.
    expect(published.build).toEqual(readBuildIdentity())
    expect(published.build?.version).toBe(readPackageVersion(repoRoot))
  })

  it('is published even when there is no heartbeat to describe', () => {
    // "No heartbeat, and here is the build that has none" is strictly more
    // diagnosable than "no heartbeat" — and this is the path taken exactly
    // when a reader most wants to know what is running.
    expect(publicHealthFromHeartbeat(undefined).build).toEqual(readBuildIdentity())
  })

  it('keeps a remote record\'s identity distinct from "the remote said unknown"', () => {
    const commit = 'b'.repeat(40)
    // A reader normalising a record from a deployed instance keeps what it said…
    expect(normalizePublicHealth({ status: 'ok', build: { version: '0.1.90', commit } })?.build)
      .toEqual({ version: '0.1.90', commit })

    // …coerces junk from that instance to `unknown` rather than passing it on…
    expect(normalizePublicHealth({ status: 'ok', build: { version: 1, commit: 'nope' } })?.build)
      .toEqual({ version: UNKNOWN_BUILD_FIELD, commit: UNKNOWN_BUILD_FIELD })

    // …and leaves the field ABSENT when the instance published none, because
    // "that instance is too old to tell you" and "that instance told you
    // unknown" are different answers, and only one of them means redeploy.
    expect(normalizePublicHealth({ status: 'ok' })?.build).toBeUndefined()
    // Never substituted from the reader's own process, either.
    expect(normalizePublicHealth({ status: 'ok' })).not.toHaveProperty('build')
  })
})
