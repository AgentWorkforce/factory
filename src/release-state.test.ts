import { chmod, mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { readFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { describe, expect, it } from 'vitest'

import { comparePackageTrees } from '../scripts/compare-package-trees.mjs'
import { planReleaseState } from '../scripts/release-state.mjs'

const base = {
  head: 'head-sha',
  tagTarget: '',
  published: false,
  tagPayloadMatches: false,
  registryPayloadMatches: false,
}

describe('release state recovery', () => {
  it('is importable when the host process has no script path', () => {
    expect(() => execFileSync(process.execPath, [
      '--input-type=module',
      '--eval',
      "process.argv.splice(1); await import('./scripts/release-state.mjs')",
    ])).not.toThrow()
  })

  it('starts a new release by creating its tag before publishing', () => {
    expect(planReleaseState(base)).toEqual({
      state: 'new-release',
      createTag: true,
      publish: true,
    })
  })

  it('resumes publishing when a matching tag already exists', () => {
    expect(planReleaseState({ ...base, tagTarget: 'head-sha' })).toEqual({
      state: 'resume-after-tag',
      createTag: false,
      publish: true,
    })
  })

  it('recovers a missing tag only after verifying the published payload', () => {
    expect(planReleaseState({
      ...base,
      published: true,
      registryPayloadMatches: true,
    })).toEqual({
      state: 'recover-missing-tag',
      createTag: true,
      publish: false,
    })
  })

  it('treats a fully matching published release as an idempotent no-op', () => {
    expect(planReleaseState({
      ...base,
      published: true,
      registryPayloadMatches: true,
      tagTarget: 'head-sha',
    })).toEqual({ state: 'complete', createTag: false, publish: false })
  })

  it('accepts the historical 0.1.58 tag only when its payload is equivalent', () => {
    expect(planReleaseState({
      ...base,
      published: true,
      registryPayloadMatches: true,
      tagTarget: 'release-commit',
      tagPayloadMatches: true,
    })).toEqual({
      state: 'complete-equivalent-legacy-tag',
      createTag: false,
      publish: false,
    })
  })

  it('fails closed for mismatched registry payloads and tag targets', () => {
    expect(() => planReleaseState({
      ...base,
      published: true,
      registryPayloadMatches: false,
    })).toThrow(/payload or provenance/)
    expect(() => planReleaseState({ ...base, tagTarget: 'other-sha' })).toThrow(/other-sha/)
  })
})

describe('packed payload comparison', () => {
  it('compares file contents and all permission modes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-release-payload-'))
    const left = join(root, 'left')
    const right = join(root, 'right')
    try {
      await Promise.all([mkdir(left), mkdir(right)])
      await Promise.all([
        writeFile(join(left, 'cli'), '#!/bin/sh\n'),
        writeFile(join(right, 'cli'), '#!/bin/sh\n'),
      ])
      await Promise.all([chmod(join(left, 'cli'), 0o755), chmod(join(right, 'cli'), 0o644)])
      expect(await comparePackageTrees(left, right)).toContain('cli: mode 755 != 644')
      await chmod(join(right, 'cli'), 0o755)
      expect(await comparePackageTrees(left, right)).toEqual([])
      await chmod(join(left, 'cli'), 0o4755)
      expect(await comparePackageTrees(left, right)).toContain('cli: mode 4755 != 755')
      await chmod(join(right, 'cli'), 0o4755)
      expect(await comparePackageTrees(left, right)).toEqual([])
      await writeFile(join(right, 'cli'), '#!/bin/false\n')
      expect(await comparePackageTrees(left, right)).toContain('cli: content differs')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

describe('build stamp in the packed payload (#446)', () => {
  /**
   * `dist/build-info.json` is the one file whose content is SUPPOSED to differ
   * between two builds of the same code, and the release-recovery path in
   * `publish.yml` rebuilds a tagged commit and compares its payload to npm's.
   * Byte comparison would make that recovery impossible by construction (see
   * the 0.1.58 incident note in `compare-package-trees.mjs`), so the stamp is
   * compared as a stamp — and the exemption is exactly one field wide.
   */
  const withTrees = async (
    run: (left: string, right: string) => Promise<void>,
  ): Promise<void> => {
    const root = await mkdtemp(join(tmpdir(), 'factory-build-stamp-payload-'))
    const left = join(root, 'left')
    const right = join(root, 'right')
    try {
      await Promise.all([mkdir(join(left, 'dist'), { recursive: true }), mkdir(join(right, 'dist'), { recursive: true })])
      await run(left, right)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  }
  const stampPath = (root: string) => join(root, 'dist', 'build-info.json')
  const a = 'a'.repeat(40)
  const b = 'b'.repeat(40)

  it('accepts two builds of the same code and names both commits', async () => {
    await withTrees(async (left, right) => {
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: b }))
      const notes: string[] = []
      expect(await comparePackageTrees(left, right, { notes })).toEqual([])
      // Tolerated, not silent: the recovery reader gets the provenance fact.
      expect(notes).toEqual([
        `dist/build-info.json: build commit differs (${a} vs ${b}); exempt from byte comparison`,
      ])
    })
  })

  it('never lets the note claim an equivalence the comparison refutes', async () => {
    // Review follow-up (#468, P2, codex). The note is emitted mid-traversal, by
    // a function that has seen ONE file. Wording like "same code" would print
    // immediately above `package.json: content differs` and certify an
    // equivalence the comparison goes on to refute. It states a fact about the
    // stamp; the verdict is the caller's.
    await withTrees(async (left, right) => {
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: b }))
      await writeFile(join(left, 'package.json'), JSON.stringify({ version: '0.1.86' }))
      await writeFile(join(right, 'package.json'), JSON.stringify({ version: '0.1.87' }))

      const notes: string[] = []
      const differences = await comparePackageTrees(left, right, { notes })

      // The payload does NOT match…
      expect(differences).toContain('package.json: content differs')
      // …and the note still names both commits, because a recovery failure is
      // exactly when a reader needs to know which two builds were compared…
      expect(notes).toEqual([
        `dist/build-info.json: build commit differs (${a} vs ${b}); exempt from byte comparison`,
      ])
      // …while claiming nothing about the tree it has not finished walking.
      expect(notes.join('\n')).not.toMatch(/same code|identical|equivalent|matches/u)
    })
  })

  it('says nothing when the two builds are the same build', async () => {
    await withTrees(async (left, right) => {
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: a }))
      const notes: string[] = []
      expect(await comparePackageTrees(left, right, { notes })).toEqual([])
      expect(notes).toEqual([])
    })
  })

  it('validates the stamp even when the two are byte-identical', async () => {
    // Review follow-up (#468, P1, cubic). Equality is not validity. A
    // short-circuit on equal bytes would exempt a stamp that is malformed on
    // BOTH sides, letting release recovery certify an artifact whose runtime
    // loader reports `commit: "unknown"` — every other file in the payload is
    // safe to skip on equal bytes, but this one is validated, not just diffed.
    await withTrees(async (left, right) => {
      for (const identical of [
        JSON.stringify({ schemaVersion: 1, commit: 'HEAD' }),
        JSON.stringify({ schemaVersion: 1 }),
        JSON.stringify({ schemaVersion: 1, commit: a.slice(0, 12) }),
        'not json',
      ]) {
        await writeFile(stampPath(left), identical)
        await writeFile(stampPath(right), identical)
        expect(await comparePackageTrees(left, right)).not.toEqual([])
      }

      // …and an identical WELL-FORMED stamp still passes silently.
      const good = JSON.stringify({ schemaVersion: 1, commit: a })
      await writeFile(stampPath(left), good)
      await writeFile(stampPath(right), good)
      const notes: string[] = []
      expect(await comparePackageTrees(left, right, { notes })).toEqual([])
      expect(notes).toEqual([])
    })
  })

  it('exempts the commit and nothing else', async () => {
    await withTrees(async (left, right) => {
      // A second field that differs is a real payload difference. The exemption
      // must not widen into "this file is not compared".
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 2, commit: b }))
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: build stamp field schemaVersion differs')

      // An added field is a difference too, in either direction.
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: b, extra: 1 }))
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: build stamp field extra differs')
    })
  })

  it('refuses a payload whose stamp is not a stamp', async () => {
    await withTrees(async (left, right) => {
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))

      await writeFile(stampPath(right), 'not json')
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: content differs (unparseable build stamp)')

      await writeFile(stampPath(right), JSON.stringify([{ commit: b }]))
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: content differs (build stamp is not an object)')

      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1 }))
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: right build stamp carries no full commit SHA (undefined)')
    })
  })

  it('grants the exemption to a commit, not to a string in the commit slot', async () => {
    // Review follow-up (#468, P2, codex). A type-only check would let a
    // CORRUPT registry stamp — `"HEAD"`, `"corrupt"`, an abbreviated SHA —
    // pass as a provenance difference, so `verify-release-payload.sh` would
    // return success for an artifact whose runtime loader reports
    // `commit: "unknown"`. That is the silent lie this change exists to
    // remove, re-entering through the check meant to catch it.
    await withTrees(async (left, right) => {
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: a }))
      for (const corrupt of ['HEAD', 'corrupt', a.slice(0, 12), a.toUpperCase(), '', 42, null]) {
        await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: corrupt }))
        const notes: string[] = []
        expect(await comparePackageTrees(left, right, { notes })).toContain(
          `dist/build-info.json: right build stamp carries no full commit SHA (${JSON.stringify(corrupt)})`,
        )
        // …and it is never reported as a tolerated provenance difference.
        expect(notes).toEqual([])
      }

      // The same rule applies to the locally rebuilt side, not just the registry's.
      await writeFile(stampPath(left), JSON.stringify({ schemaVersion: 1, commit: 'HEAD' }))
      await writeFile(stampPath(right), JSON.stringify({ schemaVersion: 1, commit: b }))
      expect(await comparePackageTrees(left, right))
        .toContain('dist/build-info.json: left build stamp carries no full commit SHA ("HEAD")')
    })
  })

  it('gives no other JSON file in the payload the same exemption', async () => {
    await withTrees(async (left, right) => {
      // Same shape, same field name, different path: still a payload mismatch.
      await writeFile(join(left, 'dist', 'other.json'), JSON.stringify({ commit: a }))
      await writeFile(join(right, 'dist', 'other.json'), JSON.stringify({ commit: b }))
      expect(await comparePackageTrees(left, right)).toContain('dist/other.json: content differs')
    })
  })
})

describe('require-current-main.sh argument validation', () => {
  const run = (args) =>
    execFileSync('bash', ['scripts/require-current-main.sh', ...args], {
      env: { ...process.env, GITHUB_REF: 'refs/heads/main' },
      encoding: 'utf8',
    })

  it('accepts --ref-only alone without contacting the remote', () => {
    expect(run(['--ref-only'])).toBe('')
  })

  it('rejects --ref-only with trailing arguments instead of ignoring them', () => {
    try {
      run(['--ref-only', 'extra'])
      expect.unreachable('expected require-current-main.sh to exit non-zero')
    } catch (error) {
      expect(error.status).toBe(2)
      expect(error.stderr.toString()).toContain('usage:')
    }
  })

  it('rejects unrecognized single arguments', () => {
    try {
      run(['--bogus'])
      expect.unreachable('expected require-current-main.sh to exit non-zero')
    } catch (error) {
      expect(error.status).toBe(2)
      expect(error.stderr.toString()).toContain('usage:')
    }
  })
})

describe('publish workflow policy', () => {
  const workflow = readFileSync('.github/workflows/publish.yml', 'utf8')

  it('uses a protected-branch-safe version PR and never pushes HEAD to main', () => {
    expect(workflow).toContain('pull-requests: write')
    expect(workflow).toContain('name: Require main branch for a live release')
    expect(workflow.match(/scripts\/require-current-main\.sh/g)).toHaveLength(3)
    expect(workflow).toContain('scripts/require-current-main.sh --ref-only')
    expect(workflow).toContain('name: Open version PR')
    expect(workflow).toContain('gh pr create')
    expect(workflow).toContain('git add package.json package-lock.json')
    expect(workflow).not.toContain('git push origin HEAD --follow-tags')
    expect(workflow).toContain('git push origin "refs/tags/v$V"')
  })

  it('gates publish on canonical version metadata and the recovery plan', () => {
    expect(workflow).toContain("steps.bump.outputs.needs_version_pr != 'true'")
    expect(workflow).toContain('node scripts/release-state.mjs')
    expect(workflow.match(/scripts\/verify-release-payload\.sh/g)).toHaveLength(3)
    expect(workflow).toContain('git worktree add --detach "$TAG_DIR" "$TAG_TARGET"')
    expect(workflow).toContain("steps.release_state.outputs.publish == 'true'")
    expect(workflow.match(/RELEASE_STATE: \$\{\{ steps\.release_state\.outputs\.state \}\}/g))
      .toHaveLength(2)
    expect(workflow.match(/\[ "\$RELEASE_STATE" = "new-release" \]/g)).toHaveLength(2)
    expect(workflow).toContain('NPM_DIST_TAG: ${{ github.event.inputs.tag }}')
    expect(workflow).toContain('npm publish --provenance --access public --tag "$NPM_DIST_TAG"')
    expect(workflow.indexOf('- name: Create release tag')).toBeLessThan(
      workflow.indexOf('- name: Publish\n'),
    )
  })
})
