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
  it('compares file contents and executable modes', async () => {
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
      await writeFile(join(right, 'cli'), '#!/bin/false\n')
      expect(await comparePackageTrees(left, right)).toContain('cli: content differs')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

describe('publish workflow policy', () => {
  const workflow = readFileSync('.github/workflows/publish.yml', 'utf8')

  it('uses a protected-branch-safe version PR and never pushes HEAD to main', () => {
    expect(workflow).toContain('pull-requests: write')
    expect(workflow).toContain('name: Require current main for a live release')
    expect(workflow.match(/scripts\/require-current-main\.sh/g)).toHaveLength(3)
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
    expect(workflow.indexOf('- name: Create release tag')).toBeLessThan(
      workflow.indexOf('- name: Publish\n'),
    )
  })
})
