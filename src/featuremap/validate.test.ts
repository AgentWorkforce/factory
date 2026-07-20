import { execFileSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { checkFeatureMap } from './check'
import {
  findFeatureLocationDrift,
  parseManifestFeatures,
  validateFeatureManifest,
  validateFeatureManifestFile,
  type ManifestFeature,
} from './validate'

const temporaryDirectories: string[] = []

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true })
  }
})

describe('feature manifest validation', () => {
  it('parses a complete manifest and checks every location from the repository root', () => {
    const rootDir = temporaryDirectory()
    writeFile(rootDir, 'src/cli.ts', 'export {}\n')
    writeFile(rootDir, 'src/api.ts', 'export {}\n')
    writeFile(rootDir, '.agentworkforce/features/manifest.yaml', manifest())

    expect(validateFeatureManifestFile({ rootDir })).toMatchObject({
      categoryCount: 1,
      features: [
        {
          id: 'feature-one',
          location: 'src/cli.ts, src/api.ts',
          tier: 2,
          criticality: 'critical',
        },
      ],
    })
  })

  it('rejects duplicate feature IDs with the offending ID', () => {
    const raw = manifest({
      featureRows: `${featureRow()}\n${featureRow()}`,
      featureCount: 2,
      tierTwoCount: 2,
    })

    expect(() => parseManifestFeatures(raw)).toThrow(
      'Duplicate feature id in manifest: feature-one',
    )
  })

  it('rejects an out-of-range verify_tier with the offending ID', () => {
    const raw = manifest({ featureRows: featureRow().replace('verify_tier: 2', 'verify_tier: 7') })

    expect(() => validateFeatureManifest(raw)).toThrow(
      'Manifest feature feature-one has invalid verify_tier',
    )
  })

  it('rejects a deleted location with the feature ID and repository path', () => {
    const rootDir = temporaryDirectory()

    expect(() => validateFeatureManifest(manifest(), { rootDir })).toThrow(
      'Missing location for feature-one: src/cli.ts',
    )
  })

  it('rejects catalog totals that drift from the parsed contents', () => {
    expect(() => validateFeatureManifest(manifest({ featureCount: 4 }))).toThrow(
      'Manifest catalog mismatch: declared 1 categories/4 features, parsed 1/1',
    )
  })
})

describe('feature location drift', () => {
  it('flags a changed referenced file when confirmation fields remain unchanged', () => {
    const base = [feature()]
    const head = [feature()]

    expect(findFeatureLocationDrift(base, head, ['README.md', './src/cli.ts'])).toEqual([
      expect.objectContaining({
        featureId: 'feature-one',
        changedLocations: ['src/cli.ts'],
        verifyTier: 2,
        message: expect.stringContaining('description and verify_tier were not updated'),
      }),
    ])
  })

  it('does not flag an entry whose description, tier, or location was re-confirmed in the PR', () => {
    const base = [feature()]
    const head = [feature({ desc: 'Updated after reviewing the implementation' })]

    expect(findFeatureLocationDrift(base, head, ['src/cli.ts'])).toEqual([])
  })

  it('does not flag unrelated changed files', () => {
    expect(findFeatureLocationDrift([feature()], [feature()], ['src/other.ts'])).toEqual([])
  })

  it('flags files changed beneath a referenced directory location', () => {
    const base = [feature({ location: 'src/featuremap/' })]
    const head = [feature({ location: 'src/featuremap/' })]

    expect(findFeatureLocationDrift(base, head, ['src/featuremap/validate.ts'])).toEqual([
      expect.objectContaining({
        featureId: 'feature-one',
        changedLocations: ['src/featuremap'],
      }),
    ])
  })

  it('reports advisory drift from a real repository diff without failing validation', async () => {
    const rootDir = temporaryDirectory()
    writeFile(rootDir, 'src/cli.ts', 'export const value = 1\n')
    writeFile(rootDir, 'src/api.ts', 'export {}\n')
    writeFile(rootDir, '.agentworkforce/features/manifest.yaml', manifest())
    git(rootDir, ['init'])
    git(rootDir, ['config', 'user.email', 'factory@example.com'])
    git(rootDir, ['config', 'user.name', 'Factory Test'])
    git(rootDir, ['add', '.'])
    git(rootDir, ['commit', '-m', 'base'])

    writeFile(rootDir, 'src/cli.ts', 'export const value = 2\n')
    const report = await checkFeatureMap({ rootDir, baseRef: 'HEAD' })

    expect(report.ok).toBe(true)
    expect(report.advisories).toEqual([
      expect.objectContaining({
        featureId: 'feature-one',
        changedLocations: ['src/cli.ts'],
      }),
    ])
  })
})

function manifest(options: {
  featureRows?: string
  featureCount?: number
  tierTwoCount?: number
} = {}): string {
  return `version: '1.0'
updated: '2026-07-20'
catalog:
  category_count: 1
  feature_count: ${options.featureCount ?? 1}
  tier_counts:
    1: 0
    2: ${options.tierTwoCount ?? 1}
    3: 0
    4: 0
    5: 0
    6: 0
categories:
  core:
    name: Core
    description: Core features
    criticality: critical
    features:
${options.featureRows ?? featureRow()}
`
}

function featureRow(): string {
  return `      - id: feature-one
        name: Feature One
        cli: factory one
        description: Exercise the first feature
        location: src/cli.ts, src/api.ts
        verify_tier: 2`
}

function feature(overrides: Partial<ManifestFeature> = {}): ManifestFeature {
  return {
    id: 'feature-one',
    name: 'Feature One',
    cli: 'factory one',
    desc: 'Exercise the first feature',
    location: 'src/cli.ts, src/api.ts',
    tier: 2,
    criticality: 'critical',
    ...overrides,
  }
}

function temporaryDirectory(): string {
  const directory = mkdtempSync(join(tmpdir(), 'factory-featuremap-'))
  temporaryDirectories.push(directory)
  return directory
}

function writeFile(rootDir: string, path: string, contents: string): void {
  const absolutePath = join(rootDir, path)
  mkdirSync(dirname(absolutePath), { recursive: true })
  writeFileSync(absolutePath, contents)
}

function git(rootDir: string, args: string[]): void {
  execFileSync('git', ['-C', rootDir, ...args], { stdio: 'ignore' })
}
