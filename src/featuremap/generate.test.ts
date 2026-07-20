import { mkdtemp, mkdir, readFile, rm, unlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  FEATURE_MAP_MANIFEST_PATH,
  generateFeatureMap,
  parseFeatureMapManifest,
} from './generate'
import { validateFeatureManifest } from './validate'

const roots: string[] = []
const fixedNow = () => new Date('2026-07-20T12:00:00.000Z')

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('generateFeatureMap', () => {
  it('bootstraps a schema-compatible manifest without crawling an unmapped repository', async () => {
    const root = await repository()
    await put(root, 'src/pricing.ts', [
      '/** Calculate the total price for an order. */',
      'export function calculateTotal(lines: number[]): number {',
      '  return lines.reduce((total, line) => total + line, 0)',
      '}',
    ].join('\n'))
    await put(root, 'src/untouched.ts', 'export function shouldNotAppear(): void {}\n')

    const result = await generateFeatureMap({
      repoPath: root,
      touchedFileGlobs: ['src/pricing.ts'],
      now: fixedNow,
    })

    expect(result.ok).toBe(true)
    expect(result.status).toBe('created')
    expect(result.touchedFiles).toEqual(['src/pricing.ts'])
    expect(result.added).toMatchObject([{
      name: 'Calculate Total',
      api: 'calculateTotal()',
      description: 'Calculate the total price for an order.',
      location: 'src/pricing.ts',
      verifyTier: 1,
    }])

    const raw = await readFile(join(root, FEATURE_MAP_MANIFEST_PATH), 'utf8')
    const manifest = parseFeatureMapManifest(raw)
    expect(validateFeatureManifest(raw, { rootDir: root }).features).toHaveLength(1)
    expect(manifest.categoryIds).toEqual(['generated-touched-surfaces'])
    expect(manifest.features).toEqual(result.added)
    expect(raw).toContain("updated: \"2026-07-20\"")
    expect(raw).toContain('  feature_count: 1')
    expect(raw).not.toContain('shouldNotAppear')
  })

  it('extends only newly touched files and preserves existing entries byte-for-byte', async () => {
    const root = await repository()
    await put(root, 'src/pricing.ts', 'export function calculateTotal(): number { return 1 }\n')
    await put(root, 'src/orders.ts', [
      "router.get('/orders', async () => {",
      "  return fetch('https://orders.example.test')",
      '})',
    ].join('\n'))

    const first = await generateFeatureMap(root, ['src/pricing.ts'], { now: fixedNow })
    expect(first.status).toBe('created')
    const firstRaw = await readFile(join(root, FEATURE_MAP_MANIFEST_PATH), 'utf8')
    const firstFeatureBlock = featureBlock(firstRaw, first.added[0].id)

    const second = await generateFeatureMap(root, ['src/pricing.ts', 'src/orders.ts'], { now: fixedNow })

    expect(second.status).toBe('updated')
    expect(second.added).toMatchObject([{
      name: 'GET /orders',
      api: 'GET /orders',
      location: 'src/orders.ts',
      verifyTier: 3,
    }])
    const secondRaw = await readFile(join(root, FEATURE_MAP_MANIFEST_PATH), 'utf8')
    const parsed = parseFeatureMapManifest(secondRaw)
    expect(parsed.features).toHaveLength(2)
    expect(new Set(parsed.features.map((feature) => feature.id)).size).toBe(2)
    expect(featureBlock(secondRaw, first.added[0].id)).toBe(firstFeatureBlock)

    const third = await generateFeatureMap(root, ['src/**/*.ts'], { now: fixedNow })
    expect(third.status).toBe('unchanged')
    expect(third.added).toEqual([])
    expect(await readFile(join(root, FEATURE_MAP_MANIFEST_PATH), 'utf8')).toBe(secondRaw)
  })

  it('supports bounded brace globs while excluding explicitly negated files', async () => {
    const root = await repository()
    await put(root, 'src/a.ts', 'export const alpha = 1\n')
    await put(root, 'src/nested/b.tsx', 'export function beta(): void {}\n')
    await put(root, 'src/nested/b.test.tsx', 'export function betaTest(): void {}\n')
    await put(root, 'README.md', '# untouched\n')

    const result = await generateFeatureMap(root, ['src/**/*.{ts,tsx}', '!**/*.test.tsx'], { now: fixedNow })

    expect(result.status).toBe('created')
    expect(result.touchedFiles).toEqual(['src/a.ts', 'src/nested/b.tsx'])
    expect(result.added.map((feature) => feature.location)).toEqual(['src/a.ts', 'src/nested/b.tsx'])
  })

  it('caps incremental growth and defers excess touched files', async () => {
    const root = await repository()
    await put(root, 'src/a.ts', 'export const alpha = 1\n')
    await put(root, 'src/b.ts', 'export const beta = 2\n')

    const result = await generateFeatureMap(root, ['src/*.ts'], {
      maxManifestFeatures: 1,
      maxNewEntries: 10,
      now: fixedNow,
    })

    expect(result.status).toBe('created')
    expect(result.added).toHaveLength(1)
    expect(parseFeatureMapManifest(await readFile(result.manifestPath, 'utf8')).features).toHaveLength(1)
    expect(result.warnings).toContain('Feature map growth capped at 1 new entries; 1 touched files were deferred.')
  })

  it('flags stale locations without deleting or rewriting their entries', async () => {
    const root = await repository()
    await put(root, 'src/old.ts', '/** Original behavior. */\nexport function oldBehavior(): void {}\n')
    const first = await generateFeatureMap(root, ['src/old.ts'], { now: fixedNow })
    const manifestPath = join(root, FEATURE_MAP_MANIFEST_PATH)
    const originalBlock = featureBlock(await readFile(manifestPath, 'utf8'), first.added[0].id)

    await unlink(join(root, 'src/old.ts'))
    await put(root, 'src/new.ts', 'export function newBehavior(): void {}\n')
    const warn = vi.fn()
    const second = await generateFeatureMap(root, ['src/new.ts'], { logger: { warn }, now: fixedNow })

    expect(second.status).toBe('updated')
    expect(second.staleLocations).toEqual([{
      featureId: first.added[0].id,
      location: 'src/old.ts',
      reason: 'missing',
    }])
    const updatedRaw = await readFile(manifestPath, 'utf8')
    expect(featureBlock(updatedRaw, first.added[0].id)).toBe(originalBlock)
    expect(parseFeatureMapManifest(updatedRaw).features.map((feature) => feature.location)).toEqual([
      'src/old.ts',
      'src/new.ts',
    ])
    expect(warn).toHaveBeenCalledWith(
      'Feature map contains stale locations; entries were preserved for drift review.',
      { staleLocations: second.staleLocations },
    )
  })

  it('adds a generated category to an existing hand-authored manifest', async () => {
    const root = await repository()
    await put(root, 'src/existing.ts', 'export function existing(): void {}\n')
    await put(root, 'src/new.ts', 'export function newlyTouched(): void {}\n')
    await put(root, FEATURE_MAP_MANIFEST_PATH, handAuthoredManifest())

    const result = await generateFeatureMap(root, ['src/existing.ts', 'src/new.ts'], { now: fixedNow })

    expect(result.status).toBe('updated')
    expect(result.added).toMatchObject([{ api: 'newlyTouched()', location: 'src/new.ts' }])
    const raw = await readFile(result.manifestPath, 'utf8')
    const manifest = parseFeatureMapManifest(raw)
    expect(manifest.categoryIds).toEqual(['core', 'generated-touched-surfaces'])
    expect(manifest.features.map((feature) => feature.id)).toEqual(['existing', result.added[0].id])
    expect(raw).toContain('        description: Original hand-authored entry\n')
  })

  it('returns non-fatal failures and timeouts instead of rejecting', async () => {
    const missing = join(tmpdir(), `factory-feature-map-missing-${Date.now()}`)
    await expect(generateFeatureMap(missing, ['src/a.ts'])).resolves.toMatchObject({
      ok: false,
      status: 'failed',
    })

    const root = await repository()
    await put(root, 'src/a.ts', 'export const alpha = 1\n')
    await expect(generateFeatureMap(root, ['src/a.ts'], { timeoutMs: 0 })).resolves.toMatchObject({
      ok: false,
      status: 'timed-out',
    })
    await expect(readFile(join(root, FEATURE_MAP_MANIFEST_PATH), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' })
  })

  it('does not overwrite a malformed existing manifest', async () => {
    const root = await repository()
    await put(root, 'src/a.ts', 'export const alpha = 1\n')
    const manifestPath = join(root, FEATURE_MAP_MANIFEST_PATH)
    await put(root, FEATURE_MAP_MANIFEST_PATH, 'version: 1\ncategories: {}\n')
    const before = await readFile(manifestPath, 'utf8')

    const result = await generateFeatureMap(root, ['src/a.ts'])

    expect(result).toMatchObject({ ok: false, status: 'failed' })
    expect(await readFile(manifestPath, 'utf8')).toBe(before)
  })
})

async function repository(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-feature-map-'))
  roots.push(root)
  return root
}

async function put(root: string, location: string, content: string): Promise<void> {
  const path = join(root, location)
  await mkdir(dirname(path), { recursive: true })
  await writeFile(path, content, 'utf8')
}

function featureBlock(raw: string, id: string): string {
  const escapedId = id.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
  const match = new RegExp(`^      - id: ["']?${escapedId}["']?[\\s\\S]*?(?=^      - id:|^  [a-z]|\\s*$)`, 'mu').exec(raw)
  if (!match) throw new Error(`Missing feature block ${id}`)
  return match[0].trimEnd()
}

function handAuthoredManifest(): string {
  return [
    "version: '1.0'",
    "updated: '2026-07-01'",
    'catalog:',
    '  category_count: 1',
    '  feature_count: 1',
    '  tier_counts:',
    '    1: 1',
    '    2: 0',
    '    3: 0',
    '    4: 0',
    '    5: 0',
    '    6: 0',
    'categories:',
    '  core:',
    '    name: Core',
    '    description: Existing features',
    '    criticality: critical',
    '    features:',
    '      - id: existing',
    '        name: Existing',
    '        api: existing()',
    '        description: Original hand-authored entry',
    '        location: src/existing.ts',
    '        verify_tier: 1',
    '',
  ].join('\n')
}
