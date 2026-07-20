import { existsSync, readFileSync } from 'node:fs'
import { isAbsolute, relative, resolve } from 'node:path'

import { parse } from 'yaml'

export const FEATURE_MAP_MANIFEST_PATH = '.agentworkforce/features/manifest.yaml'

export type FeatureCriticality = 'critical' | 'hot' | 'standard'

export interface ManifestFeature {
  id: string
  name: string
  cli?: string
  api?: string
  desc: string
  location: string
  tier: number
  criticality: FeatureCriticality
}

export interface FeatureManifestValidation {
  categoryCount: number
  features: ManifestFeature[]
}

export interface ValidateFeatureManifestOptions {
  /** Repository root used to resolve and check every declared location. */
  rootDir?: string
  /** Injectable existence probe for consumers that validate a virtual checkout. */
  pathExists?: (path: string) => boolean
}

export interface ValidateFeatureManifestFileOptions {
  rootDir?: string
  manifestPath?: string
  pathExists?: (path: string) => boolean
}

export interface FeatureLocationDriftAdvisory {
  featureId: string
  featureName: string
  changedLocations: string[]
  description: string
  verifyTier: number
  message: string
}

/**
 * Parse and structurally validate the checked-in feature manifest.
 *
 * This compatibility-shaped return value is also consumed by the deployed
 * factory-feature-guardian agent. Disk location checks are intentionally kept
 * in validateFeatureManifest so sandboxed consumers can supply their own probe.
 */
export function parseManifestFeatures(raw: string): ManifestFeature[] {
  return validateFeatureManifest(raw).features
}

/** Validate manifest structure, unique IDs, catalog totals, and optional paths. */
export function validateFeatureManifest(
  raw: string,
  options: ValidateFeatureManifestOptions = {},
): FeatureManifestValidation {
  const manifest = parseManifestYaml(raw)
  const categories = requireRecord(manifest.categories, 'Manifest is missing categories')
  const features: ManifestFeature[] = []
  const seenIds = new Set<string>()

  for (const [categoryId, inputCategory] of Object.entries(categories)) {
    const category = requireRecord(
      inputCategory,
      `Manifest category ${categoryId} must be an object`,
    )
    const criticality = category.criticality
    if (!isCriticality(criticality)) {
      throw new Error(`Manifest category ${categoryId} has invalid criticality`)
    }
    if (!Array.isArray(category.features)) {
      throw new Error(`Manifest category ${categoryId} is missing a features array`)
    }

    for (const inputFeature of category.features) {
      const feature = validateFeature(inputFeature, criticality)
      if (seenIds.has(feature.id)) {
        throw new Error(`Duplicate feature id in manifest: ${feature.id}`)
      }
      seenIds.add(feature.id)
      features.push(feature)
    }
  }

  if (features.length === 0) throw new Error('Manifest parsed but contained no features')
  validateCatalogSummary(manifest.catalog, Object.keys(categories).length, features)

  if (options.rootDir !== undefined) {
    validateFeatureLocations(features, options.rootDir, options.pathExists ?? existsSync)
  }

  return { categoryCount: Object.keys(categories).length, features }
}

/** Read the conventional repository manifest and validate all declared paths. */
export function validateFeatureManifestFile(
  options: ValidateFeatureManifestFileOptions = {},
): FeatureManifestValidation {
  const rootDir = resolve(options.rootDir ?? process.cwd())
  const manifestPath = options.manifestPath ?? FEATURE_MAP_MANIFEST_PATH
  const absoluteManifestPath = isAbsolute(manifestPath)
    ? manifestPath
    : resolve(rootDir, manifestPath)
  const raw = readFileSync(absoluteManifestPath, 'utf8')
  return validateFeatureManifest(raw, {
    rootDir,
    ...(options.pathExists ? { pathExists: options.pathExists } : {}),
  })
}

/** Split the manifest's comma-delimited location field into repository paths. */
export function featureLocations(feature: Pick<ManifestFeature, 'location'>): string[] {
  return feature.location.split(',').map((value) => value.trim()).filter(Boolean)
}

/**
 * Return advisory flags for changed feature locations whose description, tier,
 * and location were carried forward unchanged from the base manifest.
 */
export function findFeatureLocationDrift(
  baseFeatures: readonly ManifestFeature[],
  headFeatures: readonly ManifestFeature[],
  changedPaths: readonly string[],
): FeatureLocationDriftAdvisory[] {
  const normalizedChanges = changedPaths.map(normalizeRepositoryPath).filter(Boolean)
  const changedPathSet = new Set(normalizedChanges)
  const headById = new Map(headFeatures.map((feature) => [feature.id, feature]))
  const advisories: FeatureLocationDriftAdvisory[] = []

  for (const baseFeature of baseFeatures) {
    const changedLocations = featureLocations(baseFeature).flatMap((location) => {
      const normalizedLocation = normalizeRepositoryPath(location)
      const touched = changedPathSet.has(normalizedLocation) ||
        normalizedChanges.some((path) => path.startsWith(`${normalizedLocation}/`))
      return touched ? [normalizedLocation] : []
    })
    if (changedLocations.length === 0) continue

    const headFeature = headById.get(baseFeature.id)
    if (!headFeature || featureConfirmationChanged(baseFeature, headFeature)) continue

    advisories.push({
      featureId: headFeature.id,
      featureName: headFeature.name,
      changedLocations,
      description: headFeature.desc,
      verifyTier: headFeature.tier,
      message: `Re-confirm manifest feature ${headFeature.id}: changed ${changedLocations.join(', ')} but description and verify_tier were not updated`,
    })
  }

  return advisories.sort((left, right) => left.featureId.localeCompare(right.featureId))
}

function parseManifestYaml(raw: string): Record<string, unknown> {
  try {
    return requireRecord(parse(raw) as unknown, 'Manifest root must be an object')
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Manifest ')) throw error
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Manifest YAML is invalid: ${message}`)
  }
}

function validateFeature(input: unknown, criticality: FeatureCriticality): ManifestFeature {
  const record = input !== null && typeof input === 'object' && !Array.isArray(input)
    ? input as Record<string, unknown>
    : {}
  const id = nonEmptyString(record.id)
  const name = nonEmptyString(record.name)
  const description = nonEmptyString(record.description)
  const location = nonEmptyString(record.location)
  if (!id || !name || !description || !location) {
    throw new Error(`Incomplete manifest feature: ${JSON.stringify(input)}`)
  }

  const cli = nonEmptyString(record.cli)
  const api = nonEmptyString(record.api)
  if (!cli && !api) {
    throw new Error(`Manifest feature ${id} has neither cli nor api`)
  }

  const verifyTier = record.verify_tier
  if (!Number.isInteger(verifyTier) || (verifyTier as number) < 1 || (verifyTier as number) > 6) {
    throw new Error(`Manifest feature ${id} has invalid verify_tier`)
  }

  return {
    id,
    name,
    ...(cli ? { cli } : {}),
    ...(api ? { api } : {}),
    desc: description,
    location,
    tier: verifyTier as number,
    criticality,
  }
}

function validateCatalogSummary(
  inputCatalog: unknown,
  categoryCount: number,
  features: readonly ManifestFeature[],
): void {
  const catalog = requireRecord(inputCatalog, 'Manifest is missing catalog')
  const declaredCategories = catalogInteger(catalog.category_count, 'category_count')
  const declaredFeatures = catalogInteger(catalog.feature_count, 'feature_count')
  if (declaredCategories !== categoryCount || declaredFeatures !== features.length) {
    throw new Error(
      `Manifest catalog mismatch: declared ${declaredCategories} categories/${declaredFeatures} features, parsed ${categoryCount}/${features.length}`,
    )
  }

  const tierCounts = requireRecord(catalog.tier_counts, 'Manifest catalog is missing tier_counts')
  for (let tier = 1; tier <= 6; tier += 1) {
    const declared = catalogInteger(tierCounts[String(tier)], `tier ${tier}`)
    const actual = features.filter((feature) => feature.tier === tier).length
    if (declared !== actual) {
      throw new Error(`Manifest catalog tier ${tier} mismatch: declared ${declared}, parsed ${actual}`)
    }
  }
}

function validateFeatureLocations(
  features: readonly ManifestFeature[],
  rootDir: string,
  pathExists: (path: string) => boolean,
): void {
  for (const feature of features) {
    const locations = featureLocations(feature)
    if (locations.length === 0) {
      throw new Error(`Manifest feature ${feature.id} has no valid locations`)
    }
    for (const location of locations) {
      const absolutePath = isAbsolute(location) ? location : resolve(rootDir, location)
      const relativePath = relative(rootDir, absolutePath).replaceAll('\\', '/')
      if (relativePath === '..' || relativePath.startsWith('../') || isAbsolute(relativePath)) {
        throw new Error(`Feature location must be inside the repository root for ${feature.id}: ${location}`)
      }
      if (!pathExists(absolutePath)) {
        throw new Error(`Missing location for ${feature.id}: ${location}`)
      }
    }
  }
}

function featureConfirmationChanged(base: ManifestFeature, head: ManifestFeature): boolean {
  return base.desc !== head.desc ||
    base.tier !== head.tier ||
    featureLocations(base).map(normalizeRepositoryPath).join('\n') !==
      featureLocations(head).map(normalizeRepositoryPath).join('\n')
}

function normalizeRepositoryPath(path: string): string {
  return path.trim().replaceAll('\\', '/').replace(/^\.\//u, '').replace(/\/$/u, '')
}

function catalogInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`Manifest catalog is missing a valid ${label}`)
  }
  return value as number
}

function requireRecord(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(message)
  }
  return value as Record<string, unknown>
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined
}

function isCriticality(value: unknown): value is FeatureCriticality {
  return value === 'critical' || value === 'hot' || value === 'standard'
}
