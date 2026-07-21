import { existsSync, readFileSync } from 'node:fs'
import { isAbsolute, relative, resolve } from 'node:path'

import { parse } from 'yaml'

export const FEATURE_MAP_MANIFEST_PATH = '.agentworkforce/features/manifest.yaml'

export type FeatureCriticality = 'critical' | 'hot' | 'standard'

export interface ManifestFeature {
  id: string
  name: string
  category: string
  cli?: string
  api?: string
  desc: string
  location: string
  tier: number
  criticality: FeatureCriticality
  procedure?: string
}

export interface FeatureManifestValidation {
  version: string
  categoryCount: number
  categoryIds: string[]
  features: ManifestFeature[]
  verificationDocument?: string
  categoryProcedures: Record<string, string>
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
  const version = nonEmptyString(manifest.version)
  if (!version) throw new Error('Manifest is missing version')
  const categories = requireRecord(manifest.categories, 'Manifest is missing categories')
  const categoryIds = Object.keys(categories)
  const verification = validateVerification(manifest.verification, categoryIds, version)
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
      const feature = validateFeature(
        inputFeature,
        categoryId,
        criticality,
        verification.categoryProcedures[categoryId],
      )
      if (seenIds.has(feature.id)) {
        throw new Error(`Duplicate feature id in manifest: ${feature.id}`)
      }
      seenIds.add(feature.id)
      features.push(feature)
    }
  }

  if (features.length === 0) throw new Error('Manifest parsed but contained no features')
  validateCatalogSummary(manifest.catalog, categoryIds.length, features)

  if (options.rootDir !== undefined) {
    validateFeatureLocations(features, options.rootDir, options.pathExists ?? existsSync)
    if (verification.document) {
      validateRepositoryPath(
        verification.document,
        options.rootDir,
        options.pathExists ?? existsSync,
        'Feature verification document',
      )
    }
  }

  return {
    version,
    categoryCount: categoryIds.length,
    categoryIds,
    features,
    ...(verification.document ? { verificationDocument: verification.document } : {}),
    categoryProcedures: verification.categoryProcedures,
  }
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
  const relativeManifestPath = relative(rootDir, absoluteManifestPath).replaceAll('\\', '/')
  if (
    relativeManifestPath === '..' ||
    relativeManifestPath.startsWith('../') ||
    isAbsolute(relativeManifestPath)
  ) {
    throw new Error(`Feature manifest must be inside the repository root: ${absoluteManifestPath}`)
  }
  const raw = readFileSync(absoluteManifestPath, 'utf8')
  const result = validateFeatureManifest(raw, {
    rootDir,
    ...(options.pathExists ? { pathExists: options.pathExists } : {}),
  })
  if (result.verificationDocument) {
    const procedures = readFileSync(resolve(rootDir, result.verificationDocument), 'utf8')
    for (const procedure of new Set(Object.values(result.categoryProcedures))) {
      const heading = new RegExp(`^## ${escapeRegExp(procedure)}\\s*$`, 'mu')
      if (!heading.test(procedures)) {
        throw new Error(`Missing feature verification procedure heading: ## ${procedure}`)
      }
    }
  }
  return result
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

/** Parse YAML while preserving manifest-specific validation errors. */
function parseManifestYaml(raw: string): Record<string, unknown> {
  try {
    return requireRecord(parse(raw) as unknown, 'Manifest root must be an object')
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Manifest ')) throw error
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Manifest YAML is invalid: ${message}`)
  }
}

/** Escape a literal procedure name for use in a regular expression. */
function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
}

/** Validate and normalize one feature within its category and procedure route. */
function validateFeature(
  input: unknown,
  category: string,
  criticality: FeatureCriticality,
  procedure: string | undefined,
): ManifestFeature {
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
    category,
    ...(cli ? { cli } : {}),
    ...(api ? { api } : {}),
    desc: description,
    location,
    tier: verifyTier as number,
    criticality,
    ...(procedure ? { procedure } : {}),
  }
}

/** Validate the v1.1 category-to-procedure routing contract. */
function validateVerification(
  input: unknown,
  categoryIds: readonly string[],
  version: string,
): { document?: string; categoryProcedures: Record<string, string> } {
  if (input === undefined) {
    if (version === '1.1') {
      throw new Error('Manifest version 1.1 is missing verification routing')
    }
    return { categoryProcedures: {} }
  }

  const verification = requireRecord(input, 'Manifest verification must be an object')
  const document = nonEmptyString(verification.document)
  if (!document) throw new Error('Manifest verification is missing document')
  const procedures = requireRecord(
    verification.categories,
    'Manifest verification is missing categories',
  )
  const categorySet = new Set(categoryIds)
  const categoryProcedures: Record<string, string> = {}
  for (const categoryId of categoryIds) {
    const procedure = nonEmptyString(procedures[categoryId])
    if (!procedure || !/^[a-z0-9-]+$/u.test(procedure)) {
      throw new Error(`Manifest category ${categoryId} has invalid verification procedure`)
    }
    categoryProcedures[categoryId] = procedure
  }
  for (const mappedCategory of Object.keys(procedures)) {
    if (!categorySet.has(mappedCategory)) {
      throw new Error(`Manifest verification maps unknown category: ${mappedCategory}`)
    }
  }
  return { document, categoryProcedures }
}

/** Confirm declared catalog and tier totals match parsed features. */
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

/** Validate every comma-delimited repository location for every feature. */
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
      validateRepositoryPath(location, rootDir, pathExists, 'Feature location', feature.id)
    }
  }
}

/** Require a declared path to stay inside the repository and exist. */
function validateRepositoryPath(
  path: string,
  rootDir: string,
  pathExists: (path: string) => boolean,
  label: string,
  featureId?: string,
): void {
  const absolutePath = isAbsolute(path) ? path : resolve(rootDir, path)
  const relativePath = relative(rootDir, absolutePath).replaceAll('\\', '/')
  if (relativePath === '..' || relativePath.startsWith('../') || isAbsolute(relativePath)) {
    if (featureId) {
      throw new Error(`Feature location must be inside the repository root for ${featureId}: ${path}`)
    }
    throw new Error(`${label} must be inside the repository root: ${path}`)
  }
  if (!pathExists(absolutePath)) {
    if (featureId) {
      throw new Error(`Missing location for ${featureId}: ${path}`)
    }
    throw new Error(`Missing ${label.toLowerCase()}: ${path}`)
  }
}

/** Return whether a feature was explicitly reconfirmed after its source path changed. */
function featureConfirmationChanged(base: ManifestFeature, head: ManifestFeature): boolean {
  return base.desc !== head.desc ||
    base.tier !== head.tier ||
    base.category !== head.category ||
    base.procedure !== head.procedure ||
    featureLocations(base).map(normalizeRepositoryPath).join('\n') !==
      featureLocations(head).map(normalizeRepositoryPath).join('\n')
}

/** Normalize a repository-relative path for drift comparisons. */
function normalizeRepositoryPath(path: string): string {
  return path.trim().replaceAll('\\', '/').replace(/^\.\//u, '').replace(/\/$/u, '')
}

/** Read a non-negative safe integer from the catalog summary. */
function catalogInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`Manifest catalog is missing a valid ${label}`)
  }
  return value as number
}

/** Narrow an unknown manifest value to a non-array object record. */
function requireRecord(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(message)
  }
  return value as Record<string, unknown>
}

/** Return a trimmed string only when it contains content. */
function nonEmptyString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined
}

/** Narrow a value to a supported feature criticality. */
function isCriticality(value: unknown): value is FeatureCriticality {
  return value === 'critical' || value === 'hot' || value === 'standard'
}
