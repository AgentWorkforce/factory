import { createHash, randomUUID } from 'node:crypto'
import {
  mkdir,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import lockfile from 'proper-lockfile'

import type { Logger } from '../ports/system'
import {
  FEATURE_MAP_MANIFEST_PATH,
  validateFeatureManifest,
  type FeatureCriticality,
  type ManifestFeature,
} from './validate'

export { FEATURE_MAP_MANIFEST_PATH } from './validate'

const GENERATED_CATEGORY_ID = 'generated-touched-surfaces'
const DEFAULT_TIMEOUT_MS = 2_000
const DEFAULT_MAX_NEW_ENTRIES = 25
const DEFAULT_MAX_MANIFEST_FEATURES = 250
const DEFAULT_MAX_TOUCHED_FILES = 100
const DEFAULT_MAX_SCANNED_PATHS = 2_000
const DEFAULT_MAX_SOURCE_BYTES = 128 * 1024
const DEFAULT_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
const DEFAULT_MAX_STALE_CHECKS = 500
const MANIFEST_LOCK_STALE_MS = 30_000
const MANIFEST_LOCK_RETRY_MS = 10
const SKIPPED_DIRECTORIES = new Set([
  '.git',
  '.hg',
  '.svn',
  '.next',
  'build',
  'coverage',
  'dist',
  'node_modules',
  'vendor',
])

export type FeatureMapCriticality = FeatureCriticality
export type FeatureMapVerifyTier = 1 | 2 | 3 | 4 | 5 | 6

export interface FeatureMapFeature {
  id: string
  name: string
  cli?: string
  api?: string
  description: string
  location: string
  verifyTier: FeatureMapVerifyTier
  criticality: FeatureMapCriticality
}

export interface ParsedFeatureMapManifest {
  categoryIds: string[]
  features: FeatureMapFeature[]
}

export interface StaleFeatureLocation {
  featureId: string
  location: string
  reason: 'missing' | 'outside-repo'
}

export interface FeatureMapGenerationOptions {
  /** Hard wall-clock budget for discovery, inference, validation, and persistence. */
  timeoutMs?: number
  /** Per-dispatch growth cap. */
  maxNewEntries?: number
  /** Total generated plus hand-authored feature cap. */
  maxManifestFeatures?: number
  /** Maximum touched files considered after glob expansion. */
  maxTouchedFiles?: number
  /** Maximum directory entries inspected while expanding globs. */
  maxScannedPaths?: number
  /** Files larger than this receive a path-derived fallback entry without being read. */
  maxSourceBytes?: number
  /** Existing manifests larger than this are not read or changed. */
  maxManifestBytes?: number
  /** Existing locations checked for drift on one invocation. */
  maxStaleChecks?: number
  logger?: Pick<Logger, 'warn'>
  now?: () => Date
}

export interface FeatureMapGenerationInput extends FeatureMapGenerationOptions {
  repoPath: string
  touchedFileGlobs: readonly string[]
}

export type FeatureMapGenerationStatus =
  | 'created'
  | 'updated'
  | 'unchanged'
  | 'failed'
  | 'timed-out'

export interface FeatureMapGenerationResult {
  ok: boolean
  status: FeatureMapGenerationStatus
  manifestPath: string
  added: FeatureMapFeature[]
  staleLocations: StaleFeatureLocation[]
  touchedFiles: string[]
  warnings: string[]
  error?: string
}

interface GenerationLimits {
  timeoutMs: number
  maxNewEntries: number
  maxManifestFeatures: number
  maxTouchedFiles: number
  maxScannedPaths: number
  maxSourceBytes: number
  maxManifestBytes: number
  maxStaleChecks: number
}

interface ResolvedInput {
  repoPath: string
  touchedFileGlobs: readonly string[]
  options: FeatureMapGenerationOptions
}

interface CandidateFile {
  absolutePath: string
  location: string
}

interface InferredSurface {
  name: string
  cli?: string
  api?: string
  description?: string
  matchIndex: number
}

class GenerationTimeoutError extends Error {
  constructor() {
    super('Feature map generation exceeded its time budget')
    this.name = 'GenerationTimeoutError'
  }
}

class Deadline {
  readonly #expiresAt: number

  constructor(timeoutMs: number) {
    this.#expiresAt = Date.now() + timeoutMs
  }

  check(): void {
    if (Date.now() >= this.#expiresAt) throw new GenerationTimeoutError()
  }

  async run<T>(operation: (signal: AbortSignal) => Promise<T>): Promise<T> {
    this.check()
    const remainingMs = this.#expiresAt - Date.now()
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort()
        reject(new GenerationTimeoutError())
      }, remainingMs)
    })

    try {
      return await Promise.race([operation(controller.signal), timeout])
    } catch (error) {
      if (controller.signal.aborted) throw new GenerationTimeoutError()
      throw error
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  remainingMs(): number {
    return Math.max(0, this.#expiresAt - Date.now())
  }

  async pause(maximumMs: number): Promise<void> {
    await this.run(async (signal) => {
      await delay(Math.min(maximumMs, this.remainingMs()), undefined, { signal })
    })
  }
}

/**
 * Lazily create or extend a repository's feature manifest from touched files.
 *
 * The function intentionally never rejects. Dispatch callers can record a
 * failed/timed-out result and continue their primary work unchanged.
 */
export function generateFeatureMap(input: FeatureMapGenerationInput): Promise<FeatureMapGenerationResult>
export function generateFeatureMap(
  repoPath: string,
  touchedFileGlobs: readonly string[],
  options?: FeatureMapGenerationOptions,
): Promise<FeatureMapGenerationResult>
export async function generateFeatureMap(
  inputOrRepoPath: FeatureMapGenerationInput | string,
  positionalGlobs: readonly string[] = [],
  positionalOptions: FeatureMapGenerationOptions = {},
): Promise<FeatureMapGenerationResult> {
  const input = normalizeInput(inputOrRepoPath, positionalGlobs, positionalOptions)
  const unresolvedManifestPath = join(resolve(input.repoPath), FEATURE_MAP_MANIFEST_PATH)
  const baseResult = {
    manifestPath: unresolvedManifestPath,
    added: [] as FeatureMapFeature[],
    staleLocations: [] as StaleFeatureLocation[],
    touchedFiles: [] as string[],
    warnings: [] as string[],
  }

  try {
    const limits = generationLimits(input.options)
    const deadline = new Deadline(limits.timeoutMs)
    const repoPath = await deadline.run(() => realpath(resolve(input.repoPath)))
    const repoStats = await deadline.run(() => stat(repoPath))
    if (!repoStats.isDirectory()) throw new Error(`Target repository path is not a directory: ${repoPath}`)

    const manifestPath = join(repoPath, FEATURE_MAP_MANIFEST_PATH)
    const releaseLock = await acquireManifestLock(manifestPath, deadline)
    try {
      const existingRaw = await readExistingManifest(manifestPath, limits.maxManifestBytes, deadline)
      const existing = existingRaw === undefined ? undefined : parseFeatureMapManifest(existingRaw)
      const warnings: string[] = []
      const staleLocations = existing
        ? await findStaleLocations(repoPath, existing.features, limits.maxStaleChecks, warnings, deadline)
        : []

      const touchedFiles = await resolveTouchedFiles(
        repoPath,
        input.touchedFileGlobs,
        limits,
        warnings,
        deadline,
      )
      const coveredLocations = new Set(
        existing?.features.flatMap((feature) => splitLocations(feature.location).map(normalizeRelativePath)) ?? [],
      )
      const uncovered = touchedFiles.filter((candidate) => !coveredLocations.has(candidate.location))
      const existingFeatureCount = existing?.features.length ?? 0
      const remainingCapacity = Math.max(0, limits.maxManifestFeatures - existingFeatureCount)
      const additionLimit = Math.min(limits.maxNewEntries, remainingCapacity)
      if (uncovered.length > additionLimit) {
        warnings.push(
          `Feature map growth capped at ${additionLimit} new entries; ${uncovered.length - additionLimit} touched files were deferred.`,
        )
      }
      if (remainingCapacity === 0 && uncovered.length > 0) {
        warnings.push(`Feature map already contains the configured maximum of ${limits.maxManifestFeatures} features.`)
      }

      const existingIds = new Set(existing?.features.map((feature) => feature.id) ?? [])
      const additions: FeatureMapFeature[] = []
      for (const candidate of uncovered.slice(0, additionLimit)) {
        deadline.check()
        additions.push(await inferFeature(candidate, existingIds, limits.maxSourceBytes, warnings, deadline))
      }

      if (additions.length === 0) {
        reportWarnings(input.options.logger, staleLocations, warnings)
        return {
          ok: true,
          status: 'unchanged',
          manifestPath,
          added: [],
          staleLocations,
          touchedFiles: touchedFiles.map((file) => file.location),
          warnings,
        }
      }

      const updated = formatIsoDate(input.options.now?.() ?? new Date())
      const nextRaw = existingRaw === undefined
        ? renderBootstrapManifest(additions, updated)
        : extendManifest(existingRaw, existing as ParsedFeatureMapManifest, additions, updated)
      parseFeatureMapManifest(nextRaw)
      await persistManifest(manifestPath, nextRaw, deadline)
      reportWarnings(input.options.logger, staleLocations, warnings)
      return {
        ok: true,
        status: existingRaw === undefined ? 'created' : 'updated',
        manifestPath,
        added: additions,
        staleLocations,
        touchedFiles: touchedFiles.map((file) => file.location),
        warnings,
      }
    } finally {
      await releaseLock().catch((error: unknown) => {
        safeWarn(input.options.logger, 'Could not release the feature map generation lock.', {
          error: errorMessage(error),
          manifestPath,
        })
      })
    }
  } catch (error) {
    const timedOut = error instanceof GenerationTimeoutError
    const message = errorMessage(error)
    safeWarn(
      input.options.logger,
      timedOut ? 'Feature map generation timed out; dispatch may continue.' : 'Feature map generation failed; dispatch may continue.',
      { error: message, repoPath: input.repoPath },
    )
    return {
      ok: false,
      status: timedOut ? 'timed-out' : 'failed',
      ...baseResult,
      error: message,
    }
  }
}

async function acquireManifestLock(manifestPath: string, deadline: Deadline): Promise<() => Promise<void>> {
  await deadline.run(() => mkdir(dirname(manifestPath), { recursive: true }))
  while (true) {
    deadline.check()
    try {
      const release = await lockfile.lock(manifestPath, {
        realpath: false,
        stale: MANIFEST_LOCK_STALE_MS,
        update: MANIFEST_LOCK_STALE_MS / 2,
        retries: 0,
      })
      try {
        deadline.check()
        return release
      } catch (error) {
        await release().catch(() => undefined)
        throw error
      }
    } catch (error) {
      if (!isLockUnavailableError(error)) throw error
      await deadline.pause(MANIFEST_LOCK_RETRY_MS)
    }
  }
}

/** Parse through the shared validator consumed by the feature guardian and CI. */
export function parseFeatureMapManifest(raw: string): ParsedFeatureMapManifest {
  const validation = validateFeatureManifest(raw)
  return {
    categoryIds: validation.categoryIds,
    features: validation.features.map(toFeatureMapFeature),
  }
}

function toFeatureMapFeature(feature: ManifestFeature): FeatureMapFeature {
  return {
    id: feature.id,
    name: feature.name,
    ...(feature.cli ? { cli: feature.cli } : {}),
    ...(feature.api ? { api: feature.api } : {}),
    description: feature.desc,
    location: feature.location,
    verifyTier: feature.tier as FeatureMapVerifyTier,
    criticality: feature.criticality,
  }
}

function normalizeInput(
  inputOrRepoPath: FeatureMapGenerationInput | string,
  positionalGlobs: readonly string[],
  positionalOptions: FeatureMapGenerationOptions,
): ResolvedInput {
  if (typeof inputOrRepoPath === 'string') {
    return { repoPath: inputOrRepoPath, touchedFileGlobs: positionalGlobs, options: positionalOptions }
  }
  const { repoPath, touchedFileGlobs, ...options } = inputOrRepoPath
  return { repoPath, touchedFileGlobs, options }
}

function generationLimits(options: FeatureMapGenerationOptions): GenerationLimits {
  return {
    timeoutMs: boundedInteger(options.timeoutMs, DEFAULT_TIMEOUT_MS, 0),
    maxNewEntries: boundedInteger(options.maxNewEntries, DEFAULT_MAX_NEW_ENTRIES, 0),
    maxManifestFeatures: boundedInteger(options.maxManifestFeatures, DEFAULT_MAX_MANIFEST_FEATURES, 0),
    maxTouchedFiles: boundedInteger(options.maxTouchedFiles, DEFAULT_MAX_TOUCHED_FILES, 0),
    maxScannedPaths: boundedInteger(options.maxScannedPaths, DEFAULT_MAX_SCANNED_PATHS, 0),
    maxSourceBytes: boundedInteger(options.maxSourceBytes, DEFAULT_MAX_SOURCE_BYTES, 0),
    maxManifestBytes: boundedInteger(options.maxManifestBytes, DEFAULT_MAX_MANIFEST_BYTES, 1),
    maxStaleChecks: boundedInteger(options.maxStaleChecks, DEFAULT_MAX_STALE_CHECKS, 0),
  }
}

function boundedInteger(value: number | undefined, fallback: number, minimum: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value < minimum ? fallback : value
}

async function readExistingManifest(
  manifestPath: string,
  maxBytes: number,
  deadline: Deadline,
): Promise<string | undefined> {
  try {
    const metadata = await deadline.run(() => stat(manifestPath))
    if (metadata.size > maxBytes) {
      throw new Error(`Existing feature manifest exceeds the ${maxBytes}-byte read budget`)
    }
    return await deadline.run((signal) => readFile(manifestPath, { encoding: 'utf8', signal }))
  } catch (error) {
    if (isMissingFileError(error)) return undefined
    throw error
  }
}

async function findStaleLocations(
  repoPath: string,
  features: FeatureMapFeature[],
  maxChecks: number,
  warnings: string[],
  deadline: Deadline,
): Promise<StaleFeatureLocation[]> {
  const stale: StaleFeatureLocation[] = []
  const locations = features.flatMap((feature) => splitLocations(feature.location).map((location) => ({
    featureId: feature.id,
    location,
  })))
  if (locations.length > maxChecks) {
    warnings.push(`Stale-location checks capped at ${maxChecks}; ${locations.length - maxChecks} locations were not checked.`)
  }

  for (const entry of locations.slice(0, maxChecks)) {
    deadline.check()
    const absolutePath = resolve(repoPath, entry.location)
    if (!pathIsWithin(repoPath, absolutePath)) {
      stale.push({ ...entry, reason: 'outside-repo' })
      continue
    }
    try {
      await deadline.run(() => stat(absolutePath))
    } catch (error) {
      if (isMissingFileError(error)) stale.push({ ...entry, reason: 'missing' })
      else warnings.push(`Could not check feature location ${entry.location}: ${errorMessage(error)}`)
    }
  }
  return stale
}

async function resolveTouchedFiles(
  repoPath: string,
  rawPatterns: readonly string[],
  limits: GenerationLimits,
  warnings: string[],
  deadline: Deadline,
): Promise<CandidateFile[]> {
  const patterns = rawPatterns
    .map((pattern) => normalizeTouchedPattern(repoPath, pattern))
    .filter((pattern): pattern is string => pattern !== undefined)
  const negativeMatchers = patterns
    .filter((pattern) => pattern.startsWith('!'))
    .flatMap((pattern) => compileGlob(pattern.slice(1)))
  const positivePatterns = patterns.filter((pattern) => !pattern.startsWith('!'))
  const matches = new Map<string, CandidateFile>()
  const directoryCache = new Map<string, string[]>()
  const scanBudget = { paths: 0, capped: false }
  const traversalDepths = new Map<string, number>()

  for (const pattern of positivePatterns) {
    if (!hasGlobMagic(pattern)) continue
    const base = globStaticBase(pattern)
    const depth = globTraversalDepth(pattern, base)
    traversalDepths.set(base, Math.max(traversalDepths.get(base) ?? 0, depth))
  }

  for (const pattern of positivePatterns) {
    deadline.check()
    if (matches.size >= limits.maxTouchedFiles) break
    if (!hasGlobMagic(pattern)) {
      const candidate = await exactCandidate(repoPath, pattern, warnings, deadline)
      if (candidate && !negativeMatchers.some((matcher) => matcher.test(candidate.location))) {
        matches.set(candidate.location, candidate)
      }
      continue
    }

    const matchers = compileGlob(pattern)
    const base = globStaticBase(pattern)
    let files = directoryCache.get(base)
    if (!files) {
      files = await walkFiles(
        repoPath,
        base,
        traversalDepths.get(base) ?? Number.POSITIVE_INFINITY,
        limits.maxScannedPaths,
        scanBudget,
        warnings,
        deadline,
      )
      directoryCache.set(base, files)
    }
    for (const location of files) {
      if (!matchers.some((matcher) => matcher.test(location))) continue
      if (negativeMatchers.some((matcher) => matcher.test(location))) continue
      matches.set(location, { absolutePath: join(repoPath, ...location.split('/')), location })
      if (matches.size >= limits.maxTouchedFiles) break
    }
  }

  if (matches.size >= limits.maxTouchedFiles && positivePatterns.length > 0) {
    warnings.push(`Touched-file discovery capped at ${limits.maxTouchedFiles} files.`)
  }
  return [...matches.values()].sort((left, right) => left.location.localeCompare(right.location))
}

function normalizeTouchedPattern(repoPath: string, rawPattern: string): string | undefined {
  const trimmed = rawPattern.trim()
  if (!trimmed) return undefined
  const negative = trimmed.startsWith('!')
  const value = negative ? trimmed.slice(1) : trimmed
  const repoRelative = isAbsolute(value) ? relative(repoPath, value) : value
  const normalized = normalizeRelativePath(repoRelative)
  if (!normalized || normalized === '.' || normalized === '..' || normalized.startsWith('../') || isAbsolute(normalized)) {
    return undefined
  }
  return negative ? `!${normalized}` : normalized
}

async function exactCandidate(
  repoPath: string,
  location: string,
  warnings: string[],
  deadline: Deadline,
): Promise<CandidateFile | undefined> {
  const requestedPath = join(repoPath, ...location.split('/'))
  try {
    const actualPath = await deadline.run(() => realpath(requestedPath))
    if (!pathIsWithin(repoPath, actualPath)) {
      warnings.push(`Ignored touched path outside the target repository: ${location}`)
      return undefined
    }
    const metadata = await deadline.run(() => stat(actualPath))
    if (!metadata.isFile()) {
      warnings.push(`Ignored touched path that is not a file: ${location}`)
      return undefined
    }
    if (normalizeRelativePath(relative(repoPath, actualPath)) === FEATURE_MAP_MANIFEST_PATH) return undefined
    return { absolutePath: actualPath, location: normalizeRelativePath(relative(repoPath, actualPath)) }
  } catch (error) {
    if (isMissingFileError(error)) warnings.push(`Touched file no longer exists: ${location}`)
    else warnings.push(`Could not inspect touched file ${location}: ${errorMessage(error)}`)
    return undefined
  }
}

async function walkFiles(
  repoPath: string,
  base: string,
  maxDepth: number,
  maxScannedPaths: number,
  budget: { paths: number; capped: boolean },
  warnings: string[],
  deadline: Deadline,
): Promise<string[]> {
  const requestedBase = join(repoPath, ...base.split('/'))
  let actualBase: string
  try {
    actualBase = await deadline.run(() => realpath(requestedBase))
  } catch (error) {
    if (isMissingFileError(error)) warnings.push(`Touched glob base does not exist: ${base}`)
    else warnings.push(`Could not inspect touched glob base ${base}: ${errorMessage(error)}`)
    return []
  }
  if (!pathIsWithin(repoPath, actualBase)) {
    warnings.push(`Ignored touched glob base outside the target repository: ${base}`)
    return []
  }

  const files: string[] = []
  const queue = [{ directory: actualBase, depth: 0 }]
  let truncatedDirectory = false
  while (queue.length > 0 && budget.paths < maxScannedPaths) {
    deadline.check()
    const { directory, depth } = queue.shift() as { directory: string; depth: number }
    let entries
    try {
      entries = await deadline.run(() => readdir(directory, { withFileTypes: true }))
    } catch (error) {
      warnings.push(`Could not read touched glob directory ${normalizeRelativePath(relative(repoPath, directory))}: ${errorMessage(error)}`)
      continue
    }
    entries.sort((left, right) => left.name.localeCompare(right.name))
    for (const entry of entries) {
      if (budget.paths >= maxScannedPaths) {
        truncatedDirectory = true
        break
      }
      budget.paths += 1
      const absolutePath = join(directory, entry.name)
      const location = normalizeRelativePath(relative(repoPath, absolutePath))
      if (location === FEATURE_MAP_MANIFEST_PATH) continue
      if (entry.isDirectory()) {
        if (!SKIPPED_DIRECTORIES.has(entry.name) && depth + 1 < maxDepth) {
          queue.push({ directory: absolutePath, depth: depth + 1 })
        }
      } else if (entry.isFile()) {
        files.push(location)
      }
    }
  }
  if ((queue.length > 0 || truncatedDirectory) && !budget.capped) {
    budget.capped = true
    warnings.push(`Touched-glob traversal capped after inspecting ${maxScannedPaths} paths.`)
  }
  return files
}

function globStaticBase(pattern: string): string {
  const segments = pattern.split('/')
  const firstMagic = segments.findIndex(hasGlobMagic)
  if (firstMagic <= 0) return '.'
  return segments.slice(0, firstMagic).join('/')
}

function globTraversalDepth(pattern: string, base: string): number {
  const baseSegments = base === '.' ? 0 : base.split('/').length
  const remainingSegments = pattern.split('/').slice(baseSegments)
  return remainingSegments.some((segment) => segment.includes('**'))
    ? Number.POSITIVE_INFINITY
    : remainingSegments.length
}

function hasGlobMagic(value: string): boolean {
  return /[*?\[\]{}]/u.test(value)
}

function compileGlob(pattern: string): RegExp[] {
  return expandBraces(pattern).map((expanded) => {
    let source = '^'
    for (let index = 0; index < expanded.length; index += 1) {
      const character = expanded[index]
      if (character === '*') {
        if (expanded[index + 1] === '*') {
          index += 1
          if (expanded[index + 1] === '/') {
            index += 1
            source += '(?:.*/)?'
          } else {
            source += '.*'
          }
        } else {
          source += '[^/]*'
        }
      } else if (character === '?') {
        source += '[^/]'
      } else if (character === '[') {
        const end = expanded.indexOf(']', index + 1)
        if (end === -1) {
          source += '\\['
        } else {
          const rawClass = expanded.slice(index + 1, end)
          const negated = rawClass.startsWith('!') ? `^${rawClass.slice(1)}` : rawClass
          source += `[${negated.replaceAll('\\', '\\\\')}]`
          index = end
        }
      } else {
        source += escapeRegExp(character)
      }
    }
    return new RegExp(`${source}$`, 'u')
  })
}

function expandBraces(pattern: string): string[] {
  const open = pattern.indexOf('{')
  if (open === -1) return [pattern]
  const close = pattern.indexOf('}', open + 1)
  if (close === -1) return [pattern]
  const choices = pattern.slice(open + 1, close).split(',').filter(Boolean)
  if (choices.length < 2 || choices.length > 16) return [pattern]
  return choices.flatMap((choice) => expandBraces(`${pattern.slice(0, open)}${choice}${pattern.slice(close + 1)}`)).slice(0, 32)
}

function escapeRegExp(character: string): string {
  return /[\\^$.*+?()[\]{}|]/u.test(character) ? `\\${character}` : character
}

async function inferFeature(
  candidate: CandidateFile,
  existingIds: Set<string>,
  maxSourceBytes: number,
  warnings: string[],
  deadline: Deadline,
): Promise<FeatureMapFeature> {
  let source = ''
  try {
    const metadata = await deadline.run(() => stat(candidate.absolutePath))
    if (metadata.size <= maxSourceBytes) {
      source = await deadline.run((signal) => readFile(candidate.absolutePath, { encoding: 'utf8', signal }))
      if (source.includes('\0')) source = ''
    } else {
      warnings.push(`Skipped source inspection for ${candidate.location}; file exceeds ${maxSourceBytes} bytes.`)
    }
  } catch (error) {
    if (error instanceof GenerationTimeoutError) throw error
    warnings.push(`Could not read ${candidate.location}; using a path-derived feature: ${errorMessage(error)}`)
  }

  const surface = inferSurface(candidate.location, source)
  const surfaceValue = surface.cli ?? surface.api ?? candidate.location
  const baseId = `generated-${slugify(surfaceValue) || slugify(candidate.location) || 'surface'}`
  const fingerprint = createHash('sha256').update(`${candidate.location}\0${surfaceValue}`).digest('hex').slice(0, 8)
  let id = `${baseId}-${fingerprint}`
  let collision = 2
  while (existingIds.has(id)) {
    id = `${baseId}-${fingerprint}-${collision}`
    collision += 1
  }
  existingIds.add(id)

  return {
    id,
    name: surface.name,
    ...(surface.cli ? { cli: surface.cli } : { api: surface.api ?? `module:${candidate.location}` }),
    description: surface.description
      ?? `Represent the public behavior implemented in ${candidate.location}.`,
    location: candidate.location,
    verifyTier: inferVerifyTier(candidate.location, source, surface),
    criticality: 'standard',
  }
}

function inferSurface(location: string, source: string): InferredSurface {
  const cli = firstMatch(source, [
    /\.command\s*\(\s*['"`]([^'"`\r\n]+)['"`]/u,
    /\badd_parser\s*\(\s*['"`]([^'"`\r\n]+)['"`]/u,
  ])
  if (cli) {
    const command = cli.match[1].trim()
    return surfaceWithComment({
      name: `${titleCase(command.replace(/[<[].*$/u, '').trim() || command)} CLI Command`,
      cli: command,
      matchIndex: cli.index,
    }, source, location)
  }

  const route = firstMatch(source, [
    /\b(?:app|router|server)\s*\.\s*(get|post|put|patch|delete|options|head)\s*\(\s*['"`]([^'"`\r\n]+)['"`]/iu,
    /@(?:app|router)\s*\.\s*(get|post|put|patch|delete|options|head)\s*\(\s*['"`]([^'"`\r\n]+)['"`]/iu,
  ])
  if (route) {
    const method = route.match[1].toUpperCase()
    const path = route.match[2]
    return surfaceWithComment({
      name: `${method} ${path}`,
      api: `${method} ${path}`,
      matchIndex: route.index,
    }, source, location)
  }

  const publicApi = firstMatch(source, [
    /\bexport\s+(?:default\s+)?(?:async\s+)?function\s+([A-Za-z_$][\w$]*)/u,
    /\bexport\s+(?:default\s+)?class\s+([A-Za-z_$][\w$]*)/u,
    /\bexport\s+(?:const|let|var)\s+([A-Za-z_$][\w$]*)/u,
    /\bpub\s+(?:async\s+)?fn\s+([A-Za-z_][\w]*)/u,
    /^\s*(?:async\s+)?def\s+([A-Za-z][\w]*)\s*\(/mu,
    /^\s*func\s+([A-Z][\w]*)\s*\(/mu,
    /\bpublic\s+(?:static\s+)?(?:class|interface|[A-Za-z_$][\w$<>?,.[\] ]*)\s+([A-Za-z_$][\w$]*)\s*(?:\(|\{)/u,
    /\bpublic\s+function\s+([A-Za-z_][\w]*)\s*\(/u,
    /\b(?:module\.)?exports\.([A-Za-z_$][\w$]*)\s*=/u,
  ])
  if (publicApi) {
    const identifier = publicApi.match[1]
    const matchedText = publicApi.match[0]
    const callable = /function|\bfn\b|\bdef\b|^\s*func|\($/u.test(matchedText)
    return surfaceWithComment({
      name: titleCase(identifier),
      api: callable ? `${identifier}()` : identifier,
      matchIndex: publicApi.index,
    }, source, location)
  }

  const stem = location.split('/').pop()?.replace(/\.[^.]+$/u, '') ?? location
  return {
    name: titleCase(stem),
    api: `module:${location}`,
    matchIndex: 0,
  }
}

function firstMatch(source: string, expressions: RegExp[]): { match: RegExpExecArray; index: number } | undefined {
  let first: { match: RegExpExecArray; index: number } | undefined
  for (const expression of expressions) {
    const match = expression.exec(source)
    if (match && (!first || match.index < first.index)) first = { match, index: match.index }
  }
  return first
}

function surfaceWithComment(surface: InferredSurface, source: string, location: string): InferredSurface {
  const description = precedingComment(source, surface.matchIndex)
  if (description) return { ...surface, description }
  if (surface.cli) {
    return { ...surface, description: `Expose the ${surface.cli} command defined in ${location}.` }
  }
  if (surface.api?.match(/^(GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD) /u)) {
    return { ...surface, description: `Handle ${surface.api} requests through ${location}.` }
  }
  return { ...surface, description: `Provide the ${surface.api} public API from ${location}.` }
}

function precedingComment(source: string, matchIndex: number): string | undefined {
  const prefix = source.slice(Math.max(0, matchIndex - 1_500), matchIndex)
  const block = /\/\*\*?([\s\S]*?)\*\/\s*(?:@[A-Za-z_$][^\r\n]*\s*)*$/u.exec(prefix)
  if (block) return cleanComment(block[1])
  const lines = /(?:^|\r?\n)((?:(?:[ \t]*(?:\/\/|#)[^\r\n]*)(?:\r?\n|$))+)[ \t]*$/u.exec(prefix)
  return lines ? cleanComment(lines[1]) : undefined
}

function cleanComment(value: string): string | undefined {
  const cleaned = value
    .split(/\r?\n/u)
    .map((line) => line.replace(/^\s*(?:\*|\/\/|#)\s?/u, '').trim())
    .filter((line) => line && !line.startsWith('@'))
    .join(' ')
    .replace(/\s+/gu, ' ')
    .slice(0, 280)
    .trim()
  return cleaned || undefined
}

function inferVerifyTier(
  location: string,
  source: string,
  surface: InferredSurface,
): FeatureMapVerifyTier {
  const signal = `${location}\n${source}`.toLowerCase()
  if (/\bmanual(?:ly)?\b[\s\S]{0,40}\b(?:issue|pull request|pr)\b|\blive (?:issue|pull request|pr)\b/u.test(signal)) return 6
  if (/\bcloud auth\b|\bwritable (?:relayfile )?mount\b|\bcloud credentials?\b/u.test(signal)) return 5
  if (/\bfleet\b|\bbroker\b|\bdaemon\b|\bspawn(?:ing|ed)?\b|\bchild_process\b/u.test(signal)) return 4
  const isHttpRoute = /^(?:GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD)\s/u.test(surface.api ?? '')
  const needsNetworkOrPersistence = /\bfetch\s*\(|\baxios\b|\bhttps?:\/\/|\bnetwork\b|\bdatabase\b|\bdb\b|\bpostgres\b|\bmysql\b|\bsqlite\b|\bredis\b|\boauth\b|\bauth(?:entication)?\b|\bauthori[sz]ation\b|\bcredentials?\b/u.test(signal)
  if (isHttpRoute || needsNetworkOrPersistence) return 3
  if (/\bprocess\.env\b|\bconfig(?:uration)?\b|\bwritefile\b|\breadfile\b|\bfilesystem\b|\bintegration\b/u.test(signal)) return 2
  return 1
}

function renderBootstrapManifest(features: FeatureMapFeature[], updated: string): string {
  const tiers = tierCounts(features)
  return [
    "version: '1.0'",
    `updated: ${yamlScalar(updated)}`,
    'catalog:',
    '  category_count: 1',
    `  feature_count: ${features.length}`,
    '  tier_counts:',
    ...[1, 2, 3, 4, 5, 6].map((tier) => `    ${tier}: ${tiers[tier]}`),
    '',
    'categories:',
    `  ${GENERATED_CATEGORY_ID}:`,
    '    name: Generated Touched Surfaces',
    '    description: Public surfaces discovered incrementally from files touched by dispatched work',
    '    criticality: standard',
    '    features:',
    renderFeatureEntries(features, '\n').trimEnd(),
    '',
  ].join('\n')
}

function extendManifest(
  raw: string,
  parsed: ParsedFeatureMapManifest,
  additions: FeatureMapFeature[],
  updated: string,
): string {
  const eol = raw.includes('\r\n') ? '\r\n' : '\n'
  const categoriesStart = /^categories:\s*$/mu.exec(raw)
  if (!categoriesStart) throw new Error('Feature manifest is missing categories')
  const categoriesOffset = categoriesStart.index + categoriesStart[0].length
  const trailingRootKey = /^[^\s#][^:\r\n]*:\s*/gmu.exec(raw.slice(categoriesOffset))
  const categoriesEnd = trailingRootKey
    ? categoriesOffset + trailingRootKey.index
    : raw.length
  const categoryExpression = /^  (?:(?:"([^"]+)"|'([^']+)'|([^\s"':][^:\r\n]*))):\s*(?:#.*)?$/gmu
  const matches = [...raw.slice(categoriesOffset, categoriesEnd).matchAll(categoryExpression)]
    .map((match) => ({
      id: (match[1] ?? match[2] ?? match[3]).trim(),
      index: categoriesOffset + (match.index ?? 0),
    }))
  const generatedIndex = matches.findIndex((match) => match.id === GENERATED_CATEGORY_ID)
  let next = raw
  let addedCategory = false

  if (generatedIndex >= 0) {
    const category = matches[generatedIndex]
    const end = matches[generatedIndex + 1]?.index ?? categoriesEnd
    const section = raw.slice(category.index, end)
    if (!/^    features:\s*$/mu.test(section)) {
      throw new Error(`Generated category ${GENERATED_CATEGORY_ID} is missing features`)
    }
    const insertion = renderFeatureEntries(additions, eol)
    const prefix = raw.slice(0, end)
    next = `${prefix}${prefix.endsWith(eol) ? '' : eol}${insertion}${raw.slice(end)}`
  } else {
    if (parsed.categoryIds.includes(GENERATED_CATEGORY_ID)) {
      throw new Error(`Could not safely locate generated category ${GENERATED_CATEGORY_ID}`)
    }
    addedCategory = true
    const block = [
      `  ${GENERATED_CATEGORY_ID}:`,
      '    name: Generated Touched Surfaces',
      '    description: Public surfaces discovered incrementally from files touched by dispatched work',
      '    criticality: standard',
      '    features:',
      renderFeatureEntries(additions, eol).trimEnd(),
      '',
    ].join(eol)
    const prefix = raw.slice(0, categoriesEnd)
    next = `${prefix}${prefix.endsWith(eol) ? '' : eol}${block}${raw.slice(categoriesEnd)}`
  }

  const allFeatures = [...parsed.features, ...additions]
  const tiers = tierCounts(allFeatures)
  next = replaceRequired(next, /^updated:\s*.*$/mu, `updated: ${yamlScalar(updated)}`, 'updated')
  next = replaceRequired(
    next,
    /^  category_count:\s*\d+\s*$/mu,
    `  category_count: ${parsed.categoryIds.length + (addedCategory ? 1 : 0)}`,
    'category_count',
  )
  next = replaceRequired(next, /^  feature_count:\s*\d+\s*$/mu, `  feature_count: ${allFeatures.length}`, 'feature_count')
  for (let tier = 1; tier <= 6; tier += 1) {
    next = replaceRequired(next, new RegExp(`^    ${tier}:\\s*\\d+\\s*$`, 'mu'), `    ${tier}: ${tiers[tier]}`, `tier ${tier}`)
  }
  return next
}

function renderFeatureEntries(features: FeatureMapFeature[], eol: string): string {
  return features.map((feature) => [
    `      - id: ${yamlScalar(feature.id)}`,
    `        name: ${yamlScalar(feature.name)}`,
    ...(feature.cli ? [`        cli: ${yamlScalar(feature.cli)}`] : []),
    ...(feature.api ? [`        api: ${yamlScalar(feature.api)}`] : []),
    `        description: ${yamlScalar(feature.description)}`,
    `        location: ${yamlScalar(feature.location)}`,
    `        verify_tier: ${feature.verifyTier}`,
    '',
  ].join(eol)).join('')
}

async function persistManifest(manifestPath: string, content: string, deadline: Deadline): Promise<void> {
  await deadline.run(() => mkdir(dirname(manifestPath), { recursive: true }))
  const temporaryPath = `${manifestPath}.${process.pid}.${randomUUID()}.tmp`
  try {
    await deadline.run((signal) => writeFile(temporaryPath, content, { encoding: 'utf8', flag: 'wx', signal }))
    deadline.check()
    await deadline.run(() => rename(temporaryPath, manifestPath))
  } finally {
    await rm(temporaryPath, { force: true }).catch(() => undefined)
  }
}

function replaceRequired(raw: string, expression: RegExp, replacement: string, label: string): string {
  if (!expression.test(raw)) throw new Error(`Feature manifest is missing ${label}`)
  expression.lastIndex = 0
  return raw.replace(expression, replacement)
}

function tierCounts(features: FeatureMapFeature[]): Record<number, number> {
  return Object.fromEntries([1, 2, 3, 4, 5, 6].map((tier) => [
    tier,
    features.filter((feature) => feature.verifyTier === tier).length,
  ]))
}

function yamlScalar(value: string): string {
  return JSON.stringify(value)
}

function splitLocations(value: string): string[] {
  return value.split(',').map((location) => location.trim()).filter(Boolean)
}

function normalizeRelativePath(value: string): string {
  const normalized = value.split(sep).join('/').replace(/^\.\//u, '')
  return normalized || '.'
}

function pathIsWithin(root: string, candidate: string): boolean {
  const child = relative(root, candidate)
  return child === '' || (!child.startsWith(`..${sep}`) && child !== '..' && !isAbsolute(child))
}

function slugify(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/gu, '$1-$2')
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-+|-+$/gu, '')
    .slice(0, 64)
}

function titleCase(value: string): string {
  const words = value
    .replace(/([a-z0-9])([A-Z])/gu, '$1 $2')
    .replace(/[_./:-]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim()
  return words ? words.replace(/\b\w/gu, (character) => character.toUpperCase()) : 'Touched Surface'
}

function formatIsoDate(value: Date): string {
  if (Number.isNaN(value.getTime())) throw new Error('Feature map generation received an invalid date')
  return value.toISOString().slice(0, 10)
}

function reportWarnings(
  logger: Pick<Logger, 'warn'> | undefined,
  staleLocations: StaleFeatureLocation[],
  warnings: string[],
): void {
  if (staleLocations.length > 0) {
    safeWarn(logger, 'Feature map contains stale locations; entries were preserved for drift review.', { staleLocations })
  }
  for (const warning of warnings) safeWarn(logger, warning)
}

function safeWarn(logger: Pick<Logger, 'warn'> | undefined, message: string, metadata?: unknown): void {
  try {
    logger?.warn?.(message, ...(metadata === undefined ? [] : [metadata]))
  } catch {
    // Diagnostics must never turn optional feature-map generation into a dispatch failure.
  }
}

function isMissingFileError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT'
}

function isLockUnavailableError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ELOCKED'
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
