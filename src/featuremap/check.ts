import { execFile } from 'node:child_process'
import { isAbsolute, relative, resolve } from 'node:path'
import { promisify } from 'node:util'

import {
  FEATURE_MAP_MANIFEST_PATH,
  findFeatureLocationDrift,
  parseManifestFeatures,
  validateFeatureManifestFile,
  type FeatureLocationDriftAdvisory,
} from './validate'

const execFileAsync = promisify(execFile)

export interface CheckFeatureMapOptions {
  rootDir?: string
  manifestPath?: string
  /** Git revision used to compute the PR diff and load the base manifest. */
  baseRef?: string
}

export interface FeatureMapCheckReport {
  ok: true
  manifestPath: string
  categoryCount: number
  featureCount: number
  baseRef?: string
  mergeBase?: string
  advisories: FeatureLocationDriftAdvisory[]
}

/** Validate a checkout and, when requested, produce advisory PR drift flags. */
export async function checkFeatureMap(
  options: CheckFeatureMapOptions = {},
): Promise<FeatureMapCheckReport> {
  const rootDir = resolve(options.rootDir ?? process.cwd())
  const requestedManifestPath = options.manifestPath ?? FEATURE_MAP_MANIFEST_PATH
  const absoluteManifestPath = isAbsolute(requestedManifestPath)
    ? requestedManifestPath
    : resolve(rootDir, requestedManifestPath)
  const manifestPath = toRepositoryPath(relative(rootDir, absoluteManifestPath))
  if (manifestPath === '..' || manifestPath.startsWith('../') || isAbsolute(manifestPath)) {
    throw new Error(`Feature manifest must be inside the repository root: ${absoluteManifestPath}`)
  }

  const validation = validateFeatureManifestFile({
    rootDir,
    manifestPath: absoluteManifestPath,
  })
  let advisories: FeatureLocationDriftAdvisory[] = []
  let mergeBase: string | undefined

  if (options.baseRef) {
    mergeBase = (await git(rootDir, ['merge-base', options.baseRef, 'HEAD'])).trim()
    const changedPaths = (await git(rootDir, [
      'diff',
      '--name-only',
      '--diff-filter=ACMRTUXB',
      mergeBase,
      '--',
    ])).split(/\r?\n/u).map((value) => value.trim()).filter(Boolean)
    const baseManifest = await gitFileAtRevision(rootDir, mergeBase, manifestPath)
    if (baseManifest !== undefined) {
      advisories = findFeatureLocationDrift(
        parseManifestFeatures(baseManifest),
        validation.features,
        changedPaths,
      )
    }
  }

  return {
    ok: true,
    manifestPath,
    categoryCount: validation.categoryCount,
    featureCount: validation.features.length,
    ...(options.baseRef ? { baseRef: options.baseRef } : {}),
    ...(mergeBase ? { mergeBase } : {}),
    advisories,
  }
}

async function git(rootDir: string, args: string[]): Promise<string> {
  const { stdout } = await execFileAsync('git', ['-C', rootDir, ...args], {
    encoding: 'utf8',
    maxBuffer: 10 * 1024 * 1024,
  })
  return stdout
}

async function gitFileAtRevision(
  rootDir: string,
  revision: string,
  path: string,
): Promise<string | undefined> {
  try {
    return await git(rootDir, ['show', `${revision}:${path}`])
  } catch (error) {
    const stderr = typeof error === 'object' && error !== null && 'stderr' in error
      ? String(error.stderr)
      : ''
    if (/does not exist in|exists on disk, but not in|path .* not in/iu.test(stderr)) return undefined
    throw error
  }
}

function toRepositoryPath(path: string): string {
  return path.replaceAll('\\', '/')
}
