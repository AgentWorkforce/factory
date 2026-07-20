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

  // validateFeatureManifestFile enforces that manifestPath resolves inside rootDir.
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
    timeout: 30_000,
    env: { ...process.env, LC_ALL: 'C' },
  })
  return stdout
}

async function gitFileAtRevision(
  rootDir: string,
  revision: string,
  path: string,
): Promise<string | undefined> {
  if (!(await objectExistsAtRevision(rootDir, revision, path))) return undefined
  return git(rootDir, ['show', `${revision}:${path}`])
}

/** Exit-code probe, avoiding locale/version-fragile stderr matching for a missing object. */
async function objectExistsAtRevision(rootDir: string, revision: string, path: string): Promise<boolean> {
  try {
    await execFileAsync('git', ['-C', rootDir, 'cat-file', '-e', `${revision}:${path}`], {
      encoding: 'utf8',
      timeout: 30_000,
      env: { ...process.env, LC_ALL: 'C' },
    })
    return true
  } catch (error) {
    const exitCode = typeof error === 'object' && error !== null && 'code' in error
      ? error.code
      : undefined
    if (typeof exitCode === 'number') return false
    throw error
  }
}

function toRepositoryPath(path: string): string {
  return path.replaceAll('\\', '/')
}
