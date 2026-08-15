import { readFile, stat } from 'node:fs/promises'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const FACTORY_PACKAGE_NAME = '@agent-relay/factory'
const FACTORY_REGISTRY_URL = 'https://registry.npmjs.org/@agent-relay%2Ffactory'
const DEFAULT_REGISTRY_TIMEOUT_MS = 1_500

export interface FactoryVersionInfo {
  version: string
  installedAt?: string
  latestVersion?: string
  versionsBehind?: number
}

export interface FactoryVersionInfoOptions {
  includeRegistry?: boolean
  fetch?: typeof fetch
  registryUrl?: string
  registryTimeoutMs?: number
}

interface SemanticVersion {
  major: number
  minor: number
  patch: number
  prerelease: string[]
}

interface RegistryMetadata {
  'dist-tags'?: { latest?: unknown }
  versions?: Record<string, unknown>
}

const factoryManifestUrl = new URL('../package.json', import.meta.url)

/**
 * Read the identity of the artifact executing this code. Registry lookup is
 * optional so daemon startup can log its local version without network I/O.
 */
export async function readFactoryVersionInfo(
  options: FactoryVersionInfoOptions = {},
): Promise<FactoryVersionInfo> {
  const manifest = JSON.parse(await readFile(factoryManifestUrl, 'utf8')) as {
    name?: unknown
    version?: unknown
  }
  if (manifest.name !== FACTORY_PACKAGE_NAME || typeof manifest.version !== 'string' || manifest.version.length === 0) {
    throw new Error('Factory package identity is missing')
  }

  const packageRoot = dirname(fileURLToPath(factoryManifestUrl))
  const packageStats = await stat(packageRoot)
  // npm does not persist an installation timestamp in package metadata. The
  // package directory's newest filesystem timestamp is the best local artifact
  // signal and also handles package managers that update a directory in place.
  const installedAtMs = Math.max(packageStats.birthtimeMs, packageStats.mtimeMs)
  const installed: FactoryVersionInfo = {
    version: manifest.version,
    installedAt: new Date(installedAtMs).toISOString(),
  }
  if (options.includeRegistry === false) return installed

  const published = await readPublishedVersions(options)
  if (!published) return installed
  return {
    ...installed,
    latestVersion: published.latestVersion,
    versionsBehind: countVersionsBehind(
      installed.version,
      published.latestVersion,
      published.versions,
    ),
  }
}

/** A meaningful drift threshold avoids warning operators for every patch. */
export function isMeaningfullyBehind(info: FactoryVersionInfo): boolean {
  if (!info.latestVersion) return false
  const running = parseSemanticVersion(info.version)
  const latest = parseSemanticVersion(info.latestVersion)
  if (!running || !latest || compareSemanticVersions(running, latest) >= 0) return false
  if (running.major !== latest.major || running.minor !== latest.minor) return true
  return (info.versionsBehind ?? latest.patch - running.patch) >= 3
}

export function countVersionsBehind(
  runningVersion: string,
  latestVersion: string,
  publishedVersions: readonly string[],
): number | undefined {
  const running = parseSemanticVersion(runningVersion)
  const latest = parseSemanticVersion(latestVersion)
  if (!running || !latest) return undefined
  if (compareSemanticVersions(running, latest) >= 0) return 0
  if (publishedVersions.length === 0) return undefined

  const newerStableVersions = new Set<string>()
  for (const candidateText of publishedVersions) {
    const candidate = parseSemanticVersion(candidateText)
    if (!candidate || candidate.prerelease.length > 0) continue
    if (compareSemanticVersions(candidate, running) <= 0) continue
    if (compareSemanticVersions(candidate, latest) > 0) continue
    newerStableVersions.add(`${candidate.major}.${candidate.minor}.${candidate.patch}`)
  }
  return newerStableVersions.size
}

async function readPublishedVersions(
  options: FactoryVersionInfoOptions,
): Promise<{ latestVersion: string; versions: string[] } | undefined> {
  const fetchImpl = options.fetch ?? globalThis.fetch
  if (!fetchImpl) return undefined

  const controller = new AbortController()
  const timeout = setTimeout(
    () => controller.abort(),
    options.registryTimeoutMs ?? DEFAULT_REGISTRY_TIMEOUT_MS,
  )
  timeout.unref?.()
  try {
    const response = await fetchImpl(options.registryUrl ?? FACTORY_REGISTRY_URL, {
      headers: { accept: 'application/vnd.npm.install-v1+json' },
      signal: controller.signal,
    })
    if (!response.ok) return undefined
    const metadata = await response.json() as RegistryMetadata
    const latestVersion = metadata['dist-tags']?.latest
    if (typeof latestVersion !== 'string' || !parseSemanticVersion(latestVersion)) return undefined
    return {
      latestVersion,
      versions: Object.keys(metadata.versions ?? {}),
    }
  } catch {
    // Version discovery is observability only. Offline and slow registries do
    // not fail a command or produce repetitive operator-facing errors.
    return undefined
  } finally {
    clearTimeout(timeout)
  }
}

function parseSemanticVersion(value: string): SemanticVersion | undefined {
  const match = value.match(/^(?:v)?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/u)
  if (!match) return undefined
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4]?.split('.') ?? [],
  }
}

function compareSemanticVersions(left: SemanticVersion, right: SemanticVersion): number {
  for (const part of ['major', 'minor', 'patch'] as const) {
    if (left[part] !== right[part]) return left[part] - right[part]
  }
  if (left.prerelease.length === 0) return right.prerelease.length === 0 ? 0 : 1
  if (right.prerelease.length === 0) return -1
  const length = Math.max(left.prerelease.length, right.prerelease.length)
  for (let index = 0; index < length; index += 1) {
    const leftPart = left.prerelease[index]
    const rightPart = right.prerelease[index]
    if (leftPart === undefined) return -1
    if (rightPart === undefined) return 1
    if (leftPart === rightPart) continue
    const leftNumber = /^\d+$/u.test(leftPart) ? Number(leftPart) : undefined
    const rightNumber = /^\d+$/u.test(rightPart) ? Number(rightPart) : undefined
    if (leftNumber !== undefined && rightNumber !== undefined) return leftNumber - rightNumber
    if (leftNumber !== undefined) return -1
    if (rightNumber !== undefined) return 1
    return leftPart.localeCompare(rightPart)
  }
  return 0
}
