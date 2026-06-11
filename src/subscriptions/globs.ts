import { dedupeStrings } from './common'

// ported from src/main/integration-event-bridge.ts @normalizeChangePath
export function normalizeChangePath(path: string): string[] {
  const normalized = path.startsWith('/') ? path : `/${path}`
  const trimmed = normalized.replace(/\/+$/u, '')
  return trimmed === '' ? [] : trimmed.split('/').filter(Boolean)
}

// ported from src/main/integration-event-bridge.ts @globSegmentMatches
export function globSegmentMatches(pattern: string, segment: string | undefined): boolean {
  if (segment === undefined) return false
  if (pattern === '*') return true
  if (!pattern.includes('*')) return pattern === segment
  const escaped = pattern
    .split('*')
    .map((part) => part.replace(/[|\\{}()[\]^$+?.]/g, '\\$&'))
    .join('.*')
  return new RegExp(`^${escaped}$`, 'u').test(segment)
}

// ported from src/main/integration-event-bridge.ts @globMatchesPath
export function globMatchesPath(glob: string, path: string): boolean {
  const pattern = normalizeChangePath(glob)
  const target = normalizeChangePath(path)
  if (pattern.at(-1) === '**') {
    const prefix = pattern.slice(0, -1)
    return target.length >= prefix.length &&
      prefix.every((segment, index) => globSegmentMatches(segment, target[index]))
  }
  return pattern.length === target.length &&
    pattern.every((segment, index) => globSegmentMatches(segment, target[index]))
}

// ported from src/main/integration-event-bridge.ts @relayfileSdkPathFiltersFor
export function relayfileSdkPathFiltersFor(globs: string[]): string[] {
  return dedupeStrings(globs.map((glob) => {
    const segments = normalizeChangePath(glob)
    if (segments.length === 0) return '/'
    const sdkSegments = segments.map((segment, index) => {
      if (segment === '*') return segment
      if (segment === '**') return index === segments.length - 1 ? segment : '*'
      return segment.includes('*') ? '*' : segment
    })
    return `/${sdkSegments.join('/')}`
  }))
}
