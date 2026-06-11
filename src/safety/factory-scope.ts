import type { FactoryConfig } from '../config/schema'
import type { LinearIssue } from '../types'

export interface FactoryScopeSafety {
  requireTitlePrefix?: string
  requireTeamKey?: string
}

export interface NormalizedFactoryScopeSafety {
  titlePrefix: string
  teamKey: string
}

export function isInFactoryScope(
  issue: Pick<LinearIssue, 'title' | 'team' | 'raw'>,
  safety: FactoryScopeSafety = {},
): boolean {
  const expected = normalizeSafety(safety)
  const payload = wrappedPayload(issue.raw)
  const title = stringValue(payload.title) ?? issue.title
  if (!titleHasFactoryMarker(title, expected.titlePrefix)) {
    return false
  }

  const team = asRecord(payload.team)
  if (!team) {
    return issue.team ? issue.team === expected.teamKey : true
  }

  return stringValue(team.key) === expected.teamKey
}

export function assertInFactoryScope(
  issue: Pick<LinearIssue, 'key' | 'title' | 'team' | 'raw'>,
  safety: FactoryScopeSafety = {},
  context = issue.key,
): void {
  const reason = factoryScopeFailureReason(issue, safety)
  if (reason) {
    throw new Error(`Refusing Linear writeback for ${context}: ${reason}`)
  }
}

export function factoryScopeSafety(config: Pick<FactoryConfig, 'safety'>): NormalizedFactoryScopeSafety {
  return normalizeSafety(config.safety)
}

function factoryScopeFailureReason(
  issue: Pick<LinearIssue, 'title' | 'team' | 'raw'>,
  safety: FactoryScopeSafety = {},
): string | undefined {
  const expected = normalizeSafety(safety)
  const payload = wrappedPayload(issue.raw)
  const title = stringValue(payload.title) ?? issue.title
  if (!titleHasFactoryMarker(title, expected.titlePrefix)) {
    return `title must start with ${expected.titlePrefix} boundary`
  }

  const team = asRecord(payload.team)
  if (team && stringValue(team.key) !== expected.teamKey) {
    return `team key must be ${expected.teamKey}`
  }
  if (!team && issue.team && issue.team !== expected.teamKey) {
    return `team key must be ${expected.teamKey}`
  }

  return undefined
}

const normalizeSafety = (safety: FactoryScopeSafety = {}): NormalizedFactoryScopeSafety => ({
  titlePrefix: safety.requireTitlePrefix || '[factory-e2e]',
  teamKey: safety.requireTeamKey || 'AR',
})

const titleHasFactoryMarker = (title: string, marker: string): boolean =>
  title === marker || title.startsWith(`${marker} `)

const wrappedPayload = (value: unknown): Record<string, unknown> => {
  const record = asRecord(value)
  return asRecord(record?.payload) ?? record ?? {}
}

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined
