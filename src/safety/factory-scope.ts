import type { FactoryConfig } from '../config/schema'
import type { LinearIssue } from '../types'

export interface FactoryScopeSafety {
  requireTitlePrefix?: string
  requireLabel?: string
  requireTeamKey?: string
}

export interface NormalizedFactoryScopeSafety {
  titlePrefix: string
  label?: string
  teamKey: string
}

export function isInFactoryScope(
  issue: Pick<LinearIssue, 'title' | 'team' | 'raw'> & Partial<Pick<LinearIssue, 'labels'>>,
  safety: FactoryScopeSafety = {},
): boolean {
  const expected = normalizeSafety(safety)
  const payload = wrappedPayload(issue.raw)
  const title = stringValue(payload.title) ?? issue.title
  if (!hasAcceptedFactoryMarker(issue, payload, title, expected)) {
    return false
  }

  const team = asRecord(payload.team)
  if (!team) {
    return issue.team ? issue.team === expected.teamKey : true
  }

  return stringValue(team.key) === expected.teamKey
}

export function assertInFactoryScope(
  issue: Pick<LinearIssue, 'key' | 'title' | 'team' | 'raw'> & Partial<Pick<LinearIssue, 'labels'>>,
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
  issue: Pick<LinearIssue, 'title' | 'team' | 'raw'> & Partial<Pick<LinearIssue, 'labels'>>,
  safety: FactoryScopeSafety = {},
): string | undefined {
  const expected = normalizeSafety(safety)
  const payload = wrappedPayload(issue.raw)
  const title = stringValue(payload.title) ?? issue.title
  if (!hasAcceptedFactoryMarker(issue, payload, title, expected)) {
    return expected.label
      ? `title must start with ${expected.titlePrefix} boundary or labels must include ${expected.label}`
      : `title must start with ${expected.titlePrefix} boundary`
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

const normalizeSafety = (safety: FactoryScopeSafety = {}): NormalizedFactoryScopeSafety => {
  const label = normalizeRequiredLabel(safety.requireLabel)
  return {
    titlePrefix: safety.requireTitlePrefix || '[factory-e2e]',
    ...(label ? { label } : {}),
    teamKey: safety.requireTeamKey || 'AR',
  }
}

const normalizeRequiredLabel = (label: string | undefined): string | undefined => {
  if (label === undefined) return DEFAULT_FACTORY_LABEL
  const normalized = label.trim().toLowerCase()
  return normalized || undefined
}

const hasAcceptedFactoryMarker = (
  issue: Partial<Pick<LinearIssue, 'labels'>>,
  payload: Record<string, unknown>,
  title: string,
  expected: NormalizedFactoryScopeSafety,
): boolean =>
  titleHasAcceptedFactoryMarker(title, expected.titlePrefix, isGithubMirrorPayload(payload)) ||
  payloadHasFactoryLabel(issue, payload, expected.label)

const payloadHasFactoryLabel = (
  issue: Partial<Pick<LinearIssue, 'labels'>>,
  payload: Record<string, unknown>,
  expectedLabel: string | undefined,
): boolean => {
  if (!expectedLabel) return false
  const labels = [
    ...issueLabels(issue.labels),
    ...payloadLabels(payload.labels),
  ]
  return labels.some((label) => label.trim().toLowerCase() === expectedLabel)
}

const issueLabels = (labels: unknown): string[] =>
  Array.isArray(labels) ? labels.filter((label): label is string => typeof label === 'string') : []

const payloadLabels = (labels: unknown): string[] => {
  if (Array.isArray(labels)) {
    return labels.map(labelName).filter((label): label is string => Boolean(label))
  }
  const record = asRecord(labels)
  if (Array.isArray(record?.nodes)) {
    return record.nodes.map(labelName).filter((label): label is string => Boolean(label))
  }
  if (Array.isArray(record?.edges)) {
    return record.edges
      .map((edge) => labelName(asRecord(edge)?.node))
      .filter((label): label is string => Boolean(label))
  }
  return []
}

const labelName = (value: unknown): string | undefined => {
  if (typeof value === 'string') return value
  const record = asRecord(value)
  return stringValue(record?.name)
}

const titleHasFactoryMarker = (title: string, marker: string): boolean =>
  title === marker || title.startsWith(`${marker} `)

// Accept the configured prefix always. The bare `[factory]` mirror marker is
// only honored for GitHub mirror issues, so a stricter custom prefix (e.g.
// `[factory-e2e]`) still rejects a human-authored issue merely titled
// `[factory] ...`.
const titleHasAcceptedFactoryMarker = (
  title: string,
  configuredMarker: string,
  isGithubMirror: boolean,
): boolean =>
  titleHasFactoryMarker(title, configuredMarker) ||
  (isGithubMirror && titleHasFactoryMarker(title, GITHUB_MIRROR_TITLE_PREFIX))

// Mirror drafts created from GitHub issues carry source.provider === 'github'.
const isGithubMirrorPayload = (payload: Record<string, unknown>): boolean =>
  stringValue(asRecord(payload.source)?.provider)?.toLowerCase() === 'github'

const GITHUB_MIRROR_TITLE_PREFIX = '[factory]'
const DEFAULT_FACTORY_LABEL = 'factory'

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
