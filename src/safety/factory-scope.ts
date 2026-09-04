import type { FactoryConfig } from '../config/schema'
import type { LinearIssue } from '../types'
import {
  GARDEN_AUTOMATION_LABEL,
  GARDEN_E2E_TITLE_PREFIX,
  GARDEN_TITLE_PREFIX,
  gardenLabelAliases,
  hasGardenTitlePrefix,
} from '../constants/lifecycle-labels'

export interface FactoryScopeSafety {
  requireTitlePrefix?: string | null
  requireLabel?: string
  requireTeamKey?: string
}

export interface NormalizedFactoryScopeSafety {
  titlePrefix: string | null
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
    const titleFailure = expected.titlePrefix
      ? `title must start with ${expected.titlePrefix} boundary`
      : undefined
    if (titleFailure && expected.label) return `${titleFailure} or labels must include ${expected.label}`
    if (expected.label) return `labels must include ${expected.label}`
    return titleFailure ?? 'no title prefix or label safety gate is configured'
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
  // Mirrors the config-schema default after the Software Garden rename. The
  // legacy `[factory-e2e]` prefix stays acceptable at the title check below.
  const titlePrefix = safety.requireTitlePrefix === null
    ? null
    : safety.requireTitlePrefix || GARDEN_E2E_TITLE_PREFIX
  return {
    titlePrefix,
    ...(label ? { label } : {}),
    teamKey: safety.requireTeamKey || 'AR',
  }
}

const normalizeRequiredLabel = (label: string | undefined): string | undefined => {
  if (label === undefined) return GARDEN_AUTOMATION_LABEL
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
  // Rename transition: a `garden` (or `garden-ready`) requirement also admits
  // the legacy `factory`/`factory-ready` spelling, so an in-flight issue
  // labeled by an older build stays inside the safety scope.
  const aliases = new Set(gardenLabelAliases(expectedLabel))
  return labels.some((label) => aliases.has(label.trim().toLowerCase()))
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

// Accept the configured prefix always (and its legacy rename alias — a
// `[garden-e2e]` requirement still admits `[factory-e2e]`, and vice versa, so
// in-flight issues titled before the rename stay in scope). The bare mirror
// markers (`[garden]` and its legacy `[factory]` spelling) are only honored
// for GitHub mirror issues, so a stricter custom prefix still rejects a
// human-authored issue merely titled `[garden] ...`.
const titleHasAcceptedFactoryMarker = (
  title: string,
  configuredMarker: string | null,
  isGithubMirror: boolean,
): boolean =>
  (configuredMarker ? hasGardenTitlePrefix(title, configuredMarker) : false) ||
  // hasGardenTitlePrefix on the mirror prefix admits both spellings.
  (isGithubMirror && hasGardenTitlePrefix(title, GARDEN_TITLE_PREFIX))

// Mirror drafts created from GitHub issues carry source.provider === 'github'.
const isGithubMirrorPayload = (payload: Record<string, unknown>): boolean =>
  stringValue(asRecord(payload.source)?.provider)?.toLowerCase() === 'github'

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
