import type { ChangeEvent, MountClient } from '../ports'
import { isRecord, scopeStringList } from './common'

export type LinearScopePredicates = {
  teams: string[]
  projects: string[]
  labels: string[]
  assignees: string[]
}

export type LinearPredicateSubscriptionSpec = {
  linearPredicates?: LinearScopePredicates
}

// ported from src/main/integration-event-bridge.ts @linearScopePredicates
export function linearScopePredicates(scope: Record<string, unknown>): LinearScopePredicates {
  return {
    teams: scopeStringList(scope, 'teams'),
    projects: scopeStringList(scope, 'projects'),
    labels: scopeStringList(scope, 'labels'),
    assignees: scopeStringList(scope, 'assignees'),
  }
}

// ported from src/main/integration-event-bridge.ts @hasLinearPredicates
export function hasLinearPredicates(predicates: LinearScopePredicates): boolean {
  return predicates.teams.length > 0 ||
    predicates.projects.length > 0 ||
    predicates.labels.length > 0 ||
    predicates.assignees.length > 0
}

function normalizeLinearFilterValue(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) return String(value).toLowerCase()
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  return trimmed ? trimmed.toLowerCase() : null
}

function addLinearFilterCandidate(candidates: Set<string>, value: unknown): void {
  const normalized = normalizeLinearFilterValue(value)
  if (normalized) candidates.add(normalized)
}

function addLinearRecordCandidates(candidates: Set<string>, value: unknown): void {
  addLinearFilterCandidate(candidates, value)
  if (!isRecord(value)) return
  for (const key of ['id', 'name', 'displayName', 'title', 'key', 'email', 'url', 'identifier']) {
    addLinearFilterCandidate(candidates, value[key])
  }
}

// ported from src/main/integration-event-bridge.ts @linearRecordCandidates
export function linearRecordCandidates(record: Record<string, unknown>, key: keyof LinearScopePredicates): Set<string> {
  const candidates = new Set<string>()
  if (key === 'teams') {
    addLinearFilterCandidate(candidates, record.teamId)
    addLinearRecordCandidates(candidates, record.team)
    return candidates
  }
  if (key === 'projects') {
    addLinearFilterCandidate(candidates, record.projectId)
    addLinearRecordCandidates(candidates, record.project)
    return candidates
  }
  if (key === 'assignees') {
    addLinearFilterCandidate(candidates, record.assigneeId)
    addLinearRecordCandidates(candidates, record.assignee)
    return candidates
  }

  const labelIds = record.labelIds
  if (Array.isArray(labelIds)) {
    for (const label of labelIds) addLinearFilterCandidate(candidates, label)
  }
  const labels = record.labels
  if (Array.isArray(labels)) {
    for (const label of labels) addLinearRecordCandidates(candidates, label)
  }
  return candidates
}

function linearPredicateMatches(
  record: Record<string, unknown>,
  predicates: LinearScopePredicates,
  key: keyof LinearScopePredicates,
): boolean {
  const selected = predicates[key]
    .map(normalizeLinearFilterValue)
    .filter((entry): entry is string => entry !== null)
  if (selected.length === 0) return true
  const candidates = linearRecordCandidates(record, key)
  return selected.some((value) => candidates.has(value))
}

// ported from src/main/integration-event-bridge.ts @linearIssueMatchesPredicates
export function linearIssueMatchesPredicates(
  record: Record<string, unknown>,
  predicates: LinearScopePredicates,
): boolean {
  return linearPredicateMatches(record, predicates, 'teams') &&
    linearPredicateMatches(record, predicates, 'projects') &&
    linearPredicateMatches(record, predicates, 'labels') &&
    linearPredicateMatches(record, predicates, 'assignees')
}

export function isLinearIssueEventPath(path: string): boolean {
  return /^\/linear\/issues\/[^/]+\.json$/u.test(path)
}

function eventSummaryValue(value: unknown): string | undefined {
  if (typeof value === 'string' && value.trim()) return value.trim()
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  return undefined
}

function readPredicateRecord(content: unknown): Record<string, unknown> | null {
  if (typeof content === 'string') {
    try {
      const parsed = JSON.parse(content)
      return isRecord(parsed) ? parsed : null
    } catch {
      return null
    }
  }
  return isRecord(content) ? content : null
}

// ported from src/main/integration-event-bridge.ts @filterLinearPredicateSpecs
export async function filterLinearPredicateSpecs<TSpec extends LinearPredicateSubscriptionSpec>(
  input: {
    mount: Pick<MountClient, 'readFile'>
    event: ChangeEvent
    matchedSpecs: TSpec[]
    logger?: { debug?(message: string, ...args: unknown[]): void; warn?(message: string, ...args: unknown[]): void }
    projectId?: string
  },
): Promise<TSpec[]> {
  const predicateSpecs = input.matchedSpecs.filter((spec) => spec.linearPredicates)
  if (predicateSpecs.length === 0) return input.matchedSpecs
  const path = eventSummaryValue(input.event.resource.path)
  if (!path || !isLinearIssueEventPath(path)) return input.matchedSpecs

  const unfilteredSpecs = input.matchedSpecs.filter((spec) => !spec.linearPredicates)
  let record: Record<string, unknown> | null = null
  try {
    record = readPredicateRecord((await input.mount.readFile(path)).content)
  } catch {
    record = null
  }
  if (!record) {
    input.logger?.warn?.('Linear predicate issue read failed', {
      projectId: input.projectId,
      eventId: input.event.id,
      path,
    })
    return unfilteredSpecs
  }

  const filteredPredicateSpecs = predicateSpecs.filter((spec) =>
    spec.linearPredicates && linearIssueMatchesPredicates(record, spec.linearPredicates)
  )
  const droppedCount = predicateSpecs.length - filteredPredicateSpecs.length
  if (droppedCount > 0) {
    input.logger?.debug?.('filtered Linear event by scope predicates', {
      projectId: input.projectId,
      eventId: input.event.id,
      path,
      droppedSpecs: droppedCount,
    })
  }

  return [...unfilteredSpecs, ...filteredPredicateSpecs]
}
