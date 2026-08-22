import type { AgentSpec } from '../ports/fleet'
import type { IssueRef, WorkUnitOrigin } from '../types'
import { ISSUE_KEY_PARTS } from '../issue-key-match'
import { githubLifecycleIdentity } from '../state/github-lifecycle-identity'

const DISPATCH_IDENTITY_VERSION = 'factory:dispatch:v1'
const NOTION_PAGE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u

/**
 * Provider-native identity for one issue, independent of whichever Relayfile
 * alias or local mount path surfaced it to Factory.
 *
 * A declared origin outranks the sense path: a Linear mirror of a GitHub issue
 * carries a `/linear/...` path but is the same work unit as the GitHub issue,
 * and must not get an identity of its own.
 */
export function dispatchIssueIdentity(issue: IssueRef): string {
  const origin = githubOriginIdentity(issue.origin)
  if (origin) return `github:${origin}`

  const github = githubLifecycleIdentity({ issue })
  if (github) return `github:${github}`

  const uuid = issue.uuid.trim()
  if (!uuid) throw new Error(`Cannot derive dispatch identity for ${issue.key}: provider identity is empty`)
  // Classified from the issue key's own shape (`TEAM-123` vs. a bare number),
  // not the mount path, so the same work unit gets the same identity
  // regardless of which Relayfile alias surfaced it.
  return `${ISSUE_KEY_PARTS.test(issue.key) ? 'linear' : 'issue'}:${uuid}`
}

/**
 * Provider-native identity for one Notion page, independent of the mutable
 * repository, workspace path, or mount surface through which it is offered.
 */
export function dispatchNotionPageIdentity(pageId: string): string {
  const canonical = pageId.trim().toLowerCase()
  if (!NOTION_PAGE_ID.test(canonical)) {
    throw new Error('Cannot derive dispatch identity for Notion page: provider identity is not a canonical page id')
  }
  return `notion:${canonical}`
}

/**
 * Stable broker reclaim proof for one issue role. Retries of the same work
 * unit reproduce it; a same-looking issue from another provider/repository
 * does not.
 */
export function dispatchAgentIdentityKey(
  issue: IssueRef,
  role: AgentSpec['role'],
): string {
  return `${DISPATCH_IDENTITY_VERSION}:${dispatchIssueIdentity(issue)}:${role}`
}

/**
 * The GitHub half of a declared origin, in exactly the spelling
 * `githubLifecycleIdentity` emits for a GitHub-native path, so a mirror and
 * the native row it mirrors produce one identical identity.
 */
const githubOriginIdentity = (origin: WorkUnitOrigin | undefined): string | undefined => {
  if (origin?.provider !== 'github') return undefined
  const owner = origin.owner.trim().toLowerCase()
  // Some providers spell the origin repo `owner/repo`; the identity wants the bare name.
  const repo = origin.repo.trim().toLowerCase().replace(new RegExp(`^${owner}/`, 'u'), '')
  if (!owner || !repo || repo.includes('/')) return undefined
  if (!Number.isSafeInteger(origin.number) || origin.number <= 0) return undefined
  return `${owner}/${repo}#${origin.number}`
}

/**
 * The structured GitHub origin a surface declares in its provider payload
 * (`source: { provider: 'github', owner, repo, number }`) — the same record
 * `safety/factory-scope.ts` reads to recognise a `[factory]` GitHub mirror.
 *
 * Returns undefined for a native Linear issue, which has no upstream origin
 * and is correctly identified by its own uuid.
 */
export function mirrorWorkUnitOrigin(issue: { path: string; raw: unknown }): WorkUnitOrigin | undefined {
  // A surface whose own path already names the GitHub issue is not a mirror —
  // it identifies itself, and recording a redundant origin would widen every
  // persisted IssueRef for nothing.
  if (githubLifecycleIdentity({ issue: { uuid: '', key: '', path: issue.path } })) return undefined
  return workUnitOriginFromRaw(issue.raw)
}

export function workUnitOriginFromRaw(raw: unknown): WorkUnitOrigin | undefined {
  const source = asRecord(wrappedPayload(raw).source)
  if (!source || stringValue(source.provider)?.toLowerCase() !== 'github') return undefined
  const owner = stringValue(source.owner)?.trim() ?? ''
  const rawRepo = stringValue(source.repo)?.trim() ?? ''
  const repo = owner && rawRepo.toLowerCase().startsWith(`${owner.toLowerCase()}/`)
    ? rawRepo.slice(owner.length + 1)
    : rawRepo
  const number = typeof source.number === 'number' ? source.number : Number(source.number)
  if (!owner || !repo || repo.includes('/')) return undefined
  if (!Number.isSafeInteger(number) || number <= 0) return undefined
  return { provider: 'github', owner, repo, number }
}

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
