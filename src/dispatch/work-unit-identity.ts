import type { AgentSpec } from '../ports/fleet'
import type { IssueRef } from '../types'
import { ISSUE_KEY_PARTS } from '../issue-key-match'
import { githubLifecycleIdentity } from '../state/github-lifecycle-identity'

const DISPATCH_IDENTITY_VERSION = 'factory:dispatch:v1'
const NOTION_PAGE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u

/**
 * Provider-native identity for one issue, independent of whichever Relayfile
 * alias or local mount path surfaced it to Factory.
 */
export function dispatchIssueIdentity(issue: IssueRef): string {
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
