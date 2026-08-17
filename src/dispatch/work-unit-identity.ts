import type { AgentSpec } from '../ports/fleet'
import type { IssueRef } from '../types'
import { ISSUE_KEY_PARTS } from '../issue-key-match'
import { githubLifecycleIdentity } from '../state/github-lifecycle-identity'

const DISPATCH_IDENTITY_VERSION = 'factory:dispatch:v1'

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
