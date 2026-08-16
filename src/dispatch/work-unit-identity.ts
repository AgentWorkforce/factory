import type { AgentSpec } from '../ports/fleet'
import type { IssueRef } from '../types'
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
  return `${issue.path.startsWith('/linear/') ? 'linear' : 'issue'}:${uuid}`
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
