/** Linear-style issue key shape (`TEAM-123`), as opposed to a bare GitHub issue number. */
export const ISSUE_KEY_PARTS = /^([A-Z]+)-(\d+)$/iu

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

export const containsIssueKey = (value: string, issueKey: string): boolean => {
  const parts = ISSUE_KEY_PARTS.exec(issueKey)
  if (!parts) {
    const escaped = escapeRegex(issueKey)
    return new RegExp(`(^|[^A-Za-z0-9-])${escaped}([^A-Za-z0-9-]|$)`, 'i').test(value)
  }

  const prefix = escapeRegex(parts[1] ?? '')
  const number = escapeRegex(parts[2] ?? '')
  return new RegExp(`(^|[^A-Za-z0-9])${prefix}-${number}(?=$|[^A-Za-z0-9-]|-(?!\\d))`, 'i').test(value)
}

export const containsExplicitIssueReference = (value: string, issueKey: string): boolean => {
  const parts = ISSUE_KEY_PARTS.exec(issueKey)
  if (!parts) {
    if (/^\d+$/u.test(issueKey)) {
      const number = escapeRegex(issueKey)
      return new RegExp(
        [
          `#${number}(?!\\d)`,
          `https?://github\\.com/[^\\s/]+/[^\\s/]+/issues/${number}(?!\\d)`,
          `(?:^|\\n)\\s*(?:github\\s+)?issue\\s*:?[ \\t]*#?${number}(?!\\d)`,
          `(?:^|\\n)\\s*(?:closes|fixes|resolves)\\s*:?[ \\t]*#?${number}(?!\\d)`,
        ].join('|'),
        'i',
      ).test(value)
    }
    return containsIssueKey(value, issueKey)
  }

  const prefix = escapeRegex(parts[1] ?? '')
  const number = escapeRegex(parts[2] ?? '')
  const issue = `${prefix}-${number}(?=$|[^A-Za-z0-9-]|-(?!\\d))`
  return new RegExp(`(^|\\n)\\s*(?:linear|issue|closes|fixes|resolves)\\b[^\\n]*${issue}`, 'i').test(value)
}

/**
 * Factory-owned implementation branches always start with `factory/` and
 * carry the dispatched issue key. Numeric GitHub issue keys need an anchored
 * match so issue 3021 can never claim a branch owned by 3022 (or 30210).
 */
export const factoryBranchBelongsToIssue = (headRef: string, issueKey: string): boolean => {
  const normalizedHead = headRef.trim().toLowerCase()
  const normalizedKey = issueKey.trim().toLowerCase()
  if (!normalizedHead.startsWith('factory/') || !normalizedKey) return false
  return /^\d+$/u.test(normalizedKey)
    ? normalizedHead === `factory/${normalizedKey}` || normalizedHead.startsWith(`factory/${normalizedKey}-`)
    : containsIssueKey(normalizedHead, normalizedKey)
}
