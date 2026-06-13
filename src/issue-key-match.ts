const ISSUE_KEY_PARTS = /^([A-Z]+)-(\d+)$/iu

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
  if (!parts) return containsIssueKey(value, issueKey)

  const prefix = escapeRegex(parts[1] ?? '')
  const number = escapeRegex(parts[2] ?? '')
  const issue = `${prefix}-${number}(?=$|[^A-Za-z0-9-]|-(?!\\d))`
  return new RegExp(`(^|\\n)\\s*(?:linear|issue|closes|fixes|resolves)\\b[^\\n]*${issue}`, 'i').test(value)
}
