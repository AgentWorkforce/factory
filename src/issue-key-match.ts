/**
 * Linear-style issue key shape (`TEAM-123`), as opposed to a bare GitHub
 * issue number. Real team prefixes can include digits after the leading
 * letter (e.g. `CORE23-456`), so only the first character is anchored.
 */
export const ISSUE_KEY_PARTS = /^([A-Z][A-Z0-9]*)-(\d+)$/iu

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

/**
 * GitHub's closing keywords. Only these bind a merged pull request to an issue
 * as *completing* it. A bare mention (`#155`, `Refs #155`, `Diagnostic for
 * #155`) is an association, not authority to close.
 * https://docs.github.com/issues/tracking-your-work-with-issues/using-keywords-in-issues-and-pull-requests
 *
 * Deliberately body-only: GitHub does not honour closing keywords in a pull
 * request *title*, so neither do we.
 */
const CLOSING_KEYWORD = String.raw`(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)`

/**
 * Phrases that revoke closure authority. Matched only on the line that carries
 * the issue reference — a body-wide match would let "this does not fix #B"
 * block a legitimate close of #A.
 */
const CLOSURE_NEGATION =
  new RegExp(
    [
      String.raw`\b(?:does|do|did|will|would|can|could|is|are)\s+(?:n[o']?t|not)\s+(?:\w+\s+){0,2}${CLOSING_KEYWORD}\b`,
      String.raw`\b(?:doesn|don|didn|won|wouldn|can|couldn|isn|aren)'?t\s+(?:\w+\s+){0,2}${CLOSING_KEYWORD}\b`,
      String.raw`\bno[nt]?[-\s]?(?:a\s+)?(?:${CLOSING_KEYWORD})\b`,
      String.raw`\bdiagnostic\s+for\b`,
      String.raw`\bgroundwork\s+for\b`,
      String.raw`\bpartial(?:ly)?\s+(?:${CLOSING_KEYWORD})\b`,
    ].join('|'),
    'iu',
  )

/**
 * Whether `headRef` is the implementation branch for `issueKey`.
 *
 * Mirrors Factory's own strongest association signal: a numeric GitHub key
 * requires the `factory/` dispatch prefix, because a bare number is too easy to
 * collide with, while a Linear-keyed branch carries the key itself
 * (`ar-410-fix`) and needs no prefix.
 */
export const branchImplementsIssue = (headRef: string, issueKey: string): boolean =>
  /^\d+$/u.test(issueKey.trim())
    ? factoryBranchBelongsToIssue(headRef, issueKey)
    : containsIssueKey(headRef, issueKey)

export type ClosureAuthoritySource = 'factory-branch' | 'closing-keyword'

export interface ClosureAuthority {
  authorised: boolean
  source?: ClosureAuthoritySource
  /** Why authority was granted or refused, verbatim for the writeback comment. */
  evidence: string
}

export interface ClosureAuthorityInput {
  headRef?: string
  body?: string
  /** `owner/repo` of the pull request, used to bind qualified references. */
  repo?: string
}

/**
 * Reference forms that a closing keyword may bind to.
 *
 * A bare `#N` resolves to the pull request's own repository, which the caller
 * has already scoped. A *qualified* `owner/repo#N` or issue URL is only honored
 * when it names that same repository: `relayfile-cloud#155` must never grant
 * authority to close `factory#155`. That cross-repository confusion is the
 * documented prior art on this bug (#268).
 */
const referencePatterns = (issueKey: string, repo: string | undefined): string[] => {
  const parts = ISSUE_KEY_PARTS.exec(issueKey)
  if (parts) {
    const prefix = escapeRegex(parts[1] ?? '')
    const number = escapeRegex(parts[2] ?? '')
    return [String.raw`${prefix}-${number}(?=$|[^A-Za-z0-9-]|-(?!\d))`]
  }
  if (!/^\d+$/u.test(issueKey)) return []
  const number = escapeRegex(issueKey)
  const patterns = [String.raw`#${number}(?!\d)`]
  if (repo && /^[^/\s]+\/[^/\s]+$/u.test(repo)) {
    const qualified = escapeRegex(repo)
    patterns.push(
      String.raw`${qualified}#${number}(?!\d)`,
      String.raw`https?://github\.com/${qualified}/issues/${number}(?!\d)`,
    )
  }
  return patterns
}

/** Lines of `body` that reference `issueKey` in any accepted form. */
const referencingLines = (body: string, issueKey: string, repo: string | undefined): string[] => {
  const patterns = referencePatterns(issueKey, repo)
  if (patterns.length === 0) return []
  const reference = new RegExp(patterns.join('|'), 'iu')
  return body.split(/\r?\n/u).filter((line) => reference.test(line))
}

/**
 * Whether a merged pull request may close `issueKey`.
 *
 * Authority comes from exactly two sources, and neither is a prose mention:
 *
 * - `factory-branch` — the head is Factory's own dispatch branch for the issue.
 *   Factory cut that branch to implement that issue, which outranks any text.
 * - `closing-keyword` — a real GitHub closing keyword bound to a reference to
 *   this issue, in the body.
 *
 * Fail-closed by construction: anything unrecognised, negated, or merely
 * referential leaves the issue open. A missed close costs a stale label; a
 * wrong close marks a live outage COMPLETED and hides it (#313, #155).
 */
/**
 * Whether the pull request body explicitly disclaims closing `issueKey`.
 *
 * This is the weaker half of `prClosureAuthority`, for the in-flight path where
 * Factory's own dispatch already established the association and only an
 * explicit denial should stop completion.
 */
export const prBodyDisclaimsClosing = (
  input: ClosureAuthorityInput,
  issueKey: string,
): boolean =>
  referencingLines(input.body ?? '', issueKey, input.repo)
    .some((line) => CLOSURE_NEGATION.test(line))

export const prClosureAuthority = (
  input: ClosureAuthorityInput,
  issueKey: string,
): ClosureAuthority => {
  const body = input.body ?? ''
  const lines = referencingLines(body, issueKey, input.repo)

  const negated = lines.find((line) => CLOSURE_NEGATION.test(line))
  if (negated) {
    return {
      authorised: false,
      evidence: `the pull request body disclaims closing ${issueKey}: "${negated.trim().slice(0, 200)}"`,
    }
  }

  const patterns = referencePatterns(issueKey, input.repo)
  if (patterns.length > 0) {
    const keyworded = new RegExp(
      String.raw`(^|[^A-Za-z0-9-])${CLOSING_KEYWORD}\s*:?\s+(?:${patterns.join('|')})`,
      'iu',
    )
    const line = lines.find((candidate) => keyworded.test(candidate))
    if (line) {
      return {
        authorised: true,
        source: 'closing-keyword',
        evidence: `pull request body closes ${issueKey} via "${line.trim().slice(0, 200)}"`,
      }
    }
  }

  if (branchImplementsIssue(input.headRef ?? '', issueKey)) {
    return {
      authorised: true,
      source: 'factory-branch',
      evidence: `head branch ${input.headRef} is the implementation branch for ${issueKey}`,
    }
  }

  return {
    authorised: false,
    evidence: lines.length > 0
      ? `the pull request references ${issueKey} but carries no closing keyword for it`
      : `the pull request carries no closing keyword for ${issueKey}`,
  }
}
