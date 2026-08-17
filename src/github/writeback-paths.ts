import { createHash } from 'node:crypto'

const FACTORY_ISSUE_COMMENT_DRAFT_PREFIX = 'factory-'
const FACTORY_ISSUE_COMMENT_DIGEST_LENGTH = 24

/** Stable filename contract shared by the App writer and the mount guard. */
export const factoryGithubIssueCommentDraftName = (body: string): string => {
  const digest = createHash('sha256')
    .update(body)
    .digest('hex')
    .slice(0, FACTORY_ISSUE_COMMENT_DIGEST_LENGTH)
  return `${FACTORY_ISSUE_COMMENT_DRAFT_PREFIX}${digest}.json`
}

export const isFactoryGithubIssueCommentDraftName = (value: string): boolean => {
  if (!value.startsWith(FACTORY_ISSUE_COMMENT_DRAFT_PREFIX) || !value.endsWith('.json')) return false
  const digest = value.slice(FACTORY_ISSUE_COMMENT_DRAFT_PREFIX.length, -'.json'.length)
  return digest.length === FACTORY_ISSUE_COMMENT_DIGEST_LENGTH && /^[a-f0-9]+$/u.test(digest)
}
