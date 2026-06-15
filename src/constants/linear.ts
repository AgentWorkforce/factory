// AR-team workflow state UUIDs. FactoryConfig deliberately omits humanReview
// from its default stateIds so non-AR operators must opt into their own review
// state before terminalState: 'human-review' changes behavior.
export const LINEAR_STATE_IDS = {
  readyForAgent: 'b9bec744-b60c-4745-8022-d90d6ab59ae3',
  agentImplementing: '39b9881d-1196-4c95-8b80-a20f0c7263f7',
  humanReview: '24462e2d-9946-4dd1-a798-931cdd678498',
  done: '83ea5383-bfe9-425a-86ef-517b8190f09a',
  inPlanning: '3de351f2-90e6-4731-aa6b-4a55b77f481e',
} as const

export const linearIssuePath = (key: string, uuid: string) => `/linear/issues/${key}__${uuid}.json`

export const linearByStatePath = (slug: string) => `/linear/issues/by-state/${slug}/`

// Comment writeback must be nested under its issue — the relayfile cloud
// writeback executor only accepts /linear/issues/<issueRef>/comments/<draft>.json
// (top-level /linear/comments/<name>.json is rejected as "unsupported Linear
// writeback path"). issuePath is /linear/issues/<key>__<uuid>.json, so the
// issueRef segment is its basename.
export const linearCommentPath = (issuePath: string, name: string) => {
  const issueRef = issuePath.replace(/^\/linear\/issues\//u, '').replace(/\.json$/u, '')
  return `/linear/issues/${issueRef}/comments/${name}.json`
}
