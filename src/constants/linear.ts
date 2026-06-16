// Workflow-state UUIDs are NOT hardcoded — they differ per workspace/team and
// are resolved at runtime from /linear/states by name (see resolveFactoryStates)
// or pinned explicitly via config.stateIds.

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
