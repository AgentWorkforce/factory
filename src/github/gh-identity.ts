/**
 * One rule for every GitHub mutation Factory performs through the local `gh`
 * CLI.
 *
 * Factory has two GitHub write identities. Lifecycle writes (PR publication,
 * issue comments, status labels, issue closure) are selected by
 * `github.identity` and, on exact `app`, are performed server-side by the
 * connected workspace GitHub App. Everything else still shells out to `gh`,
 * which authenticates as whatever local user happens to be logged in — a
 * human account. That produced one product with two audit trails.
 *
 * The remaining `gh` mutations cannot simply be re-routed: the connected
 * `GithubConnectionWrite` surface has no merge operation and no issue-create
 * operation, and Factory must never receive or invoke a GitHub credential of
 * its own (see `src/mount/github-api-issue-read.ts`). So the honest behavior
 * under an explicit `app` identity is to refuse rather than to silently write
 * as the operator — a documented limitation instead of an invisible one.
 *
 * `auto` and `user` keep today's local-`gh` behavior. Only exact `app`
 * refuses, and the refusal names the recovery path so the gate does not take
 * an honest caller hostage.
 */
export type GithubWriteIdentity = 'app' | 'user' | 'auto'

/**
 * Whether the configured identity permits mutating GitHub through local `gh`.
 *
 * Reads are always permitted: `gh pr view` leaks no authorship, so read
 * provenance is not an identity concern (see `StandalonePullRequest.source`).
 */
export function localGhMutationAllowed(identity: GithubWriteIdentity): boolean {
  return identity !== 'app'
}

/**
 * The refusal text for a local-`gh` mutation blocked by `github.identity: "app"`.
 *
 * @param operation  what Factory was about to do, in caller-facing terms.
 * @param capability the server-side operation Relayfile Cloud would need to
 *                   expose on the connected App surface for this write to be
 *                   performed as the app instead of refused.
 */
export function localGhMutationRefusal(operation: string, capability: string): string {
  return `GitHub identity "app" refuses ${operation} through the local gh CLI, which would attribute the write to the operator's account instead of the workspace GitHub App. ` +
    `Performing it as the app requires the connected write capability "${capability}", which the Relayfile GitHub connection does not expose. ` +
    'Set github.identity to "user" or "auto" to deliberately accept local-user attribution for this operation.'
}

/** Throw the standard refusal when `identity` forbids a local-`gh` mutation. */
export function assertLocalGhMutationAllowed(
  identity: GithubWriteIdentity,
  operation: string,
  capability: string,
): void {
  if (localGhMutationAllowed(identity)) return
  throw new Error(localGhMutationRefusal(operation, capability))
}
