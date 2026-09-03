/**
 * Publishing the work an implementer did inside a cloud sandbox.
 *
 * WHY THIS PORT EXISTS
 *
 * Under cloud placement an implementer agent commits to its feature branch
 * inside a Daytona sandbox and then exits. Factory's publish path then asks
 * the workspace GitHub connection to open a PR from that branch — on the
 * assumption, stated outright in the Relayfile adapter, that "a remote
 * implementer already pushed its branch". Nothing had: the sandbox holds no
 * GitHub credential (deliberately, see below) so its `git push` cannot
 * succeed, and factory holds no sandbox handle, so it cannot push on the
 * agent's behalf either. Every cloud dispatch therefore ended with commits
 * that never left the box.
 *
 * This port is the missing half. Factory names the work — which sandbox, which
 * clone path, which branch, and what the PR should say — and the host performs
 * the push somewhere that legitimately holds a credential.
 *
 * WHY THE CREDENTIAL IS NOT HERE
 *
 * Nothing on this interface is token-shaped, and that is the point. Per
 * `RULING-sandbox-push-0902`, no push credential goes into a sandbox:
 * passwordless sudo makes in-box confinement advisory, and argv leakage on
 * this fleet is measured. The implementation reads a patch out of the sandbox
 * and performs every GitHub write on the host side with a GitHub App
 * installation token that neither factory nor the sandbox ever sees. Factory
 * stays credential-free, so there is nothing here to leak.
 *
 * The GitHub App is the actor for the resulting branch, commit and PR.
 */

/** What to publish, and where from. Carries no credential, by construction. */
export interface SandboxPushInput {
  /** Provider sandbox holding the agent's clone. */
  sandboxId: string
  /** Absolute path of the clone inside that sandbox. */
  repoPath: string
  /** `owner/name`. */
  repo: string
  /** Branch to create and open the pull request from. */
  branch: string
  /** Base branch; the implementation resolves the repo default when omitted. */
  baseRef?: string
  title: string
  body: string
}

/**
 * Three outcomes, kept distinct on purpose.
 *
 * `no-changes` is a real answer about a real sandbox — the agent committed
 * nothing — and must not be reported as a failure; `failed` is a push that was
 * attempted and did not land. Collapsing them would make "the agent did no
 * work" and "we could not publish the work" indistinguishable, which is
 * exactly the ambiguity that let a broken push look like an idle agent.
 */
export type SandboxPushResult =
  | { status: 'pushed'; branch: string; prUrl: string; commitSha: string }
  | { status: 'no-changes' }
  | { status: 'failed'; reason: string }

export interface SandboxPush {
  push(input: SandboxPushInput): Promise<SandboxPushResult>
}
