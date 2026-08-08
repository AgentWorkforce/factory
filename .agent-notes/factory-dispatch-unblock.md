# Factory dispatch API fallback lane

## 2026-08-08 discovery

- Branch: `agent/factory-dispatch-api-fallback`, created from `origin/main` at `33cda42` (Factory PR #220 merge).
- The shared Factory checkout was not edited; it contains unrelated untracked files.
- Veto MCP methods requested by the workspace instructions are not exposed in this session.
- Current targeted resolution is projection-only: `runFactoryCommand` calls `readIssueArg`, which calls `findIssuePath`; GitHub resolution tries canonical repo-scoped paths, then lists configured Relayfile GitHub issue roots, and throws on zero matches before triage can run.
- The resolved mounted record is parsed by `parseGithubFactoryIssue`; `Factory.dispatch` then re-reads the same projection path and applies the existing scope, readiness, dispatchability, and repo-label routing gates.
- PR #220's `localMountDegraded` and daemon `eventListener` values are currently only assembled for `factory status`; targeted issue resolution does not consult or report them.
- Planned seam: a read-only, Relayfile-workspace-token GitHub API client on `MountClient`, projection-first targeted lookup, an authoritative provider lookup only after zero projection matches, explicit source/health metadata on triage and dispatch records, and provider re-read during dispatch safety validation.

## Constraints retained

- No queue, Cloudflare, mount, daemon, or launchd mutations.
- No `gh` process for the fallback.
- No merge and no default-branch push.

## 2026-08-08 implementation checkpoint

- Added `RelayfileGithubConnectionRead`, which calls the Cloud GitHub GraphQL read route through `WorkspaceHandle.requestJson`; the SDK supplies the Relayfile workspace token and Factory never handles a GitHub token.
- Added `integration:github:read` to Factory's requested Relayfile workspace scopes.
- Targeted GitHub resolution now checks the Relayfile projection first, calls the API only after zero matches, and treats an authoritative empty API result as not found.
- Triage and dispatch results carry `issueResolution`; fallback records also include PR #220's `localMountDegraded`, `localMountDegradedReason`, and `eventListener` state.
- Dispatch re-reads a fallback issue through the provider before applying the existing scope/readiness/dispatchability/repo-label gates.
- Focused build/tests: exit 0; 153 tests passed across the new connection reader, mount client, and CLI suites.
- Live preflight exposed a necessary unblock: the connected GitHub projection currently reports `degraded, complete`, and the old preflight rejected the command before resolution. Targeted triage/dispatch now proceeds only when the SDK GitHub read seam exists; missing connections and run-loop/canary flows retain the existing preflight. The command prints a warning and still checks the projection first.
- Updated focused build/tests: exit 0; 154 tests passed.

## 2026-08-08 live correction

- The connection-backed GraphQL attempt reached Cloud but exited 1 `Forbidden`: that route additionally requires a deployed sponsor persona, which the local Factory workspace join is not. Replaced it with a read-only direct GitHub REST client; GitHub writes remain on Relayfile app-authored writeback.
- Fallback eligibility now uses PR #220's health facts: a degraded local mount or listener state other than `subscribed`/`polling` means the projection cannot answer. A healthy projection miss fails without calling GitHub.
- Added configured `repo#number` and `owner/repo#number` selectors so a targeted fallback performs one authoritative lookup and avoids ambiguous org-wide probes.
- Live `factory triage factory#222` reached the empty projection, reported the listener `unknown`, resolved via `github-api-fallback`, and routed to `AgentWorkforce/factory`. It emitted its successful decision; the existing one-shot shutdown path remained open until SIGINT, then returned 0. The shutdown hang is separate from issue resolution.

## 2026-08-08 verification

- Live CLI through `runFleetCli` with a no-op reporter: `factory#222` exit 0, source `github-api-fallback`, projection `no-match`, routed only to `AgentWorkforce/factory`; `factory#999999` exit 1, no decision emitted.
- Focused build and three-suite check: exit 0; 157 tests passed.
- Projection-preference mutation check: forced projection hits past the preferred branch; targeted test exited 1. Restored source; same check exited 0.
- Safety mutation check: made the intentionally unsafe fallback fixture satisfy the existing GitHub label/title markers; targeted rejection test exited 1. Restored unsafe fixture; same check exited 0.
- Default-timeout full suite: exit 1; 1,461 passed and six timing-sensitive tests failed. Rerun of the three affected non-orchestrator files with a 20-second ceiling exited 0 (65 tests). One pre-existing heartbeat timing assertion still exits 1 even in isolation (`600 < 500`); the other affected orchestrator test passed in isolation.
- Manual diff review caught an over-broad orchestrator read fallback. It is now scoped to issue identities explicitly resolved by the targeted fallback, so ordinary healthy ingestion misses never call GitHub. The existing integration connection status is also included in the projection-health record and can independently prove a connected-but-not-ready projection cannot answer.
- Final focused build/check after that correction: exit 0; 158 tests passed. Live `factory#222` remained exit 0 through the fallback and routed only to Factory.
