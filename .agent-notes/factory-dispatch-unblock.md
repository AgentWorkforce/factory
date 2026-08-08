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
