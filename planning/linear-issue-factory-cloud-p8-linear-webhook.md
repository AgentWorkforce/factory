Title: [factory] cloud: Phase 2 — add Linear webhook ingress + Linear watch-match config

Team: AR
Suggested status: Ready for Agent
Repo label: cloud
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p6 (orchestrator hosted) — parallel-safe with p7/p9
Epic: factory-cloud-watches-local-node-linear-issue.md §6, §8 (Phase 2)

---

## Context

Cloud already ingests GitHub webhooks at `POST /api/v1/webhooks/github` and runs the watch→match→dispatch loop on ingress. The factory is Linear-issue-driven, so it needs the parallel Linear ingress + watch-match config so issue label/state changes drive triage.

## Goal

Linear issue events (create, label-add, state-change) reach cloud via webhook POST and drive the factory orchestrator the same way GitHub events already do.

## Scope

- Add `POST /api/v1/webhooks/linear` mirroring the GitHub route (`cloud/.../app/api/v1/webhooks/github/route.ts`): signature validation, workspace resolution, `normalizeWebhook()`, write-through to relayfile (`/linear/issues/**`), then `dispatchIntegrationWatchEvent()`.
- Extend watch-match config so factory subscriptions can match Linear paths (`/linear/issues/**`) and label conditions, reusing `watch_globs` / `watch_rules` semantics.
- Respect the existing issue-level dedup/cooldown (`integration_watch_issue_dispatch_dedup`) to avoid label-churn storms.

## Acceptance criteria

1. Adding a configured label to a Linear issue triggers a cloud-side factory triage within one dispatch cycle.
2. Label churn within the cooldown window coalesces (no duplicate dispatch).
3. The Linear route validates signatures and rejects unconfigured workspaces.
4. relayfile `/linear/issues/**` reflects the event (write-through path verified).

## Out of scope

- Triage logic / scope mapping (p7).
- Slack ingress (already exists; not part of this PR).

## Notes

Confirm whether relayfile already delivers Linear events to cloud via the existing webhook pipe (epic §6 confirmed GitHub does). If Linear already arrives through a shared `/webhooks/{provider}` route, this PR is just the watch-match config, not a new route — verify first.
