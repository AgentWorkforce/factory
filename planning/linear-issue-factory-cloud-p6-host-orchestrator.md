Title: [factory] cloud: Phase 2 — host the factory orchestrator in proactive-runtime; StateStore backed by existing Postgres

Team: AR
Suggested status: Ready for Agent
Repo label: cloud
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p1 (StateStore port), p4 (published `@agent-relay/factory`)
Epic: factory-cloud-watches-local-node-linear-issue.md §6, §8 (Phase 2)

---

## Context

The factory's watch→triage→dispatch→merge loop must run server-side, multi-tenant, always-on — not in Electron. The scoping inventory (epic §6) confirmed cloud's `proactive-runtime` already provides every primitive needed (webhook ingress, watch dispatcher, durable Postgres state, 1-min relaycron sweep). This PR hosts `@agent-relay/factory`'s orchestrator inside that runtime and backs the `StateStore` port (p1) with the **existing** Postgres tables — NOT a new datastore.

## Goal

Cloud runs the factory orchestrator per `workspaceId`. In-flight / queued / clarification state persists in Postgres and survives restarts. No new infrastructure (no DynamoDB, no DO store, no new worker).

## Scope

- Import `@agent-relay/factory` (orchestrator + triage + github + state) into the cloud web worker.
- Implement a `PostgresStateStore` against the existing tables: `integration_watch_deliveries`, `integration_watch_issue_dispatch_dedup`, `proactive_continuations` (see `cloud/.../db/schema.ts`). Map `InFlightRegistry` / `BatchTracker` / clarification records onto these (add columns/tables only if a field has no home — prefer reuse).
- Drive the orchestrator from the existing watch dispatcher (`cloud/.../proactive-runtime/integration-watch-dispatcher.ts`) and relaycron sweep (`cloud/packages/relaycron/src/sweep.ts`) rather than a polling loop.
- Multi-tenant: scope all state + dispatch by `workspaceId`; load `WorkspaceConfig` (p2) per workspace.

## Acceptance criteria

1. A GitHub/Linear webhook for a configured workspace drives the factory orchestrator in cloud (triage runs, decision recorded) with zero local/Electron involvement.
2. In-flight + clarification state is in Postgres and survives a worker redeploy.
3. No new datastore/binding added to `wrangler.*.toml` beyond what proactive-runtime already has.
4. Two workspaces run concurrently without state bleed.

## Out of scope

- The dispatch target (Daytona vs fleet-node) — that's p9 + p10. This PR can keep dispatching to the existing target to prove the loop.
- Linear ingress — p8.
- Scope widening / label mapping — p7.

## Dependencies / sequencing

Lands after p1 + p4. Parallel-safe with p7/p8 but easier if p2 (config split) is in first.
