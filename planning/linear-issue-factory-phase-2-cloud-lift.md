Title: [factory] Phase 2 — cloud lift: factory brain in the cloud worker, emits spawns into the fleet

Team: AR
Suggested status: Ready for Agent
Repo label: cloud
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md §7 (Phase 2)
Depends on: p1 (StateStore port), p4 (published @agent-relay/factory), Phase 3 (RelayFleetClient) for the emit path
Related: relay/specs/fleet-delivery.md §6/§7; AR-268 (Nango fanout), AR-270 (GitHub path)

---

## Problem

The factory brain (triage, batch/in-flight state, clarification, merge-gate) runs in-process on the `FactoryLoop` inside the Pear daemon — single-machine, non-durable, lost on restart (epic v2 §1). Under the unified-node model the brain belongs in the cloud worker, multi-tenant by `workspaceId`, **emitting spawn invocations into the relay fleet** rather than executing anything itself.

## Design

Host `@agent-relay/factory`'s orchestrator (triage + merge-gate + state machine) inside the cloud web worker, driven by the EXISTING proactive-runtime ingress — no new watch infra:

- **Ingress (reuse):** relayfile push → `POST /api/v1/webhooks/github` (`cloud/packages/web/app/api/v1/webhooks/github/route.ts:210`) → `dispatchIntegrationWatchEvent(...)` (`cloud/packages/web/lib/proactive-runtime/integration-watch-dispatcher.ts:1523`). Add the parallel `/linear` ingress + watch-match config for `[factory]` issues (overlaps AR-270 path fix).
- **Durable state:** implement a `StateStore` (the port from p1) backed by a **Cloudflare Durable Object** (match `cloud/packages/web/lib/integrations/` runtime conventions; see memory `relayfile-deploy-topology`). Replaces in-process `FactoryLoop` state. Reuse the existing `integration_watch_deliveries` table (`cloud/packages/web/lib/db/schema.ts`) for delivery lifecycle (pending → delivered/failed → terminal) where it fits; add factory-specific columns/tables only where a field has no home.
- **Triage → recipe → emit:** triage selects a recipe (epic v2 §3/§3.5); the recipe expands to a spawn-set; the brain **emits each `spawn{capability,…}` via the `RelayFleetClient`** (Phase 3) and records `invocationId`s in the StateStore. No Daytona, no launch-member, no execution code here.
- **`team` recipe absorbs `spawn-team.ts`:** `cloud/packages/web/lib/teams/spawn-team.ts` (`spawnTeam(input, deps)`, lines 161–343) currently provisions a Daytona team directly — `MEMBER_LOCAL_ROOT = "/home/daytona/workspace"` (line 55), `resolveMembers` (498–529), `launchMember` (273). Reconstruct its roster-building as the `team` recipe that emits N implementer spawns + 1 reviewer spawn into the fleet. The Daytona-specific local-root + launch-member callback is **deleted** — placement/execution are fleet-side (RFC §6).
- **Merge-gate + writeback:** stay in cloud (cloud already reads `/github` + holds GitHub auth). Driven by the `invocationId` completion lifecycle (RFC §7: `pending → dispatched → completed`).

## End-to-end verification (captured artifact required)

1. Configure a test workspace; label a Linear issue `[factory]` + `agent:single`, move to Ready for Agent.
2. Capture the cloud log showing: webhook ingress → triage → recipe → `RelayFleetClient.spawn` emitted with an `invocationId`.
3. Capture the StateStore (DO) record showing the in-flight entry, surviving a worker redeploy (kill + redeploy mid-flight; the entry persists).
4. Capture the completion writeback to Linear once the spawn reports `completed`.
5. Capture a two-workspace run proving no state bleed.

## Acceptance criteria

1. Factory orchestrator runs in the cloud worker, multi-tenant by `workspaceId`; zero Electron/Pear involvement.
2. `StateStore` DO impl persists in-flight/batch/clarification; survives worker redeploy (captured proof).
3. Triage emits spawn invocations via `RelayFleetClient`; no Daytona/launch-member/execution code in the factory path.
4. `spawn-team.ts`'s Daytona team-provisioning is replaced by the `team` recipe (N spawns + reviewer); `MEMBER_LOCAL_ROOT` and the launch-member callback are gone.
5. Merge-gate + Linear/Slack writeback driven by the `invocationId` lifecycle.
6. No new datastore beyond the existing proactive-runtime tables + the StateStore DO.

## Out of scope

- The `RelayFleetClient` implementation itself (Phase 3).
- Node registration (Phase 4).
- Proactive-runtime (`team-launch-n1`) unification — separate deliverable.
- Recipe→scope label logic in the package (that's the `@agent-relay/factory` triage; this issue hosts it, doesn't author it).

## Related

- Epic v2 §2 (architecture), §3.5 (recipes), §6 (thin fleet client).
- `cloud/.../teams/spawn-team.ts` (absorbed), `integration-watch-dispatcher.ts:1523`, `webhooks/github/route.ts:210`, `db/schema.ts` (`integration_watch_deliveries`).
- RFC §6 (placement), §7 (invocation lifecycle).
- Phase 3 (fleet client), Phase 1 prep p1 (StateStore port).
