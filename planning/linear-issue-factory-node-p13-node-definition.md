Title: [factory] factory: Phase 4 — ship the factory node-definition for `agent-relay fleet serve`

Team: AR
Suggested status: Ready for Agent
Repo label: factory (node-def in `@agent-relay/factory`; runs via relay CLI)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p9 (DispatchTarget), p10 (RelayFleetClient), p11 (heartbeat), p12 (placement)
Epic: factory-cloud-watches-local-node-linear-issue.md §2, §8 (Phase 4)

---

## Context

The end-user command stays `agent-relay fleet serve <def>` — no rename, no new `factory start` wrapper. This PR provides the prebuilt **factory node-definition** that command runs, so a user only needs a `NodeConfig` to participate. This closes the loop: cloud watches + triages, this node executes.

## Goal

`agent-relay fleet serve <factory-node-def>` boots a node from a user's `NodeConfig` that advertises spawn capabilities, registers with relaycast, and executes cloud-placed spawns in the correct local checkout — with zero other setup.

## Scope

- Ship a factory node-definition (`defineNode({...})`) in `@agent-relay/factory` that:
  - reads `NodeConfig` (`workspaceId`, `capabilities`, `repoPaths`),
  - advertises `capabilities` (`spawn:claude` / `spawn:codex`),
  - pushes `repoPaths` keys up on registration (for p12 placement),
  - on placement, runs the agent locally via the existing harness path (`InternalFleetClient` / harness-driver) in the mapped checkout,
  - streams exit/messages back up.
- Document the one-command flow: `agent-relay fleet serve ./factory.node.ts` (or a packaged default def referenced by name).
- **`fleet serve` already auto-starts the broker** — no separate `agent-relay up --background` step. `runFleetServe` calls `startBrokerWithPortFallback(...)` before serving the node (`relay/packages/cli/src/cli/commands/fleet.ts:144`), so a single command boots the broker (with port fallback) and registers the node. The one-command UX is real; do not add a broker-start step or instruct the user to run `up` first. If anything regresses this, fix `fleet serve` so it stays true.
- (Optional) Pear read-only desktop view subscribes to the same cloud state.

## Acceptance criteria

1. With only a `NodeConfig`, `agent-relay fleet serve <factory-node-def>` registers a node visible in `nodes.list()` with the advertised capabilities.
2. Labeling a Linear issue `agent:single` spawns an agent on this node in the correct local checkout; `agent:team` / `agent:workflow` produce their shapes.
3. Closing + reopening the laptop drains queued work (relies on p11/p12); no orphaned issue.
4. No new CLI command was introduced — `fleet serve` is the entry point.
5. **Cold-broker proof:** from a machine with NO running broker, `agent-relay fleet serve <factory-node-def>` brings the broker up automatically (port fallback) and the node appears in `agent-relay fleet nodes`. No prior `agent-relay up` was run.

## Out of scope

- The cloud brain (Phases 2) and fleet plumbing (Phase 3) — consumed here, not built here.
- Multi-cloud / hosted-node variants.
