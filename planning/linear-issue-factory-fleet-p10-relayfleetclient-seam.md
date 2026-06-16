Title: [factory] relay+factory: Phase 3 — implement RelayFleetClient; dispatch fleet-node target → relaycast agent.create

Team: AR
Suggested status: Ready for Agent
Repo label: relay (with `@agent-relay/factory` RelayFleetClient)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p9 (DispatchTarget interface), p4 (published package)
Epic: factory-cloud-watches-local-node-linear-issue.md §2, §6, §8 (Phase 3)

---

## Context

`@agent-relay/factory` ships a `RelayFleetClient` that is currently a **stub throwing `relay#1056`**. The fleet fabric it needs is now ~90% shipped: `agent.create` action (`relay/packages/harness-driver/src/actions.ts`), node roster (`nodes.list`), capability advertisement (`relay/packages/fleet/src/index.ts`), durable mailbox. This PR connects the two — the cloud `fleet-node` DispatchTarget (p9) calls into relaycast to spawn an agent on the user's node.

## Goal

Cloud can place an agent spawn onto a connected fleet node via relaycast `agent.create`, scoped by capability (`spawn:claude` / `spawn:codex`), and receive run lifecycle back.

## Scope

- Implement `RelayFleetClient` in `@agent-relay/factory` against the relay SDK messaging API (`agent.create` / `agent.release`, `nodes.list({ capability })`, `deliveries.*`). Replace the `relay#1056` stub.
- Implement the `fleet-node` branch of `DispatchTarget` (p9) to drive `RelayFleetClient`: translate `scope` → spawn count/roster, select the target node (basic: the workspace's single live node — full placement is p12), invoke `agent.create`, map run lifecycle onto `DispatchTarget.poll`.
- Stream agent exit/messages back so the cloud orchestrator runs merge-gate + Linear/Slack writeback.

## Acceptance criteria

1. A `targetKind: 'fleet-node'` workspace dispatches an `agent:single` issue → an agent spawns on a connected node's local checkout.
2. Agent exit + messages reach the cloud orchestrator; merge-gate + writeback proceed.
3. `RelayFleetClient` no longer throws `relay#1056`; it round-trips a real spawn.
4. Capability mismatch (node can't `spawn:codex`) fails cleanly with a clear error.

## Out of scope

- Smart placement / multi-node selection (p12) — single-node assumption is fine here.
- Broker heartbeat/reconnect reconciliation (p11).
- workflow/team fan-out beyond the basic scope→spawn translation (can stub team to single-implementer if needed, note it).
