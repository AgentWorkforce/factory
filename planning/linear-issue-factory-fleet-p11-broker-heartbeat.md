Title: [factory] relay: Phase 3 — broker outbound heartbeat/liveness + reconnect inventory sync

Team: AR
Suggested status: Ready for Agent
Repo label: relay (relay-broker + relay SDK/relaycast)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: relay#1056 fabric (shipped) — parallel-safe with p10
Epic: factory-cloud-watches-local-node-linear-issue.md §6, §8 (Phase 3); relay#1056 §7, §9

---

## Context

Today the broker↔relaycast connection (`relay-broker/src/relaycast_ws.rs`) is effectively listen-only. For cloud to place work reliably onto a laptop, relaycast needs the node's liveness + load and must reconcile live agents after a reconnect (relay#1056 §9). This is the load-bearing piece for "laptop closed → events queue → drain on reopen."

## Goal

Nodes report heartbeat/liveness/load to relaycast on a cadence, and re-announce their live agent inventory on reconnect so placement (p12) and the durable mailbox behave correctly across network blips and restarts.

## Scope

- Broker outbound **heartbeat** (~10–15s): node name, capabilities, `activeAgents`, `load`, `lastHeartbeatAt`. Populates the `RelayNode` roster fields (`relay/packages/sdk/src/messaging/types.ts`).
- **Reconnect inventory sync**: on reconnect, broker re-announces live agents (`agent_id`, name, `invocationId`, `session_ref`) for reconciliation; relaycast releases duplicates (first-to-completed wins, #1056 §7).
- **Deregister** on graceful shutdown so the node drops out of the roster promptly.
- Mark node `offline` when heartbeats lapse; the bounded-durable mailbox (already shipped) holds messages until TTL.

## Acceptance criteria

1. `nodes.list()` shows accurate `status` / `activeAgents` / `load` / `lastHeartbeatAt` for a serving node.
2. Killing the node's network for < TTL then restoring it: queued spawns drain, no duplicates (reconcile verified).
3. Graceful `Ctrl-C` deregisters the node from the roster within one heartbeat interval.
4. A node whose heartbeats lapse is marked `offline`; messages held, not lost, until TTL.

## Out of scope

- Placement decision logic (p12).
- Cross-node session migration (deferred, #1056 §10).
