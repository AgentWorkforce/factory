Title: [factory] Phase 3 — RelayFleetClient: consume the relay fleet protocol (replace the relay#1056 stub)

Team: AR
Suggested status: Ready for Agent
Repo label: factory (@agent-relay/factory)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md §6 (Phase 3)
Depends on: relay fleet protocol shipped (relay#1056 / RFC), p4 (published package)
Related: relay/specs/fleet-delivery.md §6/§7/§9

---

## Problem

`@agent-relay/factory`'s `RelayFleetClient` is a stub: `pear/packages/factory-sdk/src/fleet/relay-fleet-client.ts` throws `new Error('RelayFleetClient not implemented — see relay#1056')` for every method. The factory brain (Phase 2) needs a real client to emit spawns. Per epic v2 §6, this is a **thin client of the fleet protocol** — it consumes RFC semantics, it does not fork them.

## Design

Implement `RelayFleetClient implements FleetClient` (the port at `pear/packages/factory-sdk/src/ports/fleet.ts:37–51`) against the relay fleet protocol. The port's methods map directly onto the RFC control surface:

| FleetClient method (ports/fleet.ts) | Fleet protocol (RFC §9 Broker↔Relaycast) |
|---|---|
| `spawn(SpawnInput)` → `SpawnResult` | `action.invoke` (create) with `{ capability, node?, session_ref?, invocationId }` (RFC §6) |
| `resume({sessionRef, node, capability})` | targeted spawn to origin node + `session_ref` (RFC §8.2) |
| `release(name, reason?)` | `action.invoke` (release) |
| `roster()` → `{agents, nodes:[{name,capabilities,live}]}` | node discovery query (RFC §9 roster) |
| `onAgentMessage` / `onAgentExit` / `onDeliveryFailed` | `deliver` / `action.result` / delivery events |

- **`SpawnInput.capability`** is `'spawn:claude' | 'spawn:codex'` today (ports/fleet.ts:4–15); extend the type to include **`'workflow:run'`** for the workflow recipe.
- **invocationId lifecycle (RFC §7):** the client supplies an `invocationId` (idempotency key); observes `pending → dispatched(node) → completed(agent_id)`; relies on the fleet for dedup, reschedule-on-node-loss, and reconcile (first-to-`completed` wins). The factory does NOT implement placement, scheduling, or reconcile — it observes the lifecycle and reports completion upward.
- **`workflow:run` capability handler (open question 1 — proposed):** the node-side handler for `{capability:'workflow:run', workflow:<path>}` **shells out to `relayflows run <workflow>`** in the node's repo checkout. Rationale: the node already has the harness + checkout; child spawns the workflow emits ride the same fleet; no embedded runtime or service dependency. The `relayflows` CLI is a dependency of the node's harness definition (Phase 4). Confirm with operator before building.
- **No reuse of `InternalFleetClient`'s broker-direct path** beyond reference — `RelayFleetClient` talks the fleet protocol, not the local `HarnessDriverClient`.

## End-to-end verification (captured artifact required)

1. Stand up one eligible node advertising `spawn:claude` (any machine).
2. From cloud factory triage, an `agent:single` issue emits one `RelayFleetClient.spawn{capability:'spawn:claude', persona, invocationId}`.
3. Capture: the spawn lands on the live node (placement is fleet-side — the factory targeted no node), the agent runs in that node's checkout, the `invocationId` progresses `pending → dispatched → completed`.
4. Capture the factory observing `completed` and writing back to Linear.
5. Capture a node-loss reschedule: kill the node mid-spawn; the same `invocationId` reschedules to another eligible node; no double-spawn.

## Acceptance criteria

1. `RelayFleetClient` implements every `FleetClient` method against the fleet protocol; no method throws `relay#1056`.
2. `SpawnInput.capability` includes `'workflow:run'`.
3. An `agent:single` spawn round-trips the fleet and lands on whichever eligible node is live (factory targeted none).
4. Completion observed via the `invocationId` lifecycle; Linear writeback fires.
5. Node-loss mid-spawn reschedules the same `invocationId` with no double-spawn (captured).
6. The `workflow:run` handler decision (shell-out to `relayflows run`) is documented + implemented or explicitly deferred with the chosen alternative recorded.

## Out of scope

- The cloud brain that emits the spawns (Phase 2).
- Node registration / the node-side capability handlers' full impl (Phase 4 owns node-side; this issue defines the `workflow:run` contract).
- Any factory-side placement/scheduling — fleet owns it (RFC §6).

## Related

- Epic v2 §6 (thin fleet client), §3.5 (recipes → capabilities).
- `pear/packages/factory-sdk/src/ports/fleet.ts:37–51` (port), `src/fleet/relay-fleet-client.ts` (stub), `src/fleet/internal-fleet-client.ts` (local reference).
- RFC §6 (spawn/placement), §7 (invocationId lifecycle), §8.2 (resume = origin-targeted spawn + session_ref), §9 (control surface).
- Phase 4 (node-side `workflow:run` + `spawn:*` handlers).
