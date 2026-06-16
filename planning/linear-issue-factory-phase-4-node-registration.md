Title: [factory] Phase 4 — node registration: `agent-relay local factory` registers a machine as a fleet node

Team: AR
Suggested status: Ready for Agent
Repo label: relay (+ @agent-relay/factory node definition)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md §7 (Phase 4)
Depends on: p2 (NodeConfig), Phase 3 (RelayFleetClient + `workflow:run` contract), relay fleet protocol (RFC §9)
Related: relay/specs/fleet-delivery.md §6/§9; relay/packages/cli/.../commands/fleet.ts

---

## Problem

Today the operator runs `pear factory start` — a daemon that owns orchestration AND execution on one machine. Under the unified-node model there is no factory execution mode. The command becomes simply: **"register this machine as a node in the fleet with these capabilities and these repo checkout paths."** Orchestration lives in cloud (Phase 2); this command is effectively a relay broker started in a specific configuration.

## Design

`agent-relay local factory` (or reuse `agent-relay fleet serve <factory-node-def>`) registers a node:

- **Reads `NodeConfig`** (p2): `workspaceId`, `capabilities` (`spawn:claude` / `spawn:codex` / `workflow:run`), and repo checkout paths (`cloneRoot` / `clonePaths`, pear#369 compact form).
- **Registers + advertises** per RFC §9 control surface: `node.register` (name, capabilities, version, max_agents, tags, resume cursor), `node.heartbeat` (~10–15s: load, active_agents), `node.deregister` on shutdown, `inventory.sync` (re-announce live agents on reconnect: agent_id, name, invocationId, session_ref).
- **Handles `action.invoke`** from Relaycast (RFC §9 Relaycast→Broker): for `spawn:claude`/`spawn:codex`, spawn the harness in the mapped checkout (the existing local PTY path — `InternalFleetClient`'s `spawnPty` is the reference impl); for `workflow:run`, **shell out to `relayflows run <workflow>`** in the checkout (per Phase 3's contract). Emits `agent.register` / `action.result` / `delivery.ack` back.
- **No orchestration logic.** No triage, no merge-gate, no batch state — all cloud (Phase 2). The node is dumb compute that advertises what it can run.
- **The broker already auto-starts:** `agent-relay fleet serve` calls `startBrokerWithPortFallback` (`relay/packages/cli/src/cli/commands/fleet.ts:144`) before serving — one command boots the broker + registers the node, no separate `agent-relay up`.

A laptop, mac mini, EC2 box, or autospawned Daytona sandbox all run this same registration — they differ only in `capabilities` and `clonePaths` (epic v2 §2).

## End-to-end verification (captured artifact required)

1. From a machine with NO running broker, run `agent-relay local factory` with only a `NodeConfig`.
2. Capture: the broker auto-starts, the node appears in the fleet roster (`agent-relay fleet nodes`) with the advertised capabilities + a live heartbeat.
3. From cloud factory triage (Phase 2), an `agent:single` spawn placed by Relaycast onto this node executes in the correct local checkout; capture the agent running + `action.result` completion.
4. Capture an `agent:workflow` spawn: the node runs `relayflows run <workflow>` and any child spawns ride the fleet.
5. Capture reconnect reconcile: drop the node's network < TTL, restore; `inventory.sync` re-announces live agents; no duplicate spawns.

## Acceptance criteria

1. `agent-relay local factory` registers the machine as a node from `NodeConfig` alone; broker auto-starts (cold-broker proof).
2. Node advertises `capabilities` + `clonePaths`; appears in the roster with heartbeat.
3. A cloud-placed `spawn:claude` / `spawn:codex` executes in the mapped checkout; `action.result` reports completion.
4. A cloud-placed `workflow:run` runs `relayflows run <workflow>` in the checkout.
5. Reconnect inventory-sync reconciles without duplicate spawns (captured).
6. The command contains zero orchestration logic (no triage/merge/state).

## Out of scope

- The cloud brain that places work (Phase 2) and the client that emits it (Phase 3).
- Multi-node placement preference tuning (RFC §6 least-loaded is the v1 default).
- Cross-node session migration beyond resume = origin-targeted spawn (RFC §8.2).

## Related

- Epic v2 §2 (nodes), §5 (NodeConfig → node-registration config), §7 (Phase 4).
- RFC §9 (node lifecycle + control surface), §6 (placement), §8.2 (resume).
- `relay/packages/cli/src/cli/commands/fleet.ts:144` (broker auto-start), `pear/packages/factory-sdk/src/fleet/internal-fleet-client.ts` (local spawn reference).
- Phase 3 (`workflow:run` contract).
