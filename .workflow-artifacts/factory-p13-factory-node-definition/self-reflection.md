# factory-p13-factory-node-definition self-reflection

## Changed files

- `src/node/factory-node.ts`
  - Added the factory fleet node-definition builder.
  - Reads/parses NodeConfig-compatible shapes.
  - Advertises configured `spawn:claude`, `spawn:codex`, and `workflow:run` capabilities with workspace and clone path metadata.
  - Maps placed actions to configured checkouts.
  - Runs `spawn:*` through broker `ctx.spawnAgent(...)` with the same Agent Relay MCP harness wiring used by `InternalFleetClient`.
  - Runs `workflow:run` by shelling out to `relayflows run <workflow>` in the mapped checkout.
  - Provides an `inventory.sync` payload helper for reconnect reconciliation proofs.
- `src/node/factory.node.ts`
  - Default-exported `defineNode(...)` entrypoint for `agent-relay fleet serve ./src/node/factory.node.ts`.
- `src/node/factory-node.test.ts`
  - Covered manifest advertisement, spawn handling, workflow handling, checkout rejection, config parsing, and inventory sync payloads.
- `src/fleet/internal-fleet-client.ts`
  - Exported the existing MCP harness helpers so the node handler uses the same local harness path.
- `src/fleet/relay-fleet-client.ts`
  - Added `repo` and `clonePath` to the spawn action payload so cloud placement can send checkout identity to nodes.
- `src/fleet/relay-fleet-client.test.ts`
  - Covered the new repo/clone path spawn payload fields.
- `src/ports/fleet.ts`
  - Added `repo` and `clonePath` to `SpawnInput`.

## Spec coverage

- NodeConfig-only registration: `readFactoryNodeConfigSync()` reads `AGENT_RELAY_FACTORY_NODE_CONFIG`, `FACTORY_NODE_CONFIG`, `FACTORY_CONFIG`, or `factory.node.json`; `parseFactoryNodeConfig()` accepts node-only, split, and legacy combined config shapes.
- Capability advertisement: the node manifest only includes configured factory capabilities and attaches `workspaceId`, `cloneRoot`, and `clonePaths` metadata.
- Cold broker flow: verified installed `agent-relay fleet serve` source calls `startBrokerWithPortFallback(...)` before `serveFleetSidecar(...)`.
- `spawn:claude` / `spawn:codex`: handlers resolve the mapped checkout and call `ctx.spawnAgent(...)` with PTY harness config and MCP injection.
- `workflow:run`: handler resolves the mapped checkout and runs `relayflows run <workflow>` there; workflow inputs are also exposed as `RELAYFLOWS_INPUTS_JSON` and `FACTORY_WORKFLOW_INPUTS_JSON`.
- Reconnect reconcile: added deterministic `factoryNodeInventorySync()` payload construction for live agents.
- No orchestration logic: no triage, merge gate, batch state, Linear state, or cloud watch behavior was added under `src/node`.

## Tests and proofs run

- `npm test -- src/node/factory-node.test.ts src/fleet/relay-fleet-client.test.ts src/fleet/internal-fleet-client.test.ts`
- `npm test -- src/node/factory-node.test.ts`
- `npm test`
- `npm run build`
- Loader proof: imported installed `agent-relay` fleet loader by filesystem path and loaded `./src/node/factory.node.ts` from a temp NodeConfig; it returned configured `spawn:codex` and `workflow:run` capabilities.
- Cold-broker source proof: inspected `node_modules/agent-relay/dist/cli/commands/fleet.js`; `runFleetServe()` loads the node definition, then calls `startBrokerWithPortFallback(...)`, then calls `serveFleetSidecar(...)`.

## Repo-rule alignment

- No repo-local `AGENTS.md` was present in this worktree.
- Edits stayed within declared implementation targets `src/fleet` and `src/node`, plus this required `.workflow-artifacts/.../self-reflection.md`.
- No orchestration code was added to the node path.
- Tests cover every testable behavior added in this pass.

## Remaining risks

- A live Relaycast end-to-end run was not possible in this non-interactive environment because it would require workspace credentials and a long-running `fleet serve` process.
- The installed `agent-relay` sidecar owns the actual wire-level `node.register`, heartbeat, `node.deregister`, action result, delivery ack, and reconnect behavior. This package provides the node definition and handlers that the sidecar serves.
- Package metadata was not changed because the declared editable targets excluded `package.json`; consumers can serve `./src/node/factory.node.ts` from source or the built `dist/node/factory.node.js`.
