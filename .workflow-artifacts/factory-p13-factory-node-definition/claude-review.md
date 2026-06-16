# Fresh-eyes review — factory-p13 (factory node-definition)

## Verdict: APPROVE WITH MINOR FOLLOW-UPS

The implementation is correct, well-tested, and faithful to the spec's architectural boundary
(node-definition + handlers here; wire-level register/heartbeat/deregister/ack owned by the
installed `agent-relay` sidecar). No correctness defects or failing acceptance criteria were found.

### Verification performed
- `npm run build` (tsc -p tsconfig.build.json): **clean**.
- `npx vitest run` (full suite): **30 files / 483 tests pass**.
- Targeted: `src/node/factory-node.test.ts` + `src/fleet/relay-fleet-client.test.ts`: **18 pass**.
- Cross-checked handler signatures against `@agent-relay/fleet` `dist/index.d.ts`
  (`action`, `defineNode`, `FleetActionContext.spawnAgent`, `FleetSpawnAgentInput`) — all align.
- Confirmed cloud side (`relay-fleet-client.ts`) now emits `repo`/`clonePath` in the spawn action
  payload and `definedRecord` strips undefined keys (no regression for self-node spawns).
- No repo-local AGENTS.md/CLAUDE.md present; edits stayed within declared targets `src/fleet` + `src/node`.

### Spec acceptance mapping
- AC1 (NodeConfig-only registration, capabilities visible): node manifest advertises only configured
  capabilities with `workspaceId`/`cloneRoot`/`clonePaths` metadata + `repo:`/`workspace:` tags. ✓ (unit-proven)
- AC2 (single/team/workflow shapes): `spawn:claude`/`spawn:codex` via `ctx.spawnAgent` with MCP harness
  wiring; `workflow:run` shells `relayflows run`. ✓
- AC4 (no new CLI command): entry stays `fleet serve`; nothing added. ✓
- AC3/AC5 (drain on reconnect, cold-broker): runtime acceptance; correctly delegated to sidecar and
  validated by source inspection only (live Relaycast run not possible in this env). Acceptable as a
  documented residual risk — see F4.

---

## Findings (all LOW / MEDIUM — none block merge)

### F1 [MEDIUM] — Node definition is not reachable via the package's `exports` map
- **File:** `package.json` (`exports`), `src/index.ts`
- **Problem:** The deliverable (`createFactoryNodeDefinition`, `readFactoryNodeConfigSync`, and the
  default-export entry `src/node/factory.node.ts`) is not exported from `src/index.ts` and has no
  `exports` subpath (only `.`, `./testing`, `./writeback` exist). The only way to consume it is a raw
  path into `dist`. The spec explicitly allows "a packaged default def referenced by name"
  (Scope + AC1's "factory-node-def"), which this gap leaves unmet — a consumer cannot do
  `agent-relay fleet serve @agent-relay/factory/node` or import the builder from the package root.
- **Required fix:** Add an `exports` entry (e.g. `"./node": { "import": "./dist/node/factory.node.js" }`)
  and/or re-export `createFactoryNodeDefinition` + `readFactoryNodeConfigSync` from `src/index.ts`.
  Confirm `files`/`dist` ships `dist/node/*`.
- **Required test:** A test asserting the public entry resolves (import from the package root or the
  `./node` subpath returns a `FleetNodeDefinition` whose manifest carries the configured capabilities),
  mirroring the existing loader proof but against the published surface rather than a source path.

### F2 [LOW/MEDIUM] — One-command flow is undocumented (spec scope item not delivered)
- **File:** `README.md` (no docs change in this PR)
- **Problem:** Spec Scope: "Document the one-command flow: `agent-relay fleet serve ./factory.node.ts`".
  No README/doc was added or updated, and the literal path in the spec (`./factory.node.ts`) differs
  from the shipped location (`src/node/factory.node.ts`) and the default config path
  (`factory.node.json`, resolved relative to CWD). A user following the spec verbatim will not find the file.
- **Required fix:** Add a short README section documenting the exact serve command, the
  `FACTORY_NODE_CONFIG` / `AGENT_RELAY_FACTORY_NODE_CONFIG` env vars, the default `factory.node.json`
  location, and a minimal `NodeConfig` example. Reconcile the path with whatever F1 settles on.
- **Required test:** N/A (docs) — but if a packaged name is introduced in F1, document that name.

### F3 [LOW] — `factoryNodeInventorySync` is exported + unit-tested but wired to nothing
- **File:** `src/node/factory-node.ts:198`
- **Problem:** The `inventory.sync` payload builder is dead within this package — referenced only by
  its own test. The self-reflection frames it as a "reconnect reconcile proof" helper, and the sidecar
  owns the actual reconnect, so this is defensible, but as shipped it is unreferenced API surface.
- **Required fix (choose one):** (a) wire it into the node definition / a triggerable handler so the
  node actually emits `inventory.sync` on reconnect, or (b) if it is genuinely consumed by the sidecar
  via the default export elsewhere, add a comment pointing to that consumer; otherwise keep but flag as
  provisional.
- **Required test:** If wired (a), add a handler-level test invoking the inventory path through
  `invokeNodeHandler`/trigger; if kept as a pure helper, the existing unit test suffices.

### F4 [LOW] — Empty `clonePaths` disables the checkout-allowlist guard
- **File:** `src/node/factory-node.ts:334-347` (`resolveCheckoutPath`)
- **Problem:** When `config.clonePaths` is empty (`configuredPaths.size === 0`), an explicit
  cloud-supplied `clonePath`/`cwd` is accepted **unconditionally**. `NodeConfigSchema` defaults
  `clonePaths` to `{}`, so a config that lists `capabilities` but omits `clonePaths` would let the
  cloud direct spawns into arbitrary local directories — weaker than the "checkout is not advertised"
  rejection that protects the configured case. This is a scope-safety edge, not a live bug (real
  configs carry clonePaths), and the rejection path is otherwise correctly tested.
- **Required fix:** Either require at least one `clonePath` (or `cloneRoot`) when any `spawn:*`
  capability is advertised, or treat empty `clonePaths` as "reject all explicit paths" rather than
  "allow all". Tie into existing `factoryScope` safety if appropriate.
- **Required test:** Add a case: NodeConfig with capabilities but empty `clonePaths` + an explicit
  `cwd` → expect rejection (or expect a config-time error), asserting the guard is not bypassed.

---

## Notes (no action required)
- Cloud→node payload sends both `clone_path` and `clonePath` (and the schema accepts both via
  `.transform`): redundant but harmless and intentional for snake/camel tolerance.
- `runRelayflowsWorkflow` correctly rejects on non-zero exit so failures surface as `action.result`
  errors; env injection (`RELAYFLOWS_INPUTS_JSON`, `FACTORY_WORKFLOW_INPUTS_JSON`, `RELAY_INVOCATION_ID`)
  is sound.
- Architectural boundary is right: no triage/merge/state/cloud-watch logic leaked into `src/node`.

## Summary
Build + full test suite pass; all directly-testable acceptance criteria are unit-proven. No correctness
or test-coverage defects. Four minor follow-ups, the most material being **F1** (expose the node
definition through the package `exports`/index so it can be "referenced by name" per the spec) and
**F2** (the spec-mandated one-command documentation was not added). Safe to merge; recommend addressing
F1/F2 before users are pointed at the package.
