# Fix log — factory-p13 (factory node-definition)

All four review findings (F1–F4) addressed. Build clean; full suite green (487 tests,
+4 new vs. the 483 at review).

---

## F1 [MEDIUM] — Node definition now reachable via the package surface

**Fixed.**
- `package.json`: added a `./node` export subpath
  (`"./node": { "types": "./dist/node/factory.node.d.ts", "import": "./dist/node/factory.node.js" }`),
  mirroring the existing `.`/`./testing`/`./writeback` entries. So a consumer can do
  `agent-relay fleet serve @agent-relay/factory/node`.
- `src/index.ts`: re-exported the builder + loader surface from the package root —
  `createFactoryNodeDefinition`, `readFactoryNodeConfigSync`, `parseFactoryNodeConfig`,
  `resolveFactoryNodeConfigPath`, `factoryNodeInventorySync`, `runRelayflowsWorkflow`, the
  three config-env constants, plus the supporting types
  (`FactoryNodeDefinitionOptions`, `WorkflowRunner(Input|Result)`, `FactoryNodeInventory*`).
- `files`/`tsconfig.build` already ship `dist/node/*` (verified `dist/node/factory.node.js`
  and `factory-node.js` emit).

**Tests added** (`src/node/factory-node.test.ts`):
- "exposes the node builder through the published package root entry" — imports from
  `../index` and asserts `createFactoryNodeDefinition`/`readFactoryNodeConfigSync` resolve
  and the built definition's manifest carries the configured capabilities.
- "resolves a FleetNodeDefinition from the packaged default node export" — writes a temp
  `factory.node.json`, points `FACTORY_NODE_CONFIG` at it, dynamically imports
  `./factory.node`, and asserts the default export is a `FleetNodeDefinition` whose manifest
  advertises the config's capabilities/tags.

**Note (pre-existing, out of F1 scope):** the emitted `dist` uses extensionless relative
imports across the whole package — the existing `.` entry (`dist/index.js`) fails identically
under raw Node ESM resolution and is resolved by the consumer's loader/bundler. The new
`./node` entry follows the exact same convention as every other export; this PR does not
introduce the characteristic and fixing it package-wide is well outside this finding.

## F2 [LOW/MEDIUM] — One-command serve flow documented

**Fixed.** `README.md` gained a "Serving as a fleet node (one-command flow)" section:
- The exact serve command (`agent-relay fleet serve @agent-relay/factory/node`).
- The config-resolution precedence (`AGENT_RELAY_FACTORY_NODE_CONFIG` → `FACTORY_NODE_CONFIG`
  → `FACTORY_CONFIG` → `./factory.node.json`), reconciling the spec's literal
  `./factory.node.ts` against the shipped packaged-name + default-`factory.node.json` reality.
- The builder-import alternative (`createFactoryNodeDefinition` + `readFactoryNodeConfigSync`).
- A minimal `NodeConfig` example and a note that `clonePaths`/`cloneRoot` is the checkout
  allowlist (ties into F4).

## F3 [LOW] — `factoryNodeInventorySync` documented as provisional sidecar surface

**Fixed (option b/c).** Added a doc comment on `factoryNodeInventorySync`
(`src/node/factory-node.ts`) explaining it is the `inventory.sync` payload builder consumed
by the installed `agent-relay` sidecar's reconnect/drain handshake via the default node
export — exported and unit-proven here so the payload shape stays pinned to `NodeConfig`,
even though no in-package handler invokes it directly. The existing unit test suffices per
the finding.

## F4 [LOW] — Empty `clonePaths` no longer disables the checkout allowlist guard

**Fixed.** `resolveCheckoutPath` (`src/node/factory-node.ts`) previously accepted any
cloud-supplied `clonePath`/`cwd` unconditionally when `clonePaths` was empty
(`configuredPaths.size === 0 || …`). Removed the "allow all when empty" bypass; an explicit
path is now honored only when it is one of the configured `clonePaths` **or** lives under the
advertised `cloneRoot` (new `isWithinCloneRoot` helper, using `path.sep`-bounded prefix
matching so `/work-sibling` is not treated as under `/work`). A config that advertises a
`spawn:*` capability but provides neither `clonePaths` nor `cloneRoot` now rejects every
explicit checkout path.

**Tests added** (`src/node/factory-node.test.ts`):
- "rejects an explicit checkout when clonePaths is empty and no cloneRoot is advertised" —
  empty `clonePaths`, no `cloneRoot`, explicit `cwd` → throws
  `checkout path is not advertised by this node`.
- "accepts an explicit checkout under the advertised cloneRoot even with empty clonePaths" —
  `cloneRoot: '/work'`, empty `clonePaths`: `cwd: /work/relay` accepted; `cwd: /work-sibling/relay`
  rejected (proves the prefix guard isn't fooled by a sibling directory).

---

## Notes / non-findings

- One full-suite run surfaced a failing `internal-fleet-client.test.ts` →
  `protectedPids()` returned `[68009, 95499]` instead of `[68009]`. This is an **environment
  artifact, not a code defect**: this fixer runs inside an agent-relay session, which sets
  `AGENT_RELAY_STATE_DIR` to a live `.agentworkforce/relay/connection.json` whose `pid` is
  `95499`; `protectedPids()` correctly reads it. It is unrelated to this PR's changes (that
  file's tracked diff doesn't touch `protectedPids`) and was not a review finding. Confirmed
  green in a clean env via `env -u AGENT_RELAY_STATE_DIR`.

## Commands run

```bash
npm run build                                   # tsc -p tsconfig.build.json — clean
npx vitest run src/node/factory-node.test.ts    # 11 passed (was 7; +2 F4, +2 F1)
npx vitest run src/fleet/internal-fleet-client.test.ts          # 1 env-artifact failure (AGENT_RELAY_STATE_DIR)
env -u AGENT_RELAY_STATE_DIR npx vitest run src/fleet/internal-fleet-client.test.ts  # 29 passed
env -u AGENT_RELAY_STATE_DIR npx vitest run    # 30 files / 487 tests passed
```
