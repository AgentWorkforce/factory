Title: [factory] pear: extract Phase 1 prep #2 — split FactoryConfig into WorkspaceConfig + NodeConfig

Team: AR
Suggested status: Ready for Agent
Repo label: pear
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)

---

## Context

Prep PR #2 of 4 for the factory extraction epic. See PR #1 (state store) for the audit and the broader plan. This PR is independent of PR #1 — can be done in parallel or after.

Today's `FactoryConfig` (`packages/factory-sdk/src/config/schema.ts`) mixes two concerns: (a) per-workspace orchestration policy that lives in the cloud worker, and (b) per-node local execution details that live on the user's machine. After extraction + Phase 2 cloud lift, those two halves live in completely separate locations — they need clean schema boundaries before the lift.

## Goal

Split `FactoryConfig` into two Zod schemas:

- **`WorkspaceConfig`** — set once per workspace in the cloud web UI (or via `agent-relay factory config`). Lives in cloud durable storage.
- **`NodeConfig`** — read by `agent-relay local factory` from a local file on the user's machine. Lives on disk.

Keep `FactoryConfig` as a composed alias (`WorkspaceConfig & NodeConfig`) for backwards compatibility during the rest of the prep stack. The orchestrator's internal types update to consume the split. Today's monolithic `factory.config.json` continues to be accepted; the loader detects the legacy shape and splits it in-memory.

## Design

### Split (per epic §5)

`WorkspaceConfig` (cloud-side, what triage / merge gate / batching consume):

```ts
{
  workspaceId: string
  subscription: { teams: string[]; labels: string[] }
  repos: { byLabel: Record<string, string>; default: string }
  batchSize: number
  mergePolicy: 'never' | 'auto-on-green' | ...
  safety: { requireTitlePrefix: string; requireTeamKey: string }
  slack?: { channel: string }
  stateIds: { readyForAgent, agentImplementing, done, inPlanning, humanReview? }
  terminalState?: 'done' | 'human-review'   // see AR-272 (Human Review)
  dispatch: { maxAttempts, errorCooldownMs, ... }
}
```

`NodeConfig` (node-local, what `agent-relay local factory` reads):

```ts
{
  workspaceId: string  // confirms which workspace this node serves
  capabilities: string[]  // e.g. ['spawn:claude', 'spawn:codex']
  repoPaths: Record<string, string>  // 'AgentWorkforce/pear' -> '/abs/path/to/pear'
  dryRun?: boolean
  factoryLoopHeartbeatPath?: string   // moved here from FactoryConfig
  factoryLoopRegistryPath?: string
}
```

### Legacy shape acceptance

`factory.config.json` today has fields from both halves. The loader (where `FactoryConfigSchema.parse` is called) extracts the workspace fields into a `WorkspaceConfig` and the node fields into a `NodeConfig`. Both must be present for the orchestrator to construct; once split files are introduced later, the loader supports either form.

### Schema exports

```ts
// src/config/index.ts
export {
  WorkspaceConfig,
  WorkspaceConfigSchema,
  NodeConfig,
  NodeConfigSchema,
  FactoryConfig,           // = WorkspaceConfig & NodeConfig
  FactoryConfigSchema,     // = WorkspaceConfigSchema.merge(NodeConfigSchema)
  loadFactoryConfig,       // accepts legacy or split form
}
```

### Internal consumer updates

`FactoryLoop` is constructed with `WorkspaceConfig`; the entry point (`bin/fleet.mjs` → `cli/fleet.ts`) is what reads `NodeConfig` to decide which workspace + which repo paths. Wire the split through cleanly — no `FactoryConfig` references inside the orchestrator after this PR.

### Tests

- Existing `schema.test.ts` cases continue to pass.
- New cases: load a legacy `factory.config.json` (todays shape) → loader returns both halves correctly. Load a split form (two files) → also works.
- Conformance: `WorkspaceConfig & NodeConfig` equals the legacy `FactoryConfig` exactly (no fields lost, no extras introduced).

## End-to-end verification

1. `npm test -w @pear/factory-sdk` — full suite green.
2. Run `pear factory start --mode live --config ./factory.config.json` — behavior identical to pre-PR.
3. Demonstrate split form: write `workspace.config.json` + `node.config.json`, run with `--workspace-config` / `--node-config` flags (or whatever new CLI flag pair makes sense), confirm equivalence with the legacy single-file form.

## Acceptance criteria

1. `WorkspaceConfigSchema` and `NodeConfigSchema` defined; `FactoryConfigSchema` is a merge of the two with no field drift.
2. `loadFactoryConfig` accepts both the legacy monolithic shape and the new split shape.
3. `FactoryLoop`'s internal config type is `WorkspaceConfig` (or a tighter subset). No `NodeConfig`-specific fields reach the orchestrator.
4. All existing tests pass; new tests cover both shapes.
5. E2E run with the legacy `factory.config.json` shows no regression.

## Out of scope

- Moving the workspace config to cloud durable storage (Phase 2).
- Web UI for setting workspace config (Phase 4).
- Multi-node configs / hot-reload — defer.

## Related

- Epic: `factory-cloud-watches-local-node-linear-issue.md` §5.
- `pear/packages/factory-sdk/src/config/schema.ts` — schema target.
- `pear/factory.config.json` — current monolithic config (loader must keep accepting).
- Sibling PR #1: `linear-issue-factory-extract-p1-state-store-port.md`.
- Sibling PR #3: `linear-issue-factory-extract-p3-publish-prep.md`.
