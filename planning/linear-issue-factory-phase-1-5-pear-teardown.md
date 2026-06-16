Title: [factory] Phase 1.5 — Pear teardown: delete the in-Pear factory daemon

Team: AR
Suggested status: Ready for Agent
Repo label: pear
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md §7 (Phase 1.5)
Depends on: p4 (pear consumes published @agent-relay/factory)
Related: epic v2 §2 (no factory execution in Pear)

---

## Problem

Once the factory brain is in cloud (Phase 2) and execution is fleet-side (Phases 3/4), Pear must stop running an in-process factory daemon. INVARIANT under the unified-node reframe — Pear was never the right home; this teardown is architecture-independent.

## Design

Delete the local-daemon model; reduce Pear to (optionally) a read-only viewer of cloud factory state.

**Delete:**
- `pear/src/main/factory-manager.ts` — the `FactoryManager` class + `factoryManager` singleton that `spawn()`s the daemon child process, reads heartbeat/liveness, tracks the in-flight registry.
- The `pear factory <action>` passthrough in `pear/bin/pear.mjs` (`pear factory start`, `reap-orphans`).
- The on-disk daemon artifacts: loop heartbeat file, in-flight registry, `/tmp/factory-run/`.

**Rewire:**
- `pear/src/shared/types/ipc.ts` — the `factory` IPC namespace (`status / start / stop / readConfig / saveConfig / onEvent` + `FactoryStatus`, `FactoryAgentStatus`, `FactoryLogLine`, `FactoryEvent`, `FactoryConfigReadResult`): drop `start`/`stop`; repoint `status`/`onEvent` at cloud state; scope `readConfig`/`saveConfig` to NodeConfig only (Phase 4 node-registration config).
- `pear/src/renderer/src/components/factory/FactoryPage.tsx` — convert the control panel to a **read-only cloud-state viewer**, or remove if the cloud web UI subsumes it.
- Renderer touchpoints: `pear/src/renderer/src/App.tsx` (route), `stores/ui-store.ts`, `components/common/AppTopBar.tsx`, `components/common/CommandMenu.tsx`, `components/settings/AccountSettings.tsx`, `lib/ipc-mock.ts`.

**Keep:** nothing factory-specific in Pear's main process. Pear becomes a consumer of `@agent-relay/factory` types + (optional) a viewer of cloud state.

## End-to-end verification (captured artifact required)

1. `npm run typecheck` / build pear after the teardown — capture clean output.
2. Capture: launching Pear spawns no factory daemon process and creates no `/tmp/factory-run/`.
3. Capture: `pear factory ...` is no longer a command (or removed from `bin/pear.mjs`).
4. If the read-only viewer is kept: capture it rendering cloud factory state (in-flight issues) fetched from the cloud worker.

## Acceptance criteria

1. `src/main/factory-manager.ts` is gone; no main-process code spawns a factory daemon.
2. `pear factory` passthrough removed from `bin/pear.mjs`; no `/tmp/factory-run/` artifacts.
3. `factory` IPC namespace no longer exposes `start`/`stop`; `status`/`onEvent` read cloud state.
4. `FactoryPage` is read-only (or removed); all renderer touchpoints updated; `grep factory src/` shows no dangling daemon references.
5. Pear builds + tests clean.

## Out of scope

- The cloud brain (Phase 2), fleet client (Phase 3), node registration (Phase 4).
- NodeConfig authoring UX polish.

## Related

- Epic v2 §2 (Pear = optional read-only consumer).
- `pear/src/main/factory-manager.ts`, `bin/pear.mjs`, `src/shared/types/ipc.ts`, `src/renderer/src/components/factory/FactoryPage.tsx`.
- Executable workflow: `factory-build/wave3-cloud-lift/01-p5-pear-teardown.ts`.
