Title: [factory] pear: Phase 1.5 — teardown the in-Pear factory daemon; reduce Pear to a read-only cloud view

Team: AR
Suggested status: Ready for Agent
Repo label: pear
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p4 (extraction) merged — pear consumes `@agent-relay/factory` from npm
Epic: factory-cloud-watches-local-node-linear-issue.md §8 (Phase 1.5)

---

## Context

After p1–p4, the factory brain is a published package and the cloud lift (Phase 2) is where the daemon actually runs. Pear must stop running the factory as a local Electron-spawned daemon and become a read-only viewer of cloud state. This PR deletes the local-daemon process model and rewires the UI/IPC.

## Goal

Pear no longer spawns or manages a factory daemon. Its only remaining factory ties are: (a) consuming `@agent-relay/factory` types, (b) editing the local `NodeConfig`, (c) showing a read-only status view backed by cloud.

## Scope

**Delete (local-daemon process model):**
- `src/main/factory-manager.ts` — `FactoryManager` class + `factoryManager` singleton (spawns the daemon child process ~line 175, reads loop heartbeat/liveness, tracks in-flight registry).
- The `pear factory <action>` passthrough block in `bin/pear.mjs` (forwards to `packages/factory-sdk/bin/...`, e.g. `pear factory start`, `pear factory reap-orphans`).
- On-disk daemon artifacts: loop heartbeat file, in-flight registry file, `/tmp/factory-run/` working dir handling.

**Rewire (control panel → read-only):**
- `src/shared/types/ipc.ts` — `factory` IPC namespace currently exposes `status / start / stop / readConfig / saveConfig / onEvent` (+ `FactoryStatus`, `FactoryAgentStatus`, `FactoryLogLine`, `FactoryEvent`, `FactoryConfigReadResult`). Drop `start`/`stop`; repoint `status`/`onEvent` at cloud state; scope `readConfig`/`saveConfig` to **NodeConfig** only.
- `src/renderer/src/components/factory/FactoryPage.tsx` — convert control panel → read-only cloud-backed status view (or remove if the cloud web UI subsumes it).
- Renderer touchpoints to update: `src/renderer/src/App.tsx` (route), `src/renderer/src/stores/ui-store.ts`, `src/renderer/src/components/common/AppTopBar.tsx`, `src/renderer/src/components/common/CommandMenu.tsx`, `src/renderer/src/components/settings/AccountSettings.tsx`, `src/renderer/src/lib/ipc-mock.ts`.

## Acceptance criteria

1. `src/main/factory-manager.ts` is gone; no main-process code spawns a factory daemon.
2. `pear factory ...` CLI passthrough removed from `bin/pear.mjs`.
3. The `factory` IPC namespace no longer exposes `start`/`stop`; `status`/`onEvent` read cloud state.
4. `FactoryPage` renders read-only status (no start/stop/configure controls beyond NodeConfig editing).
5. Pear builds + tests clean; app launches with no factory-daemon side effects.

## Out of scope

- The cloud-side runtime (Phase 2). This PR assumes cloud either already hosts the brain or the view tolerates an empty cloud state.
- NodeConfig authoring UX polish (basic edit is enough here).

## Risks

- Dangling imports of `factoryManager` / removed IPC channels — grep `factory` across `src/` before finishing (the touchpoint list above is the known set as of 2026-06-16).
