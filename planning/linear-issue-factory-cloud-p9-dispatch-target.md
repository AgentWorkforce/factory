Title: [factory] cloud: Phase 2 — make proactive-runtime dispatch target pluggable (daytona | fleet-node)

Team: AR
Suggested status: Ready for Agent
Repo label: cloud
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p6 (orchestrator hosted)
Epic: factory-cloud-watches-local-node-linear-issue.md §2, §6, §8 (Phase 2)

---

## Context

This is the **crux** of the whole epic. Today cloud's proactive-runtime spawns work into a **Daytona sandbox** (`createDeploymentSandboxRuntime` in `deployment-trigger-delivery.ts`). To run agents on the user's laptop, dispatch must target a **fleet node** instead. This PR introduces the abstraction; p10 implements the fleet-node side.

## Goal

A single `DispatchTarget` interface with two implementations — `daytona` (existing behavior, refactored behind the interface) and `fleet-node` (new, implemented in p10). The orchestrator chooses per workspace/issue without forking the delivery path.

## Scope

- Define `DispatchTarget` in/near `cloud/.../proactive-runtime/`:
  ```ts
  interface DispatchTarget {
    spawn(input: SpawnRequest): Promise<SpawnHandle>   // SpawnRequest carries scope, repo, capability, payload
    poll(handle: SpawnHandle): Promise<RunStatus>
    cancel(handle: SpawnHandle): Promise<void>
  }
  ```
- Refactor the current Daytona path (`deliverDeploymentTrigger` / `createDeploymentSandboxRuntime` / `pollDeploymentTriggerRun`) to implement `DispatchTarget` — no behavior change.
- Add a `targetKind: 'daytona' | 'fleet-node'` selector resolved from `WorkspaceConfig` (default `daytona` so nothing regresses).
- Leave `fleet-node` as a stub/throw here; p10 implements it.

## Acceptance criteria

1. Existing Daytona dispatch goes through `DispatchTarget` with zero behavior change (regression-tested against current proactive-runtime flows).
2. `targetKind` is selectable per workspace and defaults to `daytona`.
3. The fleet-node branch is reachable but explicitly unimplemented (clear error), ready for p10.

## Out of scope

- The fleet-node implementation + relaycast `agent.create` wiring (p10).
- Placement/targeting logic (p12).
