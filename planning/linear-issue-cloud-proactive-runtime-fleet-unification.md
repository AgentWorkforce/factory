Title: [cloud] Proactive-runtime fleet unification — stop hardcoding Daytona; emit spawns into the fleet

Team: AR
Suggested status: Ready for Agent
Repo label: cloud
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md (sibling — same principle, different system)
Depends on: relay fleet protocol (RFC), RelayFleetClient pattern (Phase 3)
Related: relay/specs/fleet-delivery.md §6/§7

---

## Problem

`cloud/packages/web/lib/proactive-runtime/team-launch-n1.ts` hardcodes Daytona for every proactive spawn (granola, hn-monitor, spotify-releases, daytona-monitor):

- `const MEMBER_LOCAL_ROOT = "/home/daytona/workspace";` (line 245)
- `const daytonaAuth = resolveServerDaytonaAuthParams();` (line 803), spread into the credential bundle (line 829)
- `dispatchTeamLaunchN1(input, deps)` (lines 453–498) → `buildTeamLaunchMemberOptions` (148–230) → `launchMember` — a monolithic single-machine Daytona launch.

Under the unified-node model this is the same anti-pattern the factory extraction fixes: a cloud system that *chooses Daytona* instead of *emitting a spawn into the fleet* and letting Relaycast place it. Orthogonal to the factory extraction, same architectural principle.

## Design

Reframe proactive spawns as fleet spawns:

- `dispatchTeamLaunchN1` emits `spawn{ capability, persona }` into the fleet (same `RelayFleetClient` pattern as Phase 3) instead of building Daytona launch options.
- **Relaycast places it** (RFC §6: targeted or least-loaded) on any live eligible node; if none eligible AND the workspace has Daytona-autospawn permission, Relaycast spins an ephemeral Daytona node (epic v2 §2 fallback). The proactive runtime does NOT branch on this.
- Delete the Daytona-specific local root + `resolveServerDaytonaAuthParams` injection from the proactive path; placement/credentials become fleet/node concerns.
- Personas resolve from `AgentWorkforce/agents/<name>/persona.ts` (single source — confirm both factory team-recipe and proactive read the same registry, epic v2 §8 Q4).

## End-to-end verification (captured artifact required)

1. Trigger a proactive agent (e.g. hn-monitor cron tick) in a test workspace with a live non-Daytona node advertising the required capability.
2. Capture: the spawn lands on that node (NOT Daytona) — proving placement is fleet-side, not hardcoded.
3. Capture: with NO eligible node + autospawn permission, Relaycast spins an ephemeral Daytona node and the spawn lands there — proving the fallback works without proactive-side branching.
4. Capture the `invocationId` lifecycle to completion.

## Acceptance criteria

1. `team-launch-n1.ts` emits a fleet spawn; `MEMBER_LOCAL_ROOT` + `resolveServerDaytonaAuthParams` Daytona-specific injection are removed from the proactive path.
2. A proactive spawn lands on a live eligible non-Daytona node when one exists (captured).
3. With no eligible node + autospawn permission, an ephemeral Daytona node is autospawned by Relaycast (captured) — no proactive-side Daytona branch.
4. Personas resolve from `AgentWorkforce/agents/` (same registry as factory recipes).

## Out of scope

- The factory extraction itself (epic v2 Phases 1–4) — this is a sibling.
- Relaycast's autospawn implementation (fleet-side; consumed here).

## Related

- Epic v2 §2 (nodes + autospawn fallback), §8 Q4 (persona registry).
- `cloud/packages/web/lib/proactive-runtime/team-launch-n1.ts:245,803,829` (Daytona hardcodes), `dispatchTeamLaunchN1` (453–498).
- RFC §6 (placement + autospawn), §7 (invocation lifecycle).
- Phase 3 (RelayFleetClient pattern reused here).
