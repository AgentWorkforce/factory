Title: [factory] AR-272 — labels select recipe and roster (dispatch by label, unified-node framing)

Team: AR
Suggested status: Ready for Agent
Repo label: factory (@agent-relay/factory triage)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Epic: factory-unified-node-architecture-linear-issue.md §3 / §3.5
Related: relay/specs/fleet-delivery.md §6 (placement); AR-272

Note: the prior AR-272 draft (`linear-issue-factory-dispatch-implementer-by-label.md`) was not present in the workspace at amendment time, so this is a fresh write of the amended design rather than an in-place edit. Reconcile with the filed AR-272 if it differs.

---

## Problem

The original AR-272 framing was "implementer NAMES come from repo labels." That's still right, but under the unified-node model the cleaner framing is **labels select recipe and roster** — a shape label picks the recipe (epic v2 §3.5), and repo labels shape the roster within it.

## Design

**Shape label → recipe** (mutually exclusive; exactly one expected):

- `agent:single` → single recipe (1 spawn).
- `agent:workflow` → workflow recipe (1 `workflow:run` spawn).
- `agent:team` → team recipe (N implementer spawns + 1 reviewer spawn).

**Repo labels (`cloud`, `relayfile`, `pear`, …) → roster / placement, per recipe:**

| Recipe | Repo labels determine | Notes |
|---|---|---|
| `agent:team` | **Roster:** one implementer per repo label, named after that label (e.g. labels `cloud`+`relayfile` → `cloud-team-impl-cloud`, `cloud-team-impl-relayfile`). Reviewer naming unchanged. | Cap at **4 implementers** (fail-loud past the cap). |
| `agent:single` | **Placement only:** roster is always 1 implementer regardless of repo-label count; the repo label informs which checkout/node the single spawn targets (RFC §6 placement input). | Multiple repo labels on a single-recipe issue → use the primary/first for placement; warn. |
| `agent:workflow` | **Workflow inputs:** the workflow YAML/TS defines its own roster; repo labels are passed as inputs the workflow can read. | Roster count is the workflow's concern, not the label's. |

**Invariants kept from AR-272:**
- **Fail-loud on unrouteable labels:** a repo label with no entry in `repos.byLabel` (WorkspaceConfig) aborts triage with a clear error — never silently drop or guess.
- **Cap at 4 implementers** for team recipes.
- Reviewer naming/role unchanged.

**Where this lives:** in `@agent-relay/factory` triage (the recipe-selection step the cloud brain runs, Phase 2). The factory emits the resulting spawn-set; placement is fleet-side (RFC §6) — the repo label is a placement *input*, not a factory-side placement decision.

## End-to-end verification (captured artifact required)

1. Team recipe: issue labeled `agent:team` + `cloud` + `relayfile` → capture the emitted spawn-set: 2 implementer spawns named per label + 1 reviewer spawn.
2. Single recipe: issue labeled `agent:single` + `cloud` → capture 1 implementer spawn, placement targeting the `cloud` checkout.
3. Workflow recipe: issue labeled `agent:workflow` → capture 1 `workflow:run` spawn; repo labels present as inputs.
4. Unrouteable label: issue labeled with an unknown repo label → capture the fail-loud triage abort (no spawn emitted).
5. Cap: `agent:team` with 5 repo labels → capture the cap-at-4 fail-loud behavior.

## Acceptance criteria

1. Shape label selects recipe (`single | workflow | team`); exactly-one enforced.
2. `agent:team` rosters one implementer per repo label (named after the label), reviewer unchanged, capped at 4.
3. `agent:single` always 1 implementer; repo label used only for placement.
4. `agent:workflow` ignores repo labels for roster; passes them as workflow inputs.
5. Unrouteable repo label fails loud (no silent drop); captured.
6. Logic lives in `@agent-relay/factory` triage; placement remains fleet-side.

## Out of scope

- Placement algorithm itself (RFC §6 least-loaded, fleet-side).
- Recipe execution (Phases 3/4).

## Related

- Epic v2 §3 (label → recipe), §3.5 (recipe → spawn-set), §8 Q2 (single-recipe = team at N=1).
- RFC §6 (placement input).
- WorkspaceConfig `repos.byLabel` (p2 config split).
