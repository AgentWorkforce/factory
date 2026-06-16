Title: [factory] Phase 2 — widen TriageDecision.scope to single|workflow|team; map issue labels → scope

Team: AR
Suggested status: Ready for Agent
Repo label: factory (logic in `@agent-relay/factory`; consumed by cloud)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p4 (published package)
Epic: factory-cloud-watches-local-node-linear-issue.md §3 (Phase 2)

---

## Context

The product surface is: a user labels a Linear issue to choose the execution shape — a **single agent**, a **workflow**, or a **team**. Today `TriageDecision.scope` only supports `single | team` and is computed implicitly. This PR makes scope a three-value knob driven by explicit labels, with tiered inference as the fallback.

## Goal

`scope: 'single' | 'workflow' | 'team'`, selected deterministically by label and inferred otherwise. One knob, not three code paths — placement (p10) translates scope into spawn count / roster / workflow ref.

## Scope

- Widen the `TriageDecision.scope` type + all switches in `@agent-relay/factory` (`src/triage/`, `src/types.ts`, orchestrator dispatch).
- Label mapping (explicit, wins over inference):
  - `agent:single` → single
  - `agent:workflow` → workflow
  - `agent:team` → team
- Inference fallback (no shape label): `TieredTriage` (heuristic → LLM) emits scope + confidence; low confidence routes to the existing Slack clarification state machine before spawning.
- Keep routing labels (repo-name → `repos.byLabel`) and safety gates (`requireTitlePrefix`, `requireTeamKey`) orthogonal and unchanged.
- Define the `workflow` scope's spec shape so it maps onto cloud's existing workflow execution path (see open question in epic §7.4 — resolve here).

## Acceptance criteria

1. An issue labeled `agent:single` / `agent:workflow` / `agent:team` produces the matching `scope` deterministically.
2. An unlabeled issue gets an inferred scope; a thin/low-confidence one routes to Slack clarification, not a spawn.
3. `workflow` scope emits a spec cloud's workflow path understands (no new dialect).
4. Unit tests cover all three explicit labels + the inference + clarification branches.

## Out of scope

- Actually spawning the three shapes on a node (p10).
- Cloud hosting (p6).
