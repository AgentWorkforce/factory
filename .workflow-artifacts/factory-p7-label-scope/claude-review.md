# Fresh-eyes review — factory-p7-label-scope

**Spec:** `planning/linear-issue-factory-cloud-p7-label-scope.md` — widen `TriageDecision.scope` to `single | workflow | team`, map issue labels → scope deterministically, keep repo-routing/safety gates orthogonal, define the `workflow` spec shape.

## Verification performed
- `npx tsc -p tsconfig.json --noEmit` → clean (exit 0).
- `npx vitest run src/triage/triage.test.ts src/orchestrator/factory.test.ts src/fleet/internal-fleet-client.test.ts` → 3 files / **251 tests passed**.
- Confirmed `dispatch()` always routes through `labelDerivedDispatchDecision` (factory.ts:1078), which guarantees ≥1 repo route before building a workflow spec, so the defensive empty-array branch in `dispatchSpecs` is unreachable in the real path.
- No repo-local `AGENTS.md`/`CLAUDE.md` present in this worktree (matches self-reflection).

## Spec acceptance check
- AC1 (explicit `agent:single|workflow|team` → matching scope): ✅ covered in `triage.test.ts` (heuristic each-case) and `factory.test.ts` (dispatch for workflow + single).
- AC2 (unlabeled → inferred; thin/low-confidence → clarification, not spawn): ✅ pre-existing inference/clarification paths untouched; explicit labels short-circuit cleanly.
- AC3 (`workflow` scope emits a spec the cloud path understands): ✅ adds `capability: 'workflow:run'`, `workflow`, `inputs` to `AgentSpec`/`SpawnInput`/schema and threads them through `fleet.spawn`. Downstream `relayflows` execution is out of scope per spec (noted as a risk).
- AC4 (tests cover three labels + inference + clarification): ✅ three explicit labels + tiered preservation added; inference/clarification covered by existing suite.

Repo-routing labels and safety gates (`requireTitlePrefix`, `requireTeamKey`) are left orthogonal — shape labels are filtered out of `labelRoutesForIssue` and `routeByLabels`. Good.

---

## Findings (actionable)

### 1. [Medium — test gap] The `too-many-labels` relaxation for single/workflow scope is untested
**File:** `src/orchestrator/factory.ts:3545`

The guard changed from `if (routesByLabel.routes.length > maxImplementers)` to `if (scope === 'team' && routesByLabel.routes.length > maxImplementers)`. This is the behavioral core of "single/workflow win even with many repo labels" — but no test exercises it. The new `agent:single` test (`factory.test.ts`, issue 726) uses only **2** repo labels while the effective cap is `min(config.triage.maxImplementers=2, MAX_LABEL_IMPLEMENTERS=4) = 2`, so `2 > 2` is false and the guard would not fire even under the old code. If someone reverted the `scope === 'team' &&` clause, the suite would still pass green.

**Required fix:** none to product code (behavior is correct).
**Required test:** add a dispatch case with an `agent:single` (and separately `agent:workflow`) issue carrying **3+ mapped repo labels** that exceed the effective cap, asserting it succeeds with exactly one implementer / one workflow spawn rather than returning `too-many-labels`. To make the cap bite, configure 3 `byLabel` mappings (cap stays 2). This is the only assertion that actually pins the guard relaxation.

### 2. [Low] Contradictory shape labels resolve silently by precedence
**File:** `src/triage/heuristic.ts:386` (`scopeFromLabels`)

When an issue carries conflicting shape labels (e.g. both `agent:single` and `agent:team`), resolution is silent: `team` > `workflow` > `single`. Deterministic, but a misconfigured issue gets a surprising scope with no signal.

**Required fix (optional/defensive):** when more than one distinct `agent:*` shape label is present, either log a warning or surface it in the dispatch/clarification comment so the human notices the conflict. Not a blocker.
**Required test:** if implemented, a `scopeFromLabels(['agent:single','agent:team'])` unit case asserting the documented winner + that a warning is emitted.

### 3. [Low — observation, no change required] Inferred `workflow` scope is overridden by `team` when ≥2 routes
**File:** `src/triage/tiered.ts:39-45` and `src/triage/heuristic.ts:86-92`

In `mergeDecisions`/`buildDecision`, for `scopeSource: 'inference'`, `routes.length >= 2` forces `team` before the `workflow` branch is considered. So an LLM that infers `workflow` on a 2+ route issue is silently downgraded to `team`. Explicit labels are unaffected (they short-circuit via `scopeSource === 'label'`), so this does not affect the PR's primary feature. Flagging only so the precedence is an intentional choice, not an accident.
**Required action:** none, unless inferred-workflow-with-multiple-routes is a real case — then reorder so `workflow` is checked before the `>=2 → team` rule for inference.

### 4. [Nit] `repoLabels` derivation differs between the two workflow-spec builders
**Files:** `src/triage/heuristic.ts:316` (`workflowSpec`, raw label strings) vs `src/orchestrator/factory.ts:3672` (`routeWorkflowSpec`, route slugs)

The two builders populate `inputs.repoLabels` from different sources (raw labels vs slugs). In the live dispatch path `routeWorkflowSpec` fully rebuilds `inputs`, discarding the heuristic version, so there's no observable bug today. But the divergence is a latent trap if a future caller consumes `decision.workflow.inputs` straight from triage.
**Required fix (optional):** make both derive `repoLabels` the same way (slugs), or add a comment noting the dispatch-path rebuild is authoritative.
**Required test:** none.

---

## Verdict
No blocking defects. Type-checks clean and all 251 affected tests pass; the spec's four acceptance criteria are met and safety gates / repo routing remain orthogonal. The one item worth acting on before merge is **Finding 1** — add a test that actually pins the `scope === 'team' &&` guard relaxation (single/workflow with repo-labels exceeding the cap), since the current suite would not catch its regression. Findings 2–4 are low-severity / observational.
