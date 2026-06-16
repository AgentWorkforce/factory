# Fix report — factory-p7-label-scope

Acted on the fresh-eyes review in `claude-review.md`. The review reported **no blocking defects**; the one item flagged as "worth acting on before merge" was Finding 1 (a test gap). That is fixed. Findings 2–4 were explicitly low-severity / observational with "Required action: none" or "optional" — addressed where it carried zero behavior risk (Finding 4), otherwise left intentionally as the reviewer recommended (Findings 2, 3) with rationale below.

## Finding 1 [Medium — test gap] — FIXED (test only, no product change)
**File:** `src/orchestrator/factory.ts:3545` guard `if (scope === 'team' && routesByLabel.routes.length > maxImplementers)`.

The behavioral core of "single/workflow win even with many repo labels" was unpinned: the existing `agent:single` test (issue 726) used only 2 repo labels with an effective cap of 2, so `2 > 2` is false and the guard would not fire even under the old `scope`-agnostic code. A revert of the `scope === 'team' &&` clause would have stayed green.

**Fix:** added two dispatch tests in `src/orchestrator/factory.test.ts`, each configuring **3** `byLabel` mappings with `triage.maxImplementers: 2` so the effective cap `min(2, MAX_LABEL_IMPLEMENTERS=4) = 2` is exceeded by 3 routes:
- `dispatches explicit single labels even when repo labels exceed the implementer cap` (issue 727, labels `pear,cloud,relayfile,agent:single`) — asserts exactly one implementer (`ar-727-impl-pear`) + reviewer spawn, a success summary comment, and **no** "Too many repo labels" skip.
- `dispatches explicit workflow labels even when repo labels exceed the implementer cap` (issue 728, labels `pear,cloud,relayfile,agent:workflow`) — asserts exactly one `workflow:run` spawn spanning all three routes (`repoLabels: [pear,cloud,relayfile]`), a success summary, and no skip.

**Guard-pinning verified:** temporarily reverting the clause to `if (routesByLabel.routes.length > maxImplementers)` makes **both** new tests fail (`2 failed`); restoring it makes them pass. The regression the reviewer described is now caught.

## Finding 4 [Nit] — FIXED (clarifying comment)
**File:** `src/triage/heuristic.ts:301` (`workflowSpec`).

`workflowSpec` derives `inputs.repoLabels` from raw label strings while the dispatch-path `routeWorkflowSpec` (factory.ts) rebuilds from route slugs. No observable bug today (the live spawn always uses the dispatch-path rebuild), but a latent trap for a future caller reading `decision.workflow.inputs` straight from triage. Added a comment marking the dispatch-path rebuild as authoritative, per the reviewer's "optional" suggestion. No behavior change.

## Finding 2 [Low] — NOT CHANGED (intentional, per reviewer)
Contradictory shape labels (`agent:single` + `agent:team`) resolve silently by precedence (`team > workflow > single`). Reviewer marked this "Not a blocker" and the fix as "optional/defensive". Surfacing a warning would require threading a logger into the pure `scopeFromLabels` helper or into the dispatch/clarification-comment path — a behavior change beyond the spec's scope (deterministic label→scope mapping). The resolution is deterministic and documented; left as-is deliberately.

## Finding 3 [Low — observation] — NOT CHANGED (reviewer: "none required")
Inferred `workflow` scope is overridden by `team` when ≥2 routes. Affects only `scopeSource: 'inference'`; explicit labels (this PR's feature) short-circuit and are unaffected. Reviewer's "Required action: none, unless inferred-workflow-with-multiple-routes is a real case." It is not a case this spec introduces, so the precedence is left intentional.

## Commands run
- `npx tsc -p tsconfig.json --noEmit` → **exit 0** (clean).
- `npx vitest run src/triage/triage.test.ts src/orchestrator/factory.test.ts src/fleet/internal-fleet-client.test.ts` → **253 passed (253)** (251 original + 2 new), env `AGENT_RELAY_STATE_DIR` unset to match a clean CI environment.
- Guard-pinning check: reverted `scope === 'team' &&` → 2 new tests fail; restored → all pass.

## Environment note (not a defect)
When the suite is run *inside this live agent-relay session*, the single fleet test `surfaces the broker pid as protected process state` fails because `AGENT_RELAY_STATE_DIR` points at a real `.agentworkforce/relay/connection.json` (pid 73795); `protectedPids()` then returns `[68009, 73795]` instead of `[68009]`. This is an environment artifact of running tests under a real broker — not a code defect, not caused by this change, and it passes with `AGENT_RELAY_STATE_DIR` unset (as the original review ran it). No fix applied; documented here for transparency.
