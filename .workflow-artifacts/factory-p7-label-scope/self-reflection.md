# factory-p7-label-scope self-reflection

## Changed files

- `src/types.ts`: widened `TriageDecision.scope` to `single | workflow | team` and added optional `TriageDecision.workflow`.
- `src/triage/heuristic.ts`: added explicit `agent:single`, `agent:workflow`, and `agent:team` label parsing; kept shape labels separate from repo routing labels; added workflow spec construction using `capability: "workflow:run"`, `workflow: "workflows/factory/linear-issue.ts"`, and issue/route inputs.
- `src/triage/schema.ts`: widened runtime validation for workflow scope, workflow role, workflow capability, and workflow inputs.
- `src/triage/tiered.ts`: preserved explicit label-selected scope across heuristic to LLM merges; low-confidence fallback behavior remains unchanged.
- `src/triage/llm.ts`: updated prompt contract for explicit shape labels and workflow spec shape.
- `src/triage/index.ts`: exported shape-label helpers for dispatch.
- `src/orchestrator/factory.ts`: filtered shape labels out of repo-label validation; preserved explicit single/team/workflow scope during label-derived dispatch; emitted one workflow spawn for workflow scope; passed `workflow` and `inputs` through `fleet.spawn`.
- `src/ports/fleet.ts`: widened capability/role types and spawn input fields for `workflow:run`.
- `src/fleet/internal-fleet-client.ts`, `src/testing/fakes.ts`, `src/cli/fleet.ts`: widened local/fake/CLI capability handling to include `workflow:run`.
- `src/triage/triage.test.ts`, `src/orchestrator/factory.test.ts`, `src/fleet/internal-fleet-client.test.ts`: added and updated coverage for explicit labels, workflow spec validation, tiered preservation, dispatch behavior, and roster capability expectations.

## Spec coverage

- Explicit labels are deterministic:
  - `agent:single` maps to `scope: "single"` and keeps one implementer even with multiple repo labels.
  - `agent:workflow` maps to `scope: "workflow"` and emits one `workflow:run` spec.
  - `agent:team` maps to `scope: "team"`.
- Shape labels are orthogonal to repo routing labels:
  - `agent:*` labels are ignored by repo-label route matching and dispatch validation.
  - Repo labels still select routes/placement inputs.
- Tiered fallback remains heuristic to LLM:
  - Explicit shape labels survive LLM merge.
  - Low-confidence or thin decisions still flow into the existing Slack clarification state machine before spawning.
- Workflow scope uses the existing spawn-style cloud path:
  - `AgentSpec`/`SpawnInput` now support `capability: "workflow:run"`, `workflow`, and `inputs`.
  - Workflow inputs include issue metadata, labels, repo labels, and resolved routes.

## Tests/proofs run

- `npm test -- src/triage/triage.test.ts` - passed, 29 tests.
- `npm run build` - passed.
- `npm test -- src/orchestrator/factory.test.ts` - passed, 193 tests.
- `npm test` - passed, 28 files / 462 tests.
- `npm run build` - passed after final test adjustment.

## Repo-rule alignment

- Read for repo-local `AGENTS.md` / `CLAUDE.md`; none were present in this worktree.
- Kept implementation close to existing triage/dispatch helpers and validation patterns.
- Did not change safety gates (`requireTitlePrefix`, `requireTeamKey`) or repo route precedence.
- Used existing Slack escalation behavior rather than adding a new clarification mechanism.
- Avoided reverting or deleting unrelated untracked workspace files.

## Remaining risks

- The concrete default workflow path is `workflows/factory/linear-issue.ts`; this defines the package-side spec shape but assumes downstream workflow registries provide that file or remap the workflow reference.
- `InternalFleetClient` advertises and accepts `workflow:run`; actual local execution depends on the node/fleet implementation supporting the `relayflows` CLI path.
