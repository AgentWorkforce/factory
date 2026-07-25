# Factory dispatch-mode benchmark

Machinery to answer, with real numbers instead of vibes: does Factory's
multi-agent dispatch (`team`) beat a single agent (`single`) on real coding
tasks, and does live collaboration (the new `swarm` scope — a lead + workers
sharing one checkout and a relay channel, see `../src/triage/heuristic.ts`)
beat parallel task-splitting, or is it just coordination overhead?

## How it works

```
benchmark/tasks/<id>/task.json + verify.sh   — the task corpus (ground truth: real tests, not a self-report)
        │
        ▼
benchmark/matrix.ts    — builds the (task x mode x repeat) matrix, skipping cells already recorded (resumable)
        │
        ▼
benchmark/run.ts       — for each cell: open a labeled GitHub issue, `factory dispatch` at the forced mode,
        │                 wait for a PR, check it out, run verify.sh — via benchmark/cli-dispatch-runner.ts
        ▼
benchmark/results.jsonl (append-only, one line per cell)
        │
        ▼
benchmark/report.ts    — aggregates into success rate / wall-clock / cost per task x mode
        │
        ▼
benchmark/report.md
```

`matrix.ts`, `report.ts`, `orchestrate.ts`, `results-store.ts`, and
`schema.ts` are pure logic with full unit coverage
(`npx vitest run benchmark/`). `cli-dispatch-runner.ts` is the real-IO
adapter (shells out to `gh` and `factory dispatch`) — same category as
`../scripts/verify-tailscale-preview-e2e.mjs`: proven by running it for
real against live infrastructure, not by mocking `child_process`.

## Before running this for real

1. **Point it at a disposable sandbox repo, never a product repo.** Every
   task's `targetRepo` must be a repo Factory can freely open/close PRs and
   issues against with no consequence — e.g. a dedicated
   `AgentWorkforce/factory-benchmark-fixtures` repo (not created by this
   change; create and seed it before authoring real tasks).
2. **Author real tasks.** `benchmark/tasks/` ships empty (only `.gitkeep`).
   `benchmark/templates/` has two fully-shaped examples — a `single-file`
   control-group task and a `multi-service` task where coordination should
   matter — to copy the shape from. Aim for ~12-15 tasks spanning
   `single-file` → `multi-file` → `multi-service`; single-file is the
   control (little coordination benefit expected), multi-service is where
   team/swarm should differentiate from single, if they differentiate at
   all.
3. **Optionally add a public-benchmark subset** with
   `node benchmark/swe-bench-adapter.mjs --count 20 --out benchmark/tasks`.
   **Read the script's header comment first** — every generated task points
   `targetRepo` at the real upstream OSS repo (e.g. `django/django`); you
   must repoint it to a fork you control before dispatching, and the
   generated `verify.sh` is a best-effort pytest reconstruction, not a
   guarantee of official-SWE-bench-harness parity. Say so plainly in any
   report built from these tasks — don't imply leaderboard-comparable
   numbers if any instance needed reshaping.
4. **`factory.config.json`** for the workspace must map every task's
   `targetRepo` to a real `clonePath`, same as any other Factory config.

## Running it

```bash
npx tsx benchmark/run.ts --config ./factory.config.json
# narrow while iterating:
npx tsx benchmark/run.ts --config ./factory.config.json --only-task rename-error-type --only-mode swarm --repeats 1
```

This dispatches real agents and spends real money/time — there is
deliberately no offline/fixture mode for the scored runs, because fixture
output can't be scored for code quality. Smoke-test one task across all
three modes with `--repeats 1` before committing to the full matrix.

It's safe to kill and re-run: `results.jsonl` is append-only, and `run.ts`
skips any `(task, mode, repeat)` cell that already has a recorded result.

## Reading the result

- A `single-file` task where `team`/`swarm` don't at least match `single`
  means dispatch overhead is hurting on tasks too small to benefit from it —
  expected and fine, it's the control group.
- A `multi-file`/`multi-service` task is where `team` (parallel fan-out) and
  `swarm` (live collaboration) get a real chance to beat `single`. If they
  don't, that's a legitimate, reportable finding — don't only report the
  runs that flatter multi-agent dispatch.
- Compare `team` vs `swarm` directly on the same tasks to answer the
  original question: does *live* collaboration beat *isolated parallel*
  collaboration, or is the extra coordination channel just overhead?
