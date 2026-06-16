# Factory build-out — sequenced relayflows (p1–p13)

Autonomous, sequenced relayflows that build the **cloud-watches → local-node factory**
end to end (epic: `../planning/factory-cloud-watches-local-node-linear-issue.md`).
Each workflow implements one planning issue with a full squad + review loop and ships a
draft PR. Run via `./run-factory-build.sh` (or `relayflows run <file>`).

## Why these live here
These live **in the factory repo** (`@agent-relay/factory`) — the build-out lives with the
thing it builds. The repo consumes the relayflows SDK directly: `@relayflows/core` (the
`workflow` builder + `createGitHubStep`) and `@relayflows/cli` (the `relayflows run` runner)
are **devDependencies** (both published on npm). They are dev-only tooling — the `files`
allowlist in `package.json` excludes `factory-build/` from the published npm tarball.
The runner uses the locally-installed CLI (`node_modules/@relayflows/cli`); ricky can drive
`./run-factory-build.sh` or set `FACTORY_BUILD_RUNNER` to its own runner.

## The squad (per the user's spec)
- **lead-claude** — lead + QA (plans, assigns, repairs red gates)
- **impl-codex** — primary implementer
- **shadow-claude** — live shadow reviewer (flags spec drift while work happens)
- **reviewer-claude / fixer-claude** — first fresh-eyes review/fix loop
- **reviewer-codex / fixer-codex** — second loop (deep tier only)

The squad + the 80-to-100 review ladder (self-reflection → scoped change-detection →
soft/hard validate → Claude review/fix/final → [deep] Codex review/fix/final →
green-or-blocked acceptance → signoff → scoped commit → push → draft PR) live once in
`lib/factory-build-lib.ts`. Each wave file is thin: it supplies repo, branch, spec, file
targets, acceptance command, tier, and the implementation goal.

## Review tiers
- **standard** (Claude loop): p1, p2, p3, p5 — low-risk, focused refactors.
- **deep** (Claude + Codex loops): p6–p13, p11 — cloud/relay integration and the crux.

## ⚠️ Prerequisites before wave1 (factory-sdk config PRs)
Two in-flight pear PRs rewrite `packages/factory-sdk/config/schema.ts` — the file **p2** edits — so land them before wave1 or the workflow PRs conflict:
- **[pear#368](https://github.com/AgentWorkforce/pear/pull/368) — LANDED.** Dynamic per-team Linear states (`linear.states`, `linear.statesByTeam`, `stateIds` fallback; `resolveFactoryStates` reads `/linear/states`). Also touched `orchestrator/factory.ts` + `types.ts` (p1) + `cli/fleet.ts` (p3). Provider dep: the `/linear/states` resource (relayfile-adapters `feat/linear-states-adapter`) must be materialized — flows into the cloud lift (**p6/p8**).
- **[pear#369](https://github.com/AgentWorkforce/pear/pull/369) — LANDED.** Compact `repos` form (`org`/`cloneRoot`/`names`/`overrides`) with a Zod `.transform()` deriving `byLabel`/`clonePaths`/`labels`. Only touches `config/schema.ts` — overlaps **p2**.

**p2 splits #369's compact form along the workspace/node seam:** `org`/`names`/`overrides`/`byLabel`/`default` → **WorkspaceConfig**; `cloneRoot`/`clonePaths` (the per-machine checkout paths) → **NodeConfig** (this *is* `repoPaths`). It also folds #368's `linear.states`/`statesByTeam`/`stateIds` into WorkspaceConfig. Preserve both #369's `.transform` derivation and #368's dynamic state resolution.

## Waves & dependency order
```
wave0 (prereq)    pear#368 + pear#369 — config/schema.ts — ✅ BOTH LANDED
wave1 (parallel)  p1 p2 p3 (pear prep)   p11 (relay broker — independent)
wave2             p4  extraction  ──►  ⛔ PUBLISH GATE (human: npm publish + pear swap)
wave3 (parallel)  p5 (pear teardown)     p6 (cloud host orchestrator)
wave4 (parallel)  p7 (label→recipe)  p8 (linear webhook)        [p9 deleted — no daytona|fleet-node branch]
wave5             p10 (RelayFleetClient — thin fleet-protocol client, Phase 3)
wave6             p12 (node-targeted placement — relay-side, RFC §6/§7)
wave7             p13 (node registration — Phase 4)
wave8 (sibling)   proactive-runtime fleet unification (independent; run anytime)
```

Run:
```bash
./run-factory-build.sh prep --dry-run     # validate wave1
./run-factory-build.sh wave1              # extraction prep + broker heartbeat
./run-factory-build.sh wave2              # p4 → stops at the publish gate
#   operator: publish @agent-relay/factory + swap pear (see PUBLISH_READY.md)
./run-factory-build.sh post-publish       # wave3..wave7
```

## ⛔ The publish gate (between wave2 and wave3)
p4 seeds + pushes `AgentWorkforce/factory` and **stops before `npm publish`** (irreversible).
A human publishes `@agent-relay/factory@0.1.0` and runs the pear dep-swap (see the generated
`PUBLISH_READY.md`). Only then can wave3+ run: **p6** imports the published package into cloud;
**p5** assumes pear consumes it. p7/p10/p13 edit the factory repo source (which exists after
the p4 seed) and don't strictly need the publish, but the gate keeps the sequence simple.

## Net result
After the full sequence: **no factory logic lives in pear.** Pear imports
`@agent-relay/factory` only for types and renders a read-only view of cloud state (p5 deletes
the Electron daemon entirely). The brain runs in cloud; the user runs one command —
`agent-relay fleet serve <factory-node-def>` — which **auto-starts the broker**
(`startBrokerWithPortFallback`, no separate `agent-relay up` needed) and executes
cloud-placed spawns on their own machine.

## Safety notes
- PRs are opened **draft** with a `[factory]` title and the **`no-agent-relay-review`** label,
  which disables the autonomous pr-reviewer bot that otherwise pushes unreviewed commits to
  held draft PRs.
- PR creation uses local `gh` (the broker runs on the user's machine where `gh` is authed).
  For cloud execution, swap the `open-pr` step in the lib for `createGitHubStep({action:'createPR'})`
  (imported from `@relayflows/core/integrations/github`).
- **Repair-not-skip:** the workflow is `.repairable({ repairRetries: 12 })`. Any failing
  gate **auto-invokes the repair (fixer) agent to fix it and reruns the gate** — it never
  skips, never writes a "blocked" artifact, never signs off red work. Only an exhausted
  repair budget (12 fix→rerun cycles per gate) can end a run unfixed, which in practice
  means a human is genuinely needed. To make failure literally unreachable, raise
  `repairRetries` in `lib/factory-build-lib.ts` (the engine bounds repair; there is no
  unbounded mode without a relayflows change).

## Cross-repo workflows (residual risk)
p4, p10, p11 touch two repos. They set `cwd` to the primary repo and reference the secondary
by absolute path via `crossRepoNote`; they open the secondary repo's PR separately and note it
in the signoff. Review these closely.

## Per-issue traceability
Each file maps 1:1 to a planning doc in `factory/planning/`. The workflow name is
`factory-<id>-<slug>`; artifacts land in `<target repo>/.workflow-artifacts/factory-<id>-<slug>/`.
