# verify-features

Use this skill to verify one Factory feature, a changed subsystem, or the whole
product from an agent/user perspective. A result is complete only when the
agent records the environment, commands, observable assertions, cleanup, and
any tier that could not be exercised.

## Authoritative files

```text
.agentworkforce/features/manifest.yaml
.agentworkforce/features/verify/procedures.md
.agentworkforce/features/critical-paths.md
workflows/verify-features.ts
```

The manifest is the feature inventory. `verification.categories` routes every
feature category to a named `##` procedure containing prerequisites, setup,
commands, assertions, cleanup, and automation limits. Never infer a procedure
from the tier alone and never treat a skipped live tier as a pass.

## 1. Find and validate the feature

```bash
npm run build
node bin/factory.mjs featuremap check

# Prefer structural lookup; category and feature lengths are not fixed.
if command -v yq >/dev/null 2>&1; then
  yq '.. | select(type == "!!map" and .id == "dispatch-dependency-admission")' \
    .agentworkforce/features/manifest.yaml
  yq '.verification.categories."dispatch-orchestration"' \
    .agentworkforce/features/manifest.yaml
else
  awk '
    $0 == "      - id: dispatch-dependency-admission" { found = 1 }
    found && $0 != "      - id: dispatch-dependency-admission" && /^      - id: / { exit }
    found && /^  [[:alnum:]][[:alnum:]-]*:$/ { exit }
    found { print }
  ' .agentworkforce/features/manifest.yaml
fi
```

Read the feature's `cli`/`api`, `description`, `location`, `verify_tier`, and
category criticality. Resolve the category through `verification.categories`
and read that entire procedure before running commands.

## 2. Satisfy the tier prerequisites

| Tier | Required environment | Safe interpretation |
| --- | --- | --- |
| 1 | Installed package or source checkout | Local build, exports, schemas, and deterministic tests only. |
| 2 | Valid `factory.config.json` | Prefer the checked-in fixture and a unique temporary directory. |
| 3 | Reachable Linear or GitHub ticket provider | Use a known opted-in test issue; canary must remain read-only. |
| 4 | Internal broker or hosted Relay fleet | Prove spawn, task receipt, exit, roster reconciliation, and release. |
| 5 | Cloud auth and writable Relayfile mount | Use disposable provider records and require provider acknowledgement/readback. |
| 6 | Live issue or pull request | Manual destructive-path check with exact identity and cleanup guards. |

Run lower-tier prerequisites first. Provider, fleet, cloud, and destructive
checks require explicit operator opt-in; report `SKIP` or `MANUAL` with the
missing prerequisite rather than substituting a mock.

## 3. Run end to end

1. Create the procedure's isolated fixture and install its cleanup trap before
   starting a process, agent, issue, PR, or provider write.
2. Run the exact commands in the named procedure. Capture stdout, stderr, exit
   status, and the run-specific identifiers.
3. Assert externally observable state: mounted provider receipt/readback,
   roster presence/absence, persisted lifecycle state, labels/status, branch
   and PR identity, or Slack timestamp as applicable.
4. Exercise the documented negative/fail-closed case. Safety checks are not
   proven by a happy path alone.
5. Run every applicable critical path below, then clean up and prove disposable
   resources and owned processes are gone.
6. Report `PASS`, `FAIL`, `SKIP`, or `MANUAL` for each tier. Include the first
   failing command and evidence; do not claim that an unrun tier works.

The deterministic whole-repository gate is:

```bash
npm run build
npm run featuremap:check
npm test
npm run verify:e2e
```

The scheduled/portable verification workflow is:

```bash
relay node workflow run workflows/verify-features.ts
```

Optional live inputs are documented at the top of that workflow.

## 4. Map changed code to procedures

| Changed area | Minimum procedures/tiers |
| --- | --- |
| `src/cli/`, `bin/` | `cli-and-package`, `public-api`, plus the command's category; tiers 1–2 |
| `src/config/`, `src/triage/` | `triage-and-configuration`, `provider-discovery`; tiers 1–3 |
| `src/orchestrator/`, `src/dispatch/`, `src/git/` | `issue-dispatch-lifecycle`, `fleet-execution`; tiers 1–4, then 5–6 when writeback/PR behavior changes |
| `src/hosted/` | `hosted-control-plane`, `pull-request-lifecycle`; tiers 1–2 and 5, then 6 for merge/writeback |
| `src/observability/`, reporting config | `cloud-observability`; tiers 1–2 and 5 |
| `src/mount/`, `src/writeback/` | `integrations-and-writeback`; tiers 1–2 and 5–6 |
| `src/subscriptions/` | `event-intake`; tiers 1–2 and 5 |
| `src/state/`, reaper/process identity | `loop-and-recovery`, `safety-boundaries`; tiers 1–2 and 4 |
| `.agentworkforce/features/`, guardian, workflow | `release-verification`, `proactive-health`; tiers 1–2 and 5 |
| Public exports | `public-api`; tier 1 plus packed E2E |

## 5. Critical paths

Before declaring the affected surface verified, run the relevant sequences in
`critical-paths.md`:

1. Config → discover → triage
2. Triage → dependency/batch admission → isolated team dispatch
3. Implement → publish PR → review
4. PR → babysitter → human review
5. Merge gate → merge → close
6. GitHub-native lifecycle
7. Human clarification round trip
8. Live daemon → heartbeat → recovery
9. Fleet node placement and release
10. Hosted run/reconcile/fencing
11. Durable cloud reporting without orchestration blockage
12. Manifest → procedure → workflow → proactive guardian

## Catalog maintenance rules

Update `manifest.yaml` whenever a public CLI leaf, package export, config field,
provider behavior, lifecycle transition, safety fence, hosted operation,
observability contract, or guardian behavior is added, renamed, or removed.
Update the named procedure whenever prerequisites, commands, assertions,
cleanup, or automation limits change. Update `critical-paths.md` when a
foundational sequence changes. Finally run all four deterministic gates above;
the manifest contract test must fail if a public surface is omitted.
