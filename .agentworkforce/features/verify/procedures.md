# Feature Verification Procedures

Verify Factory from the user's point of view, lowest prerequisite tier first. A higher tier does not replace the lower tiers: it adds provider, fleet, cloud, or live-work prerequisites.

Use a disposable workspace, issue, repository branch, and Slack channel for tiers 3–6. Never point mutation checks at production work unless the operator explicitly selected those records.

---

## Tier 1 — Package Only

Requires only a source checkout with dependencies installed. These checks are deterministic and must run on every change.

### Build, test, entrypoints, and manifest

```bash
npm run build
npm test

node bin/factory.mjs --help
# → exits 0 and lists every top-level command and global option

node --input-type=module <<'NODE'
import { existsSync, readFileSync } from 'node:fs'
import { parse } from 'yaml'

const manifest = parse(readFileSync('.agentworkforce/features/manifest.yaml', 'utf8'))
const categories = Object.values(manifest.categories)
const features = categories.flatMap((category) => category.features)
const ids = features.map((feature) => feature.id)
if (new Set(ids).size !== ids.length) throw new Error('duplicate feature IDs')
const tierCounts = Object.fromEntries([1, 2, 3, 4, 5, 6].map((tier) => [
  tier,
  features.filter((feature) => feature.verify_tier === tier).length,
]))
if (manifest.catalog.category_count !== categories.length ||
    manifest.catalog.feature_count !== features.length ||
    JSON.stringify(manifest.catalog.tier_counts) !== JSON.stringify(tierCounts)) {
  throw new Error('catalog summary does not match manifest contents')
}
for (const feature of features) {
  if (!feature.id || !feature.name || (!feature.cli && !feature.api) ||
      !feature.description || !feature.location ||
      !Number.isInteger(feature.verify_tier) || feature.verify_tier < 1 || feature.verify_tier > 6) {
    throw new Error(`invalid feature: ${JSON.stringify(feature)}`)
  }
  for (const location of feature.location.split(',').map((value) => value.trim())) {
    if (!existsSync(location)) throw new Error(`missing location for ${feature.id}: ${location}`)
  }
}
console.log(`${features.length} features in ${categories.length} categories`)
NODE
```

Pass criteria: build and all tests exit 0; both published entrypoints remain importable; the manifest parses with unique, complete entries.

### Focused Tier 1 checks

```bash
npx vitest run \
  src/__tests__/dist-entrypoints.test.ts \
  src/config/schema.test.ts \
  src/config/local-clone-paths.test.ts \
  src/github/merge-gate.test.ts \
  src/logging.test.ts \
  src/webhook/handler.test.ts \
  src/webhook/registrar.test.ts \
  src/subscriptions/__tests__/globs.test.ts \
  src/subscriptions/__tests__/linear-filter.test.ts \
  src/subscriptions/__tests__/specs.test.ts \
  src/subscriptions/__tests__/event-client.test.ts
```

Confirm specifically:

- invalid webhook signatures return 403, malformed JSON returns 400, duplicate event IDs return 200 without a second callback, and Linear/Slack/GitHub paths reach the matching handler;
- the merge gate refuses missing fields, unknown/dirty/unapproved state, no checks, blocking checks, and moved heads;
- `Error` diagnostics survive as bounded, redacted JSON-safe data while cycles, BigInt, getters, `toJSON`, and custom stack hooks cannot crash or execute through the logger;
- subscription path aliases, Slack DM globs, targets, predicates, event conversion, coalescing, token workspace checks, polling fallback, and stream self-recovery remain deterministic;
- public ESM exports for the root, `/node`, `/writeback`, and `/testing` load from `dist`.

---

## Tier 2 — Valid Config or Fixture Config

Use the checked-in fixture for hermetic orchestration. Add the canonical Linear URL to a temporary copy—the source requires it as part of a real provider identity, while the current fixture omits it. This uses fake mount and fleet records, so no provider credentials or broker are used.

### Fixture-backed CLI cycle

```bash
npm run build

node --input-type=module <<'NODE'
import { readFileSync, writeFileSync } from 'node:fs'
const config = JSON.parse(readFileSync('test/fixtures/factory.config.json', 'utf8'))
config.fixtureFiles['/linear/issues/AR-77__uuid-77.json'].payload.url =
  'https://linear.app/agent-relay/issue/AR-77/cli-dry-run'
writeFileSync('/tmp/factory-tier2-config.json', JSON.stringify(config, null, 2))
NODE

node bin/factory.mjs run-once \
  --config /tmp/factory-tier2-config.json \
  --dry-run > /tmp/factory-tier2-run-once.json

node --input-type=module <<'NODE'
import { readFileSync } from 'node:fs'
const report = JSON.parse(readFileSync('/tmp/factory-tier2-run-once.json', 'utf8'))
if (!report.dryRun) throw new Error('expected dryRun=true')
if (!report.pulled.some((issue) => issue.key === 'AR-77')) throw new Error('AR-77 not discovered')
const decision = report.triaged.find((entry) => entry.issue.key === 'AR-77')
if (!decision) throw new Error('AR-77 not triaged')
if (decision.routes[0]?.repo !== 'AgentWorkforce/pear') throw new Error('wrong route')
if (!report.dispatched.some((entry) => entry.issue.key === 'AR-77')) throw new Error('AR-77 not dry-run dispatched')
console.log('fixture cycle passed')
NODE

node bin/factory.mjs status --config /tmp/factory-tier2-config.json
# → JSON with inFlight, queued, counters, and slackDegraded
```

Pass criteria: the fixture issue is discovered, scoped, routed, and represented as a dry-run dispatch with no real writes or spawns.

### Config matrix

Run the schema and domain suites:

```bash
npx vitest run \
  src/config/schema.test.ts \
  src/config/local-clone-paths.test.ts \
  src/cli/fleet.test.ts \
  src/safety/factory-scope.test.ts \
  src/triage/triage.test.ts \
  src/dispatch/templates.test.ts \
  src/dispatch/relayflow-registry.test.ts \
  src/linear/state-resolver.test.ts \
  src/orchestrator/factory.test.ts \
  src/orchestrator/reaper.test.ts \
  src/state/file-state-store.test.ts \
  src/node/factory-node.test.ts
```

The config suite must prove every field listed in the manifest, including defaults, min/max validation, derived repo maps and paths, split/combined envelopes, state mapping precedence, and workspace mismatch rejection. It must also prove exact `~`/leading `~/` expansion, unsupported `~user` rejection, one-repo cwd inference only after a matching GitHub remote, linked-worktree acceptance, invalid checkout preflight failure, and no local environment inspection for relay or maintenance commands. The safety suite must prove boundary-aware titles, label forms, team gating, GitHub mirror markers, and fail-closed writeback.

The triage/orchestrator suite must preserve byte-compatible Linear names while repo-qualifying every GitHub role and isolating equal-number GitHub issues by composite repo/number identity across dispatch, registry, resume, PR, publication, retry, and babysitter state.

### Loop and recovery files

Use a temporary config that overrides `loop.heartbeatPath` and `loop.registryPath` under `/tmp/factory-feature-verify/`. Run a one-iteration fixture loop and then:

```bash
node bin/factory.mjs loop --config /tmp/factory-feature-verify/config.json --dry-run
node bin/factory.mjs loop-status --config /tmp/factory-feature-verify/config.json
# → heartbeat exists, reports idle after the bounded loop, and is not malformed
```

Do not run `kill-loop` at this tier; process signaling is tier 6.

---

## Tier 3 — Reachable Ticket Provider

Requires read credentials for the selected issue source and a known disposable issue. Use `--dry-run` throughout this tier.

### Linear source

Prepare one real issue that:

- is in the configured Ready for Agent state;
- has the configured team and title prefix or label;
- has one valid repo route label;
- contains a substantial description with acceptance signals.

```bash
factory triage AR-VERIFY --config ./factory.config.json
# → route source is label, confidence is high, expected shape and agents are present

factory canary AR-VERIFY --config ./factory.config.json
# → { "ok": true, "issue": "AR-VERIFY", "status": "triaged"|"dispatched" }
```

Repeat with a sparse synced record if the provider exposes one. Pass only if canonical fallback or state-name backfill recovers the real state.

### GitHub-native source

Set `issueSource: "github"`, create an open disposable issue with `safety.requireLabel` plus a configured repo route label, and run:

```bash
factory triage <issue-number> --config ./factory-github.config.json
factory canary <issue-number> --config ./factory-github.config.json
factory run-once --config ./factory-github.config.json --dry-run
```

Repeat against compact `owner__repo` and nested `owner/repo` mounts, covering `meta.json`, `by-id`, flat issue records, shallow listings, and paginated listings. Prove `repos.default` disambiguates a bare positive number, multiple configured matches reject as ambiguous, non-positive numbers reject with source context, provider read failures are not hidden by fallback scans, and repository-only config selects GitHub while explicit/legacy Linear config remains Linear.

Pass criteria: the labeled open issue appears once and routes correctly; an unlabeled or closed neighbor does not dispatch; alternate paths for the same repository deduplicate without merging different repositories. No provider mutation occurs.

---

## Tier 4 — Fleet Backend Available

Requires either a reachable internal relay broker or hosted Relay fleet credentials and at least one capable node.

### Internal broker lifecycle

```bash
factory fleet roster --backend internal
# → reuses the running broker, or starts one if none is reachable

NAME="factory-vf-codex-$(date +%s)"
factory fleet spawn spawn:codex --backend internal \
  --name "$NAME" --task "Reply with exactly FACTORY_VERIFY_OK"
factory fleet roster --backend internal
# → roster contains $NAME
factory fleet release "$NAME" --reason feature-verification --backend internal
# → { "released": "$NAME" }
```

Repeat with `spawn:claude` when that harness is installed. Verify a resumed session with `--resume <session-ref>` when the spawn result supplies one.

### Hosted Relay fleet

With `RELAY_WORKSPACE_KEY` set and an enrolled node online:

```bash
factory fleet roster --backend relay
factory fleet spawn spawn:codex --backend relay \
  --name "factory-vf-remote-$(date +%s)" \
  --node <node-name> \
  --cwd <advertised-checkout> \
  --task "Reply with exactly FACTORY_VERIFY_REMOTE_OK"
```

Pass criteria: capability placement selects the requested live node; an unadvertised checkout is refused; the invocation reaches a terminal result; roster reconciliation emits one exit; rehydrating the saved invocation after a client restart does not duplicate it.

For a normal multi-repository dispatch and a no-session restart, inspect the placement request and prove `AgentSpec.repo` is present both times. Resume must retain the placed node, repo, and clone path; a remote node PID must never be signaled as a local process.

### Dispatch messaging and recovery

Against a disposable fixture/provider issue, verify:

1. implementer(s) and reviewer/workflow spawn once;
2. task delivery receives `delivery_injected` acknowledgement before input submission;
3. a simulated delivery failure retries the persisted critical message;
4. an interrupted session resumes once;
5. batch overflow queues and completion promotes the next issue;
6. registry data contains agent/session/PID or remote invocation/node identity;
7. `factory reap-orphans` never terminates broker/node/protected PIDs.

For the durable relay lifecycle, additionally prove:

1. lease epochs fence a second same-host owner before duplicate spawn or publication;
2. separate `FileStateStore` instances enforce one global batch slot, including clarification parking;
3. restart adopts a roster-visible spawn across the spawn-ack persistence gap;
4. running, queued, publishing, parking, releasing, and waiting-for-human phases resume autonomously;
5. transient publication/release failures retry, a provider receipt is reused after lease loss, and capacity is released at the documented checkpoint;
6. cross-host active/active control-plane ownership is not claimed as supported—this check requires one shared same-host file store.

For clarification, reserve the first concurrent question, release the complete team, confirm fleet absence before committing the parked state, and wake once from the first authorized reply. Exercise saved-session resume, cold-start context, wake-lease renewal, reply-before-restart recovery, scope-loss cancellation, and retry of a failed seven-day escalation.

For babysitters, persist and restore exact repo/PR/session ownership, critical state, and pending wake categories. Coalesce repeated wake kinds, retain an event arriving during confirmed delivery, retry failure without duplication, persist the critical fence before ACK, defer PTY submit through the critical section, and cancel pending work after a terminal PR readback.

Use the focused suites when live failure injection is impractical:

```bash
npx vitest run \
  src/fleet/ensure-relay-broker.test.ts \
  src/fleet/internal-fleet-client.test.ts \
  src/fleet/relay-fleet-client.test.ts \
  src/fleet/create-fleet.test.ts \
  src/node/factory-node.test.ts \
  src/orchestrator/factory.test.ts \
  src/orchestrator/reaper.test.ts
```

---

## Tier 5 — Cloud Auth and Writable Relayfile Mount

Requires a non-interactive cloud session, readable Linear/GitHub/Slack integrations as applicable, and write permission for a disposable scope.

### Mount preflight and event intake

```bash
factory run-once --config ./factory.config.json --dry-run
# → resolves active workspace when workspaceId is omitted
# → starts or refreshes .integrations mount
# → reads provider roots and current high-water mark
```

Inspect `.integrations/.relay/state.json`: workspace ID must match the resolved handle or UUID alias, `lastReconcileAt` must be fresh, and a recorded PID must be live when present. Simulate a stale timestamp in a disposable mount and confirm `ensureLocalMount()` refreshes it or prints an actionable recovery warning.

### Live subscription modes

Exercise `subscribe`, `poll`, and `subscribe-and-poll` with a disposable issue update:

1. start the daemon before changing the issue;
2. update the issue once;
3. observe exactly one intake/dispatch attempt;
4. replay the same event ID and confirm suppression;
5. restart from a high-water fallback and confirm buffered events drain after the full pull;
6. confirm the heartbeat stays fresh during slow remote reads.

Canonical PR routing must accept only consistent normalized repo/PR identity for review, review-comment, issue-comment, failed/cancelled/timed-out check, conflict, and base-divergence records. Pending/green checks and inconsistent or body-only identity must not wake. Route paginated poll events through the same coalescer and re-read mounted PR metadata before delivery/cancellation.

### Provider writebacks

On a scoped disposable issue, verify each independently:

- Linear comment, state update, create-mirror, and verification reads return provider acknowledgement;
- GitHub-native comment and lifecycle labels reach the issue;
- Slack root and reply drafts reach the configured channel and bot replies do not feed back into Factory;
- an out-of-scope Linear draft, unsupported GitHub path, or unapproved delete is rejected;
- webhook register is idempotent and unregister removes the selected subscription.

For a relay implementer that exits after pushing, publish from its remote head without consulting an orchestrator-local clone. Confirm mounted metadata matches the exact repo, PR, expected head, open state, and non-draft state; then simulate owner loss after provider acknowledgement and prove the receipt is reused rather than creating a second PR. Restart must finish writeback and release cleanup.

### Human clarification

Trigger one thin issue and one running-agent question. Confirm the Slack thread persists, a non-bot reply is delivered once, the clarified issue re-triages or the answer reaches the right worker, and restart re-arms the watcher. Without Slack, confirm a correlated GitHub question is posted and unauthorized/bot replies are ignored.

Pass criteria: provider-visible state matches the write, every mutation is scoped and acknowledged, event replay is idempotent, and core execution continues when Slack becomes degraded.

---

## Tier 6 — Manual Live Issue or Pull Request

These checks mutate a real disposable issue/PR or signal a process and require an operator to inspect the external systems.

### Full issue-to-PR path

Run the first five critical paths from `../critical-paths.md` with a disposable issue and repository:

```text
discover → triage → dispatch → implement → PR → review/babysit
  → Human Review → merge → Done/closed
```

Verify the PR contains the issue key, commits, tests, and review activity; no draft PR counts as complete; no issue closes before an observed merge.

### Automatic babysitter

Enable `babysitter.enabled`, open a non-draft PR through the issue-driven path, and confirm only one babysitter starts. Make CI fail or add a review request, confirm only the exact repo/PR owner wakes, then enter the documented destructive critical section and prove Factory ACKs only after the no-submit fence persists. Confirm coalesced activity is delivered once after exit, the babysitter fixes the current PR branch, then send/observe its readiness signal. A mismatched signal or draft/closed PR must not advance the issue.

Do not attempt a cross-host active/active control-plane test: the supported ownership topology is multiple Factory processes sharing one same-host `FileStateStore`. Remote relay execution nodes are supported and do not need that directory.

### Standalone babysitter

```bash
factory babysit https://github.com/<owner>/<repo>/pull/<number> \
  --config ./factory.config.json
```

Pass criteria: the command rejects a draft, closed, merged, incomplete, or cross-repository PR; uses one explicit linked issue when available; otherwise uses PR title/body; prints a spawned receipt; leaves final merge to a human.

### Guarded merge and close

For a disposable PR, prove each refusal (dirty, unapproved, missing checks, failing/pending check, moved head), then make it clean, approved, and green. With `mergePolicy: on-green-with-review` and `terminalState: done`, verify the guarded squash merge uses the checked head SHA and the issue advances only after merge observation.

For a synthetic probe:

```bash
factory close-probe <number> --repo <owner/repo> --issue <KEY> \
  --config ./factory.config.json
```

Pass criteria: wrong key/title marker refuses closure; the correct probe closes and reports live CLOSED state.

### Loop process controls

Start a disposable live daemon, read its heartbeat, run `factory kill-loop`, and confirm SIGTERM reaches only that PID. After simulating a stale heartbeat/registry, run `factory reap-orphans` and inspect the report before accepting the result.

---

## Verification by Change Area

| Change area | Minimum tiers |
| --- | --- |
| CLI parser or entrypoint (`src/cli/`, `bin/`) | 1, 2, plus the command's tier |
| Config or triage (`src/config/`, `src/triage/`) | 1, 2, 3 |
| Logging normalization (`src/logging.ts`) | 1, 2 |
| Core orchestrator (`src/orchestrator/factory.ts`) | 1–5; tier 6 for PR/completion changes |
| Safety or guarded write paths | 1, 2, 5, 6 |
| Linear/GitHub/Slack writeback | 1, 2, 5 |
| Merge gate, babysitter, probe closer | 1, 2, 5, 6 |
| Internal/hosted fleet clients | 1, 2, 4 |
| Node capability or checkout resolution | 1, 2, 4 |
| Mount/event/subscription code | 1, 2, 5 |
| State/reaper/process identity | 1, 2, 4 |
| Durable dispatch/babysitter state | 1, 2, 4, 5; tier 6 for destructive-fence behavior |
| Public exports | 1 |

---

## Manifest Coverage Index

Every feature below maps to the procedure for its manifest tier. This index is intentionally explicit so a manifest/procedure drift check can prove that no feature lacks a verification route.

### Tier 1 IDs

`cli-help`, `cli-config-option`, `safety-relay-token-types`, `safety-merge-head-sha`, `webhook-hmac-validation`, `webhook-event-routing`, `webhook-event-deduplication`, `subscription-canonical-paths`, `subscription-delivery-targets`, `subscription-linear-predicates`, `api-config-schemas`, `api-triage-engines`, `api-fleet-ports`, `api-mount-ports`, `api-writeback-ports`, `api-state-stores`, `api-reaper`, `api-relayflow-policy`, `api-testing`, `api-safe-log-serialization`, `config-node-path-env`, `config-node-name-env`.

### Tier 2 IDs

`cli-run-once`, `cli-status`, `cli-loop`, `cli-loop-status`, `cli-dry-run-option`, `triage-label-routing`, `triage-project-routing`, `triage-keyword-routing`, `triage-default-routing`, `triage-unroutable-escalation`, `triage-single-scope`, `triage-team-scope`, `triage-workflow-scope`, `triage-shape-labels`, `triage-surface-inference`, `triage-thin-issue-detection`, `triage-llm-fallback`, `triage-tiered-fail-safe`, `triage-decision-normalization`, `triage-repo-qualified-agent-identities`, `dispatch-batch-admission`, `dispatch-duplicate-suppression`, `dispatch-composite-issue-identity`, `dispatch-retry-backoff`, `dispatch-retry-limit`, `dispatch-agent-task-rendering`, `dispatch-inflight-registry`, `pr-never-auto-merge`, `safety-title-prefix`, `safety-label-gate`, `safety-team-gate`, `safety-babysit-pr-identity`, `safety-node-checkout-containment`, `node-definition`, `node-inventory-sync`, `api-factory-lifecycle`, `api-factory-events`, `api-linear-state-resolution`, `config-workspace-id`, `config-issue-source`, `config-batch-size`, `config-subscription-teams`, `config-subscription-projects`, `config-subscription-labels`, `config-subscription-assignees`, `config-live-transport`, `config-live-poll-interval`, `config-live-event-limit`, `config-live-replay-skew`, `config-dispatch-cooldown`, `config-dispatch-attempts`, `config-triage-implementers`, `config-repos-org`, `config-repos-names`, `config-repos-overrides`, `config-repos-by-label`, `config-repos-by-project`, `config-repos-keyword-rules`, `config-repos-default`, `config-repos-clone-root`, `config-repos-clone-paths`, `config-clone-root`, `config-clone-paths`, `config-home-clone-expansion`, `config-cwd-clone-inference`, `config-local-checkout-preflight`, `config-loop-max-iterations`, `config-loop-failure-limit`, `config-loop-heartbeat-path`, `config-loop-registry-path`, `config-loop-heartbeat-stale`, `config-loop-heartbeat-alias`, `config-loop-registry-alias`, `config-model-implementer`, `config-model-reviewer`, `config-model-triage`, `config-model-babysitter`, `config-babysitter-enabled`, `config-merge-policy`, `config-terminal-state`, `config-slack-channel`, `config-slack-style`, `config-slack-bot-user`, `config-slack-staleness`, `config-slack-conversation-coalesce`, `config-linear-ready-name`, `config-linear-implementing-name`, `config-linear-planning-name`, `config-linear-done-name`, `config-linear-human-review-name`, `config-linear-states-by-team`, `config-linear-team-ids`, `config-state-id-ready`, `config-state-id-implementing`, `config-state-id-planning`, `config-state-id-done`, `config-state-id-human-review`, `config-safety-title-prefix`, `config-safety-label`, `config-safety-team`, `config-node-capabilities`, `config-node-dry-run`, `config-combined-envelope`, `config-split-envelope`, `config-fixture-files`.

### Tier 3 IDs

`cli-canary`, `cli-triage`, `cli-github-numeric-issue`, `discovery-linear-ready`, `discovery-github-native`, `discovery-github-path-shapes`, `discovery-canonical-fallback`, `discovery-state-name-backfill`, `dispatch-label-authority`, `safety-github-open-label`.

### Tier 4 IDs

`cli-start-live`, `cli-reap-orphans`, `cli-dispatch`, `cli-backend-option`, `cli-agent-exit-timeout-option`, `fleet-spawn-codex`, `fleet-spawn-claude`, `fleet-spawn-workflow`, `fleet-resume`, `fleet-target-node`, `fleet-spawn-inputs`, `fleet-roster`, `fleet-release`, `dispatch-queue-promotion`, `dispatch-implementers`, `dispatch-reviewer`, `dispatch-workflow-agent`, `dispatch-confirmed-task-injection`, `dispatch-critical-delivery-retry`, `dispatch-agent-resume`, `dispatch-remote-re-adoption`, `dispatch-durable-relay-lifecycle`, `dispatch-same-host-owner-fencing`, `human-agent-questions`, `human-team-release-for-clarification`, `human-durable-clarification-wake`, `human-slack-conversation-resume`, `pr-durable-babysitter-sessions`, `pr-babysitter-wake-coalescing`, `pr-babysitter-critical-ack-fence`, `fleet-internal-backend`, `fleet-broker-reuse`, `fleet-broker-autostart`, `fleet-owned-broker-drain`, `fleet-relay-backend`, `fleet-relay-repo-placement`, `fleet-relay-reconciliation`, `fleet-message-events`, `node-spawn-capabilities`, `node-workflow-capability`, `node-relay-mcp-harness`.

### Tier 5 IDs

`discovery-github-linear-mirror`, `discovery-github-mirror-close`, `discovery-source-auto-select`, `discovery-startup-backfill`, `discovery-live-subscribe`, `discovery-live-poll`, `discovery-live-hybrid`, `discovery-replay-suppression`, `discovery-duplicate-suppression`, `discovery-high-water-fallback`, `discovery-relayflow-events`, `dispatch-exit-pr-publication`, `dispatch-remote-publication-recovery`, `dispatch-state-writeback`, `human-slack-dispatch-thread`, `human-triage-escalation`, `human-github-question-fallback`, `human-slack-reply-watch`, `human-clarification-escalation-retry`, `human-clarified-redispatch`, `human-slack-degraded-mode`, `pr-completion-detection`, `pr-draft-block`, `pr-canonical-event-routing`, `pr-polling-readback-backstop`, `pr-human-review-terminal`, `pr-done-terminal`, `pr-post-merge-advance`, `safety-guarded-linear-drafts`, `safety-guarded-github-drafts`, `safety-delete-fail-closed`, `mount-active-workspace`, `mount-cloud-session`, `mount-cloud-filesystem`, `mount-write-confirmation`, `mount-local-auto-start`, `mount-stale-self-heal`, `mount-clone-paths`, `writeback-linear-state`, `writeback-linear-comment`, `writeback-linear-create`, `writeback-linear-verify`, `writeback-github-lifecycle`, `writeback-slack-thread`, `webhook-registration`, `webhook-unregistration`, `subscription-stream-recovery`.

### Tier 6 IDs

`cli-kill-loop`, `cli-babysit`, `cli-close-probe`, `human-authorized-github-replies`, `pr-babysitter-opt-in`, `pr-standalone-babysitter-validation`, `pr-babysitter-ready-signal`, `pr-merge-gate-check`, `pr-guarded-merge`, `pr-on-green-merge-policy`, `pr-synthetic-probe-cleanup`, `safety-probe-close-identity`, `writeback-github-pr-publication`, `writeback-github-pr-close`.

---

## Quick Sanity Check

```bash
set -euo pipefail
npm run build
npm test
node bin/factory.mjs --help >/dev/null
node --input-type=module <<'NODE'
import { readFileSync, writeFileSync } from 'node:fs'
const config = JSON.parse(readFileSync('test/fixtures/factory.config.json', 'utf8'))
config.fixtureFiles['/linear/issues/AR-77__uuid-77.json'].payload.url =
  'https://linear.app/agent-relay/issue/AR-77/cli-dry-run'
writeFileSync('/tmp/factory-tier2-config.json', JSON.stringify(config, null, 2))
NODE
node bin/factory.mjs run-once --config /tmp/factory-tier2-config.json --dry-run \
  | node --input-type=module -e "let s='';process.stdin.on('data',c=>s+=c);process.stdin.on('end',()=>{const r=JSON.parse(s);if(!r.dryRun||r.triaged.length<1)process.exit(1)})"
```

This is the default post-change gate. It is safe, deterministic, and requires no provider or broker.
