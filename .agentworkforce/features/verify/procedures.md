# Feature Verification Procedures

This is the executable companion to `../manifest.yaml`. Its
`verification.categories` map assigns every feature category to one named
procedure below. Verify Factory from the user's point of view, lowest
prerequisite tier first. A higher tier does not replace the lower tiers: it adds
provider, fleet, cloud, or live-work prerequisites.

Use a disposable workspace, issue, repository branch, and Slack channel for tiers 3–6. Never point mutation checks at production work unless the operator explicitly selected those records.

For any mutating procedure, start in a Bash shell with a unique run and an
exact cleanup target:

```bash
set -Eeuo pipefail
RUN_ID="${CI_RUN_ID:-${GITHUB_RUN_ID:-local}}"
RUN_RANDOM="$(od -An -N6 -tx1 /dev/urandom | tr -d '[:space:]')"
RUN="factory-feature-${RUN_ID}-$(date +%s)-$RUN_RANDOM"
TMP="$(mktemp -d)"
CONFIG="$TMP/factory.config.json"
cleanup() {
  status=$?
  if [[ -n "${FACTORY_VERIFY_PID:-}" ]] && kill -0 "$FACTORY_VERIFY_PID" 2>/dev/null; then
    command="$(ps -ww -p "$FACTORY_VERIFY_PID" -o command= 2>/dev/null || true)"
    if printf '%s\n' "$command" | grep -F -- "bin/factory.mjs" >/dev/null && \
       printf '%s\n' "$command" | grep -F -- "$CONFIG" >/dev/null; then
      kill -TERM "$FACTORY_VERIFY_PID" 2>/dev/null || true
      wait "$FACTORY_VERIFY_PID" 2>/dev/null || true
    else
      printf 'Refusing to terminate unexpected PID %s\n' "$FACTORY_VERIFY_PID" >&2
    fi
  fi
  rm -rf "$TMP"
  exit "$status"
}
trap cleanup EXIT
```

The cleanup guard never signals a PID merely because it was recorded. Provider
issues, branches, PRs, comments, labels, webhooks, Slack threads, and cloud
records must use `$RUN` in their identity and be removed or closed through the
same provider after assertions. If the available API cannot create and safely
remove that fixture, the procedure is manual for that environment.

---

## Tier 1 — Package Only

Requires only a source checkout with dependencies installed. These checks are deterministic and must run on every change.

### Build, test, entrypoints, and manifest

```bash
npm run build
npm test

node bin/factory.mjs --help
# → exits 0 and lists every top-level command and global option

node bin/factory.mjs featuremap check
# → validates schema, unique ids, catalog totals, tiers, and every location path
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

## cli-and-package

**Categories:** `cli-operations`.

**Prerequisites:** source checkout, Node 20.18.1 or newer, and `npm ci`. No
provider credentials are required for the package-only portion.

```bash
npm run build
npm test -- --run src/cli/fleet.test.ts src/featuremap/validate.test.ts
node bin/factory.mjs --help | tee "$TMP/help.txt"
node bin/factory.mjs --version | tee "$TMP/version.txt"
node bin/factory.mjs featuremap check | tee "$TMP/featuremap.json"
node -e 'const p=require("./package.json"); if (!p.version) process.exit(1)'
grep -Fq 'featuremap check' "$TMP/help.txt"
grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+' "$TMP/version.txt"
node -e 'const r=require(process.argv[1]); if (!r.ok || r.featureCount < 1) process.exit(1)' "$TMP/featuremap.json"
```

Then exercise `run-once`, `status`, `loop`, and `loop-status` with the fixture
from Tier 2. Exercise `triage` and the always-dry `canary` with the disposable
provider fixture from `provider-discovery`; exercise `dispatch` only after the
fleet fixture is ready. `start`, `kill-loop`, `reap-orphans`, `babysit`, and
`close-probe` use their owning procedures below. Assert stdout is parseable JSON
where promised, progress logs stay on stderr, unknown commands/options fail, and
help/version/feature-map validation do not load config or construct providers.

**Automation limit:** the package and fixture subset is deterministic. Commands
that signal a process or mutate an issue/PR remain live or manual checks.

## fleet-execution

**Categories:** `fleet-cli`, `fleet-node`, `config-node-and-loading`.

**Prerequisites:** an isolated Relay project; installed/authenticated Codex or
Claude CLI for real harness spawns. For hosted placement, use a disposable
workspace key and an enrolled node advertising a disposable checkout.

```bash
npm run build
npx vitest run \
  src/cli/fleet.test.ts \
  src/fleet/create-fleet.test.ts \
  src/fleet/ensure-relay-broker.test.ts \
  src/fleet/internal-fleet-client.test.ts \
  src/fleet/relay-fleet-client.test.ts \
  src/node/factory-node.test.ts

NAME="factory-vf-$RUN"
node bin/factory.mjs fleet roster --backend internal | tee "$TMP/roster-before.json"
node bin/factory.mjs fleet spawn spawn:codex --backend internal \
  --name "$NAME" --task 'Reply with exactly FACTORY_VERIFY_OK' | tee "$TMP/spawn.json"
node bin/factory.mjs fleet roster --backend internal | tee "$TMP/roster-live.json"
node bin/factory.mjs fleet release "$NAME" --reason feature-verification --backend internal
node bin/factory.mjs fleet roster --backend internal | tee "$TMP/roster-after.json"
grep -Fq "$NAME" "$TMP/roster-live.json"
if grep -Fq "$NAME" "$TMP/roster-after.json"; then exit 1; fi
```

Repeat with `spawn:claude`, `workflow:run`, `--model`, `--cwd`, and
`--resume <session-ref>` when those fixtures exist. For `--backend relay`, pass
`--node` and an advertised checkout, assert returned invocation/node identity,
then retry with an unadvertised path and require refusal. Restart the client from
saved tracking and prove it adopts the same invocation before processing an
exit. Canonical local presence must exclude stale/offline rows, preserve live
non-MCP workers, fail closed when presence is unavailable, and confirm absence
before a released name is reused. An owned broker must honor its isolated state
directory and stop only after the configured task-exit drain; an operator-owned
broker must remain running.

## provider-discovery

**Categories:** `discovery-events`, `config-linear-states`.

**Prerequisites:** one disposable Linear issue and/or GitHub issue selected by a
disposable config. Tier-3 reads use `--dry-run`; tier-5 stream/mirror checks need
connected integrations. Copy the real config to `$CONFIG` and change only test
scope to records named with `$RUN`.

```bash
factory triage "$FACTORY_VERIFY_CANARY_ISSUE" --config "$CONFIG" | tee "$TMP/triage.json"
factory canary "$FACTORY_VERIFY_CANARY_ISSUE" --config "$CONFIG" | tee "$TMP/canary.json"
factory run-once --config "$CONFIG" --dry-run | tee "$TMP/discovery.json"
node -e 'const r=require(process.argv[1]); if (!r.ok) process.exit(1)' "$TMP/canary.json"
```

For Linear, assert canonical and sparse aliases resolve the configured team
state names/IDs. For GitHub, cover `owner__repo` and `owner/repo`, `meta.json`,
`by-id`, flat and paginated listings, numeric disambiguation, closed/unlabeled
exclusion, source auto-selection, and cross-path deduplication without merging
equal numbers from different repos. Then start each `subscribe`, `poll`, and
`subscribe-and-poll` transport before one test update; assert exactly one intake,
replay suppression, buffered high-water fallback, and stream recovery. In
Linear-source mode, assert a factory-labeled GitHub issue creates one mirror and
its closure advances that mirror once.

## triage-and-configuration

**Categories:** `triage-routing`, `config-intake`, `config-repositories`.

**Prerequisites:** package-only fixtures for schema/routing; the provider fixture
from `provider-discovery` for a true issue round trip.

```bash
npx vitest run \
  src/config/schema.test.ts \
  src/config/local-clone-paths.test.ts \
  src/triage/triage.test.ts \
  src/safety/factory-scope.test.ts
```

Assert label → project → keyword → default precedence, low-confidence refusal,
single/team/workflow shape, surface detection, LLM fallback, decision
normalization, and repo-qualified GitHub agent identities. Parse defaults and
both envelope forms for every manifest-listed field, then test min/max and enum
rejections. Validate compact repo derivation, explicit precedence, exact `~` and
`~/` expansion, `~user` rejection, one-repo cwd inference only after matching
the GitHub remote, linked-worktree acceptance, and preflight refusal for a
missing/non-git/non-root checkout. Finally compare the live `triage` result with
the deterministic fixture result; no write or spawn is allowed during this
procedure.

## issue-dispatch-lifecycle

**Categories:** `dispatch-orchestration`.

**Prerequisites:** Tier 2 fixture for deterministic paths, then a disposable
provider issue plus fleet for the full lifecycle.

```bash
npx vitest run \
  src/orchestrator/batch-tracker.test.ts \
  src/orchestrator/factory.test.ts \
  src/dispatch/templates.test.ts \
  src/git/agent-worktree.test.ts \
  src/state/file-state-store.test.ts \
  src/state/github-lifecycle-identity.test.ts
factory dispatch "$FACTORY_VERIFY_CANARY_ISSUE" --config "$CONFIG" --dry-run \
  | tee "$TMP/dispatch-dry.json"
```

Assert one batch admission, capacity queue/promotion, duplicate suppression,
bounded retry, authoritative label identity, rendered per-role tasks, confirmed
task injection, and one resumption. Add `Blocked by:` fixtures for same-repo,
cross-repo, closed, merged, missing, and cyclic dependencies; parked dependencies
must not consume capacity and cycles must report rather than run. With a real
fleet, verify each agent receives a unique owned worktree and branch, remote PR
branches recover without overwriting divergence, and cleanup refuses any path
outside `.factory-worktrees`. Crash at spawn intent, acknowledgement,
publication receipt, writeback, and release; a replacement same-host owner must
adopt exactly once, preserve global capacity, canonicalize legacy GitHub aliases
atomically, reuse provider receipts, publish from the remote head, and reach a
terminal state without duplicate agent, PR, or comment.

## human-clarification

**Categories:** `human-loop`, `config-models-and-human`.

**Prerequisites:** deterministic state tests, then a disposable Slack channel
and issue reporter identity. GitHub fallback needs a disposable issue owned by
that reporter.

```bash
npx vitest run \
  src/config/schema.test.ts \
  src/orchestrator/coalesced-task-queue.test.ts \
  src/orchestrator/factory.test.ts \
  src/state/file-state-store.test.ts
```

Create one thin triage issue and one running worker that emits the documented
needs-input signal. Assert configured stakeholder IDs are mentioned, only the
first concurrent question is reserved, the full team is released and absent
before the issue is parked, and one authorized non-bot reply claims one wake.
Restart before and after the reply to cover saved-session resume, cold start,
reply recovery, wake lease renewal, scope-loss cancellation, and seven-day
escalation retry. Ordinary Slack conversation turns must be coalesced within the
configured window and delivered only through a fresh resume task—never injected
into a running PTY. Disable or stale Slack and require a correlated GitHub
comment; bot/unauthorized replies must be ignored. Exercise both harness
capabilities and all per-role models, then delete only `$RUN` threads/comments.

## pull-request-lifecycle

**Categories:** `pr-lifecycle`.

**Prerequisites:** disposable issue, same-repository non-draft PR, provider
mount, and fleet. The deterministic refusals run without credentials.

```bash
npx vitest run \
  src/github/merge-gate.test.ts \
  src/github/probe-closer.test.ts \
  src/github/standalone-babysitter.test.ts \
  src/orchestrator/factory.test.ts \
  src/state/file-state-store.test.ts
```

With babysitting enabled, assert exactly one PR owner/session for the exact
normalized repo and number. Route review, review-comment, issue-comment,
failed/cancelled/timed-out check, conflict, and base-divergence events; pending,
green, inconsistent, or body-only identities must not wake. Coalesce wakes,
retain an event during delivery, retry without duplication, persist the
destructive no-submit fence before ACK, defer PTY input through the critical
section, and cancel after terminal mounted readback. A valid readiness signal
advances to Human Review; wrong agent/issue, draft, closed, or merged PR does
not. For standalone mode, run `factory babysit <URL> --config "$CONFIG"` and
assert one receipt for the existing branch while final merge remains human.
Test merge refusals, then green+approved+stable-head guarded merge and post-merge
issue advancement. Close only a `$RUN` synthetic probe and prove the wrong key
or title marker refuses.

## safety-boundaries

**Categories:** `safety`, `config-safety`.

**Prerequisites:** package fixtures; a disposable provider scope for guarded
write assertions.

```bash
npx vitest run \
  src/safety/factory-scope.test.ts \
  src/github/merge-gate.test.ts \
  src/github/probe-closer.test.ts \
  src/node/factory-node.test.ts \
  src/__tests__/mount-delete-callsite-invariant.test.ts \
  src/__tests__/writefile-callsite-invariant.test.ts
```

Assert boundary-aware title, exact label forms, team restrictions, open GitHub
status, token-type rejection, node checkout containment, stable merge head,
probe/standalone PR identity, and fail-closed delete behavior. Against the
disposable provider, attempt one out-of-scope Linear write, unsupported GitHub
draft, unapproved delete, unadvertised node path, moved-head merge, and wrong
probe close; every operation must fail before external mutation. Never weaken
scope merely to make this procedure pass.

## integrations-and-writeback

**Categories:** `mount-writeback`.

**Prerequisites:** non-interactive Cloud session and disposable Linear, GitHub,
and optional Slack integrations with bounded write permission.

```bash
npx vitest run \
  src/mount/local-mount-preflight.test.ts \
  src/mount/relayfile-binary.test.ts \
  src/mount/relayfile-cloud-mount-client.test.ts \
  src/mount/relayfile-github-connection-write.test.ts \
  src/mount/relayfile-integration-preflight.test.ts \
  src/writeback/writeback.test.ts
factory run-once --config "$CONFIG" --dry-run | tee "$TMP/mount-preflight.json"
```

Inspect the selected workspace, session, filesystem, local mount state, clone
mounts, and provider connections. Missing integrations must name the provider
and offer OAuth only in an interactive non-dry run; headless/dry runs must stop
with an actionable command. Simulate stale/mismatched/dead mount state and
require self-heal; live `start` must warm mounts in the background without using
stale mirrors as source truth. For each provider, perform a scoped `$RUN`
comment/state/create/thread/reply/PR write, require provider acknowledgement,
then re-read the external object. Exercise strict `github.identity=app`, strict
`user`, and `auto`; record confirmed identity/author and forbid fallback for a
strict mode. Confirm the bounded mount health record. Remove only the test
records after readback.

## event-intake

**Categories:** `webhook-subscriptions`.

**Prerequisites:** package fixtures; live registration needs a disposable
workspace and callback owned by the test.

```bash
npx vitest run \
  src/webhook/handler.test.ts \
  src/webhook/registrar.test.ts \
  src/subscriptions/__tests__/globs.test.ts \
  src/subscriptions/__tests__/linear-filter.test.ts \
  src/subscriptions/__tests__/slack-filter.test.ts \
  src/subscriptions/__tests__/specs.test.ts \
  src/subscriptions/__tests__/event-client.test.ts
```

Assert invalid HMAC → 403, malformed JSON → 400, replayed event ID → 200 with
no second callback, and correct Linear/Slack/GitHub routing. Verify canonical
paths, DM/thread aliases, targets, predicates, token-workspace checks,
path-coalescing, polling fallback, and stream recovery. Register the unique live
callback twice and require one subscription, deliver one signed test event and
assert its body/header at the controlled receiver, then unregister and prove it
is absent. A shared public request bin is not an acceptable receiver.

## public-api

**Categories:** `programmatic-api`.

**Prerequisites:** package checkout and a clean temporary consumer.

```bash
npm run build
npx vitest run src/__tests__/dist-entrypoints.test.ts
npm pack --pack-destination "$TMP"
mkdir "$TMP/consumer" && cd "$TMP/consumer"
npm init -y >/dev/null
npm install --ignore-scripts "$TMP"/*.tgz >/dev/null
node --input-type=module <<'NODE'
for (const subpath of ['', '/observability', '/testing', '/writeback', '/featuremap', '/hosted', '/environments']) {
  const mod = await import(`@agent-relay/factory${subpath}`)
  if (Object.keys(mod).length === 0) throw new Error(`empty export ${subpath || '/'}`)
}
const node = await import('@agent-relay/factory/node')
if (!node.default) throw new Error('node default export missing')
NODE
```

Back in the checkout, run the focused tests named by each API entry. Assert root
types and all eight package export keys resolve, hosted remains worker-safe,
feature-map validation includes procedure routing, dependency/worktree ports are
exported, and fake clients support a hermetic consumer. Clean only `$TMP`.

## hosted-control-plane

**Categories:** `hosted-control-plane`.

**Prerequisites:** deterministic port fakes. A deployed check additionally needs
a disposable Cloud worker, Durable Object namespace, providers, and fleet.

```bash
npx vitest run \
  src/hosted/orchestrator.test.ts \
  src/hosted/state-store.test.ts \
  src/hosted/worker-safety.test.ts
npm run verify:e2e
```

Assert reconciliation precedes discovery; scope, dedupe, batch capacity, triage,
deterministic invocation IDs, clarification, spawn, pushed completion, polling
completion, merge gate, and every provider writeback transition. Repeat each
external success immediately before a simulated fenced save and require the
same invocation or idempotency key after takeover. Two owners must yield one
live epoch; expired owners cannot save. Run the same matrix through in-memory
and Durable Object transaction adapters, including invocation index removal.
Finally inspect the hosted dependency graph and packed export for Node
filesystem/process/child-process imports. The deployed portion is Tier 5 and is
skipped—not passed—when those fixtures are unavailable.

## cloud-observability

**Categories:** `cloud-observability`.

**Prerequisites:** local filesystem for event/outbox tests; disposable Cloud
account/workspace for authenticated ingestion.

```bash
npx vitest run \
  src/observability/events.test.ts \
  src/observability/instance-identity.test.ts \
  src/observability/outbox.test.ts \
  src/observability/cloud-reporter.test.ts \
  src/cli/fleet.test.ts
```

Attempt forbidden task, prompt, message, path, command, source, token, raw error,
and stack fields and require schema rejection. Assert stable nonzero trace IDs
for the same durable run and different IDs for different runs without span
claims. Concurrent identity creation must converge on one mode-0600 opaque UUID
and never derive it from host/user/path. Restart the outbox, compact beyond
count/byte limits while retaining critical events, batch under 240 KiB, and ACK
only exact delivered IDs. Against Cloud, verify authenticated 201 ingestion,
duplicate acceptance, token refresh, timeout, retry/backoff, permanent rejection,
shutdown deadline, and replay after restart. Every reporter failure must leave
Factory control flow and exit semantics unchanged.

## release-verification

**Categories:** `release-verification`.

**Prerequisites:** package checkout. PR evidence also requires exact head/base
SHAs from the reviewed event.

```bash
npm run build
npm run featuremap:check
npm test
FACTORY_E2E_HEAD_SHA="$(git rev-parse HEAD)" \
FACTORY_E2E_BASE_SHA="$(git merge-base HEAD origin/main 2>/dev/null || git rev-parse HEAD^)" \
  npm run verify:e2e
node -e 'const a=require("./artifacts/factory-e2e-attestation.json"); if (a.git?.headSha !== process.argv[1]) process.exit(1)' "$(git rev-parse HEAD)"
```

For Kubernetes provider changes, a Docker-capable node with kind, kubectl, and
Helm must additionally run `npm run test:e2e:kubernetes`; the check is not
passed when those prerequisites are absent.

Assert the manifest contract test covers every CLI leaf, config field, public
subpath, category procedure, and required implementation area. The packed run
must install the tarball in a clean consumer, import root/hosted surfaces, drive
the fixture and hosted lifecycle, and record only passing checks. Follow
`docs/pr-end-to-end-verification.md`; never accept an artifact from the base,
synthetic merge, another head, or a run that skipped its declared check. The
Kubernetes binding run must prove a positive cross-namespace connectivity
control before its deny assertion, then retain the exact production namespace
and workload identities through teardown and reaping.

## proactive-health

**Categories:** `proactive-health`.

**Prerequisites:** proactive preview for deterministic tests; deployed check
needs the scoped GitHub clone, exact Relayfile credentials, and the configured
Slack channel.

```bash
npx vitest run \
  .agentworkforce/agents/factory-feature-guardian/manifest-contract.test.ts \
  .agentworkforce/agents/factory-feature-guardian/agent.test.ts
```

In preview, begin with no state, inject a checkpoint failure after a confirmed
Slack write, and rerun: the same cycle/feature idempotency key must return the
same provider timestamp and create only one Slack message. Verify positions 2
and 3 across fresh contexts; additions preserve progress, one checked retirement
starts a new generation under CAS, unchecked retirement preserves it, suspicious
shrink/multiple retirements fail closed, and complete cycles increment exactly
once. Reject malformed, duplicate, oversized, stale-revision, authorization,
timeout, corrupt-readback, and receiptless states without advancing. Force LLM
failure and require the fallback to retain CLI/API, source, tier, and procedure.
In deployment, assert the manifest is read only from
`/github/repos/AgentWorkforce/factory/.agentworkforce/features/**`, Slack can
write only to `${SLACK_CHANNEL}`, and one scheduled tick posts one question with
a real provider `ts` before the exact state checkpoint advances.

## loop-and-recovery

**Categories:** `config-loop`.

**Prerequisites:** fixture config with heartbeat and registry paths inside
`$TMP`; a live manual check may start one exact process using `$CONFIG`.

```bash
node bin/factory.mjs loop --config "$CONFIG" --dry-run
node bin/factory.mjs loop-status --config "$CONFIG" | tee "$TMP/liveness.json"
npx vitest run \
  src/orchestrator/factory.test.ts \
  src/orchestrator/process-identity.test.ts \
  src/orchestrator/reaper.test.ts \
  src/state/file-state-store.test.ts
```

Assert iteration/failure limits, heartbeat/registry aliases and paths, stale
threshold, circuit opening, idle completion, atomic registry recovery, PID
identity, and protected infrastructure. For the manual signal check, launch
`factory start --config "$CONFIG"` in the background, record its exact PID and
command, run `loop-status`, then `kill-loop`; only that PID may receive SIGTERM.
Seed one stale Factory-owned child and one unrelated/broker/node PID, run
`reap-orphans`, and require only the exact stale child to terminate. The shared
cleanup guard handles a failed assertion without signalling a reused PID.

## verification-environments

**Categories:** `verification-environments`.

**Prerequisites:** tiers 1–2 require only the checkout and fixture config. The
live scenarios require Docker, `kind`, and `kubectl`; the manual merge decision
also requires one disposable reviewed pull request and therefore remains tier 6.

```bash
npm run build
npx vitest run \
  src/environments/stack-descriptor.test.ts \
  src/environments/kubernetes-provider.test.ts \
  src/environments/verification-stack-descriptor.test.ts \
  src/environments/verification-stack-deployer.test.ts \
  src/environments/kubernetes-verification-provider.test.ts \
  src/environments/load-harness.test.ts \
  src/environments/verification-pipeline.test.ts \
  src/orchestrator/environment-reaper.test.ts

kind create cluster --name factory-gate-e2e
npm run test:e2e:verification
kind delete cluster --name factory-gate-e2e
```

Require the real cluster run to prove one green decision, E2E-red and load-red
decisions, bounded timeout teardown, hard-kill reaping, namespace absence, and
Cloud-reporter evidence. Unit coverage must prove that a red verifier never
reaches the guarded merge operation and that the exact reviewed head is passed
to the verifier. Always delete the kind cluster in a shell trap. Do not claim
the tier-6 merge lifecycle without a disposable approved PR.

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
| Dependency admission or isolated worktrees | 1, 2, 3, 4 |
| Hosted control plane (`src/hosted/`) | 1, 2, 5; tier 6 for merge/writeback behavior |
| Cloud observability and reporting | 1, 2, 5 |
| Feature manifest, verification workflow, or guardian | 1, 2, 5 |
| State/reaper/process identity | 1, 2, 4 |
| Durable dispatch/babysitter state | 1, 2, 4, 5; tier 6 for destructive-fence behavior |
| Public exports | 1 |

---

## Manifest Coverage Contract

The v1.1 manifest is the authoritative feature list. Its
`verification.categories` map routes every category to one named procedure in
this document; the validator rejects missing routes, unknown category routes,
missing procedure headings, duplicate IDs, stale summary counts, invalid source
paths, and path escapes. The guardian manifest contract additionally enumerates
the public CLI leaves, config schema fields, package export subpaths, and required
implementation areas so a newly shipped surface cannot silently bypass the
catalog.

To inspect the live mapping without maintaining a second list of IDs:

```bash
npm run build
node bin/factory.mjs featuremap check
node --input-type=module - <<'NODE'
import { readFileSync } from 'node:fs'
import { parseManifestFeatures } from './dist/featuremap/validate.js'
const features = parseManifestFeatures(readFileSync('.agentworkforce/features/manifest.yaml', 'utf8'))
for (const feature of features) {
  console.log(`${feature.id}\t${feature.category}\t${feature.procedure}\ttier-${feature.tier}`)
}
NODE
```

---

## Quick Sanity Check

```bash
set -euo pipefail
VERIFY_TMP="$(mktemp -d)"
trap 'rm -rf "$VERIFY_TMP"' EXIT
npm run build
npm test
node bin/factory.mjs --help >/dev/null
node --input-type=module - "$VERIFY_TMP/factory.config.json" <<'NODE'
import { readFileSync, writeFileSync } from 'node:fs'
const config = JSON.parse(readFileSync('test/fixtures/factory.config.json', 'utf8'))
config.fixtureFiles['/linear/issues/AR-77__uuid-77.json'].payload.url =
  'https://linear.app/agent-relay/issue/AR-77/cli-dry-run'
writeFileSync(process.argv[2], JSON.stringify(config, null, 2))
NODE
node bin/factory.mjs run-once --config "$VERIFY_TMP/factory.config.json" --dry-run \
  | node --input-type=module -e "let s='';process.stdin.on('data',c=>s+=c);process.stdin.on('end',()=>{const r=JSON.parse(s);if(!r.dryRun||r.triaged.length<1)process.exit(1)})"
```

This is the default post-change gate. It is safe, deterministic, and requires no provider or broker.
