# Critical Paths

The sequences that must work for `@agent-relay/factory` to turn opted-in issues into reviewed pull requests safely. Verify these first after changes and treat a regression in any safety guard as a release blocker.

---

## Path 1: Config → Discover → Triage (Foundation)

Factory must parse configuration, find a real ready issue, reconcile sparse provider records, enforce scope, and produce a deterministic route before it can spawn anything.

```bash
npm run build
# Prepare /tmp/factory-tier2-config.json as documented in verify/procedures.md.
factory run-once --config /tmp/factory-tier2-config.json --dry-run
# → pulled contains AR-77
# → triaged contains a label-derived AgentWorkforce/pear route
# → dispatched describes the dry-run agents
# → no provider writes or fleet spawns occur
```

Live Linear acceptance:

1. Read canonical issues plus Ready for Agent aliases.
2. Recover missing `state.id` from canonical aliases or configured state names.
3. Require the configured title/label and team scope.
4. Route by label → project → keyword → default, never by guesswork.

GitHub-native direct acceptance:

1. A positive numeric `canary`, `triage`, or `dispatch` target resolves through the configured source repository without requiring a repo-qualified path.
2. `repos.default` may disambiguate the number; otherwise multiple matching configured repositories fail as ambiguous.
3. Both `owner__repo` and `owner/repo` mounts resolve `meta.json`, `by-id`, flat issue records, and scoped fallback listings without collapsing distinct repositories.
4. Repository-only configuration selects GitHub intake, while explicit or legacy Linear signals keep Linear selected and resolution errors name the selected source.

**What breaks if this fails:** all issue-driven operation, the canary, safety scoping, and every downstream dispatch.

---

## Path 2: Triage → Batch → Team Dispatch

An eligible issue must enter the bounded batch exactly once, spawn its implementation shape, receive confirmed tasks, and write the lifecycle transition.

```bash
factory dispatch AR-123 --config ./factory.config.json --dry-run
# → normalized TriageDecision
# → stable ar-123-* agent names
# → no duplicate agent names on a repeated concurrent request

factory dispatch AR-123 --config ./factory.config.json
# → implementer(s) + reviewer or workflow:run are spawned
# → dispatch summary is posted
# → Linear moves to Agent Implementing, or GitHub gets factory:in-progress
```

For GitHub-native issues, every collision-prone role is repository-qualified and every state key is composite: normalized source repository plus issue number. Equal-number issues in different repositories must keep agents, registry records, retries, PR caches, publication guards, and babysitter ownership isolated. Linear agent names remain byte-compatible.

Batch capacity must queue excess work and promote the next issue when the current record completes. Dispatch failures must back off and stop at `dispatch.maxAttempts`. Local internal dispatch expands supported home-relative clone paths, infers cwd only for one remote-matching repository, and fails before spawn when a configured checkout is missing, non-git, or not its worktree root.

**What breaks if this fails:** agent execution, batch fairness, retry safety, issue lifecycle accuracy.

---

## Path 3: Implement → Publish PR → Review

The dispatched team must work in the configured checkout, commit and push a branch, and produce a normal pull request through the connected workspace GitHub write path.

```text
implementer receives issue + repo task
  → creates branch, edits, tests, commits, pushes
  → exits normally
factory resolves repo default branch
  → publishes branch ref through Relayfile
  → creates PR through Relayfile
  → confirms provider acknowledgement and reads PR receipt
reviewer receives the implementation handoff
  → reads PR, comments/approves, finishes
```

The PR publication fallback must also work when an implementer exits after pushing but before opening the PR. Draft PRs never count as completion.

Relay-backed dispatch is a durable lifecycle, not an exit callback: it records a spawn intent, deterministic invocation/branch identity, placement, publication phase and receipt, release checkpoints, and terminal writeback. After a crash or lease handoff it must adopt a roster-visible spawn, publish from the remote pushed head without reading an orchestrator-local clone, confirm the exact mounted PR is open/non-draft/on the expected head, reuse provider receipts, and resume release cleanup without duplicating the team or PR.

**What breaks if this fails:** Factory produces code with no reviewable artifact, and issues remain stuck in flight.

---

## Path 4: PR → Babysitter → Human Review

With `babysitter.enabled: true`, a PR-open event must spawn one babysitter, not complete the issue immediately.

```text
non-draft PR event or implementer-exit safety net
  → match PR to the in-flight issue
  → spawn ar-<n>-babysit with issue + PR + integration context
  → babysitter fixes CI/conflicts/review findings and pushes
  → babysitter DMs [factory-pr-ready] <KEY>
  → Factory verifies mounted PR is still open and non-draft
  → issue moves to Human Review and agents are released
```

Repeated PR events must not double-spawn the babysitter. A readiness signal from the wrong agent, wrong issue, a draft PR, a closed PR, or an already-merged PR must not advance the issue.

After spawn, canonical review, review-comment, issue-comment, failed-check, conflict, and base-divergence records must route only to the exact normalized repo/PR owner. Metadata-only wakes are durably coalesced and delivery-confirmed; events arriving during delivery are retained, transient failures retry, and restart restores session ownership plus pending categories. Before destructive babysitter work, Factory must persist the critical no-submit fence and only then ACK; wakes and PTY submissions remain deferred until the matching exit. Paginated polling follows the same router, and a mounted PR readback cancels stale or terminal work before delivery.

**What breaks if this fails:** PRs stall red, unsafe readiness claims are trusted, or duplicate agents mutate the same branch.

---

## Path 5: Merge Gate → Merge → Close

The actual close path is guarded and has two valid product branches.

### Human-owned merge (default)

```text
mergePolicy: never
  → Factory leaves the PR open
  → issue remains Human Review
  → merged PR event later arrives
  → Linear advances to Done or the GitHub-native issue closes
```

### Guarded automatic merge

```text
mergePolicy: on-green-with-review
terminalState: done
  → live GitHub check reports MERGEABLE + CLEAN
  → reviewDecision is APPROVED
  → at least one status check exists; none is blocking
  → live head SHA matches the checked SHA
  → gh pr merge --squash --delete-branch --match-head-commit <sha>
  → mounted PR state proves the merge
  → Linear advances to Done or the GitHub-native issue closes
```

Unknown mergeability, missing fields, no checks, pending/failing checks, absent approval, conflicts, or a moved head must leave the PR open.

**What breaks if this fails:** unsafe code merges, completed work never closes, or issues close before their PR actually merges.

---

## Path 6: GitHub-Native Issue Lifecycle

A workspace without Linear must still complete the full factory loop in GitHub.

```text
open GitHub issue + configured factory label + repo route label
  → issueSource resolves to github
  → safety and route checks pass
  → agents dispatch
  → factory:in-progress label + comment
  → PR opens and reaches Human Review
  → factory:human-review label + comment
  → PR merge event
  → issue closes with completion comment
```

When `issueSource: linear`, the same source issue must instead be mirrored once into Linear and closed-source changes must propagate to the mirror.

**What breaks if this fails:** GitHub-only customers cannot use Factory, or source and mirror lifecycles diverge.

---

## Path 7: Human Clarification Round Trip

Ambiguous intake and mid-task agent questions must reach an authorized human and return to the correct workers.

```text
thin/low-confidence triage OR [factory-needs-input] agent DM
  → post to persisted Slack dispatch thread
  → reserve the first question and release the complete agent team
  → durably park the issue only after fleet absence is confirmed
  → subscribe + poll for a non-bot reply
  → deduplicate the reply
  → lease exactly one wake
  → resume saved sessions, or cold-start from persisted issue/question/answer context
  → for ordinary thread conversation, durably coalesce replies and resume the thread-owned session with a fresh task
  → never inject ordinary Slack conversation input into a running agent session
```

If Slack is not configured, Factory must use a correlated GitHub issue comment and accept a reply only from the original issue reporter. Watchers, thread-to-session ownership, coalesced pending turns, release/parking phases, pending replies, wake progress, and seven-day stakeholder escalations must survive restart; failed resume/escalation attempts retry, and work that loses Factory scope cancels rather than waking.

**What breaks if this fails:** ambiguous work is guessed, agents wait forever, or an unauthorized commenter can steer execution.

---

## Path 8: Live Daemon → Heartbeat → Recovery

Production operation must remain live, observable, and recoverable across failures.

```bash
factory start --mode live --config ./factory.config.json
factory loop-status --config ./factory.config.json
# → ok=true, stale=false, heartbeat pid is live

# after a simulated stale owner:
factory reap-orphans --config ./factory.config.json
# → only identity-matched factory-owned processes are terminated
```

The daemon must register live intake before startup fallback pulls, suppress replay/duplicate events, refresh the heartbeat while remote operations are slow, persist in-flight agents, re-adopt hosted invocations after restart, and protect broker/node processes from the reaper. Durable dispatch ownership uses lease epochs and a shared `FileStateStore` to fence multiple control-plane processes and batch capacity on the same host through placement, publication, clarification parking, release, and completion.

Cross-host active/active Factory control planes are intentionally unsupported: separate local files cannot provide a truthful compare-and-set fence. Relay execution nodes may be remote and do not need the control-plane state directory, but all simultaneously active control-plane owners must share the same host state store.

**What breaks if this fails:** ready issues are missed or duplicated, agents leak, healthy infrastructure is killed, or hosted work is orphaned after restart.

---

## Path 9: Fleet Node Placement

Hosted execution must place work only on nodes that advertise both the required capability and an authorized checkout.

```text
factory --backend relay dispatch <KEY>
  → hosted action selects spawn:codex / spawn:claude / workflow:run
  → initial and no-session restart requests retain AgentSpec.repo
  → node resolves repo to clonePaths or cloneRoot
  → unadvertised cwd is rejected
  → task runs with invocation identity
  → inventory/action reconciliation reports exit to orchestrator
```

Internal execution must reuse an operator-owned broker when present, otherwise start a workspace-joined broker and shut down only the infrastructure Factory owns.

**What breaks if this fails:** work runs on the wrong machine or checkout, remote exits disappear, or Factory disrupts an operator's broker.

---

## Path 10: Hosted Run → Reconcile → Fence

The hosted control plane must turn one scheduled/event invocation into one
durable Factory run without allowing a stale owner to mutate current state.

```text
stable invocation input + owner lease
  → deterministic invocation/run/branch identities
  → one run-once execution
  → durable state and provider receipts
  → completion ingestion/reconciliation after retries or restart
  → merge/writeback decisions pass the same live safety gates as local Factory
  → stale lease epochs and duplicate completion events are rejected
```

Durable Object transaction state, invocation idempotency, lease generation, and
completion reconciliation must survive a worker restart. Failed cloud reporting
must not change orchestration success or duplicate provider mutations.

**What breaks if this fails:** the hosted service duplicates work, loses completion, or lets an expired owner publish or merge.

---

## Path 11: Lifecycle Event → Durable Outbox → Cloud

Every admitted lifecycle transition must emit a bounded, privacy-safe event
with stable instance/run identity and deterministic W3C trace correlation.

```text
Factory lifecycle transition
  → validate bounded event schema (no task/prompt/source/stack payload)
  → append to durable local outbox before delivery
  → batch to authenticated Cloud reporter
  → acknowledge only confirmed records
  → retry retained records after timeout/restart
  → never block or change the Factory orchestration result
```

Disabled reporting performs no network work. Malformed records remain bounded
and diagnosable, and delivery failures cannot erase undelivered events.

**What breaks if this fails:** Cloud progress lies or leaks sensitive content, or telemetry failure stalls production work.

---

## Path 12: Catalog → Procedure → Workflow → Guardian

Release verification must remain executable and proactive rather than a prose
inventory that can drift from the product.

```text
public CLI/config/export/source change
  → v1.1 manifest entry with exact implementation location
  → category routes to a named end-to-end procedure
  → manifest contract enumerates the public surface
  → deterministic workflow runs tiers 1–2 and reports live tiers explicitly
  → hourly guardian reads the scoped Factory clone
  → exact CAS cycle state selects one unchecked feature
  → idempotent Slack write receives a provider timestamp
  → checkpoint records that exact receipt; full cycle then resets
```

A manifest read failure, unsafe shrink, state conflict, malformed/oversized
state, missing exact credentials, Slack failure, or receiptless response must
fail closed without advancing the feature cycle.

**What breaks if this fails:** shipped behavior is omitted from verification, or the guardian skips/duplicates questions while claiming coverage.

---

## Hot Paths (Sensitive, Frequently Touched)

| Path | Risk | Code Area |
| --- | --- | --- |
| Scope title/label/team checks | Out-of-scope issues mutate provider state | `src/safety/`, `src/orchestrator/factory.ts`, `src/writeback/linear.ts` |
| Sparse Linear canonical fallback | Real ready issues appear non-dispatchable | `src/orchestrator/factory.ts`, `src/linear/state-resolver.ts` |
| Label-derived dispatch identity | Wrong repo or duplicate implementers | `src/triage/heuristic.ts`, `src/orchestrator/factory.ts` |
| GitHub numeric/path resolution | Issue is missed, cross-repo ambiguity is hidden, or wrong source is selected | `src/cli/fleet.ts` |
| Repo-qualified identity + composite issue key | Equal-number issues share agents, PR caches, or lifecycle state | `src/triage/agent-names.ts`, `src/orchestrator/factory.ts` |
| Clone expansion/inference/preflight | Local work spawns in a missing or wrong checkout | `src/config/schema.ts`, `src/config/local-clone-paths.ts`, `src/cli/fleet.ts` |
| Live high-water + replay suppression | Missed or duplicate dispatch | `src/orchestrator/factory.ts`, `src/subscriptions/event-client.ts` |
| Critical message confirmation | Agents start without receiving tasks | `src/orchestrator/factory.ts`, `src/fleet/internal-fleet-client.ts` |
| Durable relay lifecycle + owner epoch | Duplicate remote team/PR, leaked capacity, or stalled release | `src/orchestrator/factory.ts`, `src/ports/state.ts`, `src/state/file-state-store.ts` |
| Remote head publication + receipt recovery | In-flight issue stalls or publishes a stale/local branch | `src/orchestrator/factory.ts`, `src/mount/relayfile-github-connection-write.ts` |
| Canonical babysitter routing + durable wake | Wrong PR wakes, event loss, or duplicate branch mutation | `src/orchestrator/factory.ts`, `src/ports/state.ts`, `src/state/file-state-store.ts` |
| Babysitter critical ACK fence | PTY input interrupts destructive work before safety state is durable | `src/orchestrator/factory.ts`, `src/dispatch/templates.ts` |
| Clarification release/park/wake | Capacity leaks, duplicate workers, or human replies disappear | `src/orchestrator/factory.ts`, `src/ports/state.ts`, `src/state/file-state-store.ts` |
| Bounded log normalization | Errors disappear or hostile metadata crashes/redacts incorrectly | `src/logging.ts` |
| Merge gate live fields/head guard | Unsafe or stale-head merge | `src/github/merge-gate.ts`, `src/orchestrator/factory.ts` |
| GitHub post-merge advancement | Issue stays Human Review after merge | `src/orchestrator/factory.ts`, `src/writeback/github.ts` |
| Slack/GitHub answer authorization | Wrong human steers active agents | `src/orchestrator/factory.ts` |
| File state + re-adoption | Restart loses live agent ownership | `src/state/`, `src/fleet/relay-fleet-client.ts` |
| Relay repo placement on spawn/restart | Hosted work lands on a node without the source checkout | `src/ports/fleet.ts`, `src/fleet/relay-fleet-client.ts`, `src/orchestrator/factory.ts` |
| Reaper PID identity/protection | Healthy broker, node, or unrelated process is killed | `src/orchestrator/reaper.ts`, `src/orchestrator/process-identity.ts` |
| Node checkout containment | Hosted input executes outside advertised repos | `src/node/factory-node.ts` |
| Relayfile guarded write + confirmation | Writes escape scope or are reported before provider ack | `src/mount/`, `src/writeback/` |
| Hosted invocation/lease/reconciliation | Duplicate run, stale-owner mutation, or lost completion | `src/hosted/` |
| Cloud event outbox/reporter | Lost progress, sensitive payload, or orchestration blockage | `src/observability/`, `src/ports/observability.ts` |
| Manifest/procedure/guardian contract | Public behavior is unverified or proactive progress lies | `.agentworkforce/features/`, `.agentworkforce/agents/factory-feature-guardian/`, `workflows/verify-features.ts` |

---

## First Health Check

Run these in order when the system's state is unclear:

```bash
factory --help
npm run build
npm run featuremap:check
npm test
npm run verify:e2e
# Prepare /tmp/factory-tier2-config.json as documented in verify/procedures.md.
factory run-once --config /tmp/factory-tier2-config.json --dry-run
factory status --config /tmp/factory-tier2-config.json
```

Then, with provider and fleet prerequisites available:

```bash
factory canary <known-ready-key> --config ./factory.config.json
factory loop-status --config ./factory.config.json
factory fleet roster --backend internal
```

The first failing step narrows the problem to package/CLI health, config and deterministic orchestration, provider sync fidelity, daemon liveness, or fleet availability.
