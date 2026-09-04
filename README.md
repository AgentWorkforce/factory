# Agent Workforce Factory

**Turn issues into reviewed pull requests, automatically.**

Point the factory at your ticketing system (Linear, GitHub, Shortcut, Asana etc) and it does the loop a human
otherwise babysits: it discovers the issues that are ready, decides how to tackle
each one, spawns coding agents to implement and review the change, opens a PR,
drives it through a merge gate, and closes the issue — all inside a safety scope
you define, so it only ever acts on work you've explicitly opted in.

## Why use it

- **Clear the small-but-real backlog.** The well-scoped fixes and chores that pile
  up get done without a person shepherding each one.
- **It only touches what you allow.** A safety gate (title prefix + team) means
  the factory dispatches *exactly* the issues you mark for it and ignores
  everything else — opt-in by construction.
- **Real PRs, not blind merges.** Every change goes through an implement → review
  → merge-gate flow and lands as a normal PR. It defaults to *never* auto-merging
  until you turn that on.
- **You stay in the loop.** It posts threaded status to Slack and can ask a human
  for clarification mid-task when an issue is ambiguous.
- **Drop in by label.** Hand it new work just by labeling a Linear or GitHub
  issue — no new tooling in your day-to-day.

A good fit when you have a steady stream of scoped issues and want them turned
into PRs without standing up your own agent orchestration.

## How it works

```
discover ready issues → triage (how to do it) → dispatch agents (implement + review)
        → open PR → merge gate → close issue
```

Each step is gated by your config and the safety scope. Issues outside the scope
are pulled but never dispatched.

## Install

```bash
npm install @agent-relay/factory
```

The factory talks to a relay broker through the **`agent-relay`** sidecar; install
and sign in to that separately (it's a peer of this package). Once installed, the
CLI is available as `factory`:

```bash
factory run-once --config ./factory.config.json --dry-run
```

From a source checkout instead of an npm install, run
`npm ci && npm run build` first, then `node bin/factory.mjs <action> …`.

## Quick start

From the repository checkout you want Factory to work in, the quickest setup is:

```bash
factory init
```

It derives the repository from `origin`, checks for `agent-relay` and
`relayfile`, finds the active Relay workspace, starts the local mount, verifies
GitHub access, and creates `factory.config.json` for GitHub-native issues. If
something is missing, it explains what to do and writes no partial config. Use
`factory init owner/repo` when the checkout has no GitHub `origin`, or add
`--workspace <id>` to choose a workspace explicitly.

After init, add the `factory` label to an open issue and run a dry run below.

1. **Connect GitHub to your relay workspace** with push access for the target
   repositories. Factory uses that workspace connection to publish branches and
   open pull requests by default. A local `gh` installation and `gh auth login`
   are required when `github.identity` is `"user"`, or when the default
   `"auto"` mode uses the compatibility GitHub issue lifecycle path. Exact
   `"app"` mode needs no local GitHub credential. If a required
   connection is missing, an interactive Factory command offers to open the
   Relayfile connection flow and waits for it to finish. Linear-backed operations
   require both Linear and GitHub; GitHub-native operations require GitHub.

2. **Write a minimal config** (`factory.config.json`). Only `workspaceId` and a
   repo route are required:

   ```json
   {
     "workspaceId": "your-workspace-id",
     "repos": {
       "byLabel": { "pear": "AgentWorkforce/pear" },
       "clonePaths": { "AgentWorkforce/pear": "/path/to/your/pear/checkout" },
       "default": "AgentWorkforce/pear"
     }
   }
   ```

   `workspaceId` is your relay workspace; `repos.byLabel` maps an issue label to a
   repo; `clonePaths` tells the agent where that repo lives locally so it has
   somewhere to make changes.

   For a GitHub-only workspace, add `"issueSource": "github"` (or omit it and
   Factory will select GitHub when Relayfile authoritatively reports that Linear
   is not connected). Connection errors and connected-but-still-syncing states
   stop the command instead of silently selecting the wrong source. An open
   issue carrying the configured `safety.requireLabel`
   label—`factory` by default—is then dispatched directly, with lifecycle
   updates written back as GitHub comments and `factory:in-progress` /
   `factory:human-review` labels. No Linear mirror is created.

3. **Plan a cycle without touching anything** — `--dry-run` discovers and triages
   but writes nothing and spawns no agents:

   ```bash
   factory run-once --config ./factory.config.json --dry-run
   ```

4. **Let it work for real:**

   ```bash
   # One discovery→dispatch cycle, then exit.
   factory run-once --config ./factory.config.json

   # Or run continuously as a daemon (the production form).
   factory start --mode live --config ./factory.config.json
   ```

> **Pulled some issues but dispatched none?** That's the safety gate doing its
> job — the issues are real but outside your scope. See
> [Tell it what to work on](#tell-it-what-to-work-on).

## Commands

| Command | What it does |
|---|---|
| `factory init [owner/repo]` | Verify local Relay prerequisites and GitHub access, then configure the current checkout for GitHub-native issue dispatch. |
| `factory run-once` | One discover→triage→dispatch cycle, then exit. Honors `--dry-run`. |
| `factory loop` | A bounded multi-iteration loop, then exit. |
| `factory start --mode live` | Long-lived daemon — the production entrypoint. Runs until you stop it. |
| `factory status` | Print current factory status as JSON, including held agents, lifecycle deadlines, and periodic readiness-reconcile health. |
| `factory triage <KEY\|path>` | Triage one issue and print the decision. |
| `factory dispatch <KEY\|path>` | Triage + dispatch one issue. Honors `--dry-run`. |
| `factory babysit <PR\|PR-URL>` | Spawn a one-shot babysitter for an existing open PR, even when it was not created by Factory. |
| `factory diagnose --deployed <url>` | Ask a **deployed** instance why it is or is not dispatching. Reads the unauthenticated `/healthz` diagnostics block — readiness failure count, error class, in-flight sweep age — with no credential; `--token` (or `FACTORY_EVIDENCE_TOKEN`) also reads the gated `/evidence` message. Exits non-zero when the instance is not dispatching. See [deployed diagnostics](docs/deployed-diagnostics.md). |
| `factory canary <KEY\|path>` | Assert a known "Ready for Agent" issue is dispatch-ready by the real dry-run triage path. Prints `{ok,issue,status,reason}`; exits non-zero (with the skip reason) if it isn't. |
| `factory reap-orphans [--include-held]` | Report stale factory processes and held agents. By default held agents are visible but retained; `--include-held` releases held agents whose configured deadline has elapsed. Kubernetes cleanup is reported as not applicable when no environment provider is configured. |
| `factory featuremap check [--manifest <path>] [--base <ref>]` | Validate the repository feature/test manifest and optionally report advisory drift for unchanged entries whose locations changed. |
| `factory intake notion <manifest>` | Normalize ready specs from a read-only Notion mount into GitHub lifecycle issues or exact-path fleet work. Honors `--dry-run`. |

`factory status` includes `inFlightDispatches`, grouped by issue with agent
names and the provider-claim state (`pending`, `verified`, or `degraded`). This
view is read from Factory's local in-flight registry, so it remains available
when GitHub lifecycle writeback is the degraded subsystem.

It also reports `fleetControlPlane`. Factory bounds its read-only roster probe,
pauses a live dispatch immediately when that probe fails, and opens a circuit
after repeated failures. While the circuit is open, new spawn and resume calls
fail fast. After `fleetHealth.resetTimeoutMs`, one half-open roster probe may
close the circuit. Configure `fleetHealth.rosterTimeoutMs`,
`fleetHealth.failureThreshold`, and `fleetHealth.resetTimeoutMs` when the fleet
has intentionally higher control-plane latency. Mutating calls are never
abandoned behind a local timeout because an accepted-but-late spawn would be
ambiguous.

A paused `run-once` or `loop` exits non-zero and logs `dispatch paused by the
circuit`; `factory status` reads the daemon heartbeat and reports the circuit's
`state`, `lastError`, and `retryAtMs`. If the broker is healthy but slower than
the configured bound, raise `fleetHealth.rosterTimeoutMs` (maximum 60 seconds)
to match measured control-plane latency. If the control path is unavailable,
repair the isolated broker and wait until `retryAtMs`; the next successful
half-open roster probe closes the circuit automatically. Do not treat an open
circuit as an empty queue or successful no-work iteration.

For production, set `fleetHealth.requireDedicatedBroker` to `true` and point
`AGENT_RELAY_STATE_DIR` at a state directory that is distinct from the
project's `.agentworkforce/relay`. Factory then refuses startup if it would
silently reuse the interactive project broker.

Factory now defaults `batchSize` to `1`. Operators may explicitly raise it to
at most `5` after isolating Factory workers from an interactive broker and
measuring control-plane latency under the intended recipe mix.

For a live daemon, `factory status` also includes `readinessReconcile`. Its
state advances from `retrying` to `degraded` after three consecutive periodic
discovery failures and returns to `healthy` after a successful checkpoint.
The same record includes the last error, attempt duration, and failure count.

Those counters only move when a sweep *settles*, so a sweep that hangs would
leave every field reading `healthy` forever. `readinessReconcile` therefore
also carries `intervalMs` and `inFlightMs`, and reports `stalled` when a pass
has been in flight for more than ten sweep intervals. The redacted subset of
this record is published on the deployed instance's `/healthz` and is what
`factory diagnose --deployed` reads — see
[deployed diagnostics](docs/deployed-diagnostics.md).

Dispatch lifecycle writes are claim-critical. Factory applies the
`factory:in-progress` label/state before the dispatch comment, confirms the
GitHub label by provider read-back, and retries either write three times. An
exhausted write is logged at error level as dead-lettered, recorded as a
degraded claim in the registry, and fails the dispatch instead of reporting a
clean dispatch with missing GitHub state. Durable lifecycle recovery repeats
the same label-and-comment claim without respawning acknowledged agents.

Global options work anywhere in the args: `--config <path>`, `--dry-run`,
`--backend <internal|relay>`, and `--agent-exit-timeout <ms>`. The internal
backend reuses a relay broker that's already running for your workspace, and
starts one if none is. For self-started brokers, the agent-exit timeout defaults
to 30 minutes and can also be set with `FACTORY_AGENT_EXIT_TIMEOUT_MS`. Set
`FACTORY_LOG_LEVEL=debug` to include per-checkout details for summarized local
mount refreshes.

Integration connection prompts only run for commands that need provider data or
GitHub write access. Maintenance commands such as `status`, `loop-status`,
`kill-loop`, and `reap-orphans` do not preflight connections. `--dry-run`, the
intrinsically read-only canary, and non-interactive invocations never open an
OAuth flow; when a required integration is missing they exit with an actionable
instruction to rerun the Factory command in an interactive terminal instead.

(There are a few more operational commands — `loop-status`, `kill-loop`,
`reap-orphans`, `close-probe` — for running the daemon in production.)

### Mounted Notion specs

Notion is an intake source, not a lifecycle issue source or writeback surface.
Factory reads a Relayfile-mounted page, binds it to a stable
`notion:<page-id>` identity and digest, then either creates a labeled GitHub
issue for a repository target or dispatches an exact-path workspace task
through the fleet. The config field `issueSource` continues to select the
discovery and lifecycle-writeback adapter (`linear` or `github`); Notion intake
normalizes into that lifecycle instead of becoming a third adapter.

The absence of Notion writeback is deliberate. Relayfile supplies the mounted
page as a read-only execution contract, and Factory has no guarded Notion
property/comment writeback adapter or operator-defined lifecycle mapping.
Repository targets reconcile accepted, dispatched, PR, blocked, and completed
state on their generated GitHub issue. Native Notion lifecycle reconciliation
requires a separate provider capability and product contract.

New pages fail closed unless the first block is an explicit ready spec:

```markdown
# Chief Spec
Status: ready
Title: Reconcile the mail integration
Summary: Resume the reviewed recovery procedure and preserve every gate.
Recipe: team
Repos: AgentWorkforce/cloud

The full private execution contract starts here.
```

Supported destinations are `Repos: owner/name, ...` and
`Project-Paths: /absolute/path, ...`; `Node:` can pin exact-path work to a fleet
node. `Recipe:` is `single`, `workflow`, or `team`. For a public repository the
header must also contain a reviewed `Public-Summary:`. Factory never copies the
mounted body into a public issue.

The intake manifest identifies pages and the local mount root. Headerless legacy
pages can be admitted only with a bootstrap entry containing the exact
`authorizedPageId`, destination, safe summary, and operator reason. That escape
hatch is deliberately page-specific; there is no title or content heuristic.

Factory can generate that manifest directly from the **Factory Tasks** Notion
data source. The generator uses `NOTION_API_KEY` for read-only data-source and
page-markdown requests, selects only rows whose `Status` is `Ready for Agent`,
and never updates a row or its status. `Labels` and `Route` values are combined
for repository issue labels. A repository row can supply a separately reviewed
`Public Summary`; Factory uses that text for a public lifecycle issue and never
copies the private page body. The output order is stable by page ID, so the same
database state produces the same manifest on every run.

```bash
factory intake notion generate \
  --mount-root ../.integrations/notion \
  --worker-mount-root .integrations/notion \
  --worker-mount-transport relay-channel \
  --state-path ../.factory/notion-intake-state.json \
  > ./ops/notion-intake.json
```

`--data-source` can override the built-in Factory Tasks data source ID. Paths in
the generated manifest retain the existing intake semantics: `mountRoot` and
`statePath` are resolved relative to the saved manifest, while
`workerMountRoot` is the path workers receive. Generation fails closed if there
are no ready rows, a required property is missing, a row sets both (or neither)
of `Repo` and `Project Path`, or Notion returns a truncated page body.

```json
{
  "version": 1,
  "mountRoot": ".integrations/notion",
  "workerMountRoot": ".integrations/notion",
  "workerMountTransport": { "kind": "relay-channel" },
  "statePath": ".factory/notion-intake-state.json",
  "tasks": [
    { "page": "https://app.notion.com/p/Reconcile-3b36800c1c90801db1cfc8f2e1cff7cf" }
  ]
}
```

`mountRoot` and `statePath` resolve relative to the manifest file. `statePath`
is a local receipt cache, not the dispatch authority: before creating an issue
or spawning an agent, Factory creates one immutable, digest-bound claim in the
active Agent Relay workspace. Workspace-global claim-channel uniqueness stops
two machines with independent caches from dispatching the same source key. A
failed or ambiguous claim write blocks dispatch. All non-dry-run Notion intake
therefore requires a resolvable active Agent Relay workspace key.

`workerMountRoot` is the repo-relative read-only mount workers receive. With the recommended
`relay-channel` transport, Factory base64-chunks the digest-bound mounted bytes
into a workspace-private Agent Relay channel. A worker on any fleet machine can
reconstruct the exact file at `workerMountRoot`, set it to mode `0444`, and
apply the source SHA-256 gate without exposing the page in a public issue.
`workerMountTransport` defaults to `{ "kind": "local" }` whenever it is
omitted, including in new manifests. Portable delivery must be selected
explicitly; otherwise the worker uses the local mount path. Missing portable
delivery or claim capability fails closed.

If dispatch stops after creating the durable claim but before recording its
receipt, re-running stays blocked rather than guessing whether the downstream
side effect happened. The blocked result includes the source key. A workspace
administrator can derive the claim channel as
`factory-notion-claim-<sha256(sourceKey)>`. After verifying that no matching
lifecycle issue or workspace agent exists, the administrator may delete that
channel in Agent Relay and re-run intake. Never clear a claim merely because
its local receipt is missing: the shared claim, issue marker, and running agent
must be reconciled first. A blocked portable-mount migration uses the same
channel formula with `<sourceKey>:portable-mount` as the hashed value; verify
the worker was not redispatched before clearing it.
`page` accepts a Notion URL or a bare page ID.

Plan without writes, then dispatch:

```bash
factory intake notion ./ops/notion-intake.json --dry-run
factory intake notion ./ops/notion-intake.json --backend relay
```

Repository targets require the `factory-ready` and matching
`agent:<recipe>` labels to already exist. Factory automatically prefixes their
issue titles with `[factory]` so the hosted brain's independent safety gate can
accept them. Re-running reconciles the workspace-global claim with the GitHub
source marker or the running exact-path agent; the local digest-bound receipt is
only a cache. A changed mounted spec blocks instead of silently mutating
already-dispatched work.

### Feature-map validation

Repositories with `.agentworkforce/features/manifest.yaml` can run
`factory featuremap check` in CI. The command rejects malformed or duplicate
entries, invalid verification tiers, catalog-summary drift, locations that do
not exist, and incomplete v1.1 category-to-procedure routing. The same checker
is published as `@agent-relay/factory/featuremap` for programmatic use.

During review, pass the PR base ref with `--base <ref>`. A changed file named by
an existing manifest entry produces an advisory when that entry's description,
verification tier, and locations are unchanged. The reviewer must re-confirm
that metadata; the advisory does not itself fail the command because a covered
file can change without changing the feature contract.

Factory's agent-facing runbook is `.claude/skills/verify-features.md`. It tells
an agent how to resolve a manifest category to its named end-to-end procedure,
run the applicable tier with safe fixtures and cleanup, assert provider/fleet
state, and report unexercised live tiers explicitly. The deterministic full gate
is `npm run build && npm run featuremap:check && npm test && npm run verify:e2e`;
`workflows/verify-features.ts` adds opt-in provider, fleet, and cloud checks.

The checked-in `factory-feature-guardian` proactive persona mirrors Relay's
guardian operating model. Hourly, it reads the catalog from a scoped Factory
repository mount, selects one unchecked feature from exact revisioned cycle
state, posts an idempotent Slack question, requires a provider timestamp, and
only then checkpoints progress. Manifest/state/delivery ambiguity fails closed
instead of silently skipping a feature. Configure its Slack channel and deploy
the persona through the normal Agent Workforce proactive-agent path.

The checked-in `factory-maintainability` reviewer applies the same catalog as a
historian on every non-draft PR revision. It reads the exact PR diff and full git
history, follows the repository-owned maintainability charter, and posts one
bounded advisory review without editing the checkout or merging. Its compiled
persona is scoped to `AgentWorkforce/factory`; deploy it through the normal
Agent Workforce cloud-persona path.

### Cloud progress and trace correlation

Authenticated Cloud progress reporting is enabled by default. Factory persists
its bounded lifecycle events to a local outbox before delivery; set
`reporting.enabled` to `false` only when the Cloud dashboard is intentionally
not part of the deployment.

Every run-scoped event carries the same deterministic, W3C-valid `traceId`,
derived from the durable opaque run ID. This is a correlation identifier only:
Factory does **not** currently create or export OpenTelemetry spans, and it does
not invent span IDs. No task text, prompt, message, path, command, source code,
or exception stack is used to derive the identifier or admitted by the event
contract.

The exporter follow-up should add optional OpenTelemetry SDK initialization,
short spans around dispatch, spawn, writeback, publish, and release operations,
and W3C context propagation across Cloud requests and remote fleet delivery.
Persist or rehydrate the run context so replacement Factory processes keep the
same trace; do not hold a single span open for the lifetime of a long-running
run. Export with a batch processor through the standard OTLP environment
contract (`OTEL_EXPORTER_OTLP_ENDPOINT`, signal-specific overrides, and headers)
to an OpenTelemetry Collector. Keep the existing bounded attribute allowlist
and make exporter failure non-fatal. See the official
[OpenTelemetry JavaScript exporter guidance](https://opentelemetry.io/docs/languages/js/exporters/)
and [W3C Trace Context specification](https://www.w3.org/TR/trace-context/).

`factory babysit 10` uses `repos.default`; a full URL such as
`factory babysit https://github.com/org/repo/pull/10` supplies the repository
directly. The explicit command is opt-in on its own and does not require
`babysitter.enabled`. It rejects closed, merged, and draft PRs, uses a linked
issue spec when one can be resolved (otherwise the PR title/body), fixes the
existing PR branch, and always leaves the final review and merge to a human.
The command prints a spawn receipt and returns; the PR-keyed task-exit worker
continues on the relay broker and reports completion or access blockers there.

### Routed-PR discovery (activation disabled)

This release adds the declarative configuration and read-only discovery surface
for widening babysitter intake beyond Factory-created PRs. To configure that
discovery surface, set:

```json
{
  "babysitter": {
    "enabled": true,
    "mode": "routed-open-prs",
    "excludeLabels": ["factory:skip-babysitter"],
    "excludePullRequests": [],
    "notifyHumans": false
  }
}
```

Discovery scans only repositories named by `repos.names`; other routing
fallbacks do not silently expand it. It classifies open, non-draft,
same-repository PRs, applies `excludeLabels` and `excludePullRequests`,
deduplicates mount aliases, and reports incomplete or unreadable metadata.

**Routed-PR activation is deliberately disabled.** The named
`ROUTED_PR_BABYSITTER_ACTIVATION_ENABLED` guard is `false`, and configuration
cannot turn it on. No routed candidate is claimed, spawned, renewed, released,
interpreted as complete, advanced to Human Review, or used to notify anyone.
Activation will be implemented separately after the durable lifecycle and
completion-CAS design is reviewed. The existing issue-created babysitter path
is unchanged.

### Scheduled sync-fidelity canary

`factory canary` is the regression detector for upstream sync drift: if a synced
issue stops carrying enough state to be dispatchable (e.g. the Linear sync
regresses to records without `state.id`), a known-good issue flips from
dispatch-ready to skipped. Run it on a schedule against a standing "Ready for
Agent" canary issue and alert on failure.

`scripts/factory-canary.sh` wraps the command for cron/launchd: it runs from your
deployment dir (reusing the running relay broker), bounds a hung run, and posts a
Slack alert via `FACTORY_CANARY_SLACK_WEBHOOK` on failure. See
`scripts/com.agentrelay.factory-canary.plist.example` for an every-6h launchd
template.

### Slack questions

Set `slack.channel` to the Slack channel name, channel ID, or mounted channel
directory. For example, `factory`, `C1234567890`, and
`C1234567890__factory` are all accepted when the channel is present under the
relayfile Slack mount.

## Tell it what to work on

How an issue enters the factory depends on `issueSource`:

| Source | What you do | Result |
|---|---|---|
| **Linear** (`issueSource: "linear"`) | Title it `[factory] <task>`, set the configured team + a repo label, move it to **Ready for Agent** | dispatched directly from Linear |
| **GitHub native** (`issueSource: "github"`) | Add the configured readiness label (`factory` by default) and a repo route label | dispatched directly from GitHub; lifecycle comments and labels stay on the GitHub issue |
| **GitHub mirror** (`issueSource: "linear"`) | Add the **`factory`** label to the GitHub issue | mirrored into a `[factory]` Linear issue, then dispatched through the Linear flow |

The **safety gate** keeps both flows opt-in. Linear dispatch uses the configured
title prefix and team; GitHub-native dispatch uses `safety.requireLabel` and an
open issue. Set `safety.requireTitlePrefix` to `null` to disable the title path
explicitly when a deployment uses its required label as the only issue opt-in;
omitting the key retains the `[factory-e2e]` default, and an empty string is
rejected. Everything else is ignored. Loosen these checks deliberately —
they're the main guardrail.

To sequence issues, add one exact standalone line to the issue body or Linear
description:

```text
Blocked by: #123, owner/other-repo#456
```

A bare number refers to the issue's routed repository. Factory parks the issue
and posts a comment naming every open blocker; capacity queuing remains a
separate hold reason. Closed issues and merged pull requests satisfy blockers,
and the next discovery cycle promotes newly unblocked work. Dependency cycles
fail closed and are reported instead of waiting indefinitely. Other prose and
`Related:` lines are not interpreted as dependencies.

> Tip: `[factory-e2e]` is reserved for the factory's own self-test soak (its PRs
> auto-close). For real work you want to keep, use the `[factory]` prefix.

## Run it as a fleet node

The package ships a fleet **node definition** so a machine can advertise
`spawn:claude` / `spawn:codex` / `workflow:run` to the engine and run agents in the
checkouts it owns. Bringing a worker machine online is two steps:

```bash
# once per machine: redeem an enrollment token for durable node credentials
agent-relay cloud enroll --token ocl_node_enr_…

# each boot: start the node with the factory definition
agent-relay node up --config agent-relay.ts
```

`agent-relay.ts` is a re-export in the node's working directory (`node up`
auto-discovers it there, making `--config` optional). It must **default-export**
the definition:

```ts
export { default } from '@agent-relay/factory/node'
```

The definition reads its node config from `./factory.node.json` (or
`$FACTORY_NODE_CONFIG`): `workspaceId`, `capabilities`, and the
`clonePaths`/`cloneRoot` map naming the checkouts this node services. Each
mapped repo is advertised as a `repo:<label>` tag so placement can route
repo-scoped spawns to it. Spawns for unadvertised paths are refused on the node.

The default `@agent-relay/factory/node` export is a generic worker-machine
definition; it does not host a persona and therefore does not publish an agent
card. An application that intentionally couples a persona to a Factory node
uses the exported `createFactoryNodeDefinition({ persona })` and
`startFactoryNode({ cardPublisher })` runtime. `startFactoryNode` owns the
online-registration edge and publishes the canonical card there; cloud personas
such as the checked-in `factory-feature-guardian` continue to use the Agent
Workforce cloud-persona deployment path described above.

### Tailnet live previews

Factory can attach an issue-lifetime [Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve)
route to a repository's local development port. Configure the same service on
the control plane and execution node (a combined local config needs it only
once):

```json
{
  "preview": {
    "provider": "tailscale-serve",
    "access": "tailnet",
    "services": {
      "AgentWorkforce/pear": {
        "port": 3000,
        "portSpan": 25,
        "startCommand": "npm ci && exec npm run dev"
      }
    }
  }
}
```

The node then advertises `preview:tailscale-serve`; Factory places the preview
first and pins the issue's agents to that node. The URL is included in agent
tasks, the Slack dispatch root, and the pull-request description. Factory uses
Serve—not Funnel—so the URL remains inside the configured tailnet and normal
tailnet grants/ACLs apply. Factory checks the live Serve status and refuses to
surface a route marked as Funnel.

`port` is the preferred node-local app port. Factory reserves the first free
port in `port..port + portSpan - 1` (the span defaults to 100), places that
allocated port in every agent task, and reserves a separate HTTPS port from
`preview.httpsPortRange`. Keep that HTTPS range dedicated to Factory previews.
Preview provisioning happens immediately after Factory creates the isolated
issue worktree, before an agent has had a chance to install ignored dependencies
such as `node_modules`. `startCommand` must therefore include any bounded,
non-interactive bootstrap the fresh checkout needs, followed by a foreground
development command that honors `PORT` (for example,
`npm ci && exec npm run dev`).
The node starts it in the issue checkout, waits for the allocated local HTTP
port to respond, and verifies on Linux or macOS that the listener belongs to the
supervised process tree before returning the URL. It persists an exact process
identity so the command survives agent handoffs and can be safely recovered or
stopped. The command must not daemonize or bind a different port. Active sweeps
repeat listener ownership verification and disable the exact Factory route if
the port is taken over by an unrelated process.
For safety, preview commands receive only `PORT` plus basic shell, locale, home,
and temporary-directory variables; they do not inherit arbitrary Factory,
Relay, or provider credentials from the node process. Load intentional
application settings through a checkout-local environment mechanism. Do not put
secrets directly in `startCommand`, because lifecycle recovery persists the
command as metadata.

Terminal lifecycle completion is withheld until routes and their supervised
commands have been removed at Human Review or Done. If the source-state
writeback wins a crash race, startup recovery observes that terminal source
state and finishes preview teardown before terminalizing the durable lifecycle.
A startup and periodic sweep reaps only orphaned resources in the current
Factory workspace whose persisted route and process identities still match.
See the [provider evaluation](planning/preview-provider-evaluation.md)
for the decision and lifecycle contract.

To exercise the real provider lifecycle on a signed-in node, build first and
run `TAILSCALE_BIN=/path/to/tailscale node scripts/verify-tailscale-preview-e2e.mjs`.
The check drives the node's advertised `preview:tailscale-serve` action to start
a detached HTTP service, reaches it through Serve, recovers it through a fresh
node-action instance, tears it down, then proves a startup orphan sweep reaps a
second abandoned route and process while preserving unrelated Serve
configuration. Override its dedicated ports with
`FACTORY_PREVIEW_E2E_HTTPS_PORT` and `FACTORY_PREVIEW_E2E_TARGET_PORT`.

### Dispatching to nodes (`--backend relay`)

With `--backend relay`, the factory orchestrator dispatches work through the
hosted engine instead of a local broker: placement picks a live node with the
required capability (a named `node` target passes through), the node runs the
agent in its mapped checkout, and the orchestrator detects exits by reconciling
its tracked agents against the engine roster.

Relay dispatch is lifecycle-owned, not fire-and-forget. A one-shot `factory
dispatch --backend relay` keeps a small publisher runtime alive until the
remote branch has produced a PR, terminal issue writeback is acknowledged, and
remote agents are released. The lifecycle (including a per-run branch,
placement results, PR receipt, and a fenced owner lease) is persisted beside
the configured loop registry so `factory start` or a replacement dispatch
process on the same control-plane host can take over after a crash. Execution
nodes never need access to that state file, and remote PIDs are never signalled
as local processes.

The supported topology for the **CLI control plane** is one Factory host per
workspace, with any number of relay execution nodes. Multiple Factory processes
that open the same state file do not mutate it concurrently: `FileStateStore`
holds a cross-process filesystem lock around the complete read/modify/write,
so a second process waits, reloads after it acquires the lock, and then publishes
through fsync plus atomic rename. The in-process operation queue alone is not
the cross-process fence.
Active/active CLI control planes on different hosts remain intentionally
unsupported: separate local state files cannot provide a truthful cross-host
fence.

### Hosting the control plane in Cloud

Embedded CLI hosts can inject durable coordination storage through the
host-neutral [`WatchStateDocumentStore` port](docs/document-state-store.md)
via `stateStoreFactory`.

`@agent-relay/factory/hosted` is the worker-safe control-plane entrypoint. It
contains no Node filesystem/process dependency and runs the complete sweep:

```text
reconcile invocation completions → discover ready issues → triage → dispatch
                              → merge gate → idempotent provider writeback
```

The Cloud host supplies integration ports for discovery, fleet spawn/status,
merge-gate evaluation, and Linear/Slack writeback. `runOnce()` polls every
persisted invocation before discovery, so a dropped completion webhook is
recovered by the next scheduled sweep. The same lifecycle can accept pushed
completion events through `ingestCompletion()`.

```ts
import {
  createHostedFactory,
  DurableObjectHostedFactoryStateStore,
} from '@agent-relay/factory/hosted'

const state = new DurableObjectHostedFactoryStateStore(durableObjectState.storage)
const factory = createHostedFactory(
  { workspaceId, ownerId: isolateId, config },
  { state, discovery, fleet, completions, mergeGate, writeback, reporter },
)

await factory.runOnce() // invoke from cron/alarm and safe webhook wakeups
```

The optional `reporter` implements the canonical `FactoryEventReporter`.
Hosted runs persist a deterministic run ID per workspace and issue, then emit
lifecycle events only after the corresponding fenced state write. Worker hosts
that only need the Factory-owned wire schema, event creator, and reporter types
should import `@agent-relay/factory/telemetry`; unlike the broader
`@agent-relay/factory/observability` surface, it does not export the
filesystem-backed outbox or instance identity helpers.

The Durable Object adapter stores each workspace independently and performs
lease claims plus lifecycle writes in storage transactions. Every mutation is
checked against the current owner and monotonically increasing lease epoch; an
expired host cannot write after takeover. Spawn invocation IDs are deterministic
and provider writebacks carry stable idempotency keys, making recovery safe when
an external operation succeeds just before the worker loses its lease.

Tokens involved — set only the first one on the orchestrator host:

| Token | Prefix | Who holds it |
|---|---|---|
| Workspace key | `rk_live_` | the orchestrator (`RELAY_WORKSPACE_KEY`); used to mint the factory's own agent identity on first use |
| Agent token | `at_live_` | optional `RELAY_AGENT_TOKEN` to pin the orchestrator's agent identity; spawned agents get their own automatically |
| Node token | `nt_live_` | each worker node, minted by `cloud enroll` |
| Observer token | `ot_live_` | read-only dashboards/streams only — never dispatch |

## Configuration

Factory resolves exactly one contract: the path passed via `--config`, or
`./factory.config.json` in the command's current working directory when the
flag is omitted. It does not search target repositories, walk to a clone root,
or merge multiple configs; a config in another repository is inert unless it
is selected explicitly.

Beyond the two required fields above, useful
knobs include issue **routing** (`repos.byLabel` / `byProject` / `keywordRules` /
`default`), the **safety gate** (`safety.requireTitlePrefix`, `safety.requireTeamKey`),
`mergePolicy` (defaults to `never`), per-role **model** overrides, and an optional
**Slack** channel for status threads.

Every successfully placed agent team has a wall-clock release backstop. Configure
the window with `dispatch.agentHoldTimeoutMs`; it defaults to four hours. The
clock starts at the first successful agent placement, not while a dispatch waits
for capacity or while a released team waits for human clarification. If the issue
has not reached its configured `terminalState` before the deadline, Factory
releases the team with reason `held-past-deadline` and records the lifecycle as
abandoned so a restart cannot respawn it:

```jsonc
{
  "dispatch": {
    "agentHoldTimeoutMs": 14400000
  }
}
```

### Recover names created before dispatch identity proofs

Factory stamps every dispatched agent with a stable broker identity derived from
the provider-native issue identity and role. For example, the reviewer for
`AgentWorkforce/factory#244` uses
`factory:dispatch:v1:github:agentworkforce/factory#244:reviewer`. A retry of that
work unit can reclaim its deterministic agent name after a crash, while another
repository's issue 244 cannot.

Agent records created before this behavior have no identity stamp and cannot be
reclaimed automatically. This includes the burned `ar-244-review-factory` seat
and any `ar-244-impl-factory` / `ar-244-babysit-factory` siblings created by the
same pre-fix dispatch. After verifying the named process is no longer live, run
the broker's guarded, one-time legacy recovery for each affected name. The
workspace key and Relaycast base URL use their normal environment variables;
the raw proof is supplied only through the environment and is never placed on
the command line:

```bash
BROKER_BIN="$(node --input-type=module -e \
  "import { getBrokerBinaryPath } from '@agent-relay/harness-driver/broker-path'; const p = getBrokerBinaryPath(); if (!p) process.exit(1); process.stdout.write(p)")"

RELAY_AGENT_IDENTITY_KEY='factory:dispatch:v1:github:agentworkforce/factory#244:reviewer' \
  "$BROKER_BIN" reclaim-legacy-identity ar-244-review-factory
```

Use the matching role suffix (`implementer`, `reviewer`, or `babysitter`) for
each sibling. Recovery refuses a live record, a record that already has an
identity stamp, or a concurrent competing claim; it does not weaken ordinary
collision checks. Once stamped, the next Factory retry presents the same proof
and reclaims normally.

The full schema — every field and default — is validated by Zod at load time, so
an invalid config fails fast with a field-level error. See
[`src/config/schema.ts`](src/config/schema.ts) for the authoritative reference,
and [`test/fixtures/factory.config.json`](test/fixtures/factory.config.json) for a
worked example (including offline fixture mode).

Factory GitHub write attribution is controlled explicitly with `github.identity`:

```jsonc
{
  "github": {
    "identity": "app"
  }
}
```

- `"app"` publishes pull requests and performs GitHub issue lifecycle writes
  through the connected workspace GitHub App. Status transitions provision the
  target Factory label, add only that label, and remove only the prior Factory
  label, so labels applied by people are never replaced from a stale mount
  projection. If any required write capability is unavailable, Factory fails
  loudly and never falls back to a personal account.
- `"user"` publishes pull requests and performs issue lifecycle writes with the
  account authenticated by the local `gh` CLI, even when the app path is
  available.
- `"auto"` is the default. Pull requests and issue lifecycle writes prefer the
  connected App when their complete capability is available. A non-cloud host
  can fall back to its local `gh` user; the gh-less cloud container instead
  fails loudly when the connected lifecycle capability is incomplete.

Each successful publication log includes `identity` (`app` or `user`) and the
confirmed `author`. App writes are not reported complete until the connected
mount acknowledges the provider mutation. Provider-authoritative issue reads
remain optional on the writeback interface; when unavailable, their existing
call sites keep their conservative fallback behavior.

#### Writes that still shell out to `gh`

Two Factory GitHub mutations are not represented on the connected App surface
and therefore cannot be performed as the app today. Under `"app"` they refuse
rather than writing as the operator, so an explicit app identity never produces
a human-attributed write:

| Write | Built-in CLI identity gate | Missing connected capability |
|---|---|---|
| Guarded squash merge (`mergePolicy: "on-green-with-review"`) | `"app"` declines and logs; nothing is merged | `mergePullRequest` |
| Notion intake issue create | only exact `"user"`; `"auto"` and `"app"` block before any durable claim | `createIssue` |
| Notion intake issue edit | only exact `"user"`; `"auto"` and `"app"` block before any CLI access | `updateIssue` |

The merge refusal names `"user"` or `"auto"` as its local-user recovery path.
The built-in Notion publisher is stricter: its local-host opt-in is exact
`github.identity: "user"`, because the production Factory container contains
no `gh` binary and the App surface has no issue-create or source-marker query.

Notion intake is a separate surface from the Factory lifecycle writeback and
still requires local `gh` authentication when enabled. Its CLI entry point
resolves `github.identity` from the selected contract — including a split
`workspaceConfig`/`nodeConfig` contract, where the node half wins. Only exact
`"user"` enables the built-in CLI publisher. An absent contract resolves to
`"auto"` and therefore blocks repository intake explicitly; a contract that
exists but cannot be parsed is an error rather than a silent downgrade.

The gate covers the built-in publisher's visibility, label, reconciliation,
create, and edit operations. `"auto"` and `"app"` fail before spawning `gh` and
before reserving a durable claim. Project-only intake and injected custom
publishers do not use this adapter and remain available.

The standalone babysitter reads complete PR metadata from the authenticated
mounted projection and reports `source: 'mount'`; it does not fall back to
local `gh`. Review replies and pushes on a babysat PR are performed by the
dispatched agent under the agent's own credential, not by the Factory process.

Authenticated Factory progress reporting is enabled by default for real CLI
sessions. Factory sends privacy-bounded lifecycle events, worker ownership,
heartbeats, and sanitized failure categories to the active Cloud workspace; it
never sends task text, prompts, agent output, source code, local paths, command
lines, tokens, or raw stack traces. Delivery uses a private disk outbox and does
not interrupt orchestration when Cloud is unavailable. Serialized event batches
are capped at 240 KiB, below Cloud's 256 KiB ingestion limit. Set
`reporting.enabled` to `false` to disable it, or use `reporting.outboxPath`,
`batchSize`, and `requestTimeoutMs` to tune delivery.

`cloneRoot` and every `clonePaths` value accept `~` or a leading `~/`, expanded
against the current user's home directory. Named-user forms such as `~alice`
are rejected because Node cannot expand them portably.

For a local, single-repository run, the checkout mapping can be omitted:

```json
{
  "repos": {
    "org": "your-org",
    "names": ["your-repo"],
    "default": "your-org/your-repo"
  }
}
```

Run Factory from that repository (or one of its subdirectories). When exactly
one `repos.names` entry is configured and no `cloneRoot` or `clonePaths` field is
supplied, Factory resolves the checkout's git top-level and uses it only if a
GitHub remote matches the resolved `org/name`. The inference is logged. Missing,
unparseable, or mismatched remotes fail with a config-oriented error instead of
silently dispatching in the wrong directory. Explicit local clone paths are also
preflighted before commands that can dispatch through the internal backend;
relay-backend paths are left for their worker nodes to validate.

## Notes

- The daemon is headless by design; tools like Pear can consume this package and
  wrap it, but the published CLI is `factory`.
- The published `dist/` is plain ESM, runnable directly by Node (`node bin/factory.mjs`)
  and importable by ESM consumers.
- For production operation (the live-daemon + reaper backstop model, heartbeats,
  and connected-workspace prerequisites), see the operations notes alongside the config schema.
// Canary probe by factory-canary-verifier-r1 2026-08-31T13:44:25Z
