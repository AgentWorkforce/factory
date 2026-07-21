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

1. **Connect GitHub to your relay workspace** with push access for the target
   repositories. Factory uses that workspace connection to publish branches and
   open pull requests by default. A local `gh` installation and `gh auth login`
   are required only when `github.identity` selects the user path (or for the
   existing GitHub issue lifecycle writeback described below). If a required
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
| `factory run-once` | One discover→triage→dispatch cycle, then exit. Honors `--dry-run`. |
| `factory loop` | A bounded multi-iteration loop, then exit. |
| `factory start --mode live` | Long-lived daemon — the production entrypoint. Runs until you stop it. |
| `factory status` | Print current factory status as JSON. |
| `factory triage <KEY\|path>` | Triage one issue and print the decision. |
| `factory dispatch <KEY\|path>` | Triage + dispatch one issue. Honors `--dry-run`. |
| `factory babysit <PR\|PR-URL>` | Spawn a one-shot babysitter for an existing open PR, even when it was not created by Factory. |
| `factory canary <KEY\|path>` | Assert a known "Ready for Agent" issue is dispatch-ready by the real dry-run triage path. Prints `{ok,issue,status,reason}`; exits non-zero (with the skip reason) if it isn't. |
| `factory featuremap check [--base <ref>]` | Validate the repository feature/test manifest and optionally report advisory drift for unchanged entries whose locations changed. |

Global options work anywhere in the args: `--config <path>`, `--dry-run`,
`--backend <internal|relay>`, and `--agent-exit-timeout <ms>`. The internal
backend reuses a relay broker that's already running for your workspace, and
starts one if none is. For self-started brokers, the agent-exit timeout defaults
to 30 minutes and can also be set with `FACTORY_AGENT_EXIT_TIMEOUT_MS`.

Integration connection prompts only run for commands that need provider data or
GitHub write access. Maintenance commands such as `status`, `loop-status`,
`kill-loop`, and `reap-orphans` do not preflight connections. `--dry-run`, the
intrinsically read-only canary, and non-interactive invocations never open an
OAuth flow; when a required integration is missing they exit with an actionable
instruction to rerun the Factory command in an interactive terminal instead.

(There are a few more operational commands — `loop-status`, `kill-loop`,
`reap-orphans`, `close-probe` — for running the daemon in production.)

### Feature-map validation

Repositories with `.agentworkforce/features/manifest.yaml` can run
`factory featuremap check` in CI. The command rejects malformed or duplicate
entries, invalid verification tiers, catalog-summary drift, and locations that
do not exist. The same checker is published as `@agent-relay/factory/featuremap`
for programmatic use.

During review, pass the PR base ref with `--base <ref>`. A changed file named by
an existing manifest entry produces an advisory when that entry's description,
verification tier, and locations are unchanged. The reviewer must re-confirm
that metadata; the advisory does not itself fail the command because a covered
file can change without changing the feature contract.

An hourly per-repository sweep or Slack confirmation bot is explicitly outside
this feature's scope. Factory's own guardian cycle is useful for tier-5/6 checks
that need live or human confirmation, but duplicating it for every customer repo
would add standing noise and infrastructure without evidence that those entries
rot between PRs. Revisit that only if usage data demonstrates silent tier-5/6
drift that PR checks do not catch.

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
open issue. Everything else is ignored. Loosen these checks deliberately —
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
The check starts a detached HTTP service, reaches it through Serve, recovers it
through a fresh manager instance, tears it down, then proves a startup orphan
sweep reaps a second abandoned route and process while preserving unrelated
Serve configuration. Override its dedicated ports with
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
on that host are fenced through the shared `FileStateStore` lock/lease.
Active/active CLI control planes on different hosts remain intentionally
unsupported: separate local state files cannot provide a truthful cross-host
fence.

### Hosting the control plane in Cloud

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
  { state, discovery, fleet, completions, mergeGate, writeback },
)

await factory.runOnce() // invoke from cron/alarm and safe webhook wakeups
```

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

Pass a JSON file via `--config`. Beyond the two required fields above, useful
knobs include issue **routing** (`repos.byLabel` / `byProject` / `keywordRules` /
`default`), the **safety gate** (`safety.requireTitlePrefix`, `safety.requireTeamKey`),
`mergePolicy` (defaults to `never`), per-role **model** overrides, and an optional
**Slack** channel for status threads.

The full schema — every field and default — is validated by Zod at load time, so
an invalid config fails fast with a field-level error. See
[`src/config/schema.ts`](src/config/schema.ts) for the authoritative reference,
and [`test/fixtures/factory.config.json`](test/fixtures/factory.config.json) for a
worked example (including offline fixture mode).

Factory PR authorship is controlled explicitly with `github.identity`:

```jsonc
{
  "github": {
    "identity": "app"
  }
}
```

- `"app"` always publishes through the connected workspace GitHub App. If that
  write path is unavailable, Factory fails loudly and never falls back to a
  personal account.
- `"user"` always publishes with the account authenticated by the local `gh`
  CLI, even when the app path is available.
- `"auto"` is the default and preserves compatibility: prefer the app path,
  then fall back to the local `gh` user when the app writer is unavailable.

Each successful publication log includes `identity` (`app` or `user`) and the
confirmed `author`. This setting currently controls PR creation only. GitHub
issue comments and lifecycle status labels still use the existing local `gh`
writeback; extending the identity policy to those operations requires a
connected-app issue writeback surface.

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
