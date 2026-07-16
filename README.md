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
   open pull requests; a local `gh` installation or `gh auth login` is not a
   prerequisite.

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
   Factory will select GitHub automatically when `/linear/issues` is not
   connected). An open issue carrying the configured `safety.requireLabel`
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
| `factory canary <KEY\|path>` | Assert a known "Ready for Agent" issue is dispatch-ready by the real dry-run triage path. Prints `{ok,issue,status,reason}`; exits non-zero (with the skip reason) if it isn't. |

Global options work anywhere in the args: `--config <path>`, `--dry-run`,
`--backend <internal|relay>`, and `--agent-exit-timeout <ms>`. The internal
backend reuses a relay broker that's already running for your workspace, and
starts one if none is. For self-started brokers, the agent-exit timeout defaults
to 30 minutes and can also be set with `FACTORY_AGENT_EXIT_TIMEOUT_MS`.

(There are a few more operational commands — `loop-status`, `kill-loop`,
`reap-orphans`, `close-probe` — for running the daemon in production.)

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

### Dispatching to nodes (`--backend relay`)

With `--backend relay`, the factory orchestrator dispatches work through the
hosted engine instead of a local broker: placement picks a live node with the
required capability (a named `node` target passes through), the node runs the
agent in its mapped checkout, and the orchestrator detects exits by reconciling
its tracked agents against the engine roster.

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

## Notes

- The daemon is headless by design; tools like Pear can consume this package and
  wrap it, but the published CLI is `factory`.
- The published `dist/` is plain ESM, runnable directly by Node (`node bin/factory.mjs`)
  and importable by ESM consumers.
- For production operation (the live-daemon + reaper backstop model, heartbeats,
  and connected-workspace prerequisites), see the operations notes alongside the config schema.
