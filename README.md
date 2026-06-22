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

1. **Write a minimal config** (`factory.config.json`). Only `workspaceId` and a
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

   `workspaceId` is your relay workspace; `repos.byLabel` maps a Linear label to a
   repo; `clonePaths` tells the agent where that repo lives locally so it has
   somewhere to make changes.

2. **Plan a cycle without touching anything** — `--dry-run` discovers and triages
   but writes nothing and spawns no agents:

   ```bash
   factory run-once --config ./factory.config.json --dry-run
   ```

3. **Let it work for real:**

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
`--backend <internal|relay>`. The internal backend reuses a relay broker that's
already running for your workspace, and starts one if none is.

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

Two ways to hand the factory an issue — both are just labeling/titling, nothing
to install:

| Source | What you do | Result |
|---|---|---|
| **Linear** | Title it `[factory] <task>`, set the team + a repo label, move it to **Ready for Agent** | dispatched directly |
| **GitHub** | Add the **`factory`** label to the issue | mirrored into a `[factory]` Linear issue, then dispatched |

The **safety gate** is what keeps this opt-in: by default the factory only
dispatches an issue whose **title starts with your configured prefix** *and* whose
**team matches** your configured team. Everything else is ignored. Loosen it
deliberately — it's the main guardrail.

> Tip: `[factory-e2e]` is reserved for the factory's own self-test soak (its PRs
> auto-close). For real work you want to keep, use the `[factory]` prefix.

## Run it as a fleet node (optional)

The package also ships a fleet **node definition** so a machine can advertise
`spawn:claude` / `spawn:codex` / `workflow:run` to the cloud and run agents in the
checkouts it owns:

```bash
agent-relay fleet serve @agent-relay/factory/node
```

It reads its node config from `./factory.node.json` (or `$FACTORY_NODE_CONFIG`).
Prefer to build the definition yourself?

```ts
import { createFactoryNodeDefinition, readFactoryNodeConfigSync } from '@agent-relay/factory'

export default createFactoryNodeDefinition({ config: readFactoryNodeConfigSync() })
```

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
  and `gh`-auth preconditions), see the operations notes alongside the config schema.
