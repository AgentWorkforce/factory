# @pear/factory-sdk

The autonomous issue factory: a loop that discovers Linear issues, triages them,
dispatches agents to implement fixes, opens PRs, drives them to completion through
a merge gate, and closes the issues — all under a hard safety scope.

The CLI binary is **`fleet`** (`bin/fleet.mjs`). It is an unpublished workspace
package, so from a repo checkout you invoke it through one of these — both work
with no install step:

```bash
# A root npm script (pass flags after `--`)
npm run factory:start -- --config <cfg>

# Or directly
node packages/factory-sdk/bin/fleet.mjs factory <action> --config <cfg>
```

Once you've installed the `pear` command (see below), the ergonomic form is:

```bash
pear factory <action> [options]
```

All three are equivalent; `pear factory …` simply forwards to `fleet factory …`.

### Installing the `pear` command (operators)

The factory is an operator tool, run from a repo checkout. `pear` is declared as
this repo's `bin` (`bin/pear.mjs`), but npm does not put a package's own bin on
your PATH automatically — so link it once. This is the standard way to install a
local Node CLI for development, not a workaround:

```bash
npm link        # from the repo root — creates a global `pear` → bin/pear.mjs
# now, from anywhere:
pear factory start --mode live --config ./factory.config.json
```

To remove it later: `npm unlink -g pear-by-agent-relay`.

> **Note:** the packaged Pear `.app` also offers *"Install 'pear' command in
> PATH"* (Pear menu), but that installs the **GUI** launcher for `pear open <dir>`
> only — the factory CLI is not bundled into the app. `pear factory …` is for
> operators running from the repo via `npm link` (or the `node`/`npm run` forms
> above).

> **Heads-up:** every `factory` action requires `--config <path>` (see
> [Configuration](#configuration)). Commands fail fast with
> `factory commands require --config <path>` if it's missing.

---

## Quick start

```bash
# One-shot batch — discover + triage + dispatch one cycle, then exit.
# Add --dry-run to plan without writing or spawning anything.
pear factory run-once --config ./factory.config.json --dry-run

# Live daemon — subscription-driven; runs until SIGINT/SIGTERM.
pear factory start --mode live --config ./factory.config.json
```

## Commands

| Command | What it does |
|---|---|
| `factory run-once` | One discovery→triage→dispatch cycle, then exit. Honors `--dry-run`. |
| `factory loop` | Bounded multi-iteration loop (`loop.maxIterations`), then exit. |
| `factory start --mode live` | Long-lived daemon. Subscription-driven; writes + refreshes a loop heartbeat. The production entrypoint. |
| `factory status` | Print the in-memory factory status as JSON. |
| `factory loop-status` | Read the heartbeat file and report liveness (stale vs. alive). |
| `factory kill-loop` | Send SIGTERM to the daemon PID recorded in the heartbeat. |
| `factory reap-orphans` | Crash backstop: reap orphaned agent pairs whose heartbeat is stale. Run as a scheduled job. |
| `factory triage <KEY\|path>` | Triage a single issue and print the decision. |
| `factory dispatch <KEY\|path>` | Triage + dispatch a single issue. Honors `--dry-run`. |
| `factory close-probe <PR#> --repo <owner/repo> --issue <KEY>` | Manually close a synthetic E2E probe PR. |

Global options (accepted anywhere in the args): `--config <path>`,
`--dry-run`, `--backend <internal\|relay>`.

## The 2-process production model

The factory runs as **two** coordinated processes (see issue #321 §4):

1. **Live daemon** — `pear factory start --mode live --config <live>`. Drives the
   loop and refreshes a heartbeat file.
2. **External reaper** — a *scheduled* `pear factory reap-orphans --config <live>`
   that acts as a crash backstop, cleaning up orphaned agent pairs if the daemon dies.

> **HARD precondition:** the reaper **must** use the **same `--config`** as the
> live daemon. A mismatched config reaps nothing *and* leaves the backstop broken
> (the coupling is load-bearing — see §7 of issue #321).

### Other operating preconditions (issue #321 §7)

- **`gh`-authenticated environment.** The gh-resolver is completion-load-bearing
  while the cloud GitHub→mount PR-sync is degraded; a `gh` auth drop halts completion.
- Run the real binary path (`node bin/fleet.mjs` / `pear factory`), not a shim.

## Configuration

Pass a JSON file via `--config`. The full schema (with every default) lives in
[`src/config/schema.ts`](src/config/schema.ts), validated by Zod at load time —
an invalid config fails fast with a field-level error.

The file is either a bare config object, or a `{ "factoryConfig": { … } }`
wrapper (the wrapper form also allows a sibling `fixtureFiles` map — see
[Fixture mode](#fixture-mode-offline-testing)).

### Minimal config

Only two fields are required — `workspaceId` and `repos.byLabel`. Everything else
has a default:

```json
{
  "workspaceId": "your-workspace-id",
  "repos": {
    "byLabel": { "pear": "AgentWorkforce/pear" },
    "default": "AgentWorkforce/pear"
  }
}
```

### Complete annotated config

A realistic live config, showing the fields you'll actually want to set (comments
are illustrative — strip them; JSON has no comments):

```jsonc
{
  // relayfile cloud mount workspace id — the workspace whose /linear and /slack
  // trees the factory reads. Same id the Pear app uses for this workspace.
  "workspaceId": "ws_abc123",

  // Which Linear issues to pull. Empty arrays = no filter on that dimension.
  // Values are Linear team keys / project names / label names / assignee ids.
  "subscription": {
    "teams": ["AR"],
    "labels": ["pear"],
    "projects": [],
    "assignees": []
  },

  // Issue → repo routing. Precedence (first match wins):
  //   byLabel  →  byProject  →  keywordRules (regex on title/desc)  →  default
  // No match and no default → the issue is escalated, never dispatched.
  "repos": {
    "byLabel":  { "pear": "AgentWorkforce/pear", "cloud": "AgentWorkforce/cloud" },
    "byProject": { "Pear": "AgentWorkforce/pear" },
    "keywordRules": [{ "pattern": "relayfile|mount", "repo": "AgentWorkforce/relayfile" }],
    // Where each repo is checked out locally for the agent to work in.
    "clonePaths": { "AgentWorkforce/pear": "/Users/you/Projects/pear" },
    "default": "AgentWorkforce/pear"
  },

  // SAFETY GATE — an issue is only dispatched if BOTH hold (see below):
  "safety": {
    "requireTitlePrefix": "[factory-e2e]",
    "requireTeamKey": "AR"
  },

  "batchSize": 5,                       // max issues dispatched per cycle
  "dispatch": { "maxAttempts": 2, "errorCooldownMs": 60000 },
  "mergePolicy": "never",               // see mergePolicy note below
  "models": {                           // optional per-role model overrides
    "implementer": "claude-opus-4-8",
    "reviewer": "claude-opus-4-8",
    "triage": "claude-haiku-4-5-20251001"
  },
  "slack": { "channel": "C0B902XR6PN" },   // optional threaded status updates
  "loop": {
    "heartbeatPath": "/tmp/factory-run/factory-loop-heartbeat.json",
    "registryPath":  "/tmp/factory-run/factory-loop-registry.json",
    "heartbeatStaleMs": 60000
  }
}
```

### Where the values come from

- **`workspaceId`** *(required)* — the relayfile cloud mount workspace id for this
  workspace (the same one the Pear app mounts `/linear` and `/slack` under).
- **`repos.byLabel`** *(required)* — map a Linear label → `owner/repo`. The other
  routing maps are optional; **precedence is `byLabel` → `byProject` →
  `keywordRules` → `default`**, else the issue is escalated (never dispatched).
- **`repos.clonePaths`** — `owner/repo` → local working-tree path. Without an entry
  the agent has nowhere to apply changes for that repo, so set it for every repo
  you actually dispatch to.
- **`stateIds`** — the Linear workflow-state UUIDs for `readyForAgent`,
  `agentImplementing`, `done`, `inPlanning`. Defaults are the **AR team's** states
  (see [`src/constants/linear.ts`](src/constants/linear.ts)). If you run against a
  different Linear team, override all four with that team's state UUIDs (read them
  from the Linear API / the issue JSON's `state.id`).
- **`safety`** — the scope gate (below). Defaults `[factory-e2e]` + team `AR`.

### The safety gate (what actually gets dispatched)

`isInFactoryScope` ([`src/safety/factory-scope.ts`](src/safety/factory-scope.ts))
dispatches an issue only when **both** are true:

1. The issue **title starts with `safety.requireTitlePrefix`** — exactly
   `[factory-e2e]`, or `[factory-e2e] <rest>`. Anything else is out of scope.
2. The issue's **team key equals `safety.requireTeamKey`** (`AR`).

This is why `factory run-once` may pull issues but dispatch none — they're real
issues that fall outside the gate. Loosen the gate deliberately; it's the primary
guardrail against the factory acting on issues it shouldn't.

### Other notable fields (defaults in parentheses)

- `mergePolicy` (`never`) — `never` keeps PRs open; `on-green-with-review` enables
  autonomous merge on green + approved review. **Stays `never` until the flip is
  thrown** (issue #321 §6).
- `loop.heartbeatPath` / `loop.registryPath` / `loop.heartbeatStaleMs` (`60000`) —
  daemon/reaper coupling (the reaper must point at the same paths via the same
  `--config`).
- `batchSize` (`5`), `dispatch.maxAttempts` (`2`), `dispatch.errorCooldownMs`
  (`60000`), `models.{implementer,reviewer,triage}`, `slack.channel`,
  `subscription.*`, `liveSubscription.*`, `stateIds.*`.

### Fixture mode (offline testing)

If the config (or a `{ "factoryConfig": …, "fixtureFiles": … }` wrapper) includes a
`fixtureFiles` map, the CLI swaps in fake fleet + mount clients backed by those files —
no cloud, no real agents. See [`test/fixtures/factory.config.json`](test/fixtures/factory.config.json).

## Notes

- **`pear factory …` does not launch the Electron app.** It detects the `factory`
  verb in `bin/pear.mjs` and shells out to `fleet.mjs` in a plain Node process;
  the daemon is headless by design.
- First run builds the CLI with esbuild into `node_modules/.cache/pear-factory-sdk`;
  pass `--rebuild` (or `FLEET_FORCE_BUILD=1`) to force a rebuild.
