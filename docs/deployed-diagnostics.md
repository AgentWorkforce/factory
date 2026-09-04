# Diagnosing a deployed Factory

> Issue: [#295](https://github.com/AgentWorkforce/factory/issues/295) · companion: AgentWorkforce/factory-cloud `fix/295-healthz-diagnostics` — a deployed Factory had no
> operator-reachable diagnostics. The field naming the 2026-08-19/20 outage existed the whole time
> and was unreachable for ~10 hours.

## The command

```sh
factory diagnose --deployed https://<factory-host>
```

No credential required. It reads the unauthenticated `/healthz` and answers one question — *is this
instance dispatching, and if not, why* — with a non-zero exit when the answer is no, so a lane brief
or a cron entry can act on `$?` alone.

```sh
factory diagnose --deployed <url>              # human-readable
factory diagnose --deployed <url> --json       # the same diagnosis as JSON
factory diagnose --deployed <url> --token <t>  # also read the gated /evidence
factory diagnose --deployed <url> --timeout-ms 30000
```

`--token` defaults to `FACTORY_EVIDENCE_TOKEN` when set. Without it the command still works; it just
cannot show the free-text `lastError`, and says so.

Exit codes follow `factory canary`: `0` when the instance is dispatching, `1` when it is not or
cannot be reached.

## Which build is answering

`/healthz` names the running build (#446). Two facts, and no credential:

```sh
curl -fsS https://<factory-host>/healthz | jq '.heartbeat.health.build'
# { "version": "0.1.86", "commit": "23e97cadc24f3e239879671975a962b577cf4979" }
```

`factory diagnose --deployed <url>` renders the same pair as the first line of its health
block — after the `factory diagnose — <url>` header, `reachable`, and any `phase` /
`instance liveness` lines — and `--json` carries it at `.health.build`.

That answers the first question of every outage — *is the fix I merged actually running?* — as a
lookup. It used to be an argument: compare `heartbeat.startedAt` against a merge time and infer that
a deploy must have happened in between. The inference is only sound if a deploy did happen, which
the endpoint could not say either. It matters more than it looks, because the container rolls
roughly hourly: which build is up changes with no one deploying.

`version` is the published `@agent-relay/factory` version — the same string `factory-version.json`
pins in factory-cloud, read back from the image instead of from the repo that built it. `commit` is
the Git commit that produced the artifact, stamped into `dist/build-info.json` by
`scripts/write-build-info.mjs` during `npm run build`.

Three readings, three different remedies:

| what you see | what it means |
|---|---|
| `"commit": "<40 hex>"` | that commit is running. `git log <commit>..main` is the gap. |
| `"commit": "unknown"` | the instance answered, and its artifact carries no stamp — it is a release older than #446. |
| no `build` key at all | the instance is older still: it predates the field. Never read this as `unknown`. |

Nothing synthesises a stand-in for a missing stamp. The build itself refuses to emit one — no SHA,
no build — so `unknown` in a released artifact means "published before this existed", never "the
stamp failed quietly". `src/orchestrator/build-identity.ts` carries the reasoning; the guarantee is
tested against the packed tarball in `scripts/verify-packed-e2e.mjs`, which reads the stamp out of
the installed consumer copy and requires it to be the commit under test.

Two places it is not reachable, both in `factory-cloud`'s `container/entrypoint.mjs` and both
fixable there rather than here:

- **`/evidence`** assembles its own projection (`detailedHeartbeat()`) and drops the `health` block,
  so it carries no build. `/healthz` is unauthenticated, so the readback above needs no token anyway.
- **Before the daemon's first heartbeat write** the container has no `health` block to pass through,
  so `/healthz` names no build during cold start — which is also when it answers 503. Ask again once
  it is up; "no build key" from a *live* instance means an image older than #446, not a boot.

## Why the other routes do not work

| route | why not |
|---|---|
| `factory status`, `factory loop-status` | inspect a **local** instance only |
| `wrangler tail` | Worker scope — the Factory process runs in the Container and its stdout does not surface |
| `wrangler containers ssh` | WebSocket 400: the container is private-networked with no sshd |
| `GET /evidence` | carries the answer, but is bearer-gated by a token minted per deploy and destroyed at the end of the run that created it |

Log egress therefore has to be pull-through-the-Worker. `factory diagnose --deployed` is that pull.

## What `/healthz` now carries

The daemon writes a redacted projection of its loop heartbeat — `heartbeat.health`, built by
`publicHealthFromHeartbeat()` — and the container serves it verbatim. The container has no redaction
logic of its own by design: the boundary lives in one place, in this repo, with tests.

```jsonc
// The daemon stamps this when it WRITES the heartbeat, so `ageMs` is 0 and
// `stale` false in the file; freshness is `updatedAtMs` against the clock of
// whoever serves it. Here now = 1787229155805 (2026-08-20T12:32:35.805Z).
{
  "schemaVersion": 1,
  // Identity, not health (#446). It rides here because `health` is the one
  // part of the heartbeat the container passes through to /healthz verbatim.
  "build": { "version": "0.1.86", "commit": "23e97ca…4979" },
  "ok": true,                       // process liveness — see below
  "status": "degraded",             // the amber
  "stale": false,
  "updatedAtMs": 1787229155805,
  "ageMs": 0,
  "loopStatus": "running",
  "degradedSubsystems": ["readinessReconcile"],
  "reason": "dispatch-gating subsystem not healthy: readinessReconcile",
  "readinessReconcile": {
    "state": "stalled",             // not-running | healthy | retrying | degraded | stalled
    "consecutiveFailures": 0,
    "failureThreshold": 3,
    "intervalMs": 60000,
    "lastStartedAtMs": 1787224595805,   // 11:16:35.805Z
    "lastCompletedAtMs": 1787224535802, // 11:15:35.802Z — 60s EARLIER
    "inFlightSinceMs": 1787224595805,   // when the oldest sweep still running began
    "inFlightMs": 4560000,          // this pass has run 76 minutes
    "missedPasses": 76,
    "lastErrorClass": "TimeoutError",
    // The last ENUMERATING sweep's arithmetic (#355). Absent until one enumerates.
    "candidates": 7,                // work units it pulled and evaluated
    "dispatched": 0,                // work units it dispatched
    "skipped": 7,                   // work units it saw and declined
    "skipReasons": { "dispatch-terminal": 7 },
    // When THOSE counts were measured. Not lastCompletedAtMs, which also
    // advances on a deferred pass that enumerated nothing.
    "lastEnumeratedAtMs": 1787224535802
  },
  "eventListener": { "state": "subscribed" },
  "fleetControlPlane": { "state": "closed", "consecutiveFailures": 0, "failureThreshold": 3 }
}
```

### Reading it

- **`consecutiveFailures` / `lastErrorClass`** — the failing case. During the outage this read 7 then
  8 while `/healthz` said `ok: true` and published nothing but the string `degraded`.
- **`inFlightSinceMs`, or `lastStartedAtMs` vs `lastCompletedAtMs`** — the *silent* case. A sweep that
  hangs takes neither the success nor the failure path, so no state is written and every settled field
  keeps reading green. `inFlightSinceMs` is the daemon saying outright when the oldest sweep still
  running began; `inFlightMs` is its age. Where it is absent — a heartbeat written by a build before
  #296 — fall back to `lastStarted > lastCompleted`, which infers the same thing from timestamp order.
  Prefer the published field: once a sweep has passed its deadline (below) the wait records a failure
  while the sweep underneath it keeps running, and order alone then reports nothing in flight.
- **`candidates` / `dispatched` / `skipped`** — the *green-but-idle* case, and the fastest question to
  ask when nothing is being dispatched and every state above reads healthy. On 2026-08-23 a sub-second
  sweep with `state: healthy`, `consecutiveFailures: 0` and a free dispatch slot declined seven
  eligible issues, and no surface anyone could reach said which half of the pipeline was at fault.

  - `candidates > 0` — the sweep **saw** those issues and **rejected** them. The bug is in eligibility
    evaluation, and `skipReasons` names which gate.
  - `candidates == 0` — the sweep **never pulled** them. The bug is upstream, in discovery/ingestion.
  - **the three fields absent entirely** — this daemon has not completed a sweep that **enumerated**
    (or predates #355). That is not a zero, and must not be read as one: it says nothing about either
    half. Check `discoveryDeferred` first, then `lastCompletedAtMs`, `lastFailureAtMs`, and `inFlightMs`
    to distinguish a completed deferral, a failure, work still running, and a pre-counter daemon.

  They describe the last sweep that settled successfully **and enumerated**. A pass that failed —
  or that deferred — leaves them untouched rather than zeroing them.

- **`lastEnumeratedAtMs`** — when those counts were measured, and the field to check before acting on
  them. It is **not** `lastCompletedAtMs`: that one advances on every settled pass including a
  deferred one, so on a daemon contending for the discovery lease the counts would otherwise sit
  beside an ever-fresh completion stamp with no way to tell a measurement one interval old from one
  four days old. Equal to `lastCompletedAtMs` on a daemon sweeping normally; where they differ, the
  gap is exactly how stale the counts are.

- **`discoveryDeferred: "sweep-in-flight"`** — the **most recent** pass returned immediately because
  another process held the discovery lease, so it enumerated nothing. It is tracked apart from the
  three counts, which describe the last sweep that actually enumerated:

  - **with the counts** — those numbers are from an *earlier* pass, not the one `lastCompletedAtMs`
    dates. A deferred pass records only this marker; its zeroes measure nothing and must not
    overwrite a real sweep's numbers, which under a persistently-held lease would erase them.
  - **alone, with no counts** — nothing has enumerated successfully yet on this daemon. The most
    recent pass deferred because another process held the lease; an earlier startup attempt may
    instead have failed before it could publish an outcome.

  `lastCompletedAtMs` *does* move for a deferred pass. That is deliberate: the stall derivation above
  reads it against `lastStartedAtMs`, so freezing it would report a daemon that is correctly
  deferring to another owner as hung after ten intervals.

- **`skipReasons`** — `skipped` split by a closed vocabulary
  (`FACTORY_SWEEP_SKIP_REASON_CODES`); zero-count codes are omitted, so an absent key is a zero, and
  the counts always sum to `skipped`. The full vocabulary, grouped by what to do about it:

  | code | |
  |---|---|
  | `dispatch-terminal`, `dispatch-retry-limit` | **needs a human** — permanently declined, never clears on its own |
  | `dispatch-backoff`, `dispatch-in-flight`, `already-tracked`, `queued-or-escalated` | transient; resolves by itself |
  | `out-of-scope`, `not-ready`, `not-dispatchable` | the gate is working as configured and the issue does not match it — check the deployed `safety` config against the issue, not the daemon |
  | `parked-dependency`, `dependency-cycle` | parked on other work; a cycle needs a human to break it |
  | `read-failed`, `dispatch-failed` | per-item failures the sweep **absorbed and continued past** (#292/#297) — see below |
  | `other` | a code this reader's vocabulary does not know, from a producer on another version |

  `read-failed` and `dispatch-failed` count work units an otherwise-**successful** pass gave up on
  individually. Do **not** reach for `lastErrorClass` to explain them: that field describes a pass
  that *failed as a whole*, and the success path clears it, so it is absent in exactly this scenario.
  The per-item messages go to the container log (`[factory] relayfile shed a ready-issue read…`,
  `[factory] skipped a work unit whose dispatch failed…`); the count here is what tells you to go
  looking. A rising `read-failed` alongside `state: healthy` is the #297 shedding signature.

  Counts only, by construction: issue keys, paths and titles carry customer project and repository
  names and never cross onto this surface. The keys are rebuilt from the reader's own copy of the
  vocabulary — anything unrecognised is counted under `other` rather than dropped, so the parts keep
  summing to `skipped` — which is also why a record from another version cannot publish an arbitrary
  string as a key.

- **`fleetControlPlane`** — an `open` circuit fails every spawn and resume fast, so it gates dispatch
  as hard as a failing sweep. `closed` is the healthy value.
- **`state: "stalled"`** — derived, not written: an in-flight pass older than ten sweep intervals.
  A cold container legitimately spends minutes in its first pass (#36 measured 61 minutes while the
  Relayfile mirror hydrated), so check `lastCompletedAtMs`: absent means "first pass since boot,
  still hydrating"; present and hours old means "was fine, then wedged".
- **How long a stall can last** — two deadlines, at different scales.

  `liveSubscription.relayfileOperationTimeoutMs` bounds ONE relayfile call, five minutes by default
  (#351). This is the one that catches a wedge. Expiry cancels the request, fails the pass with
  `lastErrorClass: "RelayfileOperationTimeoutError"` and a `lastError` naming the call
  (`relayfile listTree did not respond within 300000ms (GitHub issue ingestion)`), and unwinds the
  sweep — which releases the discovery lease, so the next cycle starts clean.

  `liveSubscription.reconcileTimeoutMs` bounds the whole sweep, 90 minutes by default (#296). It is
  the outer backstop only. On expiry the *wait* fails, so `consecutiveFailures` starts rising and the
  loop schedules the next pass; the sweep itself is not cancelled, because it holds a durable
  discovery lease, so `inFlightSinceMs` keeps ageing until it really finishes — and the next pass
  coalesces onto that same running `runOnce()`. The deadline sits above #36's 61-minute measurement on
  purpose: setting it below realistic cold-mirror hydration would turn a slow boot into a crash loop.
  Per-call bounds can be far tighter precisely because that cold-mirror cost is spread across
  thousands of calls rather than concentrated in one.

  A `stalled` state that never turns into a rising `consecutiveFailures` means either the process is
  not running the loop at all, or it predates #351 — on a current build a hung call fails within
  `relayfileOperationTimeoutMs`.

- **`treeReads` / `emptyTreeReads`** — the case a timeout cannot catch. A mount that starts serving
  *empty* trees instead of hanging raises no timeout, no failure and no `lastError`: the sweep
  completes `healthy` and dispatches nothing, which on every other field is indistinguishable from a
  workspace that simply has no ready work.

  Read them as a pair, against `candidates`. An empty read on its own is ordinary — a healthy sweep
  lists two path forms per repo and only one of them exists. The fault is
  `emptyTreeReads === treeReads` with `treeReads > 0`: the mount served nothing at all. So
  `candidates: 0, treeReads: 3, emptyTreeReads: 1` is an empty workspace, and
  `candidates: 0, treeReads: 3, emptyTreeReads: 3` is a silent mount.

  `factory diagnose --deployed` makes that reading for you: it renders a `tree reads` line and
  folds the verdict into the "Last enumerating sweep" sentence, so an all-empty pass says
  `the mount served nothing at all` rather than leaving a zero `candidates` to speak for itself.

  Both numbers count only the reads the readiness sweep's own discovery pass issued. In live mode
  event drains and completion timers list trees too, and a populated lookup landing in the
  denominator would make `emptyTreeReads < treeReads` on a sweep whose every discovery read was
  empty — masking the fault. So a sweep whose roots all came from the discovery cache reports
  `treeReads: 0`, which claims nothing in either direction.

### Why `ok` stays `true` while `status` goes amber

`/healthz` is the Cloudflare **Container ping endpoint** (`pingEndpoint = 'localhost/healthz'` in the
Worker). A non-200 there is a liveness verdict the platform acts on: it recycles the container. That
would destroy the in-memory evidence of the wedge and restart the cold-start hydration — turning a
diagnosable degradation into a restart loop that also erases its own cause.

So the two questions are split:

- `ok` — *is this process alive?* Unchanged semantics, safe to keep driving the ping and the HTTP
  status code.
- `status` (`ok` / `degraded` / `unknown`) and `degradedSubsystems` — *is dispatch gated?* No platform
  reads these, so a monitor can alert on `status != "ok"` with no lifecycle side effect.

A liveness endpoint that cannot go amber is not much of a signal — this one goes amber in a field
that cannot restart the box.

## What never crosses

`lastError` is dependency-controlled free text and routinely carries provider prose, filesystem paths
and URLs with credentials in the query string. It stays on the authenticated `/evidence` surface. The
public block carries only its **class**, through the same allowlist that guards
`IterationReport.skipped[].reason` (`src/observability/error-class.ts`): a pattern-checked class name,
falling back to `Error`.

Every other field is constructed explicitly and validated for what it is — states against closed
enums, counters and timestamps coerced with range and sign checks, `degradedSubsystems` filtered to
a fixed set of names, and the one assembled string (`reason`) built from those same names, then
control-stripped and length-bounded. Nothing is spread, so a field added upstream cannot reach the
public surface by default. See `src/orchestrator/public-health.ts`.

Regression coverage: `src/orchestrator/public-health.test.ts` and the `#295` block in
`src/orchestrator/factory.test.ts` feed a `lastError` containing a path, a URL and a token and assert
none of it appears in the published record.

## Serving the block (factory-cloud)

The container entrypoint passes the block through unchanged:

```js
// container/entrypoint.mjs — publicHeartbeat()
return {
  status: parsed.status,
  updatedAt: parsed.updatedAt,
  updatedAtMs: parsed.updatedAtMs,
  eventListener: parsed.eventListener?.state,
  readinessReconcile: parsed.readinessReconcile?.state,
  health: parsed.health,        // already redacted by the daemon
}
```

so `/healthz` answers `{ ok, phase, factoryProcess, heartbeat: { …, health } }`. `factory diagnose`
reads the block from `heartbeat.health`, and accepts a top-level `health` as well.

Instances running a Factory older than this change publish no `health` block; `factory diagnose`
detects that and says so rather than reporting a false green.

Two other shapes the command refuses to read as green:

- **Event-driven short-sleep mode.** With `FACTORY_EVENT_DRIVEN_SLEEP_ENABLED=1` the Worker answers
  `/healthz` itself and never probes the container, deliberately — anonymous polling must not be a
  second wake path. That response (`phase: "worker-ready"`, `container: "not-probed"`) is Worker
  liveness and carries no Factory health, so `factory diagnose` reports *cannot tell* and points at
  `/evidence`, which does reach the container.
- **A container serving a heartbeat its daemon stopped updating.** The block's own `stale`/`ageMs`
  are not measurements of a read — they are constants of the write: `ageMs` is always `0` and
  `stale` always `false` in the file, whether that file is one second or one week old. Freshness
  comes from `updatedAtMs` measured against the clock of whoever serves it, which is what the
  container does on every request; that verdict (`ok: false`, HTTP 503) outranks anything the block
  still claims.
