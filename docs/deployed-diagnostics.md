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
    // The last COMPLETED sweep's arithmetic (#355). Absent until one completes.
    "candidates": 7,                // work units it pulled and evaluated
    "dispatched": 0,                // work units it dispatched
    "skipped": 7,                   // work units it saw and declined
    "skipReasons": { "dispatch-terminal": 7 }
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
  - **the three fields absent entirely** — this daemon has not *completed* a sweep (or predates #355).
    That is not a zero, and must not be read as one: it says nothing about either half. Check
    `lastCompletedAtMs` and `inFlightMs`.

  They describe the last sweep that settled **successfully**, the same tense as `lastDurationMs`;
  `lastCompletedAtMs` dates them. A pass that failed leaves them untouched rather than zeroing them.

- **`discoveryDeferred: "sweep-in-flight"`** — the sweep returned immediately because another process
  held the discovery lease, so it enumerated nothing. Without this, that pass is indistinguishable
  from one that queried the provider and legitimately found no ready work: both publish
  `candidates: 0`.

- **`skipReasons`** — `skipped` split by a closed vocabulary
  (`FACTORY_SWEEP_SKIP_REASON_CODES`); zero-count codes are omitted, so an absent key is a zero, and
  the counts always sum to `skipped`. `dispatch-terminal` and `dispatch-retry-limit` are the two that
  never clear on their own — a work unit in either needs a human. `dispatch-backoff`,
  `already-tracked` and `queued-or-escalated` resolve by themselves. `out-of-scope` and `not-ready`
  mean the gate is working as configured and the issue does not match it — check the deployed
  `safety` config against the issue rather than the daemon.

  Counts only, by construction: issue keys, paths and titles carry customer project and repository
  names and never cross onto this surface. The keys are rebuilt from the reader's own copy of the
  vocabulary, so a record from another version cannot publish an arbitrary string as one.

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
