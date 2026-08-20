# Diagnosing a deployed Factory

> Issue: [#295](https://github.com/AgentWorkforce/factory/issues/295) · companion: AgentWorkforce/factory-cloud `fix/295-healthz-diagnostics` — a deployed Factory had no
> operator-reachable diagnostics. The field naming the 2026-08-19/20 outage existed the whole time
> and was unreachable for ~10 hours.

## The command

```
factory diagnose --deployed https://<factory-host>
```

No credential required. It reads the unauthenticated `/healthz` and answers one question — *is this
instance dispatching, and if not, why* — with a non-zero exit when the answer is no, so a lane brief
or a cron entry can act on `$?` alone.

```
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
    "inFlightMs": 4560000,          // now − lastStarted: this pass has run 76 minutes
    "missedPasses": 76,
    "lastErrorClass": "TimeoutError"
  },
  "eventListener": { "state": "subscribed" },
  "fleetControlPlane": { "state": "closed", "consecutiveFailures": 0, "failureThreshold": 3 }
}
```

### Reading it

- **`consecutiveFailures` / `lastErrorClass`** — the failing case. During the outage this read 7 then
  8 while `/healthz` said `ok: true` and published nothing but the string `degraded`.
- **`lastStartedAtMs` vs `lastCompletedAtMs`** — the *silent* case. A sweep that hangs takes neither
  the success nor the failure path, so no state is written and every settled field keeps reading
  green. `lastStarted > lastCompleted` is the only evidence that a pass is in flight, and `inFlightMs`
  says for how long.
- **`fleetControlPlane`** — an `open` circuit fails every spawn and resume fast, so it gates dispatch
  as hard as a failing sweep. `closed` is the healthy value.
- **`state: "stalled"`** — derived, not written: an in-flight pass older than ten sweep intervals.
  A cold container legitimately spends minutes in its first pass (#36 measured 61 minutes while the
  Relayfile mirror hydrated), so check `lastCompletedAtMs`: absent means "first pass since boot,
  still hydrating"; present and hours old means "was fine, then wedged".

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
falling back to `Error`. Every other public field is a closed enum or a coerced number, built by
construction rather than by spreading the record — see `src/orchestrator/public-health.ts`.

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
  were true at write time and stay frozen; the container recomputes liveness from `updatedAtMs`
  against its own clock on every request, and that verdict (`ok: false`, HTTP 503) outranks anything
  the block still claims.
