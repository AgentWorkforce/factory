Title: [factory] EPIC — cloud watches → local-node execution; label-driven single agent / workflow / team

Team: AR
Suggested status: Design / Epic
Repo label: pear (spans pear, cloud, relay, factory)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Related: AgentWorkforce/pear#347 (serverless factory), AgentWorkforce/relay#1056 (Fleet Delivery RFC)
Children: linear-issue-factory-extract-p1..p4 (Phase 1 extraction stack)

---

## TL;DR

A user runs **one command** — `agent-relay fleet serve <factory-node-def>` — and walks away. From then on, **labeling a Linear issue** is the entire interface:

- the **subscription/watch layer runs in our cloud** (always-on, multi-tenant — the user provisions nothing),
- the **label on the issue decides the shape** of work: a **single agent**, a **workflow**, or a **team**,
- the **agents spawn on the user's own machine**, in their real local checkouts, using their local harness (Claude/Codex),
- in-flight state, triage, and the merge gate live in cloud so a closed laptop never orphans work.

To make this real, the factory **moves out of Pear into its own published package, `@agent-relay/factory`** (repo `AgentWorkforce/factory`), consumed by cloud (the brain), the `agent-relay` CLI (the body), and Pear (an optional desktop view).

**Headline scoping result (see §6):** this needs **almost no net-new infrastructure.** Cloud's `proactive-runtime` already is the serverless watch→dispatch→durable-state engine #347 describes, and relaycast's **fleet** layer already does node registration + spawn-to-laptop. The real work is *code* — wiring the dispatch target to fleet nodes, hosting the triage/merge brain, and finishing ~10% of fleet plumbing.

---

## 1. Why this exists / problem statement

The factory today is a long-running daemon **spawned by Electron** inside Pear. It works, but:

- It only runs while Pear is open on one machine — not a product, an app feature.
- The orchestration brain (triage, merge gate, in-flight tracking) is welded to the Electron process and to in-memory state, so it can't be multi-tenant or survive a restart.
- Dispatch is hardcoded to the local harness (`InternalFleetClient`); the `RelayFleetClient` is a stub that throws `relay#1056`.
- There is no clean way for a user to say "single agent vs workflow vs team" — scope is computed implicitly and only supports single/team.

We want the opposite: **zero-infra for the user, cloud does the watching, the user's machine does the work, and the label is the API.**

---

## 2. Target architecture

```
CLOUD  (always-on, multi-tenant — hosted by us; REUSES existing proactive-runtime)
  relayfile webhook POST: /api/v1/webhooks/{github,linear}   ◀── already shipped
        │
        ├─ watch→match→dispatch   ← integration-watch-dispatcher.ts (EXISTS)
        ├─ triage()               ← @agent-relay/factory TriageEngine  (NEW code, hosted here)
        │     └─ reads issue LABEL → scope: single | workflow | team
        ├─ merge-gate()           ← @agent-relay/factory GithubMergeGate (NEW code, hosted here)
        ├─ durable state          ← EXISTING Postgres tables: integration_watch_deliveries,
        │                           integration_watch_issue_dispatch_dedup, proactive_continuations
        └─ dispatch target ───────────┐  (TODAY: Daytona sandbox.  NEW: make pluggable → fleet node)
                                       │
                            relaycast fleet fabric (relay#1056 — ~90% shipped)
                                       │  agent.create { capability, node }
LOCAL   `agent-relay fleet serve <factory-node-def>`  ◀┘   (today's command, no rename)
  • reads node-local config (repo→path map, capabilities)
  • registers machine as a NODE, advertises spawn:claude / spawn:codex   (EXISTS)
  • receives spawn → runs agent(s) locally in the correct checkout       (EXISTS: InternalFleetClient / harness-driver)
  • streams exit/messages back to cloud → merge-gate + Linear/Slack writeback
```

**Split of responsibility**

| Concern | Runs in | Status |
|---|---|---|
| Subscription / watches (Linear, Slack, GitHub) | **Cloud** | ✅ relayfile webhook POST + watch dispatcher exist |
| Triage (heuristic → LLM tiers) + scope selection | **Cloud** | NEW code (`@agent-relay/factory`), hosted in proactive-runtime |
| Durable state (in-flight, queued, clarification) | **Cloud** | ✅ existing Postgres tables (not DynamoDB/DO) |
| Merge gate verdict + merge execution | **Cloud** | NEW code; cloud already reads `/github` + holds GitHub auth |
| Placement (which node runs the spawn) | **Cloud / relaycast** | ⚠️ targeting logic is the ~10% net-new |
| Agent spawn / local execution | **Local node** | ✅ `fleet serve` + `agent.create` + harness-driver exist |
| Trust boundary (accept/reject spawns) | **Local node** | broker is token authority for what it spawns (#1056 §4) |

---

## 3. The label → single agent / workflow / team mechanic

The label on a Linear issue selects the **execution shape**. This widens the existing `TriageDecision.scope` from `single | team` to three values and binds it to labels.

**Explicit labels (deterministic, win over inference):**

| Label | Shape | What spawns |
|---|---|---|
| `agent:single` | **Single agent** | One implementer, on the node, in the target repo |
| `agent:workflow` | **Workflow** | A defined multi-step sequence (e.g. plan → implement → test), one or more spawns driven by a workflow definition |
| `agent:team` | **Team** | Lead + N implementers + reviewer roster (today's factory "team" scope / cloud `cloud-team-issue`) |

**Inference fallback (no explicit shape label):** `TieredTriage` runs — heuristic first (label/keyword/size signals), escalate to `LlmTriage` for thin or low-confidence issues — and emits a scope plus confidence. Low confidence → route to Slack for clarification (existing clarification state machine) before spawning.

**Routing labels** (existing, orthogonal to shape): repo-name labels still select the target repo via `repos.byLabel`. Safety gates (`requireTitlePrefix`, `requireTeamKey`) still apply.

**Design note:** "single agent / workflow / team" is one knob (`scope`) with three values — *not* three code paths. Triage emits `scope`; placement translates `scope` into a spawn count + roster + (optional) workflow ref; the node executes whatever it's handed. Cloud's existing single-agent / workflow / team execution shapes are the convergence target — the factory should emit specs those paths already understand rather than inventing a fourth dialect.

---

## 4. Where the factory lives — extract `@agent-relay/factory`

**Decision: the factory moves out of `pear/packages/factory-sdk` into its own published package/repo, `@agent-relay/factory` (GitHub `AgentWorkforce/factory`).**

Rationale: three consumers across three separate git repos need the same logic.

```
@agent-relay/factory  (repo AgentWorkforce/factory, published to npm public)
  ├─ triage/         TriageEngine, Heuristic / Llm / Tiered  (cloud)
  ├─ github/         GithubMergeGate                          (cloud)
  ├─ orchestrator/   watch → triage → dispatch → complete loop, state machine (cloud)
  ├─ state/          StateStore port (InMemory | cloud-Postgres-backed)        (cloud)
  ├─ fleet/          FleetClient port; InternalFleetClient (local), RelayFleetClient (#1056)
  ├─ config/         WorkspaceConfig (cloud) + NodeConfig (local)  — split per child p2
  └─ ports/          LinearWriteback / SlackWriteback / GithubRead (already clean)

Consumers:
  cloud         → imports orchestrator + triage + github + state  (the brain, hosted)
  relay CLI     → imports fleet + config                          (`agent-relay factory start`)
  pear          → imports config + a thin view client             (optional desktop UI)
```

The extraction mechanics are tracked in the child stack:
- **p1** — define `StateStore` port + `InMemoryStateStore`, route `BatchTracker` / `InFlightRegistry` / clarification through it.
- **p2** — split `FactoryConfig` → `WorkspaceConfig` + `NodeConfig`.
- **p3** — tsc build output, drop `private`, rename bin `fleet` → `factory`.
- **p4** — move to `AgentWorkforce/factory`, publish `@agent-relay/factory@0.1.0`, pear consumes it.

The package already has the right seams: `FactoryPorts`, clean `LinearWriteback` / `SlackWriteback` / `GithubRead`, and `RelayfileCloudMountClient`. The extraction is mostly moving files + splitting config, not a rewrite.

---

## 5. Config split

Today `factory.config.json` mixes two concerns. Split them (child p2):

**WorkspaceConfig (lives in cloud, set once via web or `agent-relay factory config`):**
```jsonc
{
  "workspaceId": "rw_...",
  "subscription": { "teams": ["AR"], "labels": [ ... ] },
  "repos": { "byLabel": { "pear": "AgentWorkforce/pear", "factory": "AgentWorkforce/factory", ... }, "default": "AgentWorkforce/pear" },
  "batchSize": 5,
  "mergePolicy": "never",
  "safety": { "requireTitlePrefix": "[factory]", "requireTeamKey": "AR" },
  "slack": { "channel": "C0..." }
}
```

**NodeConfig (the only file `agent-relay factory start` reads):**
```jsonc
{
  "workspaceId": "rw_...",
  "capabilities": ["spawn:claude", "spawn:codex"],
  "repoPaths": {
    "AgentWorkforce/pear":  "/Users/khaliqgant/Projects/AgentWorkforce/pear",
    "AgentWorkforce/relay": "/Users/khaliqgant/Projects/AgentWorkforce/relay"
  }
}
```

The node pushes its `repoPaths` keys + `capabilities` up on registration so cloud triage only places work the node can actually service; a placement for an unmapped repo is rejected by the node and reconciled.

**StateStore note:** p1 defines the port + `InMemoryStateStore`. The **cloud** impl (Phase 2) backs it with the **existing** proactive-runtime Postgres tables (`integration_watch_deliveries`, `integration_watch_issue_dispatch_dedup`, `proactive_continuations`) — **not** a new DynamoDB/DO store as the original #347 draft imagined.

---

## 6. Infrastructure: what exists vs. net-new (the scoping answer)

**Conclusion: no new infrastructure/services are required.** Everything #347 proposed building already runs in `../cloud` + relay. Verified inventory:

### Cloud — already provides the entire serverless watch+state engine
| #347 requirement | Reality today | File |
|---|---|---|
| relayfile **webhook delivery** (flagged as *the* unknown) | relayfile already **POSTs** to cloud; dispatch runs on ingress | `cloud/.../app/api/v1/webhooks/github/route.ts` |
| watch→match→dispatch, no polling | exists, fires on `/linear`, `/github` path changes; matches `watch_globs`/`watch_rules` | `cloud/.../proactive-runtime/integration-watch-dispatcher.ts` |
| durable `StateStore` (DynamoDB/DO) | Postgres already holds full lifecycle + cooldown/coalesce + multi-turn | `cloud/.../db/schema.ts` (`integration_watch_deliveries`, `_dedup`, `proactive_continuations`) |
| cron sweep | relaycron Worker `* * * * *` drains deliveries + reaps sandboxes | `cloud/packages/relaycron/src/sweep.ts` |
| multi-cloud AWS+CF handlers | **unnecessary** — host in proactive-runtime | — (skip) |

### Relay — fleet fabric is ~90% shipped (relay#1056)
| Capability | Status | File |
|---|---|---|
| node registration + run a node | ✅ `agent-relay fleet serve <def>` | `relay/packages/cli/.../commands/fleet.ts` |
| capability advertisement (`spawn:claude/codex`) | ✅ `defineNode({ capabilities })` | `relay/packages/fleet/src/index.ts` |
| spawn on a connected broker | ✅ `agent.create` action | `relay/packages/harness-driver/src/actions.ts` |
| node roster + capability discovery | ✅ `nodes.list({ capability })` | `relay/packages/sdk/src/messaging/types.ts` |
| durable bounded mailbox (offline→drain) | ✅ `deliveries.ack/fail/defer`, TTL | `relay/packages/sdk/src/messaging/types.ts` |
| broker↔cloud control connection | ✅ relaycast WS | `relay-broker/src/relaycast_ws.rs` |

### The genuinely net-new work (all code, no infra)
1. **The seam:** make proactive-runtime's dispatch target **pluggable** — Daytona sandbox *or* a fleet node (emit relaycast `agent.create` to the user's node). Today it only spawns Daytona. **This is the crux.**
2. **Host the triage/merge brain** (`@agent-relay/factory`) inside proactive-runtime; wire label → `scope`.
3. **Linear webhook ingress** (`/api/v1/webhooks/linear`) parallel to the existing GitHub route + Linear watch-match config.
4. **Finish the fleet ~10%:** broker outbound **heartbeat/liveness**, **reconnect inventory sync**, **node-targeted placement** (target by node name, not least-loaded), resumable `session_ref`.
5. **`agent-relay factory start`** = thin wrapper over `fleet serve` that ships a prebuilt factory node-def + reads `NodeConfig`.

---

## 7. Design decisions & open questions

1. **Where does merge execution run?** Recommend **cloud** (it has the verdict + GitHub auth via relayfile) rather than local `gh`. Confirm.
2. **Trust / consent model.** Cloud will instruct a user's laptop to spawn processes that write code and run shell. The node is the trust boundary: accepts placements only for its workspace, capabilities explicitly advertised, user opted in by running the command. The node's broker is the token authority for agents it spawns (#1056 §4). Define the consent + scope surface.
3. **Laptop sleeps mid-implementation.** A dead broker brings no agents back (#1056 §7) — the spawn is lost and reschedules on reconnect. Acceptable for v1 *because* in-flight state is cloud-durable, so the issue is re-dispatched, not orphaned. Confirm the reschedule semantics.
4. **Workflow shape definition.** What is a "workflow" concretely — a named cloud workflow (`ctx.workflow.run`), or a factory-defined step sequence? Pick one and reuse cloud's existing execution path.
5. **Multiple nodes per workspace** (laptop + desktop). v1: pick any live eligible node. Confirm we don't need stickiness.
6. **Dispatch-target abstraction shape.** Define the pluggable interface so proactive-runtime can target `daytona | fleet-node` without forking the delivery path.

---

## 8. Phased plan

**Phase 0 — unblock** ✅ *Already satisfied (see §6).* relayfile→cloud webhook delivery exists; no verification/build needed. The only "unblock" is defining the pluggable dispatch-target interface (folds into Phase 2).

**Phase 1 — extract the package** (children p1–p4)
- [ ] p1: `StateStore` port + `InMemoryStateStore`; route `BatchTracker` / `InFlightRegistry` / clarification through it.
- [ ] p2: split `FactoryConfig` → `WorkspaceConfig` + `NodeConfig`.
- [ ] p3: tsc build output, drop `private`, rename bin `fleet` → `factory`.
- [ ] p4: move to `AgentWorkforce/factory`, publish `@agent-relay/factory@0.1.0`, pear consumes it.

**Phase 1.5 — Pear teardown**

The factory brain leaving Pear means deleting the Electron daemon model and reducing Pear to an optional, read-only view of cloud state.

*Delete (the local-daemon process model — superseded by the cloud brain):*
- [ ] `src/main/factory-manager.ts` — the `FactoryManager` class + `factoryManager` singleton. It `spawn()`s a child Node process running the daemon (~line 175), reads loop heartbeat/liveness, tracks the in-flight registry. Moves to cloud.
- [ ] The `pear factory <action>` passthrough in `bin/pear.mjs` (forwards verbatim to `packages/factory-sdk/bin/fleet.mjs`, e.g. `pear factory start`, `pear factory reap-orphans`). The daemon + external reaper no longer run as a local Node process.
- [ ] On-disk daemon artifacts: loop heartbeat file, in-flight registry file, `/tmp/factory-run/` working dir.

*Rewire (control panel → read-only cloud view):*
- [ ] `src/shared/types/ipc.ts` — the `factory` IPC namespace (`status / start / stop / readConfig / saveConfig / onEvent` + `FactoryStatus`, `FactoryAgentStatus`, `FactoryLogLine`, `FactoryEvent`, `FactoryConfigReadResult`). Drop `start`/`stop`; repoint `status`/`onEvent` at cloud state; scope `readConfig`/`saveConfig` to **NodeConfig** only.
- [ ] `src/renderer/src/components/factory/FactoryPage.tsx` — convert control panel → read-only cloud-backed status view (or remove if the cloud web UI subsumes it).
- [ ] Renderer touchpoints: `App.tsx` (route), `stores/ui-store.ts`, `components/common/AppTopBar.tsx`, `components/common/CommandMenu.tsx`, `components/settings/AccountSettings.tsx`, `lib/ipc-mock.ts`.

*Keep:* nothing factory-specific remains in Pear's main process — Pear becomes a consumer of `@agent-relay/factory` types + a viewer of cloud state.

**Phase 2 — lift the brain to cloud**
- [ ] Host watch → triage → state machine → merge-gate inside the **existing** proactive-runtime, multi-tenant by `workspaceId`.
- [ ] `StateStore` cloud impl backed by the **existing** Postgres tables (not new storage).
- [ ] Widen `TriageDecision.scope` to `single | workflow | team`; wire label → scope.
- [ ] Add `/api/v1/webhooks/linear` ingress + Linear watch-match config.
- [ ] Define + implement the **pluggable dispatch-target** interface (`daytona | fleet-node`).

**Phase 3 — fleet slice (relay#1056) — mostly built, finish the last ~10%**
- [ ] Wire the dispatch target to emit relaycast `agent.create` onto the user's node (the seam).
- [ ] Broker outbound heartbeat/liveness reporting.
- [ ] Reconnect inventory sync (re-announce live agents for reconciliation).
- [ ] Node-targeted placement (target by node name / "self"); reject-and-reconcile unmapped repos.
- [ ] Implement `RelayFleetClient` (replaces the stub) wrapping `agent.create`.
- [ ] (Reuse as-is: capability advertisement, node roster, durable mailbox — already shipped.)

**Phase 4 — the node definition** (command stays `agent-relay fleet serve`, no rename)
- [ ] Ship a prebuilt **factory node-definition** that `agent-relay fleet serve <factory-node-def>` runs: reads `NodeConfig`, advertises capabilities, bridges placement → local spawn → streams results up.
- [ ] Pear optional desktop view reads the same cloud state.

---

## 9. Acceptance criteria

- A user runs `agent-relay fleet serve <factory-node-def>` with only a `NodeConfig`; nothing else is provisioned.
- Labeling a Linear issue `agent:single` / `agent:workflow` / `agent:team` spawns the corresponding shape **on the user's machine**, in the correct local checkout.
- With no shape label, triage infers the shape; low-confidence issues route to Slack for clarification before spawning.
- Closing the laptop mid-watch queues incoming events; reopening drains them; no issue is orphaned.
- Triage, merge-gate, and in-flight state run in cloud and survive a node restart.
- The factory no longer requires Pear/Electron to run.
- No new datastore, Lambda, API Gateway, or standalone always-on worker was introduced (reuse only).

---

## 10. Non-goals (v1)

- Full multi-node least-loaded scheduling, migration, capability negotiation (defer to full #1056 §10).
- Replacing cloud's existing team/workflow execution paths — the factory should *emit specs into them*, not fork them.
- Removing Pear's desktop view — it becomes an optional consumer, not the host.
- Multi-cloud (AWS Lambda/DynamoDB) handlers from the original #347 draft — unnecessary; host in cloud's proactive-runtime.
