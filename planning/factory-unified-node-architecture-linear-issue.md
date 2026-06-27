Title: [factory] EPIC v2 — unified-node architecture: factory as a spec-emitter into the relay fleet

Team: AR
Suggested status: Design / Epic
Repo label: pear (spans pear, cloud, relay, factory, agents)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Related: relay/specs/fleet-delivery.md (RFC, source of truth), AgentWorkforce/relay#1056 (RFC mirror), AgentWorkforce/pear#347
Supersedes: factory-cloud-watches-local-node-linear-issue.md (v1 — archived; v1's cloud/local split is wrong, see §1)

---

## 1. What changed from v1 (read this first)

v1 (`factory-cloud-watches-local-node-linear-issue.md`) anchored on a **cloud-watches / local-node-executes** split. Per Will's RFC (`relay/specs/fleet-delivery.md`, 2026-06-06) that split is **architecturally wrong**:

- **RFC §2 — two planes only:** a messaging fabric (agents as peers) and a compute layer (nodes). There is no cloud-vs-local distinction at the architecture layer.
- **RFC §6 — "Spawn is a node capability,"** expressed through the action system (`actions.register('create', handler → driver.spawn)`); placement is targeted (`node: <name>|"self"`) or **least-loaded(eligible)**.
- A **node is any machine that runs agents** — Daytona sandbox, the operator's laptop, a mac mini, an EC2 box are the *same primitive*, each advertising capabilities (`spawn:claude`, `spawn:codex`, and by extension `workflow:run`).

**Therefore the factory is a spec-emitter.** It produces N `spawn` invocations into the relay fleet and observes their lifecycle. It does **not** execute anything, does **not** choose where work runs, and has **no `local | cloud-sandbox` execution mode**. The `single | workflow | team` shapes become **recipes over one spawn primitive** (§3, §3.5).

**Invariant from v1 (do not relitigate):** the package extraction (§4) and the config split (§5) — both still correct, the split is *more* correct under unified-node. Phase 1 (extract) and Phase 1.5 (Pear teardown) are unchanged. Phases 2/3/4 are reframed below.

---

## 2. Architecture (unified node)

```
                         RELAY FLEET (messaging fabric + nodes — RFC §2)
                         ┌───────────────────────────────────────────────┐
 CLOUD (the factory brain, multi-tenant by workspaceId)                   │
   relayfile push: /linear /github  ──▶ watch → triage → recipe           │
        │                                         │                       │
        │                                emit spawn invocations           │
        │                                (RFC §6 action: create)          │
        ▼                                         ▼                       │
   durable StateStore (Cloudflare DO)     Relaycast placement ────────────┤  least-loaded(eligible)
   (in-flight, batch, clarification)              │                       │  or targeted node
                                                  ▼                       │
                              ┌───────────────────┴───────────────────┐   │
                              ▼                   ▼                    ▼   │
                          node: laptop      node: mac-mini     node: Daytona (ephemeral,
                          spawn:claude       spawn:codex        autospawned if no eligible
                          workflow:run       …                  node + workspace permits)
                              └────────── run agent / workflow ─────────┘  │
                                                  │                       │
                              completion (RFC §7 invocationId lifecycle) ──┘
                                                  ▼
                                cloud merge-gate + Linear/Slack writeback
```

**Split of responsibility**

| Concern | Owner | Notes |
|---|---|---|
| Watch (`/linear`, `/github`), triage, recipe selection, batch/in-flight state, merge-gate, writeback | **Cloud factory brain** | multi-tenant by workspaceId; durable StateStore |
| Turning a recipe into spawn invocations | **Cloud factory brain** | emits `spawn{capability,…}`; never executes |
| Placement (which node runs a spawn) | **Relaycast** (RFC §6) | targeted or least-loaded; factory is unaware |
| Ephemeral node autospawn (Daytona) when no eligible node | **Relaycast + workspace permission** | factory does not branch on it |
| Running the agent / workflow | **Whatever node won placement** | Daytona / laptop / mac-mini / EC2 — same primitive |
| Reliable delivery, idempotency, reconcile | **Relay fleet** (RFC §7) | `invocationId`, first-to-`completed` wins |
| Node registration + capability advertisement | **The node** (a relay broker in a config; Phase 4) | `spawn:claude`, `spawn:codex`, `workflow:run` |

There is no factory-side execution code and no factory-side placement code. The factory's only fleet-facing surface is a **thin client of the fleet protocol** (§6, Phase 3).

---

## 3. Label → recipe → spawn flow

A Linear issue's label selects a **recipe**; the recipe expands to a **set of spawn invocations** the factory emits into the fleet. Personas come from `AgentWorkforce/agents/<name>/persona.ts` in all cases. Node capabilities advertised in the fleet are matched against the spawn's `capability` during placement (RFC §6).

- **Explicit shape labels win:** `agent:single` / `agent:workflow` / `agent:team`.
- **No shape label → tiered inference** (heuristic → LLM) picks the recipe; low confidence routes to Slack clarification before emitting any spawn.
- **Repo labels** (`cloud`, `relayfile`, `pear`, …) select roster + placement target per AR-272 (see §3.5 and the AR-272 amendment).

The recipe is one knob with three expansions — not three execution paths. The factory emits the spawn-set and tracks the `invocationId` lifecycle; the fleet does the rest.

---

## 3.5 Recipe → spawn-set mapping

| Recipe (label) | Spawn-set emitted | Capability | Persona / workflow source | Roster from repo labels |
|---|---|---|---|---|
| `agent:single` | **1** spawn `{ capability: 'spawn:claude' (or per-persona harness), persona: <X>, node?: <repo-label placement>, session_ref? }` | `spawn:claude` / `spawn:codex` | `agents/<persona>/persona.ts` | ignored for count (always 1); repo label still informs placement (which checkout/node) |
| `agent:workflow` | **1** spawn `{ capability: 'workflow:run', workflow: '<path>.{yaml,ts,py}', inputs }` — the node invokes the Relayflows SDK in-process, which may emit further **child spawns** | `workflow:run` | workflow file defines its own roster; personas referenced by its steps resolve from `agents/` | ignored for roster; repo labels become workflow inputs |
| `agent:team` | **N** implementer spawns + **1** reviewer spawn + roster metadata. This is the logic that today lives in `cloud/.../teams/spawn-team.ts`, reconstructed as a recipe over the spawn primitive | `spawn:claude` / `spawn:codex` per member | `agents/cloud-team-implementer/persona.ts`, `agents/cloud-team-reviewer/persona.ts` | **one implementer per repo label** (capped at 4 per AR-272); reviewer naming unchanged |

Concrete example (today's AR-267 team): labels `cloud`, `relayfile`, `agent:team` → emit `spawn{spawn:claude, persona: cloud-team-implementer, node-target via cloud checkout}`, `spawn{… relayfile checkout}`, and `spawn{spawn:claude, persona: cloud-team-reviewer}`. Placement, execution, and completion are all fleet-side.

---

## 3.6 Source-agnostic input — issues are the convergence point

The factory watches Linear issues; it does not care how an issue came to exist. Multiple source systems all converge on the same thing — a recipe-labeled Linear issue — and from there the factory's spec-emitter path (§3, §3.5) is identical regardless of origin:

| Source | Path to a recipe-labeled Linear issue |
|---|---|
| Linear webhooks | a `[factory]` issue is authored / labeled directly |
| GitHub events | a `factory`-labeled GitHub issue is mirrored into Linear |
| Slack triggers | a Slack message escalates into a filed issue |
| **Audio transcripts (relayscribe / meeting-actions)** | a recorded meeting/brainstorm is transcribed, then `meeting-actions` extracts action items and files recipe-labeled issues — see Deliverable 8 (`linear-issue-meeting-actions-fleet-recipe-multilang.md`) |

The unified-node model makes this clean: each source is just a producer of Linear issues with shape labels; the factory neither knows nor branches on the source. Audio transcripts are one such input source among several, with the added wrinkle that they carry a `source_language` the implementer prompt should preserve (Deliverable 8 §5).

---

## 4. Package extraction (PRESERVED from v1 — choices locked, do not relitigate)

The factory moves out of `pear/packages/factory-sdk` into its own published package, **`@agent-relay/factory`** (GitHub **`AgentWorkforce/factory`**). Locked choices:

- npm scope + name: `@agent-relay/factory`, published **public**.
- Bin name: `factory` (renamed from `fleet`).
- History preservation: `git filter-repo --subdirectory-filter packages/factory-sdk`.
- Local dest dir: `/Users/khaliqgant/Projects/AgentWorkforce/factory` (git-init'd; placeholder `0.0.0` already published to reserve the name).

Three consumers across three repos: **cloud** (the brain — triage/merge/state), the **`agent-relay` CLI** (node registration — Phase 4), **pear** (optional read-only viewer). Tracked by children p1–p4. Audit (2026-06-16): 67 TS files, ~21.7K LOC, zero pear-internal imports.

---

## 5. Config split — workspace config vs node-registration config (PRESERVED, reframed)

Split `FactoryConfig` into two schemas (child p2; builds on pear#368 dynamic Linear states + pear#369 compact `repos`):

- **WorkspaceConfig** — cloud orchestration policy: subscription, repos identity (`org`/`names`/`overrides`/`byLabel`/`default`), batchSize, mergePolicy, safety, slack, Linear states (`linear.states`/`statesByTeam`/`stateIds`). Lives in **cloud durable storage** (Phase 2).
- **NodeConfig** (reframed from "node-local config" → **node-registration config**): this machine registers into the fleet as a node with these **capabilities** (`spawn:claude`/`spawn:codex`/`workflow:run`) and these **repo checkout paths** (`cloneRoot`/`clonePaths`, pear#369 compact form). Lives on disk on the node. Consumed by Phase 4 node registration.

The reframe makes the split *cleaner*: node-local config is no longer "where the factory executes" — it's "how this machine advertises itself to the fleet."

---

## 6. The factory's fleet surface — a thin client, not a "minimal slice"

v1 called relay#1056 "a minimal slice the factory needs." Under unified-node, **relay#1056 / the RFC IS the entire mechanism.** The factory ships exactly one fleet-facing component: a `RelayFleetClient` that **consumes** the fleet protocol (RFC §6 spawn action, §7 invocation lifecycle, §9 control surface) — it does not fork or reimplement any fleet logic. Today's `RelayFleetClient` is a stub that throws `relay#1056`; Phase 3 makes it a real (thin) protocol client. There is no factory-side placement, scheduling, or execution code.

---

## 7. Phased plan

**Phase 0.5 — prerequisites (pear#368 + pear#369, both LANDED).** Dynamic per-team Linear states + compact `repos` config. (Detail in p2.)

**Phase 1 — extract `@agent-relay/factory`** (children p1–p4). INVARIANT — code hygiene, architecture-independent.
- p1 StateStore port + InMemory · p2 config split · p3 publish-prep · p4 extraction.

**Phase 1.5 — Pear teardown** (`linear-issue-factory-phase-1-5-pear-teardown.md`). INVARIANT — delete the in-Pear daemon.

**Phase 2 — cloud lift** (`linear-issue-factory-phase-2-cloud-lift.md`). Factory brain (triage, batch/in-flight state, merge-gate) moves into the cloud worker, multi-tenant by workspaceId, durable StateStore (Cloudflare DO). Emits spawn invocations into the fleet. Cloud's `spawn-team.ts` becomes the `team` recipe construction layer. **No execution code.**

**Phase 3 — consume the fleet contract** (`linear-issue-factory-phase-3-fleet-client.md`). Real `RelayFleetClient` over the RFC protocol. Acceptance: an `agent:single` spawn from cloud triage round-trips the fleet, lands on whichever eligible node is live, completion observed via the invocation lifecycle, written back to Linear.

**Phase 4 — node registration** (`linear-issue-factory-phase-4-node-registration.md`). `agent-relay local factory` = "register this machine as a node with these capabilities + repo paths." Effectively a relay broker started in a config (NodeConfig from p2). No orchestration logic — that's all in cloud (Phase 2).

**Sibling — proactive-runtime fleet unification** (`linear-issue-cloud-proactive-runtime-fleet-unification.md`). `team-launch-n1.ts` stops hardcoding Daytona; proactive spawns use the same spawn primitive. Same principle, different cloud system.

---

## 8. Open questions (surface to operator)

1. **`workflow:run` capability handler shape.** Decided: the node embeds the Relayflows runtime through `@relayflows/core`; it does not depend on a globally installed CLI. The node already has the harness + repo checkout, and child spawns ride the same fleet.
2. **Single-recipe in cloud.** Cloud has no single-agent path today (only team via `spawn-team.ts`, proactive via `team-launch-n1`). `agent:single` is just a 1-spawn recipe — confirm it needs nothing beyond team-recipe at N=1.
3. **Multi-node placement preference.** Laptop + mac-mini both advertise `spawn:claude` — RFC §6 says least-loaded. Good enough for v1? (Assumed yes.)
4. **Persona discovery single source.** Both cloud (team-recipe construction) and the node-side workflow runtime must read `AgentWorkforce/agents/`. Confirm both point at the same registry.

---

## 9. Non-goals (v1 §10 rewritten)

- **No factory-side execution.** The factory never runs an agent or a workflow; it emits spawns.
- **No factory-side placement/scheduling.** Relaycast owns placement (RFC §6). The factory does not pick nodes, does not branch on Daytona-vs-laptop, does not implement least-loaded.
- **No `local | cloud` mode flag.** Removed entirely — there are only nodes.
- **No reimplementation of fleet semantics.** The factory consumes relay#1056; it does not fork spawn/placement/reconcile.
- **Not moving personas.** They stay in `AgentWorkforce/agents/`; both recipes and workflows read from there.

---

## 10. Related

- **RFC (source of truth):** `relay/specs/fleet-delivery.md` §2 (two planes), §6 (spawn & placement), §7 (reliable invocation), §9 (node lifecycle). Mirror: relay#1056.
- **Children:** p1–p4 (Phase 1), Phase 1.5 / 2 / 3 / 4 issues, proactive-runtime unification, AR-272 amendment (labels select recipe + roster).
- **Filed bug context:** AR-268 (Nango→relayfile fanout), AR-269 (Slack gate), AR-270 (GitHub path mismatch), AR-271 (IntegrationFanout).
- **Memory:** `relayfile-deploy-topology` (cloud hosts the relayfile worker), `relayfile-sdk permanent polling fallback`.
- **Supersedes:** `factory-cloud-watches-local-node-linear-issue.md` (v1).
