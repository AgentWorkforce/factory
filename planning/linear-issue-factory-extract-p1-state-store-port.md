Title: [factory] pear: extract Phase 1 prep #1 — define StateStore port + InMemoryStateStore, route BatchTracker / InFlightRegistry / clarification through it

Team: AR
Suggested status: Ready for Agent
Repo label: pear
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)

---

## Context

This is **prep PR #1 of 4** for the factory extraction epic (`factory-cloud-watches-local-node-linear-issue.md`). The end goal is to publish `@agent-relay/factory` from a new `AgentWorkforce/factory` repo (npm public, bin `factory`), with the factory brain liftable into the cloud worker per Phase 2 of the epic.

The package itself is already a clean island (audit 2026-06-16): 67 TS files, ~21.7K LOC, zero pear-internal imports, only published external deps (`@agent-relay/cloud`, `@agent-relay/harness-driver`, `@relayfile/sdk`, `zod`). Only two pear-side consumers (`src/main/factory-manager.ts`, `bin/pear.mjs`), both already slated for Phase 1.5 deletion.

The extraction itself is mostly mechanical; **three prep refactors** in pear make the published shape correct on first publish so we never have to re-extract:

1. **This PR** — `StateStore` port + InMemory impl, route state machine through it. Required because cloud-lift (Phase 2) needs to swap in a Durable Object / Dynamo impl without touching the orchestrator.
2. **PR #2** — Split `FactoryConfig` into workspace + node-local (epic §5).
3. **PR #3** — Build output, drop `private`, rename bin `fleet` → `factory`.
4. **PR #4 (EX)** — `git filter-repo` into `AgentWorkforce/factory`, npm publish setup, pear consumes published.

## Problem

The factory's state machine (in-flight tracking, batch tracking, clarification-pending issues) is in-process and accessed directly by `FactoryLoop`. Without a port abstraction, the Phase 2 cloud lift either has to fork the orchestrator code or carry the entire in-memory machine into the worker. Either is bad.

Concretely today (`packages/factory-sdk/src/orchestrator/factory.ts`):

- `#batch: BatchTracker` (constructed inline at `:258`) holds per-issue dispatch attempts, backoff state, cooldowns.
- `#criticalMessages`, `#resumeInFlight`, `#resumedExitKeys`, `#slackThreadIds`, `#dispatchAttempts`, `#canonicalIssueStates`, `#dispatchFailureReaperHandoffs`, `#seenAgentQuestionKeys`, etc — all `Map`/`Set` instances on the loop itself.
- The clarification-pending state lives behind `AgentNeedsInput` markers + the `#seenAgentQuestionKeys` order ring.

None of this survives a process restart. For cloud, we need it durable, scoped by `workspaceId`, and queryable across workers.

## Goal

Introduce a `StateStore` port that captures all factory state that needs to be durable. Provide an `InMemoryStateStore` impl that preserves today's behavior exactly. Wire the orchestrator to read/write through the port. No behavior change.

This PR is a refactor, not a feature. Cloud-side durable impls land in Phase 2; this PR makes that swap a one-line change.

## Design

### 1. New port: `src/ports/state.ts`

```ts
export interface StateStore {
  // Batch / in-flight tracking
  getBatch(workspaceId: string): Promise<BatchSnapshot>
  recordDispatchAttempt(workspaceId: string, issueKey: string, attempt: DispatchAttemptState): Promise<void>
  getDispatchAttempts(workspaceId: string, issueKey: string): Promise<DispatchAttemptState | undefined>
  releaseInFlight(workspaceId: string, issueKey: string): Promise<void>

  // Critical messages / resume
  recordCritical(workspaceId: string, key: string, value: CriticalRecord): Promise<void>
  consumeCritical(workspaceId: string, key: string): Promise<CriticalRecord | undefined>
  isResumed(workspaceId: string, exitKey: string): Promise<boolean>
  markResumed(workspaceId: string, exitKey: string): Promise<void>

  // Slack thread association
  setSlackThread(workspaceId: string, issueKey: string, threadId: string): Promise<void>
  getSlackThread(workspaceId: string, issueKey: string): Promise<string | undefined>

  // Question dedupe (clarification)
  seenAgentQuestion(workspaceId: string, key: string): Promise<boolean>
  markAgentQuestion(workspaceId: string, key: string): Promise<void>  // applies LRU cap internally

  // Dispatch-failure reaper handoffs
  recordFailureHandoff(workspaceId: string, key: string, handoff: RegistryHandoffAgent): Promise<void>
  getFailureHandoff(workspaceId: string, key: string): Promise<RegistryHandoffAgent | undefined>
  clearFailureHandoff(workspaceId: string, key: string): Promise<void>

  // Canonical issue state cache
  recordCanonicalState(workspaceId: string, key: string, stateId: string): Promise<void>
  getCanonicalState(workspaceId: string, key: string): Promise<string | undefined>
}
```

Workspace-scoping every method now (even though `InMemoryStateStore` only ever sees one workspace) is the whole point — the cloud impl uses it as the partition key.

All methods async even for the in-memory impl, because the cloud impl will be I/O-bound and we can't have the orchestrator's hot path go sync→async later.

### 2. `src/state/in-memory-state-store.ts`

Wraps the existing `BatchTracker`, plus the `Map`/`Set` instances currently on `FactoryLoop`. Preserves capacity caps (e.g. `AGENT_QUESTION_DEDUPE_LIMIT`) exactly.

### 3. Wire into `FactoryPorts`

```ts
interface FactoryPorts {
  mount: MountClient
  fleet: FleetClient
  // ... existing
  stateStore?: StateStore  // defaults to new InMemoryStateStore(config.batchSize)
}
```

`createFactory` constructs the default if not provided. The constant `LIVE_DEDUPE_LIMIT`, `AGENT_QUESTION_DEDUPE_LIMIT` etc move into the InMemory impl as construction params.

### 4. Orchestrator refactor

Replace every direct `this.#batch.*` / `this.#xMap.set` / `this.#xMap.get` access in `factory.ts` with the corresponding `this.#state.method()` call. Adds `await` where today's path is sync. Tests must still pass with no test logic changes — only mock setup adjusts.

`BatchTracker` itself stays as a class; it just becomes an implementation detail of `InMemoryStateStore`.

### 5. Tests

- All existing `factory.test.ts` cases pass with `InMemoryStateStore`.
- New `state/in-memory-state-store.test.ts` covers LRU caps and per-workspace partition correctness.
- New port conformance test that any future `StateStore` impl must pass.

## End-to-end verification

1. `npm test -w @pear/factory-sdk` — full suite green with no changes to test bodies (only setup mocks).
2. Run `pear factory start --mode live --config ./factory.config.json` against `rw_7ccfea89` — observable behavior identical to pre-PR (dispatch, in-flight tracking, slack thread association, question dedupe). Capture before/after metric snapshots.
3. Re-dispatch path verified: move a `Done` issue back to `Ready for Agent` and confirm the factory re-engages (state.ts:1452 logic must still see the canonical state map).

## Acceptance criteria

1. `src/ports/state.ts` exports `StateStore` interface with the methods listed in §1.
2. `src/state/in-memory-state-store.ts` implements it, wrapping `BatchTracker` + the maps formerly on `FactoryLoop`.
3. Every direct state access in `factory.ts` now goes through `this.#state.*`. No `Map`/`Set` instances remain on `FactoryLoop` for state that the port now owns.
4. `FactoryPorts.stateStore` is optional; default constructed in `createFactory`.
5. All existing tests pass without behavior-test changes (only mock setup).
6. New conformance test ensures any future `StateStore` impl is shape-correct.
7. E2E run against `rw_7ccfea89` shows identical factory behavior pre- and post-PR.

## Out of scope

- Durable persistence impls (DurableObject / Dynamo) — Phase 2.
- Splitting config — PR #2 in this stack.
- Build output / publish prep — PR #3.
- Extraction itself — PR #4.

## Related

- Epic: `factory-cloud-watches-local-node-linear-issue.md` §2 (target architecture), §4 (extract `@agent-relay/factory`).
- `pear/packages/factory-sdk/src/orchestrator/factory.ts` — orchestrator refactor target.
- `pear/packages/factory-sdk/src/orchestrator/batch-tracker.ts` (or wherever `BatchTracker` lives) — wraps inside InMemory impl.
- `pear/packages/factory-sdk/src/ports/{fleet,mount,system,writeback}.ts` — existing port pattern to mirror.
- Sibling: `linear-issue-factory-extract-p2-config-split.md` (next in stack).
- Sibling: `linear-issue-factory-extract-p3-publish-prep.md` (next in stack).
- Sibling: `linear-issue-factory-extract-p4-extraction.md` (final).
