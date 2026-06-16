# Fresh-eyes review — factory p10 (RelayFleetClient seam)

Scope reviewed: `src/fleet/relay-fleet-client.ts` (new impl), `src/fleet/relay-fleet-client.test.ts` (new), `src/fleet/create-fleet.test.ts` (modified), `src/ports/fleet.ts` (doc/comment), against the p10 spec (`planning/linear-issue-factory-fleet-p10-relayfleetclient-seam.md`) and the self-reflection. No `AGENTS.md`/`CLAUDE.md` exists in the repo.

Verdict: **CHANGES REQUIRED** — 2 blocking findings, 2 minor.

The implementation itself is solid: the stub is gone, spawn/resume/release/roster/messaging/events are mapped cleanly, and the `#awaitInvocation` polling correctly tolerates node-loss reschedule (dispatched→pending→dispatched→completed) without re-invoking. The problems are around constructability and the test surface, not the core protocol mapping.

---

## Finding 1 (BLOCKING) — Relay client throws at construction when no token env is set; the new test only passes because of an ambient `RELAY_API_KEY`

- File: `src/fleet/relay-fleet-client.ts:107` and `:275-280`
- The `RelayFleetClient` constructor eagerly builds `new HttpRelayFleetTransport(options)`, and that transport's constructor **throws** (`'RelayFleetClient requires agentToken or workspaceKey…'`) whenever none of `agentToken`/`workspaceKey`/`RELAY_AGENT_TOKEN`/`RELAY_WORKSPACE_KEY`/`RELAY_API_KEY` is present.
- Therefore `createFleet({ backend: 'relay' })` (`src/fleet/create-fleet.ts:19` → `new RelayFleetClient()`) throws **at construction time**, not on first use. This is a regression vs. the old stub, which was constructible and only threw `relay#1056` when a method was called.
- The new `create-fleet.test.ts` test (`returns the relay fleet client for the relay backend`) passed for the author only because `RELAY_API_KEY` is set in their shell. Reproduced failing in a clean env:

  ```
  $ env -u RELAY_AGENT_TOKEN -u RELAY_WORKSPACE_KEY -u RELAY_API_KEY npx vitest run src/fleet/create-fleet.test.ts
  × returns the relay fleet client for the relay backend
    Error: RelayFleetClient requires agentToken or workspaceKey (or RELAY_AGENT_TOKEN / RELAY_WORKSPACE_KEY)
      ❯ new HttpRelayFleetTransport src/fleet/relay-fleet-client.ts:279
      ❯ new RelayFleetClient   src/fleet/relay-fleet-client.ts:107
      ❯ createFleet            src/fleet/create-fleet.ts:20
  ```

  A test that passes only because of ambient host env is effectively broken; this will be red in CI.
- Required fix: make construction side-effect-free. Defer building the default transport (and the token check) to first network use — e.g. lazily instantiate `HttpRelayFleetTransport` on the first `invokeAction`/`listNodes`/`sendMessage` call, or move the token validation out of the constructor and into `#request`. Constructing a `RelayFleetClient` (and `createFleet({ backend: 'relay' })`) must never throw merely because no token is configured.
- Required test: in `create-fleet.test.ts` (or `relay-fleet-client.test.ts`), explicitly delete `RELAY_AGENT_TOKEN`/`RELAY_WORKSPACE_KEY`/`RELAY_API_KEY` (via `vi.stubEnv`/`delete process.env.*` in the test, not relying on the host) and assert `createFleet({ backend: 'relay' })` constructs a `RelayFleetClient` without throwing. Add a complementary test asserting the token error surfaces on the first transport call, not at construction.

## Finding 2 (BLOCKING) — Stale CLI test still asserts the removed stub and now fails

- File: `src/cli/fleet.test.ts:286-297` (`selects the RelayFleetClient stub when --backend relay is requested`)
- It still asserts `code === 1` and `errors.text()` contains `'RelayFleetClient not implemented'`. That stub message no longer exists (acceptance criterion 3: stub removed), so this test is now red. With Finding 1 unfixed it fails with a *different* error message; with Finding 1 fixed it still fails because the stub text is gone.
- This is the same fleet seam touched by this PR — leaving the suite red is in-scope, not a follow-up. The self-reflection explicitly defers it ("stale CLI test should be updated in a follow-up"); that is not acceptable since `npm test` is now red.
- Required fix: update `src/cli/fleet.test.ts:286-297` to assert the real relay backend behavior — e.g. that `roster --backend relay` drives `RelayFleetClient` and surfaces the relay request/auth error (or a configured-transport path), rather than the deleted `'RelayFleetClient not implemented'` string. Remove the word "stub" from the test name.
- Required test: this finding *is* the test fix; verify `npx vitest run src/cli/fleet.test.ts` is green afterward.

## Finding 3 (MINOR) — No test covers acceptance criterion 4 (capability mismatch → clean error)

- File: `src/fleet/relay-fleet-client.ts:231-233` (`#awaitInvocation` throws on `failed`/`denied`) — behavior exists, but spec acceptance criterion 4 ("Capability mismatch … fails cleanly with a clear error") has no test.
- The whole capability-mismatch path is fleet-side (relay returns `denied`/`failed`), which is a reasonable design choice, but it is currently unverified.
- Required fix: none in source (behavior is correct).
- Required test: in `relay-fleet-client.test.ts`, queue an invocation that reaches `status: 'denied'` (or `'failed'`) with `error: 'node cannot spawn:codex'` and assert `fleet.spawn(...)` rejects with a message that includes the action name, invocation id, and the relay error string.

## Finding 4 (MINOR) — `RelayFleetEvent` is imported by the test but not exported from the module

- File: `src/fleet/relay-fleet-client.ts:10` declares `type RelayFleetEvent` **without `export`**, yet `src/fleet/relay-fleet-client.test.ts:3` imports `type RelayFleetEvent` from it. `tsc -p tsconfig.json --noEmit` reports `TS2724: '"./relay-fleet-client"' has no exported member named 'RelayFleetEvent'`.
- It is masked today because `npm run build` uses `tsconfig.build.json`, which excludes `**/*.test.ts`, and `vitest` erases type-only imports — so neither gate catches it. It is still a real type error and the event type is part of the public `RelayFleetTransport.onEvent` surface.
- Required fix: add `export` to `type RelayFleetEvent` (line 10) — it is the parameter type of the exported `RelayFleetTransport.onEvent`, so it should be public anyway.
- Required test: none beyond restoring a clean `tsc --noEmit` over the test files; the export change makes the existing test's import resolve under typecheck.

---

## Notes / non-blocking observations

- `HttpRelayFleetTransport.invokeAction` sends the invocation id three ways (`invocation_id` + `invocationId` at the top level, plus `invocationId` inside `input` via `spawnActionInput`). Harmless, but redundant; the self-reflection already flags the dual top-level keys as a protocol-compatibility risk. Consider settling on one once the relay protocol server's accepted shape is known.
- Out-of-scope items (smart placement p12, broker reconnect p11, workflow/team fan-out) are correctly left alone; `workflow:run` is emitted and documented in `src/ports/fleet.ts:1-4`, matching the spec's "stub team to single-implementer / note it" allowance.
- Pre-existing (not introduced by this PR, do not block): `src/cli/fleet.test.ts:160,756` have `TS2322` errors from the `createFleet` mock signature vs. `FakeFleetClient`. They predate this change; flagging only so they aren't mistaken for new regressions.

---

## Summary

Reviewed the p10 RelayFleetClient implementation against its spec. Core protocol mapping is correct and the `relay#1056` stub is genuinely gone. Two blocking issues must be fixed in this PR: (1) the client throws at construction without a token, so `createFleet({ backend: 'relay' })` and the newly-added `create-fleet.test.ts` fail in any clean environment — confirmed by reproduction; the author's run only passed due to an ambient `RELAY_API_KEY`; (2) the stale `src/cli/fleet.test.ts` relay-stub test still asserts the removed `'RelayFleetClient not implemented'` message and is now red. Two minor items: add a capability-mismatch/denied-invocation test (acceptance criterion 4 is untested) and `export` the `RelayFleetEvent` type the new test imports (currently a `tsc` TS2724, hidden by the build's test exclusion).

Artifact produced: `.workflow-artifacts/factory-p10-fleet-client/claude-review.md`
