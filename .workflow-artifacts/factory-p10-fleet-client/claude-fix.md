# Fix log — factory p10 (RelayFleetClient seam)

Addressed all four findings from `claude-review.md` (2 blocking, 2 minor). Source and tests updated; relevant suites rerun clean in a token-stripped environment.

---

## Finding 1 (BLOCKING) — Relay client threw at construction with no token → `createFleet({ backend: 'relay' })` red in clean env

**Fix (source):** Made `RelayFleetClient` construction side-effect-free by building the default `HttpRelayFleetTransport` lazily on first network use instead of in the constructor.
- `src/fleet/relay-fleet-client.ts`:
  - Store the incoming `options` (`#options`) and keep `#transport` as `RelayFleetTransport | undefined`. The constructor now only assigns `options.transport` (which may be `undefined`); it no longer calls `new HttpRelayFleetTransport(...)`.
  - Added `#getTransport()` which lazily instantiates `HttpRelayFleetTransport` (`this.#transport ??= new HttpRelayFleetTransport(this.#options)`) on first use. The token validation still lives in the transport's constructor, so it now fires only when a method actually touches the network.
  - Routed every transport access (`spawn`, `resume`, `release`, `roster`, `sendMessage`, `waitForInjected`, `#awaitInvocation`, `#ensureEventSubscription`) through `#getTransport()`.
  - `dispose()` uses `this.#transport?.dispose?.()` so it never constructs a transport just to tear one down.
- Net effect: constructing a `RelayFleetClient` (and `createFleet({ backend: 'relay' })`) never throws merely because no token is configured; the token error surfaces on the first transport call.

**Fix (tests):** `src/fleet/create-fleet.test.ts`
- Replaced the env-dependent test with two deterministic ones that strip `RELAY_AGENT_TOKEN` / `RELAY_WORKSPACE_KEY` / `RELAY_API_KEY` via `vi.stubEnv(..., '')` (no reliance on host env), plus `afterEach(() => vi.unstubAllEnvs())`:
  - `constructs the relay fleet client without throwing even when no token env is configured` — asserts construction does not throw and returns a `RelayFleetClient`.
  - `surfaces the relay token error on first transport use, not at construction` — asserts `fleet.roster()` rejects with `/requires agentToken or workspaceKey/`.

## Finding 2 (BLOCKING) — Stale CLI test still asserted the removed stub message

**Fix:** `src/cli/fleet.test.ts:286` — rewrote the test (renamed `drives the real RelayFleetClient when --backend relay is requested`, "stub" removed). It strips the relay credentials via `vi.stubEnv` (deterministic, no real network), runs `roster --backend relay`, and asserts:
- `code === 1`,
- stderr contains `requires agentToken or workspaceKey` (the real relay auth error from the lazily-built transport, replacing the deleted `'RelayFleetClient not implemented'` string),
- stdout is empty.
`vi.unstubAllEnvs()` restores env in a `finally`.

## Finding 3 (MINOR) — No test for acceptance criterion 4 (capability mismatch → clean error)

**Fix (tests):** `src/fleet/relay-fleet-client.test.ts` — added two tests:
- `fails cleanly when the fleet denies a spawn for a capability mismatch` — queues an invocation that reaches `status: 'denied'` with `error: 'node cannot spawn:codex'` and asserts `fleet.spawn(...)` rejects with `/spawn invocation inv-denied denied: node cannot spawn:codex/` (action name + invocation id + relay error string).
- `fails cleanly when the fleet marks an invocation failed` — same shape for `status: 'failed'`.
No source change (the `#awaitInvocation` throw behavior was already correct).

## Finding 4 (MINOR) — `RelayFleetEvent` imported by the test but not exported (TS2724)

**Fix (source):** `src/fleet/relay-fleet-client.ts:10` — added `export` to `type RelayFleetEvent` (it is the parameter type of the public `RelayFleetTransport.onEvent`). `tsc -p tsconfig.json --noEmit` no longer reports TS2724.

---

## Commands run

- `npx tsc -p tsconfig.json --noEmit`
  - TS2724 for `RelayFleetEvent` is **gone**.
  - Remaining output: only the two **pre-existing** `TS2322` errors at `src/cli/fleet.test.ts` (the `createFleet` mock-signature vs. `FakeFleetClient` mismatch) that the review explicitly flagged as pre-existing / do-not-block. Not introduced by this PR.

- `env -u RELAY_AGENT_TOKEN -u RELAY_WORKSPACE_KEY -u RELAY_API_KEY npx vitest run src/fleet/relay-fleet-client.test.ts src/fleet/create-fleet.test.ts src/cli/fleet.test.ts`
  - **3 files passed, 37 tests passed** (clean, token-stripped env — reproduces what CI sees).

- `env -u RELAY_AGENT_TOKEN -u RELAY_WORKSPACE_KEY -u RELAY_API_KEY npx vitest run` (full suite)
  - **474 passed, 1 failed.** The single failure is `src/fleet/internal-fleet-client.test.ts:314` (`protectedPids()` expected `[68009]`, got `[34941, 68009]`).
  - **Environmental, not caused by this PR:** PID 34941 is the live `agent-relay-broker` process currently hosting this workflow session (verified with `ps -p 34941` → `agent-relay-broker init --instance-name factory-p10-fleet-client-...`). `protectedPids()` scans real OS processes and legitimately detected the running broker. This PR does not touch `internal-fleet-client.ts` (or its source); the test passes in CI where that broker is not running.

## Files changed

- `src/fleet/relay-fleet-client.ts` — lazy transport (Finding 1 source) + `export type RelayFleetEvent` (Finding 4).
- `src/fleet/create-fleet.test.ts` — deterministic no-token construction + first-use error tests (Finding 1 tests).
- `src/fleet/relay-fleet-client.test.ts` — denied/failed capability-mismatch tests (Finding 3).
- `src/cli/fleet.test.ts` — updated relay-backend test, stub assertion removed (Finding 2).
