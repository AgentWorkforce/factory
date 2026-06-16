# Factory P10 RelayFleetClient Self-Reflection

## Changed files

- `src/fleet/relay-fleet-client.ts`
  - Replaced the relay#1056 stub with a thin relay fleet protocol client.
  - Added an injectable `RelayFleetTransport` and default HTTP/WebSocket transport over Relaycast control routes.
  - Implemented `spawn`, `resume`, `release`, `roster`, `sendMessage`, `waitForInjected`, event listeners, and `dispose`.
  - `spawn` emits action `spawn` with `{ capability, name, agent, session_ref?, workflow?, inputs?, ... }`, supplies an invocation id, polls `pending/dispatched/invoked -> completed`, and maps completion output to `SpawnResult`.
  - Ordinary cloud spawns do not target placement: incoming `node: "self"` is omitted for `spawn`; explicit node names are preserved. `resume` preserves the requested node because resume is origin-targeted spawn plus `session_ref`.
  - `release` invokes action `release` and observes terminal invocation status.
  - Event mapping covers delivery failures, agent messages, and agent exit/offline events.
- `src/fleet/relay-fleet-client.test.ts`
  - Added focused tests for spawn mapping, explicit target handling, resume, release, `workflow:run`, node-loss reschedule observation with one invocation id, roster mapping, and event callbacks.
- `src/fleet/create-fleet.test.ts`
  - Updated the relay backend test to expect a real `RelayFleetClient` instead of the removed stub behavior.
- `src/ports/fleet.ts`
  - Documented the `workflow:run` contract: Phase 4 node-side handler shells out to `relayflows run <workflow>` in the node checkout; factory only emits the capability/workflow/inputs.

## Spec coverage

- Acceptance 1: Implemented every `FleetClient` method that was previously throwing `relay#1056`; no method now throws the stub error.
- Acceptance 2: `SpawnInput.capability` already included `workflow:run`; preserved it and documented the node-side handler contract.
- Acceptance 3: `RelayFleetClient.spawn` does not perform factory-side placement. It emits a fleet spawn action and omits `node: "self"` so Relaycast chooses an eligible node. Explicit node names are still passed through.
- Acceptance 4: Spawn/release/resume observe action invocation lifecycle by polling `getInvocation` until terminal status; completion output is returned upward as `SpawnResult`.
- Acceptance 5: Node-loss reschedule is fleet-side. The client keeps polling the same invocation id and does not reinvoke; covered by a test with dispatched alpha -> pending/dispatched beta -> completed.
- Acceptance 6: `workflow:run` is explicitly documented as shelling out to `relayflows run <workflow>` on the node side. Factory-side implementation emits the workflow path and inputs through the same spawn action.

## Tests and proofs run

- `npx vitest run src/fleet/relay-fleet-client.test.ts src/fleet/create-fleet.test.ts`
  - Passed: 2 files, 11 tests.
- `npm run build`
  - Passed.
- `npm test`
  - Failed one stale test outside the declared edit scope: `src/cli/fleet.test.ts` still expects the old text `RelayFleetClient not implemented`.
  - Actual behavior proves the stub is gone: relay backend attempts a relay request and returned `Relay fleet request failed (404): Route not found` in this environment.

## Repo-rule alignment

- Edits stayed within declared source targets (`src/fleet`, `src/ports/fleet.ts`) plus the required `.workflow-artifacts/factory-p10-fleet-client/self-reflection.md`.
- Did not edit package metadata or unrelated runtime/config files.
- Did not use any MCP/subagent spawning tools.
- Did not add factory-side scheduling, placement, Daytona branching, or broker-direct `InternalFleetClient` behavior.

## Remaining risks

- The default HTTP transport sends both `invocation_id` and `invocationId` with action invocation. This matches the required client-side contract, but older relay engines may ignore caller-supplied invocation ids until the relay protocol server accepts them.
- Full e2e capture with live nodes, Linear writeback, and real node-loss reschedule was not possible from this isolated worktree. The unit proof covers the client-side contract; live protocol verification remains environment-dependent.
- The stale CLI test should be updated in a follow-up change outside this issue's declared file targets to assert the relay backend is real rather than a stub.
