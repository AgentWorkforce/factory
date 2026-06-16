VERDICT: ISSUES_FOUND

## Findings

1. **Relay resume leaks the local `self` sentinel as a fleet node target**

   Severity: High

   Files:
   - `src/fleet/relay-fleet-client.ts:129`
   - `src/fleet/relay-fleet-client.ts:415`
   - `src/orchestrator/factory.ts:2129`

   `RelayFleetClient.spawn()` intentionally calls `spawnActionInput(..., { includeSelfNode: false })`, so the factory's local `node: 'self'` sentinel is removed before emitting a relay spawn. That matches the p10 invariant that normal factory spawns do not target a node and placement is fleet-side.

   `RelayFleetClient.resume()` uses `{ includeSelfNode: true }`, so a resume input with `node: 'self'` is serialized into the relay action payload as `node: 'self'`. The orchestrator passes exactly that default during resume/restart (`tracked.spec.node ?? 'self'`). Under the relay backend, cloud/factory has no fleet node literally named `self`; this is an internal/local backend sentinel. A real relay resume can therefore be mis-targeted to a non-existent node named `self` instead of letting the fleet resolve the original/session-owning node or omitting the node when the origin is unknown.

   This also leaves a test gap: `src/fleet/relay-fleet-client.test.ts` covers resume with an explicit `origin-node`, but not the default `self` path used by the orchestrator.

   Recommended fix:
   - Do not forward `node: 'self'` from `RelayFleetClient.resume()` to the relay fleet payload. Treat it like `spawn()` does, or only include `node` for an actual fleet node name.
   - Add a unit test that `fleet.resume({ sessionRef, node: 'self' })` emits `session_ref` but does not include `node`.
   - If relay requires origin-targeted resume, persist/return the actual dispatched fleet node id/name in the spawn result or tracked metadata, then pass that concrete node on resume instead of using `self`.

## Verification

- Read the p10 spec and epic fleet-surface rules.
- Reviewed the changed files and untracked relay client test.
- Ran `npm run build` successfully.
- Ran focused p10 tests successfully: `npx vitest run src/fleet/relay-fleet-client.test.ts src/fleet/create-fleet.test.ts src/cli/fleet.test.ts`.
- Ran full test suite successfully: `npm test` (`29` files, `475` tests).

## Summary

Created `.workflow-artifacts/factory-p10-fleet-client/codex-review.md` with one actionable finding. Build and all tests pass, but the relay resume path can still emit the local-only `self` node sentinel to the fleet protocol and should be fixed before accepting p10.
