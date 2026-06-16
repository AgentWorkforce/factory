VERDICT: FIXED

## Findings Addressed

1. **Relay resume leaks the local `self` sentinel as a fleet node target**

   Status: Fixed

   Files changed:
   - `src/fleet/relay-fleet-client.ts`
   - `src/fleet/relay-fleet-client.test.ts`

   Fix:
   - `RelayFleetClient.resume()` now builds its relay spawn payload with the same `self` filtering used by `spawn()`.
   - Concrete fleet node targets are still preserved for resume, so `node: 'origin-node'` remains serialized as `node: 'origin-node'`.
   - The local-only factory sentinel `node: 'self'` is no longer forwarded to the relay action payload. The payload still includes `session_ref`, so relay can resume by session reference without trying to place work on a non-existent fleet node named `self`.

   Proof added:
   - Added a regression test in `src/fleet/relay-fleet-client.test.ts` covering `fleet.resume({ sessionRef, node: 'self' })`.
   - The test asserts the relay action payload includes `session_ref` and does not include `node`.

## Verification

- `npm test -- --run src/fleet/relay-fleet-client.test.ts src/fleet/create-fleet.test.ts src/cli/fleet.test.ts`
  - Passed: 3 files, 38 tests.
- `npm run build`
  - Passed.
- `npm test`
  - Passed: 29 files, 476 tests.

## Summary

Fixed the valid Codex review finding by preventing relay resume from leaking the local `self` node sentinel into the fleet protocol. Added focused regression coverage and reran focused tests, TypeScript build, and the full test suite successfully.
