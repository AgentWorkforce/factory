# Pull-request end-to-end verification

Software Garden changes are merged and published only from a tested Git object. The required `package`
check is the enforcement point: it builds and tests the checkout, installs the resulting npm
tarball into a clean consumer, runs the packaged CLI, and drives the public hosted control plane
through a complete lifecycle.

## Required evidence for every pull request

The `package` check must pass on the current PR head after it is brought up to date with `main`.
Its uploaded `factory-e2e-<head-sha>` artifact must attest all of these checks:

1. The PR head SHA and tested checkout SHA are recorded explicitly.
2. The packed npm tarball installs in a clean consumer and its public root and `./hosted` exports load.
3. The packaged CLI starts and renders help.
4. A ready issue is discovered, triaged, dispatched to an implementer and reviewer, and written back.
5. Repeated discovery does not duplicate fleet spawns or writeback.
6. Terminal completions reconcile into a ready merge gate and one completion writeback.
7. Replayed terminal completion remains idempotent.
8. A competing control-plane host is fenced by the shared owner/epoch lease.

The same command runs again in the publish workflow before versioning or npm publication. A
package can therefore never be published from a commit that did not pass the packed lifecycle.

## Risk tiers beyond the universal gate

The packed lifecycle is the minimum, not a substitute for environment-specific proof:

- **Tier 1 — package boundary (all PRs):** required `package` check and SHA-bound attestation.
- **Tier 2 — adapter/preview:** changes to GitHub, Relayfile, Relaycast, Cloud, or browser adapters must
  exercise the changed adapter in a disposable preview or provider sandbox and link the run in the PR.
- **Tier 3 — live canary:** lifecycle, lease, mount, dispatch, babysitter, merge, or release changes must
  run a disposable `[factory-e2e]` issue from ready discovery through PR creation, review-feedback
  handling, green checks, merge, issue closure, and teardown. Record issue, PR, run, and cleanup links.
- **Tier 4 — load/failure injection:** concurrency, recovery, or durability changes must additionally
  demonstrate the relevant SLO under forced restart, stale lease, duplicate delivery, mount loss, or
  broker disconnect.

Tier 2–4 evidence is head-specific. Any code change after the evidence invalidates it and requires a
rerun. A documentation-only change may mark higher tiers not applicable with a concrete rationale;
the universal Tier 1 gate still runs.

## Merge procedure

1. Resolve every actionable review thread.
2. Bring the branch up to date with `main` so the tested merge surface includes the latest gate.
3. Wait for the required `package` check and inspect its attestation artifact.
4. Verify the PR records any applicable Tier 2–4 evidence and cleanup receipts.
5. Merge the exact tested head. Do not push additional commits between verification and merge.
6. For a release, run the publish workflow from the merged commit; its repeated E2E gate must pass
   before npm publication is allowed.

Issues #141–#146 track the deeper disposable environment, load harness, and automated live-canary
infrastructure. Until those tiers are automated, their evidence is an explicit reviewer obligation;
Tier 1 is automated and fail-closed today.
