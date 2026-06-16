Title: [factory] extract: move @agent-relay/factory into AgentWorkforce/factory repo, publish to npm public, switch pear to consume it

Team: AR
Suggested status: Ready for Agent
Repo label: factory  (NEW repo label — add to `factory.config.json` `repos.byLabel`)
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)

---

## Context

Final PR (#4) of the factory extraction Phase 1 stack. Hard depends on PRs #1, #2, #3 landing first. After this lands, the new repo `AgentWorkforce/factory` publishes `@agent-relay/factory` to npm public, pear consumes the published package, and Phase 2 (cloud lift) can begin.

The destination repo already exists on both ends (verified 2026-06-16): GitHub `AgentWorkforce/factory` is created (PUBLIC), and the local clone `/Users/khaliqgant/Projects/AgentWorkforce/factory` already has `origin` wired. So repo creation + `git remote add` are NOT part of this PR — go straight to seeding history.

**Scaffold already present (reconcile, don't clobber):** the repo has been seeded with `planning/` (this issue stack), a placeholder `package.json` (v0.0.0 — name `@agent-relay/factory`, with `repository` + `publishConfig`), stub `index.js`/`index.d.ts`/`README.md`, and `.github/workflows/publish.yml` (OIDC provenance publish, mirrors workforce). `@agent-relay/factory@0.0.0` may already be published to reserve the name. When seeding the real package: **keep** `package.json`'s `repository`/`publishConfig`, **keep** `publish.yml` + `planning/`, **replace** the stub `index.*`/`README.md` with the real entrypoints, and ship the **first real release as 0.1.0** via the publish workflow.

This PR has two halves that must land atomically (or near-atomically with a flag):

A. **Extract** — seed the new repo with `packages/factory-sdk/` history preserved via `git filter-repo --subdirectory-filter`.
B. **Switch** — pear's workspace removes `packages/factory-sdk`, updates its 2 consumers to import from the published `@agent-relay/factory` dep.

## Goal

`AgentWorkforce/factory` exists with full history, npm-publishes `@agent-relay/factory@0.1.0`, and pear consumes the published version.

## Steps (detailed playbook for the picking-up agent)

### Step 1 — Verify prerequisites

- Confirm PRs #1, #2, #3 are merged on pear `main`.
- Confirm `packages/factory-sdk` builds clean: `npm run build -w @agent-relay/factory && npm test -w @agent-relay/factory`.
- Confirm `npm pack --dry-run -w @agent-relay/factory` lists only `dist/`, `bin/`, `package.json`, `README.md`.
- Confirm the destination directory exists: `/Users/khaliqgant/Projects/AgentWorkforce/factory/.git` initialized, no commits yet.

### Step 2 — Extract with history

Working from a sibling worktree of pear:

```bash
git clone /Users/khaliqgant/Projects/AgentWorkforce/pear /tmp/pear-extract
cd /tmp/pear-extract
git filter-repo --subdirectory-filter packages/factory-sdk
# now /tmp/pear-extract contains only factory-sdk history with paths rewritten to repo root
```

Push to the destination. `origin` is already wired and the repo is empty, but the local clone has an **untracked `planning/`** dir (this issue stack) — commit it first so it's preserved, then merge in the filtered history:

```bash
cd /Users/khaliqgant/Projects/AgentWorkforce/factory
# origin already = git@github.com:AgentWorkforce/factory.git (no `git remote add` needed)
git add planning && git commit -m "docs: factory epic + issue stack"
git pull /tmp/pear-extract main --allow-unrelated-histories   # merges filtered factory-sdk history alongside planning/
git push -u origin main
```

(If you'd rather keep history linear, `git pull --rebase` the extracted history under the planning commit, or seed the package first and add `planning/` after — either ordering is fine since they touch disjoint paths.)

### Step 3 — Set up the new repo's tooling

In `AgentWorkforce/factory`:

- Add `.github/workflows/ci.yml` — node 20, `npm ci`, `npm run build`, `npm test`, `npm pack --dry-run` to validate publish shape on every PR.
- Add `.github/workflows/publish.yml` — on `main` push or tagged release, `npm publish --access public` using an `NPM_TOKEN` secret. Use changesets or a manual version bump strategy — match `@agent-relay/cloud`'s existing convention (read its repo to mirror).
- Add `LICENSE`, `README.md` (extract the relevant prose from the epic doc as a starting point), `CONTRIBUTING.md` mirroring the org pattern.
- Add `.gitignore` covering `dist/`, `node_modules/`, `*.tsbuildinfo`, `.DS_Store`, `*.log`.
- Confirm `tsconfig.json` + `tsconfig.build.json` produce clean output in the extracted repo with no path-rewrites needed.

### Step 4 — First publish

- Tag `v0.1.0` in the new repo.
- Publish to npm: `npm publish --access public`.
- Verify on npm: `npm view @agent-relay/factory` returns 0.1.0.

### Step 5 — Pear consumes the published version

In `pear`, on a new branch:

- Remove the `packages/factory-sdk/` directory entirely.
- Update `package.json` workspaces array to no longer include `packages/factory-sdk`.
- Add `@agent-relay/factory: ^0.1.0` to pear's `dependencies` (NOT `devDependencies` — `factory-manager.ts` imports from it at runtime).
- Update the two consumers (`src/main/factory-manager.ts`, `bin/pear.mjs`) — their imports were already from `@pear/factory-sdk` in PR #3; just rename to `@agent-relay/factory`. (If PR #3 already did this rename, the consumers are no-ops in this step.)
- Update `factory.config.json` `repos.byLabel` — add `"factory": "AgentWorkforce/factory"` so the factory itself can dispatch implementers into the new repo.
- `npm install`, `npm run build`, `npm test`.
- Smoke-test `pear factory start --mode live` — must be byte-identical behavior to pre-extraction.

### Step 6 — Cleanup

- Delete `pear/packages/factory-sdk/` (already removed in step 5 but double-check no stragglers like `dist/` that might have been gitignored).
- Update pear's README / docs if they reference `@pear/factory-sdk`.

## End-to-end verification

1. `AgentWorkforce/factory` repo exists, has the full extracted history (verify by `git log --oneline | head -20` includes commits authored in pear under `packages/factory-sdk/`).
2. CI green on the new repo.
3. `npm view @agent-relay/factory@0.1.0` shows the package.
4. Pear builds clean after the swap; `pear factory start --mode live --config ./factory.config.json` runs against `rw_7ccfea89` with no observable behavior change.
5. The factory itself (running on this PR's branch) can dispatch implementers into `AgentWorkforce/factory` — file a `[factory]` Linear issue with label `factory`, confirm an implementer agent is spawned with the `factory` worktree path. (This is the closing-the-loop demonstration that the extraction succeeded.)

## Acceptance criteria

1. `AgentWorkforce/factory` repo exists on GitHub with full history-preserved import.
2. CI + publish workflows green.
3. `@agent-relay/factory@0.1.0` published on npm public.
4. Pear's `packages/factory-sdk/` is gone; pear consumes `@agent-relay/factory@^0.1.0` from npm.
5. `pear factory ...` commands behave identically.
6. `factory.config.json` `repos.byLabel` has a `factory` entry pointing at the new repo.
7. A demonstration `[factory]`-labeled Linear issue dispatches an implementer into the new repo's worktree, closing the loop.

## Out of scope

- Phase 1.5 (Pear teardown — delete `factory-manager.ts`, IPC namespace cleanup, FactoryPage rewrite). Separate PR per the epic.
- Phase 2 (cloud lift). Lands after this PR + #1.5.
- Cloud / relay CLI consumers — they start importing `@agent-relay/factory` in Phases 2 / 4.

## Risks + mitigations

- **filter-repo path collisions** if pear has any files at the same paths as factory-sdk subdirs (unlikely — `packages/factory-sdk/src/...` → root after filter, no root-level pear file should conflict). Mitigation: dry-run filter-repo in a scratch clone first.
- **Workspace dep resolution** during the swap window — pear's `packages/factory-sdk/` is deleted in the same commit that adds the npm dep. Mitigation: do the swap atomically in a single commit so CI never sees the missing-dep state.
- **Bin script paths in pear scripts** — `pear factory:start` etc reference `node_modules/.bin/factory` post-swap (was `packages/factory-sdk/bin/fleet.mjs`). PR #3 already renamed; just verify the path resolution lands.
- **Running factory daemon mid-swap** — kill any local `pear factory start` instances before deploying the swap, or accept a restart. Document in the PR description.

## Related

- Epic: `factory-cloud-watches-local-node-linear-issue.md` §4, §8 (Phase 1).
- Sibling PR #1: `linear-issue-factory-extract-p1-state-store-port.md`.
- Sibling PR #2: `linear-issue-factory-extract-p2-config-split.md`.
- Sibling PR #3: `linear-issue-factory-extract-p3-publish-prep.md`.
- Pear local destination: `/Users/khaliqgant/Projects/AgentWorkforce/factory` (already git-init'd, ready to receive extract).
- GitHub repo: `AgentWorkforce/factory` — already created (PUBLIC, empty as of 2026-06-16), local `origin` already wired.
