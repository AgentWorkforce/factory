Title: [factory] pear: extract Phase 1 prep #3 — tsc build output, drop `private`, rename bin `fleet` → `factory`

Team: AR
Suggested status: Ready for Agent
Repo label: pear
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)

---

## Context

Prep PR #3 of 4 for the factory extraction epic. Stacks after PR #1 (state store) and PR #2 (config split), but technically independent — the only conflict surface is the package.json/tsconfig/bin files. Best to land last in the prep trio so the published shape reflects the final API.

This PR makes the package publishable as `@agent-relay/factory` on npm public. After this lands, the actual extraction PR (#4) is purely a file move + a new repo.

## Goal

1. Build the package with `tsc` into `dist/` so external consumers (cloud, relay CLI) can `import` it without resolving raw TS.
2. Drop `private: true` so the package can be published.
3. Rename the bin `fleet` → `factory`.
4. Set up the package metadata for an npm public publish: scope `@agent-relay`, target name `@agent-relay/factory`, public access.

Pear continues to consume the local workspace package until the extraction PR swaps the dep for the published version. No behavior change in pear.

## Design

### 1. Build output

Current `package.json` exports raw TS:

```jsonc
"exports": {
  ".": { "types": "./src/index.ts", "default": "./src/index.ts" },
  "./testing": { "types": "./src/testing/index.ts", "default": "./src/testing/index.ts" },
  "./writeback": { "types": "./src/writeback/index.ts", "default": "./src/writeback/index.ts" }
}
```

After:

```jsonc
"exports": {
  ".":          { "types": "./dist/index.d.ts",           "import": "./dist/index.js" },
  "./testing":  { "types": "./dist/testing/index.d.ts",   "import": "./dist/testing/index.js" },
  "./writeback":{ "types": "./dist/writeback/index.d.ts", "import": "./dist/writeback/index.js" }
}
```

Add a `tsconfig.build.json` that produces ESM `dist/` with declarations:

```jsonc
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "noEmit": false
  },
  "include": ["src/**/*"],
  "exclude": ["**/*.test.ts", "**/__tests__/**", "src/testing/**/*.test.ts"]
}
```

Add `"build": "tsc -p tsconfig.build.json"` to package.json scripts. The pear workspace `npm run build` already runs sub-workspace builds (or extend if not); confirm `dist/` is regenerated on every pear build.

The package is currently `type: "commonjs"`. Switch to `type: "module"` since cloud + relay-CLI consumers are all ESM. The bin's on-the-fly esbuild bundling (`bin/fleet.mjs` lines 27–40 — required because of the cjs/esm mismatch) can go away — the published dist is ESM directly, so the bin can `import` it.

### 2. Drop `private`

Remove `"private": true` from `package.json`. Add `"publishConfig": { "access": "public" }` so `npm publish` works without an extra flag.

Add `"files": ["dist", "bin", "package.json", "README.md", "LICENSE"]` to control what gets published. Do NOT publish `src/` to npm — consumers should never resolve raw TS.

### 3. Rename bin `fleet` → `factory`

```jsonc
"bin": {
  "factory": "bin/factory.mjs"
}
```

Rename `bin/fleet.mjs` → `bin/factory.mjs`. Update the entry to point at the new build output: `dist/cli/factory.js` (was `src/cli/fleet.ts` via runtime esbuild).

Pear's `bin/pear.mjs` currently passes `pear factory ...` through to the bin via path resolution. Update that resolution to `bin/factory.mjs` (or remove entirely if Phase 1.5's teardown plan is already in flight — but Phase 1.5 is a separate PR; this PR keeps backwards compatibility).

The package's npm scripts that reference `fleet` (e.g. `pear factory:start` → `node packages/factory-sdk/bin/fleet.mjs factory start`) update to use the new bin path. Run a grep + replace across the repo root.

### 4. Package metadata

```jsonc
{
  "name": "@agent-relay/factory",
  "version": "0.1.0",
  "description": "Agent factory — triage, dispatch, merge-gate for relayfile-driven workspaces",
  "license": "UNLICENSED",  // or whatever the org default is
  "repository": {
    "type": "git",
    "url": "https://github.com/AgentWorkforce/factory"   // points to the future repo
  },
  "publishConfig": { "access": "public" },
  "engines": { "node": ">=20" },
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "files": [...],
  "bin": { "factory": "bin/factory.mjs" },
  "exports": {...},
  "scripts": {
    "build": "tsc -p tsconfig.build.json",
    "test": "vitest run",
    "lint": "..."
  },
  "dependencies": {
    "@agent-relay/cloud": "^x.y.z",
    "@agent-relay/harness-driver": "^x.y.z",
    "@relayfile/sdk": "^x.y.z",
    "zod": "^3.x.x"
  },
  "devDependencies": {
    "typescript": "^5.x",
    "vitest": "^x.y.z"
  }
}
```

Rename from `@pear/factory-sdk` → `@agent-relay/factory`. Inside pear, update the two consumers (`src/main/factory-manager.ts`, `bin/pear.mjs`) and the workspace package.json deps.

Workspace dep continues to work via npm workspaces or via `"@agent-relay/factory": "workspace:*"` — verify the rest of the repo resolves it.

### 5. CI / publish setup (deferred to PR #4)

This PR does NOT set up GitHub Actions publish or version bumping. Those land in PR #4 (the extraction) where the new repo is the publish source of truth. For now, the package is just `private: false` + buildable; nothing is actually published from pear.

## End-to-end verification

1. `npm run build -w @agent-relay/factory` produces `dist/` with `.js` + `.d.ts` + `.js.map` + `.d.ts.map` files matching every `src/*.ts` entry point.
2. `npm test -w @agent-relay/factory` — full suite green against the new ESM build.
3. `pear factory start --mode live --config ./factory.config.json` runs through the renamed bin (`bin/factory.mjs`) — observable behavior identical.
4. `npm pack -w @agent-relay/factory` produces a tarball. Inspect contents: `dist/`, `bin/`, `package.json`, `README.md` — no `src/`, no tests, no `tsconfig.json`.
5. `npm publish --dry-run -w @agent-relay/factory --access public` succeeds (does NOT actually publish — that's PR #4).

## Acceptance criteria

1. Package renamed `@pear/factory-sdk` → `@agent-relay/factory`. Workspace + the 2 pear consumers updated.
2. `type: "module"` with ESM-only published output.
3. `tsconfig.build.json` + `npm run build` produces `dist/` with .js + .d.ts.
4. `exports` map points at `dist/`. Raw TS no longer reachable from external consumers.
5. `private: true` removed. `publishConfig.access: "public"` added. `files` array controls publish contents.
6. Bin renamed `fleet` → `factory`. `bin/fleet.mjs` removed. The runtime esbuild bundling logic is gone — the bin just imports from `dist/`.
7. All scripts that referenced the old bin path updated.
8. `npm pack` dry-run + `npm publish --dry-run` both succeed.
9. Pear's `pear factory ...` commands continue to work (until Phase 1.5 deletes them — separate PR).

## Out of scope

- The actual git-history extraction into `AgentWorkforce/factory` — PR #4.
- GH Actions / changesets publish automation — PR #4.
- Cloud or relay CLI consuming the published package — Phase 2 / Phase 4 of the epic.
- Deleting Pear's `factory-manager.ts` / bin passthrough — Phase 1.5 (separate PR per the epic).

## Related

- Epic: `factory-cloud-watches-local-node-linear-issue.md` §4.
- `pear/packages/factory-sdk/package.json` — primary target.
- `pear/packages/factory-sdk/bin/fleet.mjs` — to be renamed + simplified.
- `pear/package.json` scripts that reference `bin/fleet.mjs`.
- Sibling PR #1: `linear-issue-factory-extract-p1-state-store-port.md`.
- Sibling PR #2: `linear-issue-factory-extract-p2-config-split.md`.
- Sibling PR #4: `linear-issue-factory-extract-p4-extraction.md`.
