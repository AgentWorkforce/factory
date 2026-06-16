# @agent-relay/factory

Autonomous Linear-issue → triage → PR factory. **This is currently a placeholder
reservation** — the real package (orchestrator, triage engine, GitHub merge-gate,
fleet client, StateStore, config) is extracted here from `pear/packages/factory-sdk`
by the **p4** workflow in the factory build-out stack.

See the planning + epic under [`planning/`](./planning/) (especially
`factory-cloud-watches-local-node-linear-issue.md`).

## Publishing

Releases use npm **OIDC trusted-publisher provenance** (no `NPM_TOKEN`), via
`.github/workflows/publish.yml` — `npm publish --provenance` under
`id-token: write`. Register this repo/workflow as a trusted publisher for
`@agent-relay/factory` on npmjs.com before the first CI publish.

**Reserve the name now (manual, no provenance — that's fine for a reservation):**
```bash
cd factory
npm publish --access public      # publishes 0.0.0, reserves @agent-relay/factory
```
Then the p4 extraction lands the real code and the **first real release is 0.1.0**
via the publish workflow (`workflow_dispatch` → `custom_version: 0.1.0`), with
provenance.

> When p4 extracts `factory-sdk` into this repo, it must **preserve** this
> `package.json`'s `repository` + `publishConfig` fields and
> `.github/workflows/publish.yml`, and replace the placeholder `index.js` /
> `index.d.ts` with the real package entrypoints.
