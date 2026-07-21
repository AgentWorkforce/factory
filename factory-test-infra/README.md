# factory-test-infra handoff

This directory is a source-control-safe handoff note, not the environment data
plane. `@agent-relay/factory` is a library and must not own Cloudflare account
resources or operate verification workloads.

An organization owner must create the private
`AgentWorkforce/factory-test-infra` repository. That repository is expected to
own:

```text
infra/                         Terraform/SST account resources
containers/                    bounded Container service definitions
workers/dispatcher/            dynamic Workers for Platforms dispatch Worker
workers/reaper/                Cron Trigger and leak alerting
templates/                     Worker/Container + D1/KV/R2/Queue/Hyperdrive inputs
ci/verification-env/up.sh      Miniflare or real environment provision
ci/verification-env/run-suite.sh
ci/verification-env/down.sh    identity-checked teardown
.github/workflows/verification.yml
```

Its linked runtime resource must expose `accountId`, `apiToken`, and optionally
`dispatcherUrlTemplate`; see `docs/cloudflare-environment-provider.md`. Factory
configuration stores only the resource name and guardrail policy.

The CI scripts are a strict contract. `run-suite.sh` must make isolation,
Container quota, elapsed-time/cost budget, max-concurrency, orphan reaping, and
k6 load assertions fail loudly if a guardrail is removed. `down.sh` must be
idempotent and verify the namespace's sentinel Worker identity before deletion.
`.github/workflows/cloudflare-verification.yml` in this repository invokes that
contract against both Miniflare and a real Cloudflare account.

`kubernetes-connections.example.yaml` remains the optional escape-hatch provider
configuration for stacks Cloudflare Containers cannot faithfully run. It is not
the default verification substrate and contains no credentials.
