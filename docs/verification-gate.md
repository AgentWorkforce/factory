# Verification merge gate

Factory's `on-green-with-review` merge policy requires two independent green
decisions: GitHub checks/review and a live-stack verification verdict. The
verification gate is enabled by default and reads
`.factory/verification-stack.yaml` from the feature worktree.
Before provisioning, it verifies that the worktree's Git `HEAD` is the exact
reviewed PR head passed by the GitHub merge gate. A stale or mismatched checkout
or a checkout with uncommitted files fails closed without creating an
environment.

```yaml
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: my-app
source:
  type: manifests
  paths: [deploy/kubernetes.yaml]
services:
  - name: api
    workload: { kind: deployment }
    readiness:
      type: http
      port: 8080
      path: /health
      timeoutSeconds: 120
endpoints:
  - name: api
    service: api
    port: 8080
    path: /health
verification:
  environmentTtlSeconds: 900
  e2e:
    image: node:22.17.0-alpine3.22@sha256:fc3e945f920b7e3000cd1af86c4ae406ec70c72f328b667baf0f3a8910d69eed
    command: npm
    args: [run, test:e2e]
    timeoutSeconds: 300
  load:
    profile: .factory/load.yaml
    timeoutSeconds: 300
  overallTimeoutSeconds: 900
  teardownTimeoutSeconds: 120
```

Factory archives the exact reviewed Git `HEAD` and executes the declared E2E
command in a short-lived, non-root pod inside the leased namespace. The pod has
a read-only root filesystem, no service-account token, no inherited Factory
host environment or credentials, and only the descriptor's explicit `e2e.env`
entries. Both E2E and k6 images must be pinned by OCI sha256 digest.

The E2E pod receives `FACTORY_ENVIRONMENT_ID`,
`FACTORY_ENVIRONMENT_NAMESPACE`, and one cluster-routable
`FACTORY_ENDPOINT_<NAME>` / `FACTORY_INTERNAL_ENDPOINT_<NAME>` variable per
endpoint. A host-only port-forward URL is exposed separately as
`FACTORY_EXTERNAL_ENDPOINT_<NAME>` and must not be used from the pod. In-cluster
load jobs use the same service DNS names.

The deployment portion is the shared repository-owned descriptor documented in
`docs/verification-stack.md`; the `verification` section makes that stack
eligible for the required merge gate. Factory creates a leased, managed
namespace, caps active environments, runs readiness/E2E/load, evaluates the
verdict, closes deployment tunnels, and deletes the namespace on every return.
Expired leases can be reclaimed with `reapFactoryEnvironmentsOnce()` or the
recurring `FactoryEnvironmentReaper` after a hard process kill.

Every PR is eligible to merge only after the exact reviewed head passes this
sequence: clean-head attestation, descriptor validation, isolated provisioning,
safe render-and-apply, readiness, repository E2E, load SLO evaluation, teardown,
and the existing GitHub checks/review gate. Green-path, E2E-red, load-red,
timeout, hard-kill reaping, and telemetry assertions run against a real kind +
Calico cluster in CI. The structured artifacts are bound to the reviewed head;
any head change invalidates the prior result and reruns the matrix.

Each completed run emits a `verification.completed` event through the normal
Cloud reporter. Its bounded evidence includes the environment id, stage
statuses, E2E exit result, load measurements/SLO violations, timeout flag, and
teardown result; command output and raw errors are deliberately excluded from
Cloud telemetry.

Run the real-cluster acceptance scenario with:

```bash
kind create cluster --name factory-gate-e2e \
  --config test/fixtures/kubernetes/kind-config.yaml
bash scripts/install-calico.sh
npm run test:e2e:verification
kind delete cluster --name factory-gate-e2e
```
