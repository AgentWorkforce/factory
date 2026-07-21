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
provision:
  namespacePrefix: factory-verify
  ttl: 15m
deploy:
  manifests:
    - deploy/kubernetes.yaml
  readiness:
    - resource: deployment/api
      condition: Available
      timeout: 2m
  endpoints:
    api:
      service: api
      port: 8080
      scheme: http
      portForward: true
e2e:
  command: npm
  args: [run, test:e2e]
  timeout: 5m
load:
  profile: .factory/load.yaml
  timeout: 5m
timeouts:
  overall: 15m
  teardown: 2m
```

The E2E process receives `FACTORY_ENVIRONMENT_ID`,
`FACTORY_ENVIRONMENT_NAMESPACE`, and one `FACTORY_ENDPOINT_<NAME>` variable per
endpoint. In-cluster load jobs use the corresponding service DNS names.

Only namespaced manifest resources are accepted. Factory creates a leased,
managed namespace, caps active environments, runs the declared readiness/E2E
and load stages, evaluates the verdict, and deletes the namespace in `finally`.
Expired leases can be reclaimed with `reapFactoryEnvironmentsOnce()` or the
recurring `FactoryEnvironmentReaper` after a hard process kill.

Each completed run emits a `verification.completed` event through the normal
Cloud reporter. Its bounded evidence includes the environment id, stage
statuses, E2E exit result, load measurements/SLO violations, timeout flag, and
teardown result; command output and raw errors are deliberately excluded from
Cloud telemetry.

Run the real-cluster acceptance scenario with:

```bash
kind create cluster --name factory-gate-e2e
npm run test:e2e:verification
kind delete cluster --name factory-gate-e2e
```
