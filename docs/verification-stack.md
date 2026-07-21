# Verification-stack descriptor

Repositories declare their complete verification environment in
`.factory/verification-stack.yaml`. Factory validates the file, deploys it into
an isolated environment, waits for every declared service probe, runs seed
steps, and returns local HTTP endpoint URLs for E2E and load stages.

The published JSON Schema is available as
`@agent-relay/factory/verification-stack.schema.json`.

```yaml
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: my-app
source:
  type: manifests # helm, kustomize, manifests, or docker-compose
  paths: [deploy/stack.yaml]
secrets:
  - name: database
    data:
      PASSWORD:
        ref: resource://production-like/database-password
services:
  - name: api
    workload:
      kind: deployment
    readiness:
      type: http
      port: 8080
      path: /health
      timeoutSeconds: 120
seeds:
  - type: exec
    name: seed-api
    service: api
    command: [node, scripts/seed.js]
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

Secret and config entries contain only opaque references. The caller supplies a
`VerificationStackReferenceResolver`; a missing required reference aborts before
the stack source is applied. Inline values are rejected by the typed loader.

`resolveVerificationStackDescriptor({ repoPath, ref?, descriptorPath? })`
selects the default descriptor or a repository-relative override. Passing `ref`
loads the descriptor committed at that branch or SHA instead of the working
tree version. On deployment, Factory materializes stack assets from that same
commit so a dirty or differently checked-out working tree cannot change the
selected stack.

The optional `verification` section declares the E2E command, load profile,
environment lease, and run/teardown limits used by Factory's required merge
gate. The descriptor/deployer APIs may be used without it; the merge gate fails
closed when the section or all exposed endpoints are absent.

`VerificationStackDeployer.deploy(descriptor, environment)` supports local or
OCI/HTTP Helm charts, kustomize directories, raw Kubernetes manifests, and
Docker Compose through `kompose`. Every source is rendered before it reaches
the cluster and passes the same namespace, RBAC, network, secret, pod-security,
and cluster-scope policy checks. Helm is rendered with `helm template`; Factory
does not let Helm or repository code apply resources directly. It waits for Deployment, StatefulSet, or
DaemonSet rollout plus each HTTP, TCP, or exec probe. All waits and seed steps
have descriptor-bounded timeouts. The returned deployment owns any local
port-forward processes; call `dispose()` before destroying the environment.
