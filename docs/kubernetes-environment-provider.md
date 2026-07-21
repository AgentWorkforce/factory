# Kubernetes verification environments

`KubernetesEnvironmentProvider` deploys a repository-owned Helm, kustomize, or
manifest stack into a newly generated namespace. It supports two explicit
targets:

The published descriptor schema is
`@agent-relay/factory/kubernetes-environment-stack.schema.json`.

- `byoc` (the default) uses the customer's scoped EKS/kubeconfig connection.
  This is the high-fidelity path because Kubernetes version, CNI, IAM/IRSA,
  ingress, add-ons, and nodes are the customer's real substrate.
- `managed` uses Factory's shared verification cluster. Every managed
  environment carries this caveat in `bindings.kubernetes.fidelityCaveat`:
  our Kubernetes version, CNI, IAM/IRSA, ingress, add-ons, and node types may
  differ from the customer environment. A managed run must not be presented as
  proof of exact production fidelity.

Cluster credentials are never accepted inline. Configuration contains only an
opaque `credential.secretRef`; a `KubernetesCredentialResolver` exchanges that
reference for a scoped, short-lived kubeconfig path at runtime. The included
`env:` resolver is intended for CI, where the environment variable contains a
path to a secret-mounted kubeconfig—not kubeconfig content. An IRSA resolver
can return a kubeconfig whose exec credential uses the referenced role.

## Safety boundary

Provisioning uses Kubernetes `create` for the namespace. An existing namespace
is therefore a hard failure and is never adopted. Every environment receives:

- ownership, run identity, creation, and absolute TTL metadata;
- Pod Security Admission `restricted` enforcement;
- separate deployer and token-less workload service accounts, with a
  namespace-only deployer Role (no RBAC or NetworkPolicy mutation);
- ResourceQuota and LimitRange caps;
- default-deny ingress/egress, same-namespace and DNS allowances, explicit
  protected-namespace exclusions, and public egress that excludes private and
  link-local/metadata ranges;
- configured node selectors and tolerations for a dedicated verification pool,
  when the connection supplies them; hard quota remains the fallback;
- manifest checks that reject foreign namespaces, namespace resources,
  egress-widening policies, privilege-escalating RBAC, privileged/host-level
  pods, and cluster-scoped resources without dual opt-in plus a kind allowlist.

Opted-in cluster-scoped objects use create-only semantics, so provisioning
fails instead of adopting a customer object with the same identity. Their
identities are persisted on the namespace, ownership-labelled, and deleted in
reverse order before namespace teardown. Cluster roles cannot grant wildcard,
impersonation, secret, node, namespace, or RBAC-escalation access; cluster role
bindings may reference only a role from the same rendered stack and service
accounts in the generated namespace.

Deletion and TTL sweeping require the generated `factory-*` name, both
ownership labels, and the exact connection identity to agree. A namespace with
missing or mismatched identity is reported and left untouched. Reaping is
failure-isolated per connection, so an unavailable customer secret does not
prevent expired environments in other clusters from being reclaimed.

NetworkPolicy enforcement still depends on the cluster CNI. BYOC onboarding
must verify that the configured CNI enforces `networking.k8s.io/v1`
NetworkPolicy; the kind E2E asserts the policy behavior, not merely the object.

## Provider evaluation

The initial provider uses native namespaces plus the upstream `kubectl` and
Helm CLIs. This is integration glue, not a new scheduler or sandbox runtime.
It has no controller installed in customer clusters, preserves exact EKS/CNI/
IRSA behavior, and covers the common single-namespace chart case with the
smallest customer permission surface.

| Option | Fit | Decision |
| --- | --- | --- |
| [vCluster](https://www.vcluster.com/docs/vcluster/introduction/architecture/) / Loft | Strongest API/CRD/RBAC separation, quota policy, lifecycle, and optional dedicated-node isolation. It adds a per-preview control plane and syncer, and shared nodes are not a boundary for untrusted workloads. | Preferred future opt-in for charts that need multiple namespaces or allowlisted cluster-scoped APIs; not required in every BYOC cluster. |
| [Argo CD ApplicationSets](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/) | Excellent GitOps reconciliation and cluster/PR generation, but assumes Argo CD and cluster registration are already installed and does not itself supply the quota/network/TTL isolation boundary. | Optional deployment adapter for customers already operating Argo CD, not the environment lifecycle owner. |
| [Uffizzi](https://docs.uffizzi.com/) | Preview-environment and virtual-cluster lifecycle can reduce platform work, especially for managed infrastructure. It introduces another hosted/control-plane dependency and does not improve fidelity for a customer who grants only a scoped native namespace. | Re-evaluate for the managed multi-tenant offering; not the BYOC default. |

The choice is intentionally reversible: `EnvironmentProvider`,
`KubernetesClient`, credential resolution, and `StackDeployer` are separate
ports. A vCluster or Argo-backed client can be added without changing the gate.

## Run the binding E2E

With kind, kubectl, Helm, Docker, and Node installed:

```bash
npm run build && npm test
npm run test:e2e:kubernetes
```

The script creates `customer-eks-sim` when `KUBECONFIG` is unset, disables
kindnet, and installs pinned Calico so the NetworkPolicy check binds at the
packet layer. It cleans that cluster up on exit. If `KUBECONFIG` is already set,
it treats the cluster as externally owned, never changes its CNI, and never
deletes it. The test creates a separate production namespace, deploys the Helm
API+Postgres fixture, verifies a reachable port-forward URL, asserts RBAC,
NetworkPolicy, and ResourceQuota admission, runs the shared `runLoad` k6
harness through its SLO gate and writes structured evidence under
`artifacts/kubernetes-provider-e2e/`, destroys the live namespace, sweeps an
expired orphan, and proves the production namespace and workload identities
were never replaced.
