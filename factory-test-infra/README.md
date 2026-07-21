# Factory Kubernetes test infrastructure

`kubernetes-connections.example.yaml` is the source-control-safe shape for the
customer-to-cluster registry. Real credentials belong in the configured secret
manager. Only their references belong in Factory config.

The provider creates the namespace RBAC, quota, limits, Pod Security labels, and
NetworkPolicies for every run; there is no mutable shared base chart to drift.
Managed EKS should provide a dedicated, tainted verification node group matching
the example selector/toleration and must run a NetworkPolicy-enforcing CNI.

The shared managed connection is a fallback, not an EKS-fidelity claim. Its
mandatory caveat is surfaced on every resulting `Environment`.
