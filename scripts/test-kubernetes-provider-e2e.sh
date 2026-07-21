#!/usr/bin/env bash
set -euo pipefail

cluster_name="${KIND_CLUSTER_NAME:-customer-eks-sim}"
kubeconfig_file="${KUBECONFIG:-}"
kind_node_image="${KIND_NODE_IMAGE:-kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f}"
created_cluster=0
created_kubeconfig=0

cleanup() {
  if [[ "$created_cluster" == "1" ]]; then
    kind delete cluster --name "$cluster_name"
  fi
  if [[ "$created_kubeconfig" == "1" && -n "$kubeconfig_file" ]]; then
    rm -f "$kubeconfig_file"
  fi
}
trap cleanup EXIT

if [[ -z "$kubeconfig_file" ]]; then
  if ! kind get clusters | grep -Fxq "$cluster_name"; then
    # kindnet does not enforce NetworkPolicy. A cluster without a policy-aware
    # CNI would make the isolation assertion either fail or, worse, encourage a
    # future test to assert only that policy objects exist. Start without the
    # default CNI and install pinned Calico before deploying customer workloads.
    kind create cluster \
      --name "$cluster_name" \
      --image "$kind_node_image" \
      --config test/fixtures/kubernetes/kind-config.yaml
    created_cluster=1
  fi
  kubeconfig_file="$(mktemp)"
  created_kubeconfig=1
  kind get kubeconfig --name "$cluster_name" > "$kubeconfig_file"
fi

export KUBECONFIG="$kubeconfig_file"
export FACTORY_E2E_POSTGRES_PASSWORD="${FACTORY_E2E_POSTGRES_PASSWORD:-factory-e2e-not-production}"

if [[ "$created_cluster" == "1" ]]; then
  bash scripts/install-calico.sh
fi

npx tsx test/e2e/kubernetes-provider.e2e.ts
