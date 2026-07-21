#!/usr/bin/env bash
set -euo pipefail

cluster_name="${KIND_CLUSTER_NAME:-customer-eks-sim}"
kubeconfig_file="${KUBECONFIG:-}"
calico_version="${CALICO_VERSION:-v3.32.1}"
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
  kubectl create --filename \
    "https://raw.githubusercontent.com/projectcalico/calico/${calico_version}/manifests/calico.yaml"
  kubectl --namespace kube-system rollout status daemonset/calico-node --timeout=300s
  kubectl wait nodes --all --for=condition=Ready --timeout=300s
fi

npx tsx test/e2e/kubernetes-provider.e2e.ts
