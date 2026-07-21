#!/usr/bin/env bash
set -euo pipefail

readonly calico_version="v3.32.1"
readonly calico_sha256="a1df919d9721cf667accdc3e72848911b0cb25cfab7d2478ad0c996302c95744"
manifest="$(mktemp)"
cleanup() {
  rm -f "$manifest"
}
trap cleanup EXIT

curl --fail --silent --show-error --location \
  --output "$manifest" \
  "https://raw.githubusercontent.com/projectcalico/calico/${calico_version}/manifests/calico.yaml"
printf '%s  %s\n' "$calico_sha256" "$manifest" | shasum -a 256 --check
kubectl apply --filename "$manifest"
kubectl --namespace kube-system rollout status daemonset/calico-node --timeout=300s
kubectl --namespace kube-system rollout status deployment/calico-kube-controllers --timeout=300s
kubectl wait nodes --all --for=condition=Ready --timeout=300s
