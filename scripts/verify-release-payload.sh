#!/usr/bin/env bash
set -euo pipefail

VERSION=${1:?usage: verify-release-payload.sh VERSION [LOCAL_PACKAGE_DIR]}
LOCAL_PACKAGE_DIR=${2:-$PWD}
PACKAGE_NAME=@agent-relay/factory
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir "$TMP_DIR/local" "$TMP_DIR/registry" "$TMP_DIR/local-x" "$TMP_DIR/registry-x"
(cd "$LOCAL_PACKAGE_DIR" && npm pack --pack-destination "$TMP_DIR/local" --silent >/dev/null)
npm pack "$PACKAGE_NAME@$VERSION" --pack-destination "$TMP_DIR/registry" --silent >/dev/null
tar -xzf "$TMP_DIR/local"/*.tgz -C "$TMP_DIR/local-x"
tar -xzf "$TMP_DIR/registry"/*.tgz -C "$TMP_DIR/registry-x"
diff -qr "$TMP_DIR/local-x/package" "$TMP_DIR/registry-x/package"

PROVENANCE=$(npm view "$PACKAGE_NAME@$VERSION" \
  dist.attestations.provenance.predicateType 2>/dev/null || true)
if [ "$PROVENANCE" != "https://slsa.dev/provenance/v1" ]; then
  echo "Expected SLSA provenance for $PACKAGE_NAME@$VERSION, got: ${PROVENANCE:-<missing>}" >&2
  exit 1
fi
