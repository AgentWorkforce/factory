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
node "$(dirname "$0")/compare-package-trees.mjs" \
  "$TMP_DIR/local-x/package" "$TMP_DIR/registry-x/package"

# npm publishes the tarball before its attestation metadata becomes queryable,
# so a lookup run seconds after publish reads <missing> for a release whose
# provenance is in fact signed and in the transparency log. That is exactly how
# 0.1.88 failed this step while `latest`, `next`, the git tag and the signed
# provenance were all correct. The caller already retries the *version* lookup
# for the same reason; this field needs the same treatment.
PROVENANCE=""
for attempt in 1 2 3 4 5; do
  PROVENANCE=$(npm view "$PACKAGE_NAME@$VERSION" \
    dist.attestations.provenance.predicateType 2>/dev/null || true)
  [ "$PROVENANCE" = "https://slsa.dev/provenance/v1" ] && break
  sleep $((attempt * 2))
done
if [ "$PROVENANCE" != "https://slsa.dev/provenance/v1" ]; then
  echo "Expected SLSA provenance for $PACKAGE_NAME@$VERSION, got: ${PROVENANCE:-<missing>} (after 5 attempts over ~30s)" >&2
  exit 1
fi
