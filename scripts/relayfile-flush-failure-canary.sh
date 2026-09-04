#!/usr/bin/env bash
# Deliberately exceed the Relayfile flush request limit. This script succeeding
# is only the command result: the enclosing Agent Relay step MUST subsequently
# fail while flushing the mount with RelayfileFlushError.
set -euo pipefail

: "${RELAYFILE_MOUNT:?RELAYFILE_MOUNT must point at the provider mount}"

readonly PAYLOAD_BYTES=12582912 # 12 MiB, safely above the 10,551,296-byte cap.
readonly RUN_ID="$(date -u +%s)-${RANDOM}"
readonly RUN_DIR="${RELAYFILE_MOUNT}/factory/canary/flush-oversize/${RUN_ID}"

mkdir -p "${RUN_DIR}"
head -c "${PAYLOAD_BYTES}" /dev/urandom > "${RUN_DIR}/payload.bin"

actual_bytes="$(wc -c < "${RUN_DIR}/payload.bin")"
if [ "${actual_bytes}" -ne "${PAYLOAD_BYTES}" ]; then
  echo "canary setup failed: expected ${PAYLOAD_BYTES} bytes, wrote ${actual_bytes}" >&2
  exit 1
fi

echo "Relayfile flush-failure canary staged ${actual_bytes} incompressible bytes"
ls -la "${RUN_DIR}/payload.bin"
