#!/usr/bin/env bash
#
# Deliberately exceed the relayfile end-of-step flush body cap. A healthy
# executor must turn the ensuing 413 into a fatal RelayfileFlushError even
# though this command itself exits zero (cloud#3215, factory#417).
#
# This script intentionally leaves payload.bin on the provider mount: removing
# it before command exit would prevent the flush failure this canary exercises.

set -euo pipefail

readonly PAYLOAD_BYTES=12582912

if [[ -z "${RELAYFILE_MOUNT:-}" ]]; then
  echo "relayfile flush canary: RELAYFILE_MOUNT is not set" >&2
  exit 2
fi
if [[ ! -d "${RELAYFILE_MOUNT}" ]]; then
  echo "relayfile flush canary: RELAYFILE_MOUNT is not a directory: ${RELAYFILE_MOUNT}" >&2
  exit 2
fi

# Resolve the timestamp once. Calling date separately for mkdir and cd can
# cross a second boundary and select a directory that was never created.
readonly RUN_ID="$(date -u +%s)"
readonly RUN_DIR="${RELAYFILE_MOUNT%/}/factory/canary/flush-oversize/${RUN_ID}"

mkdir -p "${RUN_DIR}"
cd "${RUN_DIR}"
head -c "${PAYLOAD_BYTES}" /dev/urandom > payload.bin

readonly ACTUAL_BYTES="$(wc -c < payload.bin | tr -d '[:space:]')"
if [[ "${ACTUAL_BYTES}" != "${PAYLOAD_BYTES}" ]]; then
  echo "relayfile flush canary: payload size mismatch: ${ACTUAL_BYTES} != ${PAYLOAD_BYTES}" >&2
  exit 3
fi

ls -la payload.bin
echo "relayfile flush canary: staged ${ACTUAL_BYTES} incompressible bytes at ${RUN_DIR}/payload.bin"
echo "relayfile flush canary: command completed; the executor must now report the expected flush failure"
