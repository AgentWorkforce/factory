#!/usr/bin/env bash
# Deliberately exceed the Relayfile end-of-step flush request limit. The command
# exits successfully; the enclosing executor must turn the flush 413 into a
# fatal RelayfileFlushError (cloud#3215, factory#417).
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

# Resolve the run id once so mkdir and the payload write cannot disagree across
# a second boundary. The PID also keeps concurrent canaries separate.
readonly RUN_ID="$(date -u +%s)-$$"
readonly RUN_DIR="${RELAYFILE_MOUNT%/}/factory/canary/flush-oversize/${RUN_ID}"

mkdir -p "${RUN_DIR}"
head -c "${PAYLOAD_BYTES}" /dev/urandom > "${RUN_DIR}/payload.bin"

readonly ACTUAL_BYTES="$(wc -c < "${RUN_DIR}/payload.bin" | tr -d '[:space:]')"
if [[ "${ACTUAL_BYTES}" != "${PAYLOAD_BYTES}" ]]; then
  echo "relayfile flush canary: payload size mismatch: ${ACTUAL_BYTES} != ${PAYLOAD_BYTES}" >&2
  exit 3
fi

ls -la "${RUN_DIR}/payload.bin"
echo "relayfile flush canary: staged ${ACTUAL_BYTES} incompressible bytes"
echo "relayfile flush canary: command exited 0; awaiting the expected fatal flush failure"
