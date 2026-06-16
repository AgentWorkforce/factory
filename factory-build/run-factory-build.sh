#!/usr/bin/env bash
#
# run-factory-build.sh — sequenced runner for the Factory build-out (p1–p13).
#
# Files import @relayflows/core, so they are executed by the relayflows CLI
# (`relayflows run <file>`). Within a wave, workflows run in parallel
# (independent file scopes); waves run in dependency order.
#
# Ricky can drive this too: point it at these paths via its own runner, or just
# call this script. Override the runner with FACTORY_BUILD_RUNNER if needed.
#
# Usage:
#   ./run-factory-build.sh <wave1|wave2|wave3|wave4|wave5|wave6|wave7|prep|post-publish|all> [--dry-run]
#
# Examples:
#   ./run-factory-build.sh prep --dry-run     # validate wave1 (p1,p2,p3,p11) without spawning
#   ./run-factory-build.sh wave1              # extraction prep + broker heartbeat (parallel)
#   ./run-factory-build.sh wave2              # p4 extraction → STOPS at the publish gate
#   # ── operator: publish @agent-relay/factory + swap pear (see PUBLISH_READY.md) ──
#   ./run-factory-build.sh post-publish       # wave3..wave7 in dependency order
#
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"   # the factory repo
CLI_BIN="$REPO_ROOT/node_modules/@relayflows/cli/dist/cli.js"
DRY="${2:-}"

# Runner = the relayflows CLI installed as a devDependency of @agent-relay/factory.
# Override with FACTORY_BUILD_RUNNER (e.g. "relayflows" on PATH, or ricky's runner).
RUNNER_OVERRIDE="${FACTORY_BUILD_RUNNER:-}"

ensure_built() {
  if [[ -n "$RUNNER_OVERRIDE" ]]; then return 0; fi
  if [[ ! -f "$CLI_BIN" || ! -f "$REPO_ROOT/node_modules/@relayflows/core/dist/index.js" ]]; then
    echo ">>> installing @relayflows/core + @relayflows/cli (devDependencies)…"
    ( cd "$REPO_ROOT" && npm install ) || { echo "error: npm install failed" >&2; exit 1; }
  fi
}

run() {
  local f="$ROOT/$1"
  echo ">>> run $1"
  local cmd
  if [[ -n "$RUNNER_OVERRIDE" ]]; then cmd=("$RUNNER_OVERRIDE" run); else cmd=(node "$CLI_BIN" run); fi
  if [[ "$DRY" == "--dry-run" ]]; then
    FACTORY_BUILD_DRY=1 "${cmd[@]}" --dry-run "$f"
  else
    "${cmd[@]}" "$f"
  fi
}

run_parallel() {
  # Cap concurrency: the shared relayflows broker degrades under sustained
  # 4-wide load and wedges in `init` ~30min in (relayflows#9). Run in batches
  # of FACTORY_MAX_PARALLEL (default 2) until that's fixed. Set to 4 once #9 lands.
  local rc=0 batch=() max="${FACTORY_MAX_PARALLEL:-4}"
  for f in "$@"; do
    run "$f" & batch+=("$!")
    if [ "${#batch[@]}" -ge "$max" ]; then
      for p in "${batch[@]}"; do wait "$p" || rc=1; done
      batch=()
    fi
  done
  for p in "${batch[@]}"; do wait "$p" || rc=1; done
  return $rc
}

wave1() { run_parallel \
    wave1-extract-prep/01-p1-state-store-port.ts \
    wave1-extract-prep/02-p2-config-split.ts \
    wave1-extract-prep/03-p3-publish-prep.ts \
    wave1-extract-prep/04-p11-broker-heartbeat.ts; }
wave2() {
  run wave2-extraction/01-p4-extract-to-factory-repo.ts
  echo
  echo "============================================================"
  echo " PUBLISH GATE: p4 seeded AgentWorkforce/factory and stopped."
  echo " Operator: publish @agent-relay/factory + swap pear, per"
  echo "   <factory>/.workflow-artifacts/factory-p4-extraction/PUBLISH_READY.md"
  echo " Then run: ./run-factory-build.sh post-publish"
  echo "============================================================"
}
wave3() { run_parallel wave3-cloud-lift/01-p5-pear-teardown.ts wave3-cloud-lift/02-p6-host-orchestrator.ts; }
# wave4 = recipe selection + Linear ingress (p9 deleted — no daytona|fleet-node branch under unified-node)
wave4() { run_parallel \
    wave4-cloud-dispatch/01-p7-label-scope.ts \
    wave4-cloud-dispatch/02-p8-linear-webhook.ts; }
wave5() { run wave5-fleet-seam/01-p10-relayfleetclient-seam.ts; }   # Phase 3 fleet client
wave6() { run wave6-placement/01-p12-node-placement.ts; }          # relay-side placement (RFC §6/§7)
wave7() { run wave7-node/01-p13-factory-node-definition.ts; }      # Phase 4 node registration
# Sibling, independent of the factory phases (run anytime):
wave8() { run wave8-proactive/01-proactive-runtime-fleet-unification.ts; }

MODE="${1:-help}"
case "$MODE" in
  wave1|prep) ensure_built; wave1 ;;
  wave2) ensure_built; wave2 ;;
  wave3) ensure_built; wave3 ;;
  wave4) ensure_built; wave4 ;;
  wave5) ensure_built; wave5 ;;
  wave6) ensure_built; wave6 ;;
  wave7) ensure_built; wave7 ;;
  wave8|proactive) ensure_built; wave8 ;;
  post-publish) ensure_built; wave3 && wave4 && wave5 && wave6 && wave7 ;;
  all)
    ensure_built; wave1 && wave2
    echo "Stopping before wave3 for the publish gate. Run 'post-publish' after publishing + swapping pear."
    ;;
  *) sed -n '3,26p' "$0" ;;
esac
