#!/usr/bin/env bash
#
# factory-canary.sh — scheduled sync-fidelity regression detector.
#
# Runs `factory canary <issue>` against the LIVE relayfile mount and asserts a
# known "Ready for Agent" issue is still classified dispatch-ready by the real
# triage path. If it ever flips to "skipped" (e.g. the Linear sync regresses to
# sparse records with no state.id), this exits non-zero and alerts — catching the
# regression before it silently blocks every factory dispatch.
#
# Run it on a schedule (cron/launchd) from your factory deployment directory —
# the one holding factory.config.json, where the relayfile mount + relay broker
# already live (so the canary reuses the running broker rather than spawning one).
# See scripts/com.agentrelay.factory-canary.plist.example for a launchd template.
#
# Config (env vars):
#   FACTORY_CANARY_ISSUE   Linear issue key to check (default: the first arg)
#   FACTORY_WORKDIR        deployment dir with factory.config.json (default: cwd)
#   FACTORY_CONFIG         config path, relative to FACTORY_WORKDIR (default: factory.config.json)
#   FACTORY_BIN            path to factory.mjs (default: this repo's bin/factory.mjs)
#   FACTORY_BACKEND        --backend value (default: internal)
#   FACTORY_CANARY_TIMEOUT seconds before the canary is considered hung (default: 180)
#   FACTORY_CANARY_SLACK_WEBHOOK  optional Slack incoming-webhook URL for failure alerts
#
# Exit codes: 0 = dispatch-ready (healthy); 1 = NOT ready / error / hung.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ISSUE="${FACTORY_CANARY_ISSUE:-${1:-}}"
WORKDIR="${FACTORY_WORKDIR:-$PWD}"
CONFIG="${FACTORY_CONFIG:-factory.config.json}"
BIN="${FACTORY_BIN:-$SCRIPT_DIR/../bin/factory.mjs}"
BACKEND="${FACTORY_BACKEND:-internal}"
TIMEOUT="${FACTORY_CANARY_TIMEOUT:-180}"
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -z "$ISSUE" ]]; then
  echo "[$TS] factory-canary: no issue key (set FACTORY_CANARY_ISSUE or pass an arg)" >&2
  exit 1
fi
if [[ ! -f "$BIN" ]]; then
  echo "[$TS] factory-canary: factory bin not found at $BIN" >&2
  exit 1
fi
cd "$WORKDIR" || { echo "[$TS] factory-canary: cannot cd to $WORKDIR" >&2; exit 1; }

# The canary runs the real dry-run triage path (no agents spawned) and prints a
# JSON verdict {ok,issue,status,reason}; exit code mirrors ok. A hung run
# (broker/mount wedge) is bounded by FACTORY_CANARY_TIMEOUT.
RUN=(node "$BIN" factory canary "$ISSUE" --config "$CONFIG" --backend "$BACKEND")
# A hung run (broker/mount wedge) MUST be bounded — an unbounded canary on a
# scheduler (launchd/cron) can wedge the slot forever and suppress later alerts.
# macOS has no `timeout` by default; coreutils ships it as `gtimeout`. If neither
# is present, fail closed rather than run without a deadline.
TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
fi
if [[ -z "$TIMEOUT_BIN" ]]; then
  echo "[$TS] factory-canary: no timeout utility found (install coreutils for 'timeout'/'gtimeout'); refusing to run unbounded" >&2
  exit 1
fi
OUT="$("$TIMEOUT_BIN" "$TIMEOUT" "${RUN[@]}" 2>/dev/null)"
CODE=$?
if [[ $CODE -eq 124 ]]; then
  echo "[$TS] factory-canary: TIMED OUT after ${TIMEOUT}s (broker/mount may be wedged)" >&2
fi

# The CLI prints a pretty-printed (multi-line) JSON verdict, so parse the whole
# output — not just the last line (which is only the closing `}`).
echo "[$TS] factory-canary $ISSUE -> exit $CODE"
[[ $CODE -eq 0 ]] && exit 0

REASON="$(printf '%s' "$OUT" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const v=JSON.parse(s);console.log(`${v.status||"error"}: ${v.reason||"unknown"}`)}catch{console.log("unparseable verdict")}})' 2>/dev/null)"
MSG=":rotating_light: factory canary FAILED for ${ISSUE} — ${REASON}. Sync fidelity may have regressed (issue no longer dispatch-ready)."
echo "[$TS] $MSG" >&2

if [[ -n "${FACTORY_CANARY_SLACK_WEBHOOK:-}" ]]; then
  curl -sS -m 15 -X POST -H 'Content-type: application/json' \
    --data "$(node -e 'process.stdout.write(JSON.stringify({text:process.argv[1]}))' "$MSG")" \
    "$FACTORY_CANARY_SLACK_WEBHOOK" >/dev/null 2>&1 \
    && echo "[$TS] factory-canary: posted Slack alert" >&2 \
    || echo "[$TS] factory-canary: Slack alert post failed" >&2
fi

exit 1
