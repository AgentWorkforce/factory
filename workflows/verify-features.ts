/**
 * Automated feature verification for @agent-relay/factory.
 *
 * Factory's higher tiers mutate real ticket, fleet, and provider state, so this
 * workflow always runs deterministic package/config checks and makes tiers 3–5
 * explicit opt-ins. Tier 6 remains manual and is reported as such.
 *
 * Run locally:
 *   relay node workflow run workflows/verify-features.ts
 *
 * Schedule after merge:
 *   relay cloud schedule workflows/verify-features.ts --cron "0 3 * * *"
 *
 * Optional live checks:
 *   FACTORY_VERIFY_CANARY_ISSUE=AR-123
 *   FACTORY_VERIFY_CONFIG=/path/to/factory.config.json
 *   FACTORY_VERIFY_FLEET=1
 *   FACTORY_VERIFY_BACKEND=internal|relay
 *   FACTORY_VERIFY_CAPABILITY=spawn:codex|spawn:claude|workflow:run
 *   FACTORY_VERIFY_WORKFLOW=path/to/workflow.ts # required for workflow:run
 *   FACTORY_VERIFY_CLOUD=1
 */
import { workflow } from '@relayflows/core'

const ARTIFACTS = '.workflow-artifacts/verify-features'
const TIMESTAMP = new Date().toISOString().replace(/[:.]/gu, '-').slice(0, 19)
const RUN_ID = `factory-verify-${TIMESTAMP}`

async function main(): Promise<void> {
  const wf = workflow('factory-verify-features')
    .description(
      'Verifies the Factory manifest, package, tests, fixture cycle, and optional provider/fleet/cloud tiers. ' +
      'Writes a deterministic PASS/FAIL/SKIP/MANUAL report.',
    )
    .pattern('pipeline')
    .channel('factory-health')
    .maxConcurrency(1)
    .timeout(900_000)

  wf.step('acceptance-contract', {
    type: 'deterministic',
    captureOutput: true,
    failOnError: false,
    command: `
set -euo pipefail
mkdir -p "${ARTIFACTS}"
cat > "${ARTIFACTS}/acceptance-contract.txt" <<'EOF'
FACTORY FEATURE VERIFICATION ACCEPTANCE CONTRACT

T1  Package only: v1.1 manifest/procedure contract, public entrypoint, build
T2  Valid config: complete unit/guardian suite and fixture-backed CLI cycle
T3  Ticket provider: optional live sync-fidelity canary
T4  Fleet backend: optional spawn → roster → release → absence cycle
T5  Cloud mount: optional provider-backed dry-run discovery
T6  Live issue/PR: manual critical-path verification

Final-main regression contracts exercised by T1/T2:
- #77 durable clarification release, parking, wake, restart, and escalation retry
- #78 clone path expansion, cwd inference, and local checkout preflight
- #81 numeric GitHub issue resolution, source selection, and supported path shapes
- #82 repo-qualified roles and composite repo/issue state
- #83 bounded Error/log normalization
- #84 repo-aware relay placement on initial and restart paths
- #85 durable relay lifecycle, same-host fencing, remote publication/release/recovery
- #87 canonical PR routing, durable/coalesced babysitter wakes, ACK fence, poll/readback
- dependency admission/cycle fences and isolated worktree cleanup
- hosted invocation identity, completion reconciliation, owner leases, and merge/writeback gates
- bounded Cloud event contract, durable outbox, reporter retry, and non-blocking failure
- exact guardian state, manifest reconciliation, idempotent Slack delivery, and confirmed receipts
- packed tarball and head-bound release attestation

Automated overall PASS requires T1 and T2 PASS and every opted-in T3-T5 check PASS.
Skipped optional tiers are reported, never silently treated as exercised.
Tier 6 is always MANUAL; see .agentworkforce/features/critical-paths.md.
EOF
cat "${ARTIFACTS}/acceptance-contract.txt"
`,
  })

  wf.step('setup', {
    type: 'deterministic',
    dependsOn: ['acceptance-contract'],
    captureOutput: true,
    failOnError: false,
    command: `
set -euo pipefail
mkdir -p "${ARTIFACTS}"
rm -f "${ARTIFACTS}"/tier*.log "${ARTIFACTS}/summary.txt"
{
  echo "Run ID: ${RUN_ID}"
  echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Node: $(node --version)"
  echo "npm: $(npm --version)"
  echo "Branch: $(git branch --show-current)"
} | tee "${ARTIFACTS}/setup.log"
`,
  })

  wf.step('tier1-package-manifest', {
    type: 'deterministic',
    dependsOn: ['setup'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
LOG="${ARTIFACTS}/tier1.log"
PASS=0
FAIL=0
: > "$LOG"

run_check() {
  name="$1"
  shift
  if "$@" >> "$LOG" 2>&1; then
    echo "PASS  $name" | tee -a "$LOG"
    PASS=$((PASS + 1))
  else
    echo "FAIL  $name" | tee -a "$LOG"
    FAIL=$((FAIL + 1))
  fi
}

run_check "package build" npm run build
# TypeScript emits before reporting dependency-type errors. Finish alias
# rewriting so the remaining diagnostics can still distinguish CLI health from
# a build-only failure.
npx tsc-alias -p tsconfig.build.json >> "$LOG" 2>&1 || true
run_check "factory help" node bin/factory.mjs --help
run_check "factory version" node bin/factory.mjs --version
run_check "feature manifest" node bin/factory.mjs featuremap check

if [ ! -e .agentworkforce/FEATURE_MAPPING_BRIEF.md ]; then
  echo "PASS  brief removed" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  brief removed" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

if node --input-type=module <<'NODE' >> "$LOG" 2>&1
import { readFileSync } from 'node:fs'
import { validateFeatureManifestFile } from './dist/featuremap/index.js'

const procedures = readFileSync('.agentworkforce/features/verify/procedures.md', 'utf8')
const result = validateFeatureManifestFile({ rootDir: process.cwd() })
if (result.version !== '1.1') throw new Error('feature manifest must use v1.1 verification routing')
if (result.verificationDocument !== '.agentworkforce/features/verify/procedures.md') {
  throw new Error('feature manifest points to the wrong verification document')
}
for (const [category, procedure] of Object.entries(result.categoryProcedures)) {
  if (!procedures.includes('## ' + procedure)) {
    throw new Error('missing named procedure for ' + category + ': ' + procedure)
  }
}

const regressionMarkers = {
  'src/config/local-clone-paths.test.ts': [
    'infers the git top-level from a nested cwd when the resolved route remote matches',
    'validates configured paths through git and accepts linked worktrees',
  ],
  'src/cli/fleet.test.ts': [
    'auto-detects GitHub-only workspaces without resolving Linear states',
    'rejects an ambiguous bare GitHub issue number across configured repositories',
    'keeps relay dispatch ownership until the remote PR is published and the issue is parked',
  ],
  'src/triage/triage.test.ts': [
    'repo-qualifies every collision-prone GitHub-native role while preserving its source repo identity',
  ],
  'src/logging.test.ts': [
    'contains nested errors, cycles, BigInts, getters, and throwing toJSON hooks',
    'applies the global field budget to branching nested arrays',
  ],
  'src/fleet/relay-fleet-client.test.ts': [
    'dispatches spawn through placement with task-exit lifecycle and no self target',
    'hydrates tracked agents for restart recovery',
  ],
  'src/mount/relayfile-github-connection-write.test.ts': [
    'publishes an already-pushed remote branch without reading an orchestrator-local clone',
  ],
  'src/state/file-state-store.test.ts': [
    'persists and fences dispatch lifecycle ownership across processes and crash takeover',
    'restores and clears babysitter ownership plus pending wake state in a fresh process-equivalent store',
  ],
  'src/orchestrator/factory.test.ts': [
    'isolates equal-number GitHub dispatch names, state, registry, resume, and completion across repos',
    'releases the whole team before waiting for slow Slack question confirmation',
    'fences a duplicate owner while the active owner is inside a slow PR publication',
    'autonomously retries a transient remote PR publication without another exit event',
    'routes and coalesces only the owned PR review/check/comment events with metadata-only fencing',
    'installs the destructive fence before delivering its explicit begin acknowledgment',
    'routes paginated poll events through the same exact-PR coalescer',
  ],
}
for (const [path, markers] of Object.entries(regressionMarkers)) {
  const source = readFileSync(path, 'utf8')
  for (const marker of markers) {
    if (!source.includes(marker)) throw new Error('missing final-main regression marker in ' + path + ': ' + marker)
  }
}
console.log(JSON.stringify({ categories: result.categoryCount, features: result.features.length }))
NODE
then
  echo "PASS  manifest schema and procedure coverage" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  manifest schema and procedure coverage" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi


echo "Tier 1 result: $PASS passed, $FAIL failed" | tee -a "$LOG"
if [ "$FAIL" -gt 0 ]; then
  echo "TIER1_FAIL" | tee -a "$LOG"
  exit 1
fi
echo "TIER1_PASS" | tee -a "$LOG"
`,
  })

  wf.step('tier2-tests-config', {
    type: 'deterministic',
    dependsOn: ['tier1-package-manifest'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
LOG="${ARTIFACTS}/tier2.log"
PASS=0
FAIL=0
: > "$LOG"

if npm test -- --maxWorkers=1 >> "$LOG" 2>&1; then
  echo "PASS  full Vitest suite" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  full Vitest suite" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

if npm run verify:e2e >> "$LOG" 2>&1; then
  echo "PASS  packed tarball and head-bound E2E" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  packed tarball and head-bound E2E" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

CONFIG="${ARTIFACTS}/fixture-config.json"
if node --input-type=module - "$CONFIG" <<'NODE' >> "$LOG" 2>&1
import { readFileSync, writeFileSync } from 'node:fs'

const output = process.argv[2]
const config = JSON.parse(readFileSync('test/fixtures/factory.config.json', 'utf8'))
const fixture = config.fixtureFiles?.['/linear/issues/AR-77__uuid-77.json']
if (!output || !fixture?.payload) throw new Error('AR-77 fixture payload is missing')
fixture.payload.url = 'https://linear.app/agent-relay/issue/AR-77/cli-dry-run'
writeFileSync(output, JSON.stringify(config, null, 2))
NODE
then
  echo "PASS  canonical fixture identity" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  canonical fixture identity" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

REPORT="${ARTIFACTS}/fixture-report.json"
if node bin/factory.mjs run-once \
    --config "$CONFIG" \
    --dry-run > "$REPORT" 2>> "$LOG" && \
  node --input-type=module -e '
    import { readFileSync } from "node:fs"
    const report = JSON.parse(readFileSync(process.argv[1], "utf8"))
    const decision = report.triaged.find((entry) => entry.issue.key === "AR-77")
    if (!report.dryRun || !decision || decision.routes[0]?.repo !== "AgentWorkforce/pear" ||
        !report.dispatched.some((entry) => entry.issue.key === "AR-77")) process.exit(1)
  ' "$REPORT" >> "$LOG" 2>&1; then
  echo "PASS  fixture discover-triage-dispatch cycle" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  fixture discover-triage-dispatch cycle" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

if node bin/factory.mjs status --config "$CONFIG" >> "$LOG" 2>&1; then
  echo "PASS  fixture status JSON" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  fixture status JSON" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

echo "Tier 2 result: $PASS passed, $FAIL failed" | tee -a "$LOG"
if [ "$FAIL" -gt 0 ]; then
  echo "TIER2_FAIL" | tee -a "$LOG"
  exit 1
fi
echo "TIER2_PASS" | tee -a "$LOG"
`,
  })

  wf.step('tier3-provider-canary', {
    type: 'deterministic',
    dependsOn: ['tier2-tests-config'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
LOG="${ARTIFACTS}/tier3.log"
CONFIG=$(printenv FACTORY_VERIFY_CONFIG 2>/dev/null || true)
ISSUE=$(printenv FACTORY_VERIFY_CANARY_ISSUE 2>/dev/null || true)
: > "$LOG"

if [ -z "$ISSUE" ]; then
  echo "Skip reason: FACTORY_VERIFY_CANARY_ISSUE is unset" | tee -a "$LOG"
  echo "TIER3_SKIP" | tee -a "$LOG"
  exit 0
fi
if [ -z "$CONFIG" ] || [ ! -f "$CONFIG" ]; then
  echo "Failure reason: FACTORY_VERIFY_CONFIG must name a valid live config" | tee -a "$LOG"
  echo "TIER3_FAIL" | tee -a "$LOG"
  exit 1
fi
if node bin/factory.mjs canary "$ISSUE" --config "$CONFIG" >> "$LOG" 2>&1; then
  echo "TIER3_PASS" | tee -a "$LOG"
  exit 0
fi
echo "TIER3_FAIL" | tee -a "$LOG"
exit 1
`,
  })

  wf.step('tier4-fleet', {
    type: 'deterministic',
    dependsOn: ['tier3-provider-canary'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
LOG="${ARTIFACTS}/tier4.log"
: > "$LOG"
VERIFY_FLEET=$(printenv FACTORY_VERIFY_FLEET 2>/dev/null || true)
BACKEND=$(printenv FACTORY_VERIFY_BACKEND 2>/dev/null || true)
CAPABILITY=$(printenv FACTORY_VERIFY_CAPABILITY 2>/dev/null || true)
WORKFLOW=$(printenv FACTORY_VERIFY_WORKFLOW 2>/dev/null || true)
if [ -z "$CAPABILITY" ]; then CAPABILITY=spawn:codex; fi
AGENT_NAME="${RUN_ID}-fleet"
SPAWNED=0

cleanup_fleet() {
  if [ "$SPAWNED" = "1" ]; then
    node bin/factory.mjs fleet release "$AGENT_NAME" --backend "$BACKEND" \
      --reason "Factory verification cleanup" >> "$LOG" 2>&1 || true
  fi
}
trap cleanup_fleet EXIT

if [ "$VERIFY_FLEET" != "1" ]; then
  echo "Skip reason: FACTORY_VERIFY_FLEET is not 1" | tee -a "$LOG"
  echo "TIER4_SKIP" | tee -a "$LOG"
  exit 0
fi
if [ "$BACKEND" != "internal" ] && [ "$BACKEND" != "relay" ]; then
  echo "Failure reason: FACTORY_VERIFY_BACKEND must be internal or relay" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi
case "$CAPABILITY" in
  spawn:codex|spawn:claude|workflow:run) ;;
  *)
    echo "Failure reason: FACTORY_VERIFY_CAPABILITY must be spawn:codex, spawn:claude, or workflow:run" | tee -a "$LOG"
    echo "TIER4_FAIL" | tee -a "$LOG"
    exit 1
    ;;
esac
if [ "$CAPABILITY" = "workflow:run" ] && [ "$BACKEND" != "relay" ]; then
  echo "Failure reason: workflow:run verification requires FACTORY_VERIFY_BACKEND=relay" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi
if [ "$CAPABILITY" = "workflow:run" ] && [ -z "$WORKFLOW" ]; then
  echo "Failure reason: workflow:run requires FACTORY_VERIFY_WORKFLOW" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi

SPAWN_REPORT="${ARTIFACTS}/tier4-spawn.json"
ROSTER_REPORT="${ARTIFACTS}/tier4-roster.json"
FINAL_ROSTER="${ARTIFACTS}/tier4-final-roster.json"

if [ "$CAPABILITY" = "workflow:run" ]; then
  node bin/factory.mjs fleet spawn "$CAPABILITY" --backend "$BACKEND" \
    --name "$AGENT_NAME" --workflow "$WORKFLOW" \
    > "$SPAWN_REPORT" 2>> "$LOG"
else
  node bin/factory.mjs fleet spawn "$CAPABILITY" --backend "$BACKEND" \
    --name "$AGENT_NAME" --task "Reply with FACTORY_VERIFY_OK, then exit." \
    > "$SPAWN_REPORT" 2>> "$LOG"
fi
if [ "$?" != "0" ]; then
  echo "Failure reason: fleet spawn failed" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi
SPAWNED=1

if ! node bin/factory.mjs fleet roster --backend "$BACKEND" > "$ROSTER_REPORT" 2>> "$LOG" || \
  ! node --input-type=module - "$ROSTER_REPORT" "$AGENT_NAME" <<'NODE' >> "$LOG" 2>&1
import { readFileSync } from 'node:fs'
const report = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const agents = Array.isArray(report) ? report : (report.agents ?? report.items ?? [])
if (!agents.some((agent) => agent?.name === process.argv[3])) process.exit(1)
NODE
then
  echo "Failure reason: spawned agent was not present in the canonical roster" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi

if ! node bin/factory.mjs fleet release "$AGENT_NAME" --backend "$BACKEND" \
    --reason "Factory verification complete" >> "$LOG" 2>&1; then
  echo "Failure reason: fleet release failed" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi
SPAWNED=0

ABSENT=0
for _attempt in 1 2 3 4 5 6 7 8 9 10; do
  if node bin/factory.mjs fleet roster --backend "$BACKEND" > "$FINAL_ROSTER" 2>> "$LOG" && \
    node --input-type=module - "$FINAL_ROSTER" "$AGENT_NAME" <<'NODE' >> "$LOG" 2>&1
import { readFileSync } from 'node:fs'
const report = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const agents = Array.isArray(report) ? report : (report.agents ?? report.items ?? [])
if (agents.some((agent) => agent?.name === process.argv[3])) process.exit(1)
NODE
  then
    ABSENT=1
    break
  fi
  sleep 1
done
if [ "$ABSENT" != "1" ]; then
  echo "Failure reason: released agent remained in the canonical roster" | tee -a "$LOG"
  echo "TIER4_FAIL" | tee -a "$LOG"
  exit 1
fi

echo "TIER4_PASS" | tee -a "$LOG"
exit 0
`,
  })

  wf.step('tier5-cloud-mount', {
    type: 'deterministic',
    dependsOn: ['tier4-fleet'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
LOG="${ARTIFACTS}/tier5.log"
CONFIG=$(printenv FACTORY_VERIFY_CONFIG 2>/dev/null || true)
VERIFY_CLOUD=$(printenv FACTORY_VERIFY_CLOUD 2>/dev/null || true)
: > "$LOG"

if [ "$VERIFY_CLOUD" != "1" ]; then
  echo "Skip reason: FACTORY_VERIFY_CLOUD is not 1" | tee -a "$LOG"
  echo "TIER5_SKIP" | tee -a "$LOG"
  exit 0
fi
if [ -z "$CONFIG" ] || [ ! -f "$CONFIG" ]; then
  echo "Failure reason: FACTORY_VERIFY_CONFIG must name a valid cloud config" | tee -a "$LOG"
  echo "TIER5_FAIL" | tee -a "$LOG"
  exit 1
fi
if node bin/factory.mjs run-once --config "$CONFIG" --dry-run >> "$LOG" 2>&1; then
  echo "TIER5_PASS" | tee -a "$LOG"
  exit 0
fi
echo "TIER5_FAIL" | tee -a "$LOG"
exit 1
`,
  })

  wf.step('tier6-manual', {
    type: 'deterministic',
    dependsOn: ['tier5-cloud-mount'],
    captureOutput: true,
    failOnError: false,
    command: `
cat > "${ARTIFACTS}/tier6.log" <<'EOF'
TIER6_MANUAL
Run the live issue/PR procedures in:
  .agentworkforce/features/verify/procedures.md
and the end-to-end sequences in:
  .agentworkforce/features/critical-paths.md

Required manual surfaces: kill-loop, standalone babysit, guarded merge,
probe close, authorized GitHub replies, remote-head PR publication/recovery,
canonical babysitter routing/readback, destructive-work ACK fencing, and PR close.
Supported ownership topology: multiple control-plane processes sharing one
same-host FileStateStore. Cross-host active/active control planes are unsupported.
EOF
cat "${ARTIFACTS}/tier6.log"
`,
  })

  wf.step('collect-results', {
    type: 'deterministic',
    dependsOn: ['tier6-manual'],
    captureOutput: true,
    failOnError: false,
    command: `
set -uo pipefail
SUMMARY="${ARTIFACTS}/summary.txt"
FAIL=0

overall_result() {
  fail_count="$1"
  summary_path="$2"
  if [ "$fail_count" -ne 0 ] || \
      grep -Eq '^T[1-5]: (NOT_RUN|TIER[1-5]_(FAIL|UNKNOWN))' "$summary_path"; then
    printf 'FAIL'
  else
    printf 'PASS'
  fi
}

tier_result() {
  requested_tier="$1"
  tier_log="$2"
  marker=$(grep -E '^TIER[0-9]+_' "$tier_log" | tail -1 || true)
  case "$requested_tier:$marker" in
    1:TIER1_PASS|1:TIER1_FAIL|2:TIER2_PASS|2:TIER2_FAIL|\
    3:TIER3_PASS|3:TIER3_FAIL|3:TIER3_SKIP|\
    4:TIER4_PASS|4:TIER4_FAIL|4:TIER4_SKIP|\
    5:TIER5_PASS|5:TIER5_FAIL|5:TIER5_SKIP|6:TIER6_MANUAL) printf '%s' "$marker" ;;
    *) printf 'TIER%s_UNKNOWN' "$requested_tier" ;;
  esac
}

# Regression guard: an absent required tier log is represented as NOT_RUN and
# must never be certified as an overall pass, even when no explicit *_FAIL
# marker exists to match.
SELF_TEST="${ARTIFACTS}/collector-not-run-self-test.txt"
cat > "$SELF_TEST" <<'EOF'
T1: NOT_RUN
T2: TIER2_PASS
T3: TIER3_SKIP
T4: TIER4_SKIP
T5: TIER5_SKIP
EOF
if [ "$(overall_result 0 "$SELF_TEST")" != "FAIL" ]; then
  echo "Collector regression failed: required NOT_RUN tier was accepted" >&2
  exit 1
fi

printf '%s\n' 'TIER2_PASS' > "${ARTIFACTS}/collector-cross-tier-self-test.log"
if [ "$(tier_result 1 "${ARTIFACTS}/collector-cross-tier-self-test.log")" != "TIER1_UNKNOWN" ]; then
  echo "Collector regression failed: cross-tier marker was accepted" >&2
  exit 1
fi
printf '%s\n' 'TIER1_PASS_EXTRA' > "${ARTIFACTS}/collector-suffix-self-test.log"
if [ "$(tier_result 1 "${ARTIFACTS}/collector-suffix-self-test.log")" != "TIER1_UNKNOWN" ]; then
  echo "Collector regression failed: suffixed marker was accepted" >&2
  exit 1
fi

{
  echo "Factory Feature Verification: ${RUN_ID}"
  echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  for tier in 1 2 3 4 5 6; do
    LOG="${ARTIFACTS}/tier$tier.log"
    if [ ! -f "$LOG" ]; then
      RESULT="NOT_RUN"
      FAIL=1
    else
      RESULT=$(tier_result "$tier" "$LOG")
      case "$RESULT" in NOT_RUN|*_FAIL|*_UNKNOWN) FAIL=1 ;; esac
    fi
    echo "T$tier: $RESULT"
  done
  echo
  echo "Tier 6 is intentionally manual and is not counted as an automated failure."
} > "$SUMMARY"

echo "Overall: $(overall_result "$FAIL" "$SUMMARY")" >> "$SUMMARY"
cat "$SUMMARY"
`,
  })

  wf.step('report', {
    type: 'deterministic',
    dependsOn: ['collect-results'],
    captureOutput: true,
    failOnError: true,
    command: `
set -euo pipefail
report_result() {
  ! grep -q '^Overall: FAIL$' "$1"
}

REPORT_SELF_TEST="${ARTIFACTS}/report-failure-self-test.txt"
printf '%s\n' 'Overall: FAIL' > "$REPORT_SELF_TEST"
if report_result "$REPORT_SELF_TEST"; then
  echo "Report regression failed: Overall FAIL returned success" >&2
  exit 1
fi

cat "${ARTIFACTS}/summary.txt"
echo
echo "Detailed logs: ${ARTIFACTS}/tier{1,2,3,4,5,6}.log"

report_result "${ARTIFACTS}/summary.txt"
`,
  })

  const run = await wf.run()
  if (run.status !== 'completed') {
    throw new Error(`Factory feature verification ended with status ${run.status}: ${run.error ?? 'no error detail'}`)
  }
}

main()
