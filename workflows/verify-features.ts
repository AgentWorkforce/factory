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

T1  Package only: manifest schema/coverage, public entrypoint, build
T2  Valid config: complete unit suite and fixture-backed CLI cycle
T3  Ticket provider: optional live sync-fidelity canary
T4  Fleet backend: optional internal/hosted roster health
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

if [ ! -e .agentworkforce/FEATURE_MAPPING_BRIEF.md ]; then
  echo "PASS  brief removed" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  brief removed" | tee -a "$LOG"
  FAIL=$((FAIL + 1))
fi

if node --input-type=module <<'NODE' >> "$LOG" 2>&1
import { existsSync, readFileSync } from 'node:fs'
import { parse } from 'yaml'

const manifest = parse(readFileSync('.agentworkforce/features/manifest.yaml', 'utf8'))
const procedures = readFileSync('.agentworkforce/features/verify/procedures.md', 'utf8')
const categories = Object.values(manifest.categories || {})
const features = categories.flatMap((category) => category.features || [])
const ids = features.map((feature) => feature.id)
if (categories.length === 0 || features.length === 0) throw new Error('manifest is empty')
if (new Set(ids).size !== ids.length) throw new Error('duplicate manifest feature IDs')
const tierCounts = Object.fromEntries([1, 2, 3, 4, 5, 6].map((tier) => [
  tier,
  features.filter((feature) => feature.verify_tier === tier).length,
]))
if (manifest.catalog?.category_count !== categories.length ||
    manifest.catalog?.feature_count !== features.length ||
    JSON.stringify(manifest.catalog?.tier_counts) !== JSON.stringify(tierCounts)) {
  throw new Error('manifest catalog summary mismatch: ' + JSON.stringify({
    catalog: manifest.catalog,
    actual: { category_count: categories.length, feature_count: features.length, tier_counts: tierCounts },
  }))
}
for (const feature of features) {
  if (!feature.id || !feature.name || (!feature.cli && !feature.api) ||
      !feature.description || !feature.location ||
      !Number.isInteger(feature.verify_tier) || feature.verify_tier < 1 || feature.verify_tier > 6) {
    throw new Error('invalid manifest feature: ' + JSON.stringify(feature))
  }
  for (const location of feature.location.split(',').map((value) => value.trim())) {
    if (!existsSync(location)) throw new Error('manifest location does not exist for ' + feature.id + ': ' + location)
  }
  const marker = String.fromCharCode(96) + feature.id + String.fromCharCode(96)
  if (!procedures.includes(marker)) throw new Error('feature missing from procedures: ' + feature.id)
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
console.log(JSON.stringify({ categories: categories.length, features: features.length }))
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

if npm test >> "$LOG" 2>&1; then
  echo "PASS  full Vitest suite" | tee -a "$LOG"
  PASS=$((PASS + 1))
else
  echo "FAIL  full Vitest suite" | tee -a "$LOG"
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
  echo "TIER3_SKIP: FACTORY_VERIFY_CANARY_ISSUE is unset" | tee -a "$LOG"
  exit 0
fi
if [ -z "$CONFIG" ] || [ ! -f "$CONFIG" ]; then
  echo "TIER3_FAIL: FACTORY_VERIFY_CONFIG must name a valid live config" | tee -a "$LOG"
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

if [ "$VERIFY_FLEET" != "1" ]; then
  echo "TIER4_SKIP: FACTORY_VERIFY_FLEET is not 1" | tee -a "$LOG"
  exit 0
fi
if [ "$BACKEND" != "internal" ] && [ "$BACKEND" != "relay" ]; then
  echo "TIER4_FAIL: FACTORY_VERIFY_BACKEND must be internal or relay" | tee -a "$LOG"
  exit 1
fi
if node bin/factory.mjs fleet roster --backend "$BACKEND" >> "$LOG" 2>&1; then
  echo "TIER4_PASS" | tee -a "$LOG"
  exit 0
fi
echo "TIER4_FAIL" | tee -a "$LOG"
exit 1
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
  echo "TIER5_SKIP: FACTORY_VERIFY_CLOUD is not 1" | tee -a "$LOG"
  exit 0
fi
if [ -z "$CONFIG" ] || [ ! -f "$CONFIG" ]; then
  echo "TIER5_FAIL: FACTORY_VERIFY_CONFIG must name a valid cloud config" | tee -a "$LOG"
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
if [ "$(overall_result 1 "$SELF_TEST")" != "FAIL" ]; then
  echo "Collector regression failed: required NOT_RUN tier was accepted" >&2
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
      RESULT=$(grep -E "^TIER[0-9]+_(PASS|FAIL|SKIP|MANUAL)" "$LOG" | tail -1 || true)
      if [ -z "$RESULT" ]; then RESULT=$(printf 'TIER%s_UNKNOWN' "$tier"); FAIL=1; fi
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
    failOnError: false,
    command: `
set -euo pipefail
cat "${ARTIFACTS}/summary.txt"
echo
echo "Detailed logs: ${ARTIFACTS}/tier{1,2,3,4,5,6}.log"

if grep -q '^Overall: FAIL$' "${ARTIFACTS}/summary.txt"; then
  exit 1
fi
`,
  })

  await wf.run()
}

main()
