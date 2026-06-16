/**
 * Factory build — p4 (Phase 1 extraction) — BESPOKE
 *
 * Special-cased (not the generic squad builder) because:
 *   - the destination repo (AgentWorkforce/factory) has no commits yet, so the
 *     generic `git checkout -B` preflight can't run there;
 *   - it is cross-repo (seeds factory + later swaps pear);
 *   - it contains an IRREVERSIBLE `npm publish` — that is left as an explicit
 *     operator gate. This workflow seeds + pushes the factory repo and prepares
 *     publish, then STOPS at PUBLISH_READY. The pear dep-swap happens after the
 *     human publish (documented in the signoff).
 *
 * Spec: factory/planning/linear-issue-factory-extract-p4-extraction.md
 * Runs with cwd = pear (the extraction source); factory ops use absolute paths.
 */
import { workflow } from '@relayflows/core';
import { REPOS, PLANNING } from '../lib/factory-build-lib.ts';

const PEAR = REPOS.pear;
const FACTORY = REPOS.factory;
const ART = '.workflow-artifacts/factory-p4-extraction';

async function main() {
  const wf = workflow('factory-p4-extract-to-factory-repo')
    .description('[factory p4] Extract @agent-relay/factory into AgentWorkforce/factory with history; prep publish; stop at the publish gate.')
    .pattern('dag')
    .channel('wf-factory-p4-extraction')
    .maxConcurrency(3)
    .timeout(7_200_000)
    .onError('retry', { maxRetries: 1, retryDelayMs: 10_000 });

  wf.agent('lead-claude', { cli: 'claude', role: 'Lead + QA for the extraction. Coordinates the seed, verification, and publish-prep; runs repair on red gates.', retries: 1 });
  wf.agent('impl-codex', { cli: 'codex', role: 'Runs the git filter-repo extraction playbook, seeds the factory repo tooling, and pushes.', retries: 2 });
  wf.agent('assist-opencode', { cli: 'opencode', role: 'Assists with repo tooling (CI/publish workflow, LICENSE, .gitignore, README).', retries: 2 });
  wf.agent('reviewer-claude', { cli: 'claude', preset: 'reviewer', role: 'Reviews the seeded repo + the post-publish swap plan from scratch.', retries: 1 });
  wf.agent('reviewer-codex', { cli: 'codex', preset: 'reviewer', role: 'Second-pass review of the extraction integrity (history preserved, publish shape, swap plan).', retries: 1 });

  wf.step('preflight', {
    type: 'deterministic',
    captureOutput: true,
    failOnError: true,
    command: [
      'set -e',
      'git rev-parse --show-toplevel >/dev/null',
      'git filter-repo --version >/dev/null 2>&1 || { echo "MISSING_TOOL: git-filter-repo (install: brew install git-filter-repo)"; exit 1; }',
      'gh auth status >/dev/null 2>&1 || { echo "MISSING_ENV_VAR: gh auth"; exit 1; }',
      // p1-p3 must be landed: the package must be publishable as @agent-relay/factory, non-private.
      'node -e "const p=require(\'./packages/factory-sdk/package.json\'); if(p.name!==\'@agent-relay/factory\'){console.error(\'p3 not landed: package name is \'+p.name);process.exit(1)} if(p.private){console.error(\'p3 not landed: still private\');process.exit(1)}"',
      `test -d "${FACTORY}/.git" || { echo "MISSING: ${FACTORY} is not a git repo"; exit 1; }`,
      'gh repo view AgentWorkforce/factory --json name >/dev/null 2>&1 || { echo "MISSING: GitHub AgentWorkforce/factory not reachable"; exit 1; }',
      `mkdir -p ${ART}`,
      'echo PREFLIGHT_OK',
    ].join(' && '),
  });

  wf.step('read-spec', {
    type: 'deterministic',
    dependsOn: ['preflight'],
    captureOutput: true,
    failOnError: true,
    command: [
      'set -e',
      `cat ${PLANNING}/linear-issue-factory-extract-p4-extraction.md`,
      'echo READ_SPEC_OK',
    ].join(' && '),
  });

  wf.step('seed-extract', {
    agent: 'impl-codex',
    dependsOn: ['read-spec'],
    task: [
      'Execute the extraction playbook from the spec exactly.',
      `Spec: {{steps.read-spec.output}}`,
      '',
      'Steps:',
      '1. Clone pear to a scratch dir and run: git filter-repo --subdirectory-filter packages/factory-sdk (preserves @agent-relay/factory history with paths rewritten to repo root).',
      `2. In ${FACTORY} (origin already wired, repo empty): commit the existing untracked planning/ dir first, then pull the filtered history with --allow-unrelated-histories so package files and planning/ coexist.`,
      `3. RECONCILE with the existing scaffold already committed in ${FACTORY}: keep package.json "repository" + "publishConfig", keep .github/workflows/publish.yml (provenance), keep planning/. REPLACE the placeholder index.js/index.d.ts/README.md with the real package entrypoints, and merge the real factory-sdk package.json code fields onto the scaffold (do NOT drop repository/publishConfig). Add .github/workflows/ci.yml (node 20: npm ci, build, test, npm pack --dry-run), LICENSE, .gitignore (dist, node_modules, *.tsbuildinfo). The first real release is 0.1.0 (placeholder was 0.0.0).`,
      '4. Push factory main: git push -u origin main.',
      'DO NOT run npm publish — that is a human gate (next step writes the exact command).',
      `Write ${ART}/seed-report.md: commits imported (git log --oneline | head), tooling added, and any deviations. Use assist-opencode for the CI/publish/LICENSE/.gitignore tooling. End with SEED_DONE.`,
    ].join('\n'),
    verification: { type: 'file_exists', value: `${ART}/seed-report.md` },
  });
  wf.step('assist-tooling', {
    agent: 'assist-opencode',
    dependsOn: ['read-spec'],
    task: [
      'Wait for lead/impl direction, then author the new repo tooling in ' + FACTORY + ': CI workflow, publish workflow, LICENSE, README, .gitignore — mirroring the org conventions (read @agent-relay/cloud for the publish pattern). Do not run npm publish. End with ASSIST_DONE.',
    ].join('\n'),
  });

  wf.step('verify-factory-seed', {
    type: 'deterministic',
    dependsOn: ['seed-extract'],
    captureOutput: true,
    failOnError: false,
    command: [
      `set -e`,
      `test -f "${FACTORY}/package.json"`,
      `node -e "const p=require('${FACTORY}/package.json'); if(p.name!=='@agent-relay/factory'){process.exit(1)}"`,
      `test -d "${FACTORY}/src" && test -f "${FACTORY}/.github/workflows/ci.yml"`,
      `cd "${FACTORY}" && git log --oneline | head -10`,
      `cd "${FACTORY}" && npm ci 2>&1 | tail -10 && npm run build 2>&1 | tail -20 && npm pack --dry-run 2>&1 | tail -20`,
      `echo VERIFY_FACTORY_SEED_OK`,
    ].join(' && '),
  });
  wf.step('repair-seed', {
    agent: 'lead-claude',
    dependsOn: ['verify-factory-seed'],
    task: 'If the factory seed verification failed (build, package name, history, or tooling), fix it in ' + FACTORY + ' and rerun the checks until green. If green, confirm. Output:\n{{steps.verify-factory-seed.output}}',
    verification: { type: 'exit_code' },
  });

  wf.step('publish-gate', {
    type: 'deterministic',
    dependsOn: ['repair-seed'],
    captureOutput: true,
    failOnError: true,
    command: [
      'set -e',
      `printf "%s\\n" "# PUBLISH_READY — operator gate" "" "The factory repo is seeded, built, and pack-clean. Publishing is irreversible and is left to a human." "" "To publish:" "  cd ${FACTORY} && git tag v0.1.0 && npm publish --access public" "" "After publish, run the pear dep-swap (post-publish, separate PR):" "  - remove pear/packages/factory-sdk and its workspaces entry" "  - add @agent-relay/factory@^0.1.0 to pear dependencies" "  - update consumers (src/main/factory-manager.ts, bin/pear.mjs) to import @agent-relay/factory" "  - add repos.byLabel.factory = AgentWorkforce/factory to factory.config.json" "  - then Phase 1.5 teardown (p5) removes the daemon entirely" > ${ART}/PUBLISH_READY.md`,
      `cat ${ART}/PUBLISH_READY.md`,
    ].join(' && '),
  });

  wf.step('review-claude', {
    agent: 'reviewer-claude',
    dependsOn: ['publish-gate'],
    task: [
      'Fresh review of the extraction. Confirm: history is preserved (commits from packages/factory-sdk present in ' + FACTORY + '), the published shape is correct (npm pack lists only dist/bin/package.json/README), planning/ is preserved, and the post-publish pear-swap plan in PUBLISH_READY.md is correct and complete.',
      `Write ${ART}/claude-review.md with findings or NO_ISSUES_FOUND.`,
    ].join('\n'),
    verification: { type: 'file_exists', value: `${ART}/claude-review.md` },
  });
  wf.step('review-codex', {
    agent: 'reviewer-codex',
    dependsOn: ['review-claude'],
    task: [
      'Second-pass fresh review of extraction integrity from scratch. Verify no pear-internal imports leaked, the filter-repo did not collide paths, and CI/publish workflows are valid.',
      `Write ${ART}/codex-review.md with findings or NO_ISSUES_FOUND.`,
    ].join('\n'),
    verification: { type: 'file_exists', value: `${ART}/codex-review.md` },
  });
  wf.step('repair-review', {
    agent: 'impl-codex',
    dependsOn: ['review-codex'],
    task: 'Fix every valid finding from claude-review.md and codex-review.md in ' + FACTORY + ', rerun build + pack, and record fixes in ' + ART + '/review-fixes.md. If a finding cannot be fixed safely, write ' + ART + '/BLOCKED_NO_COMMIT.md with evidence.',
    verification: { type: 'exit_code' },
  });

  wf.step('signoff', {
    type: 'deterministic',
    dependsOn: ['repair-review'],
    captureOutput: true,
    failOnError: true,
    command: [
      `if [ -f ${ART}/BLOCKED_NO_COMMIT.md ]; then echo "FACTORY_P4_BLOCKED"; cat ${ART}/BLOCKED_NO_COMMIT.md; exit 0; fi`,
      `printf "%s\\n" "# Factory p4 signoff (extraction)" "" "factory repo seeded + pushed (main), build + pack clean, history preserved." "PUBLISH is the operator gate — see PUBLISH_READY.md." "pear dep-swap is post-publish; Phase 1.5 (p5) removes the daemon." "" "FACTORY_P4_SEEDED_PUBLISH_PENDING" > ${ART}/signoff.md`,
      `cat ${ART}/signoff.md`,
    ].join(' && '),
  });

  const result = await wf.run({ cwd: PEAR });
  console.log(`[factory p4] done: ${result.status} (${result.id})`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
