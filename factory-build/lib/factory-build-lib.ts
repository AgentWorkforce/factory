/**
 * factory-build-lib.ts
 *
 * Shared builder for the Factory build-out workflow stack (issues p1–p13).
 * Encodes the Ricky/relayflows "serious implementation squad loop" once so each
 * wave file stays thin and consistent:
 *
 *   squad: lead-claude (lead + QA) · impl-codex (implementer) ·
 *          assist-opencode (assist) · shadow-claude (live shadow reviewer)
 *   review ladder: self-reflection → scoped change-detection →
 *          soft validate → repair → hard validate →
 *          claude review/fix/review-final/fix-final
 *          [deep] codex review/fix/review-final/fix-final →
 *          final validate → signoff
 *   ship: scoped commit → push → draft PR (gh, local transport) labeled
 *          `no-agent-relay-review` (disables the autonomous pr-reviewer bot).
 *
 * Import surface (all resolvable from the relayflows repo):
 *   - workflow            @relayflows/core
 *   - createGitHubStep    @relayflows/core/integrations/github   (reserved for cloud transport; see ship note)
 *   - ClaudeModels/CodexModels  @agent-relay/config
 *
 * Files live in relayflows (where @relayflows/core resolves) and are run by
 * ricky via:  relayflows run <abs path to this repo's workflow file>
 */

import { workflow } from '@relayflows/core';

export const AW_ROOT = '/Users/khaliqgant/Projects/AgentWorkforce';
export const PLANNING = `${AW_ROOT}/factory/planning`;
export const EPIC = `${PLANNING}/factory-cloud-watches-local-node-linear-issue.md`;
export const RULES = `${AW_ROOT}/ricky/workflows/shared/WORKFLOW_AUTHORING_RULES.md`;

export const REPOS: Record<string, string> = {
  pear: `${AW_ROOT}/pear`,
  cloud: `${AW_ROOT}/cloud`,
  relay: `${AW_ROOT}/relay`,
  factory: `${AW_ROOT}/factory`,
};

export const GH_SLUG: Record<string, string> = {
  pear: 'AgentWorkforce/pear',
  cloud: 'AgentWorkforce/cloud',
  relay: 'AgentWorkforce/relay',
  factory: 'AgentWorkforce/factory',
};

export interface FactoryWorkflowOptions {
  /** Issue id, e.g. 'p1'. */
  id: string;
  /** Short outcome slug, e.g. 'state-store-port'. */
  slug: string;
  /** One-line description. */
  description: string;
  /** Primary repo key (cwd + ship target): pear | cloud | relay | factory. */
  repo: keyof typeof REPOS & string;
  /** Branch to create/switch, e.g. 'ricky/factory-p1-state-store-port'. */
  branch: string;
  /** Planning doc filename under factory/planning/. */
  specFile: string;
  /** Repo-relative file targets the workflow is allowed to change. */
  fileTargets: string[];
  /** Deterministic acceptance command (build/test/typecheck) for this scope. */
  acceptanceCmd: string;
  /** Review depth. Low-risk refactors: 'standard'. Integration/crux: 'deep'. */
  tier: 'standard' | 'deep';
  /** The concrete implementation goal handed to the squad. */
  task: string;
  /** PR title (prefixed with [factory]). */
  prTitle: string;
  /** PR summary body. */
  prSummary: string;
  /** Open a draft PR at the end. Default true. */
  openPr?: boolean;
  /** Optional note for cross-repo workflows that also touch another repo. */
  crossRepoNote?: string;
}

type Wf = ReturnType<typeof workflow>;

const j = (lines: string[]) => lines.join('\n');

function artifactDir(o: FactoryWorkflowOptions): string {
  return `.workflow-artifacts/factory-${o.id}-${o.slug}`;
}

function scopedChangeCmd(targets: string[]): string {
  const t = targets.join(' ');
  return j([
    'set -e',
    `changed="$(git diff --name-only -- ${t}; git ls-files --others --exclude-standard -- ${t})"`,
    'if [ -z "$changed" ]; then echo "NO_CHANGES_DETECTED"; exit 1; fi',
    'echo "CHANGES_PRESENT"; echo "$changed"',
  ]);
}

/** Add the standard factory squad. */
function addSquad(wf: Wf, tier: FactoryWorkflowOptions['tier']): void {
  wf.agent('lead-claude', {
    cli: 'claude',
    role: 'Lead + QA. Plans, assigns impl-codex and assist-opencode, watches the channel, runs QA repair on red gates, and exits only when the declared file targets are implemented and the implementer self-reflection is written.',
    retries: 1,
  });
  wf.agent('impl-codex', {
    cli: 'codex',
    role: 'Primary implementer. Implements the declared file targets per the spec and the lead plan.',
    retries: 2,
  });
  wf.agent('assist-opencode', {
    cli: 'opencode',
    role: 'Assisting implementer. Picks up secondary files, tests, and fixtures the lead assigns; never edits the same file as impl-codex concurrently.',
    retries: 2,
  });
  wf.agent('shadow-claude', {
    cli: 'claude',
    role: 'Live shadow reviewer. Reads actual files and channel updates while work is happening and posts concise spec-drift feedback before the implementers exit.',
    retries: 1,
  });
  wf.agent('reviewer-claude', {
    cli: 'claude',
    preset: 'reviewer',
    role: 'Fresh-eyes reviewer (first pass). Reviews the post-implementation state from scratch.',
    retries: 1,
  });
  wf.agent('fixer-claude', {
    cli: 'claude',
    role: 'Review-finding fixer. Repairs valid findings, adds/updates tests or proofs, reruns checks.',
    retries: 2,
  });
  if (tier === 'deep') {
    wf.agent('reviewer-codex', {
      cli: 'codex',
      preset: 'reviewer',
      role: 'Fresh-eyes reviewer (second pass). Reviews the post-Claude-fix state from scratch.',
      retries: 1,
    });
    wf.agent('fixer-codex', {
      cli: 'codex',
      role: 'Second-pass review-finding fixer. Repairs valid findings, adds/updates tests or proofs, reruns checks.',
      retries: 2,
    });
  }
}

/** Preflight + spec read. Terminal step: 'read-spec'. */
function addSetup(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  wf.step('preflight', {
    type: 'deterministic',
    captureOutput: true,
    failOnError: true,
    command: j([
      'set -e',
      'git rev-parse --show-toplevel >/dev/null',
      'git fetch origin --quiet || true',
      `git checkout -B ${o.branch}`,
      'gh auth status >/dev/null 2>&1 || { echo "MISSING_ENV_VAR: gh auth (run: gh auth login)"; exit 1; }',
      `mkdir -p ${dir}`,
      'echo PREFLIGHT_OK',
    ]),
  });
  wf.step('read-spec', {
    type: 'deterministic',
    dependsOn: ['preflight'],
    captureOutput: true,
    failOnError: true,
    command: j([
      'set -e',
      `echo "===== ISSUE SPEC: ${o.specFile} =====" `,
      `cat ${PLANNING}/${o.specFile}`,
      'echo "===== EPIC (context) ====="',
      `sed -n "1,120p" ${EPIC}`,
      'echo "===== AUTHORING RULES (context) ====="',
      `sed -n "1,60p" ${RULES} 2>/dev/null || true`,
      'echo READ_SPEC_OK',
    ]),
  });
}

/** Implementation (conversation shape). Terminal step: 'lead-coordinate'. */
function addImplementation(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  const targets = o.fileTargets.join(', ');
  const crossNote = o.crossRepoNote
    ? `\nCROSS-REPO: ${o.crossRepoNote}`
    : '';
  wf.step('lead-coordinate', {
    agent: 'lead-claude',
    dependsOn: ['read-spec'],
    task: j([
      `You are lead-claude on this channel. Workers: impl-codex (primary), assist-opencode (assist). Shadow: shadow-claude.`,
      `Issue: ${o.id} — ${o.description}`,
      `Full spec (read it in full at ${PLANNING}/${o.specFile}):`,
      `{{steps.read-spec.output}}`,
      ``,
      `Declared file targets (do not edit outside these): ${targets}`,
      o.task,
      crossNote,
      ``,
      `Run the squad: post the plan, assign files (no two agents edit the same file at once), let shadow-claude flag drift, review their work, and iterate in-channel until the spec is satisfied.`,
      `Before you exit, confirm impl-codex wrote ${dir}/self-reflection.md.`,
      `Exit only when the declared file targets are implemented and the work matches the spec. End your final message with LEAD_DONE.`,
    ]),
  });
  wf.step('impl-work', {
    agent: 'impl-codex',
    dependsOn: ['read-spec'],
    task: j([
      `You are impl-codex. Wait for lead-claude's plan on the channel, then implement your assigned file targets for issue ${o.id}.`,
      `Spec: {{steps.read-spec.output}}`,
      `Declared file targets: ${targets}`,
      o.task,
      crossNote,
      `Follow repo conventions (AGENTS.md / CLAUDE.md). Add or update tests/proofs for testable changes.`,
      `When done, write ${dir}/self-reflection.md covering: changed files, spec coverage, tests/proofs run, repo-rule alignment, and remaining risks. Post a completion message and address lead/shadow feedback.`,
    ]),
  });
  wf.step('assist-work', {
    agent: 'assist-opencode',
    dependsOn: ['read-spec'],
    task: j([
      `You are assist-opencode. Wait for lead-claude's assignment, then implement the secondary files / tests / fixtures you are given for issue ${o.id}.`,
      `Spec: {{steps.read-spec.output}}`,
      `Never edit a file impl-codex is actively editing. Post completion messages and address feedback.`,
    ]),
  });
  wf.step('shadow-review', {
    agent: 'shadow-claude',
    dependsOn: ['read-spec'],
    task: j([
      `You are shadow-claude, the live shadow reviewer for issue ${o.id}. As impl-codex and assist-opencode work, read the actual changed files and the channel, and post concise, specific feedback when you see spec drift, missed file targets, or repo-rule violations.`,
      `Spec: {{steps.read-spec.output}}`,
      `Declared file targets: ${targets}. Do not implement; review and steer. End with SHADOW_DONE when the implementers have addressed your feedback.`,
    ]),
  });
}

/** Review ladder + signoff. Terminal step: 'signoff'. */
function addReviewLadder(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  const acc = o.acceptanceCmd;

  wf.step('change-detection', {
    type: 'deterministic',
    dependsOn: ['lead-coordinate'],
    captureOutput: true,
    failOnError: true,
    command: scopedChangeCmd(o.fileTargets),
  });
  wf.step('self-reflection-gate', {
    type: 'deterministic',
    dependsOn: ['lead-coordinate'],
    captureOutput: true,
    failOnError: false,
    command: `test -f ${dir}/self-reflection.md && echo SELF_REFLECTION_OK || { echo "MISSING_SELF_REFLECTION"; exit 1; }`,
  });
  wf.step('repair-self-reflection', {
    agent: 'impl-codex',
    dependsOn: ['self-reflection-gate'],
    task: `If ${dir}/self-reflection.md is missing, write it now (changed files, spec coverage, tests/proofs, repo-rule alignment, risks). Output:\n{{steps.self-reflection-gate.output}}`,
    verification: { type: 'file_exists', value: `${dir}/self-reflection.md` },
  });

  // Soft → repair → hard validation.
  wf.step('validate-soft', {
    type: 'deterministic',
    dependsOn: ['change-detection', 'repair-self-reflection'],
    captureOutput: true,
    failOnError: false,
    command: `${acc} 2>&1 | tail -80; echo "EXIT: $?"`,
  });
  wf.step('repair-validate', {
    agent: 'lead-claude',
    dependsOn: ['validate-soft'],
    task: j([
      `Acting as QA. If validation passed, summarize the green evidence.`,
      `If it failed, use this output to assign and fix issues, then rerun "${acc}" locally until green:`,
      `{{steps.validate-soft.output}}`,
    ]),
    verification: { type: 'exit_code' },
  });
  wf.step('validate-hard', {
    type: 'deterministic',
    dependsOn: ['repair-validate'],
    captureOutput: true,
    failOnError: false,
    command: `${acc} 2>&1 | tail -80; echo "EXIT: $?"`,
  });

  // Claude fresh-eyes loop.
  wf.step('claude-review', {
    agent: 'reviewer-claude',
    dependsOn: ['validate-hard'],
    task: j([
      `First-pass fresh-eyes review of the post-implementation state for issue ${o.id}.`,
      `Read the actual changed files, git diff, repo rules (AGENTS.md/CLAUDE.md), the spec, and validation output:`,
      `{{steps.validate-hard.output}}`,
      `Write ${dir}/claude-review.md with actionable findings (file + required fix + required test) or NO_ISSUES_FOUND.`,
    ]),
    verification: { type: 'file_exists', value: `${dir}/claude-review.md` },
  });
  wf.step('claude-fix', {
    agent: 'fixer-claude',
    dependsOn: ['claude-review'],
    task: j([
      `Read ${dir}/claude-review.md. Fix every valid finding, add/update tests or proofs, rerun the relevant checks, and keep iterating locally until this round has no remaining valid issues.`,
      `Write ${dir}/claude-fix.md with fixes and commands run. If the review said NO_ISSUES_FOUND, record that no fix was needed.`,
    ]),
    verification: { type: 'exit_code' },
  });
  wf.step('claude-review-final', {
    agent: 'reviewer-claude',
    dependsOn: ['claude-fix'],
    task: j([
      `Fresh post-fix review from scratch (do not rely on prior review text or the fixer summary). Read files, diff, rules, spec.`,
      `Write ${dir}/claude-review-final.md with findings or NO_ISSUES_FOUND.`,
    ]),
    verification: { type: 'file_exists', value: `${dir}/claude-review-final.md` },
  });
  wf.step('claude-fix-final', {
    agent: 'fixer-claude',
    dependsOn: ['claude-review-final'],
    task: j([
      `If ${dir}/claude-review-final.md has findings, fix them, add/update tests or proofs, and rerun checks until green.`,
      `If something cannot be fixed safely, write ${dir}/BLOCKED_NO_COMMIT.md with exact evidence and stop.`,
      `If NO_ISSUES_FOUND, record Claude signoff in ${dir}/claude-signoff.md.`,
    ]),
    verification: { type: 'exit_code' },
  });

  let lastReviewStep = 'claude-fix-final';

  if (o.tier === 'deep') {
    wf.step('validate-after-claude', {
      type: 'deterministic',
      dependsOn: ['claude-fix-final'],
      captureOutput: true,
      failOnError: false,
      command: `test ! -f ${dir}/BLOCKED_NO_COMMIT.md && ${acc} 2>&1 | tail -80; echo "EXIT: $?"`,
    });
    wf.step('codex-review', {
      agent: 'reviewer-codex',
      dependsOn: ['validate-after-claude'],
      task: j([
        `Second-pass fresh-eyes review of the post-Claude-fix state for issue ${o.id}.`,
        `Read the actual changed files, git diff, repo rules, the spec, and validation output:`,
        `{{steps.validate-after-claude.output}}`,
        `Write ${dir}/codex-review.md with actionable findings or NO_ISSUES_FOUND.`,
      ]),
      verification: { type: 'file_exists', value: `${dir}/codex-review.md` },
    });
    wf.step('codex-fix', {
      agent: 'fixer-codex',
      dependsOn: ['codex-review'],
      task: `Read ${dir}/codex-review.md. Fix every valid finding, add/update tests or proofs, rerun checks until this round is clean. Write ${dir}/codex-fix.md. If NO_ISSUES_FOUND, record that no fix was needed.`,
      verification: { type: 'exit_code' },
    });
    wf.step('codex-review-final', {
      agent: 'reviewer-codex',
      dependsOn: ['codex-fix'],
      task: `Fresh post-Codex-fix review from scratch. Write ${dir}/codex-review-final.md with findings or NO_ISSUES_FOUND.`,
      verification: { type: 'file_exists', value: `${dir}/codex-review-final.md` },
    });
    wf.step('codex-fix-final', {
      agent: 'fixer-codex',
      dependsOn: ['codex-review-final'],
      task: j([
        `If ${dir}/codex-review-final.md has findings, fix them, add/update tests or proofs, rerun checks until green.`,
        `If something cannot be fixed safely, write ${dir}/BLOCKED_NO_COMMIT.md with exact evidence.`,
        `If NO_ISSUES_FOUND, record Codex signoff in ${dir}/codex-signoff.md.`,
      ]),
      verification: { type: 'exit_code' },
    });
    lastReviewStep = 'codex-fix-final';
  }

  // Green-or-blocked: a red final acceptance writes BLOCKED_NO_COMMIT so signoff
  // and ship are skipped — the workflow never signs off red work as complete.
  wf.step('final-validate', {
    type: 'deterministic',
    dependsOn: [lastReviewStep],
    captureOutput: true,
    failOnError: false,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "ALREADY_BLOCKED"; cat ${dir}/BLOCKED_NO_COMMIT.md; exit 0; fi`,
      `if ! { ${acc}; }; then printf "%s\\n" "# BLOCKED_NO_COMMIT" "Final acceptance was red for issue ${o.id}." "Command: ${acc}" "Fix the failing gate and re-run this workflow before shipping." > ${dir}/BLOCKED_NO_COMMIT.md; echo "ACCEPTANCE_RED -> BLOCKED"; exit 0; fi`,
      'echo FINAL_VALIDATE_GREEN',
    ]),
  });
  wf.step('signoff', {
    type: 'deterministic',
    dependsOn: ['final-validate'],
    captureOutput: true,
    failOnError: true,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "FACTORY_${o.id.toUpperCase()}_BLOCKED"; exit 0; fi`,
      `printf "%s\\n" "# Factory ${o.id} signoff (${o.slug})" "" "Spec: ${o.specFile}" "Targets: ${o.fileTargets.join(' ')}" "Review tier: ${o.tier}" "" "FACTORY_${o.id.toUpperCase()}_COMPLETE" > ${dir}/signoff.md`,
      `cat ${dir}/signoff.md`,
    ]),
  });
}

/** Scoped commit + push + draft PR (local gh transport). Terminal: 'verify-pr'. */
function addShip(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  const gh = GH_SLUG[o.repo];
  const addPaths = `${o.fileTargets.join(' ')} ${dir}`;
  wf.step('commit', {
    type: 'deterministic',
    dependsOn: ['signoff'],
    captureOutput: true,
    failOnError: false,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "BLOCKED — no commit"; exit 0; fi`,
      'set -e',
      `git add ${addPaths}`,
      `git commit -m "${o.prTitle}" -m "Factory issue ${o.id}. Autonomous build via relayflows squad loop." || echo "NOTHING_TO_COMMIT"`,
      'echo COMMIT_DONE',
    ]),
  });
  wf.step('repair-commit', {
    agent: 'lead-claude',
    dependsOn: ['commit'],
    task: `If the commit failed (and the issue is not BLOCKED), fix the blocker, re-stage only the declared targets, and commit. If it succeeded or is blocked, confirm. Output:\n{{steps.commit.output}}`,
    verification: { type: 'exit_code' },
  });
  wf.step('push', {
    type: 'deterministic',
    dependsOn: ['repair-commit'],
    captureOutput: true,
    failOnError: false,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "BLOCKED — no push"; exit 0; fi`,
      `git push -u origin ${o.branch} 2>&1 | tail -20`,
    ]),
  });
  // Local transport uses gh (broker runs on the user's machine; gh is authed in
  // preflight). For cloud execution, swap this for createGitHubStep({action:'createPR'}).
  // `no-agent-relay-review` label disables the autonomous pr-reviewer bot that
  // otherwise pushes unreviewed commits to held draft PRs.
  wf.step('open-pr', {
    type: 'deterministic',
    dependsOn: ['push'],
    captureOutput: true,
    failOnError: false,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "BLOCKED — no PR"; exit 0; fi`,
      'BODY=$(mktemp)',
      `printf "%s\\n" "## Summary" "${o.prSummary}" "" "Factory issue ${o.id} (${o.slug}). Built autonomously via the relayflows factory squad loop (${o.tier} review)." "" "Spec: factory/planning/${o.specFile}" "" "## Review" "- [x] squad implement + live shadow review" "- [x] ${o.tier === 'deep' ? 'Claude + Codex' : 'Claude'} fresh-eyes review/fix loop" "- [x] deterministic acceptance: ${o.acceptanceCmd}" > "$BODY"`,
      `gh pr view ${o.branch} --json url >/dev/null 2>&1 || gh pr create --repo ${gh} --base main --head ${o.branch} --title "${o.prTitle}" --body-file "$BODY" --draft`,
      `gh pr edit ${o.branch} --repo ${gh} --add-label no-agent-relay-review 2>/dev/null || true`,
      'rm -f "$BODY"',
      'echo OPEN_PR_DONE',
    ]),
  });
  wf.step('verify-pr', {
    type: 'deterministic',
    dependsOn: ['open-pr'],
    captureOutput: true,
    failOnError: false,
    command: j([
      `if [ -f ${dir}/BLOCKED_NO_COMMIT.md ]; then echo "FACTORY_${o.id.toUpperCase()}_BLOCKED_NO_PR"; exit 0; fi`,
      `gh pr view ${o.branch} --repo ${gh} --json url,isDraft,state || { echo "PR_NOT_FOUND"; exit 1; }`,
    ]),
  });
}

/** Build the full workflow (mutable builder). */
export function buildFactoryWorkflow(o: FactoryWorkflowOptions): Wf {
  const wf = workflow(`factory-${o.id}-${o.slug}`)
    .description(`[factory ${o.id}] ${o.description}`)
    .pattern('dag')
    .channel(`wf-factory-${o.id}-${o.slug}`)
    .maxConcurrency(4)
    .timeout(7_200_000)
    .onError('retry', { maxRetries: 2, retryDelayMs: 10_000 });

  addSquad(wf, o.tier);
  addSetup(wf, o);
  addImplementation(wf, o);
  addReviewLadder(wf, o);
  if (o.openPr !== false) {
    addShip(wf, o);
  }
  return wf;
}

/** Build + run with the correct repo cwd. */
export async function runFactoryWorkflow(o: FactoryWorkflowOptions): Promise<void> {
  const cwd = REPOS[o.repo];
  if (!cwd) {
    throw new Error(`Unknown repo "${o.repo}" — expected one of ${Object.keys(REPOS).join(', ')}`);
  }
  const wf = buildFactoryWorkflow(o);
  const result = await wf.run({ cwd });
  console.log(`[factory ${o.id}] done: ${result.status} (${result.id})`);
}
