/**
 * factory-build-lib.ts
 *
 * Shared builder for the Factory build-out workflow stack (issues p1–p13).
 * Encodes the Ricky/relayflows "serious implementation squad loop" once so each
 * wave file stays thin and consistent:
 *
 *   squad: lead-claude (lead + QA) · impl-codex (implementer) ·
 *          shadow-claude (live shadow reviewer)
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
import { execSync } from 'node:child_process';

export const AW_ROOT = '/Users/khaliqgant/Projects/AgentWorkforce';
/** Isolated git worktrees so parallel workflows never share a working tree. */
export const WORKTREES = `${AW_ROOT}/.factory-worktrees`;
export const PLANNING = `${AW_ROOT}/factory/planning`;
export const EPIC = `${PLANNING}/factory-unified-node-architecture-linear-issue.md`;
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
    role: 'Lead + QA. Plans, assigns impl-codex, watches the channel with shadow-claude, runs QA repair on red gates, and exits only when the declared file targets are implemented and the implementer self-reflection is written.',
    retries: 1,
  });
  wf.agent('impl-codex', {
    cli: 'codex',
    role: 'Primary implementer. Implements the declared file targets per the spec and the lead plan.',
    retries: 2,
  });
  // assist-opencode removed: OpenCode TUI never runs the injected prompt headlessly,
  // and even rebound to claude the assist step fails `owner completion decision missing`
  // and FAILS the run (p11/p3, 2026-06-16). lead + impl + shadow is the reliable squad.
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
      // Already in the isolated worktree on this branch (setupWorktree did the checkout).
      `git rev-parse --abbrev-ref HEAD | grep -qx "${o.branch}" || { echo "not on ${o.branch}"; exit 1; }`,
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
      `You are lead-claude on this channel. Worker: impl-codex (primary implementer). Shadow: shadow-claude.`,
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
  // NOTE: the former 'assist-work' step (assist-opencode) is removed. OpenCode's TUI
  // never runs the injected prompt headlessly (splash-only), and even rebound to claude
  // the step routinely fails `owner completion decision missing` and FAILS the whole run
  // (observed on p11/p3, 2026-06-16). lead + impl + shadow is a complete, reliable squad.
  wf.step('shadow-review', {
    agent: 'shadow-claude',
    dependsOn: ['read-spec'],
    task: j([
      `You are shadow-claude, the live shadow reviewer for issue ${o.id}. As impl-codex works, read the actual changed files and the channel, and post concise, specific feedback when you see spec drift, missed file targets, or repo-rule violations.`,
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
  // Single acceptance gate. A red gate auto-invokes the repair agent and reruns it
  // (.repairable()), so it self-heals without verbose soft/repair/hard scaffolding.
  wf.step('validate', {
    type: 'deterministic',
    dependsOn: ['change-detection'],
    captureOutput: true,
    failOnError: true,
    command: `${acc}`,
  });

  // One fresh-eyes review/fix loop (Claude) — catches logic the gates miss.
  wf.step('claude-review', {
    agent: 'reviewer-claude',
    dependsOn: ['validate'],
    task: j([
      `Fresh-eyes review of the post-implementation state for issue ${o.id}. Read the changed files, git diff, repo rules (AGENTS.md/CLAUDE.md), and the spec.`,
      `Write ${dir}/claude-review.md with actionable findings (file + required fix + required test) or NO_ISSUES_FOUND.`,
    ]),
    verification: { type: 'file_exists', value: `${dir}/claude-review.md` },
  });
  wf.step('claude-fix', {
    agent: 'fixer-claude',
    dependsOn: ['claude-review'],
    task: j([
      `Read ${dir}/claude-review.md. Fix every valid finding, add/update tests or proofs, rerun the relevant checks until clean. Do NOT skip — keep fixing.`,
      `Write ${dir}/claude-fix.md with fixes and commands run. If NO_ISSUES_FOUND, record that no fix was needed.`,
    ]),
    verification: { type: 'exit_code' },
  });

  let lastReviewStep = 'claude-fix';

  if (o.tier === 'deep') {
    // Second fresh-eyes loop (Codex) — only for the crux / high-risk workflows.
    wf.step('codex-review', {
      agent: 'reviewer-codex',
      dependsOn: ['claude-fix'],
      task: j([
        `Second-pass fresh-eyes review of the post-Claude-fix state for issue ${o.id}. Read the changed files, git diff, repo rules, and the spec.`,
        `Write ${dir}/codex-review.md with actionable findings or NO_ISSUES_FOUND.`,
      ]),
      verification: { type: 'file_exists', value: `${dir}/codex-review.md` },
    });
    wf.step('codex-fix', {
      agent: 'fixer-codex',
      dependsOn: ['codex-review'],
      task: `Read ${dir}/codex-review.md. Fix every valid finding, add/update tests or proofs, rerun checks until clean. Do NOT skip — keep fixing. Write ${dir}/codex-fix.md. If NO_ISSUES_FOUND, record that no fix was needed.`,
      verification: { type: 'exit_code' },
    });
    lastReviewStep = 'codex-fix';
  }

  // Repair-not-skip: final acceptance fails hard on red, which (under .repairable())
  // auto-invokes the repair agent to fix it and reruns the gate — never skips, never
  // signs off red work. Only an exhausted repair budget can end the run unfixed.
  wf.step('final-validate', {
    type: 'deterministic',
    dependsOn: [lastReviewStep],
    captureOutput: true,
    failOnError: true,
    command: `${acc}`,
  });
  wf.step('signoff', {
    type: 'deterministic',
    dependsOn: ['final-validate'],
    captureOutput: true,
    failOnError: true,
    command: j([
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
    failOnError: true,
    command: j([
      'set -e',
      `git add ${addPaths}`,
      `git commit -m "${o.prTitle}" -m "Factory issue ${o.id}. Autonomous build via relayflows squad loop." || echo "NOTHING_TO_COMMIT"`,
      'echo COMMIT_DONE',
    ]),
  });
  wf.step('push', {
    type: 'deterministic',
    dependsOn: ['commit'],
    captureOutput: true,
    failOnError: true,
    command: `git push -u origin ${o.branch} 2>&1 | tail -20`,
  });
  // Local transport uses gh (broker runs on the user's machine; gh is authed in
  // preflight). For cloud execution, swap this for createGitHubStep({action:'createPR'}).
  // `no-agent-relay-review` label disables the autonomous pr-reviewer bot that
  // otherwise pushes unreviewed commits to held draft PRs.
  wf.step('open-pr', {
    type: 'deterministic',
    dependsOn: ['push'],
    captureOutput: true,
    failOnError: true,
    command: j([
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
    failOnError: true,
    command: `gh pr view ${o.branch} --repo ${gh} --json url,isDraft,state || { echo "PR_NOT_FOUND"; exit 1; }`,
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
    // Repair-not-skip: any failing gate auto-invokes the repair agent to FIX it and
    // reruns the gate, up to repairRetries times — it never skips/blocks. 5 is enough
    // to self-heal real issues without a runaway loop eating hours on a stuck gate.
    // onExhaustion:'needs-human' (relayflows#6) → an exhausted repair budget ends the run
    // 'needs_human' (handled, awaiting a human) instead of 'failed' — so a genuine run
    // failure is unreachable short of a real crash.
    .repairable({ repairRetries: 5, maxRetries: 5, retryDelayMs: 5_000, repairAgent: 'fixer-claude', onExhaustion: 'needs-human' });

  addSquad(wf, o.tier);
  addSetup(wf, o);
  addImplementation(wf, o);
  addReviewLadder(wf, o);
  if (o.openPr !== false) {
    addShip(wf, o);
  }
  return wf;
}

/**
 * Create an isolated git worktree for this workflow off origin/main, so parallel
 * workflows on the same repo never share a working tree (the wave1 collision bug).
 * node_modules is symlinked from the parent repo so build/test resolve deps without
 * a fresh (slow) install per worktree.
 */
export function setupWorktree(o: FactoryWorkflowOptions): string {
  const repo = REPOS[o.repo];
  if (!repo) {
    throw new Error(`Unknown repo "${o.repo}" — expected one of ${Object.keys(REPOS).join(', ')}`);
  }
  const wt = `${WORKTREES}/${o.repo}-${o.id}-${o.slug}`;
  const sh = (cmd: string) => execSync(cmd, { stdio: 'pipe' });
  const quiet = (cmd: string) => { try { sh(cmd); } catch { /* best effort */ } };
  quiet(`git -C "${repo}" worktree remove --force "${wt}"`);
  quiet(`rm -rf "${wt}"`);
  sh(`mkdir -p "${WORKTREES}"`);
  quiet(`git -C "${repo}" fetch origin --quiet`);
  // -B resets the branch to a fresh checkout off the latest main, in an isolated dir.
  sh(`git -C "${repo}" worktree add -f -B "${o.branch}" "${wt}" origin/main`);
  // Symlink deps (gitignored, so not in the worktree). Read-mostly; build/test resolve up-tree.
  quiet(`test -d "${repo}/node_modules" && ln -snf "${repo}/node_modules" "${wt}/node_modules"`);
  return wt;
}

/** Build + run in an isolated worktree (parallel-safe). */
export async function runFactoryWorkflow(o: FactoryWorkflowOptions): Promise<void> {
  // FACTORY_BUILD_DRY=1 (set by the runner on --dry-run) skips the real worktree
  // side-effect and validates against the repo root instead.
  const cwd = process.env.FACTORY_BUILD_DRY === '1' ? REPOS[o.repo] : setupWorktree(o);
  if (process.env.FACTORY_BUILD_DRY !== '1') console.log(`[factory ${o.id}] worktree: ${cwd}`);
  const wf = buildFactoryWorkflow(o);
  const result = await wf.run({ cwd });
  console.log(`[factory ${o.id}] done: ${result.status} (${result.id})`);
}
