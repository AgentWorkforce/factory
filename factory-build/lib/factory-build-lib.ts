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

/** A worker (non-interactive) agent runs as a single subprocess call; give it a generous deadline. */
const WORKER_TIMEOUT_MS = 3_600_000;

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

/** Add the factory squad. Codex agents are WORKERS (interactive:false). */
function addSquad(wf: Wf, tier: FactoryWorkflowOptions['tier']): void {
  // impl-codex is a WORKER (`interactive: false`): relayflows spawns it via spawnAgent, which
  // runs the codex CLI as a single subprocess and resolves on process EXIT — never on a broker
  // "owner completion decision" that can drop. That owner-completion drop is what stalled
  // codex-as-an-interactive-step-owner at `implement` (p1/p2, 2026-06-16), and the conversation
  // shape before it. Workers are excluded from channel message edges (coordinator.js) — they
  // don't need them in a pipeline. claude reviewer/fixer stay interactive (reliable as owners +
  // fixer-claude is the .repairable repair agent). If a worker exits non-zero / skips its
  // artifact, the gate fails → .repairable hands to fixer-claude.
  wf.agent('impl-codex', {
    cli: 'codex',
    interactive: false,
    role: 'Primary implementer (worker). Implements the declared file targets per the spec in one pass, with tests/proofs.',
    retries: 2,
  });
  wf.agent('reviewer-claude', {
    cli: 'claude',
    preset: 'reviewer',
    role: 'Fresh-eyes reviewer (first pass). Reviews the post-implementation state from scratch.',
    retries: 1,
  });
  wf.agent('fixer-claude', {
    cli: 'claude',
    role: 'Review-finding fixer + repair agent. Repairs valid findings / red gates, adds/updates tests or proofs, reruns checks.',
    retries: 2,
  });
  if (tier === 'deep') {
    // Second review/fix pass (Codex) — also WORKERS, same completion-reliability reason.
    wf.agent('reviewer-codex', {
      cli: 'codex',
      interactive: false,
      preset: 'reviewer',
      role: 'Fresh-eyes reviewer (second pass, worker). Reviews the post-Claude-fix state from scratch.',
      retries: 1,
    });
    wf.agent('fixer-codex', {
      cli: 'codex',
      interactive: false,
      role: 'Second-pass review-finding fixer (worker). Repairs valid findings, adds/updates tests or proofs, reruns checks.',
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
      // TODO(issue-52): this archived factory-build workflow still uses its
      // local gh transport; it is not part of the shipped Factory CLI path.
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

/** Implementation: impl-codex worker (interactive:false → spawnAgent, exits on done). Terminal: 'implement'. */
function addImplementation(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  const targets = o.fileTargets.join(', ');
  const crossNote = o.crossRepoNote ? `\nCROSS-REPO: ${o.crossRepoNote}` : '';
  // impl-codex is a worker (declared interactive:false), so this agent step runs codex as a single
  // subprocess that completes on process exit — no broker owner-completion-decision to drop. The
  // spec is passed via the captured read-spec output. If codex exits non-zero or skips
  // self-reflection.md, the step/verification fails → .repairable hands off to fixer-claude.
  wf.step('implement', {
    agent: 'impl-codex',
    dependsOn: ['read-spec'],
    timeoutMs: WORKER_TIMEOUT_MS,
    task: j([
      `You are the implementer for factory issue ${o.id} — ${o.description}.`,
      `Full spec (also at ${PLANNING}/${o.specFile}):`,
      `{{steps.read-spec.output}}`,
      ``,
      `Declared file targets (do not edit outside these): ${targets}`,
      o.task,
      crossNote,
      ``,
      `Implement the spec end to end in a single pass — you are the sole implementer, do not wait for any other agent. Follow repo conventions (AGENTS.md / CLAUDE.md). Add or update tests/proofs for every testable change and run them.`,
      `When done, write ${dir}/self-reflection.md covering: changed files, spec coverage, tests/proofs run, repo-rule alignment, and remaining risks.`,
      `Do not stop until every declared file target is implemented and matches the spec.`,
    ]),
    verification: { type: 'file_exists', value: `${dir}/self-reflection.md` },
  });
}

/** Review ladder + signoff. Terminal step: 'signoff'. */
function addReviewLadder(wf: Wf, o: FactoryWorkflowOptions): void {
  const dir = artifactDir(o);
  const acc = o.acceptanceCmd;

  wf.step('change-detection', {
    type: 'deterministic',
    dependsOn: ['implement'],
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
    // Second fresh-eyes loop (Codex) — only for the crux / high-risk workflows. reviewer-codex
    // and fixer-codex are workers (interactive:false), so these agent steps complete on process
    // exit, not on a broker owner-completion-decision.
    wf.step('codex-review', {
      agent: 'reviewer-codex',
      dependsOn: ['claude-fix'],
      timeoutMs: WORKER_TIMEOUT_MS,
      task: j([
        `Second-pass fresh-eyes review of the post-Claude-fix state for issue ${o.id}. Read the changed files, git diff, repo rules, and the spec.`,
        `Write ${dir}/codex-review.md with actionable findings or NO_ISSUES_FOUND.`,
      ]),
      verification: { type: 'file_exists', value: `${dir}/codex-review.md` },
    });
    wf.step('codex-fix', {
      agent: 'fixer-codex',
      dependsOn: ['codex-review'],
      timeoutMs: WORKER_TIMEOUT_MS,
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
  // TODO(issue-52): Local transport uses gh (broker runs on the user's machine; gh is authed in
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
      `printf "%s\\n" "## Summary" "${o.prSummary}" "" "Factory issue ${o.id} (${o.slug}). Built autonomously via the relayflows factory squad loop (${o.tier} review)." "" "Spec: factory/planning/${o.specFile}" "" "## Review" "- [x] single-pass implement (impl-codex)" "- [x] ${o.tier === 'deep' ? 'Claude + Codex' : 'Claude'} fresh-eyes review/fix loop" "- [x] deterministic acceptance: ${o.acceptanceCmd}" > "$BODY"`,
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
  // RETRY: multiple workflows targeting the SAME repo (e.g. p1/p2/p3 → pear) call this
  // concurrently, and `git worktree add` locks .git/worktrees + refs — parallel adds on
  // one repo race and all-but-one fail. Retry with backoff until the lock frees.
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      quiet(`git -C "${repo}" worktree prune`);
      sh(`git -C "${repo}" worktree add -f -B "${o.branch}" "${wt}" origin/main`);
      lastErr = undefined;
      break;
    } catch (e) {
      lastErr = e;
      // staggered backoff (attempt seconds + per-branch offset) to avoid lockstep retries
      const jitter = (o.branch.length % 3) + 1;
      try { execSync(`sleep ${attempt + jitter}`); } catch { /* noop */ }
    }
  }
  if (lastErr) throw lastErr;
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
