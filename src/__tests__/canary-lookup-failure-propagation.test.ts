import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'

import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

// The P0 canary reports on one thing: whether the dispatch pipeline works. Its
// failure reports are only worth acting on if it can tell apart
//   (a) dispatch never produced the branch/PR   -> a real P0,
//   (b) its own GitHub lookups broke            -> says nothing about dispatch,
//   (c) a PR appeared from the wrong identity   -> a credential regression.
// Collapsing any of these into another is the defect class these tests guard.
//
// Everything below is driven from the workflow file itself -- the wait step's
// script, and the lookups extracted from it -- so an assertion cannot drift
// away from what CI actually runs.
const WORKFLOW = join(import.meta.dirname, '../../.github/workflows/verify-p0-canary.yml')

const EXPECTED_AUTHOR = 'app/agent-relay-code'
const CANARY_BRANCH = 'factory/1234-canary'

function waitStepRun(): string {
  const workflow = parse(readFileSync(WORKFLOW, 'utf8'))
  const steps = workflow.jobs.verify.steps as Array<{ id?: string; run?: string }>
  const wait = steps.find((step) => step.id === 'wait')
  if (!wait?.run) throw new Error('no step with id "wait" in the canary workflow')
  return wait.run
}

/** The `gh` lookups in the wait step, verbatim, as `[name, snippet]`. */
function extractLookups(): Array<[string, string]> {
  const lookups: Array<[string, string]> = []
  const lines = waitStepRun().split('\n')
  for (let i = 0; i < lines.length; i++) {
    const opens = /^\s*if ! ([A-Z_]+)="\$\(gh /.exec(lines[i])
    if (!opens) continue
    const collected = [lines[i]]
    while (!/\)"; then\s*$/.test(collected[collected.length - 1])) {
      i++
      if (i >= lines.length) throw new Error(`unterminated lookup for ${opens[1]}`)
      collected.push(lines[i])
    }
    // Strip the `if ! ` / `; then` wrapper to leave a bare assignment.
    const snippet = collected.join('\n').replace(/^(\s*)if ! /, '$1').replace(/; then\s*$/, '')
    lookups.push([opens[1], snippet])
  }
  return lookups
}

/**
 * A `gh` stand-in that applies `--jq` the way the real one does, so a jq
 * program built from untrusted input is exercised rather than skipped.
 * `branches`/`prs` are the canned API payloads; `failFirst` makes the first N
 * invocations exit non-zero, standing in for a transient 5xx.
 */
function writeGhStub(
  dir: string,
  opts: { branches?: unknown; prs?: unknown; failFirst?: number; failAlways?: boolean },
): void {
  writeFileSync(join(dir, 'branches.json'), JSON.stringify(opts.branches ?? []))
  writeFileSync(join(dir, 'prs.json'), JSON.stringify(opts.prs ?? []))
  const stub = join(dir, 'gh')
  writeFileSync(
    stub,
    `#!/usr/bin/env bash
DIR=${JSON.stringify(dir)}
COUNT_FILE="$DIR/count"
n=$(cat "$COUNT_FILE" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$COUNT_FILE"
${opts.failAlways ? 'exit 22' : ''}
if [ "$n" -le ${opts.failFirst ?? 0} ]; then exit 22; fi

# Which endpoint is being called decides the payload.
case "$1" in
  api) PAYLOAD="$DIR/branches.json" ;;
  pr)  PAYLOAD="$DIR/prs.json" ;;
  *)   PAYLOAD="$DIR/prs.json" ;;
esac

# Mirror \`gh --jq\`: the program is applied to the response body.
prog=""
while [ $# -gt 0 ]; do
  if [ "$1" = "--jq" ]; then prog="$2"; shift 2; else shift; fi
done
if [ -n "$prog" ]; then jq -r "$prog" < "$PAYLOAD"; else cat "$PAYLOAD"; fi
`,
  )
  chmodSync(stub, 0o755)
}

/**
 * Run the real wait-step script with `gh`, the clock and `sleep` tamed.
 *
 * `windowSeconds` compresses the 45-minute poll window. It has to leave room
 * for several polls -- each one spawns processes and costs the better part of
 * a second here -- so a test that needs N polls must not be given a window
 * that only fits N-1, or it measures the harness instead of the workflow.
 */
function runWaitStep(
  opts: Parameters<typeof writeGhStub>[1],
  windowSeconds = 12,
): {
  status: number
  output: string
  ghOutputs: string
} {
  const dir = mkdtempSync(join(tmpdir(), 'canary-wait-'))
  writeGhStub(dir, opts)

  const ghOutput = join(dir, 'gh-output')
  const summary = join(dir, 'summary')
  writeFileSync(ghOutput, '')
  writeFileSync(summary, '')

  const script = waitStepRun()
    // The workflow-expression values the step would be handed at runtime.
    .replace(/\$\{\{\s*steps\.file\.outputs\.issue_number\s*\}\}/g, '1234')
    .replace(/\$\{\{\s*steps\.file\.outputs\.issue_url\s*\}\}/g, 'https://example.invalid/issues/1234')
    // Keep the poll structure, drop the wall-clock cost of it: the loop, its
    // deadline test and the classification all still run.
    .replace(/\bsleep 60\b/g, 'sleep 0')
    .replace(/WAIT_MINUTES \* 60/g, `WAIT_MINUTES * ${windowSeconds}`)

  const result = spawnSync('bash', ['-c', script], {
    encoding: 'utf8',
    env: {
      ...process.env,
      PATH: `${dir}:${process.env.PATH ?? ''}`,
      WAIT_MINUTES: '1',
      TARGET_REPO: 'AgentWorkforce/factory',
      EXPECTED_PR_AUTHOR: EXPECTED_AUTHOR,
      GITHUB_OUTPUT: ghOutput,
      GITHUB_STEP_SUMMARY: summary,
    },
  })

  return {
    status: result.status ?? -1,
    output: `${result.stdout ?? ''}\n${result.stderr ?? ''}\n${readFileSync(summary, 'utf8')}`,
    ghOutputs: readFileSync(ghOutput, 'utf8'),
  }
}

const provenPr = [
  {
    number: 42,
    headRefName: CANARY_BRANCH,
    url: 'https://example.invalid/pr/42',
    author: { login: EXPECTED_AUTHOR },
  },
]
const provenBranches = [{ name: CANARY_BRANCH }]

describe('P0 canary lookup failure handling', () => {
  const lookups = extractLookups()

  // Extraction silently finding nothing would make the per-lookup cases vacuous.
  it('extracts all three gh lookups from the wait step', () => {
    expect(lookups.map(([name]) => name)).toEqual(['BRANCH', 'PR_LINE', 'FOREIGN_PR'])
  })

  // MUST FIRE: a failing gh must not read as "found nothing". If it did, the
  // loop would poll out the full window and report a dispatch outage.
  it.each(lookups)('%s reports a gh failure rather than an empty result', (_name, snippet) => {
    const dir = mkdtempSync(join(tmpdir(), 'canary-lookup-'))
    writeGhStub(dir, { failAlways: true })
    const script = [
      'set -euo pipefail',
      'TARGET_REPO=AgentWorkforce/factory',
      'NUM=1234',
      `BRANCH=${JSON.stringify(CANARY_BRANCH)}`,
      `EXPECTED_PR_AUTHOR=${EXPECTED_AUTHOR}`,
      snippet,
    ].join('\n')
    const result = spawnSync('bash', ['-c', script], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${dir}:${process.env.PATH ?? ''}` },
    })
    expect(result.status).not.toBe(0)
  })

  // MUST NOT FIRE (control): the success path still yields the first line and
  // exits 0. This is the case `|| true` was protecting -- a consumer that
  // closes the pipe early can SIGPIPE the producer -- so it has to keep
  // passing, or the fix trades a false green for a false red.
  it('the branch lookup takes the first line and succeeds when gh succeeds', () => {
    const dir = mkdtempSync(join(tmpdir(), 'canary-lookup-'))
    writeGhStub(dir, { branches: [{ name: CANARY_BRANCH }, { name: 'factory/1234-second' }] })
    const snippet = lookups.find(([name]) => name === 'BRANCH')![1]
    const script = [
      'set -euo pipefail',
      'TARGET_REPO=AgentWorkforce/factory',
      'NUM=1234',
      'BRANCH=""',
      snippet,
      'printf "VALUE=%s" "${BRANCH}"',
    ].join('\n')
    const result = spawnSync('bash', ['-c', script], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${dir}:${process.env.PATH ?? ''}` },
    })
    expect(result.status).toBe(0)
    expect(result.stdout).toContain(`VALUE=${CANARY_BRANCH}`)
  })
})

describe('P0 canary distinguishes a broken lookup from a dispatch outage', () => {
  // MUST FIRE: persistent lookup failure must fail, and must say it was the
  // canary's own API access that broke -- not report a dispatch outage.
  it('fails loudly, and as INCONCLUSIVE, when lookups keep failing', () => {
    const { status, output } = runWaitStep({ failAlways: true })
    expect(status).not.toBe(0)
    expect(output).toContain('NOT a dispatch outage')
    expect(output).toContain('INCONCLUSIVE')
    // The dispatch-timeout verdict must NOT be claimed on this path.
    expect(output).not.toContain('did not produce a branch and PR')
  }, 20_000)

  // MUST NOT FIRE (control): a couple of transient blips inside a 45-minute
  // poll are not a canary failure. Two failures then recovery must still prove
  // the canary, or we have traded a masked timeout for a flaky false page.
  it('absorbs transient lookup failures and still proves the canary', () => {
    const { status, output, ghOutputs } = runWaitStep({
      failFirst: 2,
      branches: provenBranches,
      prs: provenPr,
    })
    expect(status).toBe(0)
    expect(ghOutputs).toContain('pr_number=42')
    expect(output).toContain('P0 canary PROVEN')
  }, 20_000)

  // MUST NOT FIRE (control): a genuine dispatch timeout -- lookups healthy,
  // nothing ever appears -- must still be reported as a dispatch failure and
  // must not be relabelled as a lookup problem.
  it('still reports a real dispatch timeout as a dispatch failure', () => {
    const { status, output } = runWaitStep({ branches: [], prs: [] }, 2)
    expect(status).not.toBe(0)
    expect(output).toContain('did not produce a branch and PR')
    expect(output).not.toContain('INCONCLUSIVE')
  }, 20_000)
})

describe('P0 canary treats the branch name as data, not as jq source', () => {
  // A branch name is attacker-supplied: anyone who can push chooses it, and a
  // valid git ref may contain `"`, `)` and `#`. Spliced into a --jq program it
  // can close the string literal and comment out the author filter, so a PR
  // from any identity would count as proof -- a false GREEN on the one check
  // that detects a credential regression.
  const HOSTILE_BRANCH = 'factory/1234-x"or(true))#'

  it('is a branch name git itself accepts', () => {
    const check = spawnSync('git', ['check-ref-format', '--branch', HOSTILE_BRANCH], {
      encoding: 'utf8',
    })
    expect(check.status).toBe(0)
  })

  // MUST FIRE, end to end: spliced into the jq program this payload closes the
  // string literal and comments the author filter out, so jq emits the
  // attacker's PR object. `sed -n '1p'` then keeps its first line -- `{` --
  // which is non-empty, so the step takes it as the proven PR and reports the
  // canary GREEN off a PR the expected identity never opened.
  it('does not go green on a foreign-authored PR when the branch name attacks jq', () => {
    const { status, output } = runWaitStep(
      {
        branches: [{ name: HOSTILE_BRANCH }],
        prs: [
          {
            number: 99,
            headRefName: HOSTILE_BRANCH,
            url: 'https://example.invalid/pr/99',
            author: { login: 'attacker' },
          },
        ],
      },
      3,
    )
    expect(output).not.toContain('P0 canary PROVEN')
    expect(status).not.toBe(0)
  }, 20_000)

  // The structural half: the branch must reach gh as an argument, never as
  // part of a jq program, in either PR lookup.
  it('passes the branch via --head and never interpolates it into --jq', () => {
    for (const [name, snippet] of extractLookups()) {
      if (name === 'BRANCH') continue
      expect(snippet).toContain('--head "${BRANCH}"')
      const jqProgram = /--jq "([^"]*(?:\\.[^"]*)*)"/.exec(snippet)?.[1] ?? ''
      expect(jqProgram).not.toContain('BRANCH')
    }
  })
})
