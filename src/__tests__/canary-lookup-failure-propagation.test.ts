import { execFileSync, spawnSync } from 'node:child_process'
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

// The P0 canary distinguishes "dispatch is down" from "the canary's own API
// calls are broken". Those page different people, so a failed `gh` lookup must
// never read as "not found yet": that silently burns the whole wait window and
// then reports a dispatch timeout that never happened.
//
// These tests execute the lookup pipelines EXTRACTED FROM THE WORKFLOW ITSELF,
// not a copy of them, so the assertion cannot drift away from what CI runs.
const WORKFLOW = join(import.meta.dirname, '../../.github/workflows/verify-p0-canary.yml')

/** The `gh` lookups in the wait step, verbatim, as `[name, snippet]`. */
function extractLookups(): Array<[string, string]> {
  const workflow = parse(readFileSync(WORKFLOW, 'utf8'))
  const steps = workflow.jobs.verify.steps as Array<{ id?: string; run?: string }>
  const wait = steps.find((step) => step.id === 'wait')
  if (!wait?.run) throw new Error('no step with id "wait" in the canary workflow')

  const lookups: Array<[string, string]> = []
  const lines = wait.run.split('\n')
  for (let i = 0; i < lines.length; i++) {
    const opens = /^\s*([A-Z_]+)="\$\(gh /.exec(lines[i])
    if (!opens) continue
    const collected = [lines[i]]
    // A lookup runs until the line closing the command substitution.
    while (!/\)"\s*$/.test(collected[collected.length - 1])) {
      i++
      if (i >= lines.length) throw new Error(`unterminated lookup for ${opens[1]}`)
      collected.push(lines[i])
    }
    lookups.push([opens[1], collected.join('\n')])
  }
  return lookups
}

/**
 * Run one extracted lookup with `gh` stubbed. `ghExit`/`ghStdout` stand in for
 * a GitHub API that fails, or that answers with more lines than the lookup
 * wants. Returns the shell's exit code and the value the lookup assigned.
 */
function runLookup(
  snippet: string,
  variable: string,
  gh: { exit: number; stdout: string },
): { status: number; value: string } {
  const dir = mkdtempSync(join(tmpdir(), 'canary-lookup-'))
  // The canned stdout goes in a file rather than inline in the stub, so that
  // newlines stay newlines instead of depending on how printf quotes them.
  const canned = join(dir, 'stdout')
  writeFileSync(canned, gh.stdout)
  const stub = join(dir, 'gh')
  writeFileSync(stub, `#!/usr/bin/env bash\ncat ${JSON.stringify(canned)}\nexit ${gh.exit}\n`)
  chmodSync(stub, 0o755)

  // set -euo pipefail matches the workflow step's own shell options: without
  // pipefail a failing producer upstream of a pipe is invisible either way.
  const script = [
    'set -euo pipefail',
    'TARGET_REPO=AgentWorkforce/factory',
    'NUM=1234',
    'BRANCH=factory/1234-canary',
    'EXPECTED_PR_AUTHOR=app/agent-relay-code',
    snippet,
    `printf 'VALUE=%s' "\${${variable}}"`,
  ].join('\n')

  const result = spawnSync('bash', ['-c', script], {
    encoding: 'utf8',
    env: { ...process.env, PATH: `${dir}:${process.env.PATH ?? ''}` },
  })
  const value = /VALUE=([\s\S]*)$/.exec(result.stdout ?? '')?.[1] ?? ''
  return { status: result.status ?? -1, value }
}

describe('P0 canary lookup failure propagation', () => {
  const lookups = extractLookups()

  // Extraction silently finding nothing would make every case below vacuous.
  it('extracts all three gh lookups from the wait step', () => {
    expect(lookups.map(([name]) => name)).toEqual(['BRANCH', 'PR_LINE', 'FOREIGN_PR'])
  })

  // MUST FIRE: a failing gh has to fail the step, not read as "nothing yet".
  it.each(lookups)('%s propagates a gh failure instead of masking it', (_name, snippet) => {
    const variable = _name
    const { status } = runLookup(snippet, variable, { exit: 1, stdout: '' })
    expect(status).not.toBe(0)
  })

  // MUST NOT FIRE (control): the success path still yields the first line and
  // still exits 0. This is the case `|| true` was originally protecting -- a
  // consumer that closes the pipe early can SIGPIPE the producer -- so it has
  // to keep passing, otherwise the fix trades a false green for a false red.
  it.each(lookups)('%s takes the first line and succeeds when gh succeeds', (_name, snippet) => {
    const variable = _name
    const { status, value } = runLookup(snippet, variable, {
      exit: 0,
      stdout: 'first-line\nsecond-line\nthird-line\n',
    })
    expect(status).toBe(0)
    expect(value).toBe('first-line')
  })
})
