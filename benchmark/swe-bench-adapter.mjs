#!/usr/bin/env node
// Fetches instances from SWE-bench Verified (a public, human-filtered subset of
// SWE-bench: https://huggingface.co/datasets/princeton-nlp/SWE-bench_Verified)
// and writes each as a benchmark/tasks/<id>/task.json + verify.sh, matching
// BenchmarkTaskSchema (see ./schema.ts).
//
// IMPORTANT — read before running:
// 1. Every instance's `repo` field is an upstream open-source repo (e.g.
//    django/django) that Factory does NOT own push/PR access to and MUST NOT
//    open real PRs against. Before dispatching these tasks for real, fork
//    each distinct `repo` this script pulls into an org we control (or mirror
//    it), then repoint `targetRepo` in the generated task.json files at that
//    fork. This script deliberately does NOT do that forking for you — it
//    only writes the upstream repo name plus a loud `targetRepoNote` so a
//    human/agent has to make that call explicitly instead of silently
//    dispatching against someone else's repository.
// 2. `verify.command` is a best-effort reconstruction: it re-runs the
//    instance's own FAIL_TO_PASS pytest node IDs with `python -m pytest`.
//    SWE-bench's official harness uses per-repo, sometimes non-pytest, test
//    runners and pinned conda environments — this script does not replicate
//    that. Spot-check the generated verify.sh against the instance's actual
//    repo before trusting a "fail" result, especially for non-pytest repos.
// 3. This is best-effort, not a claim of strict SWE-bench-Verified parity —
//    say so plainly in any report generated from these tasks.
//
// Usage:
//   node benchmark/swe-bench-adapter.mjs --count 20 --out benchmark/tasks

import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const DATASET = 'princeton-nlp/SWE-bench_Verified'
const ROWS_PAGE_SIZE = 100

function parseArgs(argv) {
  const args = { count: 20, out: 'benchmark/tasks', offset: 0 }
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--count') args.count = Number(argv[++i])
    else if (arg === '--out') args.out = argv[++i]
    else if (arg === '--offset') args.offset = Number(argv[++i])
    else throw new Error(`Unknown argument: ${arg}`)
  }
  if (!Number.isInteger(args.count) || args.count < 1) {
    throw new Error(`--count must be a positive integer, got ${args.count}`)
  }
  return args
}

async function fetchInstances(count, offset) {
  const instances = []
  let cursor = offset
  while (instances.length < count) {
    const length = Math.min(ROWS_PAGE_SIZE, count - instances.length)
    const url = `https://datasets-server.huggingface.co/rows?dataset=${encodeURIComponent(DATASET)}&config=default&split=test&offset=${cursor}&length=${length}`
    const response = await fetch(url)
    if (!response.ok) {
      throw new Error(`SWE-bench Verified fetch failed: HTTP ${response.status} for ${url}`)
    }
    const body = await response.json()
    if (body.rows.length === 0) break
    instances.push(...body.rows.map((entry) => entry.row))
    cursor += body.rows.length
  }
  return instances
}

function difficultyFor(instance) {
  // SWE-bench Verified's own `difficulty` field is a human-estimated time
  // bucket, not a coordination-benefit signal — approximate ours from how
  // many distinct files the gold patch touches instead.
  const filesTouched = new Set(
    [...instance.patch.matchAll(/^diff --git a\/(\S+) /gmu)].map((match) => match[1]),
  ).size
  if (filesTouched <= 1) return 'single-file'
  if (filesTouched <= 3) return 'multi-file'
  return 'multi-service'
}

function taskIdFor(instance) {
  return `swe-bench-${instance.instance_id}`.toLowerCase().replace(/[^a-z0-9-]+/gu, '-')
}

function toTask(instance) {
  const failToPass = Array.isArray(instance.FAIL_TO_PASS)
    ? instance.FAIL_TO_PASS
    : JSON.parse(instance.FAIL_TO_PASS)

  return {
    id: taskIdFor(instance),
    title: `[SWE-bench Verified] ${instance.instance_id}`,
    issueBody: instance.problem_statement,
    targetRepo: instance.repo,
    targetRepoNote: 'UPSTREAM OSS REPO — repoint to a controlled fork before dispatching for real. See adapter script header.',
    baseRef: instance.base_commit,
    difficulty: difficultyFor(instance),
    verify: {
      // Best-effort reconstruction — see script header caveat #2.
      command: `python -m pytest ${failToPass.map((testId) => JSON.stringify(testId)).join(' ')}`,
      timeoutMs: 600_000,
    },
    source: 'swe-bench',
    sweBenchInstanceId: instance.instance_id,
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const instances = await fetchInstances(args.count, args.offset)
  if (instances.length < args.count) {
    console.warn(`Requested ${args.count} instances but only ${instances.length} were available from offset ${args.offset}.`)
  }

  for (const instance of instances) {
    const task = toTask(instance)
    const dir = join(args.out, task.id)
    await mkdir(dir, { recursive: true })
    await writeFile(join(dir, 'task.json'), `${JSON.stringify(task, null, 2)}\n`)
    await writeFile(
      join(dir, 'verify.sh'),
      `#!/usr/bin/env bash\n# Adapted from SWE-bench Verified instance ${task.sweBenchInstanceId}. See swe-bench-adapter.mjs header before trusting this.\nset -euo pipefail\n\n${task.verify.command}\n`,
      { mode: 0o755 },
    )
  }

  console.log(`Wrote ${instances.length} SWE-bench-derived task(s) to ${args.out}. Read the script header before dispatching any of them.`)
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
