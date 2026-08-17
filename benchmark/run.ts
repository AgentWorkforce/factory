#!/usr/bin/env -S npx tsx
// Runs the benchmark matrix for real against a live Factory workspace.
//
// This dispatches real coding agents, opens real PRs, and spends real
// money/time — see benchmark/README.md before running it. Never point
// factoryConfigPath at a config whose repos map to a product repo.
//
// Usage:
//   npx tsx benchmark/run.ts --config ./factory.config.json [--only-task id] [--only-mode single] [--repeats 3]

import { join } from 'node:path'

import { createCliDispatchRunner } from './cli-dispatch-runner'
import { BENCHMARK_MODES, loadTasks, type BenchmarkMode } from './schema'
import { buildMatrix } from './matrix'
import { runMatrix } from './orchestrate'
import { renderMarkdownReport, buildReportRows } from './report'
import { appendResult, loadResults } from './results-store'

const TASKS_DIR = join(import.meta.dirname, 'tasks')
const RESULTS_PATH = join(import.meta.dirname, 'results.jsonl')
const REPORT_PATH = join(import.meta.dirname, 'report.md')

function parseArgs(argv: string[]) {
  const args: { config?: string; onlyTaskIds?: string[]; onlyModes?: BenchmarkMode[]; repeats?: number } = {}
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--config') args.config = argv[++i]
    else if (arg === '--only-task') args.onlyTaskIds = [...(args.onlyTaskIds ?? []), argv[++i]!]
    else if (arg === '--only-mode') {
      const mode = argv[++i]
      if (!BENCHMARK_MODES.includes(mode as BenchmarkMode)) {
        throw new Error(`--only-mode must be one of ${BENCHMARK_MODES.join(', ')}, got "${mode}"`)
      }
      args.onlyModes = [...(args.onlyModes ?? []), mode as BenchmarkMode]
    } else if (arg === '--repeats') args.repeats = Number(argv[++i])
    else throw new Error(`Unknown argument: ${arg}`)
  }
  if (!args.config) {
    throw new Error('--config <path to factory.config.json> is required')
  }
  return args
}

async function main() {
  const args = parseArgs(process.argv.slice(2))

  const tasks = await loadTasks(TASKS_DIR)
  if (tasks.length === 0) {
    throw new Error(`No tasks found in ${TASKS_DIR}. Author real task.json files there (see benchmark/templates/ and benchmark/swe-bench-adapter.mjs) before running the benchmark.`)
  }

  const existingResults = await loadResults(RESULTS_PATH)
  const cells = buildMatrix(tasks, BENCHMARK_MODES, existingResults, {
    onlyTaskIds: args.onlyTaskIds,
    onlyModes: args.onlyModes,
    repeats: args.repeats,
  })
  if (cells.length === 0) {
    console.log('Every requested (task x mode x repeat) cell already has a recorded result. Nothing to run.')
  } else {
    console.log(`Running ${cells.length} cell(s); ${existingResults.length} already recorded and skipped.`)

    const runner = createCliDispatchRunner({
      factoryConfigPath: args.config!,
      log: (message) => console.log(`[dispatch] ${message}`),
    })
    await runMatrix(cells, {
      runner,
      appendResult: (result) => appendResult(RESULTS_PATH, result),
      log: (message) => console.log(message),
      now: () => new Date().toISOString(),
    })
  }

  const allResults = await loadResults(RESULTS_PATH)
  const rows = buildReportRows(tasks, allResults)
  const markdown = renderMarkdownReport(rows)
  const { writeFile } = await import('node:fs/promises')
  await writeFile(REPORT_PATH, markdown)
  console.log(`Report written to ${REPORT_PATH}`)
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
