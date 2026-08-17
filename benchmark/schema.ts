import { readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'

import { z } from 'zod'

/**
 * A dispatch mode under test. Mirrors TriageDecision['scope'] minus 'workflow'
 * (workflow dispatch isn't a coding-agent comparison — it runs a fixed script).
 */
export const BENCHMARK_MODES = ['single', 'team', 'swarm'] as const
export type BenchmarkMode = (typeof BENCHMARK_MODES)[number]

export const BenchmarkTaskSchema = z.object({
  id: z.string().min(1),
  title: z.string().min(1),
  /** Full issue body dispatched to Factory as the task description. */
  issueBody: z.string().min(1),
  /** owner/repo the task runs against. Must be a disposable sandbox repo, never a product repo. */
  targetRepo: z.string().regex(/^[\w.-]+\/[\w.-]+$/u, 'targetRepo must be "owner/repo"'),
  /** Branch PRs are opened against. Defaults to the repo's default branch. */
  baseRef: z.string().min(1).default('main'),
  /**
   * Coordination-benefit tier: single-file tasks are the control (little to no
   * expected benefit from team/swarm); multi-service tasks are where dispatch
   * mode should differentiate most, if it differentiates at all.
   */
  difficulty: z.enum(['single-file', 'multi-file', 'multi-service']),
  verify: z.object({
    /** Shell command run from the repo root against the checked-out PR branch. Exit 0 = pass. */
    command: z.string().min(1),
    timeoutMs: z.number().int().positive().default(300_000),
  }),
  source: z.enum(['authored', 'swe-bench']),
  /** Only for source: 'swe-bench' — the upstream instance_id, for traceability. */
  sweBenchInstanceId: z.string().optional(),
  /** Only for source: 'swe-bench' — a caveat surfaced next to targetRepo (e.g. "repoint to a fork"). */
  targetRepoNote: z.string().optional(),
})

export type BenchmarkTask = z.infer<typeof BenchmarkTaskSchema>

export interface BenchmarkResult {
  taskId: string
  mode: BenchmarkMode
  repeat: number
  runId: string
  passed: boolean
  notes?: string
  /** ISO timestamp the result was recorded. */
  timestamp: string
  /**
   * Wall-clock the cell took, dispatch → verify inclusive. Optional so
   * historical results.jsonl lines that pre-date this field still load; new
   * rows always include it.
   */
  durationMs?: number
  /**
   * USD spend attributed to this cell. Optional and left unset until Factory
   * surfaces per-dispatch cost — the report treats undefined as "no cost
   * sample" (renders `n/a`) rather than as 0.
   */
  costUsd?: number
}

export const BenchmarkResultSchema = z.object({
  taskId: z.string().min(1),
  mode: z.enum(BENCHMARK_MODES),
  repeat: z.number().int().min(0),
  runId: z.string().min(1),
  passed: z.boolean(),
  notes: z.string().optional(),
  timestamp: z.string().min(1),
  durationMs: z.number().nonnegative().optional(),
  costUsd: z.number().nonnegative().optional(),
})

/**
 * Loads every `benchmark/tasks/<id>/task.json`, validating each against
 * BenchmarkTaskSchema and asserting the directory name matches `id` (catches
 * copy-paste task authoring mistakes before they reach a real dispatch run).
 */
export async function loadTasks(tasksDir: string): Promise<BenchmarkTask[]> {
  const entries = await readdir(tasksDir, { withFileTypes: true })
  const dirs = entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort()

  const tasks: BenchmarkTask[] = []
  for (const dir of dirs) {
    const path = join(tasksDir, dir, 'task.json')
    const raw = JSON.parse(await readFile(path, 'utf8'))
    const task = BenchmarkTaskSchema.parse(raw)
    if (task.id !== dir) {
      throw new Error(`benchmark task at ${path} has id "${task.id}" but lives in directory "${dir}" — they must match`)
    }
    tasks.push(task)
  }
  return tasks
}
