import { execFile } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'

import type { DispatchRunner } from './dispatch-runner'

const exec = promisify(execFile)

export interface CliDispatchRunnerOptions {
  /** Path to the factory.config.json for the workspace that owns task.targetRepo's clonePath. */
  factoryConfigPath: string
  /** Bounds how long we poll for a PR to appear after dispatch, in addition to the task's own verify timeout. */
  dispatchTimeoutMs?: number
  pollIntervalMs?: number
  log?: (message: string) => void
}

/**
 * The real, real-infrastructure implementation of DispatchRunner: creates a
 * labeled GitHub issue via `gh`, dispatches Factory for real via the CLI
 * (mirroring scripts/factory-canary.sh's "wrap the command" pattern), polls
 * for the resulting PR, then checks it out into a scratch clone and runs the
 * task's verify command there.
 *
 * This talks to real GitHub and spends real agent time/cost — never run it
 * against a product repo, only a disposable benchmark-fixtures sandbox. It is
 * intentionally not unit-tested (same category as
 * scripts/verify-tailscale-preview-e2e.mjs): correctness here is proven by
 * running it for real, not by mocking child_process.
 */
export function createCliDispatchRunner(options: CliDispatchRunnerOptions): DispatchRunner {
  const log = options.log ?? (() => {})
  const dispatchTimeoutMs = options.dispatchTimeoutMs ?? 30 * 60_000
  const pollIntervalMs = options.pollIntervalMs ?? 15_000

  return {
    async dispatch(task, mode) {
      const label = `agent:${mode}`
      const { stdout: createOut } = await exec('gh', [
        'issue', 'create',
        '--repo', task.targetRepo,
        '--title', task.title,
        '--body', task.issueBody,
        '--label', 'factory',
        '--label', label,
      ])
      const issueUrl = createOut.trim()
      const issueNumber = issueUrl.split('/').pop()
      if (!issueNumber) {
        throw new Error(`gh issue create returned an unexpected URL: ${issueUrl}`)
      }
      log(`created ${task.targetRepo}#${issueNumber} (${label})`)

      await exec('node', ['bin/factory.mjs', 'dispatch', issueNumber, '--config', options.factoryConfigPath])
      log(`dispatched ${task.targetRepo}#${issueNumber}`)

      const prBranch = await pollForPrBranch(task.targetRepo, issueNumber, dispatchTimeoutMs, pollIntervalMs, log)
      return { runId: `${task.targetRepo}#${issueNumber}`, prBranch }
    },

    async verify(task, outcome) {
      const clonePath = await mkdtemp(join(tmpdir(), 'factory-benchmark-verify-'))
      try {
        await exec('git', ['clone', '--depth', '1', '--branch', outcome.prBranch, `https://github.com/${task.targetRepo}.git`, clonePath])
        try {
          await exec('bash', ['-c', task.verify.command], { cwd: clonePath, timeout: task.verify.timeoutMs })
          return { passed: true }
        } catch (error) {
          return { passed: false, notes: `verify failed: ${error instanceof Error ? error.message : String(error)}` }
        }
      } finally {
        await rm(clonePath, { recursive: true, force: true })
      }
    },
  }
}

async function pollForPrBranch(
  repo: string,
  issueNumber: string,
  timeoutMs: number,
  intervalMs: number,
  log: (message: string) => void,
): Promise<string> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const { stdout } = await exec('gh', [
      'issue', 'view', issueNumber,
      '--repo', repo,
      '--json', 'closedByPullRequestsReferences',
    ])
    const parsed = JSON.parse(stdout) as { closedByPullRequestsReferences?: Array<{ number: number; headRefName: string }> }
    const pr = parsed.closedByPullRequestsReferences?.[0]
    if (pr) return pr.headRefName
    log(`waiting for a PR on ${repo}#${issueNumber}...`)
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for a PR on ${repo}#${issueNumber}`)
}
