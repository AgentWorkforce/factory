import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

export interface GhRunResult {
  stdout: string
  stderr?: string
}

export type GhRunner = (args: string[]) => Promise<GhRunResult>

export interface GithubMergeGateInput {
  repo: string
  number: number
  expectedHeadSha: string
}

export interface GithubMergeGateVerdict {
  verdict: 'READY' | 'REFUSE'
  ready: boolean
  reason: string
  live: {
    mergeable?: string
    mergeStateStatus?: string
    headRefOid?: string
    checkStates: string[]
  }
}

export interface GithubMergeGate {
  check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict>
}

export class GhCliGithubMergeGate implements GithubMergeGate {
  readonly #run: GhRunner

  constructor(run: GhRunner = defaultGhRunner) {
    this.#run = run
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    try {
      const result = await this.#run([
        'pr',
        'view',
        String(input.number),
        '--repo',
        input.repo,
        '--json',
        'mergeable,mergeStateStatus,statusCheckRollup,headRefOid',
      ])
      if (result.stdout.trim().length === 0) {
        return refuse('gh returned empty output', { checkStates: [] })
      }

      return evaluateGithubMergeGate(input, parseGhJson(result.stdout))
    } catch (error) {
      return refuse(`gh merge gate failed: ${error instanceof Error ? error.message : String(error)}`, {
        checkStates: [],
      })
    }
  }
}

export const GithubMergeGate = GhCliGithubMergeGate

export function evaluateGithubMergeGate(
  input: GithubMergeGateInput,
  live: unknown,
): GithubMergeGateVerdict {
  const record = asRecord(live)
  const mergeable = stringValue(record.mergeable)
  const mergeStateStatus = stringValue(record.mergeStateStatus)
  const headRefOid = stringValue(record.headRefOid)
  const statusCheckRollup = Array.isArray(record.statusCheckRollup) ? record.statusCheckRollup : undefined
  const checkStates = statusCheckRollup ? checkStatesFromRollup(statusCheckRollup) : []

  if (!mergeable || !mergeStateStatus || !headRefOid || !statusCheckRollup) {
    return refuse('missing required live GitHub merge fields', {
      mergeable,
      mergeStateStatus,
      headRefOid,
      checkStates,
    })
  }

  if (mergeable === 'UNKNOWN' || mergeStateStatus === 'UNKNOWN') {
    return refuse('GitHub mergeability is still unknown', { mergeable, mergeStateStatus, headRefOid, checkStates })
  }

  if (headRefOid !== input.expectedHeadSha) {
    return refuse(`head moved: expected ${input.expectedHeadSha}, live ${headRefOid ?? 'unknown'}`, {
      mergeable,
      mergeStateStatus,
      headRefOid,
      checkStates,
    })
  }

  if (mergeable !== 'MERGEABLE') {
    return refuse(`mergeable is ${mergeable ?? 'unknown'}`, { mergeable, mergeStateStatus, headRefOid, checkStates })
  }

  if (mergeStateStatus !== 'CLEAN') {
    return refuse(`merge state is ${mergeStateStatus ?? 'unknown'}`, { mergeable, mergeStateStatus, headRefOid, checkStates })
  }

  if (checkStates.length === 0) {
    return refuse('no successful status checks observed', { mergeable, mergeStateStatus, headRefOid, checkStates })
  }

  const blocking = checkStates.filter(isBlockingCheckState)
  if (blocking.length > 0) {
    return refuse(`checks not merge-ready: ${blocking.join(', ')}`, { mergeable, mergeStateStatus, headRefOid, checkStates })
  }

  return {
    verdict: 'READY',
    ready: true,
    reason: 'MERGEABLE+CLEAN with matching head and no blocking checks',
    live: { mergeable, mergeStateStatus, headRefOid, checkStates },
  }
}

const defaultGhRunner: GhRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('gh', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

const parseGhJson = (stdout: string): unknown => JSON.parse(stdout)

const refuse = (reason: string, live: GithubMergeGateVerdict['live']): GithubMergeGateVerdict => ({
  verdict: 'REFUSE',
  ready: false,
  reason,
  live,
})

const checkStatesFromRollup = (value: unknown): string[] => {
  if (!Array.isArray(value)) {
    return []
  }

  return value.map((entry) => {
    const record = asRecord(entry)
    const conclusion = stringValue(record.conclusion)
    if (conclusion) {
      return conclusion
    }

    const state = stringValue(record.state)
    if (state) {
      return state
    }

    const status = stringValue(record.status)
    return status ?? 'UNKNOWN'
  })
}

const nonBlockingCheckStates = new Set(['SUCCESS', 'NEUTRAL', 'SKIPPED', 'EXPECTED'])

const isBlockingCheckState = (state: string): boolean => !nonBlockingCheckStates.has(state)

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined
