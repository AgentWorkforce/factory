import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { MountClient } from '../ports'
import { wrappedPayload } from '../writeback/shared'
import { localGhMutationAllowed, localGhMutationRefusal, type GithubWriteIdentity } from './gh-identity'

const execFileAsync = promisify(execFile)

export interface GhRunResult {
  stdout: string
  stderr?: string
}

export type GhRunner = (args: string[]) => Promise<GhRunResult>

export interface GithubMergeGateInput {
  repo: string
  number: number
  expectedHeadSha?: string
  /** Exact mounted PR metadata path returned by discovery. */
  path?: string
}

export interface GithubMergeInput {
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
    reviewDecision?: string
    checkStates: string[]
  }
}

export interface GithubMergeResult {
  merged: boolean
  reason: string
  stdout?: string
  stderr?: string
}

export interface GithubMergeGate {
  check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict>
  merge(input: GithubMergeInput): Promise<GithubMergeResult>
}

export class GhCliGithubMergeGate implements GithubMergeGate {
  readonly #run: GhRunner
  readonly #identity: GithubWriteIdentity

  /**
   * @param identity the configured `github.identity`. `check` is a read and
   *   ignores it; `merge` mutates GitHub and refuses under exact `app` rather
   *   than squash-merging as the operator's own account. Defaults to `auto`
   *   so a directly-constructed gate keeps its historical behavior.
   */
  constructor(run: GhRunner = defaultGhRunner, identity: GithubWriteIdentity = 'auto') {
    this.#run = run
    this.#identity = identity
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    throw new Error(
      `GitHub merge-gate readiness for ${input.repo}#${input.number} requires mounted PR metadata; ` +
      'the local-gh adapter supports mutations only and Factory will not disguise a missing read capability as REFUSE',
    )
  }

  async merge(input: GithubMergeInput): Promise<GithubMergeResult> {
    // Fail closed before spawning `gh`. A guarded merge run through the local
    // CLI is recorded by GitHub as the operator merging, which is precisely
    // the split audit trail `github.identity: "app"` exists to remove. There
    // is no app-authored merge to fall through to, so refuse and say why.
    if (!localGhMutationAllowed(this.#identity)) {
      return {
        merged: false,
        reason: localGhMutationRefusal(
          `the guarded squash merge of ${input.repo}#${input.number}`,
          'mergePullRequest',
        ),
      }
    }

    try {
      const result = await this.#run([
        'pr',
        'merge',
        String(input.number),
        '--repo',
        input.repo,
        '--squash',
        '--delete-branch',
        '--match-head-commit',
        input.expectedHeadSha,
      ])
      return {
        merged: true,
        reason: `merged ${input.repo}#${input.number} at ${input.expectedHeadSha}`,
        stdout: result.stdout,
        stderr: result.stderr,
      }
    } catch (error) {
      return {
        merged: false,
        reason: `gh guarded merge failed: ${error instanceof Error ? error.message : String(error)}`,
      }
    }
  }
}

export const GithubMergeGate = GhCliGithubMergeGate

/**
 * Reads provider-authoritative merge readiness from the GitHub App projection
 * and delegates the guarded mutation separately. Discovery passes the exact PR
 * path, so this adds one read and never scans the pulls tree.
 */
export class MountedGithubMergeGate implements GithubMergeGate {
  readonly #mount: Pick<MountClient, 'readFile'>
  readonly #mutation: Pick<GithubMergeGate, 'merge'>

  constructor(
    mount: Pick<MountClient, 'readFile'>,
    mutation: Pick<GithubMergeGate, 'merge'> = new GhCliGithubMergeGate(),
  ) {
    this.#mount = mount
    this.#mutation = mutation
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    const path = input.path ?? mountedPullByIdPath(input.repo, input.number)
    let content: unknown
    try {
      content = (await this.#mount.readFile(path)).content
    } catch (error) {
      throw mountedMergeGateCapabilityError(
        input,
        `could not read ${path}: ${error instanceof Error ? error.message : String(error)}`,
      )
    }

    const live = mountedMergeGateFields(content, input)
    return evaluateGithubMergeGate(input, live)
  }

  async merge(input: GithubMergeInput): Promise<GithubMergeResult> {
    return this.#mutation.merge(input)
  }
}

export function evaluateGithubMergeGate(
  input: GithubMergeGateInput,
  live: unknown,
): GithubMergeGateVerdict {
  const record = asRecord(live)
  const mergeable = stringValue(record.mergeable)
  const mergeStateStatus = stringValue(record.mergeStateStatus)
  const headRefOid = stringValue(record.headRefOid)
  const reviewDecision = stringValue(record.reviewDecision)
  const statusCheckRollup = Array.isArray(record.statusCheckRollup) ? record.statusCheckRollup : undefined
  const checkStates = statusCheckRollup ? checkStatesFromRollup(statusCheckRollup) : []

  if (!mergeable || !mergeStateStatus || !headRefOid || !reviewDecision || !statusCheckRollup) {
    return refuse('missing required live GitHub merge fields', {
      mergeable,
      mergeStateStatus,
      headRefOid,
      reviewDecision,
      checkStates,
    })
  }

  if (mergeable === 'UNKNOWN' || mergeStateStatus === 'UNKNOWN') {
    return refuse('GitHub mergeability is still unknown', { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  if (input.expectedHeadSha && headRefOid !== input.expectedHeadSha) {
    return refuse(`head moved: expected ${input.expectedHeadSha}, live ${headRefOid ?? 'unknown'}`, {
      mergeable,
      mergeStateStatus,
      headRefOid,
      reviewDecision,
      checkStates,
    })
  }

  if (mergeable !== 'MERGEABLE') {
    return refuse(`mergeable is ${mergeable ?? 'unknown'}`, { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  if (mergeStateStatus !== 'CLEAN') {
    return refuse(`merge state is ${mergeStateStatus ?? 'unknown'}`, { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  if (reviewDecision !== 'APPROVED') {
    return refuse(`review decision is ${reviewDecision ?? 'unknown'}`, { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  if (checkStates.length === 0) {
    return refuse('no successful status checks observed', { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  const blocking = checkStates.filter(isBlockingCheckState)
  if (blocking.length > 0) {
    return refuse(`checks not merge-ready: ${blocking.join(', ')}`, { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates })
  }

  return {
    verdict: 'READY',
    ready: true,
    reason: 'MERGEABLE+CLEAN with APPROVED review, matching head when supplied, and no blocking checks',
    live: { mergeable, mergeStateStatus, headRefOid, reviewDecision, checkStates },
  }
}

export const defaultGhRunner: GhRunner = async (args) => {
  // Mutation-only compatibility runner. Merge-gate reads now come from the
  // mounted App projection. Retire this when `GithubConnectionWrite` exposes
  // a server-side `mergePullRequest` capability so Factory still holds no
  // GitHub credential. Until then `merge` refuses under
  // `github.identity: "app"` rather than merging as the operator (see
  // ./gh-identity). Tracked on AgentWorkforce/factory#221.
  const { stdout, stderr } = await execFileAsync('gh', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

const mountedPullByIdPath = (repo: string, number: number): string => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0 || !Number.isSafeInteger(number) || number <= 0) {
    throw new Error(`Invalid GitHub pull request identity ${repo}#${number}`)
  }
  return `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(name)}/pulls/by-id/${number}.json`
}

const mountedMergeGateFields = (
  content: unknown,
  input: GithubMergeGateInput,
): Record<string, unknown> => {
  const payload = wrappedPayload(content)
  const explicitNumber = numberValue(payload.number)
  if (explicitNumber !== undefined && explicitNumber !== input.number) {
    throw mountedMergeGateCapabilityError(
      input,
      `mounted PR record number is ${explicitNumber}, expected ${input.number}`,
    )
  }

  const head = asRecord(payload.head)
  const mergeable = normalizeMergeable(payload.mergeable)
  const mergeStateStatus = normalizedString(
    payload.mergeStateStatus ?? payload.merge_state_status ?? payload.mergeable_state,
  )
  const headRefOid = stringValue(payload.headRefOid) ?? stringValue(head.sha)
  const reviewDecision = normalizedString(payload.reviewDecision ?? payload.review_decision)
  const statusCheckRollup = payload.statusCheckRollup ?? payload.status_check_rollup
  const missing = [
    ['mergeable', mergeable],
    ['mergeStateStatus', mergeStateStatus],
    ['headRefOid', headRefOid],
    ['reviewDecision', reviewDecision],
    ['statusCheckRollup', Array.isArray(statusCheckRollup) ? statusCheckRollup : undefined],
  ].flatMap(([name, value]) => value === undefined ? [name] : [])
  if (missing.length > 0) {
    throw mountedMergeGateCapabilityError(
      input,
      `mounted PR metadata is missing ${missing.join(', ')}`,
    )
  }

  return { mergeable, mergeStateStatus, headRefOid, reviewDecision, statusCheckRollup }
}

const mountedMergeGateCapabilityError = (input: GithubMergeGateInput, detail: string): Error =>
  new Error(
    `GitHub merge-gate capability unavailable for ${input.repo}#${input.number}: ${detail}; ` +
    'Factory requires the authenticated mounted PR projection and does not fall back to local gh',
  )

const normalizeMergeable = (value: unknown): string | undefined => {
  if (typeof value === 'boolean') return value ? 'MERGEABLE' : 'CONFLICTING'
  return normalizedString(value)
}

const normalizedString = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value.trim().toUpperCase() : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined

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
