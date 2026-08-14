import type { MountClient } from '../ports'
import { readStandalonePullRequest } from './standalone-babysitter'

export interface GithubMergeGateInput {
  repo: string
  number: number
  expectedHeadSha?: string
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

export class RelayfileGithubMergeGate implements GithubMergeGate {
  readonly #mount: MountClient

  constructor(mount: MountClient) {
    this.#mount = mount
  }

  async check(input: GithubMergeGateInput): Promise<GithubMergeGateVerdict> {
    try {
      const pullRequest = await readStandalonePullRequest(this.#mount, {
        repo: input.repo,
        prNumber: input.number,
      })
      return evaluateGithubMergeGate(input, {
        mergeable: pullRequest.mergeable,
        mergeStateStatus: pullRequest.mergeStateStatus,
        headRefOid: pullRequest.headSha,
        reviewDecision: pullRequest.reviewDecision,
        statusCheckRollup: pullRequest.statusCheckRollup,
      })
    } catch (error) {
      return refuse(`Relayfile GitHub merge gate failed: ${error instanceof Error ? error.message : String(error)}`, {
        checkStates: [],
      })
    }
  }

  async merge(input: GithubMergeInput): Promise<GithubMergeResult> {
    try {
      const mergePullRequest = this.#mount.githubWrite?.mergePullRequest
      if (!mergePullRequest) {
        throw new Error('connected GitHub App merge write path is unavailable')
      }
      // Relayfile currently exposes the guarded merge itself, but not a branch
      // deletion mutation. Leave the merged head branch in place rather than
      // reintroducing `gh --delete-branch` under a local human identity.
      const result = await mergePullRequest.call(this.#mount.githubWrite, {
        repo: input.repo,
        number: input.number,
        expectedHeadSha: input.expectedHeadSha,
        method: 'squash',
      })
      return {
        merged: true,
        reason: `merged ${input.repo}#${input.number} at ${input.expectedHeadSha}`,
        stdout: result.sha,
      }
    } catch (error) {
      return {
        merged: false,
        reason: `Relayfile GitHub App guarded merge failed: ${error instanceof Error ? error.message : String(error)}`,
      }
    }
  }
}

export const GithubMergeGate = RelayfileGithubMergeGate

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
