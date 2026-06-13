import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import { containsIssueKey } from '../issue-key-match'
import type { GhRunner } from './merge-gate'

const execFileAsync = promisify(execFile)
const FACTORY_E2E_MARKER = '[factory-e2e]'

export interface CloseProbePrInput {
  repo: string
  prNumber: number
  expectedIssueKey: string
  requireTitleMarker?: boolean
  runner?: GhRunner
}

export interface CloseProbePrResult {
  repo: string
  prNumber: number
  state: 'CLOSED'
}

export async function closeProbePr(input: CloseProbePrInput): Promise<CloseProbePrResult> {
  const run = input.runner ?? defaultGhRunner
  const before = await viewPr(run, input)
  const beforeState = assertClosableProbe(before, input)
  if (beforeState === 'CLOSED') {
    return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
  }

  await run(['pr', 'close', String(input.prNumber), '--repo', input.repo])

  const after = await viewPr(run, input)
  const afterState = stringValue(after.state)
  if (normalizeState(afterState) !== 'CLOSED') {
    throw new Error(`Refusing to report probe PR closed: live state is ${afterState ?? 'unknown'}`)
  }

  return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
}

const viewPr = async (run: GhRunner, input: CloseProbePrInput): Promise<Record<string, unknown>> => {
  const result = await run([
    'pr',
    'view',
    String(input.prNumber),
    '--repo',
    input.repo,
    '--json',
    'state,headRefName,body,title',
  ])
  if (!result.stdout.trim()) {
    throw new Error(`Unable to guard probe PR #${input.prNumber}: gh returned empty output`)
  }
  return parseGhJson(result.stdout)
}

const assertClosableProbe = (live: Record<string, unknown>, input: CloseProbePrInput): 'OPEN' | 'CLOSED' => {
  const state = stringValue(live.state)
  const normalized = normalizeState(state)
  if (normalized !== 'OPEN' && normalized !== 'CLOSED') {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: live state is ${state ?? 'unknown'}`)
  }

  const title = stringValue(live.title) ?? ''
  const body = stringValue(live.body) ?? ''
  const headRefName = stringValue(live.headRefName) ?? ''
  const haystack = `${title}\n${body}\n${headRefName}`
  if (!containsIssueKey(haystack, input.expectedIssueKey)) {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: missing issue key ${input.expectedIssueKey}`)
  }
  if ((input.requireTitleMarker ?? true) && !hasFactoryE2eMarker(title)) {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: missing ${FACTORY_E2E_MARKER} probe marker`)
  }
  return normalized
}

const hasFactoryE2eMarker = (title: string): boolean =>
  title === FACTORY_E2E_MARKER || title.startsWith(`${FACTORY_E2E_MARKER} `)

const normalizeState = (state?: string): string | undefined => state?.toUpperCase()

const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined

const parseGhJson = (stdout: string): Record<string, unknown> => {
  const parsed = JSON.parse(stdout) as unknown
  return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
    ? parsed as Record<string, unknown>
    : {}
}

const defaultGhRunner: GhRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('gh', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}
