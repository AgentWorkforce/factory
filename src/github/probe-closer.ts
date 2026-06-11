import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { GhRunner } from './merge-gate'

const execFileAsync = promisify(execFile)
const FACTORY_E2E_MARKER = '[factory-e2e]'

export interface CloseProbePrInput {
  repo: string
  prNumber: number
  expectedIssueKey: string
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
  assertOpenProbe(before, input)

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

const assertOpenProbe = (live: Record<string, unknown>, input: CloseProbePrInput): void => {
  const state = stringValue(live.state)
  if (normalizeState(state) !== 'OPEN') {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: live state is ${state ?? 'unknown'}`)
  }

  const title = stringValue(live.title) ?? ''
  const body = stringValue(live.body) ?? ''
  const headRefName = stringValue(live.headRefName) ?? ''
  const haystack = `${title}\n${body}\n${headRefName}`
  if (!containsIssueKey(haystack, input.expectedIssueKey)) {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: missing issue key ${input.expectedIssueKey}`)
  }
  if (!hasFactoryE2eMarker(title)) {
    throw new Error(`Refusing to close probe PR #${input.prNumber}: missing ${FACTORY_E2E_MARKER} probe marker`)
  }
}

const hasFactoryE2eMarker = (title: string): boolean =>
  title === FACTORY_E2E_MARKER || title.startsWith(`${FACTORY_E2E_MARKER} `)

const containsIssueKey = (value: string, issueKey: string): boolean => {
  const escaped = issueKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return new RegExp(`(^|[^A-Za-z0-9-])${escaped}([^A-Za-z0-9-]|$)`, 'i').test(value)
}

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
