import { containsIssueKey } from '../issue-key-match'
import type { GithubConnectionWrite, MountClient } from '../ports'
import { wrappedPayload } from '../writeback/shared'
import { GARDEN_E2E_TITLE_PREFIX, hasGardenTitlePrefix } from '../constants/lifecycle-labels'

const FACTORY_E2E_MARKER = GARDEN_E2E_TITLE_PREFIX

export interface CloseProbePrInput {
  repo: string
  prNumber: number
  expectedIssueKey: string
  requireTitleMarker?: boolean
  githubWrite?: GithubConnectionWrite
  /** Exact mounted PR metadata path returned by discovery. */
  path?: string
  mount?: Pick<MountClient, 'readFile'>
}

export interface CloseProbePrResult {
  repo: string
  prNumber: number
  state: 'CLOSED'
}

export async function closeProbePr(input: CloseProbePrInput): Promise<CloseProbePrResult> {
  const githubWrite = input.githubWrite
  if (!githubWrite) {
    throw new Error('GitHub write path not available on this mount — connect GitHub to your workspace')
  }
  if (!input.mount) {
    throw new Error(
      `Mounted GitHub PR read path is unavailable for ${input.repo}#${input.prNumber}; ` +
      'Factory will not fall back to the local gh CLI',
    )
  }
  const before = await viewPr(input.mount, input)
  const beforeState = assertClosableProbe(before, input)
  if (beforeState === 'CLOSED') {
    return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
  }

  await githubWrite.closePullRequest({ repo: input.repo, number: input.prNumber })
  return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
}

const viewPr = async (
  mount: Pick<MountClient, 'readFile'>,
  input: CloseProbePrInput,
): Promise<Record<string, unknown>> => {
  const path = input.path ?? pullByIdPath(input.repo, input.prNumber)
  let content: unknown
  try {
    content = (await mount.readFile(path)).content
  } catch (error) {
    throw new Error(
      `Unable to guard probe PR ${input.repo}#${input.prNumber} from mounted metadata at ${path}: ` +
      `${error instanceof Error ? error.message : String(error)}`,
    )
  }
  const payload = wrappedPayload(content)
  const explicitNumber = numberValue(payload.number)
  if (explicitNumber !== undefined && explicitNumber !== input.prNumber) {
    throw new Error(
      `Unable to guard probe PR ${input.repo}#${input.prNumber}: mounted record at ${path} identifies PR #${explicitNumber}`,
    )
  }
  const head = recordValue(payload.head)
  return {
    state: stringValue(payload.state),
    headRefName: stringValue(payload.headRefName) ?? stringValue(payload.head_ref) ?? stringValue(head.ref),
    body: stringValue(payload.body),
    title: stringValue(payload.title),
  }
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
    throw new Error(`Refusing to close probe PR #${input.prNumber}: missing ${FACTORY_E2E_MARKER} (or legacy [factory-e2e]) probe marker`)
  }
  return normalized
}

// Either spelling: in-flight soak PRs are titled with the legacy
// `[factory-e2e]` marker and stay closable during the rename transition.
const hasFactoryE2eMarker = (title: string): boolean =>
  hasGardenTitlePrefix(title, FACTORY_E2E_MARKER)

const normalizeState = (state?: string): string | undefined => state?.toUpperCase()

const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined

const pullByIdPath = (repo: string, number: number): string => {
  const [owner, name, ...extra] = repo.split('/')
  if (!owner || !name || extra.length > 0 || !Number.isSafeInteger(number) || number <= 0) {
    throw new Error(`Invalid GitHub pull request identity ${repo}#${number}`)
  }
  return `/github/repos/${encodeURIComponent(owner)}__${encodeURIComponent(name)}/pulls/by-id/${number}.json`
}

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined

const recordValue = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
