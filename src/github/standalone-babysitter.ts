import { createHash } from 'node:crypto'

import type { GhRunner } from './merge-gate'
import type { MountClient } from '../ports'
import { trajectorySessionRefFromBody } from '../trajectory'

export interface StandaloneBabysitTarget {
  repo?: string
  prNumber: number
  url?: string
}

export interface StandalonePullRequest {
  repo: string
  number: number
  title: string
  body: string
  state?: string
  draft?: boolean
  merged?: boolean
  url: string
  headRef?: string
  headSha?: string
  baseRef?: string
  headRepo?: string
  crossRepository?: boolean
  maintainerCanModify?: boolean
  filesChanged?: string[]
  source: 'mount' | 'gh' | 'mount+gh'
}

export function parseStandaloneBabysitTarget(value: string | undefined): StandaloneBabysitTarget {
  if (!value) {
    throw new Error('factory babysit requires a PR number or GitHub PR URL')
  }

  if (/^[1-9]\d*$/u.test(value)) {
    const prNumber = Number(value)
    if (Number.isSafeInteger(prNumber)) return { prNumber }
    throw new Error('factory babysit requires a positive safe-integer PR number')
  }

  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new Error('factory babysit requires a positive PR number or canonical https://github.com/<owner>/<repo>/pull/<number> URL')
  }
  const match = url.pathname.match(/^\/([^/]+)\/([^/]+)\/pull\/([1-9]\d*)\/?$/u)
  if (
    url.protocol !== 'https:' ||
    url.hostname.toLowerCase() !== 'github.com' ||
    url.username ||
    url.password ||
    url.port ||
    url.search ||
    url.hash ||
    !match ||
    match[1]?.includes('%') ||
    match[2]?.includes('%')
  ) {
    throw new Error('factory babysit requires a positive PR number or canonical https://github.com/<owner>/<repo>/pull/<number> URL')
  }

  const owner = match[1]!
  const repoName = match[2]!
  const prNumber = Number(match[3])
  if (!isGithubOwner(owner) || !isGithubRepo(repoName) || !Number.isSafeInteger(prNumber)) {
    throw new Error('factory babysit requires a positive PR number or canonical https://github.com/<owner>/<repo>/pull/<number> URL')
  }
  const repo = `${owner}/${repoName}`
  return {
    repo,
    prNumber,
    url: `https://github.com/${repo}/pull/${match[3]}`,
  }
}

export async function readStandalonePullRequest(
  mount: MountClient,
  target: { repo: string; prNumber: number; url?: string },
  ghRunner: GhRunner,
): Promise<StandalonePullRequest> {
  const mounted = await readPullRequestFromMount(mount, target)
  let ghFailure: string | undefined
  try {
    const result = await ghRunner([
      'pr',
      'view',
      String(target.prNumber),
      '--repo',
      target.repo,
      '--json',
      'number,title,body,state,isDraft,url,headRefName,headRefOid,headRepository,headRepositoryOwner,baseRefName,isCrossRepository,maintainerCanModify,files',
    ])
    const parsed = parsePullRequestPayload(JSON.parse(result.stdout), target)
    if (parsed) {
      const merged = mergePullMetadata(mounted, parsed)
      if (hasRequiredPullMetadata(merged)) {
        return { ...merged, source: mounted ? 'mount+gh' : 'gh' }
      }
    }
  } catch (error) {
    ghFailure = error instanceof Error ? error.message : String(error)
  }

  if (mounted && hasRequiredPullMetadata(mounted)) return mounted
  const suffix = ghFailure ? `: ${ghFailure}` : ''
  throw new Error(`Unable to read complete metadata for ${target.repo} PR #${target.prNumber} from the connected GitHub mount or gh${suffix}`)
}

export function standaloneBabysitterAgentName(repo: string, prNumber: number): string {
  const slug = repo
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-|-$/gu, '')
  const hash = createHash('sha256').update(repo.toLowerCase()).digest('hex').slice(0, 8)
  const suffix = `-${prNumber}-${hash}-babysit`
  const availableSlugLength = Math.max(1, 63 - 'pr-'.length - suffix.length)
  return `pr-${slug.slice(0, availableSlugLength)}${suffix}`
}

export function explicitLinkedIssueKey(body: string): string | undefined {
  const linkedLines = body.split('\n').filter((line) => /^\s*(?:linear|issue|closes|fixes|resolves)\b/iu.test(line))
  const keys = new Set<string>()
  for (const line of linkedLines) {
    for (const match of line.matchAll(/\b([A-Z][A-Z0-9]*-\d+)\b/giu)) {
      keys.add(match[1]!.toUpperCase())
    }
  }
  return keys.size === 1 ? [...keys][0] : undefined
}

export { trajectorySessionRefFromBody }

async function readPullRequestFromMount(
  mount: MountClient,
  target: { repo: string; prNumber: number; url?: string },
): Promise<StandalonePullRequest | undefined> {
  const [owner, repoName] = target.repo.split('/')
  if (!owner || !repoName) return undefined

  const roots = [
    `/github/repos/${owner}__${repoName}/pulls/`,
    `/github/repos/${owner}/${repoName}/pulls/`,
  ]
  const paths = new Set<string>()
  try {
    for (const root of roots) {
      for (const path of await mount.listTree(root)) {
        const parts = mountedPullPathParts(path)
        if (
          parts?.number === target.prNumber &&
          `${parts.owner}/${parts.repo}`.toLowerCase() === target.repo.toLowerCase()
        ) {
          paths.add(path)
        }
      }
    }
  } catch {
    return undefined
  }

  const candidates = [...paths].sort((left, right) => pullPathPreference(left) - pullPathPreference(right) || left.localeCompare(right))
  for (const path of candidates) {
    try {
      const parsed = parsePullRequestPayload((await mount.readFile(path)).content, target)
      if (parsed) return { ...parsed, source: 'mount' }
    } catch {
      // Try the next mounted representation before falling back to gh.
    }
  }
  return undefined
}

function mountedPullPathParts(path: string): { owner: string; repo: string; number: number } | undefined {
  const match = path.match(
    /^\/github\/repos\/(?:([^/]+)\/([^/]+)|([^/]+)__([^/]+))\/pulls\/(?:by-id\/)?(\d+)(?:__[^/]*)?(?:\/(?:meta|metadata)\.json|\.json)$/u,
  )
  const owner = match?.[1] ?? match?.[3]
  const repo = match?.[2] ?? match?.[4]
  const number = Number(match?.[5])
  return owner && repo && Number.isInteger(number) && number > 0
    ? { owner, repo, number }
    : undefined
}

const pullPathPreference = (path: string): number =>
  path.endsWith('/meta.json') ? 0 : path.includes('/by-id/') ? 1 : 2

function parsePullRequestPayload(
  content: unknown,
  target: { repo: string; prNumber: number; url?: string },
): Omit<StandalonePullRequest, 'source'> | undefined {
  const envelope = record(content)
  const payload = record(envelope?.payload) ?? envelope
  if (!payload) return undefined
  const number = numberValue(payload.number) ?? target.prNumber
  if (!Number.isInteger(number) || number <= 0 || number !== target.prNumber) return undefined

  const links = record(payload._links)
  const htmlLink = record(links?.html)
  const headRepo = stringValue(record(payload.headRepository)?.nameWithOwner)
    ?? stringValue(record(record(payload.head)?.repo)?.full_name)
    ?? headRepositoryName(payload)
  return {
    repo: target.repo,
    number,
    title: stringValue(payload.title) ?? '',
    body: stringValue(payload.body) ?? '',
    state: stringValue(payload.state),
    draft: booleanValue(payload.isDraft) ?? booleanValue(payload.draft),
    merged: booleanValue(payload.merged),
    url: stringValue(payload.url) ?? stringValue(payload.html_url) ?? stringValue(htmlLink?.href) ?? target.url ?? `https://github.com/${target.repo}/pull/${number}`,
    headRef: stringValue(payload.headRefName) ?? stringValue(payload.head_ref) ?? stringValue(record(payload.head)?.ref),
    headSha: stringValue(payload.headRefOid) ?? stringValue(record(payload.head)?.sha),
    baseRef: stringValue(payload.baseRefName) ?? stringValue(record(payload.base)?.ref),
    headRepo,
    crossRepository: booleanValue(payload.isCrossRepository)
      ?? (headRepo ? headRepo.toLowerCase() !== target.repo.toLowerCase() : undefined),
    maintainerCanModify: booleanValue(payload.maintainerCanModify) ?? booleanValue(payload.maintainer_can_modify),
    filesChanged: changedFiles(payload.filesChanged ?? payload.files),
  }
}

const hasRequiredPullMetadata = (pr: Omit<StandalonePullRequest, 'source'>): boolean =>
  Boolean(pr.title.trim()) &&
  Boolean(pr.state?.trim()) &&
  pr.draft !== undefined &&
  Boolean(pr.headRef?.trim()) &&
  Boolean(pr.headSha?.trim()) &&
  Boolean(pr.headRepo?.trim()) &&
  Boolean(pr.baseRef?.trim()) &&
  pr.crossRepository !== undefined

function mergePullMetadata(
  mounted: Omit<StandalonePullRequest, 'source'> | undefined,
  live: Omit<StandalonePullRequest, 'source'>,
): Omit<StandalonePullRequest, 'source'> {
  if (!mounted) return live
  return {
    repo: live.repo,
    number: live.number,
    title: live.title || mounted.title,
    body: live.body || mounted.body,
    state: live.state ?? mounted.state,
    draft: live.draft ?? mounted.draft,
    merged: live.merged ?? mounted.merged,
    url: live.url || mounted.url,
    headRef: live.headRef ?? mounted.headRef,
    headSha: live.headSha ?? mounted.headSha,
    baseRef: live.baseRef ?? mounted.baseRef,
    headRepo: live.headRepo ?? mounted.headRepo,
    crossRepository: live.crossRepository ?? mounted.crossRepository,
    maintainerCanModify: live.maintainerCanModify ?? mounted.maintainerCanModify,
    filesChanged: live.filesChanged ?? mounted.filesChanged,
  }
}

function changedFiles(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined
  const paths = value
    .map((entry) => {
      if (typeof entry === 'string') return entry
      const file = record(entry)
      return stringValue(file?.path) ?? stringValue(file?.filename)
    })
    .filter((path): path is string => Boolean(path))
  return paths.length > 0 ? paths : undefined
}

function headRepositoryName(payload: Record<string, unknown>): string | undefined {
  const owner = record(payload.headRepositoryOwner)
  const repo = record(payload.headRepository)
  const ownerName = stringValue(payload.headRepositoryOwner) ?? stringValue(owner?.login) ?? stringValue(owner?.name)
  const repoName = stringValue(repo?.name)
  return ownerName && repoName ? `${ownerName}/${repoName}` : undefined
}

const isGithubOwner = (value: string): boolean =>
  value.length <= 39 && /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/u.test(value)

const isGithubRepo = (value: string): boolean =>
  value.length <= 100 && /^[A-Za-z0-9._-]+$/u.test(value) && value !== '.' && value !== '..'

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' ? value : typeof value === 'string' && value.trim() ? Number(value) : undefined

const booleanValue = (value: unknown): boolean | undefined =>
  typeof value === 'boolean' ? value : undefined
