import type { FactoryConfig } from '../config/schema'
import type { MountClient } from '../ports'
import { asRecord, stableHash, wrappedPayload } from '../writeback/shared'

export type RoutedPrCandidate = {
  repo: string
  number: number
  title: string
  body: string
  state: string
  draft: boolean
  merged: boolean
  url: string
  headRef: string
  headSha: string
  baseRef: string
  headRepo: string
  crossRepository: boolean
  maintainerCanModify?: boolean
  labels: string[]
  filesChanged?: string[]
  path: string
  revision: string
}

export type RoutedPrDiscoveryReport = {
  scanned: number
  eligible: number
  excluded: number
  incomplete: number
  terminal: number
  crossRepository: number
  duplicates: number
  failures: Array<{ repo: string; prNumber?: number; reason: string }>
  candidates: RoutedPrCandidate[]
}

export const routedPrIdentity = (repo: string, prNumber: number): string =>
  `${repo.toLowerCase()}#${prNumber}`

export const routedPrRepos = (config: FactoryConfig): string[] => {
  const repos = new Map<string, string>()
  for (const name of config.repos.names ?? []) {
    const configured = config.repos.byLabel[name] ?? name
    const repo = configured.includes('/')
      ? configured
      : config.repos.org
        ? `${config.repos.org}/${configured}`
        : undefined
    if (!repo || !/^[^/]+\/[^/]+$/u.test(repo)) continue
    repos.set(repo.toLowerCase(), repo)
  }
  return [...repos.values()].sort((left, right) => left.localeCompare(right))
}

export async function discoverRoutedPullRequests(
  mount: MountClient,
  config: FactoryConfig,
): Promise<RoutedPrDiscoveryReport> {
  const report: RoutedPrDiscoveryReport = {
    scanned: 0,
    eligible: 0,
    excluded: 0,
    incomplete: 0,
    terminal: 0,
    crossRepository: 0,
    duplicates: 0,
    failures: [],
    candidates: [],
  }
  const seen = new Set<string>()
  const excludedLabels = new Set(config.babysitter.excludeLabels.map((label) => label.trim().toLowerCase()))
  const excludedPrs = new Set(config.babysitter.excludePullRequests.map((identity) => identity.toLowerCase()))

  for (const repo of routedPrRepos(config)) {
    const [owner, name] = repo.split('/')
    const roots = [
      `/github/repos/${owner}/${name}/pulls/`,
      `/github/repos/${owner}__${name}/pulls/by-id/`,
    ]
    const paths = new Set<string>()
    try {
      for (const root of roots) {
        for (const path of await mount.listTree(root)) paths.add(path)
      }
    } catch (error) {
      report.failures.push({ repo, reason: error instanceof Error ? error.message : String(error) })
      continue
    }
    for (const path of [...paths].sort()) {
      const pathNumber = pullNumberFromPath(path, owner!, name!)
      if (!pathNumber) continue
      report.scanned += 1
      const identity = routedPrIdentity(repo, pathNumber)
      if (seen.has(identity)) {
        report.duplicates += 1
        continue
      }
      seen.add(identity)
      let candidate: RoutedPrCandidate | undefined
      try {
        candidate = parseRoutedPrCandidate((await mount.readFile(path)).content, repo, pathNumber, path)
      } catch (error) {
        report.failures.push({ repo, prNumber: pathNumber, reason: error instanceof Error ? error.message : String(error) })
        continue
      }
      if (!candidate) {
        report.incomplete += 1
        report.failures.push({ repo, prNumber: pathNumber, reason: 'incomplete authoritative PR metadata' })
        continue
      }
      if (candidate.merged || candidate.state !== 'OPEN' || candidate.draft) {
        report.terminal += 1
        continue
      }
      if (candidate.crossRepository || candidate.headRepo.toLowerCase() !== repo.toLowerCase()) {
        report.crossRepository += 1
        continue
      }
      if (excludedPrs.has(identity) || candidate.labels.some((label) => excludedLabels.has(label.toLowerCase()))) {
        report.excluded += 1
        continue
      }
      report.eligible += 1
      report.candidates.push(candidate)
    }
  }
  report.candidates.sort((left, right) =>
    left.repo.localeCompare(right.repo) || left.number - right.number
  )
  return report
}

const pullNumberFromPath = (path: string, owner: string, repo: string): number | undefined => {
  const match = path.match(
    /^\/github\/repos\/(?:([^/]+)\/([^/]+)|([^/]+)__([^/]+))\/pulls\/(?:by-id\/)?(\d+)(?:__[^/]*)?(?:\/(?:meta|metadata)\.json|\.json)$/u,
  )
  const actualOwner = match?.[1] ?? match?.[3]
  const actualRepo = match?.[2] ?? match?.[4]
  const number = Number(match?.[5])
  return actualOwner?.toLowerCase() === owner.toLowerCase() &&
    actualRepo?.toLowerCase() === repo.toLowerCase() &&
    Number.isSafeInteger(number) && number > 0
    ? number
    : undefined
}

const parseRoutedPrCandidate = (
  content: unknown,
  repo: string,
  number: number,
  path: string,
): RoutedPrCandidate | undefined => {
  const payload = wrappedPayload(content)
  const explicitNumber = numberValue(payload.number)
  if (explicitNumber !== undefined && explicitNumber !== number) return undefined
  const head = asRecord(payload.head)
  const base = asRecord(payload.base)
  const headRepository = asRecord(payload.headRepository)
  const headOwner = asRecord(payload.headRepositoryOwner)
  const headRepo = stringValue(asRecord(head?.repo)?.full_name) ??
    stringValue(headRepository?.nameWithOwner) ??
    (() => {
      const name = stringValue(headRepository?.name)
      const owner = stringValue(headOwner?.login) ?? stringValue(headOwner?.name)
      return name && owner ? `${owner}/${name}` : undefined
    })()
  const title = stringValue(payload.title)
  const state = stringValue(payload.state)?.toUpperCase()
  const draft = booleanValue(payload.isDraft) ?? booleanValue(payload.draft)
  const merged = booleanValue(payload.merged) ?? state === 'MERGED'
  const headRef = stringValue(payload.headRefName) ?? stringValue(head?.ref) ?? stringValue(payload.head_ref)
  const headSha = stringValue(payload.headRefOid) ?? stringValue(head?.sha)
  const baseRef = stringValue(payload.baseRefName) ?? stringValue(base?.ref)
  const crossRepository = booleanValue(payload.isCrossRepository) ??
    booleanValue(payload.crossRepository) ??
    (headRepo ? headRepo.toLowerCase() !== repo.toLowerCase() : undefined)
  if (!title || !state || draft === undefined || merged === undefined || !headRef || !headSha || !baseRef || !headRepo || crossRepository === undefined || !Array.isArray(payload.labels)) {
    return undefined
  }
  const labels = labelNames(payload.labels)
  const body = stringValue(payload.body) ?? ''
  const candidate = {
    repo,
    number,
    title,
    body,
    state,
    draft,
    merged,
    url: stringValue(payload.url) ?? stringValue(payload.html_url) ?? `https://github.com/${repo}/pull/${number}`,
    headRef,
    headSha,
    baseRef,
    headRepo,
    crossRepository,
    maintainerCanModify: booleanValue(payload.maintainerCanModify) ?? booleanValue(payload.maintainer_can_modify),
    labels,
    filesChanged: changedFiles(payload.filesChanged ?? payload.files),
    path,
  }
  return {
    ...candidate,
    revision: stableHash(JSON.stringify({
      headSha: candidate.headSha,
      labels: [...candidate.labels].sort(),
      state: candidate.state,
      draft: candidate.draft,
      body: candidate.body,
      title: candidate.title,
      updatedAt: stringValue(payload.updatedAt) ?? stringValue(payload.updated_at),
      reviewDecision: stringValue(payload.reviewDecision) ?? stringValue(payload.review_decision),
      statusCheckRollup: payload.statusCheckRollup ?? payload.status_check_rollup,
    })),
  }
}

const labelNames = (value: unknown): string[] => Array.isArray(value)
  ? value.map((entry) => typeof entry === 'string' ? entry : stringValue(asRecord(entry)?.name))
    .filter((entry): entry is string => Boolean(entry))
  : []

const changedFiles = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) return undefined
  const paths = value.map((entry) => typeof entry === 'string' ? entry : stringValue(asRecord(entry)?.path))
    .filter((entry): entry is string => Boolean(entry))
  return paths.length > 0 ? paths : undefined
}

const stringValue = (value: unknown): string | undefined => typeof value === 'string' && value.trim() ? value : undefined
const booleanValue = (value: unknown): boolean | undefined => typeof value === 'boolean' ? value : undefined
const numberValue = (value: unknown): number | undefined => typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined
