import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { GithubConnectionWrite, MountClient } from '../ports'
import type { GithubPublishPullRequestInput, GithubPublishPullRequestResult } from '../ports/mount'
import type { GithubIssueStatus, GithubStatusWriteResult, GithubWriteback } from '../ports/writeback'
import { defaultGhRunner, type GhRunner } from '../github/merge-gate'
import type { LinearIssue, PrSummary } from '../types'
import { asRecord, wrappedPayload } from './shared'

const execFileAsync = promisify(execFile)

export const FACTORY_GITHUB_STATUS_LABELS: Record<Exclude<GithubIssueStatus, 'ready'>, { name: string; color: string; description: string }> = {
  'in-progress': {
    name: 'factory:in-progress',
    color: '1d76db',
    description: 'Factory agents are working on this issue.',
  },
  'human-review': {
    name: 'factory:human-review',
    color: 'fbca04',
    description: 'Factory work is ready for human review.',
  },
}

const repoDir = (repo: string): string => {
  if (repo.includes('__')) {
    return repo
  }

  const [owner, name] = repo.split('/')
  if (!owner || !name) {
    throw new Error(`GitHub repo must be owner/repo or owner__repo: ${repo}`)
  }

  return `${owner}__${name}`
}

const prPath = (repo: string, number: number): string =>
  `/github/repos/${repoDir(repo)}/pulls/by-id/${number}.json`

export const MountGithubRead = (mount: MountClient) => ({
  async getPr(repo: string, number: number): Promise<PrSummary> {
    const { content } = await mount.readFile(prPath(repo, number))
    const payload = wrappedPayload(content)

    return {
      repo,
      number: numberValue(payload.number) ?? number,
      title: typeof payload.title === 'string' ? payload.title : undefined,
      url: typeof payload.url === 'string' ? payload.url : undefined,
      state: typeof payload.state === 'string' ? payload.state : undefined,
      headRef: refName(payload.headRef) ?? refName(payload.head) ?? stringValue(payload.head_ref),
      baseRef: refName(payload.baseRef) ?? refName(payload.base) ?? stringValue(payload.base_ref),
      author: refName(payload.author) ?? stringValue(payload.user),
      filesChanged: filesChanged(payload.files_changed ?? payload.filesChanged ?? payload.files),
    }
  },
})

export interface GhCliGithubWritebackConfig {
  runner?: GhRunner
  gitRunner?: GhRunner
}

type AppIssueConnectionWrite = GithubConnectionWrite & Required<Pick<
  GithubConnectionWrite,
  'postIssueComment' | 'ensureRepositoryLabel' | 'mutateIssueLabel' | 'updateIssue'
>>

/**
 * Orchestrator-facing GitHub lifecycle adapter for the connected App writer.
 * Provider-authoritative reads remain intentionally absent until the mount
 * exposes an app-actor read contract; GithubWriteback models those as optional.
 */
export class AppGithubWriteback implements GithubWriteback {
  readonly #write: AppIssueConnectionWrite

  constructor(write: GithubConnectionWrite) {
    if (!write.postIssueComment || !write.ensureRepositoryLabel || !write.mutateIssueLabel || !write.updateIssue) {
      throw new Error(
        'GitHub App lifecycle writeback requires connected comment, label, and issue-update capabilities',
      )
    }
    this.#write = write as AppIssueConnectionWrite
  }

  async publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult> {
    return await this.#write.publishPullRequest(input)
  }

  async postComment(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.#write.postIssueComment({
      repo: ref.repo,
      number: ref.number,
      body,
      author: 'app',
    })
  }

  async setStatus(issue: LinearIssue, status: GithubIssueStatus): Promise<GithubStatusWriteResult> {
    const ref = githubIssueRef(issue)
    if (status === 'ready') {
      for (const label of Object.values(FACTORY_GITHUB_STATUS_LABELS)) {
        await this.#write.mutateIssueLabel({
          repo: ref.repo,
          number: ref.number,
          operation: 'remove',
          label: label.name,
          author: 'app',
        })
      }
      return 'applied'
    }
    const target = FACTORY_GITHUB_STATUS_LABELS[status]
    const previous = FACTORY_GITHUB_STATUS_LABELS[status === 'in-progress' ? 'human-review' : 'in-progress']
    await this.#write.ensureRepositoryLabel({
      repo: ref.repo,
      ...target,
      author: 'app',
    })
    await this.#write.mutateIssueLabel({
      repo: ref.repo,
      number: ref.number,
      operation: 'add',
      label: target.name,
      author: 'app',
    })
    await this.#write.mutateIssueLabel({
      repo: ref.repo,
      number: ref.number,
      operation: 'remove',
      label: previous.name,
      author: 'app',
    })
    return 'applied'
  }

  async closeIssue(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.postComment(issue, body)
    await this.#write.updateIssue({
      repo: ref.repo,
      number: ref.number,
      state: 'closed',
      author: 'app',
    })
  }
}

/**
 * Compatibility GitHub issue lifecycle writeback using authenticated `gh`
 * primitives. Labels are created idempotently before use so a newly-onboarded
 * repository does not need Factory status labels provisioned by hand.
 */
export class GhCliGithubWriteback implements GithubWriteback {
  readonly #run: GhRunner
  readonly #git: GhRunner

  constructor(config: GhCliGithubWritebackConfig = {}) {
    this.#run = config.runner ?? defaultGhRunner
    this.#git = config.gitRunner ?? defaultGitRunner
  }

  /** Publish a PR as the GitHub user authenticated by the local `gh` CLI. */
  async publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult> {
    const headRef = input.headRef ?? (input.clonePath
      ? await this.#gitValue(['-C', input.clonePath, 'symbolic-ref', '--short', 'HEAD'], 'current branch')
      : undefined)
    if (!headRef) {
      throw new Error('GitHub user PR publication requires headRef or clonePath')
    }
    if (input.expectedHeadRef && headRef !== input.expectedHeadRef) {
      throw new Error(
        `Refusing to publish GitHub PR: expected head branch ${input.expectedHeadRef}, found ${headRef}`,
      )
    }
    if (headRef === input.baseRef) {
      throw new Error(`Refusing to publish GitHub PR with head equal to base branch: ${headRef}`)
    }
    const headSha = input.headSha ?? (input.clonePath
      ? await this.#gitValue(['-C', input.clonePath, 'rev-parse', 'HEAD'], 'HEAD commit')
      : undefined)

    // A local exit-recovery branch may only exist in Factory's checkout. Push
    // it without force before asking GitHub to create the PR as the gh user.
    if (input.clonePath && !input.headRef) {
      await this.#git([
        '-C',
        input.clonePath,
        'push',
        'origin',
        `HEAD:refs/heads/${headRef}`,
      ])
    }

    // Best-effort: record a late attestation grant so the session reference
    // rides through to the attestation ledger. Silently omits when the relay
    // auth env vars are absent (operator key path, no workspace token).
    // Prefer the per-agent sessionRef over the process-wide env var so that
    // concurrent implementers each record their own session.
    const sessionRef = input.sessionRef ?? (process.env.RELAY_ATTEST_SESSION_ID || undefined)
    await postAttestationGrant(input.repo, sessionRef).catch(() => undefined)

    const created = await this.#run([
      'pr',
      'create',
      '--repo',
      input.repo,
      '--head',
      headRef,
      '--base',
      input.baseRef,
      '--title',
      input.title,
      '--body',
      input.body,
    ])
    const createdUrl = githubPullRequestUrl(`${created.stdout}\n${created.stderr ?? ''}`, input.repo)
    if (!createdUrl) {
      throw new Error(`gh PR publication returned no pull request URL for ${input.repo}/${headRef}`)
    }

    const viewed = await this.#run([
      'pr',
      'view',
      createdUrl,
      '--repo',
      input.repo,
      '--json',
      'number,url,headRefName,headRefOid,author',
    ])
    const receipt = asRecord(JSON.parse(viewed.stdout))
    const number = numberValue(receipt?.number)
    const url = stringValue(receipt?.url)
    const confirmedHeadRef = stringValue(receipt?.headRefName)
    const confirmedHeadSha = stringValue(receipt?.headRefOid)
    const author = stringValue(asRecord(receipt?.author)?.login) ?? stringValue(receipt?.author)
    if (!number || !url || confirmedHeadRef !== headRef || !author) {
      throw new Error(`gh PR publication returned an incomplete receipt for ${input.repo}/${headRef}`)
    }

    return {
      repo: input.repo,
      number,
      url,
      headRef: confirmedHeadRef,
      ...(confirmedHeadSha ?? headSha ? { headSha: confirmedHeadSha ?? headSha } : {}),
      author,
    }
  }

  async getIssueAuthor(issue: LinearIssue): Promise<string | undefined> {
    const ref = githubIssueRef(issue)
    const result = await this.#run([
      'issue',
      'view',
      String(ref.number),
      '--repo',
      ref.repo,
      '--json',
      'author',
    ])
    if (!result.stdout.trim()) return undefined
    const author = asRecord(JSON.parse(result.stdout))?.author
    return stringValue(asRecord(author)?.login)?.trim() || undefined
  }

  async getIssueStatus(issue: LinearIssue): Promise<GithubIssueStatus> {
    const labels = await this.#issueLabels(githubIssueRef(issue))
    if (labels.has(FACTORY_GITHUB_STATUS_LABELS['human-review'].name.toLowerCase())) return 'human-review'
    if (labels.has(FACTORY_GITHUB_STATUS_LABELS['in-progress'].name.toLowerCase())) return 'in-progress'
    return 'ready'
  }

  async postComment(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.#run([
      'issue',
      'comment',
      String(ref.number),
      '--repo',
      ref.repo,
      '--body',
      body,
    ])
  }

  async hasCommentMarker(issue: LinearIssue, marker: string): Promise<boolean> {
    const ref = githubIssueRef(issue)
    const result = await this.#run([
      'api',
      '--paginate',
      `repos/${ref.repo}/issues/${ref.number}/comments`,
      '--jq',
      '.[].body',
    ])
    return result.stdout.includes(marker)
  }

  async setStatus(issue: LinearIssue, status: GithubIssueStatus): Promise<GithubStatusWriteResult> {
    const ref = githubIssueRef(issue)
    if (status === 'ready') {
      const labels = await this.#issueLabels(ref)
      const editArgs = ['issue', 'edit', String(ref.number), '--repo', ref.repo]
      for (const label of Object.values(FACTORY_GITHUB_STATUS_LABELS)) {
        if (labels.has(label.name.toLowerCase())) {
          editArgs.push('--remove-label', label.name)
        }
      }
      const applied = editArgs.length > 5
      if (applied) {
        await this.#run(editArgs)
      }
      const confirmed = await this.#issueLabels(ref)
      if (Object.values(FACTORY_GITHUB_STATUS_LABELS).some((label) => confirmed.has(label.name.toLowerCase()))) {
        throw new Error(`GitHub writeback did not confirm removal of Factory status labels on ${ref.repo}#${ref.number}`)
      }
      return applied ? 'applied' : 'already-matched'
    }
    const target = FACTORY_GITHUB_STATUS_LABELS[status]
    const previous = FACTORY_GITHUB_STATUS_LABELS[status === 'in-progress' ? 'human-review' : 'in-progress']
    await this.#run([
      'label',
      'create',
      target.name,
      '--repo',
      ref.repo,
      '--color',
      target.color,
      '--description',
      target.description,
      '--force',
    ])
    const labels = await this.#issueLabels(ref)
    const editArgs = [
      'issue',
      'edit',
      String(ref.number),
      '--repo',
      ref.repo,
    ]
    if (!labels.has(target.name.toLowerCase())) {
      editArgs.push('--add-label', target.name)
    }
    if (labels.has(previous.name.toLowerCase())) {
      editArgs.push('--remove-label', previous.name)
    }
    const applied = editArgs.length > 5
    if (applied) {
      await this.#run(editArgs)
    }
    const confirmed = await this.#issueLabels(ref)
    if (confirmed.has(target.name.toLowerCase()) && !confirmed.has(previous.name.toLowerCase())) {
      return applied ? 'applied' : 'already-matched'
    }
    throw new Error(`GitHub writeback did not confirm ${target.name} on ${ref.repo}#${ref.number}`)
  }

  async #issueLabels(ref: { repo: string; number: number }): Promise<Set<string>> {
    const result = await this.#run([
      'issue',
      'view',
      String(ref.number),
      '--repo',
      ref.repo,
      '--json',
      'labels',
    ])
    if (!result.stdout.trim()) {
      return new Set()
    }
    const parsed = JSON.parse(result.stdout) as { labels?: Array<{ name?: unknown }> }
    return new Set(
      (parsed.labels ?? [])
        .map((label) => stringValue(label.name)?.toLowerCase())
        .filter((label): label is string => Boolean(label)),
    )
  }

  async closeIssue(issue: LinearIssue, body: string): Promise<void> {
    const ref = githubIssueRef(issue)
    await this.postComment(issue, body)
    await this.#run([
      'issue',
      'close',
      String(ref.number),
      '--repo',
      ref.repo,
      '--reason',
      'completed',
    ])
  }

  async #gitValue(args: string[], description: string): Promise<string> {
    try {
      const value = (await this.#git(args)).stdout.trim()
      if (value) return value
    } catch (error) {
      throw new Error(`Unable to resolve ${description} for GitHub user PR publication: ${errorMessage(error)}`)
    }
    throw new Error(`Unable to resolve ${description} for GitHub user PR publication`)
  }
}

const defaultGitRunner: GhRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('git', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

/**
 * Post a late attestation grant to the relay auth API so the session reference
 * rides through to the attestation ledger after the commit is pushed. The call
 * is a best-effort fire-and-forget: it requires RELAYAUTH_URL,
 * RELAY_ATTEST_API_KEY, and RELAY_ATTEST_AGENT_ID to be set in the agent
 * environment; when any of those are absent the function resolves immediately.
 * RELAY_ATTEST_SESSION_ID is optional — when set it threads the session
 * reference into the ledger entry so attestation records are linkable to the
 * Claude Code / Codex session that produced the commit.
 */
async function postAttestationGrant(repo: string, sessionRef?: string): Promise<void> {
  const baseUrl = process.env.RELAYAUTH_URL
  const apiKey = process.env.RELAY_ATTEST_API_KEY
  const agentId = process.env.RELAY_ATTEST_AGENT_ID
  if (!baseUrl || !apiKey || !agentId) return

  const url = new URL('v1/attestations/grants', baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`).toString()
  await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
    },
    body: JSON.stringify({
      agentId,
      repo,
      late: true,
      ...(sessionRef ? { sessionRef } : {}),
    }),
    signal: AbortSignal.timeout(5000),
  })
}

const githubPullRequestUrl = (value: string, repo: string): string | undefined => {
  const escapedRepo = repo.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
  return new RegExp(`https://github\\.com/${escapedRepo}/pull/[1-9][0-9]*`, 'iu').exec(value)?.[0]
}

const githubIssueRef = (issue: LinearIssue): { repo: string; number: number; url: string } => {
  const payload = wrappedPayload(issue.raw)
  const source = asRecord(payload.source)
  const provider = stringValue(source?.provider)?.toLowerCase()
  const owner = stringValue(source?.owner)
  const repoName = stringValue(source?.repo)
  const number = numberValue(source?.number)
  const url = stringValue(source?.url)
  if (provider !== 'github' || !owner || !repoName || !Number.isInteger(number) || (number ?? 0) <= 0 || !url) {
    throw new Error(`GitHub writeback requires a stable GitHub issue source: ${issue.key}`)
  }
  const repo = `${owner}/${repoName}`
  const normalizedUrl = url.toLowerCase()
  const expectedUrlPrefixes = [
    `https://github.com/${repo}/issues/${number}`,
    `https://api.github.com/repos/${repo}/issues/${number}`,
  ].map((candidate) => candidate.toLowerCase())
  if (!expectedUrlPrefixes.some((prefix) => matchesBoundary(normalizedUrl, prefix))) {
    throw new Error(`GitHub writeback source URL does not match ${repo}#${number}`)
  }
  return { repo, number: number!, url }
}

const matchesBoundary = (value: string, prefix: string): boolean => {
  if (!value.startsWith(prefix)) return false
  const next = value[prefix.length]
  return next === undefined || next === '/' || next === '?' || next === '#'
}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' ? value : undefined

const refName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.ref) ?? stringValue(record?.login)
}

const errorMessage = (error: unknown): string => error instanceof Error ? error.message : String(error)

const filesChanged = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined
  }

  const files = value
    .map((entry) => typeof entry === 'string' ? entry : stringValue(asRecord(entry)?.path) ?? stringValue(asRecord(entry)?.filename))
    .filter((entry): entry is string => Boolean(entry))
  return files.length > 0 ? files : undefined
}
