import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type {
  GithubConnectionWrite,
  GithubPublishPullRequestInput,
  GithubPublishPullRequestResult,
  MountClient,
} from '../ports'

const execFileAsync = promisify(execFile)
const WRITE_CONFIRM_TIMEOUT_MS = 90_000

export type GitCommandRunner = (args: string[]) => Promise<{ stdout: string; stderr?: string }>

export interface RelayfileGithubConnectionWriteConfig {
  mount: Pick<MountClient, 'confirmWrite' | 'readFile' | 'writeFile'>
  gitRunner?: GitCommandRunner
}

/**
 * GitHub PR mutations backed by the workspace's file-native Relayfile
 * connection. The adapter is server-side; Factory only depends on the stable
 * write paths and payload contracts exposed through MountClient.
 */
export class RelayfileGithubConnectionWrite implements GithubConnectionWrite {
  readonly #mount: RelayfileGithubConnectionWriteConfig['mount']
  readonly #git: GitCommandRunner

  constructor(config: RelayfileGithubConnectionWriteConfig) {
    this.#mount = config.mount
    this.#git = config.gitRunner ?? defaultGitRunner
  }

  async publishPullRequest(input: GithubPublishPullRequestInput): Promise<GithubPublishPullRequestResult> {
    const { owner, repo } = githubRepoParts(input.repo)
    const headRef = await this.#gitValue(['-C', input.clonePath, 'symbolic-ref', '--short', 'HEAD'], 'current branch')
    const headSha = await this.#gitValue(['-C', input.clonePath, 'rev-parse', 'HEAD'], 'HEAD commit')
    const draftName = githubDraftName(headRef, headSha)
    const repoRoot = `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`
    const fullHeadRef = `refs/heads/${headRef}`
    const refPath = `${repoRoot}/refs/${encodeURIComponent(fullHeadRef)}.json`

    await this.#writeAndConfirm(refPath, {
      ref: fullHeadRef,
      sha: headSha,
    })

    const pullRequestPath = `${repoRoot}/pull-requests/${draftName}.json`
    await this.#writeAndConfirm(pullRequestPath, {
      title: input.title,
      head: headRef,
      base: input.baseRef,
      body: input.body,
      // TODO(issue-52): make workspace PR authorship configurable once Factory
      // has an explicit user-vs-app identity policy.
      author: 'app',
    })

    const receipt = record((await this.#mount.readFile(pullRequestPath)).content)
    const number = positiveInteger(receipt.created)
    const url = stringValue(receipt.url)
    if (!number || !url) {
      throw new Error(`GitHub pull request writeback returned an incomplete receipt for ${input.repo}`)
    }

    return {
      repo: input.repo,
      number,
      url,
      headRef,
      headSha,
    }
  }

  async closePullRequest(input: { repo: string; number: number }): Promise<void> {
    const { owner, repo } = githubRepoParts(input.repo)
    if (!Number.isInteger(input.number) || input.number <= 0) {
      throw new Error(`GitHub pull request number must be a positive integer: ${input.number}`)
    }
    await this.#writeAndConfirm(
      `/github/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/pulls/${input.number}/close.json`,
      {},
    )
  }

  async #gitValue(args: string[], description: string): Promise<string> {
    try {
      const value = (await this.#git(args)).stdout.trim()
      if (value) return value
    } catch (error) {
      throw new Error(`Unable to resolve ${description} for GitHub PR publication: ${errorMessage(error)}`)
    }
    throw new Error(`Unable to resolve ${description} for GitHub PR publication`)
  }

  async #writeAndConfirm(path: string, content: unknown): Promise<void> {
    await this.#mount.writeFile(path, content, { guarded: true })
    const status = await this.#mount.confirmWrite(path, { timeoutMs: WRITE_CONFIRM_TIMEOUT_MS })
    if (status !== 'acked') {
      throw new Error(`GitHub writeback did not complete for ${path}: ${status}`)
    }
  }
}

const defaultGitRunner: GitCommandRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('git', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}

const githubRepoParts = (value: string): { owner: string; repo: string } => {
  const match = /^([^/]+)\/([^/]+)$/u.exec(value.trim())
  if (!match?.[1] || !match[2]) {
    throw new Error(`GitHub repo must be owner/repo: ${value}`)
  }
  return { owner: match[1], repo: match[2] }
}

const githubDraftName = (headRef: string, headSha: string): string => {
  const branch = headRef.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'branch'
  return `factory-${branch.slice(0, 80)}-${headSha.slice(0, 12)}`
}

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' && value.length > 0 ? value : undefined

const positiveInteger = (value: unknown): number | undefined => {
  const number = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isInteger(number) && number > 0 ? number : undefined
}

const errorMessage = (error: unknown): string => error instanceof Error ? error.message : String(error)
