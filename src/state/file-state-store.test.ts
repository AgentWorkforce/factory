import { mkdir, mkdtemp, readdir, rm, stat, utimes } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { describe, expect, it } from 'vitest'

import type { GithubIssueCommentWatchState } from '../ports/state'
import { FileStateStore } from './file-state-store'

describe('FileStateStore', () => {
  it('restores GitHub escalation watches in a fresh store instance', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const watch = githubWatch(55)
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      await first.setGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#55', watch)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.listGithubIssueCommentWatches('workspace-1')).toEqual([
        ['agentworkforce/factory#55', watch],
      ])

      await restarted.clearGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#55')
      const afterClear = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await afterClear.listGithubIssueCommentWatches('workspace-1')).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('merges interleaved writes from independently constructed stores', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-concurrent-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const lockPath = `${watchStatePath}.lock`
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      const second = new FileStateStore({ batchSize: 2, watchStatePath })

      // Prime both instances before either mutation. This reproduces the stale
      // per-process document that used to overwrite another process's update.
      await Promise.all([
        first.listGithubIssueCommentWatches('workspace-1'),
        second.listGithubIssueCommentWatches('workspace-1'),
      ])

      // A real lock directory is a barrier: both writes must remain pending
      // until it is removed, then serialize their reload/mutate/write cycles.
      await mkdir(lockPath)
      let firstSettled = false
      let secondSettled = false
      const firstWrite = first
        .setGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#55', githubWatch(55, 'claim-55'))
        .finally(() => { firstSettled = true })
      const secondWrite = second
        .setGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#62', githubWatch(62, 'claim-62'))
        .finally(() => { secondSettled = true })

      await delay(50)
      expect([firstSettled, secondSettled]).toEqual([false, false])
      await rm(lockPath, { recursive: true })
      await Promise.all([firstWrite, secondWrite])

      expect(Object.fromEntries(await first.listGithubIssueCommentWatches('workspace-1'))).toEqual({
        'agentworkforce/factory#55': githubWatch(55, 'claim-55'),
        'agentworkforce/factory#62': githubWatch(62, 'claim-62'),
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('serializes a concurrent set followed by clear on the same key', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-set-clear-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const lockPath = `${watchStatePath}.lock`
      const setter = new FileStateStore({ batchSize: 2, watchStatePath })
      const clearer = new FileStateStore({ batchSize: 2, watchStatePath })
      const key = 'agentworkforce/factory#62'
      const largeWatch = githubWatch(62)
      largeWatch.processedCommentIds = Array.from({ length: 100_000 }, (_, index) => String(index))

      let setSettled = false
      const set = setter
        .setGithubIssueCommentWatch('workspace-1', key, largeWatch)
        .finally(() => { setSettled = true })
      await expectLockWhilePending(lockPath, () => setSettled)

      // The clear starts while set owns the lock, so it is deterministically
      // the last writer. Same-key conflicts therefore have last-writer-wins
      // semantics without ever publishing a partial document.
      const clear = clearer.clearGithubIssueCommentWatch('workspace-1', key)
      await Promise.all([set, clear])

      expect(await setter.listGithubIssueCommentWatches('workspace-1')).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('recovers an expired lock left by a crashed writer', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-stale-lock-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const lockPath = `${watchStatePath}.lock`
      await mkdir(lockPath)
      const expired = new Date(Date.now() - 90_000)
      await utimes(lockPath, expired, expired)

      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      const write = store
        .setGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#62', githubWatch(62))
        .then(() => 'written')
      expect(await Promise.race([write, delay(1_000, 'timed-out')])).toBe('written')
      expect(await store.listGithubIssueCommentWatches('workspace-1')).toEqual([
        ['agentworkforce/factory#62', githubWatch(62)],
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not reclaim a paused writer lock at the old ten-second threshold', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-paused-lock-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const lockPath = `${watchStatePath}.lock`
      await mkdir(lockPath)
      const pausedAt = new Date(Date.now() - 30_000)
      await utimes(lockPath, pausedAt, pausedAt)

      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      let settled = false
      const write = store
        .setGithubIssueCommentWatch('workspace-1', 'agentworkforce/factory#62', githubWatch(62))
        .finally(() => { settled = true })

      // A 30-second process pause exceeded the old lease but remains inside
      // the widened safety window, so another writer must not reclaim it.
      await delay(50)
      expect(settled).toBe(false)
      await rm(lockPath, { recursive: true })
      await write

      expect(await store.listGithubIssueCommentWatches('workspace-1')).toEqual([
        ['agentworkforce/factory#62', githubWatch(62)],
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('removes temporary and lock artifacts after a clean write cycle', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-cleanup-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      const key = 'agentworkforce/factory#62'
      await store.setGithubIssueCommentWatch('workspace-1', key, githubWatch(62))
      await store.clearGithubIssueCommentWatch('workspace-1', key)

      expect((await readdir(root)).filter((entry) => entry.endsWith('.tmp') || entry.endsWith('.lock'))).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

const githubWatch = (number: number, claimedByCommentId?: string): GithubIssueCommentWatchState => ({
  issue: { uuid: `uuid-${number}`, key: `AR-${number}`, path: `/linear/issues/AR-${number}__uuid-${number}.json` },
  source: {
    owner: 'AgentWorkforce',
    repo: 'factory',
    number,
    url: `https://github.com/AgentWorkforce/factory/issues/${number}`,
  },
  pending: [{
    correlationId: `factory-agent-question-${number}`,
    kind: 'agent-question',
    authorizedAuthor: 'issue-reporter',
    ...(claimedByCommentId ? { claimedByCommentId } : {}),
  }],
  sinceCommentId: '100',
  lastSeenCommentId: '101',
  processedCommentIds: ['101'],
})

const expectLockWhilePending = async (
  lockPath: string,
  settled: () => boolean,
): Promise<void> => {
  const deadline = Date.now() + 1_000
  while (Date.now() < deadline) {
    try {
      await stat(lockPath)
      expect(settled()).toBe(false)
      return
    } catch (error) {
      if (!isMissingFileError(error)) throw error
    }
    await delay(1)
  }
  throw new Error('Timed out waiting for FileStateStore to acquire its lock')
}

const isMissingFileError = (error: unknown): boolean =>
  typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT'
