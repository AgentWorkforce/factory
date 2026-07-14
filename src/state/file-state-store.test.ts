import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

import type { GithubIssueCommentWatchState } from '../ports/state'
import { FileStateStore } from './file-state-store'

describe('FileStateStore', () => {
  it('restores GitHub escalation watches in a fresh store instance', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-'))
    try {
      const watchStatePath = join(root, 'github-watches.json')
      const watch: GithubIssueCommentWatchState = {
        issue: { uuid: 'uuid-1', key: 'AR-1', path: '/linear/issues/AR-1__uuid-1.json' },
        source: {
          owner: 'AgentWorkforce',
          repo: 'factory',
          number: 55,
          url: 'https://github.com/AgentWorkforce/factory/issues/55',
        },
        pending: [{
          correlationId: 'factory-agent-question-test',
          kind: 'agent-question',
          authorizedAuthor: 'issue-reporter',
        }],
        sinceCommentId: '100',
        lastSeenCommentId: '101',
        processedCommentIds: ['101'],
      }
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
})
