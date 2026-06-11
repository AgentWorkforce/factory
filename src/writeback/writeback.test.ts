import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import { linearCommentPath } from '../constants/linear'
import { slackReplyPath } from '../constants/slack'
import { createFactory, linearCommentName, MountGithubRead, MountLinearWriteback, MountSlackWriteback } from '../index'
import type { LinearIssue } from '../types'
import { FakeFleetClient, FakeMountClient } from '../testing'

const issuePath = '/linear/issues/AR-99__04ef067e-35b6-4ec4-81e7-66acc1f2e31f.json'

const issue: LinearIssue = {
  uuid: '04ef067e-35b6-4ec4-81e7-66acc1f2e31f',
  key: 'AR-99',
  title: '[factory-e2e] Reviewer merge on green',
  description: 'Dispatch reviewer after green checks',
  stateId: 'ready-state',
  state: { name: 'Ready for Agent' },
  labels: [],
  path: issuePath,
  raw: {
    payload: {
      url: 'https://linear.app/agent-relay/issue/AR-99/reviewer-merge-on-green',
    },
  },
}

const wrappedIssueRecord = (overrides: Record<string, unknown> = {}) => ({
  provider: 'linear',
  objectType: 'issue',
  objectId: issue.uuid,
  deleted: false,
  connectionId: 'linear-connection',
  payload: {
    id: issue.uuid,
    identifier: issue.key,
    title: issue.title,
    description: issue.description,
    stateId: issue.stateId,
    labels: undefined,
    labelIds: ['label-id-not-used-by-guard'],
    team: { key: 'AR', name: 'Agent Relay' },
    url: 'https://linear.app/agent-relay/issue/AR-99/reviewer-merge-on-green',
    ...overrides,
  },
})

describe('MountLinearWriteback', () => {
  it('writes only stateId to the canonical AR-prefixed issue file and verifies read-back', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    await linear.setState(issue, 'implementing-state')

    expect(mount.writes).toEqual([
      { path: issuePath, content: { stateId: 'implementing-state' } },
    ])
    expect(await linear.verify(issue, { stateId: 'implementing-state' })).toBe(true)
  })

  it('returns false on stale state read-back mismatches', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    expect(await linear.verify(issue, { stateId: 'implementing-state' })).toBe(false)
  })

  it('writes full comment payload under the canonical issue file parent', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)
    const body = 'Agent dispatched to factory-sdk/w4-writeback'
    const commentName = linearCommentName(issue, body)
    const commentPath = linearCommentPath(issuePath, commentName)

    await linear.postComment(issue, body)

    expect(mount.writes).toEqual([
      {
        path: commentPath,
        content: {
          body,
          botActor: '',
          isArtificialAgentSessionRoot: false,
          issue: {
            id: issue.uuid,
            identifier: issue.key,
            title: issue.title,
            url: 'https://linear.app/agent-relay/issue/AR-99/reviewer-merge-on-green',
          },
          issue_id: issue.uuid,
          issueId: issue.uuid,
        },
      },
    ])
    expect(commentPath).toContain('/linear/issues/AR-99__04ef067e-35b6-4ec4-81e7-66acc1f2e31f.json/comments/')
    expect(commentPath).toMatch(/\/comments\/[^/]+\.json$/)
    expect(commentPath.endsWith('.json.json')).toBe(false)
    expect(await linear.verify(issue, { commentName })).toBe(true)
  })

  it('surfaces non-acked state writebacks even when local read-back matches', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    mount.setConfirmWrite(issuePath, 'timeout')
    const linear = MountLinearWriteback(mount)

    await expect(linear.setState(issue, 'implementing-state')).rejects.toThrow(/not acked/)
    expect(await linear.verify(issue, { stateId: 'implementing-state' })).toBe(true)
  })

  it('surfaces stale writebacks instead of swallowing read-back failures', async () => {
    class StaleMountClient extends FakeMountClient {
      override async writeFile(path: string, content: unknown): Promise<void> {
        await super.writeFile(path, content)
        this.files.set(path, { content: { stateId: 'old-state' } })
      }
    }

    const mount = new StaleMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.setState(issue, 'implementing-state')).rejects.toThrow(/read-back verification failed/)
  })

  it('refuses setState and postComment on an issue without factory-e2e before writing', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ title: 'Real production work' }),
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.setState(issue, 'implementing-state')).rejects.toThrow(/title must start with \[factory-e2e\] boundary/)
    await expect(linear.postComment(issue, 'dispatch comment')).rejects.toThrow(/title must start with \[factory-e2e\] boundary/)
    expect(mount.writes).toEqual([])
  })

  it('fails closed when issue title or guard record is unreadable', async () => {
    const missingTitle = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ title: undefined }),
    })
    await expect(MountLinearWriteback(missingTitle).setState(issue, 'implementing-state'))
      .rejects.toThrow(/title must start with \[factory-e2e\] boundary/)

    const unreadable = new FakeMountClient()
    await expect(MountLinearWriteback(unreadable).postComment(issue, 'dispatch comment'))
      .rejects.toThrow(/unable to read guard fields/)
    expect(missingTitle.writes).toEqual([])
    expect(unreadable.writes).toEqual([])
  })

  it.each([
    ['no-space suffix', '[factory-e2e]x Factory work', 'body'],
    ['suffix word', '[factory-e2e]xyz Factory work', 'body'],
    ['case mismatch', '[Factory-E2E] Factory work', 'body'],
    ['not prefix', 'x[factory-e2e] Factory work', 'body'],
    ['description marker only', 'Factory work', '[factory-e2e] marker in body'],
  ])('refuses near-miss titles: %s', async (_name, title, description) => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ title, description }),
    })

    await expect(MountLinearWriteback(mount).setState(issue, 'implementing-state'))
      .rejects.toThrow(/title must start with \[factory-e2e\] boundary/)
    expect(mount.writes).toEqual([])
  })

  it('allows writeback when the title is marked factory-e2e and team is absent', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ team: undefined }),
    })

    await expect(MountLinearWriteback(mount).setState(issue, 'implementing-state'))
      .resolves.toBeUndefined()
    expect(mount.writes).toEqual([
      { path: issuePath, content: { stateId: 'implementing-state' } },
    ])
  })

  it('refuses writeback when the issue is not scoped to the AR team', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ team: { key: 'OTHER', name: 'Other Team' } }),
    })

    await expect(MountLinearWriteback(mount).postComment(issue, 'dispatch comment'))
      .rejects.toThrow(/team key must be AR/)
    expect(mount.writes).toEqual([])
  })

  it('refuses createIssue when the create payload lacks factory-e2e', async () => {
    const mount = new FakeMountClient()

    await expect(MountLinearWriteback(mount).createIssue({
      id: 'uuid-new',
      identifier: 'AR-NEW',
      title: 'marker missing from title',
      team: { key: 'AR' },
    })).rejects.toThrow(/title must start with \[factory-e2e\] boundary/)
    expect(mount.writes).toEqual([])
  })

  it('refuses createIssue when the create payload is outside the AR team', async () => {
    const mount = new FakeMountClient()

    await expect(MountLinearWriteback(mount).createIssue({
      id: 'uuid-new',
      identifier: 'AR-NEW',
      title: '[factory-e2e] synthetic issue',
      team: { key: 'OTHER' },
    })).rejects.toThrow(/team key must be AR/)
    expect(mount.writes).toEqual([])
  })

  it('creates an issue only when the payload carries the factory-e2e title prefix and AR team', async () => {
    const mount = new FakeMountClient()
    const linear = MountLinearWriteback(mount)

    await expect(linear.createIssue({
      id: 'uuid-new',
      identifier: 'AR-NEW',
      title: '[factory-e2e] synthetic issue',
      team: { key: 'AR', name: 'Agent Relay' },
    })).resolves.toEqual({
      path: '/linear/issues/AR-NEW__uuid-new.json',
    })

    expect(mount.writes).toEqual([{
      path: '/linear/issues/AR-NEW__uuid-new.json',
      content: {
        id: 'uuid-new',
        identifier: 'AR-NEW',
        title: '[factory-e2e] synthetic issue',
        team: { key: 'AR', name: 'Agent Relay' },
      },
    }])
  })

  it('creates an issue when the title is marked factory-e2e and team is absent', async () => {
    const mount = new FakeMountClient()

    await expect(MountLinearWriteback(mount).createIssue({
      id: 'uuid-no-team',
      identifier: 'AR-NO-TEAM',
      title: '[factory-e2e] synthetic issue with sparse sync',
    })).resolves.toEqual({
      path: '/linear/issues/AR-NO-TEAM__uuid-no-team.json',
    })
    expect(mount.writes).toEqual([{
      path: '/linear/issues/AR-NO-TEAM__uuid-no-team.json',
      content: {
        id: 'uuid-no-team',
        identifier: 'AR-NO-TEAM',
        title: '[factory-e2e] synthetic issue with sparse sync',
      },
    }])
  })
})

describe('MountSlackWriteback', () => {
  it('exposes only thread root and reply methods', () => {
    const slack = MountSlackWriteback(new FakeMountClient(), {
      channelDir: 'C0AD7UU0J1G__proj-cloud',
    })

    expect(Object.keys(slack).sort()).toEqual(['postThread', 'reply'])
  })

  it('writes root thread messages and threaded replies with exact mount payloads', async () => {
    const mount = new FakeMountClient()
    const slack = MountSlackWriteback(mount, {
      channelDir: 'C0AD7UU0J1G__proj-cloud',
      clientIdPrefix: 'factory-w4',
    })

    const root = await slack.postThread({
      channel: 'C0AD7UU0J1G__proj-cloud',
      text: 'What shipped\nPR link\nStatus\nDropped fourth line',
    })
    await slack.reply('1780751612.176219', 'Full PR links:\nhttps://github.example/pr/1')

    expect(mount.writes[0]?.path).toMatch(/^\/slack\/channels\/C0AD7UU0J1G__proj-cloud\/messages\/factory-w4-c0ad7uu0j1g-[a-z0-9]+\.json$/)
    expect(mount.writes[0]?.content).toEqual({
      channelId: 'C0AD7UU0J1G',
      text: 'What shipped\nPR link\nStatus',
    })
    expect(root.threadId).toMatch(/^factory-w4-c0ad7uu0j1g-/)

    const replyWrite = mount.writes[1]
    expect(replyWrite?.path).toBe(
      slackReplyPath(
        'C0AD7UU0J1G__proj-cloud',
        '1780751612_176219',
        replyWrite?.path.split('/').at(-1)?.replace(/\.json$/, '') ?? '',
      ),
    )
    expect(replyWrite?.content).toEqual({
      channelId: 'C0AD7UU0J1G',
      thread_ts: '1780751612.176219',
      text: 'Full PR links:\nhttps://github.example/pr/1',
    })
  })

  it('surfaces non-acked thread writes even when local read-back succeeds', async () => {
    class TimeoutAfterWriteMountClient extends FakeMountClient {
      override async confirmWrite(
        path: string,
        opts?: { timeoutMs?: number },
      ): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
        void path
        void opts
        return 'timeout'
      }
    }

    const timeoutMount = new TimeoutAfterWriteMountClient()
    const timeoutSlack = MountSlackWriteback(timeoutMount, {
      channelDir: 'C0AD7UU0J1G__proj-cloud',
      clientIdPrefix: 'factory-w4',
    })

    await expect(timeoutSlack.postThread({
      channel: 'C0AD7UU0J1G__proj-cloud',
      text: 'Another shipped update',
    })).rejects.toThrow(/not acked/)
    expect(timeoutMount.writes).toHaveLength(1)
    await expect(timeoutMount.readFile(timeoutMount.writes[0]?.path ?? '')).resolves.toBeTruthy()
  })
})

describe('MountGithubRead', () => {
  it('reads PR summaries from owner__repo by-id records via payload wrapper', async () => {
    const mount = new FakeMountClient({
      '/github/repos/AgentWorkforce__cloud/pulls/by-id/2086.json': {
        provider: 'github',
        objectType: 'pull_request',
        objectId: '2086',
        payload: {
          number: 2086,
          title: 'Add direct-proxy writeback fast path',
          state: 'open',
          url: 'https://github.com/AgentWorkforce/cloud/pull/2086',
          headRef: { name: 'factory-sdk/w4' },
          baseRef: { name: 'main' },
          author: { login: 'factory-bot' },
          filesChanged: [{ path: 'packages/factory-sdk/src/writeback/github.ts' }],
        },
      },
    })
    const github = MountGithubRead(mount)

    await expect(github.getPr('AgentWorkforce/cloud', 2086)).resolves.toEqual({
      repo: 'AgentWorkforce/cloud',
      number: 2086,
      title: 'Add direct-proxy writeback fast path',
      url: 'https://github.com/AgentWorkforce/cloud/pull/2086',
      state: 'open',
      headRef: 'factory-sdk/w4',
      baseRef: 'main',
      author: 'factory-bot',
      filesChanged: ['packages/factory-sdk/src/writeback/github.ts'],
    })
  })
})

describe('createFactory writeback defaults', () => {
  it('constructs default Mount-backed writeback ports when not overridden', () => {
    const config = FactoryConfigSchema.parse({
      workspaceId: 'rw_test',
      repos: { byLabel: { factory: 'AgentWorkforce/pear' } },
      slack: { channel: 'C0AD7UU0J1G__proj-cloud' },
    })

    expect(() => createFactory(config, {
      mount: new FakeMountClient(),
      fleet: new FakeFleetClient(),
    })).not.toThrow()
  })
})
