import { beforeEach, describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import { linearCommentPath } from '../constants/linear'
import { slackReplyPath } from '../constants/slack'
import { createFactory, linearCommentName, MountGithubRead, MountLinearWriteback, MountSlackWriteback } from '../index'
import type { MountClient } from '../ports'
import type { LinearIssue } from '../types'
import { FakeFleetClient, FakeMountClient } from '../testing'

const issuePath = '/linear/issues/AR-99__04ef067e-35b6-4ec4-81e7-66acc1f2e31f.json'
const issueUuid = '04ef067e-35b6-4ec4-81e7-66acc1f2e31f'
const issueKey = 'AR-99'
const issueTitle = '[factory-e2e] Reviewer merge on green'
const issueDescription = 'Dispatch reviewer after green checks'

const issue: LinearIssue = {
  uuid: issueUuid,
  key: issueKey,
  title: issueTitle,
  description: issueDescription,
  stateId: 'ready-state',
  state: { name: 'Ready for Agent' },
  labels: [],
  path: issuePath,
  raw: {
    payload: {
      id: issueUuid,
      identifier: issueKey,
      title: issueTitle,
      description: issueDescription,
      stateId: 'ready-state',
      labelIds: ['label-id-not-used-by-guard'],
      team: { key: 'AR', name: 'Agent Relay' },
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
    id: issueUuid,
    identifier: issueKey,
    title: issueTitle,
    description: issueDescription,
    stateId: issue.stateId,
    labels: undefined,
    labelIds: ['label-id-not-used-by-guard'],
    team: { key: 'AR', name: 'Agent Relay' },
    url: 'https://linear.app/agent-relay/issue/AR-99/reviewer-merge-on-green',
    ...overrides,
  },
})

describe('MountLinearWriteback', () => {
  beforeEach(() => {
    issue.stateId = 'ready-state'
    const payload = issue.raw.payload as Record<string, unknown>
    payload.stateId = 'ready-state'
  })

  it('writes a full writable issue record with only stateId changed and verifies read-back', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    await linear.setState(issue, 'implementing-state')

    expect(mount.writes).toEqual([
      {
        path: issuePath,
        content: {
          title: issueTitle,
          stateId: 'implementing-state',
          description: issueDescription,
          labelIds: ['label-id-not-used-by-guard'],
        },
      },
    ])
    expect(await linear.verify(issue, { stateId: 'implementing-state' })).toBe(true)
  })

  it('builds setState from in-memory writable fields instead of the live mount read', async () => {
    const richIssue: LinearIssue = {
      ...issue,
      raw: {
        payload: {
          ...(issue.raw.payload as Record<string, unknown>),
          team: { key: 'AR', id: 'team-ar', name: 'Agent Relay' },
          priority: 2,
          assigneeId: 'assignee-1',
          parentId: 'parent-1',
          projectId: 'project-1',
          estimate: 5,
        },
      },
    }
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({
        description: 'stale live description must not be used for RMW',
        labelIds: ['stale-label'],
      }),
    })
    const linear = MountLinearWriteback(mount)

    await linear.setState(richIssue, 'implementing-state')

    expect(mount.writes).toEqual([
      {
        path: issuePath,
        content: {
          title: issueTitle,
          teamId: 'team-ar',
          stateId: 'implementing-state',
          description: issueDescription,
          priority: 2,
          assigneeId: 'assignee-1',
          labelIds: ['label-id-not-used-by-guard'],
          parentId: 'parent-1',
          projectId: 'project-1',
          estimate: 5,
        },
      },
    ])
  })

  it('keeps back-to-back setState writes title-bearing through a partial draft window', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    await linear.setState(issue, 'implementing-state')
    mount.files.set(issuePath, { content: { stateId: 'implementing-state' } })
    await linear.setState(issue, 'done-state')

    expect(mount.writes).toEqual([
      {
        path: issuePath,
        content: {
          title: issueTitle,
          stateId: 'implementing-state',
          description: issueDescription,
          labelIds: ['label-id-not-used-by-guard'],
        },
      },
      {
        path: issuePath,
        content: {
          title: issueTitle,
          stateId: 'done-state',
          description: issueDescription,
          labelIds: ['label-id-not-used-by-guard'],
        },
      },
    ])
    expect(issue.stateId).toBe('done-state')
    expect((issue.raw.payload as Record<string, unknown>).stateId).toBe('done-state')
  })

  it('allows a comment immediately after setState when the live issue is a partial draft', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)
    const body = 'Agent dispatched after state update'

    await linear.setState(issue, 'implementing-state')
    mount.files.set(issuePath, { content: { stateId: 'implementing-state' } })
    await linear.postComment(issue, body)

    expect(mount.writes.at(-1)).toEqual({
      path: linearCommentPath(issuePath, linearCommentName(issue, body)),
      content: {
        body,
        issueId: issue.uuid,
      },
    })
  })

  it('fails closed instead of writing when the in-memory issue is not title-bearing', async () => {
    const partialIssue: LinearIssue = {
      ...issue,
      raw: { payload: { stateId: 'ready-state' } },
    }
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })

    await expect(MountLinearWriteback(mount).setState(partialIssue, 'implementing-state'))
      .rejects.toThrow(/missing title-bearing canonical issue payload/)
    expect(mount.writes).toEqual([])
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
          issueId: issue.uuid,
        },
      },
    ])
    expect(commentPath).toContain('/comments/AR-99__factory-')
    expect(commentPath.startsWith(`${issuePath.replace(/\.json$/u, '')}/comments/`)).toBe(true)
    expect(commentPath).toMatch(/^\/linear\/issues\/[^/]+\/comments\/[^/]+\.json$/)
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
    await expect(linear.verify(issue, { stateId: 'implementing-state' })).rejects.toThrow(/not acked/)
  })

  it('treats stale read-back as advisory after getOp ack for state, comment, and create writebacks', async () => {
    class StaleMountClient extends FakeMountClient {
      override async writeFile(path: string, content: unknown): Promise<void> {
        await super.writeFile(path, content)
        if (path.includes('/comments/')) {
          this.files.set(path, { content: { issueId: 'stale-issue-id' } })
        } else if (path.includes('/factory-create-')) {
          this.files.set(path, { content: { title: 'stale create mirror' } })
        } else {
          this.files.set(path, { content: { stateId: 'old-state' } })
        }
      }
    }

    const mount = new StaleMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const warnings: unknown[][] = []
    const linear = MountLinearWriteback(mount, {
      logger: {
        warn: (...args: unknown[]) => warnings.push(args),
      },
    })

    await expect(linear.setState(issue, 'implementing-state')).resolves.toBeUndefined()
    await expect(linear.postComment(issue, 'Agent dispatched after stale mirror')).resolves.toBeUndefined()
    await expect(linear.createIssue({
      id: 'uuid-stale-create',
      identifier: 'AR-STALE',
      title: '[factory-e2e] synthetic issue with stale mirror',
      team: { key: 'AR', name: 'Agent Relay' },
      stateId: 'ready-state',
    })).resolves.toEqual({ path: '/linear/issues/factory-create-uuid-stale-create.json' })

    expect(warnings).toHaveLength(3)
    expect(warnings.map((warning) => warning[0])).toEqual([
      `[factory-sdk] Linear writeback read-back verification failed for ${issuePath}; treating getOp ack as success`,
      expect.stringMatching(/^\[factory-sdk\] Linear writeback read-back verification failed for \/linear\/issues\/[^/]+\/comments\/AR-99__factory-/u),
      '[factory-sdk] Linear writeback read-back verification failed for /linear/issues/factory-create-uuid-stale-create.json; treating getOp ack as success',
    ])
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
      {
        path: issuePath,
        content: {
          title: issueTitle,
          stateId: 'implementing-state',
          description: issueDescription,
          labelIds: ['label-id-not-used-by-guard'],
        },
      },
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
      teamId: 'team-ar',
      team: { key: 'AR', name: 'Agent Relay' },
      stateId: 'ready-state',
      description: 'Create only with writable fields',
      url: 'https://linear.invalid/read-only',
    })).resolves.toEqual({
      path: '/linear/issues/factory-create-uuid-new.json',
    })

    expect(mount.writes).toEqual([{
      path: '/linear/issues/factory-create-uuid-new.json',
      content: {
        title: '[factory-e2e] synthetic issue',
        teamId: 'team-ar',
        stateId: 'ready-state',
        description: 'Create only with writable fields',
      },
    }])
  })

  it('creates an issue when the title is marked factory-e2e and team is absent', async () => {
    const mount = new FakeMountClient()

    await expect(MountLinearWriteback(mount).createIssue({
      id: 'uuid-no-team',
      identifier: 'AR-NO-TEAM',
      title: '[factory-e2e] synthetic issue with sparse sync',
      state: { id: 'read-only-state' },
    })).resolves.toEqual({
      path: '/linear/issues/factory-create-uuid-no-team.json',
    })
    expect(mount.writes).toEqual([{
      path: '/linear/issues/factory-create-uuid-no-team.json',
      content: {
        title: '[factory-e2e] synthetic issue with sparse sync',
      },
    }])
  })

  it('keys createIssue drafts by a non-provider synthetic id and never by an AR identifier', async () => {
    const mount = new FakeMountClient()

    await MountLinearWriteback(mount).createIssue({
      id: '8fc81e88-9b2e-4b23-8bb1-e1ebe03b963b',
      identifier: 'AR-CLAMPV2',
      title: '[factory-e2e] add clamp(n, min, max) util to factory-sdk',
      teamId: 'team-ar',
      team: { key: 'AR', name: 'Agent Relay' },
      stateId: 'ready-state',
      description: 'Create should let Linear assign the real issue key',
    })

    expect(mount.writes).toHaveLength(1)
    expect(mount.writes[0]?.path).toBe('/linear/issues/factory-create-8fc81e88-9b2e-4b23-8bb1-e1ebe03b963b.json')
    expect(mount.writes[0]?.path).not.toContain('AR-CLAMPV2')
    expect(mount.writes[0]?.path).not.toContain('__')
    expect(mount.writes[0]?.content).toEqual({
      title: '[factory-e2e] add clamp(n, min, max) util to factory-sdk',
      teamId: 'team-ar',
      stateId: 'ready-state',
      description: 'Create should let Linear assign the real issue key',
    })
  })

  it('refuses identifier-only createIssue drafts that look like provider issue keys', async () => {
    const mount = new FakeMountClient()

    await expect(MountLinearWriteback(mount).createIssue({
      identifier: 'AR-CLAMPV2',
      title: '[factory-e2e] add clamp(n, min, max) util to factory-sdk',
      team: { key: 'AR', name: 'Agent Relay' },
    })).rejects.toThrow(/non-provider id\/clientId/)
    expect(mount.writes).toEqual([])
  })
})

describe('MountSlackWriteback', () => {
  it('exposes only thread root and reply methods', () => {
    const slack = MountSlackWriteback(new FakeMountClient(), {
      channel: 'C0AD7UU0J1G',
      channelDir: 'C0AD7UU0J1G__proj-cloud',
    })

    expect(Object.keys(slack).sort()).toEqual(['postThread', 'reply'])
  })

  it('writes root thread messages and threaded replies with exact mount payloads', async () => {
    const mount = new FakeMountClient()
    const slack = MountSlackWriteback(mount, {
      channel: 'C0AD7UU0J1G',
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

  it('returns the real Slack parent ts from the acked thread root when available', async () => {
    class AckedSlackMountClient extends FakeMountClient {
      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        await super.writeFile(path, content, opts)
        this.files.set(path, {
          content: {
            provider: 'slack',
            objectType: 'message',
            payload: {
              channel: 'C0FACTORY',
              ts: '1780751612.176219',
              text: 'Factory update',
            },
          },
        })
      }
    }
    const mount = new AckedSlackMountClient()
    const slack = MountSlackWriteback(mount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
      clientIdPrefix: 'factory-e2e',
    })

    const root = await slack.postThread({
      channel: 'C0FACTORY__factory-e2e',
      text: 'Factory update',
    })
    await slack.reply(root.threadId, 'Factory reply')

    expect(root.threadId).toBe('1780751612.176219')
    expect(mount.writes[1]?.path).toContain('/slack/channels/C0FACTORY__factory-e2e/messages/1780751612_176219/replies/')
    expect(mount.writes[1]?.content).toMatchObject({
      channelId: 'C0FACTORY',
      thread_ts: '1780751612.176219',
      text: 'Factory reply',
    })
  })

  it('refuses Slack writes over a local mirror mount even if file writes appear acked', async () => {
    const writes: Array<{ path: string; content: unknown }> = []
    const localMirrorMount: MountClient = {
      async readFile() {
        return { content: {} }
      },
      async writeFile(path, content) {
        writes.push({ path, content })
      },
      async deleteFile() {
        throw new Error('local mirror delete is not used')
      },
      async listTree() {
        return []
      },
      subscribe() {
        return { unsubscribe: async () => undefined }
      },
      async getEvents() {
        return { events: [] }
      },
      async confirmWrite() {
        return 'acked'
      },
      async ensureSubRoot() {
        return 'ready'
      },
    }
    const slack = MountSlackWriteback(localMirrorMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
    })

    await expect(slack.reply('1780751612.176219', 'Factory reply'))
      .rejects.toThrow(/requires RelayfileCloudMountClient cloud writeback transport/)
    expect(writes).toEqual([])
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
      channel: 'C0AD7UU0J1G',
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

  it('refuses thread roots and replies outside the configured factory-e2e channel before writing', async () => {
    const postMount = new FakeMountClient()
    const postSlack = MountSlackWriteback(postMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
    })

    await expect(postSlack.postThread({
      channel: 'C0PROD__product-alerts',
      text: 'Wrong channel',
    })).rejects.toThrow(/target channel must match configured factory-e2e channel/)
    expect(postMount.writes).toEqual([])

    const channelDirBypassMount = new FakeMountClient()
    const channelDirBypassSlack = MountSlackWriteback(channelDirBypassMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0PROD__product-alerts',
    })

    await expect(channelDirBypassSlack.postThread({
      channel: 'C0FACTORY__factory-e2e',
      text: 'Wrong effective channel',
    })).rejects.toThrow(/target channel must match configured factory-e2e channel/)
    expect(channelDirBypassMount.writes).toEqual([])

    const replyMount = new FakeMountClient()
    const replySlack = MountSlackWriteback(replyMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0PROD__product-alerts',
    })

    await expect(replySlack.reply('1780751612.176219', 'Wrong channel reply'))
      .rejects.toThrow(/target channel must match configured factory-e2e channel/)
    expect(replyMount.writes).toEqual([])
  })

  it('allows thread roots and replies to the configured factory-e2e channel by name', async () => {
    const mount = new FakeMountClient()
    const slack = MountSlackWriteback(mount, {
      channel: 'factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
      clientIdPrefix: 'factory-e2e',
    })

    const root = await slack.postThread({
      channel: '#factory-e2e',
      text: 'Factory update',
    })
    await slack.reply(root.threadId, 'Factory reply')

    expect(mount.writes).toHaveLength(2)
    expect(mount.writes[0]?.path).toContain('/slack/channels/C0FACTORY__factory-e2e/messages/')
    expect(mount.writes[1]?.path).toContain(`/slack/channels/C0FACTORY__factory-e2e/messages/${root.threadId}/replies/`)
  })

  it('fails closed when the configured factory-e2e channel is unset', async () => {
    const mount = new FakeMountClient()
    const slack = MountSlackWriteback(mount, {
      channelDir: 'C0FACTORY__factory-e2e',
    })

    await expect(slack.postThread({
      channel: 'C0FACTORY__factory-e2e',
      text: 'Factory update',
    })).rejects.toThrow(/configured factory-e2e channel is required/)
    await expect(slack.reply('1780751612.176219', 'Factory reply'))
      .rejects.toThrow(/configured factory-e2e channel is required/)
    expect(mount.writes).toEqual([])
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

  it('installs a default draft predicate that rereads markerless Linear targets', async () => {
    class GuardedMountClient extends FakeMountClient {
      predicate?: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>

      setDefaultAllowedDraftPredicate(
        predicate: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>,
      ): void {
        this.predicate = predicate
      }

      override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
        if (path.startsWith('/linear/') && await this.predicate?.(path, content, opts) !== true) {
          throw new Error(`draft rejected for ${path}`)
        }
        await super.writeFile(path, content, opts)
      }
    }

    const config = FactoryConfigSchema.parse({
      workspaceId: 'rw_test',
      repos: { byLabel: { factory: 'AgentWorkforce/pear' } },
      slack: { channel: 'C0AD7UU0J1G__proj-cloud' },
    })
    const mount = new GuardedMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    createFactory(config, {
      mount,
      fleet: new FakeFleetClient(),
    })

    await expect(mount.writeFile(issuePath, { stateId: 'implementing-state' }, { guarded: true }))
      .resolves.toBeUndefined()

    const body = 'best effort markerless comment'
    await expect(mount.writeFile(
      linearCommentPath(issuePath, linearCommentName(issue, body)),
      { body, issueId: issue.uuid },
      { guarded: true },
    )).resolves.toBeUndefined()
  })
})
