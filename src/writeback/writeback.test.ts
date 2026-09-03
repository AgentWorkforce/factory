import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import { linearByIdPath, linearCommentPath } from '../constants/linear'
import { slackReplyPath } from '../constants/slack'
import { AppGithubWriteback, createFactory, GhCliGithubWriteback, isAllowedFactoryGithubDraft, linearCommentName, MountGithubRead, MountLinearWriteback, MountSlackWriteback } from '../index'
import type { GithubConnectionRead, GithubConnectionWrite, GithubWriteback, MountClient } from '../ports'
import type { LinearIssue } from '../types'
import { RelayfileGithubConnectionWrite } from '../mount/relayfile-github-connection-write'
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

  it('preserves a null title prefix for label-only writeback scope', async () => {
    const title = 'Label-only garden issue'
    const labelOnlyIssue = structuredClone(issue)
    labelOnlyIssue.title = title
    labelOnlyIssue.labels = ['garden-ready']
    labelOnlyIssue.raw = wrappedIssueRecord({
      title,
      labels: [{ name: 'garden-ready' }],
    })
    const mount = new FakeMountClient({
      [issuePath]: labelOnlyIssue.raw,
    })
    const linear = MountLinearWriteback(mount, {
      safety: {
        requireTitlePrefix: null,
        requireLabel: 'garden-ready',
        requireTeamKey: 'AR',
      },
    })

    await expect(linear.setState(labelOnlyIssue, 'implementing-state')).resolves.toEqual({ claimToken: '1' })
  })

  it('writes a full writable issue record with only stateId changed and verifies read-back', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.setState(issue, 'implementing-state')).resolves.toEqual({ claimToken: '1' })

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

  it('reads the current mounted Linear state instead of the dispatch snapshot', async () => {
    const mount = new FakeMountClient({
      [issuePath]: wrappedIssueRecord({ stateId: 'human-review-state' }),
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.getIssueStateId(issue)).resolves.toBe('human-review-state')
    expect(issue.stateId).toBe('ready-state')
  })

  it('reads a name-only current state from the canonical record behind a sparse primary alias', async () => {
    const currentIssue: LinearIssue = {
      ...issue,
      stateId: 'human-review-state',
      state: { name: 'In Human Review' },
    }
    const mount = new FakeMountClient({
      [issuePath]: { payload: { stateId: 'ready-state' } },
      [linearByIdPath(issueKey)]: wrappedIssueRecord({
        stateId: undefined,
        state: { name: 'In Human Review' },
      }),
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.getIssueStateId(currentIssue)).resolves.toBe('human-review-state')
  })

  it('refuses a name-only canonical state that disagrees with the issue projection', async () => {
    // MUST-NOT-FIRE (#346 review, CodeRabbit): the name match IS the proof.
    // A canonical record whose state name has moved on cannot lend its
    // authority to the id the caller's state catalog resolved for a different
    // name, so this must fail closed rather than reuse it.
    const currentIssue: LinearIssue = {
      ...issue,
      stateId: 'human-review-state',
      state: { name: 'In Human Review' },
    }
    const mount = new FakeMountClient({
      [issuePath]: { payload: { stateId: 'ready-state' } },
      [linearByIdPath(issueKey)]: wrappedIssueRecord({
        stateId: undefined,
        state: { name: 'Done' },
      }),
    })

    await expect(MountLinearWriteback(mount).getIssueStateId(currentIssue)).resolves.toBeUndefined()
  })

  it('fails closed when only a sparse Linear state alias is readable', async () => {
    const mount = new FakeMountClient({
      [issuePath]: { payload: { stateId: 'human-review-state' } },
    })

    await expect(MountLinearWriteback(mount).getIssueStateId(issue)).resolves.toBeUndefined()
  })

  it('conditionally restores a Linear state at the exact mounted revision', async () => {
    const mount = new FakeMountClient()
    mount.files.set(issuePath, { content: wrappedIssueRecord({ stateId: 'implementing-state' }), revision: '7' })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('applied')
    expect(mount.writes).toEqual([expect.objectContaining({
      path: issuePath,
      content: expect.objectContaining({ stateId: 'ready-state' }),
    })])
  })

  it('preserves a newer Linear state when the conditional rollback revision loses', async () => {
    class RacingMountClient extends FakeMountClient {
      override async writeFile(
        path: string,
        content: unknown,
        opts?: { guarded?: boolean; baseRevision?: string },
      ): Promise<void> {
        if (opts?.baseRevision !== undefined) {
          this.files.set(path, {
            content: wrappedIssueRecord({ stateId: 'human-review-state' }),
            revision: String(Number(opts.baseRevision) + 1),
          })
        }
        await super.writeFile(path, content, opts)
      }
    }
    const mount = new RacingMountClient()
    mount.files.set(issuePath, { content: wrappedIssueRecord({ stateId: 'implementing-state' }), revision: '7' })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('superseded')
    expect(mount.writes).toEqual([])
    expect((mount.files.get(issuePath)?.content as { payload: { stateId: string } }).payload.stateId)
      .toBe('human-review-state')
  })

  it('recognizes a statusCode-shaped Linear revision conflict', async () => {
    class StatusCodeConflictMountClient extends FakeMountClient {
      override async writeFile(
        path: string,
        content: unknown,
        opts?: { guarded?: boolean; baseRevision?: string },
      ): Promise<void> {
        if (opts?.baseRevision !== undefined) {
          this.files.set(path, {
            content: wrappedIssueRecord({ stateId: 'human-review-state' }),
            revision: String(Number(opts.baseRevision) + 1),
          })
          throw Object.assign(new Error(`Revision conflict for ${path}`), { statusCode: 409 })
        }
        await super.writeFile(path, content, opts)
      }
    }
    const mount = new StatusCodeConflictMountClient()
    mount.files.set(issuePath, { content: wrappedIssueRecord({ stateId: 'implementing-state' }), revision: '7' })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('superseded')
    expect(mount.writes).toEqual([])
  })

  it('preserves current Linear fields when conditionally restoring the state', async () => {
    const mount = new FakeMountClient()
    mount.files.set(issuePath, {
      content: wrappedIssueRecord({
        stateId: 'implementing-state',
        description: 'A newer operator-authored description',
        priority: 1,
      }),
      revision: '7',
    })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('applied')
    expect(mount.writes.at(-1)?.content).toEqual(expect.objectContaining({
      stateId: 'ready-state',
      description: 'A newer operator-authored description',
      priority: 1,
    }))
  })

  it('fails closed when the exact Linear revision is only a sparse state projection', async () => {
    const mount = new FakeMountClient()
    mount.files.set(issuePath, {
      content: { payload: { stateId: 'implementing-state' } },
      revision: '7',
    })
    // Seed the scope-bearing canonical record through the issue object, while
    // keeping the mounted compare-and-set target deliberately sparse.
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('unproven')
    expect(mount.writes).toEqual([])
  })

  it('fails closed for an identical newer Linear state whose revision is not the claim token', async () => {
    const mount = new FakeMountClient()
    mount.files.set(issuePath, { content: wrappedIssueRecord({ stateId: 'implementing-state' }), revision: '8' })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('unproven')
    expect(mount.writes).toEqual([])
  })

  it('fails closed when an unrelated Linear edit wins the rollback CAS', async () => {
    class UnrelatedEditRacingMountClient extends FakeMountClient {
      override async writeFile(
        path: string,
        content: unknown,
        opts?: { guarded?: boolean; baseRevision?: string },
      ): Promise<void> {
        if (opts?.baseRevision !== undefined) {
          this.files.set(path, {
            content: wrappedIssueRecord({
              stateId: 'implementing-state',
              description: 'A concurrent description edit',
            }),
            revision: String(Number(opts.baseRevision) + 1),
          })
        }
        await super.writeFile(path, content, opts)
      }
    }
    const mount = new UnrelatedEditRacingMountClient()
    mount.files.set(issuePath, { content: wrappedIssueRecord({ stateId: 'implementing-state' }), revision: '7' })
    const linear = MountLinearWriteback(mount)

    await expect(linear.compareAndSetState?.(issue, 'implementing-state', '7', 'ready-state'))
      .resolves.toBe('unproven')
    expect(mount.writes).toEqual([])
    expect((mount.files.get(issuePath)?.content as { payload: { description: string } }).payload.description)
      .toBe('A concurrent description edit')
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

  it('waits through delayed Linear read-back propagation', async () => {
    class EventuallyConsistentMountClient extends FakeMountClient {
      staleReadsRemaining = new Map<string, number>()

      override async writeFile(path: string, content: unknown): Promise<void> {
        await super.writeFile(path, content)
        this.staleReadsRemaining.set(path, 3)
      }

      override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
        const remaining = this.staleReadsRemaining.get(path) ?? 0
        if (remaining > 0) {
          this.staleReadsRemaining.set(path, remaining - 1)
          return { content: { stateId: 'old-state' } }
        }
        return super.readFile(path)
      }
    }

    const mount = new EventuallyConsistentMountClient({
      [issuePath]: wrappedIssueRecord(),
    })
    const warn = () => {}
    const linear = MountLinearWriteback(mount, {
      logger: { warn },
      readbackConfirmAttempts: 5,
      readbackConfirmDelayMs: 1,
    })

    await expect(linear.setState(issue, 'implementing-state')).resolves.toBeUndefined()
    expect(await linear.verify(issue, { stateId: 'implementing-state' })).toBe(true)
  })

  it('throws (no faked success) when the read-back never confirms the write landed', async () => {
    // A getOp ack can be faked-success by a busy/wedged mount; if the read-back
    // never reflects the write, it did NOT land — the writeback must fail loudly
    // rather than leave the caller believing it advanced.
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
    const linear = MountLinearWriteback(mount, {
      logger: { warn: () => {} },
      readbackConfirmAttempts: 3,
      readbackConfirmDelayMs: 1,
    })

    await expect(linear.setState(issue, 'implementing-state')).rejects.toThrow(/read-back never confirmed it landed/u)
    expect(issue.stateId).toBe('ready-state')
    expect((issue.raw.payload as Record<string, unknown>).stateId).toBe('ready-state')
    await expect(linear.postComment(issue, 'Agent dispatched after stale mirror')).rejects.toThrow(/read-back never confirmed it landed/u)
    await expect(linear.createIssue({
      id: 'uuid-stale-create',
      identifier: 'AR-STALE',
      title: '[factory-e2e] synthetic issue with stale mirror',
      team: { key: 'AR', name: 'Agent Relay' },
      stateId: 'ready-state',
    })).rejects.toThrow(/read-back never confirmed it landed/u)
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
      .resolves.toEqual({ claimToken: '1' })
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

  it('uses the cloud operation external id when the mirrored root has not reconciled yet', async () => {
    const backing = new FakeMountClient()
    const cloudMount: MountClient = {
      writebackTransport: 'relayfile-cloud',
      readFile: (path) => backing.readFile(path),
      writeFile: (path, content, opts) => backing.writeFile(path, content, opts),
      deleteFile: (path) => backing.deleteFile(path),
      listTree: (prefix) => backing.listTree(prefix),
      subscribe: (globs, onChange, opts) => backing.subscribe(globs, onChange, opts),
      getEvents: (opts) => backing.getEvents(opts),
      confirmWrite: (path, opts) => backing.confirmWrite(path, opts),
      getConfirmedWriteExternalId: async () => '1780751612.176219',
      ensureSubRoot: (prefix, opts) => backing.ensureSubRoot(prefix, opts),
    }
    const slack = MountSlackWriteback(cloudMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
    })

    const root = await slack.postThread({
      channel: 'C0FACTORY__factory-e2e',
      text: 'Factory update',
    })
    await slack.reply(root.threadId, 'Factory reply')

    expect(root.threadId).toBe('1780751612.176219')
    expect(backing.writes[1]?.content).toMatchObject({ thread_ts: '1780751612.176219' })
  })

  it('fails closed when an acked cloud root has no provider thread timestamp', async () => {
    const backing = new FakeMountClient()
    const cloudMount: MountClient = {
      writebackTransport: 'relayfile-cloud',
      readFile: (path) => backing.readFile(path),
      writeFile: (path, content, opts) => backing.writeFile(path, content, opts),
      deleteFile: (path) => backing.deleteFile(path),
      listTree: (prefix) => backing.listTree(prefix),
      subscribe: (globs, onChange, opts) => backing.subscribe(globs, onChange, opts),
      getEvents: (opts) => backing.getEvents(opts),
      confirmWrite: (path, opts) => backing.confirmWrite(path, opts),
      ensureSubRoot: (prefix, opts) => backing.ensureSubRoot(prefix, opts),
    }
    const slack = MountSlackWriteback(cloudMount, {
      channel: 'C0FACTORY__factory-e2e',
      channelDir: 'C0FACTORY__factory-e2e',
    })

    await expect(slack.postThread({
      channel: 'C0FACTORY__factory-e2e',
      text: 'Factory update',
    })).rejects.toThrow(/without a provider thread timestamp/u)
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
          filesChanged: [{ path: 'src/writeback/github.ts' }],
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
      filesChanged: ['src/writeback/github.ts'],
    })
  })
})

describe('AppGithubWriteback', () => {
  const appIssue: LinearIssue = {
    ...issue,
    uuid: 'github-221',
    key: '221',
    title: 'GitHub-native app writeback',
    stateId: '',
    labels: ['factory', 'bug', 'factory:in-progress'],
    path: '/github/repos/AgentWorkforce/factory/issues/by-id/221.json',
    raw: {
      payload: {
        source: {
          provider: 'github',
          id: 'github-221',
          owner: 'AgentWorkforce',
          repo: 'factory',
          number: 221,
          url: 'https://github.com/AgentWorkforce/factory/issues/221',
        },
      },
    },
  }

  it('delegates PRs and lifecycle writes to the app connection and fails closed without a reader', async () => {
    const publishPullRequest: GithubConnectionWrite['publishPullRequest'] = vi.fn(async (input) => ({
      repo: input.repo,
      number: 322,
      url: 'https://github.com/AgentWorkforce/factory/pull/322',
      headRef: input.headRef ?? input.expectedHeadRef!,
      author: 'app',
    }))
    const postIssueComment = vi.fn(async () => undefined)
    const updateIssue = vi.fn(async () => undefined)
    const connection: GithubConnectionWrite = {
      publishPullRequest,
      closePullRequest: async () => undefined,
      postIssueComment,
      updateIssue,
    }
    const app = new AppGithubWriteback(connection)

    await expect(app.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/221-agentworkforce-factory',
      baseRef: 'main',
      title: '221: app writeback',
      body: 'Fixes #221',
    })).resolves.toMatchObject({ number: 322, author: 'app' })
    await app.postComment(appIssue, 'Factory dispatch for 221')
    await expect(app.setStatus(appIssue, 'human-review')).resolves.toBe('acknowledged')
    await expect(app.setStatus(appIssue, 'ready')).resolves.toBe('acknowledged')
    await expect(app.closeIssue(appIssue, 'Factory observed the linked PR merge.')).resolves.toBe('acknowledged')

    expect(postIssueComment).toHaveBeenNthCalledWith(1, {
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Factory dispatch for 221',
      author: 'app',
    })
    // Relayfile's GitHub adapter routes no label resource, so a status
    // transition has no per-label writer to reach for: `GithubConnectionWrite`
    // no longer declares one (#431).
    expect(postIssueComment).toHaveBeenNthCalledWith(2, {
      repo: 'AgentWorkforce/factory',
      number: 221,
      body: 'Factory observed the linked PR merge.',
      author: 'app',
    })
    // Two status transitions plus the close, all on the routed issue PATCH.
    expect(updateIssue.mock.calls).toEqual([
      // human-review: the obsolete in-progress label goes, the rest survive.
      [{
        repo: 'AgentWorkforce/factory',
        number: 221,
        labels: ['factory', 'bug', 'factory:human-review'],
        author: 'app',
      }],
      // ready: both Factory status labels go. The dispatched projection still
      // reads `factory:in-progress`, so this is computed from that set.
      [{
        repo: 'AgentWorkforce/factory',
        number: 221,
        labels: ['factory', 'bug'],
        author: 'app',
      }],
      [{
        repo: 'AgentWorkforce/factory',
        number: 221,
        state: 'closed',
        author: 'app',
      }],
    ])
    const writeback: GithubWriteback = app
    expect(writeback.getIssueAuthor).toBeUndefined()
    await expect(writeback.getIssueStatus?.(appIssue)).resolves.toBeUndefined()
    expect(writeback.hasCommentMarker).toBeUndefined()
  })

  it('refuses selection when the connected writer lacks issue lifecycle capabilities', () => {
    expect(() => new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('unexpected publish') },
      closePullRequest: async () => undefined,
    })).toThrow('requires connected comment and issue-update capabilities')
  })

  it('fails closed on the status claim: a replace PATCH is not proof of authorship', async () => {
    // The old per-label path could in principle report `applied`. The routed
    // issue PATCH cannot: it replaces the label set, so an identical
    // concurrent transition is indistinguishable from ours. Keep the receipt
    // fail-closed rather than let a caller infer claim ownership from it.
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    })

    await expect(app.setStatus(appIssue, 'in-progress')).resolves.toBe('acknowledged')
  })

  it('computes the status label set from the connected App projection, not the dispatched one', async () => {
    // A replace PATCH built from a stale set silently drops labels added
    // since. The connected read is the freshest authority this surface has.
    const updateIssue = vi.fn(async () => undefined)
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
      getIssue: async () => ({
        outcome: 'found' as const,
        issue: {
          content: {
            // `triage` landed after the dispatched projection was read, and
            // the casing here is the provider's, not ours.
            labels: [{ name: 'Factory' }, { name: 'triage' }, { name: 'factory:in-progress' }],
          },
        },
      }),
    })

    await expect(app.setStatus(appIssue, 'human-review')).resolves.toBe('acknowledged')
    expect(updateIssue).toHaveBeenCalledWith({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['Factory', 'triage', 'factory:human-review'],
      author: 'app',
    })
  })

  it('writes nothing when the issue already carries exactly the wanted label set', async () => {
    const updateIssue = vi.fn(async () => undefined)
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
    })

    // appIssue.labels is ['factory', 'bug', 'factory:in-progress'].
    await expect(app.setStatus(appIssue, 'in-progress')).resolves.toBe('acknowledged')
    expect(updateIssue).not.toHaveBeenCalled()
  })

  it('falls back to the dispatched labels when a found projection carries none', async () => {
    // A `found` projection with no usable labels is an incomplete read, not a
    // bare issue: nothing reaches setStatus without the safety opt-in that made
    // it dispatchable. Trusting the empty set would compute the whole replace
    // PATCH from nothing — dropping `factory` (which the mount guard then
    // rejects, stalling the claim) and clobbering every other label with it.
    for (const content of [{}, { labels: [] }, { labels: [{ name: '  ' }] }]) {
      const updateIssue = vi.fn(async () => undefined)
      const app = new AppGithubWriteback({
        publishPullRequest: async () => { throw new Error('not used') },
        closePullRequest: async () => undefined,
        postIssueComment: async () => undefined,
        updateIssue,
        getIssue: async () => ({ outcome: 'found' as const, issue: { content } }),
      })

      await expect(app.setStatus(appIssue, 'human-review')).resolves.toBe('acknowledged')
      expect(updateIssue).toHaveBeenCalledWith({
        repo: 'AgentWorkforce/factory',
        number: 221,
        // The dispatched set, transitioned — not the bare ['factory:human-review'].
        labels: ['factory', 'bug', 'factory:human-review'],
        author: 'app',
      })
    }
  })

  it('keeps a non-empty connected projection authoritative over the dispatched one', async () => {
    // The must-not-fire direction of the fallback above: a projection that does
    // answer still wins, including when it contradicts the dispatched snapshot
    // by having dropped a label. Only the empty read falls back.
    const updateIssue = vi.fn(async () => undefined)
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
      // `bug` was removed on GitHub after dispatch; the connected read sees it.
      getIssue: async () => ({
        outcome: 'found' as const,
        issue: { content: { labels: [{ name: 'factory' }, { name: 'factory:in-progress' }] } },
      }),
    })

    await expect(app.setStatus(appIssue, 'human-review')).resolves.toBe('acknowledged')
    expect(updateIssue).toHaveBeenCalledWith({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['factory', 'factory:human-review'],
      author: 'app',
    })
  })

  it('falls back to the dispatched projection when the connected read cannot answer', async () => {
    const updateIssue = vi.fn(async () => undefined)
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
      getIssue: async () => { throw new Error('connected read unavailable') },
    })

    await expect(app.setStatus(appIssue, 'ready')).resolves.toBe('acknowledged')
    expect(updateIssue).toHaveBeenCalledWith({
      repo: 'AgentWorkforce/factory',
      number: 221,
      labels: ['factory', 'bug'],
      author: 'app',
    })
  })

  it('authors only writeback paths the Relayfile GitHub adapter routes', async () => {
    // The regression this pins: `setStatus` used to author
    // `/github/repos/{o}/{r}/labels/<draft>.json` and
    // `/github/repos/{o}/{r}/issues/{n}/labels/<draft>.json`. The adapter
    // routes neither, so it rejected the draft before any request reached
    // GitHub, the dispatch claim failed, and the lifecycle never reached
    // `running` — starving the whole batch at batchSize 1.
    //
    // These patterns are transcribed from the adapter's own route table
    // (relayfile-adapters `packages/github/src/resources.ts`), which factory
    // does not depend on. A path that matches none of them is unroutable.
    const adapterRoutes = [
      /^\/github\/repos\/[^/]+\/[^/]+\/issues(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/issues\/[^/]+\/comments(?:\/[^/]+(?:\.json|\/meta\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/reviews(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pull-requests(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/refs(?:\/[^/]+(?:\.json)?)?$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/close\.json$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/merge\.json$/u,
      /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/review-comments\/[^/]+\/replies(?:\/[^/]+(?:\.json)?)?$/u,
    ]

    const mount = new FakeMountClient()
    const app = new AppGithubWriteback(new RelayfileGithubConnectionWrite({ mount }))

    await app.setStatus(appIssue, 'human-review')
    await app.setStatus(appIssue, 'ready')
    await app.postComment(appIssue, 'Factory dispatch for 221')

    const paths = mount.writes.map((write) => write.path)
    expect(paths.length).toBeGreaterThan(0)
    const unroutable = paths.filter((path) => !adapterRoutes.some((route) => route.test(path)))
    expect(unroutable).toEqual([])
  })

  it('prefers the connected App issue reader over an unauthenticated fallback', async () => {
    const connectedGetIssue = vi.fn(async () => ({
      outcome: 'found' as const,
      issue: {
        repo: 'AgentWorkforce/factory',
        number: 221,
        path: appIssue.path,
        content: { payload: { labels: [{ name: 'factory:human-review' }] } },
      },
    }))
    const fallbackGetIssue = vi.fn(async () => ({
      outcome: 'indeterminate' as const,
      reason: 'repository is private',
    }))
    const app = new AppGithubWriteback({
      getIssue: connectedGetIssue,
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    }, { getIssue: fallbackGetIssue })

    await expect(app.getIssueStatus(appIssue)).resolves.toBe('human-review')
    expect(connectedGetIssue).toHaveBeenCalledWith('AgentWorkforce/factory', 221)
    expect(fallbackGetIssue).not.toHaveBeenCalled()
  })

  it('falls back to the direct issue reader after an indeterminate connected projection', async () => {
    const connectedGetIssue = vi.fn(async () => ({
      outcome: 'indeterminate' as const,
      reason: 'connected projection is still migrating',
    }))
    const fallbackGetIssue = vi.fn(async () => ({
      outcome: 'found' as const,
      issue: {
        repo: 'AgentWorkforce/factory',
        number: 221,
        path: appIssue.path,
        content: { payload: { labels: [{ name: 'factory:human-review' }] } },
      },
    }))
    const app = new AppGithubWriteback({
      getIssue: connectedGetIssue,
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    }, { getIssue: fallbackGetIssue })

    await expect(app.getIssueStatus(appIssue)).resolves.toBe('human-review')
    expect(connectedGetIssue).toHaveBeenCalledWith('AgentWorkforce/factory', 221)
    expect(fallbackGetIssue).toHaveBeenCalledWith('AgentWorkforce/factory', 221)
  })

  it('requires a connected non-in-progress projection to postdate an ambiguous claim', async () => {
    let content: unknown = {
      payload: {
        updated_at: '2026-08-24T05:00:00.000Z',
        labels: [{ name: 'factory' }],
      },
    }
    const connectedGetIssue = vi.fn(async () => ({
      outcome: 'found' as const,
      issue: { repo: 'PrivateOrg/private-repo', number: 221, path: appIssue.path, content },
    }))
    const fallbackGetIssue = vi.fn(async () => ({
      outcome: 'indeterminate' as const,
      reason: 'repository is private',
    }))
    const app = new AppGithubWriteback({
      getIssue: connectedGetIssue,
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    }, { getIssue: fallbackGetIssue })
    const privateIssue: LinearIssue = {
      ...appIssue,
      path: '/github/repos/PrivateOrg/private-repo/issues/by-id/221.json',
      raw: {
        payload: {
          source: {
            provider: 'github',
            id: 'github-221',
            owner: 'PrivateOrg',
            repo: 'private-repo',
            number: 221,
            url: 'https://github.com/PrivateOrg/private-repo/issues/221',
          },
        },
      },
    }
    const opts = { requireFresh: true, freshAfterMs: Date.parse('2026-08-24T05:30:00.000Z') }

    await expect(app.getIssueStatus(privateIssue, opts)).resolves.toBeUndefined()
    expect(fallbackGetIssue).toHaveBeenCalledTimes(1)

    content = {
      payload: {
        updated_at: '2026-08-24T06:00:00.000Z',
        labels: [{ name: 'factory:human-review' }],
      },
    }
    await expect(app.getIssueStatus(privateIssue, opts)).resolves.toBe('human-review')
    expect(fallbackGetIssue).toHaveBeenCalledTimes(1)
  })

  it('refuses a ready projection whose only evidence is the generic issue timestamp', async () => {
    // MUST-NOT-FIRE (#346 review, codex): `updated_at` moves for an unrelated
    // description or assignee edit, so a `ready` projection newer than the
    // local claim-start instant is NOT proof the claim mutation has landed —
    // it is equally what "the claim write has not reached this projection yet"
    // looks like. Releasing on it strands the issue with no lifecycle once the
    // real `factory:in-progress` projection arrives.
    const content: unknown = {
      payload: {
        updated_at: '2026-08-24T06:00:00.000Z',
        labels: [{ name: 'factory' }],
      },
    }
    const connectedGetIssue = vi.fn(async () => ({
      outcome: 'found' as const,
      issue: { repo: 'PrivateOrg/private-repo', number: 221, path: appIssue.path, content },
    }))
    const fallbackGetIssue = vi.fn(async () => ({
      outcome: 'indeterminate' as const,
      reason: 'repository is private',
    }))
    const app = new AppGithubWriteback({
      getIssue: connectedGetIssue,
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    }, { getIssue: fallbackGetIssue })
    const privateIssue: LinearIssue = {
      ...appIssue,
      path: '/github/repos/PrivateOrg/private-repo/issues/by-id/221.json',
      raw: {
        payload: {
          source: {
            provider: 'github',
            id: 'github-221',
            owner: 'PrivateOrg',
            repo: 'private-repo',
            number: 221,
            url: 'https://github.com/PrivateOrg/private-repo/issues/221',
          },
        },
      },
    }

    await expect(app.getIssueStatus(privateIssue, {
      requireFresh: true,
      freshAfterMs: Date.parse('2026-08-24T05:30:00.000Z'),
    })).resolves.toBeUndefined()
    expect(fallbackGetIssue).toHaveBeenCalledTimes(1)
  })

  it('accepts a ready projection newer than one observed carrying the claim', async () => {
    // MUST-FIRE: once this adapter has seen the projection actually carrying
    // the claim, a strictly newer projection without it is causally after the
    // claim mutation, so supersession is proven and the block may clear.
    let content: unknown = {
      payload: {
        updated_at: '2026-08-24T05:45:00.000Z',
        labels: [{ name: 'factory' }, { name: 'factory:in-progress' }],
      },
    }
    const connectedGetIssue = vi.fn(async () => ({
      outcome: 'found' as const,
      issue: { repo: 'PrivateOrg/private-repo', number: 221, path: appIssue.path, content },
    }))
    const fallbackGetIssue = vi.fn(async () => ({
      outcome: 'indeterminate' as const,
      reason: 'repository is private',
    }))
    const app = new AppGithubWriteback({
      getIssue: connectedGetIssue,
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async () => undefined,
    }, { getIssue: fallbackGetIssue })
    const privateIssue: LinearIssue = {
      ...appIssue,
      path: '/github/repos/PrivateOrg/private-repo/issues/by-id/221.json',
      raw: {
        payload: {
          source: {
            provider: 'github',
            id: 'github-221',
            owner: 'PrivateOrg',
            repo: 'private-repo',
            number: 221,
            url: 'https://github.com/PrivateOrg/private-repo/issues/221',
          },
        },
      },
    }
    const opts = { requireFresh: true, freshAfterMs: Date.parse('2026-08-24T05:30:00.000Z') }

    await expect(app.getIssueStatus(privateIssue, opts)).resolves.toBe('in-progress')
    content = {
      payload: {
        updated_at: '2026-08-24T06:00:00.000Z',
        labels: [{ name: 'factory' }],
      },
    }
    await expect(app.getIssueStatus(privateIssue, opts)).resolves.toBe('ready')
    expect(fallbackGetIssue).not.toHaveBeenCalled()
  })

  it('refuses to roll back an App claim from acknowledgement alone', async () => {
    // Watched on `updateIssue` rather than a per-label writer: the adapter
    // routes no label resource, so that is the only way a rollback could
    // write anything at all (#431).
    const updateIssue = vi.fn(async () => undefined)
    const getIssue = vi.fn<GithubConnectionRead['getIssue']>()
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
    }, { getIssue })

    await expect(app.rollbackStatusClaim(appIssue, 'in-progress', 'unavailable-token'))
      .resolves.toBe('unproven')
    expect(getIssue).not.toHaveBeenCalled()
    expect(updateIssue).not.toHaveBeenCalled()
  })

  it('fails closed instead of read-then-removing an App-backed in-progress claim', async () => {
    const updateIssue = vi.fn(async () => undefined)
    const connection: GithubConnectionWrite = {
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
    }
    const read: GithubConnectionRead = {
      getIssue: async () => ({
        outcome: 'found',
        issue: {
          repo: 'AgentWorkforce/factory',
          number: 221,
          path: appIssue.path,
          content: { payload: { labels: [{ name: 'factory' }, { name: 'factory:in-progress' }] } },
        },
      }),
    }
    const app = new AppGithubWriteback(connection, read)

    await expect(app.rollbackStatusClaim(appIssue, 'in-progress', 'provider-event-1'))
      .resolves.toBe('unproven')
    await expect(app.getIssueStatus(appIssue)).resolves.toBe('in-progress')
    expect(updateIssue).not.toHaveBeenCalled()
  })

  it('preserves a newer App-backed human-review status during claim rollback', async () => {
    const updateIssue = vi.fn(async () => undefined)
    const connection: GithubConnectionWrite = {
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue,
    }
    const read: GithubConnectionRead = {
      getIssue: async () => ({
        outcome: 'found',
        issue: {
          repo: 'AgentWorkforce/factory',
          number: 221,
          path: appIssue.path,
          content: {
            payload: {
              labels: [{ name: 'factory:in-progress' }, { name: 'factory:human-review' }],
            },
          },
        },
      }),
    }
    const app = new AppGithubWriteback(connection, read)

    await expect(app.rollbackStatusClaim(appIssue, 'in-progress', 'provider-event-1'))
      .resolves.toBe('unproven')
    await expect(app.getIssueStatus(appIssue)).resolves.toBe('human-review')
    expect(updateIssue).not.toHaveBeenCalled()
  })
})

describe('GhCliGithubWriteback', () => {
  const githubIssue: LinearIssue = {
    ...issue,
    uuid: 'github-48',
    key: '48',
    title: 'GitHub-native factory work',
    description: 'Implement the GitHub issue directly.',
    stateId: '',
    labels: ['factory'],
    path: '/github/repos/AgentWorkforce/factory/issues/by-id/48.json',
    raw: {
      payload: {
        source: {
          provider: 'github',
          id: 'github-48',
          owner: 'AgentWorkforce',
          repo: 'factory',
          number: 48,
          url: 'https://github.com/AgentWorkforce/factory/issues/48',
        },
      },
    },
  }
  const authenticatedActorCall = ['api', 'user', '--jq', '.login']
  const issueLabelEventsCall = [
    'api',
    '--paginate',
    'repos/AgentWorkforce/factory/issues/48/events',
    '--jq',
    '.[] | select(.event == "labeled" or .event == "unlabeled") | [.id, .event, .label.name, .actor.login] | @tsv',
  ]
  const issueStateEventsCall = [
    'api',
    '--paginate',
    'repos/AgentWorkforce/factory/issues/48/events',
    '--jq',
    '.[] | select(.event == "closed" or .event == "reopened") | [.id, .event, .actor.login] | @tsv',
  ]

  it('pushes a local branch and returns the gh-authenticated PR author', async () => {
    const ghCalls: string[][] = []
    const gitCalls: string[][] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        ghCalls.push(args)
        if (args[1] === 'create') {
          return { stdout: 'https://github.com/AgentWorkforce/factory/pull/124\n' }
        }
        return {
          stdout: JSON.stringify({
            number: 124,
            url: 'https://github.com/AgentWorkforce/factory/pull/124',
            headRefName: 'factory/124-configurable-pr-author',
            headRefOid: 'commit-124',
            author: { login: 'operator-user' },
          }),
        }
      },
      gitRunner: async (args) => {
        gitCalls.push(args)
        if (args.includes('symbolic-ref')) return { stdout: 'factory/124-configurable-pr-author\n' }
        if (args.includes('rev-parse')) return { stdout: 'commit-124\n' }
        return { stdout: '' }
      },
    })

    await expect(github.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: '124: configurable PR author',
      body: 'Factory issue 124',
    })).resolves.toEqual({
      repo: 'AgentWorkforce/factory',
      number: 124,
      url: 'https://github.com/AgentWorkforce/factory/pull/124',
      headRef: 'factory/124-configurable-pr-author',
      headSha: 'commit-124',
      author: 'operator-user',
    })
    expect(gitCalls).toEqual([
      ['-C', '/work/factory', 'symbolic-ref', '--short', 'HEAD'],
      ['-C', '/work/factory', 'rev-parse', 'HEAD'],
      ['-C', '/work/factory', 'push', 'origin', 'HEAD:refs/heads/factory/124-configurable-pr-author'],
    ])
    expect(ghCalls).toEqual([
      [
        'pr', 'create', '--repo', 'AgentWorkforce/factory',
        '--head', 'factory/124-configurable-pr-author', '--base', 'main',
        '--title', '124: configurable PR author', '--body', 'Factory issue 124',
      ],
      [
        'pr', 'view', 'https://github.com/AgentWorkforce/factory/pull/124',
        '--repo', 'AgentWorkforce/factory', '--json',
        'number,url,headRefName,headRefOid,author',
      ],
    ])
  })

  it('refuses a mismatched local head before push or PR creation', async () => {
    const ghCalls: string[][] = []
    const gitCalls: string[][] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        ghCalls.push(args)
        return { stdout: '' }
      },
      gitRunner: async (args) => {
        gitCalls.push(args)
        return { stdout: 'factory/3022-chief-org-live-population\n' }
      },
    })

    await expect(github.publishPullRequest({
      repo: 'AgentWorkforce/cloud',
      clonePath: '/work/cloud',
      expectedHeadRef: 'factory/3021-agentworkforce-cloud-12345678',
      baseRef: 'main',
      title: '3021: repair deployment objective CI',
      body: 'Fixes #3021',
    })).rejects.toThrow(
      'Refusing to publish GitHub PR: expected head branch factory/3021-agentworkforce-cloud-12345678, found factory/3022-chief-org-live-population',
    )
    expect(gitCalls).toEqual([['-C', '/work/cloud', 'symbolic-ref', '--short', 'HEAD']])
    expect(ghCalls).toEqual([])
  })

  it('resolves the issue reporter from GitHub when the mounted payload omits it', async () => {
    const calls: string[][] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        return { stdout: JSON.stringify({ author: { login: 'issue-reporter' } }) }
      },
    })

    await expect(github.getIssueAuthor(githubIssue)).resolves.toBe('issue-reporter')
    expect(calls).toEqual([[
      'issue',
      'view',
      '48',
      '--repo',
      'AgentWorkforce/factory',
      '--json',
      'author',
    ]])
  })

  it('sets the first lifecycle status without removing an absent label, then transitions statuses', async () => {
    const calls: string[][] = []
    const labels = new Set<string>()
    const events: string[] = []
    let nextEventId = 1
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          const added = args[args.indexOf('--add-label') + 1]
          const removed = args[args.indexOf('--remove-label') + 1]
          if (args.includes('--add-label') && added && !labels.has(added)) {
            labels.add(added)
            events.push(`${nextEventId++}\tlabeled\t${added}\tfactory-bot`)
          }
          if (args.includes('--remove-label') && removed && labels.has(removed)) {
            labels.delete(removed)
            events.push(`${nextEventId++}\tunlabeled\t${removed}\tfactory-bot`)
          }
        }
        return { stdout: '' }
      },
    })

    await github.postComment(githubIssue, 'Factory dispatch for 48')
    await expect(github.setStatus(githubIssue, 'in-progress')).resolves.toBe('applied')
    await expect(github.setStatus(githubIssue, 'human-review')).resolves.toBe('applied')

    expect(calls).toEqual([
      ['issue', 'comment', '48', '--repo', 'AgentWorkforce/factory', '--body', 'Factory dispatch for 48'],
      ['label', 'create', 'factory:in-progress', '--repo', 'AgentWorkforce/factory', '--color', '1d76db', '--description', 'Factory agents are working on this issue.', '--force'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      authenticatedActorCall,
      issueLabelEventsCall,
      ['issue', 'edit', '48', '--repo', 'AgentWorkforce/factory', '--add-label', 'factory:in-progress'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      issueLabelEventsCall,
      ['label', 'create', 'factory:human-review', '--repo', 'AgentWorkforce/factory', '--color', 'fbca04', '--description', 'Factory work is ready for human review.', '--force'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      authenticatedActorCall,
      issueLabelEventsCall,
      ['issue', 'edit', '48', '--repo', 'AgentWorkforce/factory', '--add-label', 'factory:human-review', '--remove-label', 'factory:in-progress'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      issueLabelEventsCall,
    ])
  })

  it('clears stale lifecycle labels when returning an orphaned issue to ready', async () => {
    const calls: string[][] = []
    const labels = new Set(['factory-ready', 'factory:in-progress', 'factory:human-review'])
    const events: string[] = []
    let nextEventId = 1
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return {
            stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }),
          }
        }
        if (args[0] === 'issue' && args[1] === 'edit' && args.includes('--remove-label')) {
          args.forEach((arg, index) => {
            const removed = args[index + 1]!
            if (arg === '--remove-label' && labels.delete(removed)) {
              events.push(`${nextEventId++}\tunlabeled\t${removed}\tfactory-bot`)
            }
          })
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'ready')).resolves.toBe('applied')

    expect(calls).toEqual([
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      authenticatedActorCall,
      issueLabelEventsCall,
      [
        'issue',
        'edit',
        '48',
        '--repo',
        'AgentWorkforce/factory',
        '--remove-label',
        'factory:in-progress',
        '--remove-label',
        'factory:human-review',
      ],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'labels'],
      issueLabelEventsCall,
    ])
  })

  it('does not attribute a label add won by another actor between read and edit', async () => {
    const labels = new Set(['factory:in-progress'])
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          labels.delete('factory:in-progress')
          labels.add('factory:human-review')
          events.push('1\tunlabeled\tfactory:in-progress\tother-user')
          events.push('2\tlabeled\tfactory:human-review\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'human-review')).resolves.toBe('acknowledged')
  })

  it('does not attribute a label removal won by another actor between read and edit', async () => {
    const labels = new Set(['factory:human-review'])
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          labels.delete('factory:human-review')
          events.push('1\tunlabeled\tfactory:human-review\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'ready')).resolves.toBe('acknowledged')
  })

  it('does not attribute a park that another actor removes and recreates after the edit', async () => {
    const labels = new Set(['factory:in-progress'])
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          labels.delete('factory:in-progress')
          labels.add('factory:human-review')
          events.push('1\tlabeled\tfactory:human-review\tfactory-bot')
          events.push('2\tunlabeled\tfactory:in-progress\tfactory-bot')
          labels.delete('factory:human-review')
          labels.add('factory:human-review')
          events.push('3\tunlabeled\tfactory:human-review\tother-user')
          events.push('4\tlabeled\tfactory:human-review\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'human-review')).resolves.toBe('acknowledged')
  })

  it('returns the immutable defining event when another actor only removes a stale label', async () => {
    const labels = new Set(['factory:in-progress'])
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          labels.add('factory:human-review')
          events.push('1\tlabeled\tfactory:human-review\tfactory-bot')
          labels.delete('factory:in-progress')
          events.push('2\tunlabeled\tfactory:in-progress\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.claimStatus(githubIssue, 'human-review')).resolves.toEqual({
      result: 'applied',
      claimToken: '1',
    })
  })

  it('does not attribute ready when another actor recreates the final removal', async () => {
    const labels = new Set(['factory:human-review'])
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') {
          labels.delete('factory:human-review')
          events.push('1\tunlabeled\tfactory:human-review\tfactory-bot')
          labels.add('factory:human-review')
          labels.delete('factory:human-review')
          events.push('2\tlabeled\tfactory:human-review\tother-user')
          events.push('3\tunlabeled\tfactory:human-review\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'ready')).resolves.toBe('acknowledged')
  })

  it.each([
    { status: 'ready' as const, labels: [] },
    { status: 'in-progress' as const, labels: ['factory:in-progress'] },
  ])('does not issue a $status lifecycle edit when the state already matches', async ({ status, labels: initialLabels }) => {
    const calls: string[][] = []
    const labels = new Set(initialLabels)
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        return { stdout: '' }
      },
    })
    await expect(github.setStatus(githubIssue, status)).resolves.toBe('already-matched')

    expect(calls.some((args) => args[0] === 'issue' && args[1] === 'edit')).toBe(false)
  })

  it('does not claim a status transition for cleanup after human-review already won', async () => {
    const calls: string[][] = []
    const labels = new Set(['factory:in-progress', 'factory:human-review'])
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit' && args.includes('--remove-label')) {
          labels.delete('factory:in-progress')
        }
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'human-review')).resolves.toBe('already-matched')
    expect(calls.some((args) => args[0] === 'issue' && args[1] === 'edit')).toBe(true)
  })

  it('fails closed when GitHub cannot atomically qualify label removal by the claim event', async () => {
    const calls: string[][] = []
    const labels = new Set(['factory', 'factory:in-progress'])
    const events = ['claim-1\tlabeled\tfactory:in-progress\tfactory-bot']
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        return { stdout: '' }
      },
    })

    await expect(github.rollbackStatusClaim(githubIssue, 'in-progress', 'claim-1'))
      .resolves.toBe('unproven')
    expect(calls.some((args) => args[0] === 'issue' && args[1] === 'edit')).toBe(false)
    expect(labels).toEqual(new Set(['factory', 'factory:in-progress']))
  })

  it('preserves an identical newer GitHub claim with a different defining event', async () => {
    const calls: string[][] = []
    const labels = new Set(['factory:in-progress'])
    const events = [
      'claim-1\tlabeled\tfactory:in-progress\tfactory-bot',
      '2\tunlabeled\tfactory:in-progress\tother-user',
      'claim-2\tlabeled\tfactory:in-progress\tother-user',
    ]
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        return { stdout: '' }
      },
    })

    await expect(github.rollbackStatusClaim(githubIssue, 'in-progress', 'claim-1'))
      .resolves.toBe('superseded')
    expect(calls.some((args) => args[0] === 'issue' && args[1] === 'edit')).toBe(false)
  })

  it('does not roll back a GitHub claim after human review supersedes it', async () => {
    const calls: string[][] = []
    const labels = new Set(['factory:in-progress', 'factory:human-review'])
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [...labels].map((name) => ({ name })) }) }
        }
        return { stdout: '' }
      },
    })

    await expect(github.rollbackStatusClaim(githubIssue, 'in-progress', 'claim-1'))
      .resolves.toBe('superseded')
    expect(calls.some((args) => args[0] === 'issue' && args[1] === 'edit')).toBe(false)
  })

  it('rejects an acknowledged lifecycle edit when provider read-back never shows the label', async () => {
    let edits = 0
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ labels: [] }) }
        }
        if (args[0] === 'issue' && args[1] === 'edit') edits += 1
        return { stdout: '' }
      },
    })

    await expect(github.setStatus(githubIssue, 'in-progress')).rejects.toThrow(
      'GitHub writeback did not confirm factory:in-progress on AgentWorkforce/factory#48',
    )
    expect(edits).toBe(1)
  })

  it('treats provider human-review status as authoritative over a stale in-progress label', async () => {
    const github = new GhCliGithubWriteback({
      runner: async () => ({
        stdout: JSON.stringify({
          labels: [{ name: 'factory:in-progress' }, { name: 'factory:human-review' }],
        }),
      }),
    })

    await expect(github.getIssueStatus(githubIssue)).resolves.toBe('human-review')
  })

  it('comments and closes the GitHub issue after merge', async () => {
    const calls: string[][] = []
    let state = 'OPEN'
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'issue' && args[1] === 'view' && args.includes('state')) {
          return { stdout: JSON.stringify({ state }) }
        }
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'close') {
          state = 'CLOSED'
          events.push('1\tclosed\tfactory-bot')
        }
        return { stdout: '' }
      },
    })

    await expect(
      github.closeIssue(githubIssue, 'Factory observed the linked pull request merge.'),
    ).resolves.toBe('applied')

    expect(calls).toEqual([
      ['issue', 'comment', '48', '--repo', 'AgentWorkforce/factory', '--body', 'Factory observed the linked pull request merge.'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'state'],
      authenticatedActorCall,
      issueStateEventsCall,
      ['issue', 'close', '48', '--repo', 'AgentWorkforce/factory', '--reason', 'completed'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'state'],
      issueStateEventsCall,
    ])
  })

  it('does not attribute an issue close won by another actor between read and command', async () => {
    let state = 'OPEN'
    const events: string[] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[0] === 'issue' && args[1] === 'view' && args.includes('state')) {
          return { stdout: JSON.stringify({ state }) }
        }
        if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
        if (args[0] === 'api' && args[1] === '--paginate') return { stdout: events.join('\n') }
        if (args[0] === 'issue' && args[1] === 'close') {
          state = 'CLOSED'
          events.push('1\tclosed\tother-user')
        }
        return { stdout: '' }
      },
    })

    await expect(github.closeIssue(githubIssue, 'Factory completion.')).resolves.toBe('acknowledged')
  })

  it('reports an already-closed issue without issuing a close command', async () => {
    const calls: string[][] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        if (args[0] === 'issue' && args[1] === 'view') {
          return { stdout: JSON.stringify({ state: 'CLOSED' }) }
        }
        return { stdout: '' }
      },
    })

    await expect(github.closeIssue(githubIssue, 'Factory completion.')).resolves.toBe('already-matched')
    expect(calls).toEqual([
      ['issue', 'comment', '48', '--repo', 'AgentWorkforce/factory', '--body', 'Factory completion.'],
      ['issue', 'view', '48', '--repo', 'AgentWorkforce/factory', '--json', 'state'],
    ])
  })

  it('looks up an escalation marker from all provider comment pages', async () => {
    const calls: string[][] = []
    const github = new GhCliGithubWriteback({
      runner: async (args) => {
        calls.push(args)
        return { stdout: 'ordinary comment\n<!-- factory-escalation:factory-agent-question-abc123 -->\n' }
      },
    })

    await expect(github.hasCommentMarker(
      githubIssue,
      '<!-- factory-escalation:factory-agent-question-abc123 -->',
    )).resolves.toBe(true)
    expect(calls).toEqual([[
      'api',
      '--paginate',
      'repos/AgentWorkforce/factory/issues/48/comments',
      '--jq',
      '.[].body',
    ]])
  })

  it('refuses writeback when GitHub source identity is incomplete', async () => {
    const github = new GhCliGithubWriteback({ runner: async () => ({ stdout: '' }) })
    await expect(github.postComment({
      ...githubIssue,
      raw: { payload: { source: { provider: 'github', number: 48 } } },
    }, 'unsafe')).rejects.toThrow(/stable GitHub issue source/)
  })

  it('rejects a source URL whose issue number only shares a numeric prefix', async () => {
    const github = new GhCliGithubWriteback({ runner: async () => ({ stdout: '' }) })
    await expect(github.postComment({
      ...githubIssue,
      key: '4',
      raw: {
        payload: {
          source: {
            provider: 'github',
            id: 'github-4',
            owner: 'AgentWorkforce',
            repo: 'factory',
            number: 4,
            url: 'https://github.com/AgentWorkforce/factory/issues/45',
          },
        },
      },
    }, 'unsafe')).rejects.toThrow(/source URL does not match AgentWorkforce\/factory#4/)
  })
})

describe('postAttestationGrant session ref forwarding', () => {
  const savedEnv: Record<string, string | undefined> = {}
  const attestEnvKeys = ['RELAYAUTH_URL', 'RELAY_ATTEST_API_KEY', 'RELAY_ATTEST_AGENT_ID', 'RELAY_ATTEST_SESSION_ID']

  function setAttestation(overrides: Record<string, string | undefined> = {}) {
    const defaults: Record<string, string> = {
      RELAYAUTH_URL: 'https://relayauth.test',
      RELAY_ATTEST_API_KEY: 'ra_test_key',
      RELAY_ATTEST_AGENT_ID: 'agent_factory_test',
    }
    for (const key of attestEnvKeys) {
      savedEnv[key] = process.env[key]
      const value = key in overrides ? overrides[key] : defaults[key]
      if (value === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = value
      }
    }
  }

  afterEach(() => {
    for (const key of attestEnvKeys) {
      if (savedEnv[key] === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = savedEnv[key]
      }
    }
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  function makeGithubWriteback() {
    return new GhCliGithubWriteback({
      runner: async (args) => {
        if (args[1] === 'create') {
          return { stdout: 'https://github.com/AgentWorkforce/factory/pull/1\n' }
        }
        return {
          stdout: JSON.stringify({
            number: 1,
            url: 'https://github.com/AgentWorkforce/factory/pull/1',
            headRefName: 'feat/attest-test',
            headRefOid: 'sha-attest',
            author: { login: 'bot-user' },
          }),
        }
      },
      gitRunner: async (args) => {
        if (args.includes('symbolic-ref')) return { stdout: 'feat/attest-test\n' }
        if (args.includes('rev-parse')) return { stdout: 'sha-attest\n' }
        return { stdout: '' }
      },
    })
  }

  it('forwards sessionRef to the grants body when RELAY_ATTEST_SESSION_ID is set', async () => {
    setAttestation({ RELAY_ATTEST_SESSION_ID: 'ses_test_123' })
    const requests: Array<{ url: string; body: unknown }> = []
    vi.stubGlobal('fetch', async (url: string, init?: RequestInit) => {
      requests.push({ url, body: JSON.parse(String(init?.body ?? 'null')) })
      return new Response(JSON.stringify({ jti: 'att_1', finalizeKey: 'fk', notAfter: 'n' }), { status: 201 })
    })

    await makeGithubWriteback().publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Test PR',
      body: 'body',
    })

    expect(requests).toHaveLength(1)
    expect(requests[0]?.url).toBe('https://relayauth.test/v1/attestations/grants')
    expect(requests[0]?.body).toMatchObject({
      agentId: 'agent_factory_test',
      repo: 'AgentWorkforce/factory',
      late: true,
      sessionRef: 'ses_test_123',
    })
  })

  it('omits sessionRef from the grants body when RELAY_ATTEST_SESSION_ID is absent', async () => {
    setAttestation({ RELAY_ATTEST_SESSION_ID: undefined })
    const requests: Array<{ url: string; body: unknown }> = []
    vi.stubGlobal('fetch', async (url: string, init?: RequestInit) => {
      requests.push({ url, body: JSON.parse(String(init?.body ?? 'null')) })
      return new Response(JSON.stringify({ jti: 'att_2', finalizeKey: 'fk2', notAfter: 'n2' }), { status: 201 })
    })

    await makeGithubWriteback().publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Test PR',
      body: 'body',
    })

    expect(requests).toHaveLength(1)
    expect(requests[0]?.body).toMatchObject({
      agentId: 'agent_factory_test',
      repo: 'AgentWorkforce/factory',
      late: true,
    })
    expect((requests[0]?.body as Record<string, unknown>)['sessionRef']).toBeUndefined()
  })

  it('silently skips the grants call when RELAYAUTH_URL is absent', async () => {
    setAttestation({ RELAYAUTH_URL: undefined, RELAY_ATTEST_SESSION_ID: 'ses_skip' })
    const fetchCalls: string[] = []
    vi.stubGlobal('fetch', async (url: string) => {
      fetchCalls.push(String(url))
      return new Response('{}', { status: 201 })
    })

    await makeGithubWriteback().publishPullRequest({
      repo: 'AgentWorkforce/factory',
      clonePath: '/work/factory',
      baseRef: 'main',
      title: 'Test PR',
      body: 'body',
    })

    expect(fetchCalls).toHaveLength(0)
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

describe('isAllowedFactoryGithubDraft complete-label-set PATCH', () => {
  // This guard had no coverage from the writeback side, which is how a status
  // claim that the adapter routes but the guard rejects reached CI green.
  const issuePath = '/github/repos/AgentWorkforce/pear/issues/by-id/221.json'
  const draftPath = '/github/repos/AgentWorkforce/pear/issues/221.json'
  const issueFile = {
    provider: 'github',
    objectType: 'issue',
    objectId: 'pear-221',
    payload: {
      number: 221,
      title: '[factory] complete-label-set claim',
      body: 'body',
      state: 'open',
      labels: [{ name: 'factory' }, { name: 'bug' }],
      url: 'https://github.com/AgentWorkforce/pear/issues/221',
      repository: { name: 'pear', owner: { login: 'AgentWorkforce' } },
    },
  }
  const guardConfig = (requireLabel: string) => FactoryConfigSchema.parse({
    workspaceId: 'rw_test',
    issueSource: 'github',
    repos: { byLabel: { factory: 'AgentWorkforce/pear' } },
    safety: { requireLabel, requireTitlePrefix: '[factory]' },
    slack: { channel: 'C0AD7UU0J1G__proj-cloud' },
  })
  const allows = async (content: unknown, requireLabel = 'factory'): Promise<boolean> =>
    await isAllowedFactoryGithubDraft(
      draftPath,
      content,
      { guarded: true },
      new FakeMountClient({ [issuePath]: issueFile }),
      guardConfig(requireLabel),
    )

  it('admits the exact payload setStatus authors', async () => {
    await expect(allows({ labels: ['factory', 'bug', 'factory:in-progress'] })).resolves.toBe(true)
    await expect(allows({ labels: ['factory', 'bug'] })).resolves.toBe(true)
  })

  it('refuses a set that drops the safety opt-in, including the empty set', async () => {
    await expect(allows({ labels: [] })).resolves.toBe(false)
    await expect(allows({ labels: ['bug', 'factory:in-progress'] })).resolves.toBe(false)
  })

  it('refuses two contradictory Factory claims in one write, in any casing', async () => {
    await expect(allows({
      labels: ['factory', 'factory:in-progress', 'Factory:Human-Review'],
    })).resolves.toBe(false)
  })

  it('still refuses shapes outside the status claim', async () => {
    await expect(allows({ labels: ['factory'], title: 'rewritten' })).resolves.toBe(false)
    await expect(allows({ labels: 'factory' })).resolves.toBe(false)
    await expect(allows({ labels: ['factory', '   '] })).resolves.toBe(false)
    await expect(allows({ state: 'open' })).resolves.toBe(false)
  })

  it('keeps admitting the close write and the opt-in survival check together', async () => {
    await expect(allows({ state: 'closed' })).resolves.toBe(true)
    await expect(allows({ labels: ['factory'], state: 'closed' })).resolves.toBe(true)
  })

  it('refuses the empty set even when the opt-in is exempt', async () => {
    // The exemption below must not reopen the hole the survival check closed:
    // stripping every label off an in-scope open issue is never a transition.
    await expect(allows({ labels: [] }, 'factory:in-progress')).resolves.toBe(false)
    await expect(allows({ labels: [] }, 'factory:human-review')).resolves.toBe(false)
  })

  it('admits the payload setStatus authors from an empty connected projection', async () => {
    // Closes the loop the two halves leave open: the guard is what turns an
    // empty read into a stalled claim, so the red check has to run the real
    // construction path into the real guard rather than assert on an array.
    const pearIssue: LinearIssue = {
      ...issue,
      uuid: 'github-pear-221',
      key: '221',
      title: '[factory] complete-label-set claim',
      stateId: '',
      labels: ['factory', 'bug'],
      path: issuePath,
      raw: {
        payload: {
          source: {
            provider: 'github',
            id: 'github-pear-221',
            owner: 'AgentWorkforce',
            repo: 'pear',
            number: 221,
            url: 'https://github.com/AgentWorkforce/pear/issues/221',
          },
        },
      },
    }
    const authored: unknown[] = []
    const app = new AppGithubWriteback({
      publishPullRequest: async () => { throw new Error('not used') },
      closePullRequest: async () => undefined,
      postIssueComment: async () => undefined,
      updateIssue: async ({ labels }) => { authored.push({ labels }) },
      // `found`, but with nothing this reader can extract a label from.
      getIssue: async () => ({ outcome: 'found' as const, issue: { content: {} } }),
    })

    await app.setStatus(pearIssue, 'in-progress')

    expect(authored).toEqual([{ labels: ['factory', 'bug', 'factory:in-progress'] }])
    await expect(allows(authored[0])).resolves.toBe(true)
    // And the set the unfixed read would have authored is exactly what the
    // guard refuses — the stall this fallback removes.
    await expect(allows({ labels: ['factory:in-progress'] })).resolves.toBe(false)
  })

  it('exempts a lifecycle opt-in from the survival check', async () => {
    // A self-contradictory configuration, but the survival rule must not add a
    // second way for it to fail: a status transition is supposed to change a
    // lifecycle label, so requiring it to survive would reject every claim.
    await expect(allows({ labels: ['factory', 'bug'] }, 'factory:in-progress')).resolves.toBe(true)
  })
})
