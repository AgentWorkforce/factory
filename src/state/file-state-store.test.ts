import { mkdir, mkdtemp, readdir, rm, stat, utimes, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { describe, expect, it } from 'vitest'

import type { BabysitterSessionState, ConversationSessionState, DispatchLifecycle, GithubIssueCommentWatchState, WaitingClarification } from '../ports/state'
import { FileStateStore } from './file-state-store'
import { InMemoryStateStore } from './in-memory-state-store'

describe('FileStateStore', () => {
  it('persists and fences dispatch lifecycle ownership across processes and crash takeover', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-lifecycle-'))
    try {
      const watchStatePath = join(root, 'state.json')
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      const second = new FileStateStore({ batchSize: 2, watchStatePath })
      const key = 'AR-85:uuid-85:/linear/issues/AR-85.json'
      const seed = dispatchLifecycle(85)

      const initial = await first.claimDispatchLifecycle('workspace-1', key, seed, 'owner-a', 1_000, 5_000)
      expect(initial).toMatchObject({ acquired: true, created: true, lease: { owner: 'owner-a', epoch: 1 } })

      const duplicate = await second.claimDispatchLifecycle('workspace-1', key, seed, 'owner-b', 2_000, 5_000)
      expect(duplicate).toMatchObject({ acquired: false, created: false, lifecycle: { lease: { owner: 'owner-a', epoch: 1 } } })

      const takeover = await second.claimDispatchLifecycle('workspace-1', key, seed, 'owner-b', 6_001, 5_000)
      expect(takeover).toMatchObject({ acquired: true, created: false, lease: { owner: 'owner-b', epoch: 2 } })

      expect(await first.saveDispatchLifecycle('workspace-1', key, 'owner-a', 1, 6_002, {
        ...seed,
        phase: 'published',
      })).toBe(false)
      expect(await second.saveDispatchLifecycle('workspace-1', key, 'owner-b', 2, 6_002, {
        ...takeover.lifecycle,
        phase: 'published',
        pullRequest: {
          repo: 'AgentWorkforce/factory',
          number: 85,
          url: 'https://github.com/AgentWorkforce/factory/pull/85',
          headRef: 'factory/ar-85-agentworkforce-factory',
        },
      })).toBe(true)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.getDispatchLifecycle('workspace-1', key)).toMatchObject({
        phase: 'published',
        lease: { owner: 'owner-b', epoch: 2 },
        pullRequest: { number: 85 },
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('atomically adopts an existing GitHub lifecycle across Relayfile issue aliases', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-lifecycle-github-alias-'))
    try {
      const stores = [
        new FileStateStore({ batchSize: 2, watchStatePath: join(root, 'state.json') }),
        new InMemoryStateStore({ batchSize: 2 }),
      ]
      for (const [index, store] of stores.entries()) {
        const workspace = `workspace-${index}`
        const byId = githubDispatchLifecycle(146, '/github/repos/AgentWorkforce__factory/issues/by-id/146.json')
        const slugged = githubDispatchLifecycle(
          146,
          '/github/repos/AgentWorkforce/factory/issues/146__stand-up-test-infra/meta.json',
        )
        const byIdKey = `146:${byId.issue.uuid}:${byId.issue.path}`
        const sluggedKey = `146:${slugged.issue.uuid}:${slugged.issue.path}`

        const initial = await store.claimDispatchLifecycle(
          workspace, byIdKey, byId, 'owner-a', 1_000, 5_000,
        )
        const alias = await store.claimDispatchLifecycle(
          workspace, sluggedKey, slugged, 'owner-a', 1_001, 5_000,
        )

        expect(initial).toMatchObject({ key: byIdKey, acquired: true, created: true })
        expect(alias).toMatchObject({ key: byIdKey, acquired: true, created: false })
        expect(alias.lifecycle.issue.path).toBe(byId.issue.path)
        expect(await store.listDispatchLifecycles(workspace)).toHaveLength(1)

        expect(await store.saveDispatchLifecycle(workspace, byIdKey, 'owner-a', 1, 1_002, {
          ...alias.lifecycle,
          phase: 'complete',
        })).toBe(true)
        await expect(store.claimDispatchLifecycle(
          workspace, sluggedKey, slugged, 'owner-b', 1_003, 5_000,
        )).resolves.toMatchObject({ key: byIdKey, acquired: false, created: false, lifecycle: { phase: 'complete' } })
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('restores and clears babysitter ownership plus pending wake state in a fresh process-equivalent store', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-babysitter-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const session: BabysitterSessionState = {
        issue: { uuid: 'uuid-87', key: 'AR-87', path: '/linear/issues/AR-87__uuid-87.json' },
        repo: 'AgentWorkforce/factory',
        prNumber: 87,
        agentName: 'ar-87-babysit-factory',
        path: '/github/repos/AgentWorkforce/factory/pulls/87/metadata.json',
        critical: true,
        pendingKinds: ['checks-failed', 'review-comment'],
        resourceSubscription: {
          subscriptionId: 'sub-87',
          provider: 'github',
          resourceRef: '/github/repos/AgentWorkforce__factory/pulls/by-id/87.json',
          subscriberId: 'factory-babysitter:uuid-87',
          ownerId: 'factory-runtime',
          expiresAt: '2026-12-31T00:00:00.000Z',
          terminal: true,
        },
        pendingDeliveryClaims: [{ deliveryId: 'delivery-87-terminal', claimToken: 'claim-token-87' }],
      }
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      await first.setBabysitterSession('workspace-1', 'AR-87:uuid-87:/linear/issues/AR-87__uuid-87.json', session)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.listBabysitterSessions('workspace-1')).toEqual([
        ['AR-87:uuid-87:/linear/issues/AR-87__uuid-87.json', session],
      ])

      await restarted.clearBabysitterSession('workspace-1', 'AR-87:uuid-87:/linear/issues/AR-87__uuid-87.json')
      expect(await new FileStateStore({ batchSize: 2, watchStatePath }).listBabysitterSessions('workspace-1'))
        .toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('never returns a babysitter generation token without atomically storing its record', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-babysitter-generation-claim-'))
    try {
      const memory = new InMemoryStateStore({ batchSize: 2 })
      const filePath = join(root, 'state.json')
      const scenarios = [
        { name: 'memory', first: memory, second: memory },
        {
          name: 'file',
          first: new FileStateStore({ batchSize: 2, watchStatePath: filePath }),
          second: new FileStateStore({ batchSize: 2, watchStatePath: filePath }),
        },
      ]

      for (const { name, first, second } of scenarios) {
        const workspaceId = `workspace-${name}`
        const ownershipKey = `AR-230:agentworkforce/factory#230:${name}`
        const claims = await Promise.all([
          first.markRunning(workspaceId, ownershipKey, 'babysitter-a', 1_000, 5_000),
          second.markRunning(workspaceId, ownershipKey, 'babysitter-b', 1_000, 5_000),
        ])
        const acquired = claims.filter((claim): claim is { generationId: string } => claim !== null)

        expect(acquired).toHaveLength(1)
        expect(await second.getBabysitterGeneration(workspaceId, ownershipKey)).toEqual({
          generationId: acquired[0]!.generationId,
          agentName: claims[0] ? 'babysitter-a' : 'babysitter-b',
          claimedAtMs: 1_000,
          leaseUntilMs: 6_000,
          phase: 'claimed',
        })
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('fences stale babysitter generations from renewal, completion, and takeover', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-babysitter-generation-cas-'))
    try {
      const memory = new InMemoryStateStore({ batchSize: 2 })
      const filePath = join(root, 'state.json')
      const scenarios = [
        { first: memory, second: memory },
        {
          first: new FileStateStore({ batchSize: 2, watchStatePath: filePath }),
          second: new FileStateStore({ batchSize: 2, watchStatePath: filePath }),
        },
      ]

      for (const [index, { first, second }] of scenarios.entries()) {
        const workspaceId = `workspace-${index}`
        const ownershipKey = `AR-230:agentworkforce/factory#230:${index}`
        const initial = await first.markRunning(
          workspaceId, ownershipKey, 'babysitter-old', 1_000, 100,
        )
        expect(initial).not.toBeNull()

        await expect(second.markRunning(
          workspaceId, ownershipKey, 'babysitter-early', 1_050, 100, { force: true },
        )).resolves.toBeNull()
        await expect(second.markRunning(
          workspaceId, ownershipKey, 'babysitter-unfenced', 1_101, 100,
        )).resolves.toBeNull()

        const takeover = await second.markRunning(
          workspaceId, ownershipKey, 'babysitter-new', 1_101, 100, { force: true },
        )
        expect(takeover).not.toBeNull()
        expect(takeover?.generationId).not.toBe(initial?.generationId)

        expect(await first.renewBabysitterGeneration(
          workspaceId, ownershipKey, initial!.generationId, 1_102, 200,
        )).toBe(false)
        expect(await first.durableCompletionCas(
          workspaceId, ownershipKey, initial!.generationId,
        )).toBe(false)
        expect(await first.clearBabysitterGeneration(
          workspaceId, ownershipKey, initial!.generationId,
        )).toBe(false)
        expect(await second.renewBabysitterGeneration(
          workspaceId, ownershipKey, takeover!.generationId, 1_102, 200,
        )).toBe(true)
        const completions = await Promise.all([
          first.durableCompletionCas(workspaceId, ownershipKey, takeover!.generationId),
          second.durableCompletionCas(workspaceId, ownershipKey, takeover!.generationId),
        ])
        expect(completions.filter(Boolean)).toHaveLength(1)
        await expect(first.markRunning(
          workspaceId, ownershipKey, 'babysitter-after-complete', 2_000, 100, { force: true },
        )).resolves.toBeNull()

        expect(await second.getBabysitterGeneration(workspaceId, ownershipKey)).toMatchObject({
          generationId: takeover!.generationId,
          agentName: 'babysitter-new',
          leaseUntilMs: 1_302,
          phase: 'completed',
        })
        expect(await second.clearBabysitterGeneration(
          workspaceId, ownershipKey, takeover!.generationId,
        )).toBe(true)
        expect(await first.getBabysitterGeneration(workspaceId, ownershipKey)).toBeUndefined()
        const restarted = await first.markRunning(
          workspaceId, ownershipKey, 'babysitter-restarted', 2_000, 100,
        )
        expect(restarted?.generationId).not.toBe(takeover!.generationId)
        expect(await second.getBabysitterGeneration(workspaceId, ownershipKey)).toMatchObject({
          generationId: restarted!.generationId,
          agentName: 'babysitter-restarted',
          phase: 'claimed',
        })
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects invalid persisted babysitter generation timestamps', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-invalid-generation-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      await writeFile(watchStatePath, JSON.stringify({
        version: 3,
        workspaces: {
          'workspace-1': {
            githubIssueCommentWatches: {},
            waitingClarifications: {},
            babysitterGenerations: {
              'AR-230:agentworkforce/factory#230': {
                generationId: 'generation-invalid',
                agentName: 'babysitter-invalid',
                claimedAtMs: 1.5,
                leaseUntilMs: 2_000,
                phase: 'claimed',
              },
            },
          },
        },
      }))

      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      await expect(store.getBabysitterGeneration(
        'workspace-1', 'AR-230:agentworkforce/factory#230',
      )).rejects.toThrow('Factory GitHub watch state file is invalid')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('persists Slack thread session ownership and coalesced turns across process-equivalent stores', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-slack-session-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const threadId = '1780751612.176219'
      const conversationId = `slack:${threadId}`
      const session: ConversationSessionState = {
        provider: 'slack',
        issue: { uuid: 'uuid-130', key: 'AR-130', path: '/linear/issues/AR-130__uuid-130.json' },
        externalId: threadId,
        context: { channelDir: 'C0FACTORY__factory-e2e' },
        agent: {
          name: 'ar-130-impl-factory',
          sessionRef: 'session-ar-130-impl-factory',
          capability: 'spawn:codex',
          repo: 'AgentWorkforce/factory',
        },
        history: [],
        processedMessageIds: [],
        pending: [],
      }
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await first.reserveConversationSession('workspace-1', conversationId, session)).toBe(true)
      expect(await first.reserveConversationSession('workspace-1', conversationId, {
        ...session,
        agent: { ...session.agent, sessionRef: 'must-not-overwrite' },
      })).toBe(false)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.listConversationSessions('workspace-1')).toEqual([[conversationId, session]])
      await restarted.appendConversationMessage('workspace-1', conversationId, {
        id: '1780751613.000001', text: 'First thought.', receivedAtMs: 1_000, author: 'U123',
      })
      await first.appendConversationMessage('workspace-1', conversationId, {
        id: '1780751613.000002', text: 'And one more.', receivedAtMs: 1_001, author: 'U123',
      })

      const claimed = await restarted.claimConversationTurn(
        'workspace-1', conversationId, 'owner-a', 'claim-a', 2_000, 60_000,
      )
      expect(claimed?.delivery?.messages.map((message) => message.text)).toEqual(['First thought.', 'And one more.'])
      expect(await first.claimConversationTurn('workspace-1', conversationId, 'owner-b', 'claim-b', 2_001, 60_000))
        .toBeUndefined()
      expect(await restarted.renewConversationTurn(
        'workspace-1', conversationId, 'owner-a', 'claim-a', 2_002,
      )).toBe(true)
      expect(await restarted.completeConversationTurn('workspace-1', conversationId, 'owner-a', 'claim-a', {
        name: 'ar-130-impl-factory', sessionRef: 'session-ar-130-turn-2',
      })).toBe(true)

      expect(await new FileStateStore({ batchSize: 2, watchStatePath })
        .getConversationSession('workspace-1', conversationId)).toMatchObject({
        agent: { sessionRef: 'session-ar-130-turn-2' },
        history: [{ id: '1780751613.000001' }, { id: '1780751613.000002' }],
        pending: [],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('fences stale claim completion and preserves a conversation owner rebound during resume', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-slack-fencing-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      const conversationId = 'slack:1780751612.176219'
      await store.reserveConversationSession('workspace-1', conversationId, {
        provider: 'slack',
        issue: { uuid: 'uuid-130', key: 'AR-130', path: '/linear/issues/AR-130__uuid-130.json' },
        externalId: '1780751612.176219',
        context: { channelDir: 'C0FACTORY__factory-e2e' },
        agent: { name: 'ar-130-impl-factory', sessionRef: 'session-impl' },
        history: [],
        processedMessageIds: [],
        pending: [],
      })
      await store.appendConversationMessage('workspace-1', conversationId, {
        id: 'message-1', text: 'How is it going?', receivedAtMs: 1_000,
      })
      await store.claimConversationTurn(
        'workspace-1', conversationId, 'owner-a', 'claim-a', 2_000, 60_000,
      )
      await store.rebindConversationSession('workspace-1', conversationId, {
        name: 'ar-130-babysit-factory', sessionRef: 'session-babysitter',
      })

      expect(await store.completeConversationTurn('workspace-1', conversationId, 'owner-a', 'stale-claim', {
        name: 'ar-130-impl-factory', sessionRef: 'session-impl-rotated',
      })).toBe(false)
      expect(await store.completeConversationTurn('workspace-1', conversationId, 'owner-a', 'claim-a', {
        name: 'ar-130-impl-factory', sessionRef: 'session-impl-rotated',
      })).toBe(true)
      expect(await store.getConversationSession('workspace-1', conversationId)).toMatchObject({
        agent: { name: 'ar-130-babysit-factory', sessionRef: 'session-babysitter' },
        history: [{ id: 'message-1' }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('recovers a legacy v3 in-flight conversation claim by requeueing its messages', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-slack-v3-migration-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const conversationId = 'slack:1780751612.176219'
      await writeFile(watchStatePath, JSON.stringify({
        version: 3,
        workspaces: {
          'workspace-1': {
            githubIssueCommentWatches: {},
            waitingClarifications: {},
            babysitterSessions: {},
            dispatchLifecycles: {},
            conversationSessions: {
              [conversationId]: {
                provider: 'slack',
                issue: { uuid: 'uuid-130', key: 'AR-130', path: '/linear/issues/AR-130__uuid-130.json' },
                externalId: '1780751612.176219',
                context: { channelDir: 'C0FACTORY__factory-e2e' },
                agent: { name: 'ar-130-impl-factory', sessionRef: 'session-impl' },
                history: [],
                pending: [],
                delivery: {
                  owner: 'legacy-owner',
                  claimedAtMs: 1_000,
                  attempts: 1,
                  messages: [{ id: 'message-1', text: 'Do not lose me.', receivedAtMs: 900 }],
                },
              },
            },
          },
        },
      }))

      const restored = await new FileStateStore({ batchSize: 2, watchStatePath })
        .getConversationSession('workspace-1', conversationId)
      expect(restored).toMatchObject({
        processedMessageIds: ['message-1'],
        pending: [{ id: 'message-1', text: 'Do not lose me.' }],
      })
      expect(restored?.delivery).toBeUndefined()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('preserves babysitter sessions and dispatch lifecycles together across restart and independent updates', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-combined-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const issueKey = 'AR-9387:uuid-9387:/linear/issues/AR-9387__uuid-9387.json'
      const session: BabysitterSessionState = {
        issue: { uuid: 'uuid-9387', key: 'AR-9387', path: '/linear/issues/AR-9387__uuid-9387.json' },
        repo: 'AgentWorkforce/factory',
        prNumber: 93,
        agentName: 'ar-9387-babysit-factory',
        path: '/github/repos/AgentWorkforce/factory/pulls/93/metadata.json',
        critical: true,
        pendingKinds: ['review-comment'],
      }
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      const claim = await first.claimDispatchLifecycle(
        'workspace-1', issueKey, dispatchLifecycle(93), 'owner-a', 1_000, 5_000,
      )
      expect(claim.acquired).toBe(true)
      await first.setBabysitterSession('workspace-1', issueKey, session)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.listBabysitterSessions('workspace-1')).toEqual([[issueKey, session]])
      expect(await restarted.getDispatchLifecycle('workspace-1', issueKey)).toMatchObject({
        phase: 'dispatching',
        lease: { owner: 'owner-a', epoch: 1 },
      })

      expect(await restarted.saveDispatchLifecycle('workspace-1', issueKey, 'owner-a', 1, 1_001, {
        ...claim.lifecycle,
        phase: 'published',
      })).toBe(true)
      expect(await restarted.listBabysitterSessions('workspace-1')).toEqual([[issueKey, session]])

      await restarted.clearBabysitterSession('workspace-1', issueKey)
      expect(await new FileStateStore({ batchSize: 2, watchStatePath }).getDispatchLifecycle('workspace-1', issueKey))
        .toMatchObject({ phase: 'published' })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('enforces batch capacity across store instances and promotes queued work after a durable slot release', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-lifecycle-capacity-'))
    try {
      const watchStatePath = join(root, 'state.json')
      const first = new FileStateStore({ batchSize: 1, watchStatePath })
      const second = new FileStateStore({ batchSize: 1, watchStatePath })
      const firstKey = 'AR-851:uuid-851:/linear/issues/AR-851.json'
      const secondKey = 'AR-852:uuid-852:/linear/issues/AR-852.json'
      const firstClaim = await first.claimDispatchLifecycle(
        'workspace-1', firstKey, dispatchLifecycle(851), 'owner-a', 1_000, 5_000,
      )
      const secondClaim = await second.claimDispatchLifecycle(
        'workspace-1', secondKey, dispatchLifecycle(852), 'owner-b', 1_001, 5_000,
      )

      expect(firstClaim.lifecycle.phase).toBe('dispatching')
      expect(secondClaim).toMatchObject({ acquired: true, lifecycle: { phase: 'queued' } })
      expect(await second.promoteDispatchLifecycle('workspace-1', secondKey, 'owner-b', 1, 1_002)).toBe(false)

      expect(await first.saveDispatchLifecycle('workspace-1', firstKey, 'owner-a', 1, 1_003, {
        ...firstClaim.lifecycle,
        phase: 'releasing',
      })).toBe(true)
      expect(await second.promoteDispatchLifecycle('workspace-1', secondKey, 'owner-b', 1, 1_004)).toBe(true)
      expect(await second.getDispatchLifecycle('workspace-1', secondKey)).toMatchObject({ phase: 'dispatching' })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('fences queued lifecycle cleanup against a concurrent promotion', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-lifecycle-queued-clear-'))
    try {
      const stores = [
        new FileStateStore({ batchSize: 2, watchStatePath: join(root, 'state.json') }),
        new InMemoryStateStore({ batchSize: 2 }),
      ]
      for (const [index, store] of stores.entries()) {
        const workspace = `workspace-${index}`
        const promotedKey = `promoted-${index}`
        const queuedSeed = { ...dispatchLifecycle(880 + index), phase: 'queued' as const }
        const snapshot = await store.claimDispatchLifecycle(
          workspace, promotedKey, queuedSeed, `owner-${index}`, 1_000, 5_000,
        )
        expect(snapshot.lease).toBeDefined()
        expect(await store.promoteDispatchLifecycle(
          workspace, promotedKey, `owner-${index}`, snapshot.lease!.epoch, 1_001,
        )).toBe(true)

        expect(await store.clearQueuedDispatchLifecycle(workspace, promotedKey, snapshot.lease)).toBe(false)
        expect(await store.getDispatchLifecycle(workspace, promotedKey)).toMatchObject({ phase: 'dispatching' })

        const queuedKey = `queued-${index}`
        const queued = await store.claimDispatchLifecycle(
          workspace, queuedKey, { ...dispatchLifecycle(890 + index), phase: 'queued' }, `owner-q-${index}`, 1_002, 5_000,
        )
        expect(await store.clearQueuedDispatchLifecycle(workspace, queuedKey, queued.lease)).toBe(true)
        expect(await store.getDispatchLifecycle(workspace, queuedKey)).toBeUndefined()
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not count a bare-repo lifecycle after its canonical PR is handed to a babysitter', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-lifecycle-bare-repo-handoff-'))
    try {
      const stores = [
        new FileStateStore({ batchSize: 1, watchStatePath: join(root, 'state.json') }),
        new InMemoryStateStore({ batchSize: 1 }),
      ]
      for (const [index, store] of stores.entries()) {
        const workspace = `workspace-${index}`
        const first = await store.claimDispatchLifecycle(
          workspace,
          `first-${index}`,
          handedOffDispatchLifecycle(860 + index),
          `owner-${index}`,
          1_000,
          5_000,
        )
        const second = await store.claimDispatchLifecycle(
          workspace,
          `second-${index}`,
          dispatchLifecycle(870 + index),
          `owner-${index}`,
          1_001,
          5_000,
        )

        expect(first.lifecycle.phase).toBe('dispatching')
        expect(second.lifecycle.phase).toBe('dispatching')
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects a persisted babysitter session that omits the critical fence state', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-invalid-babysitter-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      await writeFile(watchStatePath, JSON.stringify({
        version: 2,
        workspaces: {
          'workspace-1': {
            githubIssueCommentWatches: {},
            waitingClarifications: {},
            babysitterSessions: {
              'AR-87:uuid-87:/linear/issues/AR-87__uuid-87.json': {
                issue: { uuid: 'uuid-87', key: 'AR-87', path: '/linear/issues/AR-87__uuid-87.json' },
                repo: 'AgentWorkforce/factory',
                prNumber: 87,
                agentName: 'ar-87-babysit-factory',
                pendingKinds: [],
              },
            },
          },
        },
      }))

      const store = new FileStateStore({ batchSize: 2, watchStatePath })
      await expect(store.listBabysitterSessions('workspace-1')).rejects.toThrow('state file is invalid')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

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

  it('durably stores a parked team and atomically claims only the first human reply', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-clarification-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      const waiting = waitingClarification(77)
      expect(await first.reserveWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77', waiting)).toBe(true)
      expect(await first.reserveWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77', {
        ...waiting,
        question: 'This duplicate must not overwrite the original question.',
      })).toBe(false)

      const restarted = new FileStateStore({ batchSize: 2, watchStatePath })
      expect(await restarted.getWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77')).toEqual(waiting)

      const [firstClaim, duplicateClaim] = await Promise.all([
        restarted.claimClarificationReply('workspace-1', 'AR-77:uuid-77:path-77', {
          id: 'reply-1', text: 'Use the durable wake path.', receivedAtMs: 200,
        }),
        first.claimClarificationReply('workspace-1', 'AR-77:uuid-77:path-77', {
          id: 'reply-2', text: 'Duplicate thread noise.', receivedAtMs: 201,
        }),
      ])
      expect([firstClaim, duplicateClaim].filter(Boolean)).toHaveLength(1)
      const claimed = await restarted.getWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77')
      expect(claimed?.reply?.id).toMatch(/^reply-[12]$/u)
      expect(await restarted.claimClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'too-early', 250, 60_000))
        .toBeUndefined()
      await restarted.markClarificationAgentReleased('workspace-1', 'AR-77:uuid-77:path-77', 'ar-77-impl')
      expect((await first.getWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77'))?.releasedAgents)
        .toEqual(['ar-77-impl'])
      expect(await restarted.markClarificationParked('workspace-1', 'AR-77:uuid-77:path-77', 274)).toBeUndefined()
      await restarted.markClarificationAgentReleased('workspace-1', 'AR-77:uuid-77:path-77', 'ar-77-review')
      await restarted.markClarificationParked('workspace-1', 'AR-77:uuid-77:path-77', 275)

      const [wakeA, wakeB] = await Promise.all([
        restarted.claimClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'factory-a', 300, 60_000),
        first.claimClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'factory-b', 300, 60_000),
      ])
      expect([wakeA, wakeB].filter(Boolean)).toHaveLength(1)
      const wakeOwner = (wakeA ?? wakeB)?.wake?.owner
      expect(wakeOwner).toMatch(/^factory-[ab]$/u)
      expect(await restarted.renewClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'wrong-owner', 301)).toBe(false)
      expect(await restarted.renewClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', wakeOwner!, 301)).toBe(true)
      expect(await restarted.markClarificationAgentInjected('workspace-1', 'AR-77:uuid-77:path-77', wakeOwner!, 'ar-77-impl')).toBe(true)
      expect(await restarted.completeClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'wrong-owner')).toBe(false)
      await restarted.releaseClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', wakeOwner!)
      expect((await first.getWaitingClarification('workspace-1', 'AR-77:uuid-77:path-77'))?.wake?.owner).toBe('')
      const retry = await first.claimClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'factory-retry', 302, 60_000)
      expect(retry).toMatchObject({ wake: { owner: 'factory-retry', attempts: 2, injectedAgents: ['ar-77-impl'] } })

      expect(await restarted.completeClarificationWake('workspace-1', 'AR-77:uuid-77:path-77', 'factory-retry')).toBe(true)
      expect(await first.listWaitingClarifications('workspace-1')).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('leases delivery across daemons and persists fast replies without waking before confirmation', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-file-state-question-delivery-'))
    try {
      const watchStatePath = join(root, 'factory-state.json')
      const first = new FileStateStore({ batchSize: 2, watchStatePath })
      const second = new FileStateStore({ batchSize: 2, watchStatePath })
      const waiting = { ...waitingClarification(78), questionPostedAtMs: undefined }
      const key = 'AR-78:uuid-78:path-78'
      await first.reserveWaitingClarification('workspace-1', key, waiting)
      expect(await first.claimClarificationReply('workspace-1', key, {
        id: 'thread-noise', text: 'Not an answer to a delivered question.', receivedAtMs: 199,
      })).toBeUndefined()
      await first.markClarificationAgentReleased('workspace-1', key, 'ar-78-impl')
      await first.markClarificationAgentReleased('workspace-1', key, 'ar-78-review')
      await first.markClarificationParked('workspace-1', key, 199)

      const [claimA, claimB] = await Promise.all([
        first.claimClarificationQuestionDelivery('workspace-1', key, 'factory-a', 200, 60_000),
        second.claimClarificationQuestionDelivery('workspace-1', key, 'factory-b', 200, 60_000),
      ])
      expect([claimA, claimB].filter(Boolean)).toHaveLength(1)
      const owner = (claimA ?? claimB)?.questionDelivery?.owner
      expect(owner).toMatch(/^factory-[ab]$/u)
      expect(await first.claimClarificationQuestionDelivery('workspace-1', key, owner!, 201, 60_000))
        .toBeUndefined()
      expect(await first.renewClarificationQuestionDelivery('workspace-1', key, 'wrong-owner', 201)).toBe(false)
      expect(await second.renewClarificationQuestionDelivery('workspace-1', key, owner!, 202)).toBe(true)
      expect(await second.claimClarificationReply('workspace-1', key, {
        id: 'fast-answer', text: 'Visible in Slack before confirmation.', receivedAtMs: 203,
      })).toMatchObject({ reply: { id: 'fast-answer' } })
      expect(await first.claimClarificationWake('workspace-1', key, 'too-early', 203, 60_000))
        .toBeUndefined()

      await second.releaseClarificationQuestionDelivery('workspace-1', key, owner!)
      const retry = await first.claimClarificationQuestionDelivery('workspace-1', key, 'factory-retry', 204, 60_000)
      expect(retry?.questionDelivery).toMatchObject({ owner: 'factory-retry', attempts: 2 })
      expect(await second.completeClarificationQuestionDelivery('workspace-1', key, 'factory-retry', 205)).toBe(true)
      expect(await first.claimClarificationWake('workspace-1', key, 'factory-wake', 206, 60_000))
        .toMatchObject({ reply: { id: 'fast-answer' }, wake: { owner: 'factory-wake' } })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

const dispatchLifecycle = (number: number): DispatchLifecycle => ({
  runId: `run-${number}`,
  issue: {
    key: `AR-${number}`,
    uuid: `uuid-${number}`,
    path: `/linear/issues/AR-${number}.json`,
  },
  decision: {
    issue: {
      key: `AR-${number}`,
      uuid: `uuid-${number}`,
      path: `/linear/issues/AR-${number}.json`,
    },
    routes: [{ repo: 'AgentWorkforce/factory', clonePath: '/work/factory', rationale: 'test' }],
    scope: 'single',
    implementers: [],
    reviewer: {
      name: `ar-${number}-review`,
      role: 'reviewer',
      capability: 'spawn:claude',
      task: 'review',
      repo: 'AgentWorkforce/factory',
    },
    thin: false,
    confidence: 'high',
    rationale: 'test',
  },
  dryRun: false,
  phase: 'dispatching',
  agents: [],
  invocationIds: [],
  updatedAtMs: 1_000,
})

const githubDispatchLifecycle = (number: number, path: string): DispatchLifecycle => {
  const lifecycle = dispatchLifecycle(number)
  const issue = {
    key: String(number),
    uuid: `AgentWorkforce/factory#${number}`,
    path,
  }
  return {
    ...lifecycle,
    issue,
    decision: { ...lifecycle.decision, issue },
  }
}

const handedOffDispatchLifecycle = (number: number): DispatchLifecycle => {
  const lifecycle = dispatchLifecycle(number)
  const implementer = {
    name: `ar-${number}-impl-pear`,
    role: 'implementer' as const,
    capability: 'spawn:codex' as const,
    task: 'implement',
    repo: 'pear',
  }
  lifecycle.decision.implementers = [implementer]
  lifecycle.agents = [{
    name: `ar-${number}-babysit-pear`,
    tracked: {
      spec: {
        name: `ar-${number}-babysit-pear`,
        role: 'babysitter',
        capability: 'spawn:claude',
        task: 'babysit',
        repo: 'pear',
        ownedPullRequest: { repo: 'AgentWorkforce/pear', number },
      },
    },
  }]
  return lifecycle
}

const waitingClarification = (number: number): WaitingClarification => {
  const issue = { uuid: `uuid-${number}`, key: `AR-${number}`, path: `path-${number}` }
  const implementer = {
    name: `ar-${number}-impl`,
    role: 'implementer' as const,
    capability: 'spawn:codex' as const,
    repo: 'AgentWorkforce/factory',
    task: 'Implement the issue.',
  }
  const reviewer = {
    name: `ar-${number}-review`,
    role: 'reviewer' as const,
    capability: 'spawn:codex' as const,
    repo: 'AgentWorkforce/factory',
    task: 'Review the implementation.',
  }
  return {
    issue,
    decision: {
      issue,
      routes: [{ repo: 'AgentWorkforce/factory', rationale: 'label' }],
      scope: 'single',
      implementers: [implementer],
      reviewer,
      thin: false,
      confidence: 'high',
      rationale: 'test',
    },
    dryRun: false,
    threadId: '1780000000.000077',
    askerName: implementer.name,
    question: 'Which wake path should I use?',
    askedAtMs: 100,
    questionPostedAtMs: 150,
    agents: [
      { name: implementer.name, tracked: { spec: implementer, sessionRef: 'session-impl' } },
      { name: reviewer.name, tracked: { spec: reviewer, sessionRef: 'session-review' } },
    ],
  }
}

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
