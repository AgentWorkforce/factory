import { describe, expect, it } from 'vitest'

import {
  DurableObjectHostedFactoryStateStore,
  InMemoryHostedFactoryStateStore,
  type HostedFactoryDurableObjectStorage,
  type HostedFactoryDurableObjectTransaction,
} from './state-store'
import type { HostedFactoryIssueRecord } from './types'

class MemoryDurableStorage implements HostedFactoryDurableObjectStorage {
  readonly values = new Map<string, unknown>()

  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.values.get(key)) as T | undefined
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.values.set(key, structuredClone(value))
  }

  async delete(key: string): Promise<boolean> {
    return this.values.delete(key)
  }

  async list<T>(options?: { prefix?: string }): Promise<Map<string, T>> {
    const entries = [...this.values]
      .filter(([key]) => !options?.prefix || key.startsWith(options.prefix))
      .map(([key, value]) => [key, structuredClone(value)] as [string, T])
    return new Map(entries)
  }

  async transaction<T>(
    closure: (transaction: HostedFactoryDurableObjectTransaction) => Promise<T>,
  ): Promise<T> {
    return await closure(this)
  }
}

function record(workspaceId: string, issueId: string, invocationId: string): HostedFactoryIssueRecord {
  return {
    workspaceId,
    issue: {
      uuid: issueId,
      key: 'AR-1',
      title: '[factory] test',
      description: 'test',
      stateId: 'ready',
      labels: ['factory'],
      path: `/linear/issues/${issueId}.json`,
      raw: {},
    },
    phase: 'dispatching',
    invocations: [{
      invocationId,
      status: 'pending',
      spec: {
        name: 'ar-1-impl',
        role: 'implementer',
        capability: 'spawn:codex',
        task: 'test',
        repo: 'AgentWorkforce/factory',
      },
    }],
    createdAt: '2026-07-19T12:00:00.000Z',
    updatedAt: '2026-07-19T12:00:00.000Z',
  }
}

describe.each([
  ['memory', (now: () => number) => new InMemoryHostedFactoryStateStore({ now })],
  ['Durable Object', (now: () => number) => new DurableObjectHostedFactoryStateStore(
    new MemoryDurableStorage(),
    { now },
  )],
])('%s hosted Factory state store', (_name, createStore) => {
  it('rejects stale writes after lease takeover and preserves monotonic epochs', async () => {
    let nowMs = 1_000
    const store = createStore(() => nowMs)
    const first = await store.claimRunLease('ws-a', 'host-a:1', 100)
    expect(first.acquired).toBe(true)
    if (!first.acquired) throw new Error('expected first lease')
    expect(await store.saveIssue(record('ws-a', 'issue-a', 'inv-a'), first.lease)).toBe(true)

    nowMs = 1_101
    const second = await store.claimRunLease('ws-a', 'host-b:1', 100)
    expect(second.acquired).toBe(true)
    if (!second.acquired) throw new Error('expected takeover lease')
    expect(second.lease.epoch).toBe(first.lease.epoch + 1)
    expect(await store.saveIssue(record('ws-a', 'stale', 'inv-stale'), first.lease)).toBe(false)

    await store.releaseRunLease(first.lease)
    expect(await store.renewRunLease(second.lease, 100)).toMatchObject({
      ownerId: 'host-b:1',
      epoch: second.lease.epoch,
    })
    expect(await store.saveIssue(record('ws-a', 'issue-b', 'inv-b'), second.lease)).toBe(true)

    nowMs = 1_202
    const sameOwnerNewClaim = await store.claimRunLease('ws-a', 'host-b:1', 100)
    expect(sameOwnerNewClaim.acquired).toBe(true)
    expect(sameOwnerNewClaim.lease.epoch).toBe(second.lease.epoch + 1)
  })

  it('fences a live same-owner claimant with a new epoch', async () => {
    const store = createStore(() => 1_000)
    const first = await store.claimRunLease('ws-a', 'reused-owner', 100)
    expect(first.acquired).toBe(true)
    if (!first.acquired) throw new Error('expected first lease')
    expect(await store.saveIssue(record('ws-a', 'issue-a', 'inv-a'), first.lease)).toBe(true)

    const replacement = await store.claimRunLease('ws-a', 'reused-owner', 100)
    expect(replacement.acquired).toBe(true)
    if (!replacement.acquired) throw new Error('expected replacement lease')
    expect(replacement.lease.epoch).toBe(first.lease.epoch + 1)
    expect(await store.saveIssue(record('ws-a', 'stale', 'inv-stale'), first.lease)).toBe(false)
    expect(await store.saveIssue(record('ws-a', 'current', 'inv-current'), replacement.lease)).toBe(true)
  })

  it('isolates workspace state and indexes invocation IDs', async () => {
    const store = createStore(() => 1_000)
    const a = await store.claimRunLease('ws-a', 'host-a', 100)
    const b = await store.claimRunLease('ws-b', 'host-b', 100)
    if (!a.acquired || !b.acquired) throw new Error('expected leases')
    await store.saveIssue(record('ws-a', 'same-issue', 'inv-a'), a.lease)
    await store.saveIssue(record('ws-b', 'same-issue', 'inv-b'), b.lease)

    expect((await store.listIssues('ws-a')).map(({ invocations }) => invocations[0]?.invocationId)).toEqual(['inv-a'])
    expect(await store.findIssueByInvocation('ws-a', 'inv-b')).toBeNull()
    expect((await store.findIssueByInvocation('ws-b', 'inv-b'))?.workspaceId).toBe('ws-b')
  })
})
