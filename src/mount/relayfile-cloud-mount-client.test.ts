import { describe, expect, it, vi } from 'vitest'
import type { OperationStatusResponse } from '@relayfile/sdk'

import { RelayfileCloudMountClient, type RelayFileClientLike } from './relayfile-cloud-mount-client'

class FakeRelayFileClient implements RelayFileClientLike {
  readonly readFileCalls: Array<{ workspaceId: string; path: string }> = []
  readonly writeFileCalls: Array<{
    workspaceId: string
    path: string
    baseRevision: string
    content: string
    contentType?: string
  }> = []
  readonly listTreeCalls: Array<{ workspaceId: string; options?: { path?: string; depth?: number; cursor?: string } }> = []
  readonly getEventsCalls: Array<{ workspaceId: string; opts?: { cursor?: string; limit?: number } }> = []
  readonly getOpCalls: Array<{ workspaceId: string; opId: string }> = []

  files = new Map<string, { revision: string; content: string; contentType: string }>()
  ops = new Map<string, OperationStatusResponse>()
  events = [{
    eventId: 'evt-1',
    type: 'file.updated' as const,
    path: '/linear/issues/AR-1.json',
    revision: '2',
    timestamp: '2026-01-01T00:00:00.000Z',
  }]

  async readFile(workspaceId: string, path: string) {
    this.readFileCalls.push({ workspaceId, path })
    const file = this.files.get(path)
    if (!file) throw Object.assign(new Error('not found'), { status: 404 })
    return {
      path,
      revision: file.revision,
      content: file.content,
      contentType: file.contentType,
    }
  }

  async writeFile(input: {
    workspaceId: string
    path: string
    baseRevision: string
    content: string
    contentType?: string
  }) {
    this.writeFileCalls.push(input)
    this.files.set(input.path, {
      revision: String(Number(input.baseRevision) + 1),
      content: input.content,
      contentType: input.contentType ?? 'application/json',
    })
    return {
      opId: `op-${this.writeFileCalls.length}`,
      status: 'queued' as const,
      targetRevision: 'next',
    }
  }

  async listTree(workspaceId: string, options?: { path?: string; depth?: number; cursor?: string }) {
    this.listTreeCalls.push({ workspaceId, options })
    return {
      path: options?.path ?? '/',
      entries: [...this.files.keys()].map((path) => ({ path, type: 'file' as const, revision: '1' })),
      nextCursor: null,
    }
  }

  async getEvents(workspaceId: string, opts?: { cursor?: string; limit?: number }) {
    this.getEventsCalls.push({ workspaceId, opts })
    return { events: this.events, nextCursor: null }
  }

  async getResourceAtEvent(eventId: string, context?: { workspaceId: string }) {
    return { path: `/events/${eventId}.json`, data: context, digest: eventId }
  }

  async getOp(workspaceId: string, opId: string) {
    this.getOpCalls.push({ workspaceId, opId })
    return this.ops.get(opId) ?? {
      opId,
      status: 'succeeded' as const,
      attemptCount: 1,
      providerResult: {
        status: 200,
        externalId: 'linear-id',
      },
    }
  }

  async getToken() {
    return 'relayfile-token'
  }

  getBaseUrl() {
    return 'https://relayfile.invalid'
  }
}

describe('RelayfileCloudMountClient', () => {
  it('delegates readFile/listTree/getEvents with the configured workspace id', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-1.json', {
      revision: '7',
      content: '{"payload":{"identifier":"AR-1"}}',
      contentType: 'application/json',
    })
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake, isAllowedDraft: () => true })

    await expect(mount.readFile('/linear/issues/AR-1.json')).resolves.toEqual({
      content: { payload: { identifier: 'AR-1' } },
      revision: '7',
    })
    await expect(mount.listTree('/linear/issues')).resolves.toEqual(['/linear/issues/AR-1.json'])
    await expect(mount.getEvents({ cursor: 'evt-0', limit: 10 })).resolves.toMatchObject({
      events: fake.events,
      nextCursor: null,
    })

    expect(fake.readFileCalls[0]).toEqual({ workspaceId: 'rw_test', path: '/linear/issues/AR-1.json' })
    expect(fake.listTreeCalls[0]).toEqual({
      workspaceId: 'rw_test',
      options: { path: '/linear/issues', cursor: undefined },
    })
    expect(fake.getEventsCalls[0]).toEqual({ workspaceId: 'rw_test', opts: { cursor: 'evt-0', limit: 10 } })
  })

  it('writes through the RelayFileClient with workspace id and live baseRevision', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-1.json', {
      revision: '4',
      content: '{"stateId":"old"}',
      contentType: 'application/json',
    })
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake, isAllowedDraft: () => true })

    await mount.writeFile('/linear/issues/AR-1.json', { stateId: 'new' })

    expect(fake.writeFileCalls).toEqual([{
      workspaceId: 'rw_test',
      path: '/linear/issues/AR-1.json',
      baseRevision: '4',
      content: '{"stateId":"new"}',
      contentType: 'application/json',
    }])
  })

  it('uses baseRevision 0 for creates and confirms the queued operation', async () => {
    const fake = new FakeRelayFileClient()
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await mount.writeFile('/linear/issues/new.json', { title: 'new' })

    expect(fake.writeFileCalls[0]?.baseRevision).toBe('0')
    await expect(mount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 })).resolves.toBe('acked')
    expect(fake.getOpCalls).toEqual([{ workspaceId: 'rw_test', opId: 'op-1' }])
  })

  it('fails closed on unpollable write confirmations for legacy clients and restarted instances', async () => {
    const fake = new FakeRelayFileClient()
    const clientWithoutGetOp: RelayFileClientLike = {
      readFile: fake.readFile.bind(fake),
      writeFile: fake.writeFile.bind(fake),
      listTree: fake.listTree.bind(fake),
      getEvents: fake.getEvents.bind(fake),
      getResourceAtEvent: fake.getResourceAtEvent.bind(fake),
      getToken: fake.getToken.bind(fake),
      getBaseUrl: fake.getBaseUrl.bind(fake),
    }
    const legacyMount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: clientWithoutGetOp,
      isAllowedDraft: () => true,
    })
    const restartedMount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await legacyMount.writeFile('/linear/issues/new.json', { title: 'new' })

    await expect(legacyMount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 })).resolves.toBe('timeout')
    await expect(restartedMount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 })).resolves.toBe('timeout')
    expect(fake.getOpCalls).toEqual([])
  })

  it('refuses provider writeback paths when the draft predicate is unset or rejects', async () => {
    const fake = new FakeRelayFileClient()
    const unset = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })
    const rejecting = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => false,
    })

    await expect(unset.writeFile('/linear/issues/new.json', { title: 'Real work' }))
      .rejects.toThrow(/draft predicate rejected or is unset/)
    await expect(rejecting.writeFile('/slack/channels/C123/messages/root.json', { text: 'Wrong channel' }))
      .rejects.toThrow(/draft predicate rejected or is unset/)
    expect(fake.writeFileCalls).toEqual([])
  })

  it('allows markerless provider writes only when the injected predicate approves the guarded draft', async () => {
    const fake = new FakeRelayFileClient()
    const calls: Array<{ path: string; content: unknown; guarded?: boolean }> = []
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: async (path, content, opts) => {
        calls.push({ path, content, guarded: opts?.guarded })
        return opts?.guarded === true && path === '/linear/issues/AR-1__uuid-1.json'
      },
    })

    await mount.writeFile('/linear/issues/AR-1__uuid-1.json', { stateId: 'implementing' }, { guarded: true })

    expect(calls).toEqual([{
      path: '/linear/issues/AR-1__uuid-1.json',
      content: { stateId: 'implementing' },
      guarded: true,
    }])
    expect(fake.writeFileCalls).toHaveLength(1)
  })

  it('does not confirm a succeeded draft op without providerResult 200 and externalId', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: { status: 200 },
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await mount.writeFile('/linear/issues/new.json', { title: 'new' })

    await expect(mount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 }))
      .rejects.toThrow(/provider result incomplete/)
  })

  it('keeps polling queued and running ops instead of treating them as acked', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'running',
      attemptCount: 1,
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await mount.writeFile('/linear/issues/new.json', { title: 'new' })

    await expect(mount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 })).resolves.toBe('timeout')
  })

  it('surfaces provider writeback lastError on failed ops', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'failed',
      attemptCount: 1,
      lastError: 'Field "id" is read-only and cannot be written',
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await mount.writeFile('/linear/issues/new.json', { title: 'new' })

    await expect(mount.confirmWrite('/linear/issues/new.json', { timeoutMs: 5 }))
      .rejects.toThrow(/Field "id" is read-only/)
  })

  it('delegates subscribe through the workspace-scoped event client', () => {
    const fake = new FakeRelayFileClient()
    const unsubscribe = vi.fn()
    const eventClient = {
      subscribe: vi.fn(() => ({ unsubscribe })),
    }
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake, eventClient })
    const onChange = vi.fn()

    const subscription = mount.subscribe(['/linear/issues/**'], onChange)

    expect(eventClient.subscribe).toHaveBeenCalledWith(['/linear/issues/**'], onChange, undefined)
    expect(subscription.unsubscribe).toBe(unsubscribe)
  })
})
