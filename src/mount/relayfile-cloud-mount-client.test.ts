import { describe, expect, it, vi } from 'vitest'
import { CloudAuthError, type CloudSession, type StoredAuth } from '@agent-relay/cloud'
import type { ChangeEvent, OperationStatusResponse } from '@relayfile/sdk'

import {
  FACTORY_RELAYFILE_SCOPES,
  RelayfileCloudMountClient,
  type CloudSessionProvider,
  type RelayfileSetupFactory,
  type RelayfileCloudMountClientConfig,
  type RelayFileClientLike,
} from './relayfile-cloud-mount-client'

const storedAuth = (overrides: Partial<StoredAuth> = {}): StoredAuth => ({
  apiUrl: 'https://cloud.example',
  accessToken: 'cld_at_aaa',
  refreshToken: 'cld_rt_aaa',
  accessTokenExpiresAt: '2026-07-01T00:00:00.000Z',
  ...overrides,
})

const cloudSession = (auth: StoredAuth): CloudSession => ({
  auth,
  client: {} as CloudSession['client'],
})

const factoryReadScopeCovers = (path: string): boolean =>
  FACTORY_RELAYFILE_SCOPES.some((scope) => {
    const prefix = 'relayfile:fs:read:'
    if (!scope.startsWith(prefix)) return false
    const scopePath = scope.slice(prefix.length)
    if (scopePath.endsWith('/**')) return path.startsWith(scopePath.slice(0, -3))
    return path === scopePath
  })

class FakeRelayFileClient implements RelayFileClientLike {
  readonly readFileCalls: Array<{ workspaceId: string; path: string }> = []
  readonly writeFileCalls: Array<{
    workspaceId: string
    path: string
    baseRevision: string
    content: string
    contentType?: string
  }> = []
  readonly deleteFileCalls: Array<{
    workspaceId: string
    path: string
    baseRevision: string
  }> = []
  readonly listTreeCalls: Array<{ workspaceId: string; options?: { path?: string; depth?: number; cursor?: string } }> = []
  readonly getEventsCalls: Array<{ workspaceId: string; opts?: { cursor?: string; limit?: number; provider?: string; last?: number } }> = []
  readonly listLastNChangesCalls: Array<{ limit: number; context?: { workspaceId: string } }> = []
  readonly getOpCalls: Array<{ workspaceId: string; opId: string }> = []
  getSyncStatus?: RelayFileClientLike['getSyncStatus']

  files = new Map<string, { revision: string; content: string; contentType: string }>()
  ops = new Map<string, OperationStatusResponse>()
  events: Array<{
    eventId: string
    type: 'file.updated'
    path: string
    provider?: string
    revision: string
    timestamp: string
  }> = [{
    eventId: 'evt-1',
    type: 'file.updated' as const,
    path: '/linear/issues/AR-1.json',
    provider: 'linear',
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

  async deleteFile(input: {
    workspaceId: string
    path: string
    baseRevision: string
  }) {
    this.deleteFileCalls.push(input)
    this.files.delete(input.path)
    return {
      opId: `delete-op-${this.deleteFileCalls.length}`,
      status: 'queued' as const,
      targetRevision: 'deleted',
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

  async getEvents(workspaceId: string, opts?: { cursor?: string; limit?: number; provider?: string; last?: number }) {
    this.getEventsCalls.push({ workspaceId, opts })
    return { events: this.events, nextCursor: null }
  }

  async listLastNChanges(limit: number, context?: { workspaceId: string }): Promise<{ events: ChangeEvent[] }> {
    this.listLastNChangesCalls.push({ limit, context })
    return {
      events: this.events.map((event) => ({
        id: event.eventId,
        workspace: 'rw_test',
        type: 'relayfile.changed' as const,
        occurredAt: event.timestamp,
        resource: {
          path: event.path,
          kind: 'file',
          id: event.path,
          provider: event.provider,
        },
        summary: {},
        expand: async () => ({ level: 'summary' as const, path: event.path, summary: {} }),
      }) as unknown as ChangeEvent),
    }
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
  it('fromConfig delegates through the shared cloud session with least-privilege factory scopes', async () => {
    const fake = new FakeRelayFileClient()
    const auth = storedAuth({ accessToken: 'cld_at_shared', refreshToken: 'cld_rt_shared' })
    const handle = {
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const setup = {
      joinWorkspace: vi.fn(async () => handle),
    }
    const cloudSessionProvider = vi.fn(async () => cloudSession(auth))
    let capturedTokenProvider: (() => Promise<string>) | undefined
    const relayfileSetupFactory: RelayfileSetupFactory = vi.fn(({ tokenProvider }) => {
      capturedTokenProvider = tokenProvider
      return setup
    })

    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      agentName: 'factory-agent',
      cloudSessionProvider,
      relayfileSetupFactory,
    })

    expect(mount.workspaceId).toBe('rw_test')
    expect(cloudSessionProvider).toHaveBeenCalledWith(expect.objectContaining({ interactive: false }))
    expect(relayfileSetupFactory).toHaveBeenCalledWith({
      cloudApiUrl: 'https://cloud.example',
      tokenProvider: expect.any(Function),
    })
    expect(setup.joinWorkspace).toHaveBeenCalledWith('rw_test', {
      agentName: 'factory-agent',
      scopes: [...FACTORY_RELAYFILE_SCOPES],
    })
    const joinCalls = setup.joinWorkspace.mock.calls as unknown as Array<[string, { scopes: string[] }]>
    expect(joinCalls[0]).toBeDefined()
    const joinOptions = joinCalls[0][1]
    expect(joinOptions.scopes).not.toContain('relayfile:fs:read:/**')
    expect(joinOptions.scopes).not.toContain('relayfile:fs:write:/**')
    expect(joinOptions.scopes).toContain('relayfile:fs:read:/slack/users/**')
    expect(factoryReadScopeCovers('/slack/users/U123/messages/1781267200_000000/meta.json')).toBe(true)
    expect(capturedTokenProvider).toBeDefined()
    await expect(capturedTokenProvider?.()).resolves.toBe('cld_at_shared')
  })

  it('uses the shared cloud session provider for refreshed relayfile workspace token mints', async () => {
    let auth = storedAuth({ accessToken: 'cld_at_shared', refreshToken: 'cld_rt_shared' })
    const setup = {
      joinWorkspace: vi.fn(async () => ({
        client: () => new FakeRelayFileClient(),
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      })),
    }
    const cloudSessionProvider = vi.fn(async () => cloudSession(auth))
    let capturedTokenProvider: (() => Promise<string>) | undefined
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => {
      capturedTokenProvider = tokenProvider
      return setup
    }

    await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider,
      relayfileSetupFactory,
    })
    auth = storedAuth({
      accessToken: 'cld_at_rotated',
      refreshToken: 'cld_rt_rotated',
    })

    expect(capturedTokenProvider).toBeDefined()
    await expect(capturedTokenProvider?.()).resolves.toBe('cld_at_rotated')
    expect(cloudSessionProvider).toHaveBeenCalledTimes(2)
  })

  it('coalesces concurrent shared session resolutions for relayfile token refresh', async () => {
    const setup = {
      joinWorkspace: vi.fn(async () => ({
        client: () => new FakeRelayFileClient(),
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      })),
    }
    const cloudSessionProvider = vi.fn<CloudSessionProvider>()
    cloudSessionProvider.mockResolvedValueOnce(cloudSession(storedAuth({ accessToken: 'cld_at_initial' })))
    let releaseSecondSession: (() => void) | undefined
    cloudSessionProvider.mockImplementationOnce(async () => new Promise<CloudSession>((resolve) => {
      releaseSecondSession = () => resolve(cloudSession(storedAuth({ accessToken: 'cld_at_coalesced' })))
    }))
    let capturedTokenProvider: (() => Promise<string>) | undefined
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => {
      capturedTokenProvider = tokenProvider
      return setup
    }

    await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider,
      relayfileSetupFactory,
    })
    expect(capturedTokenProvider).toBeDefined()
    const tokenProvider = capturedTokenProvider as () => Promise<string>
    const first = tokenProvider()
    const second = tokenProvider()

    expect(cloudSessionProvider).toHaveBeenCalledTimes(2)
    releaseSecondSession?.()
    await expect(Promise.all([first, second])).resolves.toEqual(['cld_at_coalesced', 'cld_at_coalesced'])
  })

  it('rejects explicit legacy credential paths from JavaScript callers', async () => {
    const config = { credsPath: '/tmp/legacy-cloud-credentials.json' } as unknown as RelayfileCloudMountClientConfig

    await expect(RelayfileCloudMountClient.fromConfig(config))
      .rejects.toThrow(/no longer accepts credsPath/)
  })

  it('rejects legacy credential paths even when a client is injected', async () => {
    const config = {
      client: new FakeRelayFileClient(),
      credsPath: '/tmp/legacy-cloud-credentials.json',
    } as unknown as RelayfileCloudMountClientConfig

    await expect(RelayfileCloudMountClient.fromConfig(config))
      .rejects.toThrow(/no longer accepts credsPath/)
  })

  it('surfaces a cloud login action when no shared session exists', async () => {
    const cloudSessionProvider = vi.fn(async () => {
      throw new CloudAuthError(
        'AUTH_BROWSER_REQUIRED',
        'Cloud login required. Run `agent-relay login`.',
      )
    })

    await expect(RelayfileCloudMountClient.fromConfig({ cloudSessionProvider }))
      .rejects.toThrow('Relayfile cloud session required; run `agent-relay login`')
  })

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

  it('uses recent change-log events for provider-filtered getEvents tail reads', async () => {
    const fake = new FakeRelayFileClient()
    fake.events = [
      {
        eventId: 'evt-linear',
        type: 'file.updated' as const,
        path: '/linear/issues/AR-1.json',
        provider: 'linear',
        revision: '2',
        timestamp: '2026-01-01T00:00:00.000Z',
      },
      {
        eventId: 'evt-slack',
        type: 'file.updated' as const,
        path: '/slack/channels/C0/messages/1/meta.json',
        provider: 'slack',
        revision: '3',
        timestamp: '2026-01-01T00:01:00.000Z',
      },
    ]
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getEvents({ provider: 'slack', last: 100, limit: 100 })).resolves.toMatchObject({
      events: [
        {
          id: 'evt-slack',
          occurredAt: '2026-01-01T00:01:00.000Z',
          resource: { provider: 'slack', path: '/slack/channels/C0/messages/1/meta.json' },
        },
      ],
      nextCursor: null,
    })
    expect(fake.listLastNChangesCalls).toEqual([{ limit: 100, context: { workspaceId: 'rw_test' } }])
    expect(fake.getEventsCalls).toEqual([])
  })

  it('normalizes nested provider sync freshness from integration metadata', async () => {
    const fake = new FakeRelayFileClient()
    fake.getSyncStatus = vi.fn(async () => ({
      provider: 'slack',
      connections: [{
        provider: 'slack',
        status: 'ready',
        lastEventAt: '2026-06-12T10:00:00.000Z',
        lagSeconds: 42,
      }],
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getSyncStatus?.('slack')).resolves.toEqual({
      provider: 'slack',
      status: 'ready',
      lastEventAt: '2026-06-12T10:00:00.000Z',
      lastEventAtMs: Date.parse('2026-06-12T10:00:00.000Z'),
      watermarkTs: '2026-06-12T10:00:00.000Z',
      lagSeconds: 42,
    })
    expect(fake.getSyncStatus).toHaveBeenCalledWith('rw_test', { provider: 'slack' })
  })

  it('prefers nested provider freshness over wrapper status metadata', async () => {
    const fake = new FakeRelayFileClient()
    fake.getSyncStatus = vi.fn(async () => ({
      status: 'ready',
      connections: [{
        provider: 'slack',
        last_event_at_ms: 1_781_267_200_000,
        lag_seconds: 12,
      }],
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getSyncStatus?.('slack')).resolves.toEqual({
      provider: 'slack',
      status: undefined,
      lastEventAt: undefined,
      lastEventAtMs: 1_781_267_200_000,
      watermarkTs: undefined,
      lagSeconds: 12,
    })
  })

  it('selects the numeric event high-watermark instead of lexicographic max', async () => {
    const fake = new FakeRelayFileClient()
    fake.events = ['7', '8', '9', '10', '11'].map((eventId) => ({
      eventId,
      type: 'file.updated' as const,
      path: `/linear/issues/AR-${eventId}.json`,
      revision: eventId,
      timestamp: '2026-01-01T00:00:00.000Z',
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getEventHighWatermark()).resolves.toBe('11')
    expect(fake.getEventsCalls).toEqual([])
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
      deleteFile: fake.deleteFile.bind(fake),
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

  it('deletes non-provider paths without requiring a delete predicate', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/tmp/draft.json', {
      revision: '3',
      content: '{"draft":true}',
      contentType: 'application/json',
    })
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await mount.deleteFile('/tmp/draft.json')

    expect(fake.deleteFileCalls).toEqual([{
      workspaceId: 'rw_test',
      path: '/tmp/draft.json',
      baseRevision: '3',
    }])
    await expect(mount.confirmWrite('/tmp/draft.json', { timeoutMs: 5 })).resolves.toBe('acked')
  })

  it('refuses untracked provider deletes before calling client.deleteFile', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-E2ECANARY.json', {
      revision: '7',
      content: JSON.stringify({
        provider: 'linear',
        objectType: 'issue',
        payload: {
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] untracked orphan-shaped draft',
        },
      }),
      contentType: 'application/json',
    })
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.deleteFile('/linear/issues/AR-E2ECANARY.json'))
      .rejects.toThrow(/create operation is unknown/)
    expect(fake.deleteFileCalls).toEqual([])
  })

  it('refuses url-bearing provider deletes even when the tracked op failed', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-133__uuid-133.json', {
      revision: '8',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-133',
          url: 'https://linear.app/agent-relay/issue/AR-133/real',
        },
      }),
      contentType: 'application/json',
    })
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'failed',
      attemptCount: 1,
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    await mount.writeFile('/linear/issues/AR-133__uuid-133.json', {
      payload: {
        identifier: 'AR-133',
        url: 'https://linear.app/agent-relay/issue/AR-133/real',
      },
    })

    await expect(mount.deleteFile('/linear/issues/AR-133__uuid-133.json'))
      .rejects.toThrow(/reconciled or linked/)
    expect(fake.deleteFileCalls).toEqual([])
  })

  it('refuses real-key provider deletes even when the tracked op failed', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-133__uuid-133.json', {
      revision: '8',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-133',
          title: '[factory-e2e] real-key draft',
        },
      }),
      contentType: 'application/json',
    })
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'failed',
      attemptCount: 1,
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    await mount.writeFile('/linear/issues/AR-133__uuid-133.json', {
      payload: {
        identifier: 'AR-133',
        title: '[factory-e2e] real-key draft',
      },
    })

    await expect(mount.deleteFile('/linear/issues/AR-133__uuid-133.json'))
      .rejects.toThrow(/reconciled or linked/)
    expect(fake.deleteFileCalls).toEqual([])
  })

  it('refuses deletes for tracked provider writes that succeeded with an externalId', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-E2ECANARY.json', {
      revision: '2',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] linked source draft',
        },
      }),
      contentType: 'application/json',
    })
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: {
        status: 200,
        externalId: 'dac27fce-linked-real-issue',
      },
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    await mount.writeFile('/linear/issues/AR-E2ECANARY.json', {
      payload: {
        identifier: 'AR-E2ECANARY',
        title: '[factory-e2e] linked source draft',
      },
    })

    await expect(mount.deleteFile('/linear/issues/AR-E2ECANARY.json'))
      .rejects.toThrow(/linked provider object/)
    expect(fake.deleteFileCalls).toEqual([])
  })

  it('refuses deletes while the tracked provider write is still pending', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-E2ECANARY.json', {
      revision: '2',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] pending draft',
        },
      }),
      contentType: 'application/json',
    })
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'pending',
      attemptCount: 1,
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    await mount.writeFile('/linear/issues/AR-E2ECANARY.json', {
      payload: {
        identifier: 'AR-E2ECANARY',
        title: '[factory-e2e] pending draft',
      },
    })

    await expect(mount.deleteFile('/linear/issues/AR-E2ECANARY.json'))
      .rejects.toThrow(/status is pending/)
    expect(fake.deleteFileCalls).toEqual([])
  })

  it('allows provider deletes only for this client’s failed unlinked orphan drafts', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-E2ECANARY.json', {
      revision: '2',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] orphan draft',
        },
      }),
      contentType: 'application/json',
    })
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
      isAllowedDelete: () => true,
    })
    await mount.writeFile('/linear/issues/AR-E2ECANARY.json', {
      payload: {
        identifier: 'AR-E2ECANARY',
        title: '[factory-e2e] orphan draft',
      },
    })

    await mount.deleteFile('/linear/issues/AR-E2ECANARY.json')

    expect(fake.deleteFileCalls).toEqual([{
      workspaceId: 'rw_test',
      path: '/linear/issues/AR-E2ECANARY.json',
      baseRevision: '3',
    }])
  })

  it('refuses failed unlinked orphan deletes when the injected delete predicate is unset', async () => {
    const fake = new FakeRelayFileClient()
    fake.files.set('/linear/issues/AR-E2ECANARY.json', {
      revision: '2',
      content: JSON.stringify({
        payload: {
          identifier: 'AR-E2ECANARY',
          title: '[factory-e2e] orphan draft',
        },
      }),
      contentType: 'application/json',
    })
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'failed',
      attemptCount: 1,
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    await mount.writeFile('/linear/issues/AR-E2ECANARY.json', {
      payload: {
        identifier: 'AR-E2ECANARY',
        title: '[factory-e2e] orphan draft',
      },
    })

    await expect(mount.deleteFile('/linear/issues/AR-E2ECANARY.json'))
      .rejects.toThrow(/delete predicate rejected or is unset/)
    expect(fake.deleteFileCalls).toEqual([])
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
