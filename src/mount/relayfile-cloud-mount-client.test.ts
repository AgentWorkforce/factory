import { describe, expect, it, vi } from 'vitest'
import { CloudAuthError, type CloudSession, type StoredAuth } from '@agent-relay/cloud'
import type {
  AcceptDurableSubscriptionDeliveryInput,
  ChangeEvent,
  ClaimDurableSubscriptionDeliveriesInput,
  CreateOrRenewDurableResourceSubscriptionInput,
  OperationStatusResponse,
} from '@relayfile/sdk'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  createHostedCloudAccessTokenProvider,
  FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV,
  FACTORY_RELAYFILE_SCOPES,
  RelayfileCloudMountClient,
  relayfileWorkspaceTokenProvider,
  resolveFactoryWorkspace,
  type CloudSessionProvider,
  type RelayfileSetupFactory,
  type RelayfileCloudMountClientConfig,
  type RelayFileClientLike,
} from './relayfile-cloud-mount-client'
import { RelayfileGithubConnectionWrite } from './relayfile-github-connection-write'
import { MountAuthScopeError } from './mount-auth-error'

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
  readonly listTreeCalls: Array<{ workspaceId: string; options?: { path?: string; depth?: number; cursor?: string; signal?: AbortSignal } }> = []
  readonly getEventsCalls: Array<{ workspaceId: string; opts?: { cursor?: string; limit?: number; provider?: string; last?: number } }> = []
  readonly listLastNChangesCalls: Array<{ limit: number; context?: { workspaceId: string } }> = []
  readonly getOpCalls: Array<{ workspaceId: string; opId: string }> = []
  readonly createSubscriptionCalls: CreateOrRenewDurableResourceSubscriptionInput[] = []
  readonly claimDeliveryCalls: ClaimDurableSubscriptionDeliveriesInput[] = []
  readonly acceptDeliveryCalls: AcceptDurableSubscriptionDeliveryInput[] = []
  readonly cancelSubscriptionCalls: Array<{
    workspaceId: string
    subscriptionId: string
    options?: { signal?: AbortSignal }
  }> = []
  getSyncStatus?: RelayFileClientLike['getSyncStatus']
  treePageSize?: number

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
    const paths = [...this.files.keys()]
      .filter((path) => path.startsWith(options?.path ?? '/'))
      .sort()
    const start = Number(options?.cursor ?? 0)
    const pageSize = this.treePageSize ?? paths.length
    const page = paths.slice(start, start + pageSize)
    const next = start + page.length
    return {
      path: options?.path ?? '/',
      entries: page.map((path) => ({ path, type: 'file' as const, revision: '1' })),
      nextCursor: next < paths.length ? String(next) : null,
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

  async createOrRenewDurableResourceSubscription(input: CreateOrRenewDurableResourceSubscriptionInput) {
    this.createSubscriptionCalls.push(input)
    return {
      id: 'sub-1',
      ownerId: 'configured-factory-agent',
      subscriberId: input.subscriberId,
      provider: input.provider,
      resourceRef: input.resourceRef,
      eventTypes: input.eventTypes,
      terminalEventTypes: input.terminalEventTypes ?? [],
      intent: input.intent ?? null,
      status: 'active' as const,
      createdAt: '2026-07-21T00:00:00.000Z',
      updatedAt: '2026-07-21T00:00:00.000Z',
      expiresAt: '2026-12-31T00:00:00.000Z',
      retiredAt: null,
    }
  }

  async claimDurableSubscriptionDeliveries(input: ClaimDurableSubscriptionDeliveriesInput) {
    this.claimDeliveryCalls.push(input)
    return {
      deliveries: [{
        id: 'delivery-1',
        claimToken: 'claim-token-1',
        subscriptionId: 'sub-1',
        ownerId: 'configured-factory-agent',
        subscriberId: 'factory-babysitter:uuid-1',
        provider: 'github',
        resourceRef: '/github/repos/AgentWorkforce__pear/pulls/by-id/1.json',
        event: {
          id: 'event-1',
          type: 'pull_request.closed',
          path: '/github/repos/AgentWorkforce__pear/pulls/by-id/1.json',
          revision: '2',
          origin: 'github',
          provider: 'github',
          correlationId: 'corr-1',
          timestamp: '2026-07-21T00:00:00.000Z',
        },
        terminal: true,
        status: 'claimed' as const,
        createdAt: '2026-07-21T00:00:00.000Z',
        claimedAt: '2026-07-21T00:00:01.000Z',
        claimLeaseExpiresAt: '2026-07-21T00:01:01.000Z',
        acceptedAt: null,
      }],
    }
  }

  async acceptDurableSubscriptionDelivery(input: AcceptDurableSubscriptionDeliveryInput) {
    this.acceptDeliveryCalls.push(input)
    if (input.claimToken !== 'claim-token-1') {
      throw Object.assign(new Error('delivery claim mismatch'), { status: 409 })
    }
    const claimed = (await this.claimDurableSubscriptionDeliveries({ workspaceId: input.workspaceId })).deliveries[0]!
    return {
      delivery: {
        ...claimed,
        claimToken: null,
        status: 'accepted' as const,
        acceptedAt: '2026-07-21T00:00:02.000Z',
      },
    }
  }

  async cancelDurableResourceSubscription(
    workspaceId: string,
    subscriptionId: string,
    options?: { signal?: AbortSignal },
  ) {
    this.cancelSubscriptionCalls.push({ workspaceId, subscriptionId, options })
  }

  async getToken() {
    return 'relayfile-token'
  }

  getBaseUrl() {
    return 'https://relayfile.invalid'
  }
}

describe('RelayfileCloudMountClient', () => {
  it('uses the SDK client token provider so long-lived subscriptions receive rotated tokens', async () => {
    const fake = new FakeRelayFileClient()
    const getToken = vi.fn()
      .mockResolvedValueOnce('rotated-relayfile-token-1')
      .mockResolvedValueOnce('rotated-relayfile-token-2')
    fake.getToken = getToken
    const workspaceGetToken = vi.fn(async () => 'original-relayfile-token')
    const provider = relayfileWorkspaceTokenProvider(fake, {
      client: () => fake,
      getToken: workspaceGetToken,
      info: { relayfileUrl: 'https://relayfile.example' },
    })

    await expect(provider()).resolves.toBe('rotated-relayfile-token-1')
    await expect(provider()).resolves.toBe('rotated-relayfile-token-2')
    expect(workspaceGetToken).not.toHaveBeenCalled()
  })

  it('falls back to the original workspace token for older SDK clients', async () => {
    const fake = new FakeRelayFileClient()
    const clientWithoutToken = { ...fake, getToken: undefined } as unknown as RelayFileClientLike
    const workspaceGetToken = vi.fn(async () => 'original-relayfile-token')
    const provider = relayfileWorkspaceTokenProvider(clientWithoutToken, {
      client: () => clientWithoutToken,
      getToken: workspaceGetToken,
      info: { relayfileUrl: 'https://relayfile.example' },
    })

    await expect(provider()).resolves.toBe('original-relayfile-token')
    expect(workspaceGetToken).toHaveBeenCalledTimes(1)
  })

  it('adapts the retained SDK workspace handle for integration status and connect flows', async () => {
    const fake = new FakeRelayFileClient()
    const getConnectionStatus = vi.fn(async () => ({ ready: false, state: 'not_connected' }))
    const connectIntegration = vi.fn(async () => ({
      alreadyConnected: false,
      connectLink: 'https://connect.example/github',
      connectionId: 'conn-github',
    }))
    const waitForConnection = vi.fn(async () => {})
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
      getConnectionStatus,
      connectIntegration,
      waitForConnection,
    }
    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => ({
        joinWorkspace: vi.fn(async () => handle),
      })),
    })

    await expect(mount.integrationConnections?.getStatus('github')).resolves.toEqual({
      ready: false,
      state: 'not_connected',
    })
    await expect(mount.integrationConnections?.connect('github')).resolves.toMatchObject({
      connectionId: 'conn-github',
    })
    await mount.integrationConnections?.waitForConnection('github', 'conn-github')
    await mount.integrationConnections?.getStatus('github')

    expect(getConnectionStatus).toHaveBeenNthCalledWith(1, 'github', 'cloud-workspace-uuid')
    expect(getConnectionStatus).toHaveBeenNthCalledWith(2, 'github', 'conn-github')
    expect(connectIntegration).toHaveBeenCalledWith('github', { allowedIntegrations: ['github'] })
    expect(waitForConnection).toHaveBeenCalledWith('github', {
      connectionId: 'conn-github',
      timeoutMs: 5 * 60_000,
    })
  })

  it('starts local mirrors through the authenticated Relayfile SDK mount session', async () => {
    const fake = new FakeRelayFileClient()
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async () => ({ stop }))
    const setup = {
      joinWorkspace: vi.fn(async () => handle),
      ensureMountedWorkspace,
    }
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => options.startMount())

    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => setup),
      localMountPreflight,
    })

    await expect(mount.ensureLocalMount('/work/repo', {
      stateWaitTimeoutMs: 3210,
    })).resolves.toBeUndefined()

    expect(localMountPreflight).toHaveBeenCalledWith('rw_test', '/work/repo', expect.objectContaining({
      localDir: join('/work/repo', '.integrations'),
      acceptableWorkspaceIds: ['cloud-workspace-uuid'],
      stateWaitTimeoutMs: 3210,
      startMount: expect.any(Function),
    }))
    expect(ensureMountedWorkspace).toHaveBeenCalledWith({
      workspace: handle,
      localDir: join('/work/repo', '.integrations'),
      remotePath: '/',
      mode: 'poll',
      background: true,
      agentName: 'agent-relay-factory',
      scopes: [...FACTORY_RELAYFILE_SCOPES],
      verifyProvider: false,
      supervise: false,
      readyTimeoutMs: 3210,
    })

    await mount.dispose()
    expect(stop).toHaveBeenCalledTimes(1)
  })

  it('passes an exact nonstandard registered mirror root to local preflight', async () => {
    const localDir = '/work/chief/relayfile-mirror'
    const fake = new FakeRelayFileClient()
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => options.startMount())
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      relayfileSetup: { joinWorkspace: vi.fn(), ensureMountedWorkspace: vi.fn(async () => ({ stop: async () => {} })) },
      relayfileWorkspace: handle,
      localMountRoot: localDir,
      localMountPreflight,
    })

    try {
      await mount.ensureLocalMount('/work/unrelated-repository')
      expect(localMountPreflight).toHaveBeenCalledWith('rw_test', '/work/chief', expect.objectContaining({ localDir }))
    } finally {
      await mount.dispose()
    }
  })

  it('looks up a configured workspace mirror only once during fromConfig', async () => {
    const fake = new FakeRelayFileClient()
    const resolver = vi.fn(() => undefined)
    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => ({
        joinWorkspace: vi.fn(async () => ({
          workspaceId: 'cloud-workspace-uuid',
          client: () => fake,
          getToken: async () => 'delegated-relayfile-token',
          info: { relayfileUrl: 'https://relayfile.example' },
        })),
      })),
      workspaceMirrorResolver: resolver,
    })

    expect(resolver).toHaveBeenCalledTimes(1)
    expect(resolver).toHaveBeenCalledWith(['rw_test', 'cloud-workspace-uuid'])
    await mount.dispose()
  })

  it('uses one registered workspace mirror even when callers name different repository checkouts', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-shared-workspace-mirror-'))
    const localDir = join(root, 'chief', '.integrations')
    const fake = new FakeRelayFileClient()
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async () => ({ stop }))
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => options.startMount())
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_shared',
      client: fake,
      relayfileSetup: { joinWorkspace: vi.fn(), ensureMountedWorkspace },
      relayfileWorkspace: handle,
      localMountRoot: localDir,
      localMountPreflight,
    })

    try {
      await Promise.all([
        mount.ensureLocalMount(join(root, 'repo-a')),
        mount.ensureLocalMount(join(root, 'repo-b')),
      ])

      expect(mount.getLocalMountRoot()).toBe(localDir)
      expect(ensureMountedWorkspace).toHaveBeenCalledTimes(1)
      expect(ensureMountedWorkspace).toHaveBeenCalledWith(expect.objectContaining({ localDir }))
      expect(localMountPreflight).toHaveBeenCalledWith('rw_shared', join(root, 'chief'), expect.any(Object))
    } finally {
      await mount.dispose()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('resolves a direct client mirror through the cloud workspace identifier alias', async () => {
    const fake = new FakeRelayFileClient()
    const resolver = vi.fn((workspaceIds: readonly string[]) =>
      workspaceIds.includes('cloud-workspace-uuid') ? '/work/chief/.integrations' : undefined)
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_shared',
      client: fake,
      relayfileSetup: { joinWorkspace: vi.fn(), ensureMountedWorkspace: vi.fn() },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      workspaceMirrorResolver: resolver,
    })

    expect(resolver).toHaveBeenCalledWith(['rw_shared', 'cloud-workspace-uuid'])
    expect(mount.getLocalMountRoot()).toBe('/work/chief/.integrations')
    await mount.dispose()
  })

  it('uses the registered root reported by Relayfile instead of re-homing an unresolved fallback', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-admission-registered-mirror-'))
    const fallbackDir = join(root, 'first-checkout', '.integrations')
    const registeredDir = join(root, 'chief', '.integrations')
    const fake = new FakeRelayFileClient()
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async ({ localDir }: { localDir: string }) => {
      if (localDir === fallbackDir) {
        throw new Error(
          `workspace rw_shared is already mirrored at ${registeredDir}; refusing to silently re-home it to ${fallbackDir}`,
        )
      }
      return { stop }
    })
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => options.startMount())
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_shared',
      client: fake,
      relayfileSetup: { joinWorkspace: vi.fn(), ensureMountedWorkspace },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountPreflight,
    })

    try {
      await Promise.all([
        mount.ensureLocalMount(join(root, 'first-checkout')),
        mount.ensureLocalMount(join(root, 'second-checkout')),
      ])

      expect(ensureMountedWorkspace).toHaveBeenNthCalledWith(1, expect.objectContaining({ localDir: fallbackDir }))
      expect(ensureMountedWorkspace).toHaveBeenNthCalledWith(2, expect.objectContaining({ localDir: registeredDir }))
      expect(ensureMountedWorkspace).toHaveBeenCalledTimes(2)
      expect(mount.getLocalMountRoot()).toBe(registeredDir)
    } finally {
      await mount.dispose()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reports a stale registered mirror and refreshes that same directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-stale-registered-mirror-'))
    const localDir = join(root, 'registered', '.integrations')
    await mkdir(join(localDir, '.relay'), { recursive: true })
    await writeFile(join(localDir, '.relay', 'state.json'), JSON.stringify({
      workspaceId: 'cloud-workspace-uuid',
      lastReconcileAt: new Date(Date.now() - 5 * 60_000).toISOString(),
      pid: process.pid,
    }))
    const fake = new FakeRelayFileClient()
    const ensureMountedWorkspace = vi.fn(async () => ({ stop: async () => {} }))
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => {
      await options.startMount()
      await writeFile(join(localDir, '.relay', 'state.json'), JSON.stringify({
        workspaceId: 'cloud-workspace-uuid',
        lastReconcileAt: new Date().toISOString(),
        pid: process.pid,
      }))
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_shared',
      client: fake,
      relayfileSetup: { joinWorkspace: vi.fn(), ensureMountedWorkspace },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountRoot: localDir,
      localMountPreflight,
    })

    try {
      expect(mount.getLocalMountHealth()).toMatchObject({
        degraded: true,
        reason: expect.stringMatching(/^last reconcile \d+m ago$/u),
        localDir,
      })

      await mount.ensureLocalMount(join(root, 'unrelated-repository'))

      expect(ensureMountedWorkspace).toHaveBeenCalledWith(expect.objectContaining({ localDir }))
      expect(mount.getLocalMountHealth()).toEqual({ degraded: false, localDir })
    } finally {
      await mount.dispose()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reports and supervises an initial SDK mount failure with no prior state file', async () => {
    vi.useFakeTimers()
    const fake = new FakeRelayFileClient()
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async () => ({ stop }))
    let preflightCalls = 0
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => {
      preflightCalls += 1
      if (preflightCalls === 1) throw new Error('initial mount unavailable')
      await options.startMount()
    })
    const healthEvents: Array<{ state: string; reason: string; degradedMounts: number }> = []
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      relayfileSetup: {
        joinWorkspace: vi.fn(),
        ensureMountedWorkspace,
      },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountPreflight,
      localMountHealthIntervalMs: 1_000,
      onLocalMountHealth: (event) => { healthEvents.push(event) },
    })

    try {
      await expect(mount.ensureLocalMount('/work/repo')).rejects.toThrow('initial mount unavailable')
      expect(healthEvents).toEqual([{
        state: 'degraded',
        reason: 'mount_refresh_failed',
        degradedMounts: 1,
      }])

      await vi.advanceTimersByTimeAsync(1_000)
      await vi.waitFor(() => expect(localMountPreflight).toHaveBeenCalledTimes(2))
      expect(ensureMountedWorkspace).toHaveBeenCalledTimes(1)
    } finally {
      await mount.dispose()
      vi.useRealTimers()
    }
  })

  it('treats a MountAuthScopeError as terminal: no retry, marks auth-degraded', async () => {
    vi.useFakeTimers()
    const fake = new FakeRelayFileClient()
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async () => ({ stop }))
    const localMountPreflight = vi.fn(async () => {
      throw new MountAuthScopeError('missing fs:read', { missingScope: 'fs:read' })
    })
    const healthEvents: Array<{ state: string; reason: string; degradedMounts: number }> = []
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      relayfileSetup: {
        joinWorkspace: vi.fn(),
        ensureMountedWorkspace,
      },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountPreflight,
      localMountHealthIntervalMs: 1_000,
      onLocalMountHealth: (event) => { healthEvents.push(event) },
    })

    try {
      await expect(mount.ensureLocalMount('/work/repo')).rejects.toBeInstanceOf(MountAuthScopeError)
      expect(mount.isLocalMountAuthDegraded()).toBe(true)
      expect(healthEvents).toEqual([{
        state: 'degraded',
        reason: 'mount_auth_scope',
        degradedMounts: 1,
      }])

      // The supervisor must NOT re-run the preflight for a terminal failure.
      await vi.advanceTimersByTimeAsync(5_000)
      expect(localMountPreflight).toHaveBeenCalledTimes(1)
    } finally {
      await mount.dispose()
      vi.useRealTimers()
    }
  })

  it('coalesces concurrent mount checks for the same checkout', async () => {
    const fake = new FakeRelayFileClient()
    let releasePreflight!: () => void
    const preflightReleased = new Promise<void>((resolve) => { releasePreflight = resolve })
    const localMountPreflight = vi.fn(async () => preflightReleased)
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      relayfileSetup: {
        joinWorkspace: vi.fn(),
        ensureMountedWorkspace: vi.fn(async () => ({ stop: async () => {} })),
      },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountPreflight,
    })

    const first = mount.ensureLocalMount('/work/repo')
    const duplicate = mount.ensureLocalMount('/work/repo')
    await vi.waitFor(() => expect(localMountPreflight).toHaveBeenCalledTimes(1))
    releasePreflight()
    await Promise.all([first, duplicate])

    expect(localMountPreflight).toHaveBeenCalledTimes(1)
    await mount.dispose()
  })

  it('coalesces routed checkouts onto one workspace-mirror mount operation', async () => {
    const fake = new FakeRelayFileClient()
    let active = 0
    let maximumActive = 0
    let releasePreflights!: () => void
    const preflightsReleased = new Promise<void>((resolve) => { releasePreflights = resolve })
    const localMountPreflight = vi.fn(async () => {
      active += 1
      maximumActive = Math.max(maximumActive, active)
      await preflightsReleased
      active -= 1
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      relayfileSetup: {
        joinWorkspace: vi.fn(),
        ensureMountedWorkspace: vi.fn(async () => ({ stop: async () => {} })),
      },
      relayfileWorkspace: {
        workspaceId: 'cloud-workspace-uuid',
        client: () => fake,
        getToken: async () => 'delegated-relayfile-token',
        info: { relayfileUrl: 'https://relayfile.example' },
      },
      localMountPreflight,
    })

    const checks = Array.from({ length: 6 }, (_, index) => mount.ensureLocalMount(`/work/repo-${index}`))
    await vi.waitFor(() => expect(localMountPreflight).toHaveBeenCalledTimes(1))
    expect(maximumActive).toBe(1)
    releasePreflights()
    await Promise.all(checks)

    expect(localMountPreflight).toHaveBeenCalledTimes(1)
    expect(maximumActive).toBe(1)
    await mount.dispose()
  })

  it('refreshes local mounts before token expiry and reports failure and recovery transitions', async () => {
    vi.useFakeTimers()
    vi.setSystemTime('2026-07-20T12:00:00.000Z')
    const startDir = await mkdtemp(join(tmpdir(), 'factory-mount-supervision-'))
    const localDir = join(startDir, '.integrations')
    await mkdir(join(localDir, '.relay'), { recursive: true })
    await writeFile(join(localDir, '.relay', 'state.json'), JSON.stringify({
      workspaceId: 'cloud-workspace-uuid',
      lastReconcileAt: '2026-07-20T12:00:00.000Z',
    }))
    const fake = new FakeRelayFileClient()
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const firstStop = vi.fn(async () => {})
    const recoveredStop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn()
      .mockResolvedValueOnce({
        stop: firstStop,
        suggestedRefreshAt: '2026-07-20T12:00:01.000Z',
      })
      .mockRejectedValueOnce(new Error('refresh token expired'))
      .mockResolvedValueOnce({
        stop: recoveredStop,
        suggestedRefreshAt: '2026-07-20T13:00:00.000Z',
      })
    const setup = {
      joinWorkspace: vi.fn(async () => handle),
      ensureMountedWorkspace,
    }
    const healthEvents: Array<{ state: string; reason: string; degradedMounts: number }> = []
    const localMountPreflight = vi.fn(async (
      _workspaceId: string,
      _startDir: string,
      options: { startMount: () => Promise<void> },
    ) => options.startMount())
    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => setup),
      localMountPreflight,
      localMountHealthIntervalMs: 1_000,
      onLocalMountHealth: (event) => { healthEvents.push(event) },
    })

    try {
      await mount.ensureLocalMount(startDir)
      await vi.advanceTimersByTimeAsync(1_000)
      expect(healthEvents).toEqual([{
        state: 'degraded',
        reason: 'mount_refresh_failed',
        degradedMounts: 1,
      }])

      await vi.advanceTimersByTimeAsync(1_000)
      expect(healthEvents).toEqual([
        { state: 'degraded', reason: 'mount_refresh_failed', degradedMounts: 1 },
        { state: 'recovered', reason: 'mount_stale', degradedMounts: 0 },
      ])
      expect(ensureMountedWorkspace).toHaveBeenCalledTimes(3)
      expect(firstStop).toHaveBeenCalledTimes(1)
    } finally {
      await mount.dispose()
      await rm(startDir, { recursive: true, force: true })
      vi.useRealTimers()
    }
    expect(recoveredStop).toHaveBeenCalledTimes(1)
  })

  it('stops a mount whose launch finishes after disposal begins', async () => {
    const fake = new FakeRelayFileClient()
    const handle = {
      workspaceId: 'cloud-workspace-uuid',
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    let finishLaunch: ((mounted: { stop: () => Promise<void> }) => void) | undefined
    const stop = vi.fn(async () => {})
    const ensureMountedWorkspace = vi.fn(async () => await new Promise<{ stop: () => Promise<void> }>((resolve) => {
      finishLaunch = resolve
    }))
    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => ({
        joinWorkspace: vi.fn(async () => handle),
        ensureMountedWorkspace,
      })),
      localMountPreflight: async (
        _workspaceId,
        _startDir,
        options: { startMount: () => Promise<void> },
      ) => options.startMount(),
    })

    const mounting = mount.ensureLocalMount('/work/repo')
    await vi.waitFor(() => expect(ensureMountedWorkspace).toHaveBeenCalledTimes(1))
    await mount.dispose()
    finishLaunch?.({ stop })
    await mounting

    expect(stop).toHaveBeenCalledTimes(1)
  })

  it('does not report recovery while a periodic health check still finds a stale mount', async () => {
    vi.useFakeTimers()
    vi.setSystemTime('2026-07-20T12:30:00.000Z')
    const startDir = await mkdtemp(join(tmpdir(), 'factory-stale-mount-supervision-'))
    const localDir = join(startDir, '.integrations')
    await mkdir(join(localDir, '.relay'), { recursive: true })
    await writeFile(join(localDir, '.relay', 'state.json'), JSON.stringify({
      workspaceId: 'cloud-workspace-uuid',
      lastReconcileAt: '2026-07-20T12:00:00.000Z',
    }))
    const fake = new FakeRelayFileClient()
    const ensureMountedWorkspace = vi.fn()
    const healthEvents: Array<{ state: string; reason: string; degradedMounts: number }> = []
    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider: vi.fn(async () => cloudSession(storedAuth())),
      relayfileSetupFactory: vi.fn(() => ({
        joinWorkspace: vi.fn(async () => ({
          workspaceId: 'cloud-workspace-uuid',
          client: () => fake,
          getToken: async () => 'delegated-relayfile-token',
          info: { relayfileUrl: 'https://relayfile.example' },
        })),
        ensureMountedWorkspace,
      })),
      localMountHealthIntervalMs: 1_000,
      onLocalMountHealth: (event) => { healthEvents.push(event) },
    })

    try {
      await mount.ensureLocalMount(startDir, { refreshStaleMount: false })
      await vi.advanceTimersByTimeAsync(1_000)

      expect(ensureMountedWorkspace).not.toHaveBeenCalled()
      expect(healthEvents).toEqual([{
        state: 'degraded',
        reason: 'mount_stale',
        degradedMounts: 1,
      }])
    } finally {
      await mount.dispose()
      await rm(startDir, { recursive: true, force: true })
      vi.useRealTimers()
    }
  })

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
    expect(joinOptions.scopes).toContain('relayfile:fs:read:/linear/states/**')
    // github uses the provider root `/github/**`; `/github/repos/**` is rejected
    // by RelayAuth's path-token validator and would fail the whole batch mint.
    expect(joinOptions.scopes).toContain('relayfile:fs:write:/github/**')
    expect(joinOptions.scopes).not.toContain('relayfile:fs:write:/github/repos/**')
    expect(joinOptions.scopes).toContain('relayfile:fs:write:/factory/observability/**')
    expect(joinOptions.scopes).toContain('relayfile:fs:read:/slack/users/**')
    expect(mount.githubWrite).toBeDefined()
    expect(factoryReadScopeCovers('/linear/states/_index.json')).toBe(true)
    expect(factoryReadScopeCovers('/linear/states/state-ready.json')).toBe(true)
    expect(factoryReadScopeCovers('/slack/users/U123/messages/1781267200_000000/meta.json')).toBe(true)
    expect(capturedTokenProvider).toBeDefined()
    await expect(capturedTokenProvider?.()).resolves.toBe('cld_at_shared')
  })

  it('uses an injected hosted token provider without consulting local Cloud login or AGENT_RELAY_BIN', async () => {
    const fake = new FakeRelayFileClient()
    const handle = {
      client: vi.fn(() => fake),
      getToken: vi.fn(async () => 'delegated-relayfile-token'),
      info: { relayfileUrl: 'https://relayfile.example' },
    }
    const setup = { joinWorkspace: vi.fn(async () => handle) }
    const cloudSessionProvider = vi.fn(async () => {
      throw new Error('local Cloud login must not be consulted')
    })
    const cloudAccessTokenProvider = vi.fn(async () => 'relay_pa_hosted_access')
    let capturedTokenProvider: (() => Promise<string>) | undefined
    const relayfileSetupFactory: RelayfileSetupFactory = vi.fn(({ tokenProvider }) => {
      capturedTokenProvider = tokenProvider
      return setup
    })

    const mount = await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudApiUrl: 'https://cloud.example',
      cloudAccessTokenProvider,
      cloudSessionProvider,
      cloudSessionEnv: { AGENT_RELAY_BIN: '/definitely/not/a/node-cli' },
      relayfileSetupFactory,
    })

    expect(mount.workspaceId).toBe('rw_test')
    expect(cloudSessionProvider).not.toHaveBeenCalled()
    expect(relayfileSetupFactory).toHaveBeenCalledWith({
      cloudApiUrl: 'https://cloud.example',
      tokenProvider: expect.any(Function),
    })
    expect(setup.joinWorkspace).toHaveBeenCalledWith('rw_test', {
      agentName: 'agent-relay-factory',
      scopes: [...FACTORY_RELAYFILE_SCOPES],
    })
    await expect(capturedTokenProvider?.()).resolves.toBe('relay_pa_hosted_access')
  })

  it('rejects a non-path token from an injected hosted provider before workspace join', async () => {
    const joinWorkspace = vi.fn()
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => ({
      joinWorkspace: async () => {
        await tokenProvider()
        return await joinWorkspace()
      },
    })

    await expect(RelayfileCloudMountClient.fromConfig({
      cloudApiUrl: 'https://cloud.example',
      cloudAccessTokenProvider: async () => 'relay_ws_overbroad',
      relayfileSetupFactory,
    })).rejects.toThrow('invalid token class')
    expect(joinWorkspace).not.toHaveBeenCalled()
  })

  it('uses an explicit hosted endpoint without resolving local Cloud workspace state', async () => {
    const fake = new FakeRelayFileClient()
    const cloudSessionProvider = vi.fn(async () => {
      throw new Error('local Cloud login must not be consulted')
    })
    const joinWorkspace = vi.fn(async () => ({
      client: () => fake,
      getToken: async () => 'delegated-relayfile-token',
      info: { relayfileUrl: 'https://relayfile.example' },
    }))
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => ({
      joinWorkspace: async (workspaceId, options) => {
        await tokenProvider()
        return await joinWorkspace(workspaceId, options)
      },
    })

    await RelayfileCloudMountClient.fromConfig({
      cloudAccessTokenUrl: 'http://factory-auth.do/v1/access',
      cloudApiUrl: 'https://cloud.example',
      cloudSessionProvider,
      cloudAccessTokenFetch: vi.fn(async () => Response.json({ accessToken: 'relay_pa_hosted_access' })) as unknown as typeof fetch,
      relayfileSetupFactory,
    })

    expect(cloudSessionProvider).not.toHaveBeenCalled()
    expect(joinWorkspace).toHaveBeenCalledWith('rw_7ccfea89', expect.objectContaining({
      agentName: 'agent-relay-factory',
    }))
  })

  it('fails loudly when a hosted token provider has no Cloud API URL', async () => {
    await expect(RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudAccessTokenProvider: async () => 'relay_pa_hosted_access',
    })).rejects.toThrow('cloudApiUrl')
  })

  it('loads the hosted CLI credential through the private endpoint without local login or AGENT_RELAY_BIN', async () => {
    const fake = new FakeRelayFileClient()
    const cloudSessionProvider = vi.fn(async () => {
      throw new Error('local Cloud login must not be consulted')
    })
    const fetchImpl = vi.fn(async () => Response.json({ accessToken: 'relay_pa_rotated_access' }))
    let capturedTokenProvider: (() => Promise<string>) | undefined
    const relayfileSetupFactory: RelayfileSetupFactory = vi.fn(({ tokenProvider }) => {
      capturedTokenProvider = tokenProvider
      return {
        joinWorkspace: vi.fn(async () => ({
          client: () => fake,
          getToken: async () => 'delegated-relayfile-token',
          info: { relayfileUrl: 'https://relayfile.example' },
        })),
      }
    })

    await RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider,
      cloudSessionEnv: {
        [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: 'http://factory-auth.do/v1/access',
        CLOUD_API_URL: 'https://cloud.example',
        AGENT_RELAY_BIN: '/definitely/not/a/node-cli',
      },
      cloudAccessTokenFetch: fetchImpl as unknown as typeof fetch,
      relayfileSetupFactory,
    })

    expect(cloudSessionProvider).not.toHaveBeenCalled()
    expect(relayfileSetupFactory).toHaveBeenCalledWith({
      cloudApiUrl: 'https://cloud.example',
      tokenProvider: expect.any(Function),
    })
    await expect(capturedTokenProvider?.()).resolves.toBe('relay_pa_rotated_access')
    expect(fetchImpl).toHaveBeenCalledWith(new URL('http://factory-auth.do/v1/access'), expect.objectContaining({
      method: 'GET',
      redirect: 'error',
      headers: expect.objectContaining({ 'cache-control': 'no-store' }),
    }))
  })

  it('fails closed when the hosted credential endpoint variable is defined but blank', async () => {
    const cloudSessionProvider = vi.fn(async () => {
      throw new Error('local Cloud login must not be consulted')
    })

    await expect(RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionProvider,
      cloudSessionEnv: {
        [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: '   ',
        CLOUD_API_URL: 'https://cloud.example',
      },
    })).rejects.toThrow(`${FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV} must be an absolute URL`)
    expect(cloudSessionProvider).not.toHaveBeenCalled()
  })

  it('does not resolve a local Cloud workspace when hosted credentials are configured', async () => {
    const activeWorkspaceResolver = vi.fn(async () => {
      throw new Error('local Cloud workspace resolution must not run')
    })

    await expect(resolveFactoryWorkspace(activeWorkspaceResolver, {
      [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: 'http://factory-auth.do/v1/access',
    })).resolves.toEqual({ workspaceId: 'rw_7ccfea89' })
    expect(activeWorkspaceResolver).not.toHaveBeenCalled()
  })

  it('cancels the hosted credential request when the caller aborts', async () => {
    let requestSignal: AbortSignal | undefined
    const fetchImpl = vi.fn(async (_url: unknown, init?: RequestInit) => {
      requestSignal = init?.signal ?? undefined
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => {
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))
        }, { once: true })
      })
    })
    const provider = createHostedCloudAccessTokenProvider({
      url: 'http://factory-auth.do/v1/access',
      fetchImpl: fetchImpl as unknown as typeof fetch,
      timeoutMs: 60_000,
    })

    const controller = new AbortController()
    const pending = provider({ signal: controller.signal })
    await vi.waitFor(() => { expect(fetchImpl).toHaveBeenCalledOnce() })
    controller.abort()

    await expect(pending).rejects.toThrow('hosted Cloud access-token request was cancelled')
    expect(requestSignal?.aborted).toBe(true)
  })

  it('refuses a hosted credential request whose caller signal is already aborted', async () => {
    const fetchImpl = vi.fn(async () => Response.json({ accessToken: 'relay_pa_never' }))
    const provider = createHostedCloudAccessTokenProvider({
      url: 'http://factory-auth.do/v1/access',
      fetchImpl: fetchImpl as unknown as typeof fetch,
    })

    await expect(provider({ signal: AbortSignal.abort() }))
      .rejects.toThrow('hosted Cloud access-token request was cancelled')
    expect(fetchImpl).not.toHaveBeenCalled()
  })

  it('resolves a local Cloud workspace when the hosted credential variable is blank', async () => {
    const activeWorkspaceResolver = vi.fn(async () => ({
      relayfileWorkspaceId: 'rw_local',
      cloudWorkspaceId: 'ws-uuid',
    }))

    await expect(resolveFactoryWorkspace(activeWorkspaceResolver, {
      [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: '   ',
    })).resolves.toEqual({ workspaceId: 'rw_local', cloudWorkspaceId: 'ws-uuid' })
    expect(activeWorkspaceResolver).toHaveBeenCalledOnce()
  })

  it('cancels a failed hosted credential response body without reading it', async () => {
    const response = new Response('sensitive failure details', { status: 503 })
    const cancel = vi.spyOn(response.body!, 'cancel')
    const fetchImpl = vi.fn(async () => response)
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => ({
      joinWorkspace: async () => {
        await tokenProvider()
        throw new Error('unreachable')
      },
    })

    await expect(RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionEnv: {
        [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: 'http://factory-auth.do/v1/access',
        CLOUD_API_URL: 'https://cloud.example',
      },
      cloudAccessTokenFetch: fetchImpl as unknown as typeof fetch,
      relayfileSetupFactory,
    })).rejects.toThrow('returned HTTP 503')
    expect(cancel).toHaveBeenCalledOnce()
  })

  it('fails hosted startup when the private endpoint does not return a RelayAuth path token', async () => {
    const fetchImpl = vi.fn(async () => Response.json({ accessToken: 'relay_ws_overbroad' }))
    const relayfileSetupFactory: RelayfileSetupFactory = ({ tokenProvider }) => ({
      joinWorkspace: async () => {
        await tokenProvider()
        throw new Error('unreachable')
      },
    })

    await expect(RelayfileCloudMountClient.fromConfig({
      workspaceId: 'rw_test',
      cloudSessionEnv: {
        [FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]: 'http://factory-auth.do/v1/access',
        CLOUD_API_URL: 'https://cloud.example',
      },
      cloudAccessTokenFetch: fetchImpl as unknown as typeof fetch,
      relayfileSetupFactory,
    })).rejects.toThrow('invalid token class')
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

  it('adapts durable resource subscriptions through the canonical Relayfile SDK methods', async () => {
    const fake = new FakeRelayFileClient()
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    const client = mount.resourceSubscriptions!
    await expect(client.createOrRenew('rw_test', {
      provider: 'github',
      resourceRef: '/github/repos/AgentWorkforce__pear/pulls/by-id/1.json',
      eventTypes: ['pull_request_review_comment.created'],
      terminalEventTypes: ['pull_request.closed'],
      subscriberId: 'factory-babysitter:uuid-1',
      ttlSeconds: 3600,
    })).resolves.toMatchObject({ subscriptionId: 'sub-1', ownerId: 'configured-factory-agent' })
    await expect(client.claimDeliveryClaims('rw_test')).resolves.toEqual([expect.objectContaining({
      deliveryId: 'delivery-1',
      claimToken: 'claim-token-1',
      terminal: true,
    })])
    await expect(client.acceptDelivery('rw_test', { deliveryId: 'delivery-1', claimToken: 'wrong-token' }))
      .rejects.toMatchObject({ status: 409 })
    await expect(client.acceptDelivery('rw_test', { deliveryId: 'delivery-1', claimToken: 'claim-token-1' }))
      .resolves.toEqual({ deliveryId: 'delivery-1', subscriptionId: 'sub-1', terminal: true })
    await client.cancel('rw_test', { subscriptionId: 'sub-1' })

    expect(fake.createSubscriptionCalls).toEqual([expect.objectContaining({
      workspaceId: 'rw_test',
      provider: 'github',
      resourceRef: '/github/repos/AgentWorkforce__pear/pulls/by-id/1.json',
      eventTypes: ['pull_request_review_comment.created'],
      terminalEventTypes: ['pull_request.closed'],
      subscriberId: 'factory-babysitter:uuid-1',
      ttlSeconds: 3600,
    })])
    expect(fake.claimDeliveryCalls[0]).toMatchObject({ workspaceId: 'rw_test' })
    expect(fake.acceptDeliveryCalls).toEqual([
      expect.objectContaining({ workspaceId: 'rw_test', deliveryId: 'delivery-1', claimToken: 'wrong-token' }),
      expect.objectContaining({ workspaceId: 'rw_test', deliveryId: 'delivery-1', claimToken: 'claim-token-1' }),
    ])
    expect(fake.cancelSubscriptionCalls).toEqual([
      expect.objectContaining({ workspaceId: 'rw_test', subscriptionId: 'sub-1' }),
    ])
  })

  it('fails closed for non-claimed SDK deliveries and forwards lifecycle cancellation', async () => {
    const fake = new FakeRelayFileClient()
    fake.claimDurableSubscriptionDeliveries = vi.fn(async (input) => ({
      deliveries: [{
        ...(await new FakeRelayFileClient().claimDurableSubscriptionDeliveries(input)).deliveries[0]!,
        claimToken: null,
        status: 'pending' as const,
      }],
    }))
    const malformed = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
    })
    await expect(malformed.resourceSubscriptions!.claimDeliveryClaims('rw_test'))
      .rejects.toThrow(/without a live claim/u)

    const controller = new AbortController()
    const cancelledSdk = new FakeRelayFileClient()
    const claim = vi.spyOn(cancelledSdk, 'claimDurableSubscriptionDeliveries')
    const cancelled = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: cancelledSdk,
      resourceSubscriptionSignal: controller.signal,
    })
    await cancelled.resourceSubscriptions!.claimDeliveryClaims('rw_test')
    expect(claim).toHaveBeenCalledWith(expect.objectContaining({
      workspaceId: 'rw_test',
      signal: controller.signal,
    }))
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
      // #351: reads carry the per-operation deadline's signal, so a relayfile
      // call that stops answering is cancelled instead of waited on forever.
      options: { path: '/linear/issues', cursor: undefined, signal: expect.any(AbortSignal) },
    })
    expect(fake.listTreeCalls[0]?.options?.signal?.aborted).toBe(false)
    expect(fake.getEventsCalls[0]).toEqual({ workspaceId: 'rw_test', opts: { cursor: 'evt-0', limit: 10 } })
  })

  it('paginates listTree to exhaustion', async () => {
    const fake = new FakeRelayFileClient()
    fake.treePageSize = 2
    for (const number of [1, 2, 3, 4, 5]) {
      fake.files.set(`/github/repos/AgentWorkforce__factory/issues/by-id/${number}.json`, {
        revision: '1',
        content: '{}',
        contentType: 'application/json',
      })
    }
    fake.files.set('/linear/issues/AR-1.json', {
      revision: '1',
      content: '{}',
      contentType: 'application/json',
    })
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.listTree('/github/repos/AgentWorkforce__factory/issues')).resolves.toEqual(
      [1, 2, 3, 4, 5].map((number) =>
        `/github/repos/AgentWorkforce__factory/issues/by-id/${number}.json`,
      ),
    )
    expect(fake.listTreeCalls.map((call) => call.options?.cursor)).toEqual([undefined, '2', '4'])
  })

  // #351: the SDK attaches an AbortSignal to its fetch only when the caller
  // supplies one, and nothing here did — so every relayfile read was a bare
  // fetch() with no deadline. One of them stopped answering on 2026-08-23 and
  // the readiness reconcile cycle that issued it never finished.
  describe('bounded relayfile reads', () => {
    class HangingListTreeClient extends FakeRelayFileClient {
      seenSignal?: AbortSignal

      override async listTree(
        workspaceId: string,
        options?: { path?: string; depth?: number; cursor?: string; signal?: AbortSignal },
      ): Promise<never> {
        this.listTreeCalls.push({ workspaceId, options })
        this.seenSignal = options?.signal
        return await new Promise<never>((_resolve, reject) => {
          options?.signal?.addEventListener('abort', () => {
            reject((options.signal as AbortSignal & { reason?: unknown }).reason)
          })
        })
      }
    }

    class HangingReadFileClient extends FakeRelayFileClient {
      seenSignal?: AbortSignal

      override async readFile(
        workspaceId: string,
        path: string,
        _correlationId?: string,
        signal?: AbortSignal,
      ): Promise<never> {
        this.readFileCalls.push({ workspaceId, path })
        this.seenSignal = signal
        return await new Promise<never>((_resolve, reject) => {
          signal?.addEventListener('abort', () => {
            reject((signal as AbortSignal & { reason?: unknown }).reason)
          })
        })
      }
    }

    it('cancels a read that stops answering and names the operation', async () => {
      const client = new HangingListTreeClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        operationTimeoutMs: 25,
      })

      await expect(mount.listTree('/github/repos')).rejects.toMatchObject({
        name: 'RelayfileOperationTimeoutError',
        operation: 'listTree',
      })
      // Cancelled, not merely abandoned. An abandoned wait leaves the socket
      // and the SDK's retry loop live, and hands the next caller the same
      // wedged in-flight read.
      expect(client.seenSignal?.aborted).toBe(true)
    })

    it('honours the ensureSubRoot timeout it used to accept and discard', async () => {
      const client = new HangingListTreeClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        // Client-wide budget far past the caller's, so only the per-call
        // argument can end this. It was silently dropped before #351, which is
        // why `#ensureGithubIngestionReady` passing 90_000 bounded nothing.
        operationTimeoutMs: 60_000,
      })

      await expect(mount.ensureSubRoot('/github/issues', { timeoutMs: 25 })).rejects.toMatchObject({
        name: 'RelayfileOperationTimeoutError',
        operation: 'ensureSubRoot',
      })
    })

    it('cancels the revision read before an unguarded write', async () => {
      const client = new HangingReadFileClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        operationTimeoutMs: 25,
      })

      await expect(mount.writeFile('/tmp/draft.json', { draft: true })).rejects.toMatchObject({
        name: 'RelayfileOperationTimeoutError',
        operation: 'writeFile.readRevision',
      })
      expect(client.seenSignal?.aborted).toBe(true)
      expect(client.writeFileCalls).toEqual([])
    })

    it('cancels the current-revision read before a delete', async () => {
      const client = new HangingReadFileClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        operationTimeoutMs: 25,
      })

      await expect(mount.deleteFile('/tmp/draft.json')).rejects.toMatchObject({
        name: 'RelayfileOperationTimeoutError',
        operation: 'deleteFile.readCurrent',
      })
      expect(client.seenSignal?.aborted).toBe(true)
      expect(client.deleteFileCalls).toEqual([])
    })

    it('caps an explicit ensureSubRoot timeout at the tighter client-wide budget', async () => {
      const client = new HangingListTreeClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        operationTimeoutMs: 25,
      })

      // The caller's argument must cap, not replace: a client-wide budget
      // tighter than the argument has to still cancel at the transport, or the
      // orchestrator's backstop fires first and abandons the wait instead
      // (codex on #354).
      await expect(mount.ensureSubRoot('/github/issues', { timeoutMs: 60_000 })).rejects.toMatchObject({
        name: 'RelayfileOperationTimeoutError',
        operation: 'ensureSubRoot',
        timeoutMs: 25,
      })
      expect(client.seenSignal?.aborted).toBe(true)
    })

    it('leaves the call unbounded when no budget is configured', async () => {
      const client = new HangingListTreeClient()
      const mount = new RelayfileCloudMountClient({
        workspaceId: 'rw_test',
        client,
        operationTimeoutMs: 0,
      })

      const pending = mount.listTree('/github/repos')
      const settled = await Promise.race([
        pending.then(() => 'settled' as const, () => 'settled' as const),
        new Promise<'pending'>((resolve) => { setTimeout(() => resolve('pending'), 50) }),
      ])
      expect(settled).toBe('pending')
      expect(client.seenSignal).toBeUndefined()
      // Nothing will ever settle this; drop the reference explicitly so the
      // rejection-free pending promise is not mistaken for a leak.
      void pending.catch(() => undefined)
    })
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
        webhookHealthy: true,
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
      webhookHealthy: true,
    })
    expect(fake.getSyncStatus).toHaveBeenCalledWith('rw_test', { provider: 'slack' })
  })

  it('normalizes snake-case webhook health as independent provider freshness', async () => {
    const fake = new FakeRelayFileClient()
    fake.getSyncStatus = vi.fn(async () => ({
      status: 'ready',
      connections: [{
        provider: 'slack',
        status: 'lagging',
        last_event_at: '2026-06-06T12:05:00.000Z',
        webhook_healthy: true,
      }],
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getSyncStatus?.('slack')).resolves.toEqual({
      provider: 'slack',
      status: 'lagging',
      lastEventAt: '2026-06-06T12:05:00.000Z',
      lastEventAtMs: Date.parse('2026-06-06T12:05:00.000Z'),
      watermarkTs: '2026-06-06T12:05:00.000Z',
      lagSeconds: undefined,
      webhookHealthy: true,
    })
  })

  it.each([
    { webhookHealthy: true, label: 'healthy' },
    { webhookHealthy: false, label: 'unhealthy' },
  ])('preserves nested sync freshness when wrapper webhook delivery is $label', async ({ webhookHealthy }) => {
    const fake = new FakeRelayFileClient()
    fake.getSyncStatus = vi.fn(async () => ({
      webhookHealthy,
      connections: [{
        provider: 'slack',
        status: 'lagging',
        lastEventAt: '2026-06-06T12:05:00.000Z',
        lagSeconds: 86_400,
      }],
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getSyncStatus?.('slack')).resolves.toEqual({
      provider: 'slack',
      status: 'lagging',
      lastEventAt: '2026-06-06T12:05:00.000Z',
      lastEventAtMs: Date.parse('2026-06-06T12:05:00.000Z'),
      watermarkTs: '2026-06-06T12:05:00.000Z',
      lagSeconds: 86_400,
      webhookHealthy,
    })
  })

  it('lets an explicit unhealthy signal win across split provider metadata', async () => {
    const fake = new FakeRelayFileClient()
    fake.getSyncStatus = vi.fn(async () => ({
      webhookHealthy: true,
      connections: [{
        provider: 'slack',
        status: 'ready',
        last_event_at: '2026-06-12T10:00:00.000Z',
        webhook_healthy: false,
      }],
    }))
    const mount = new RelayfileCloudMountClient({ workspaceId: 'rw_test', client: fake })

    await expect(mount.getSyncStatus?.('slack')).resolves.toMatchObject({
      lastEventAt: '2026-06-12T10:00:00.000Z',
      webhookHealthy: false,
    })
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
      webhookHealthy: undefined,
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

    await expect(mount.writeFile('/linear/issues/AR-1.json', { stateId: 'new' }))
      .resolves.toEqual({ targetRevision: 'next' })

    expect(fake.writeFileCalls).toEqual([{
      workspaceId: 'rw_test',
      path: '/linear/issues/AR-1.json',
      baseRevision: '4',
      content: '{"stateId":"new"}',
      contentType: 'application/json',
    }])
  })

  it('does not refresh or retry an explicit baseRevision after a conflict', async () => {
    const fake = new FakeRelayFileClient()
    const conflict = Object.assign(new Error('revision conflict'), { status: 409 })
    const write = vi.spyOn(fake, 'writeFile').mockRejectedValue(conflict)
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await expect(mount.writeFile(
      '/linear/issues/AR-1.json',
      { stateId: 'ready' },
      { baseRevision: '7' },
    )).rejects.toBe(conflict)

    expect(write).toHaveBeenCalledTimes(1)
    expect(write).toHaveBeenCalledWith({
      workspaceId: 'rw_test',
      path: '/linear/issues/AR-1.json',
      baseRevision: '7',
      content: '{"stateId":"ready"}',
      contentType: 'application/json',
    })
    expect(fake.readFileCalls).toEqual([])
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

  it('confirms a succeeded draft op with providerResult 201 and externalId', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: {
        status: 201,
        externalId: '52',
      },
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })

    await mount.writeFile('/github/pull-requests/new.json', { title: 'new' })

    await expect(mount.confirmWrite('/github/pull-requests/new.json', { timeoutMs: 5 })).resolves.toBe('acked')
    await expect(mount.getConfirmedWriteExternalId('/github/pull-requests/new.json')).resolves.toBe('52')
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

  it('does not confirm a succeeded draft op without a providerResult externalId', async () => {
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

  it('does not confirm a succeeded draft op with a non-2xx providerResult status', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: {
        status: 500,
        externalId: 'linear-id',
      },
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
    await expect(mount.getConfirmedWriteFailureReason('/linear/issues/new.json'))
      .resolves.toBe('Field "id" is read-only and cannot be written')
  })

  it('feeds an existing-reference provider failure into the GitHub update-ref fallback', async () => {
    const fake = new FakeRelayFileClient()
    fake.ops.set('op-1', {
      opId: 'op-1',
      status: 'failed',
      attemptCount: 1,
      lastError: 'GitHub writeback failed with status 422: Reference already exists',
    })
    fake.ops.set('op-2', {
      opId: 'op-2',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: { status: 200, externalId: 'factory/test-existing-ref' },
    })
    fake.ops.set('op-3', {
      opId: 'op-3',
      status: 'succeeded',
      attemptCount: 1,
      providerResult: { status: 201, externalId: '66' },
    })
    const mount = new RelayfileCloudMountClient({
      workspaceId: 'rw_test',
      client: fake,
      isAllowedDraft: () => true,
    })
    const writer = new RelayfileGithubConnectionWrite({ mount })

    await expect(writer.publishPullRequest({
      repo: 'AgentWorkforce/factory',
      headRef: 'factory/test-existing-ref',
      headSha: '1234567890abcdef1234567890abcdef12345678',
      baseRef: 'main',
      title: 'Title',
      body: 'Body',
    })).resolves.toMatchObject({ number: 66 })

    expect(fake.writeFileCalls.map((call) => call.path)).toEqual([
      '/github/repos/AgentWorkforce/factory/refs/factory.json',
      '/github/repos/AgentWorkforce/factory/refs/refs%2Fheads%2Ffactory%2Ftest-existing-ref.json',
      expect.stringMatching(/\/github\/repos\/AgentWorkforce\/factory\/pull-requests\/factory-/u),
    ])
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
