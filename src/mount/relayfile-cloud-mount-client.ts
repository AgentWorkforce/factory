import {
  CloudAuthError,
  defaultApiUrl,
  ensureCloudSession,
  resolveActiveWorkspace,
  type ActiveWorkspaceDescriptor,
  type CloudSession,
  type CloudSessionOptions,
} from '@agent-relay/cloud'
import {
  type ChangeEvent,
  type DeleteFileInput,
  type EventFeedResponse,
  type FileReadResponse,
  type GetEventsOptions,
  type ListTreeOptions,
  type ResourceAtEventResult,
  type OperationStatusResponse,
  type Subscription,
  type TreeResponse,
  type WriteFileInput,
  type WriteQueuedResponse,
} from '@relayfile/sdk'
import { RelayfileSetup } from '@relayfile/sdk/cli'
import { existsSync } from 'node:fs'
import { isAbsolute, join, resolve } from 'node:path'

import type {
  EventPage,
  FactoryIntegrationConnections,
  FactoryIntegrationProvider,
  GithubConnectionRead,
  GithubConnectionWrite,
  LocalMountOptions,
  LocalMountHealth,
  Logger,
  MountClient,
  ProviderSyncStatus,
  SubscribeOptions,
} from '../ports'
import {
  createResourceSubscriptionsSdkClient,
  createWorkspaceScopedEventClient,
  type RelayfileEventClient,
  type ResourceSubscriptionsSdk,
  type TokenProvider,
  type WorkspaceEventClientSource,
} from '../subscriptions'
import type { ResourceSubscriptionsClient } from '../subscriptions'
import { RelayfileGithubConnectionWrite } from './relayfile-github-connection-write'
import { GithubApiIssueRead } from './github-api-issue-read'
import {
  ensureLocalMount as runLocalMountPreflight,
  type EnsureLocalMountOptions,
} from './local-mount-preflight'
import { checkMountStaleness } from './relayfile-binary'
import { MountAuthScopeError } from './mount-auth-error'
import { resolveRegisteredWorkspaceMirror } from './workspace-mirror'

const DEFAULT_WORKSPACE_ID = 'rw_7ccfea89'
const DEFAULT_AGENT_NAME = 'agent-relay-factory'
const DEFAULT_LOCAL_MOUNT_HEALTH_INTERVAL_MS = 30_000
const DEFAULT_LOCAL_MOUNT_MAX_CONCURRENCY = 4
const DEFAULT_HOSTED_ACCESS_TOKEN_TIMEOUT_MS = 10_000
export const FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV = 'FACTORY_CLOUD_ACCESS_TOKEN_URL'
export const FACTORY_RELAYFILE_SCOPES = [
  'relayfile:fs:read:/linear/issues/**',
  'relayfile:fs:read:/linear/states/**',
  'relayfile:fs:write:/linear/issues/**',
  // RelayAuth's path-token validator rejects `/github/repos/**` as an invalid
  // relayfile path (github paths must be the provider root or carry an owner
  // segment), and because all scopes mint in one batch that single bad path
  // fails the ENTIRE delegated-token mint with `invalid_paths` — leaving the
  // mount with no fs token and a `403 missing required scope: fs:read` on every
  // read. `/github/**` is the github provider root (a valid superset) and mints
  // cleanly. Do NOT narrow this back to `/github/repos/**`.
  'relayfile:fs:read:/github/**',
  'relayfile:fs:write:/github/**',
  'relayfile:fs:read:/slack/channels/**',
  'relayfile:fs:write:/slack/channels/**',
  'relayfile:fs:read:/slack/users/**',
  'relayfile:fs:read:/factory/observability/**',
  'relayfile:fs:write:/factory/observability/**',
] as const

export type CloudSessionProvider = (options?: CloudSessionOptions) => Promise<CloudSession>

export type ActiveWorkspaceResolver = (
  options?: { interactive?: boolean },
) => Promise<ActiveWorkspaceDescriptor>

export interface ResolvedFactoryWorkspace {
  /** Relayfile workspace handle (e.g. `rw_7ccfea89`) — what the mount/config use. */
  workspaceId: string
  /**
   * Cloud-side UUID for the same workspace (e.g. `50587328-…`). The local
   * relayfile mount records this form in its state.json, so it is passed to the
   * staleness check as an accepted alias to avoid a spurious "workspace
   * mismatch" warning when handle and UUID refer to the same workspace.
   */
  cloudWorkspaceId?: string
}

/**
 * Derive the factory's workspace from the active cloud session rather than a
 * hardcoded config value. `resolveActiveWorkspace()` resolves the locally
 * pinned workspace against the cloud API and returns both identifier forms.
 * Falls back to the built-in default when resolution fails (offline / no
 * session), so callers always get a usable handle.
 */
export async function resolveFactoryWorkspace(
  resolver: ActiveWorkspaceResolver = resolveActiveWorkspace,
): Promise<ResolvedFactoryWorkspace> {
  try {
    const descriptor = await resolver({ interactive: false })
    if (descriptor.relayfileWorkspaceId) {
      return {
        workspaceId: descriptor.relayfileWorkspaceId,
        cloudWorkspaceId: descriptor.cloudWorkspaceId,
      }
    }
  } catch {
    // fall through to the built-in default below
  }
  return { workspaceId: DEFAULT_WORKSPACE_ID }
}

export interface RelayfileWorkspaceHandleLike {
  workspaceId?: string
  info: { relayfileUrl: string }
  client(): RelayFileClientLike
  getToken(): Promise<string> | string
  getConnectionStatus?(provider: FactoryIntegrationProvider, connectionId: string): Promise<{
    ready: boolean
    state?: string
    initialSyncState?: string
  }>
  connectIntegration?(provider: FactoryIntegrationProvider, options?: { allowedIntegrations?: string[] }): Promise<{
    alreadyConnected: boolean
    connectLink: string | null
    connectionId: string
  }>
  waitForConnection?(provider: FactoryIntegrationProvider, options?: {
    connectionId?: string
    timeoutMs?: number
  }): Promise<void>
}

export interface MountedWorkspaceHandleLike {
  stop(): Promise<void>
  status?(): Promise<{ ready: boolean }>
  expiresAt?: string | null
  suggestedRefreshAt?: string | null
}

export interface LocalMountHealthEvent {
  state: 'degraded' | 'recovered'
  // `mount_auth_scope` is terminal: the cloud session lacks the filesystem
  // scope the mount needs, so — unlike `mount_stale`/`mount_refresh_failed` —
  // the supervisor stops retrying rather than looping against a doomed refresh.
  reason: 'mount_stale' | 'mount_refresh_failed' | 'mount_auth_scope'
  degradedMounts: number
}

export interface RelayfileSetupLike {
  joinWorkspace(workspaceId: string, options?: { agentName?: string; scopes?: string[] }): Promise<RelayfileWorkspaceHandleLike>
  ensureMountedWorkspace?(input: {
    workspace: RelayfileWorkspaceHandleLike
    localDir: string
    remotePath?: string
    mode?: 'poll' | 'fuse'
    background?: boolean
    agentName?: string
    scopes?: string[]
    verifyProvider?: boolean
    supervise?: boolean
    readyTimeoutMs?: number
  }): Promise<MountedWorkspaceHandleLike>
}

export type RelayfileSetupFactory = (options: {
  cloudApiUrl: string
  tokenProvider: () => Promise<string>
}) => RelayfileSetupLike

export type LocalMountPreflight = (
  workspaceId: string,
  startDir: string,
  options: EnsureLocalMountOptions,
) => Promise<void>

export interface RelayfileCloudMountClientConfig {
  backend?: 'relayfile-cloud'
  workspaceId?: string
  cloudApiUrl?: string
  /**
   * Non-interactive host credential used only for the Cloud workspace join.
   * Hosted runtimes inject a rotating, fixed RelayAuth path-token provider;
   * when present, Factory never reads the local Cloud login or shells out.
   */
  cloudAccessTokenProvider?: () => Promise<string>
  /** Private host endpoint that returns the current fixed RelayAuth access token. */
  cloudAccessTokenUrl?: string
  /** Internal fetch override for hosted credential-provider tests. */
  cloudAccessTokenFetch?: typeof fetch
  /** Internal hosted credential request timeout override. */
  cloudAccessTokenTimeoutMs?: number
  cloudSessionProvider?: CloudSessionProvider
  cloudSessionRefreshTimeoutMs?: number
  cloudSessionEnv?: NodeJS.ProcessEnv
  relayfileSetupFactory?: RelayfileSetupFactory
  /** Internal SDK objects retained by fromConfig() for local mount startup. */
  relayfileSetup?: RelayfileSetupLike
  relayfileWorkspace?: RelayfileWorkspaceHandleLike
  localMountPreflight?: LocalMountPreflight
  agentName?: string
  scopes?: string[]
  client?: RelayFileClientLike
  tokenProvider?: TokenProvider
  baseUrl?: string
  eventClient?: RelayfileEventClient
  /** Override the standard Relayfile SDK durable-subscription adapter. */
  resourceSubscriptions?: ResourceSubscriptionsClient
  /** Optional lifecycle cancellation forwarded through the Relayfile SDK. */
  resourceSubscriptionSignal?: AbortSignal
  logger?: Logger
  onLocalMountHealth?: (event: LocalMountHealthEvent) => Promise<void> | void
  /** Internal health cadence override for tests. */
  localMountHealthIntervalMs?: number
  /** Internal mount-work concurrency override for tests. */
  localMountMaxConcurrency?: number
  /** Explicit registered mirror root; never inferred from a routed checkout. */
  localMountRoot?: string
  /** Read-only registration lookup override for tests and alternate runtimes. */
  workspaceMirrorResolver?: (workspaceIds: readonly string[]) => string | undefined
  /** Internal: fromConfig already attempted the registration lookup, including no-match. */
  skipRegisteredMirrorLookup?: boolean
  isAllowedDraft?: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>
  isAllowedDelete?: (path: string, currentContent: unknown) => boolean | Promise<boolean>
}

export type RelayFileClientLike = {
    readFile(workspaceId: string, path: string): Promise<FileReadResponse>
    writeFile(input: WriteFileInput): Promise<WriteQueuedResponse>
    deleteFile(input: DeleteFileInput): Promise<WriteQueuedResponse>
    listTree(workspaceId: string, options?: ListTreeOptions): Promise<TreeResponse>
    getEvents(workspaceId: string, options?: GetEventsOptions): Promise<EventFeedResponse>
    listLastNChanges?(limit: number, context?: { workspaceId: string; token?: string }): Promise<{ events: ChangeEvent[] }>
    getResourceAtEvent(eventId: string, context?: { workspaceId: string; token?: string }): Promise<ResourceAtEventResult>
    getOp?(workspaceId: string, opId: string): Promise<OperationStatusResponse>
    getSyncStatus?(workspaceId: string, options?: { provider?: string }): Promise<unknown>
    getToken?(): Promise<string> | string
    getBaseUrl?(): string
  } & Partial<ResourceSubscriptionsSdk>

const hasResourceSubscriptionsSdk = (
  client: RelayFileClientLike,
): client is RelayFileClientLike & ResourceSubscriptionsSdk =>
  typeof client.createOrRenewDurableResourceSubscription === 'function' &&
  typeof client.claimDurableSubscriptionDeliveries === 'function' &&
  typeof client.acceptDurableSubscriptionDelivery === 'function' &&
  typeof client.cancelDurableResourceSubscription === 'function'

export function relayfileWorkspaceTokenProvider(
  client: RelayFileClientLike,
  workspace: RelayfileWorkspaceHandleLike,
): TokenProvider {
  return async () => {
    const current = await client.getToken?.()
    return current ?? await workspace.getToken()
  }
}

export class RelayfileCloudMountClient implements MountClient {
  readonly workspaceId: string
  readonly writebackTransport = 'relayfile-cloud'
  readonly githubRead?: GithubConnectionRead
  readonly githubWrite: GithubConnectionWrite
  readonly resourceSubscriptions?: ResourceSubscriptionsClient
  readonly integrationConnections?: FactoryIntegrationConnections

  readonly #client: RelayFileClientLike
  readonly #tokenProvider: TokenProvider
  readonly #baseUrl?: string
  readonly #eventClient?: RelayfileEventClient
  readonly #logger?: Logger
  readonly #onLocalMountHealth?: (event: LocalMountHealthEvent) => Promise<void> | void
  readonly #localMountHealthIntervalMs: number
  readonly #localMountMaxConcurrency: number
  readonly #relayfileSetup?: RelayfileSetupLike
  readonly #relayfileWorkspace?: RelayfileWorkspaceHandleLike
  readonly #localMountPreflight: LocalMountPreflight
  readonly #localMountAgentName: string
  readonly #localMountScopes: string[]
  #localMountRoot?: string
  readonly #localMounts = new Map<string, MountedWorkspaceHandleLike>()
  readonly #localMountSupervisions = new Map<string, {
    startDir: string
    options: LocalMountOptions
    launch: () => Promise<MountedWorkspaceHandleLike>
    suggestedRefreshAtMs?: number
  }>()
  readonly #localMountHealthTimers = new Map<string, ReturnType<typeof setTimeout>>()
  readonly #localMountOperations = new Map<string, Promise<void>>()
  readonly #localMountOperationWaiters: Array<() => void> = []
  readonly #degradedLocalMounts = new Set<string>()
  // Mounts that failed with a terminal auth-scope shortfall. These are NOT
  // rescheduled for refresh (a re-launch just 403s again); recovery requires
  // re-authenticating and restarting Factory.
  readonly #authDegradedLocalMounts = new Set<string>()
  #activeLocalMountOperations = 0
  #disposed = false
  #isAllowedDraft?: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>
  readonly #isAllowedDelete?: (path: string, currentContent: unknown) => boolean | Promise<boolean>
  readonly #lastOpByPath = new Map<string, string>()
  readonly #confirmedExternalIdByPath = new Map<string, string>()
  readonly #confirmedFailureReasonByPath = new Map<string, string>()

  constructor(config: RelayfileCloudMountClientConfig = {}) {
    if (!config.client) {
      throw new Error('RelayfileCloudMountClient requires a client; use fromConfig() to load cloud credentials')
    }

    this.workspaceId = config.workspaceId ?? DEFAULT_WORKSPACE_ID
    this.#client = config.client
    this.#tokenProvider = config.tokenProvider ?? (() => this.#client.getToken?.())
    this.#baseUrl = config.baseUrl ?? this.#client.getBaseUrl?.()
    this.#eventClient = config.eventClient
    this.resourceSubscriptions = config.resourceSubscriptions ?? (
      hasResourceSubscriptionsSdk(this.#client)
        ? createResourceSubscriptionsSdkClient(this.#client, {
          signal: config.resourceSubscriptionSignal,
        })
        : undefined
    )
    this.#logger = config.logger
    this.#onLocalMountHealth = config.onLocalMountHealth
    this.#localMountHealthIntervalMs = Math.max(
      1_000,
      Math.floor(config.localMountHealthIntervalMs ?? DEFAULT_LOCAL_MOUNT_HEALTH_INTERVAL_MS),
    )
    this.#localMountMaxConcurrency = Math.max(
      1,
      Math.floor(config.localMountMaxConcurrency ?? DEFAULT_LOCAL_MOUNT_MAX_CONCURRENCY),
    )
    this.#relayfileSetup = config.relayfileSetup
    this.#relayfileWorkspace = config.relayfileWorkspace
    this.#localMountPreflight = config.localMountPreflight ?? runLocalMountPreflight
    this.#localMountAgentName = config.agentName ?? DEFAULT_AGENT_NAME
    this.#localMountScopes = config.scopes ?? [...FACTORY_RELAYFILE_SCOPES]
    const workspaceIds = this.#acceptableWorkspaceIds([this.workspaceId])
    this.#localMountRoot = config.localMountRoot ?? (config.skipRegisteredMirrorLookup
      ? undefined
      : config.workspaceMirrorResolver?.(workspaceIds) ??
        resolveRegisteredWorkspaceMirror(workspaceIds)?.localDir)
    this.#isAllowedDraft = config.isAllowedDraft
    this.#isAllowedDelete = config.isAllowedDelete
    this.githubRead = new GithubApiIssueRead()
    this.githubWrite = new RelayfileGithubConnectionWrite({ mount: this })
    this.integrationConnections = relayfileIntegrationConnections(
      config.relayfileWorkspace,
      this.workspaceId,
    )
  }

  setDefaultAllowedDraftPredicate(
    predicate: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>,
  ): void {
    this.#isAllowedDraft ??= predicate
  }

  static async fromConfig(config: RelayfileCloudMountClientConfig = {}): Promise<RelayfileCloudMountClient> {
    if ('credsPath' in config) {
      throw new Error('RelayfileCloudMountClient no longer accepts credsPath; run `agent-relay login` to use the shared cloud session')
    }
    if (config.client) return new RelayfileCloudMountClient(config)

    const workspaceId = config.workspaceId ?? DEFAULT_WORKSPACE_ID
    const runtimeEnv = config.cloudSessionEnv ?? process.env
    const hostedAccessTokenUrl = config.cloudAccessTokenUrl
      ?? runtimeEnv[FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV]?.trim()
    const hostedTokenProvider = !config.cloudAccessTokenProvider && hostedAccessTokenUrl
      ? createHostedCloudAccessTokenProvider({
          url: hostedAccessTokenUrl,
          fetchImpl: config.cloudAccessTokenFetch ?? fetch,
          timeoutMs: config.cloudAccessTokenTimeoutMs ?? DEFAULT_HOSTED_ACCESS_TOKEN_TIMEOUT_MS,
        })
      : undefined
    const directTokenProvider = config.cloudAccessTokenProvider ?? hostedTokenProvider
    const sharedSession = directTokenProvider ? undefined : createSharedCloudSessionResolver(config)
    const initialSession = sharedSession ? await sharedSession.resolve() : undefined
    const cloudApiUrl = config.cloudApiUrl
      ?? initialSession?.auth.apiUrl
      ?? (hostedTokenProvider ? (runtimeEnv.CLOUD_API_URL?.trim() || defaultApiUrl()) : undefined)
    if (!cloudApiUrl) {
      throw new Error('Relayfile hosted access requires cloudApiUrl with cloudAccessTokenProvider')
    }
    const tokenProvider = directTokenProvider ?? sharedSession?.getAccessToken
    if (!tokenProvider) {
      throw new Error('Relayfile hosted access token provider is unavailable')
    }
    const setup = (config.relayfileSetupFactory ?? createDefaultRelayfileSetup)({
      cloudApiUrl,
      tokenProvider,
    })
    const handle = await setup.joinWorkspace(workspaceId, {
      agentName: config.agentName ?? DEFAULT_AGENT_NAME,
      scopes: config.scopes ?? [...FACTORY_RELAYFILE_SCOPES],
    })
    const client = handle.client()

    const registeredMountRoot = config.localMountRoot ??
      config.workspaceMirrorResolver?.([workspaceId, handle.workspaceId ?? '']) ??
      resolveRegisteredWorkspaceMirror([workspaceId, handle.workspaceId ?? ''])?.localDir
    return new RelayfileCloudMountClient({
      ...config,
      workspaceId,
      ...(registeredMountRoot ? { localMountRoot: registeredMountRoot } : {}),
      skipRegisteredMirrorLookup: true,
      client,
      // WorkspaceHandle.getToken() returns the token originally minted by
      // joinWorkspace. RelayFileClient.getToken() resolves the SDK's rotating
      // provider, so long-lived websocket/poll subscriptions must use it.
      tokenProvider: relayfileWorkspaceTokenProvider(client, handle),
      baseUrl: handle.info.relayfileUrl,
      relayfileSetup: setup,
      relayfileWorkspace: handle,
    })
  }

  async ensureLocalMount(startDir: string, options: LocalMountOptions = {}): Promise<void> {
    // A Relayfile workspace has one registered local mirror.  Do not derive a
    // new one from every routed checkout: that asks Relayfile to re-home the
    // workspace and makes all but the first route fail.  The fallback is only
    // invoked once per Factory command rather than once per repository.
    const canDiscoverRegisteredMirror = this.#localMountRoot === undefined
    const localDir = this.#localMountRoot ?? join(resolve(startDir), '.integrations')
    this.#localMountRoot = localDir
    return this.#runLocalMountOperation(
      localDir,
      () => this.#ensureLocalMountWithRegisteredFallback(localDir, options, canDiscoverRegisteredMirror),
    )
  }

  getLocalMountRoot(): string | undefined {
    return this.#localMountRoot
  }

  getLocalMountHealth(): LocalMountHealth {
    const localDir = this.#localMountRoot
    if (!localDir) {
      return { degraded: true, reason: 'Relayfile workspace mirror is not registered' }
    }
    const statePath = join(localDir, '.relay', 'state.json')
    if (!existsSync(statePath)) {
      return { degraded: true, reason: `mount state is missing at ${statePath}`, localDir }
    }
    const staleness = checkMountStaleness(statePath, this.workspaceId, this.#acceptableWorkspaceIds())
    return {
      degraded: staleness.stale,
      ...(staleness.reason ? { reason: staleness.reason } : {}),
      localDir,
    }
  }

  async #ensureLocalMount(localDir: string, options: LocalMountOptions): Promise<void> {
    const setup = this.#relayfileSetup
    const workspace = this.#relayfileWorkspace
    if (!setup?.ensureMountedWorkspace || !workspace) {
      throw new Error('Relayfile SDK mount setup is unavailable; construct the mount with RelayfileCloudMountClient.fromConfig()')
    }
    const ensureMountedWorkspace = setup.ensureMountedWorkspace.bind(setup)

    const acceptableWorkspaceIds = new Set(this.#acceptableWorkspaceIds(options.acceptableWorkspaceIds))
    const launch = (): Promise<MountedWorkspaceHandleLike> => ensureMountedWorkspace({
      workspace,
      localDir,
      remotePath: '/',
      mode: 'poll',
      background: true,
      agentName: this.#localMountAgentName,
      scopes: [...this.#localMountScopes],
      // Factory mirrors the whole workspace across several integrations,
      // so there is no single provider to verify before mounting.
      verifyProvider: false,
      // Factory owns the outer, telemetry-aware supervisor. Newer Relayfile
      // SDKs also supervise by default, so opt out here to avoid two refresh
      // loops racing over the same local directory.
      supervise: false,
      ...(options.stateWaitTimeoutMs === undefined ? {} : { readyTimeoutMs: options.stateWaitTimeoutMs }),
    })
    this.#localMountSupervisions.set(localDir, {
      startDir: join(localDir, '..'),
      options: { ...options, acceptableWorkspaceIds: [...acceptableWorkspaceIds] },
      launch,
      suggestedRefreshAtMs: this.#localMountSupervisions.get(localDir)?.suggestedRefreshAtMs,
    })

    const statePath = join(localDir, '.relay', 'state.json')
    const staleBefore = existsSync(statePath)
      ? checkMountStaleness(statePath, this.workspaceId, [...acceptableWorkspaceIds])
      : undefined
    if (staleBefore?.stale) this.#markLocalMountDegraded(localDir, 'mount_stale')

    try {
      await this.#localMountPreflight(this.workspaceId, join(localDir, '..'), {
        ...options,
        localDir,
        acceptableWorkspaceIds: [...acceptableWorkspaceIds],
        startMount: async () => {
          await this.#replaceLocalMount(localDir, launch)
        },
      })
    } catch (error) {
      // A terminal auth-scope shortfall cannot be healed by re-launching, so
      // mark it distinctly and do NOT arm the retry supervisor — just surface it
      // to startup, which fails fast with the remediation.
      if (error instanceof MountAuthScopeError) {
        this.#markLocalMountAuthDegraded(localDir)
        throw error
      }
      // A first-ever mount has no state file from which staleness can be
      // inferred. Surface the launch failure and arm the same retry supervisor
      // used for later stale sessions before returning control to startup.
      this.#markLocalMountDegraded(localDir, 'mount_refresh_failed')
      this.#scheduleLocalMountHealthCheck(localDir)
      throw error
    }
    const staleAfter = checkMountStaleness(statePath, this.workspaceId, [...acceptableWorkspaceIds])
    if (staleAfter.stale) this.#markLocalMountDegraded(localDir, 'mount_refresh_failed')
    else this.#markLocalMountRecovered(localDir)
    this.#scheduleLocalMountHealthCheck(localDir)
  }

  async #ensureLocalMountWithRegisteredFallback(
    localDir: string,
    options: LocalMountOptions,
    canDiscoverRegisteredMirror: boolean,
  ): Promise<void> {
    try {
      await this.#ensureLocalMount(localDir, options)
      return
    } catch (error) {
      // Older Relayfile installations do not persist the registration in a
      // local file we can read. The mount admission response is nevertheless
      // authoritative about the already-registered root. This retry remains
      // inside the shared operation, so concurrent callers all await the same
      // recovery rather than racing to re-home a checkout.
      const registeredRoot = canDiscoverRegisteredMirror ? registeredMirrorFromMountError(error) : undefined
      if (!registeredRoot || registeredRoot === localDir) throw error
      this.#clearLocalMountHealthCheck(localDir)
      this.#localMountSupervisions.delete(localDir)
      this.#degradedLocalMounts.delete(localDir)
      this.#authDegradedLocalMounts.delete(localDir)
      await this.#ensureLocalMount(registeredRoot, options)
      this.#localMountRoot = registeredRoot
    }
  }

  #acceptableWorkspaceIds(extra: readonly string[] = []): string[] {
    const workspace = this.#relayfileWorkspace?.workspaceId
    return [...new Set([...extra, ...(workspace && workspace !== this.workspaceId ? [workspace] : [])])]
  }

  #clearLocalMountHealthCheck(localDir: string): void {
    const timer = this.#localMountHealthTimers.get(localDir)
    if (timer) clearTimeout(timer)
    this.#localMountHealthTimers.delete(localDir)
  }

  #runLocalMountOperation(localDir: string, operation: () => Promise<void>): Promise<void> {
    const existing = this.#localMountOperations.get(localDir)
    if (existing) return existing
    const running = (async () => {
      await this.#acquireLocalMountOperation()
      try {
        if (!this.#disposed) await operation()
      } finally {
        this.#releaseLocalMountOperation()
      }
    })()
    const tracked = running.finally(() => {
      if (this.#localMountOperations.get(localDir) === tracked) {
        this.#localMountOperations.delete(localDir)
      }
    })
    this.#localMountOperations.set(localDir, tracked)
    return tracked
  }

  async #acquireLocalMountOperation(): Promise<void> {
    if (this.#activeLocalMountOperations < this.#localMountMaxConcurrency) {
      this.#activeLocalMountOperations += 1
      return
    }
    await new Promise<void>((resolve) => this.#localMountOperationWaiters.push(resolve))
  }

  #releaseLocalMountOperation(): void {
    const next = this.#localMountOperationWaiters.shift()
    if (next) next()
    else this.#activeLocalMountOperations -= 1
  }

  async dispose(): Promise<void> {
    this.#disposed = true
    for (const timer of this.#localMountHealthTimers.values()) clearTimeout(timer)
    this.#localMountHealthTimers.clear()
    this.#localMountSupervisions.clear()
    this.#degradedLocalMounts.clear()
    this.#authDegradedLocalMounts.clear()
    const mounted = [...this.#localMounts.values()]
    this.#localMounts.clear()
    await Promise.allSettled(mounted.map(async (handle) => handle.stop()))
  }

  async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
    const response = await this.#client.readFile(this.workspaceId, path)
    return {
      content: parseRemoteContent(response),
      revision: response.revision,
    }
  }

  async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
    if (isProviderWritebackPath(path) && await this.#isAllowedDraft?.(path, content, opts) !== true) {
      throw new Error(`Refusing provider writeback draft for ${path}: draft predicate rejected or is unset`)
    }

    const serialized = serializeContent(content)
    this.#confirmedExternalIdByPath.delete(path)
    this.#confirmedFailureReasonByPath.delete(path)

    const writeAtCurrentRevision = async (): Promise<WriteQueuedResponse> => {
      let baseRevision = '0'
      try {
        baseRevision = (await this.#client.readFile(this.workspaceId, path)).revision
      } catch (error) {
        if (!isHttpStatus(error, 404)) throw error
      }

      return this.#client.writeFile({
        workspaceId: this.workspaceId,
        path,
        baseRevision,
        content: serialized.content,
        contentType: serialized.contentType,
      })
    }

    try {
      this.#lastOpByPath.set(path, (await writeAtCurrentRevision()).opId)
    } catch (error) {
      if (!isHttpStatus(error, 409)) throw error
      this.#lastOpByPath.set(path, (await writeAtCurrentRevision()).opId)
    }
  }

  async deleteFile(path: string): Promise<void> {
    this.#confirmedExternalIdByPath.delete(path)
    this.#confirmedFailureReasonByPath.delete(path)
    const current = await this.#client.readFile(this.workspaceId, path)
    const currentContent = parseRemoteContent(current)
    if (isProviderPath(path)) {
      await this.#assertProviderDeleteAllowed(path, currentContent)
    }

    this.#lastOpByPath.set(path, (await this.#client.deleteFile({
      workspaceId: this.workspaceId,
      path,
      baseRevision: current.revision,
    })).opId)
  }

  async #assertProviderDeleteAllowed(path: string, currentContent: unknown): Promise<void> {
    if (providerContentLooksLinked(currentContent)) {
      throw new Error(`Refusing provider delete for ${path}: current record is reconciled or linked`)
    }

    const opId = this.#lastOpByPath.get(path)
    if (!opId || !this.#client.getOp) {
      throw new Error(`Refusing provider delete for ${path}: create operation is unknown`)
    }

    let op: OperationStatusResponse
    try {
      op = await this.#client.getOp(this.workspaceId, opId)
    } catch (error) {
      throw new Error(`Refusing provider delete for ${path}: unable to verify create operation: ${errorMessage(error)}`)
    }

    const providerResult = op.providerResult
    if (typeof providerResult?.externalId === 'string' && providerResult.externalId.length > 0) {
      throw new Error(`Refusing provider delete for ${path}: create operation linked provider object ${providerResult.externalId}`)
    }

    if (op.status !== 'failed' && op.status !== 'dead_lettered' && op.status !== 'canceled') {
      throw new Error(`Refusing provider delete for ${path}: create operation status is ${op.status}`)
    }

    if (await this.#isAllowedDelete?.(path, currentContent) !== true) {
      throw new Error(`Refusing provider delete for ${path}: delete predicate rejected or is unset`)
    }
  }

  async listTree(prefix: string): Promise<string[]> {
    const paths: string[] = []
    let cursor: string | undefined
    for (;;) {
      const response = await this.#client.listTree(this.workspaceId, {
        path: prefix,
        cursor,
      })
      paths.push(...response.entries.map((entry) => entry.path))
      if (!response.nextCursor) break
      cursor = response.nextCursor
    }
    return paths.sort()
  }

  subscribe(globs: string[], onChange: (event: ChangeEvent) => void, opts?: SubscribeOptions): Subscription {
    const eventClient = this.#eventClient ?? createWorkspaceScopedEventClient(
      this.#client as WorkspaceEventClientSource,
      this.workspaceId,
      this.#tokenProvider,
      this.#baseUrl,
    )
    return eventClient.subscribe(globs, onChange as Parameters<RelayfileEventClient['subscribe']>[1], opts)
  }

  async #replaceLocalMount(
    localDir: string,
    launch: () => Promise<MountedWorkspaceHandleLike>,
  ): Promise<void> {
    const previous = this.#localMounts.get(localDir)
    if (previous) {
      this.#localMounts.delete(localDir)
      await previous.stop()
    }
    const mounted = await launch()
    if (this.#disposed) {
      await mounted.stop()
      return
    }
    this.#localMounts.set(localDir, mounted)
    const supervision = this.#localMountSupervisions.get(localDir)
    if (supervision) {
      const suggestedRefreshAtMs = Date.parse(mounted.suggestedRefreshAt ?? '')
      supervision.suggestedRefreshAtMs = Number.isFinite(suggestedRefreshAtMs)
        ? suggestedRefreshAtMs
        : undefined
    }
  }

  #scheduleLocalMountHealthCheck(localDir: string): void {
    if (this.#disposed || this.#localMountHealthTimers.has(localDir)) return
    // A terminal auth-scope failure is never retried — recovery requires
    // re-auth + restart, not another refresh cycle.
    if (this.#authDegradedLocalMounts.has(localDir)) return
    const supervision = this.#localMountSupervisions.get(localDir)
    if (!supervision) return
    const untilRefresh = supervision.suggestedRefreshAtMs === undefined
      ? this.#localMountHealthIntervalMs
      : Math.max(0, supervision.suggestedRefreshAtMs - Date.now())
    const delayMs = Math.max(1_000, Math.min(this.#localMountHealthIntervalMs, untilRefresh))
    const timer = setTimeout(() => {
      this.#localMountHealthTimers.delete(localDir)
      void this.#superviseLocalMount(localDir)
    }, delayMs)
    timer.unref?.()
    this.#localMountHealthTimers.set(localDir, timer)
  }

  async #superviseLocalMount(localDir: string): Promise<void> {
    if (this.#disposed) return
    const supervision = this.#localMountSupervisions.get(localDir)
    if (!supervision) return
    try {
      if (
        supervision.suggestedRefreshAtMs !== undefined &&
        Date.now() >= supervision.suggestedRefreshAtMs
      ) {
        await this.#runLocalMountOperation(localDir, async () => {
          await this.#replaceLocalMount(localDir, supervision.launch)
          this.#markLocalMountRecovered(localDir)
        })
      } else {
        // ensureLocalMount performs the filesystem health check and owns the
        // resulting degraded/recovered transition.
        await this.ensureLocalMount(supervision.startDir, supervision.options)
      }
    } catch (error) {
      if (error instanceof MountAuthScopeError) {
        // Terminal: stop the retry loop for this mount entirely.
        this.#markLocalMountAuthDegraded(localDir)
        return
      }
      this.#logger?.warn?.('[factory] supervised Relayfile mount refresh failed', {
        errorClass: error instanceof Error ? error.name : 'Error',
      })
      this.#markLocalMountDegraded(localDir, 'mount_refresh_failed')
    } finally {
      // Never re-arm the supervisor for a terminally auth-degraded mount.
      if (!this.#authDegradedLocalMounts.has(localDir)) {
        this.#scheduleLocalMountHealthCheck(localDir)
      }
    }
  }

  #markLocalMountDegraded(localDir: string, reason: LocalMountHealthEvent['reason']): void {
    const wasHealthy = this.#degradedLocalMounts.size === 0
    this.#degradedLocalMounts.add(localDir)
    if (!wasHealthy) return
    this.#emitLocalMountHealth({
      state: 'degraded',
      reason,
      degradedMounts: this.#degradedLocalMounts.size,
    })
  }

  /**
   * Whether any local mount is terminally degraded by a filesystem-scope
   * shortfall. Consumers (e.g. the factory loop) use this to refuse to spawn or
   * resume agents against a read-denied mirror.
   */
  isLocalMountAuthDegraded(): boolean {
    return this.#authDegradedLocalMounts.size > 0
  }

  #markLocalMountAuthDegraded(localDir: string): void {
    // Stop any pending refresh for this mount; it is terminal.
    const timer = this.#localMountHealthTimers.get(localDir)
    if (timer) {
      clearTimeout(timer)
      this.#localMountHealthTimers.delete(localDir)
    }
    const wasHealthy = this.#degradedLocalMounts.size === 0
    const wasAuthDegraded = this.#authDegradedLocalMounts.has(localDir)
    this.#degradedLocalMounts.add(localDir)
    this.#authDegradedLocalMounts.add(localDir)
    // Emit once per transition into the auth-degraded state.
    if (wasHealthy || !wasAuthDegraded) {
      this.#emitLocalMountHealth({
        state: 'degraded',
        reason: 'mount_auth_scope',
        degradedMounts: this.#degradedLocalMounts.size,
      })
    }
  }

  #markLocalMountRecovered(localDir: string): void {
    this.#authDegradedLocalMounts.delete(localDir)
    if (!this.#degradedLocalMounts.delete(localDir) || this.#degradedLocalMounts.size > 0) return
    this.#emitLocalMountHealth({
      state: 'recovered',
      reason: 'mount_stale',
      degradedMounts: 0,
    })
  }

  #emitLocalMountHealth(event: LocalMountHealthEvent): void {
    void Promise.resolve(this.#onLocalMountHealth?.(event)).catch((error: unknown) => {
      this.#logger?.warn?.('[factory] unable to report Relayfile mount health', {
        errorClass: error instanceof Error ? error.name : 'Error',
      })
    })
  }

  async getEvents(opts: { cursor?: string; limit?: number; provider?: string; last?: number }): Promise<EventPage> {
    if (opts.last !== undefined && this.#client.listLastNChanges) {
      const response = await this.#client.listLastNChanges(opts.last, { workspaceId: this.workspaceId })
      const events = opts.provider
        ? response.events.filter((event) => eventProvider(event) === opts.provider)
        : response.events
      return {
        events: events.slice(0, opts.limit ?? events.length) as unknown as EventPage['events'],
        nextCursor: null,
      }
    }
    const response = await this.#client.getEvents(this.workspaceId, opts)
    return {
      events: response.events as unknown as EventPage['events'],
      nextCursor: response.nextCursor,
    }
  }

  async getEventHighWatermark(opts: { provider?: string } = {}): Promise<string | undefined> {
    if (!this.#client.listLastNChanges) return undefined
    const response = await this.#client.listLastNChanges(10, { workspaceId: this.workspaceId })
    const events = opts.provider
      ? response.events.filter((event) => event.resource.provider === opts.provider)
      : response.events
    return maxEventId(events.map((event) => event.id))
  }

  async getSyncStatus(provider: string): Promise<ProviderSyncStatus | undefined> {
    if (!this.#client.getSyncStatus) return undefined
    return normalizeProviderSyncStatus(await this.#client.getSyncStatus(this.workspaceId, { provider }), provider)
  }

  async confirmWrite(
    path: string,
    opts: { timeoutMs?: number } = {},
  ): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
    const opId = this.#lastOpByPath.get(path)
    if (!opId || !this.#client.getOp) return 'timeout'

    const deadline = Date.now() + (opts.timeoutMs ?? 90_000)
    for (;;) {
      const operation = await this.#client.getOp(this.workspaceId, opId)
      let status: 'acked' | 'pending' | 'failed'
      try {
        status = mapOperationStatus(operation)
      } catch (error) {
        this.#confirmedFailureReasonByPath.set(path, providerResultError(operation))
        throw error
      }
      if (status !== 'pending') {
        const providerId = [
          operation.providerResult?.externalId,
          operation.providerResult?.ts,
          operation.providerResult?.thread_ts,
        ].find((value): value is string => typeof value === 'string' && value.length > 0)
        if (status === 'acked' && providerId) {
          this.#confirmedExternalIdByPath.set(path, providerId)
        }
        return status
      }
      if (Date.now() >= deadline) return 'timeout'
      await sleep(Math.min(500, Math.max(25, deadline - Date.now())))
    }
  }

  async getConfirmedWriteFailureReason(path: string): Promise<string | undefined> {
    return this.#confirmedFailureReasonByPath.get(path)
  }

  async getConfirmedWriteExternalId(path: string): Promise<string | undefined> {
    return this.#confirmedExternalIdByPath.get(path)
  }

  async ensureSubRoot(prefix: string, _opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'> {
    try {
      await this.#client.listTree(this.workspaceId, { path: prefix, depth: 1 })
      return 'ready'
    } catch (error) {
      if (isHttpStatus(error, 404)) return 'absent'
      throw error
    }
  }
}

function relayfileIntegrationConnections(
  workspace: RelayfileWorkspaceHandleLike | undefined,
  fallbackWorkspaceId: string,
): FactoryIntegrationConnections | undefined {
  if (!workspace?.getConnectionStatus || !workspace.connectIntegration || !workspace.waitForConnection) {
    return undefined
  }

  const connectionId = workspace.workspaceId ?? fallbackWorkspaceId
  const activeConnectionIds = new Map<FactoryIntegrationProvider, string>()
  return {
    getStatus: (provider) => workspace.getConnectionStatus!(
      provider,
      activeConnectionIds.get(provider) ?? connectionId,
    ),
    connect: async (provider) => {
      const result = await workspace.connectIntegration!(provider, {
        allowedIntegrations: [provider],
      })
      activeConnectionIds.set(provider, result.connectionId)
      return result
    },
    waitForConnection: async (provider, requestedConnectionId) => {
      activeConnectionIds.set(provider, requestedConnectionId)
      await workspace.waitForConnection!(provider, {
        connectionId: requestedConnectionId,
        timeoutMs: 5 * 60_000,
      })
    },
  }
}

const createDefaultRelayfileSetup: RelayfileSetupFactory = ({ cloudApiUrl, tokenProvider }) =>
  new RelayfileSetup({
    cloudApiUrl,
    accessToken: tokenProvider,
  }) as unknown as RelayfileSetupLike

const createHostedCloudAccessTokenProvider = (options: {
  url: string
  fetchImpl: typeof fetch
  timeoutMs: number
}): (() => Promise<string>) => {
  let url: URL
  try {
    url = new URL(options.url)
  } catch {
    throw new Error(`${FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV} must be an absolute URL`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${FACTORY_CLOUD_ACCESS_TOKEN_URL_ENV} must use http or https`)
  }
  if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new Error('hosted Cloud access-token timeout must be positive')
  }

  return async (): Promise<string> => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), options.timeoutMs)
    try {
      const response = await options.fetchImpl(url, {
        method: 'GET',
        headers: { accept: 'application/json', 'cache-control': 'no-store' },
        redirect: 'error',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`hosted Cloud access-token provider returned HTTP ${String(response.status)}`)
      }
      const payload = await response.json() as unknown
      const accessToken = payload !== null && typeof payload === 'object' && !Array.isArray(payload)
        && typeof (payload as { accessToken?: unknown }).accessToken === 'string'
        ? (payload as { accessToken: string }).accessToken.trim()
        : ''
      if (!accessToken.startsWith('relay_pa_')) {
        throw new Error('hosted Cloud access-token provider returned an invalid token class')
      }
      return accessToken
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`hosted Cloud access-token provider timed out after ${String(options.timeoutMs)}ms`)
      }
      throw error
    } finally {
      clearTimeout(timer)
    }
  }
}

const createSharedCloudSessionResolver = (config: RelayfileCloudMountClientConfig): {
  resolve: () => Promise<CloudSession>
  getAccessToken: () => Promise<string>
} => {
  const provider = config.cloudSessionProvider ?? ensureCloudSession
  let session: CloudSession | undefined
  let inFlight: Promise<CloudSession> | undefined

  const resolve = async (): Promise<CloudSession> => {
    if (!inFlight) {
      const options: CloudSessionOptions = {
        interactive: false,
      }
      const apiUrl = session?.auth.apiUrl ?? config.cloudApiUrl
      if (apiUrl) options.apiUrl = apiUrl
      if (config.cloudSessionRefreshTimeoutMs !== undefined) options.refreshTimeoutMs = config.cloudSessionRefreshTimeoutMs
      if (config.cloudSessionEnv !== undefined) options.env = config.cloudSessionEnv

      inFlight = provider(options).catch((error) => {
        if (error instanceof CloudAuthError && error.code === 'AUTH_BROWSER_REQUIRED') {
          throw new Error('Relayfile cloud session required; run `agent-relay login`')
        }
        throw error
      }).finally(() => {
        inFlight = undefined
      })
    }
    session = await inFlight
    return session
  }

  return {
    resolve,
    getAccessToken: async () => (await resolve()).auth.accessToken,
  }
}

const parseRemoteContent = (response: FileReadResponse): unknown => {
  if (!response.contentType.includes('json')) return response.content
  try {
    return JSON.parse(response.content)
  } catch {
    return response.content
  }
}

const serializeContent = (content: unknown): { content: string; contentType: string } => {
  if (typeof content === 'string') {
    return { content, contentType: 'text/plain' }
  }
  return {
    content: JSON.stringify(content),
    contentType: 'application/json',
  }
}

/**
 * Relayfile's single-mirror admission check names the registered directory in
 * its refusal. Treat that directory as a read-only registration lookup: the
 * retry asks for the already registered root and never supplies `--rehome`.
 */
function registeredMirrorFromMountError(error: unknown): string | undefined {
  const message = error instanceof Error ? error.message : String(error)
  const match = /\balready mirrored at\s+(.+?)(?:;|\n|$)/iu.exec(message)
  if (!match?.[1]) return undefined
  const localDir = match[1].trim().replace(/^["']|["']$/gu, '')
  return isAbsolute(localDir) ? resolve(localDir) : undefined
}

const isHttpStatus = (error: unknown, status: number): boolean => {
  const record = error !== null && typeof error === 'object' ? error as Record<string, unknown> : undefined
  return record?.status === status || record?.statusCode === status
}

const mapOperationStatus = (
  response: OperationStatusResponse,
): 'acked' | 'pending' | 'failed' => {
  if (response.status === 'succeeded') {
    const providerResult = response.providerResult
    if (
      providerResult &&
      typeof providerResult.status === 'number' &&
      providerResult.status >= 200 &&
      providerResult.status < 300 &&
      typeof providerResult.externalId === 'string' &&
      providerResult.externalId.length > 0
    ) {
      return 'acked'
    }
    throw new Error(`Writeback provider result incomplete for ${response.path ?? response.opId}: ${providerResultError(response)}`)
  }
  if (response.status === 'failed' || response.status === 'dead_lettered' || response.status === 'canceled') {
    throw new Error(`Writeback operation failed for ${response.path ?? response.opId}: ${providerResultError(response)}`)
  }
  return 'pending'
}

const providerResultError = (response: OperationStatusResponse): string => {
  const providerResult = response.providerResult
  const resultError = typeof providerResult?.lastError === 'string'
    ? providerResult.lastError
    : typeof providerResult?.error === 'string'
      ? providerResult.error
      : typeof providerResult?.message === 'string'
        ? providerResult.message
        : undefined
  return resultError ?? response.lastError ?? 'unknown provider error'
}

const normalizeProviderSyncStatus = (value: unknown, provider: string): ProviderSyncStatus | undefined => {
  const record = value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
  if (!record) return undefined
  const candidates = [
    record,
    asRecord(record[provider]),
    asRecord(record.integration),
    asRecord(record.connection),
    ...arrayRecords(record.providers),
    ...arrayRecords(record.integrations),
    ...arrayRecords(record.connections),
    ...arrayRecords(record.items),
  ].filter((candidate): candidate is Record<string, unknown> =>
    candidate !== undefined &&
    (stringField(candidate, 'provider') === undefined || stringField(candidate, 'provider') === provider))
  const source = candidates.find(hasProviderSyncFreshness) ??
    candidates.find((candidate) =>
      stringField(candidate, 'status')) ?? record
  const lastEventAt = stringField(source, 'lastEventAt') ??
    stringField(source, 'last_event_at') ??
    stringField(source, 'watermarkAt') ??
    stringField(source, 'watermark_at') ??
    stringField(source, 'watermarkTs') ??
    stringField(source, 'watermark_ts')
  const lastEventAtMs = numberField(source, 'lastEventAtMs') ??
    numberField(source, 'last_event_at_ms') ??
    (lastEventAt ? Date.parse(lastEventAt) : undefined)
  const watermarkTs = stringField(source, 'watermarkTs') ?? stringField(source, 'watermark_ts') ?? lastEventAt
  // Webhook health is independent of sync freshness and may be reported on a
  // wrapper while the provider watermark lives on a nested connection. Keep
  // freshness source selection focused on sync fields, then reconcile health
  // across every applicable shape. A reported false wins conservatively: a
  // false-positive healthy result would send questions to an unread Slack.
  const webhookHealthySignals = [source, ...candidates]
    .map((candidate) =>
      booleanField(candidate, 'webhookHealthy') ?? booleanField(candidate, 'webhook_healthy'))
    .filter((signal): signal is boolean => signal !== undefined)
  return {
    provider: stringField(source, 'provider') ?? provider,
    status: stringField(source, 'status'),
    lastEventAt,
    lastEventAtMs: Number.isFinite(lastEventAtMs) ? lastEventAtMs : undefined,
    watermarkTs,
    lagSeconds: numberField(source, 'lagSeconds') ?? numberField(source, 'lag_seconds'),
    webhookHealthy: webhookHealthySignals.includes(false) ? false : webhookHealthySignals[0],
  }
}

const hasProviderSyncFreshness = (candidate: Record<string, unknown>): boolean => Boolean(
    stringField(candidate, 'lastEventAt') ||
    stringField(candidate, 'last_event_at') ||
    stringField(candidate, 'watermarkAt') ||
    stringField(candidate, 'watermark_at') ||
    stringField(candidate, 'watermarkTs') ||
    stringField(candidate, 'watermark_ts') ||
    numberField(candidate, 'lastEventAtMs') !== undefined ||
    numberField(candidate, 'last_event_at_ms') !== undefined ||
    numberField(candidate, 'lagSeconds') !== undefined ||
    numberField(candidate, 'lag_seconds') !== undefined
)

const stringField = (record: Record<string, unknown>, key: string): string | undefined =>
  typeof record[key] === 'string' ? record[key] : undefined

const numberField = (record: Record<string, unknown>, key: string): number | undefined =>
  typeof record[key] === 'number' ? record[key] : undefined

const booleanField = (record: Record<string, unknown>, key: string): boolean | undefined =>
  typeof record[key] === 'boolean' ? record[key] : undefined

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined

const eventProvider = (event: ChangeEvent | unknown): string | undefined => {
  const record = asRecord(event) ?? {}
  return stringField(asRecord(record.resource) ?? {}, 'provider') ?? stringField(record, 'provider')
}

const arrayRecords = (value: unknown): Array<Record<string, unknown>> =>
  Array.isArray(value) ? value.map(asRecord).filter((entry): entry is Record<string, unknown> => entry !== undefined) : []

const isProviderWritebackPath = (path: string): boolean =>
  path.startsWith('/linear/issues/') ||
  path.startsWith('/linear/comments/') ||
  path.startsWith('/github/repos/') ||
  /^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)

const isProviderPath = (path: string): boolean =>
  path.startsWith('/linear/') || path.startsWith('/github/') || path.startsWith('/slack/')

const providerContentLooksLinked = (content: unknown): boolean => {
  const record = content !== null && typeof content === 'object' && !Array.isArray(content)
    ? content as Record<string, unknown>
    : {}
  const payload = record.payload !== null && typeof record.payload === 'object' && !Array.isArray(record.payload)
    ? record.payload as Record<string, unknown>
    : record
  const url = payload.url
  if (typeof url === 'string' && url.trim().length > 0) return true
  const identifier = payload.identifier
  return typeof identifier === 'string' && /^[A-Z][A-Z0-9]*-\d+$/u.test(identifier)
}

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const maxEventId = (ids: string[]): string | undefined => {
  let max: string | undefined
  for (const id of ids) {
    if (!max || compareEventIds(id, max) > 0) {
      max = id
    }
  }
  return max
}

const compareEventIds = (left: string, right: string): number => {
  const leftSequence = eventSequenceNumber(left)
  const rightSequence = eventSequenceNumber(right)
  if (leftSequence !== undefined && rightSequence !== undefined) {
    return leftSequence - rightSequence
  }
  return left.localeCompare(right)
}

const eventSequenceNumber = (eventId: string): number | undefined => {
  const whole = Number(eventId)
  if (Number.isFinite(whole)) return whole
  const trailing = eventId.match(/(\d+)$/u)?.[1]
  if (!trailing) return undefined
  const parsed = Number(trailing)
  return Number.isFinite(parsed) ? parsed : undefined
}
