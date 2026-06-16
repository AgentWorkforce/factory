import {
  CloudAuthError,
  ensureCloudSession,
  resolveActiveWorkspace,
  type ActiveWorkspaceDescriptor,
  type CloudSession,
  type CloudSessionOptions,
} from '@agent-relay/cloud'
import {
  RelayfileSetup,
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

import type { EventPage, MountClient, ProviderSyncStatus, SubscribeOptions } from '../ports'
import {
  createWorkspaceScopedEventClient,
  type RelayfileEventClient,
  type TokenProvider,
  type WorkspaceEventClientSource,
} from '../subscriptions'

const DEFAULT_WORKSPACE_ID = 'rw_7ccfea89'
const DEFAULT_AGENT_NAME = 'pear-factory-sdk'
export const FACTORY_RELAYFILE_SCOPES = [
  'relayfile:fs:read:/linear/issues/**',
  'relayfile:fs:write:/linear/issues/**',
  'relayfile:fs:read:/github/repos/**',
  'relayfile:fs:read:/slack/channels/**',
  'relayfile:fs:write:/slack/channels/**',
  'relayfile:fs:read:/slack/users/**',
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
  info: { relayfileUrl: string }
  client(): RelayFileClientLike
  getToken(): Promise<string> | string
}

export type RelayfileSetupFactory = (options: {
  cloudApiUrl: string
  tokenProvider: () => Promise<string>
}) => {
  joinWorkspace(workspaceId: string, options?: { agentName?: string; scopes?: string[] }): Promise<RelayfileWorkspaceHandleLike>
}

export interface RelayfileCloudMountClientConfig {
  backend?: 'relayfile-cloud'
  workspaceId?: string
  cloudApiUrl?: string
  cloudSessionProvider?: CloudSessionProvider
  cloudSessionRefreshTimeoutMs?: number
  cloudSessionEnv?: NodeJS.ProcessEnv
  relayfileSetupFactory?: RelayfileSetupFactory
  agentName?: string
  scopes?: string[]
  client?: RelayFileClientLike
  tokenProvider?: TokenProvider
  baseUrl?: string
  eventClient?: RelayfileEventClient
  isAllowedDraft?: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>
  isAllowedDelete?: (path: string, currentContent: unknown) => boolean | Promise<boolean>
}

export type RelayFileClientLike =
  {
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
  }

export class RelayfileCloudMountClient implements MountClient {
  readonly workspaceId: string
  readonly writebackTransport = 'relayfile-cloud'

  readonly #client: RelayFileClientLike
  readonly #tokenProvider: TokenProvider
  readonly #baseUrl?: string
  readonly #eventClient?: RelayfileEventClient
  #isAllowedDraft?: (path: string, content: unknown, opts?: { guarded?: boolean }) => boolean | Promise<boolean>
  readonly #isAllowedDelete?: (path: string, currentContent: unknown) => boolean | Promise<boolean>
  readonly #lastOpByPath = new Map<string, string>()

  constructor(config: RelayfileCloudMountClientConfig = {}) {
    if (!config.client) {
      throw new Error('RelayfileCloudMountClient requires a client; use fromConfig() to load cloud credentials')
    }

    this.workspaceId = config.workspaceId ?? DEFAULT_WORKSPACE_ID
    this.#client = config.client
    this.#tokenProvider = config.tokenProvider ?? (() => this.#client.getToken?.())
    this.#baseUrl = config.baseUrl ?? this.#client.getBaseUrl?.()
    this.#eventClient = config.eventClient
    this.#isAllowedDraft = config.isAllowedDraft
    this.#isAllowedDelete = config.isAllowedDelete
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
    const sharedSession = createSharedCloudSessionResolver(config)
    const initialSession = await sharedSession.resolve()
    const setup = (config.relayfileSetupFactory ?? createDefaultRelayfileSetup)({
      cloudApiUrl: initialSession.auth.apiUrl,
      tokenProvider: sharedSession.getAccessToken,
    })
    const handle = await setup.joinWorkspace(workspaceId, {
      agentName: config.agentName ?? DEFAULT_AGENT_NAME,
      scopes: config.scopes ?? [...FACTORY_RELAYFILE_SCOPES],
    })

    return new RelayfileCloudMountClient({
      ...config,
      workspaceId,
      client: handle.client(),
      tokenProvider: () => handle.getToken(),
      baseUrl: handle.info.relayfileUrl,
    })
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
      const status = mapOperationStatus(await this.#client.getOp(this.workspaceId, opId))
      if (status !== 'pending') return status
      if (Date.now() >= deadline) return 'timeout'
      await sleep(Math.min(500, Math.max(25, deadline - Date.now())))
    }
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

const createDefaultRelayfileSetup: RelayfileSetupFactory = ({ cloudApiUrl, tokenProvider }) =>
  new RelayfileSetup({
    cloudApiUrl,
    accessToken: tokenProvider,
  })

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
      providerResult.status === 200 &&
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
  return {
    provider: stringField(source, 'provider') ?? provider,
    status: stringField(source, 'status'),
    lastEventAt,
    lastEventAtMs: Number.isFinite(lastEventAtMs) ? lastEventAtMs : undefined,
    watermarkTs,
    lagSeconds: numberField(source, 'lagSeconds') ?? numberField(source, 'lag_seconds'),
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
  /^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)

const isProviderPath = (path: string): boolean =>
  path.startsWith('/linear/') || path.startsWith('/slack/')

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
