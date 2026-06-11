import {
  RelayFileSync,
  type ChangeEvent as SdkChangeEvent,
  type Expansion,
  type ExpansionLevel,
  type FilesystemEvent,
  type FilesystemEventType,
  type RelayFileClient,
  type RelayFileSyncOptions,
  type RelayFileSyncState,
  type Subscription,
} from '@relayfile/sdk'
import type { Logger, TelemetrySink } from '../ports'
import { sameStringList, toErrorMessage, isRecord } from './common'
import { globMatchesPath, relayfileSdkPathFiltersFor } from './globs'

const REMOTE_STREAM_ERROR_POLLING_FALLBACK_THRESHOLD = 5
const REMOTE_STREAM_POLL_INTERVAL_MS = 5_000

export type ChangeEvent = Omit<SdkChangeEvent, 'type' | 'expand'> & {
  type: SdkChangeEvent['type'] | FilesystemEventType | 'relayfile.changed.summary'
  origin?: FilesystemEvent['origin']
  expand: (level?: ExpansionLevel) => Promise<Expansion>
}

export type TokenProvider = () => string | undefined | Promise<string | undefined>

export type RelayFileSyncFactory = (options: RelayFileSyncOptions) => RelayFileSyncLike

export type RelayFileSyncLike = {
  on(event: 'event', handler: (event: FilesystemEventLike) => void): unknown
  on(event: 'state', handler: (state: RelayFileSyncState) => void): unknown
  on(event: 'error', handler: (error: Event | Error | unknown) => void): unknown
  start(): void
  stop(): Promise<void>
}

export type FilesystemEventLike =
  Omit<FilesystemEvent, 'eventId' | 'revision' | 'timestamp'> &
  Partial<Pick<FilesystemEvent, 'eventId' | 'revision' | 'timestamp'>>

export type WorkspaceEventClientSource = Pick<RelayFileClient, 'getEvents' | 'getResourceAtEvent'>

export type WorkspaceScopedEventClientOptions = {
  logger?: Logger
  telemetry?: TelemetrySink
}

export type WorkspaceScopedSubscribeOptions = {
  coalesce?: 'none' | 'fire-once'
  coalesceMs?: number
  pathScope?: string[]
  from?: 'now' | 'legacy'
  onCoalesced?: () => void
  onQueueDepth?: (depth: number) => void
}

export type RelayfileEventClient = {
  subscribe(
    globs: string[],
    onChange: (event: ChangeEvent) => void,
    options?: WorkspaceScopedSubscribeOptions,
  ): Subscription
}

export type IntegrationRelayFileSyncOptionsInput = Omit<RelayFileSyncOptions, 'token'> & {
  tokenProvider: TokenProvider
}

// ported from src/main/integration-event-bridge.ts @integrationRelayFileSyncOptions
export function integrationRelayFileSyncOptions(
  input: IntegrationRelayFileSyncOptionsInput,
): RelayFileSyncOptions {
  const { tokenProvider, ...options } = input
  return {
    ...options,
    token: tokenProvider,
  }
}

function emitLog(
  logger: Logger | undefined,
  level: 'debug' | 'warn',
  message: string,
  metadata: Record<string, unknown>,
): void {
  const sink = level === 'warn' ? logger?.warn : logger?.debug
  sink?.call(logger, message, metadata)
}

function emitTelemetry(
  telemetry: TelemetrySink | undefined,
  name: string,
  metadata?: Record<string, unknown>,
): void {
  telemetry?.increment?.(name, 1, metadata)
}

function warn(
  ports: WorkspaceScopedEventClientOptions | undefined,
  key: string,
  message: string,
  metadata: Record<string, unknown>,
): void {
  emitTelemetry(ports?.telemetry, 'subscription.warning', { key, ...metadata })
  emitLog(ports?.logger, 'warn', message, metadata)
}

function log(
  ports: WorkspaceScopedEventClientOptions | undefined,
  message: string,
  metadata: Record<string, unknown>,
): void {
  emitLog(ports?.logger, 'debug', message, metadata)
}

function decodeBase64UrlJson(value: string): Record<string, unknown> | null {
  try {
    const padded = value.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(value.length / 4) * 4, '=')
    const decoded = Buffer.from(padded, 'base64').toString('utf8')
    const parsed = JSON.parse(decoded)
    return isRecord(parsed) ? parsed : null
  } catch {
    return null
  }
}

function workspaceIdFromJwt(token: string | undefined): string | null {
  if (!token) return null
  const parts = token.split('.')
  if (parts.length !== 3 || !parts[1]) return null
  const claims = decodeBase64UrlJson(parts[1])
  return typeof claims?.workspace_id === 'string' && claims.workspace_id ? claims.workspace_id : null
}

function shouldPublishFilesystemEvent(event: FilesystemEventLike): boolean {
  return event.type === 'file.created' || event.type === 'file.updated' || event.type === 'file.deleted'
}

// ported from src/main/integration-event-bridge.ts @filesystemEventToChangeEvent
export function filesystemEventToChangeEvent(
  client: WorkspaceEventClientSource | null,
  workspaceId: string,
  event: FilesystemEventLike,
): ChangeEvent {
  const path = event.path.startsWith('/') ? event.path : `/${event.path}`
  const provider = event.provider || path.split('/').filter(Boolean)[0] || 'relayfile'
  const resourceId = path.split('/').filter(Boolean).at(-1) || path
  const summary = {
    title: path,
  }

  return {
    id: event.eventId || `${workspaceId}:${path}:${event.revision}`,
    workspace: workspaceId,
    type: 'relayfile.changed',
    occurredAt: event.timestamp || new Date().toISOString(),
    resource: {
      path,
      provider,
      kind: 'record',
      id: resourceId,
      origin: event.origin,
      revision: event.revision,
    },
    summary,
    digest: event.revision ? `revision:${event.revision}` : undefined,
    origin: event.origin,
    expand: async (level = 'summary') => {
      if (level === 'summary') {
        return {
          level,
          path,
          summary,
        }
      }
      if (level === 'full') {
        if (client && event.eventId) {
          try {
            const resource = await client.getResourceAtEvent(event.eventId, { workspaceId })
            return {
              level,
              path: resource.path,
              data: resource.data,
            }
          } catch {
            // Fall through to the local fallback below.
          }
        }
        return {
          level,
          path,
          data: { path, deleted: event.type === 'file.deleted' },
        }
      }
      throw new Error(`ChangeEvent.expand(${JSON.stringify(level)}) is not implemented for integration events`)
    },
  } as ChangeEvent
}

// ported from src/main/integration-event-bridge.ts @createWorkspaceScopedEventClient
export function createWorkspaceScopedEventClient(
  client: WorkspaceEventClientSource,
  workspaceId: string,
  tokenProvider: TokenProvider,
  baseUrl?: string,
  syncFactory: RelayFileSyncFactory = (options) => new RelayFileSync(options),
  ports?: WorkspaceScopedEventClientOptions,
): RelayfileEventClient {
  return {
    subscribe(globs, onChange, options) {
      let active = true
      let sync: RelayFileSyncLike | null = null
      let polling = false
      let pollingTimer: ReturnType<typeof setTimeout> | null = null
      let pollingInFlight = false
      let consecutiveStreamErrors = 0
      let lastEventCursor: string | undefined
      const polledEventIds = new Set<string>()
      const pendingByPath = new Map<string, ReturnType<typeof setTimeout>>()
      const coalesceMs = Math.max(0, Math.floor(options?.coalesceMs ?? 750))
      const shouldCoalesce = (options?.coalesce ?? 'fire-once') !== 'none'
      const pathScope = options?.pathScope?.length && !sameStringList(options.pathScope, globs)
        ? options.pathScope
        : null
      const relayfilePathFilters = relayfileSdkPathFiltersFor(
        options?.pathScope?.length ? options.pathScope : globs,
      )

      const dispatch = (event: FilesystemEventLike): void => {
        if (!active) return
        const changeEvent = filesystemEventToChangeEvent(client, workspaceId, event)
        Promise.resolve(onChange(changeEvent)).catch((error) => {
          const errorMessage = toErrorMessage(error)
          warn(
            ports,
            `change handler failed:${workspaceId}`,
            'change handler failed',
            {
              workspaceId,
              eventId: event.eventId,
              path: event.path,
              error: errorMessage,
            },
          )
        })
      }

      const handleEvent = (event: FilesystemEventLike): void => {
        if (!active || !shouldPublishFilesystemEvent(event)) return
        consecutiveStreamErrors = 0
        if (event.eventId) {
          lastEventCursor = event.eventId
          polledEventIds.add(event.eventId)
        }
        const path = event.path.startsWith('/') ? event.path : `/${event.path}`
        if (!globs.some((glob) => globMatchesPath(glob, path))) return
        if (pathScope && !pathScope.some((glob) => globMatchesPath(glob, path))) return

        if (!shouldCoalesce) {
          dispatch({ ...event, path })
          return
        }

        const existing = pendingByPath.get(path)
        if (existing) {
          clearTimeout(existing)
          options?.onCoalesced?.()
          emitTelemetry(ports?.telemetry, 'subscription.coalesced', { workspaceId, path })
        }
        pendingByPath.set(path, setTimeout(() => {
          pendingByPath.delete(path)
          options?.onQueueDepth?.(pendingByPath.size)
          ports?.telemetry?.gauge?.('subscription.queueDepth', pendingByPath.size, { workspaceId })
          dispatch({ ...event, path })
        }, coalesceMs))
        options?.onQueueDepth?.(pendingByPath.size)
        ports?.telemetry?.gauge?.('subscription.queueDepth', pendingByPath.size, { workspaceId })
      }

      const pollOnce = async (): Promise<void> => {
        if (!active || pollingInFlight) return
        pollingInFlight = true
        try {
          let cursor = lastEventCursor
          for (;;) {
            const response = await client.getEvents(workspaceId, {
              cursor,
              limit: 1000,
            })
            const events = response.events ?? []
            for (const event of events) {
              if (event.eventId && polledEventIds.has(event.eventId)) {
                lastEventCursor = event.eventId
                continue
              }
              handleEvent(event)
            }
            const nextCursor = response.nextCursor || null
            if (events.length > 0) {
              lastEventCursor = events[events.length - 1]?.eventId ?? lastEventCursor
            }
            if (nextCursor) lastEventCursor = nextCursor
            if (!nextCursor || nextCursor === cursor) break
            cursor = nextCursor
          }
          consecutiveStreamErrors = 0
        } catch (error) {
          const errorMessage = toErrorMessage(error)
          warn(
            ports,
            `remote stream polling error:${workspaceId}`,
            'remote stream polling error',
            {
              workspaceId,
              error: errorMessage,
            },
          )
        } finally {
          pollingInFlight = false
        }
      }

      const schedulePolling = (delayMs = REMOTE_STREAM_POLL_INTERVAL_MS): void => {
        if (!active || !polling || pollingTimer) return
        pollingTimer = setTimeout(() => {
          pollingTimer = null
          void pollOnce().finally(() => schedulePolling())
        }, delayMs)
      }

      const startPollingFallback = (reason: string): void => {
        if (!active || polling) return
        polling = true
        warn(
          ports,
          `remote stream forced polling fallback:${workspaceId}`,
          'remote stream forced polling fallback',
          {
            workspaceId,
            reason,
            cursor: lastEventCursor,
          },
        )
        void sync?.stop().catch(() => undefined)
        sync = null
        void pollOnce().finally(() => schedulePolling())
      }

      void Promise.resolve(tokenProvider())
        .then((token: string | undefined) => {
          if (!active) return
          const tokenWorkspaceId = workspaceIdFromJwt(token)
          if (tokenWorkspaceId && tokenWorkspaceId !== workspaceId) {
            warn(
              ports,
              `skipping remote stream with mismatched workspace JWT:${workspaceId}`,
              'skipping remote stream with mismatched workspace JWT',
              {
                workspaceId,
                tokenWorkspaceId,
              },
            )
            return
          }
          log(ports, 'remote stream starting', {
            workspaceId,
            globs,
            pathScope: options?.pathScope,
            relayfilePathFilters,
            from: options?.from ?? 'now',
            transport: baseUrl ? 'websocket' : 'polling',
          })
          sync = syncFactory(integrationRelayFileSyncOptions({
            client: client as RelayFileClient,
            workspaceId,
            baseUrl,
            tokenProvider,
            from: options?.from ?? 'now',
            paths: relayfilePathFilters,
            onPollingFallback: (info) => {
              warn(
                ports,
                `remote stream polling fallback:${workspaceId}`,
                'remote stream polling fallback',
                {
                  workspaceId,
                  reason: info.reason,
                },
              )
            },
          }))
          sync.on('event', handleEvent)
          sync.on('state', (state) => {
            if (state === 'open') consecutiveStreamErrors = 0
            log(ports, 'remote stream state', {
              workspaceId,
              state,
            })
          })
          sync.on('error', (error) => {
            consecutiveStreamErrors += 1
            const errorMessage = toErrorMessage(error)
            warn(
              ports,
              `remote stream error:${workspaceId}`,
              'remote stream error',
              {
                workspaceId,
                error: errorMessage,
              },
            )
            if (consecutiveStreamErrors >= REMOTE_STREAM_ERROR_POLLING_FALLBACK_THRESHOLD) {
              startPollingFallback('repeated-stream-errors')
            }
          })
          sync.start()
        })
        .catch((error: unknown) => {
          const errorMessage = toErrorMessage(error)
          warn(
            ports,
            `remote stream token check failed:${workspaceId}`,
            'remote stream token check failed',
            {
              workspaceId,
              error: errorMessage,
            },
          )
        })

      return {
        async unsubscribe() {
          active = false
          if (pollingTimer) clearTimeout(pollingTimer)
          pollingTimer = null
          for (const timer of pendingByPath.values()) clearTimeout(timer)
          pendingByPath.clear()
          await sync?.stop()
        },
      }
    },
  }
}
