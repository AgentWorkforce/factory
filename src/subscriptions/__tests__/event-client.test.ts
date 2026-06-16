import { describe, expect, it, vi } from 'vitest'

import {
  createWorkspaceScopedEventClient,
  filesystemEventToChangeEvent,
  type FilesystemEventLike,
  type RelayFileSyncLike,
  type WorkspaceEventClientSource,
} from '../event-client'

type FakeSyncState = 'idle' | 'connecting' | 'open' | 'polling' | 'reconnecting' | 'closed'
type FakeSyncHandler =
  | ((event: FilesystemEventLike) => void)
  | ((state: FakeSyncState) => void)
  | ((error: unknown) => void)

class FakeSync implements RelayFileSyncLike {
  started = false
  stopped = false

  #eventHandlers: Array<(event: FilesystemEventLike) => void> = []
  #stateHandlers: Array<(state: FakeSyncState) => void> = []
  #errorHandlers: Array<(error: unknown) => void> = []

  on(event: 'event', handler: (event: FilesystemEventLike) => void): unknown
  on(event: 'state', handler: (state: FakeSyncState) => void): unknown
  on(event: 'error', handler: (error: unknown) => void): unknown
  on(event: 'event' | 'state' | 'error', handler: FakeSyncHandler): unknown {
    if (event === 'event') this.#eventHandlers.push(handler as (value: FilesystemEventLike) => void)
    if (event === 'state') this.#stateHandlers.push(handler as (value: FakeSyncState) => void)
    if (event === 'error') this.#errorHandlers.push(handler as (value: unknown) => void)
    return () => undefined
  }

  start(): void {
    this.started = true
  }

  async stop(): Promise<void> {
    this.stopped = true
  }

  emit(event: FilesystemEventLike): void {
    for (const handler of this.#eventHandlers) handler(event)
  }

  emitState(state: FakeSyncState): void {
    for (const handler of this.#stateHandlers) handler(state)
  }

  emitError(error: unknown): void {
    for (const handler of this.#errorHandlers) handler(error)
  }
}

function event(overrides: Partial<FilesystemEventLike>): FilesystemEventLike {
  return {
    eventId: 'evt-1',
    type: 'file.updated',
    path: '/linear/issues/AR-1__uuid.json',
    revision: '1',
    provider: 'linear',
    timestamp: '2026-06-11T00:00:00.000Z',
    ...overrides,
  }
}

describe('workspace scoped event client', () => {
  it('normalizes filesystem events into ChangeEvents with fallback expansion', async () => {
    const change = filesystemEventToChangeEvent(null, 'ws-1', event({
      eventId: undefined,
      type: 'file.deleted',
      revision: '7',
      path: 'linear/issues/AR-1__uuid.json',
    }))

    expect(change.id).toBe('ws-1:/linear/issues/AR-1__uuid.json:7')
    expect(change.resource).toMatchObject({
      path: '/linear/issues/AR-1__uuid.json',
      provider: 'linear',
      id: 'AR-1__uuid.json',
    })
    await expect(change.expand('full')).resolves.toEqual({
      level: 'full',
      path: '/linear/issues/AR-1__uuid.json',
      data: { path: '/linear/issues/AR-1__uuid.json', deleted: true },
    })
  })

  it('coalesces repeated path updates and reports logger/telemetry events', async () => {
    vi.useFakeTimers()
    try {
      const sync = new FakeSync()
      const changes: string[] = []
      const coalesced = vi.fn()
      const queueDepth = vi.fn()
      const logger = { debug: vi.fn(), warn: vi.fn() }
      const telemetry = { increment: vi.fn(), gauge: vi.fn() }
      const client = {
        getEvents: vi.fn(),
        getResourceAtEvent: vi.fn(),
      } as unknown as WorkspaceEventClientSource

      createWorkspaceScopedEventClient(
        client,
        'ws-1',
        async () => undefined,
        'https://relayfile.invalid',
        () => sync,
        { logger, telemetry },
      ).subscribe(['/linear/issues/**'], (change) => {
        changes.push(change.id)
      }, { coalesceMs: 25, onCoalesced: coalesced, onQueueDepth: queueDepth })

      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      sync.emit(event({ eventId: 'evt-1', revision: '1' }))
      sync.emit(event({ eventId: 'evt-2', revision: '2' }))
      await vi.advanceTimersByTimeAsync(25)

      expect(sync.started).toBe(true)
      expect(changes).toEqual(['evt-2'])
      expect(coalesced).toHaveBeenCalledTimes(1)
      expect(queueDepth).toHaveBeenLastCalledWith(0)
      expect(telemetry.increment).toHaveBeenCalledWith('subscription.coalesced', 1, {
        workspaceId: 'ws-1',
        path: '/linear/issues/AR-1__uuid.json',
      })
      expect(telemetry.gauge).toHaveBeenCalledWith('subscription.queueDepth', 0, { workspaceId: 'ws-1' })
      expect(logger.debug).toHaveBeenCalledWith('remote stream starting', expect.objectContaining({
        workspaceId: 'ws-1',
        relayfilePathFilters: ['/linear/issues/**'],
      }))
    } finally {
      vi.useRealTimers()
    }
  })

  it('does NOT tear down the sync on repeated stream errors — the SDK self-heals', async () => {
    // Regression (relayfile-ws-self-heal-latch): the bridge used to stop the
    // sync after 5 errors and run its own getEvents poll loop forever. That
    // killed the SDK's own WS self-heal (which already delivers via
    // sync.on('event')) and stranded the factory on redundant polling. The
    // bridge now leaves recovery entirely to the SDK.
    vi.useFakeTimers()
    try {
      const sync = new FakeSync()
      const changes: string[] = []
      const logger = { debug: vi.fn(), warn: vi.fn() }
      const telemetry = { increment: vi.fn(), gauge: vi.fn() }
      const client = {
        getEvents: vi.fn(),
        getResourceAtEvent: vi.fn(),
      } as unknown as WorkspaceEventClientSource

      createWorkspaceScopedEventClient(
        client,
        'ws-1',
        async () => undefined,
        'https://relayfile.invalid',
        () => sync,
        { logger, telemetry },
      ).subscribe(['/linear/issues/**'], (change) => {
        changes.push(change.id)
      }, { coalesce: 'none' })

      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()

      // Five consecutive stream errors — previously the trip wire.
      for (let index = 0; index < 5; index += 1) sync.emitError(new Error(`boom-${index}`))
      await Promise.resolve()

      // The sync is left alive and the bridge never ran its own poll.
      expect(sync.stopped).toBe(false)
      expect(client.getEvents).not.toHaveBeenCalled()

      // Events keep flowing through the still-live sync — including ones the
      // SDK's own internal polling surfaces via sync.on('event').
      sync.emit(event({ eventId: 'evt-1', revision: '1' }))
      sync.emit(event({ eventId: 'evt-2', revision: '2', path: '/linear/issues/AR-2__uuid.json' }))
      await Promise.resolve()
      expect(changes).toEqual(['evt-1', 'evt-2'])

      // Errors are surfaced, but no forced-polling-fallback warning fires.
      expect(logger.warn).toHaveBeenCalledWith('remote stream error', expect.objectContaining({
        workspaceId: 'ws-1',
      }))
      expect(logger.warn).not.toHaveBeenCalledWith(
        'remote stream forced polling fallback',
        expect.anything(),
      )
    } finally {
      vi.useRealTimers()
    }
  })
})
