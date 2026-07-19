import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import lockfile from 'proper-lockfile'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { FactoryCloudReporter } from './cloud-reporter'
import { FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES, createFactoryCloudEventV1 } from './events'
import { FileFactoryCloudEventOutbox } from './outbox'

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('FactoryCloudReporter', () => {
  it('refuses a request-body ceiling above the safe Cloud limit', async () => {
    await expect(createReporter({ maxBatchBytes: FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES + 1 }))
      .rejects.toThrow(/maxBatchBytes must be less than or equal/u)
  })

  it('batches authenticated events, acknowledges accepted duplicates, and never sends workspace or private text', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    const reporter = await createReporter({
      fetch: vi.fn(async (url, init) => {
        requests.push({ url: String(url), init })
        const count = (JSON.parse(String(init?.body)) as { events: unknown[] }).events.length
        return response(201, { accepted: count - 1, duplicates: 1, serverTime: new Date().toISOString() })
      }),
      batchSize: 2,
    })
    await reporter.report(progress('event-1'))
    await reporter.report(progress('event-2'))
    await reporter.report(progress('event-3'))

    await expect(reporter.flush()).resolves.toEqual({
      delivered: 3,
      pending: 0,
      attempts: 2,
      stoppedReason: 'empty',
    })
    expect(requests).toHaveLength(2)
    expect(requests[0]?.url).toBe('https://cloud.example/api/v1/factory/events')
    expect(new Headers(requests[0]?.init?.headers).get('authorization')).toBe('Bearer cloud-token')
    const body = JSON.parse(String(requests[0]?.init?.body)) as Record<string, unknown>
    expect(body).toMatchObject({ contract: 'factory.telemetry.v1' })
    expect(body).not.toHaveProperty('workspaceId')
    expect(JSON.stringify(body)).not.toMatch(/prompt|description|errorMessage|stack|workspaceId/u)
  })

  it('honors Retry-After, refreshes credentials, and retries transient failures', async () => {
    let now = 0
    const waits: number[] = []
    const forceRefresh: Array<boolean | undefined> = []
    const fetch = vi.fn()
      .mockResolvedValueOnce(new Response(undefined, { status: 429, headers: { 'Retry-After': '0.025' } }))
      .mockResolvedValueOnce(response(201, { accepted: 1, duplicates: 0 }))
    const reporter = await createReporter({
      fetch,
      getAccessToken: async (options) => {
        forceRefresh.push(options?.forceRefresh)
        return 'cloud-token'
      },
      maxAttempts: 2,
      now: () => now,
      sleep: async (ms) => {
        waits.push(ms)
        now += ms
      },
    })
    await reporter.report(progress('event-retry'))

    expect(await reporter.flush()).toMatchObject({ delivered: 1, pending: 0, attempts: 2 })
    expect(waits).toEqual([25])
    expect(forceRefresh).toEqual([false, true])
  })

  it('forces a credential refresh after an unauthorized response', async () => {
    const forceRefresh: Array<boolean | undefined> = []
    const fetch = vi.fn()
      .mockResolvedValueOnce(new Response(undefined, { status: 401 }))
      .mockResolvedValueOnce(response(201, { accepted: 1, duplicates: 0 }))
    const reporter = await createReporter({
      fetch,
      getAccessToken: async (options) => {
        forceRefresh.push(options?.forceRefresh)
        return options?.forceRefresh ? 'refreshed-token' : 'stale-token'
      },
      maxAttempts: 2,
    })
    await reporter.report(progress('event-auth-refresh'))

    expect(await reporter.flush()).toMatchObject({ delivered: 1, pending: 0, attempts: 2 })
    expect(forceRefresh).toEqual([false, true])
    expect(new Headers(fetch.mock.calls[1]?.[1]?.headers).get('authorization')).toBe('Bearer refreshed-token')
  })

  it('keeps events durable and resolves when delivery exhausts retries', async () => {
    const warnings: unknown[][] = []
    const reporter = await createReporter({
      fetch: vi.fn(async () => { throw Object.assign(new Error('sensitive local path'), { code: 'ECONNREFUSED' }) }),
      maxAttempts: 2,
      retryBaseMs: 1,
      retryMaxMs: 1,
      sleep: async () => {},
      logger: { warn: (...args) => warnings.push(args) },
    })

    await expect(reporter.report(progress('event-offline'))).resolves.toBeUndefined()
    await expect(reporter.flush()).resolves.toMatchObject({
      delivered: 0,
      pending: 1,
      attempts: 2,
      stoppedReason: 'retry-exhausted',
    })
    expect(JSON.stringify(warnings)).toContain('ECONNREFUSED')
    expect(JSON.stringify(warnings)).not.toContain('sensitive local path')
  })

  it('fails open when another process holds the bounded outbox lock', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cloud-reporter-lock-'))
    roots.push(root)
    const path = join(root, 'outbox.json')
    const release = await lockfile.lock(path, { realpath: false })
    const warnings: unknown[][] = []
    try {
      const reporter = await createReporter({
        outbox: new FileFactoryCloudEventOutbox({ path, lockRetries: 0 }),
        logger: { warn: (...args) => warnings.push(args) },
      })
      const startedAt = Date.now()
      await expect(reporter.report(progress('event-locked'))).resolves.toBeUndefined()
      expect(Date.now() - startedAt).toBeLessThan(500)
      expect(JSON.stringify(warnings)).toContain('ELOCKED')
    } finally {
      await release()
    }
  })

  it('bounds a hung access-token lookup by the flush deadline', async () => {
    const reporter = await createReporter({
      getAccessToken: async () => await new Promise<string>(() => {}),
    })
    await reporter.report(progress('event-token-hung'))

    const startedAt = Date.now()
    await expect(reporter.flush({ deadlineMs: 25 })).resolves.toMatchObject({
      pending: 1,
      stoppedReason: 'deadline',
    })
    expect(Date.now() - startedAt).toBeLessThan(500)
  })

  it('bounds a stalled acknowledgment body and permits a later flush', async () => {
    const stalled = new Response(undefined, { status: 201 })
    vi.spyOn(stalled, 'json').mockImplementation(async () => await new Promise<never>(() => {}))
    const fetch = vi.fn()
      .mockResolvedValueOnce(stalled)
      .mockResolvedValueOnce(response(201, { accepted: 1, duplicates: 0 }))
    const reporter = await createReporter({ fetch })
    await reporter.report(progress('event-body-hung'))

    await expect(reporter.flush({ deadlineMs: 25 })).resolves.toMatchObject({ stoppedReason: 'deadline' })
    await new Promise((resolve) => setTimeout(resolve, 0))
    await expect(reporter.flush()).resolves.toMatchObject({ delivered: 1, pending: 0 })
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('bounds close even when an earlier unbounded flush is already running', async () => {
    const tokenRequested = Promise.withResolvers<void>()
    const reporter = await createReporter({
      getAccessToken: async () => {
        tokenRequested.resolve()
        return await new Promise<string>(() => {})
      },
    })
    await reporter.report(progress('event-existing-flush'))
    void reporter.flush()
    await tokenRequested.promise

    const startedAt = Date.now()
    await expect(reporter.close({ deadlineMs: 25 })).resolves.toMatchObject({
      pending: 1,
      stoppedReason: 'deadline',
    })
    expect(Date.now() - startedAt).toBeLessThan(500)
  })

  it('retains a batch when Cloud does not acknowledge every event', async () => {
    const reporter = await createReporter({
      fetch: vi.fn(async () => response(201, { accepted: 0, duplicates: 0 })),
    })
    await reporter.report(progress('event-incomplete'))
    expect(await reporter.flush()).toMatchObject({ delivered: 0, pending: 1, stoppedReason: 'rejected' })
  })

  it('preserves a Cloud deployment base path when resolving the endpoint', async () => {
    const requests: string[] = []
    const reporter = await createReporter({
      apiUrl: 'https://cloud.example/cloud',
      fetch: vi.fn(async (url) => {
        requests.push(String(url))
        return response(201, { accepted: 1, duplicates: 0 })
      }),
    })

    await reporter.report(progress('event-base-path'))
    await reporter.flush()

    expect(requests).toEqual(['https://cloud.example/cloud/api/v1/factory/events'])
  })
})

async function createReporter(
  overrides: Partial<ConstructorParameters<typeof FactoryCloudReporter>[0]> = {},
): Promise<FactoryCloudReporter> {
  const root = await mkdtemp(join(tmpdir(), 'factory-cloud-reporter-'))
  roots.push(root)
  return new FactoryCloudReporter({
    apiUrl: 'https://cloud.example',
    instance: { id: 'instance-1', bootId: 'boot-1', version: '0.1.32' },
    outbox: new FileFactoryCloudEventOutbox({ path: join(root, 'outbox.json') }),
    getAccessToken: async () => 'cloud-token',
    autoFlush: false,
    ...overrides,
  })
}

const progress = (id: string) => createFactoryCloudEventV1({
  id,
  type: 'run.phase_changed',
  runId: 'run-1',
  phase: 'running',
  status: 'running',
  run: { source: 'github', repository: 'AgentWorkforce/factory', issueKey: 'factory#123' },
  attributes: { backend: 'internal', component: 'orchestrator', operation: 'save_lifecycle' },
}, { now: () => new Date('2026-07-19T12:00:00.000Z') })

const response = (status: number, body: unknown): Response => new Response(JSON.stringify(body), {
  status,
  headers: { 'Content-Type': 'application/json' },
})
