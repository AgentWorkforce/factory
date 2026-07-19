import { z } from 'zod'

import type { FactoryEventReporter, FactoryEventReportResult } from '../ports/observability'
import type { Logger } from '../ports/system'
import {
  FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE,
  FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES,
  FactoryCloudEventInputV1Schema,
  FactoryCloudInstanceV1Schema,
  type FactoryCloudEventBatchV1,
  type FactoryCloudEventInputV1,
  type FactoryCloudInstanceV1,
} from './events'
import type { FactoryCloudEventOutbox } from './outbox'

const DEFAULT_BATCH_SIZE = FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000
const DEFAULT_MAX_ATTEMPTS = 3
const DEFAULT_RETRY_BASE_MS = 1_000
const DEFAULT_RETRY_MAX_MS = 60_000

const ingestResponseSchema = z.object({
  accepted: z.number().int().nonnegative(),
  duplicates: z.number().int().nonnegative(),
  serverTime: z.string().optional(),
  nextHeartbeatSeconds: z.number().positive().optional(),
}).passthrough()

export type FactoryCloudAccessTokenProvider = (
  options?: { forceRefresh?: boolean },
) => string | Promise<string>

export interface FactoryCloudReporterOptions {
  /** Cloud API origin. The reporter appends /api/v1/factory/events. */
  apiUrl?: string
  /** Full ingestion endpoint; useful for custom deployments and tests. */
  endpoint?: string
  instance: FactoryCloudInstanceV1
  outbox: FactoryCloudEventOutbox
  getAccessToken: FactoryCloudAccessTokenProvider
  fetch?: typeof fetch
  logger?: Logger
  batchSize?: number
  /** Serialized request-body ceiling; defaults below Cloud's 256 KiB limit. */
  maxBatchBytes?: number
  requestTimeoutMs?: number
  maxAttempts?: number
  retryBaseMs?: number
  retryMaxMs?: number
  autoFlush?: boolean
  now?: () => number
  random?: () => number
  sleep?: (ms: number) => Promise<void>
}

type DeliveryResult = {
  delivered: boolean
  attempts: number
  stoppedReason?: FactoryEventReportResult['stoppedReason']
  retryDelayMs?: number
}

/**
 * Durable Cloud progress reporter. All public operations resolve rather than
 * reject: observability must not change Factory's control flow. Failed batches
 * remain in the disk outbox and an auto-flushing reporter schedules a retry.
 */
export class FactoryCloudReporter implements FactoryEventReporter {
  readonly #endpoint: string
  readonly #instance: FactoryCloudInstanceV1
  readonly #outbox: FactoryCloudEventOutbox
  readonly #getAccessToken: FactoryCloudAccessTokenProvider
  readonly #fetch: typeof fetch
  readonly #logger?: Logger
  readonly #batchSize: number
  readonly #maxBatchBytes: number
  readonly #requestTimeoutMs: number
  readonly #maxAttempts: number
  readonly #retryBaseMs: number
  readonly #retryMaxMs: number
  readonly #autoFlush: boolean
  readonly #now: () => number
  readonly #random: () => number
  readonly #sleep: (ms: number) => Promise<void>
  #flushInFlight?: Promise<FactoryEventReportResult>
  #retryTimer?: ReturnType<typeof setTimeout>
  #closed = false
  #lastKnownPending = 0

  constructor(options: FactoryCloudReporterOptions) {
    this.#endpoint = resolveEndpoint(options)
    this.#instance = FactoryCloudInstanceV1Schema.parse(options.instance)
    this.#outbox = options.outbox
    this.#getAccessToken = options.getAccessToken
    this.#fetch = options.fetch ?? fetch
    this.#logger = options.logger
    this.#batchSize = boundedPositiveInteger(options.batchSize ?? DEFAULT_BATCH_SIZE, 'batchSize', FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE)
    this.#maxBatchBytes = boundedPositiveInteger(
      options.maxBatchBytes ?? FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES,
      'maxBatchBytes',
      FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES,
    )
    this.#requestTimeoutMs = positiveInteger(options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS, 'requestTimeoutMs')
    this.#maxAttempts = positiveInteger(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS, 'maxAttempts')
    this.#retryBaseMs = positiveInteger(options.retryBaseMs ?? DEFAULT_RETRY_BASE_MS, 'retryBaseMs')
    this.#retryMaxMs = positiveInteger(options.retryMaxMs ?? DEFAULT_RETRY_MAX_MS, 'retryMaxMs')
    if (this.#retryMaxMs < this.#retryBaseMs) throw new Error('retryMaxMs must be greater than or equal to retryBaseMs')
    this.#autoFlush = options.autoFlush ?? true
    this.#now = options.now ?? Date.now
    this.#random = options.random ?? Math.random
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)))
  }

  async report(rawEvent: FactoryCloudEventInputV1): Promise<void> {
    try {
      const event = FactoryCloudEventInputV1Schema.parse(rawEvent)
      const result = await this.#outbox.enqueue(this.#instance, event)
      this.#lastKnownPending = result.pending
      if (result.droppedEvents > 0) {
        this.#logger?.warn?.('[factory] cloud event outbox compacted', {
          droppedEvents: result.droppedEvents,
        })
      }
      if (this.#autoFlush && !this.#closed) this.#scheduleFlush(0)
    } catch (error) {
      this.#logger?.warn?.('[factory] unable to persist cloud progress event', {
        errorClass: errorClass(error),
      })
    }
  }

  async flush(options: { deadlineMs?: number } = {}): Promise<FactoryEventReportResult> {
    const deadlineAt = options.deadlineMs === undefined
      ? Number.POSITIVE_INFINITY
      : this.#now() + Math.max(0, options.deadlineMs)
    let operation = this.#flushInFlight
    if (!operation) {
      operation = this.#flush(deadlineAt).finally(() => {
        if (this.#flushInFlight === operation) this.#flushInFlight = undefined
        if (this.#autoFlush && !this.#closed && !this.#retryTimer) {
          void this.#outbox.stats()
            .then(({ pending }) => {
              this.#lastKnownPending = pending
              if (pending > 0) this.#scheduleFlush(0)
            })
            .catch(() => undefined)
        }
      })
      this.#flushInFlight = operation
    }
    return await this.#awaitFlush(operation, deadlineAt)
  }

  async close(options: { deadlineMs?: number } = {}): Promise<FactoryEventReportResult> {
    this.#closed = true
    if (this.#retryTimer) clearTimeout(this.#retryTimer)
    this.#retryTimer = undefined
    return await this.flush(options)
  }

  async #flush(deadlineAt: number): Promise<FactoryEventReportResult> {
    let delivered = 0
    let attempts = 0
    let stoppedReason: FactoryEventReportResult['stoppedReason'] = 'empty'
    try {
      for (;;) {
        if (this.#now() >= deadlineAt) {
          stoppedReason = 'deadline'
          break
        }
        const batch = await this.#outbox.peekBatch(this.#batchSize, this.#maxBatchBytes)
        if (!batch) {
          stoppedReason = 'empty'
          break
        }
        this.#lastKnownPending = Math.max(this.#lastKnownPending, batch.events.length)
        const result = await this.#deliver(batch, deadlineAt)
        attempts += result.attempts
        if (!result.delivered) {
          stoppedReason = result.stoppedReason ?? 'unavailable'
          if (this.#autoFlush && !this.#closed) this.#scheduleFlush(result.retryDelayMs ?? this.#retryMaxMs)
          break
        }
        const ids = batch.events.map((event) => event.id)
        const acknowledged = await this.#outbox.acknowledge(ids)
        delivered += acknowledged
        this.#lastKnownPending = Math.max(0, this.#lastKnownPending - acknowledged)
      }
    } catch (error) {
      stoppedReason = 'unavailable'
      this.#logger?.warn?.('[factory] cloud progress flush unavailable', { errorClass: errorClass(error) })
      if (this.#autoFlush && !this.#closed) this.#scheduleFlush(this.#retryMaxMs)
    }
    const stats = await this.#safeStats(deadlineAt)
    return { delivered, pending: stats.pending, attempts, stoppedReason }
  }

  async #deliver(batch: FactoryCloudEventBatchV1, deadlineAt: number): Promise<DeliveryResult> {
    let retryDelayMs = this.#retryBaseMs
    for (let attempt = 1; attempt <= this.#maxAttempts; attempt += 1) {
      if (this.#now() >= deadlineAt) return { delivered: false, attempts: attempt - 1, stoppedReason: 'deadline' }
      let response: Response
      try {
        const token = await this.#withinDeadline(
          Promise.resolve().then(async () => await this.#getAccessToken({ forceRefresh: attempt > 1 })),
          deadlineAt,
        )
        if (!token.trim()) throw new Error('Cloud access token is empty')
        response = await this.#fetchWithTimeout(batch, token, deadlineAt)
      } catch (error) {
        if (isDeadlineExceeded(error)) {
          return { delivered: false, attempts: attempt, stoppedReason: 'deadline' }
        }
        this.#logger?.warn?.('[factory] cloud progress delivery attempt failed', {
          attempt,
          errorClass: errorClass(error),
        })
        if (attempt >= this.#maxAttempts) {
          return { delivered: false, attempts: attempt, stoppedReason: 'retry-exhausted', retryDelayMs }
        }
        if (!await this.#waitForRetry(retryDelayMs, deadlineAt)) {
          return { delivered: false, attempts: attempt, stoppedReason: 'deadline' }
        }
        retryDelayMs = this.#nextRetryDelay(attempt)
        continue
      }

      if (response.status === 201) {
        const payload = ingestResponseSchema.safeParse(
          await this.#withinDeadline(readJson(response), deadlineAt),
        )
        if (payload.success && payload.data.accepted + payload.data.duplicates === batch.events.length) {
          return { delivered: true, attempts: attempt }
        }
        this.#logger?.warn?.('[factory] cloud progress response was incomplete', { status: response.status })
        return { delivered: false, attempts: attempt, stoppedReason: 'rejected', retryDelayMs: this.#retryMaxMs }
      }

      // A Cloud CLI token can expire between provider resolution and the
      // request. Retry once through the provider's forced-refresh path without
      // delaying the durable queue.
      if (response.status === 401 && attempt < this.#maxAttempts) {
        continue
      }

      if (!isRetryableStatus(response.status)) {
        this.#logger?.warn?.('[factory] cloud progress batch rejected', { status: response.status })
        return { delivered: false, attempts: attempt, stoppedReason: 'rejected', retryDelayMs: this.#retryMaxMs }
      }

      retryDelayMs = retryAfterMs(response, this.#now()) ?? this.#nextRetryDelay(attempt)
      if (attempt >= this.#maxAttempts) {
        return { delivered: false, attempts: attempt, stoppedReason: 'retry-exhausted', retryDelayMs }
      }
      if (!await this.#waitForRetry(retryDelayMs, deadlineAt)) {
        return { delivered: false, attempts: attempt, stoppedReason: 'deadline' }
      }
    }
    return { delivered: false, attempts: this.#maxAttempts, stoppedReason: 'retry-exhausted', retryDelayMs }
  }

  async #fetchWithTimeout(batch: FactoryCloudEventBatchV1, token: string, deadlineAt: number): Promise<Response> {
    const controller = new AbortController()
    const remainingMs = Math.max(1, deadlineAt - this.#now())
    const timeoutMs = Math.min(this.#requestTimeoutMs, remainingMs)
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    timer.unref?.()
    try {
      return await this.#fetch(this.#endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(batch),
        signal: controller.signal,
      })
    } finally {
      clearTimeout(timer)
    }
  }

  async #waitForRetry(delayMs: number, deadlineAt: number): Promise<boolean> {
    const remaining = deadlineAt - this.#now()
    if (remaining <= 0 || delayMs > remaining) return false
    await this.#sleep(delayMs)
    return this.#now() < deadlineAt
  }

  #nextRetryDelay(attempt: number): number {
    const exponential = Math.min(this.#retryMaxMs, this.#retryBaseMs * (2 ** Math.max(0, attempt - 1)))
    return Math.max(1, Math.round(exponential * (0.5 + this.#random() * 0.5)))
  }

  #scheduleFlush(delayMs: number): void {
    if (this.#closed || this.#retryTimer) return
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined
      void this.flush()
    }, Math.max(0, delayMs))
    this.#retryTimer.unref?.()
  }

  async #awaitFlush(
    operation: Promise<FactoryEventReportResult>,
    deadlineAt: number,
  ): Promise<FactoryEventReportResult> {
    try {
      return await this.#withinDeadline(operation, deadlineAt)
    } catch (error) {
      if (!isDeadlineExceeded(error)) throw error
      return {
        delivered: 0,
        pending: this.#lastKnownPending,
        attempts: 0,
        stoppedReason: 'deadline',
      }
    }
  }

  async #withinDeadline<T>(operation: Promise<T>, deadlineAt: number): Promise<T> {
    if (!Number.isFinite(deadlineAt)) return await operation
    const remainingMs = deadlineAt - this.#now()
    if (remainingMs <= 0) throw new FactoryCloudDeadlineError()
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new FactoryCloudDeadlineError()), remainingMs)
          timer.unref?.()
        }),
      ])
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  async #safeStats(deadlineAt = Number.POSITIVE_INFINITY): Promise<{ pending: number }> {
    try {
      const stats = await this.#withinDeadline(this.#outbox.stats(), deadlineAt)
      this.#lastKnownPending = stats.pending
      return stats
    } catch {
      return { pending: this.#lastKnownPending }
    }
  }
}

class FactoryCloudDeadlineError extends Error {
  constructor() {
    super('Factory Cloud reporting deadline exceeded')
    this.name = 'FactoryCloudDeadlineError'
  }
}

const isDeadlineExceeded = (error: unknown): error is FactoryCloudDeadlineError =>
  error instanceof FactoryCloudDeadlineError

function resolveEndpoint(options: Pick<FactoryCloudReporterOptions, 'apiUrl' | 'endpoint'>): string {
  if (options.endpoint) return new URL(options.endpoint).toString()
  if (!options.apiUrl) throw new Error('FactoryCloudReporter requires apiUrl or endpoint')
  const baseUrl = new URL(options.apiUrl)
  if (!baseUrl.pathname.endsWith('/')) baseUrl.pathname += '/'
  return new URL('api/v1/factory/events', baseUrl).toString()
}

const isRetryableStatus = (status: number): boolean =>
  status === 408 || status === 425 || status === 429 || status >= 500

function retryAfterMs(response: Response, nowMs: number): number | undefined {
  const value = response.headers.get('retry-after')?.trim()
  if (!value) return undefined
  const seconds = Number(value)
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1_000)
  const dateMs = Date.parse(value)
  return Number.isFinite(dateMs) ? Math.max(0, dateMs - nowMs) : undefined
}

async function readJson(response: Response): Promise<unknown> {
  try {
    return await response.json()
  } catch {
    return undefined
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive safe integer`)
  return value
}

function boundedPositiveInteger(value: number, name: string, max: number): number {
  const parsed = positiveInteger(value, name)
  if (parsed > max) throw new Error(`${name} must be less than or equal to ${max}`)
  return parsed
}

const errorClass = (error: unknown): string => {
  if (error && typeof error === 'object') {
    const code = 'code' in error && typeof error.code === 'string' ? error.code : undefined
    const name = 'name' in error && typeof error.name === 'string' ? error.name : undefined
    return (code ?? name ?? 'Error').slice(0, 120)
  }
  return 'Error'
}
