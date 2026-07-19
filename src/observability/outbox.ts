import { randomUUID } from 'node:crypto'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { dirname } from 'node:path'

import lockfile from 'proper-lockfile'
import { z } from 'zod'

import {
  FACTORY_CLOUD_EVENT_CONTRACT_V1,
  FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE,
  FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES,
  FactoryCloudEventBatchV1Schema,
  FactoryCloudEventInputV1Schema,
  FactoryCloudEventV1Schema,
  FactoryCloudInstanceV1Schema,
  isCriticalFactoryCloudEvent,
  type FactoryCloudEventBatchV1,
  type FactoryCloudEventInputV1,
  type FactoryCloudEventV1,
  type FactoryCloudInstanceV1,
} from './events'

const OUTBOX_DOCUMENT_VERSION = 1 as const
const DEFAULT_MAX_EVENTS = 10_000
const DEFAULT_MAX_BYTES = 10 * 1024 * 1024
const OUTBOX_LOCK_STALE_MS = 60_000
const OUTBOX_LOCK_RETRIES = 4

const storedEntrySchema = z.object({
  instance: FactoryCloudInstanceV1Schema,
  event: FactoryCloudEventV1Schema,
}).strict()

const outboxDocumentSchema = z.object({
  version: z.literal(OUTBOX_DOCUMENT_VERSION),
  nextSequence: z.number().int().positive(),
  droppedEvents: z.number().int().nonnegative(),
  entries: z.array(storedEntrySchema),
}).strict()

type StoredEntry = z.infer<typeof storedEntrySchema>
type OutboxDocument = z.infer<typeof outboxDocumentSchema>
type LoadedOutboxDocument = { document: OutboxDocument; compactBytes: number }

export interface FileFactoryCloudEventOutboxOptions {
  path: string
  maxEvents?: number
  maxBytes?: number
  /** Finite advisory-lock retries; defaults to a short fail-open window. */
  lockRetries?: number
}

export interface FactoryCloudEventEnqueueResult {
  event: FactoryCloudEventV1
  duplicate: boolean
  droppedEvents: number
  pending: number
}

export interface FactoryCloudEventOutboxStats {
  pending: number
  droppedEvents: number
  oldestOccurredAt?: string
}

export interface FactoryCloudEventOutbox {
  enqueue(instance: FactoryCloudInstanceV1, event: FactoryCloudEventInputV1): Promise<FactoryCloudEventEnqueueResult>
  peekBatch(limit?: number, maxPayloadBytes?: number): Promise<FactoryCloudEventBatchV1 | undefined>
  acknowledge(eventIds: readonly string[]): Promise<number>
  /** Remove undeliverable events and increment the durable dropped counter. */
  discard(eventIds: readonly string[]): Promise<number>
  stats(): Promise<FactoryCloudEventOutboxStats>
}

/**
 * A process-safe, append-by-rewrite outbox. Documents are written atomically
 * with mode 0600 and guarded by the same advisory-lock pattern as Factory's
 * durable lifecycle store. Event IDs are first-write-wins idempotency keys.
 */
export class FileFactoryCloudEventOutbox implements FactoryCloudEventOutbox {
  readonly #path: string
  readonly #maxEvents: number
  readonly #maxBytes: number
  readonly #lockRetries: number
  #operation: Promise<void> = Promise.resolve()

  constructor(options: FileFactoryCloudEventOutboxOptions) {
    this.#path = options.path
    this.#maxEvents = positiveInteger(options.maxEvents ?? DEFAULT_MAX_EVENTS, 'maxEvents')
    this.#maxBytes = positiveInteger(options.maxBytes ?? DEFAULT_MAX_BYTES, 'maxBytes')
    this.#lockRetries = nonNegativeInteger(options.lockRetries ?? OUTBOX_LOCK_RETRIES, 'lockRetries')
  }

  async enqueue(
    rawInstance: FactoryCloudInstanceV1,
    rawEvent: FactoryCloudEventInputV1,
  ): Promise<FactoryCloudEventEnqueueResult> {
    const instance = FactoryCloudInstanceV1Schema.parse(rawInstance)
    const input = FactoryCloudEventInputV1Schema.parse(rawEvent)
    return await this.#exclusive(async () => this.#withLock(async () => {
      const loaded = await this.#load()
      const { document } = loaded
      const existing = document.entries.find(({ event }) => event.id === input.id)
      if (existing) {
        return {
          event: structuredClone(existing.event),
          duplicate: true,
          droppedEvents: 0,
          pending: document.entries.length,
        }
      }

      const event = FactoryCloudEventV1Schema.parse({ ...input, sequence: document.nextSequence })
      const storedEntry = { instance, event }
      let compactBytes = loaded.compactBytes
      compactBytes += integerBytes(document.nextSequence + 1) - integerBytes(document.nextSequence)
      compactBytes += serializedBytes(storedEntry) + (document.entries.length > 0 ? 1 : 0)
      document.nextSequence += 1
      document.entries.push(storedEntry)
      const { droppedEvents } = compactDocument(document, this.#maxEvents, this.#maxBytes, compactBytes)
      await this.#persist(document)
      return { event, duplicate: false, droppedEvents, pending: document.entries.length }
    }))
  }

  async peekBatch(
    limit = FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE,
    maxPayloadBytes = FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES,
  ): Promise<FactoryCloudEventBatchV1 | undefined> {
    const boundedLimit = Math.min(FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE, positiveInteger(limit, 'limit'))
    const boundedPayloadBytes = positiveInteger(maxPayloadBytes, 'maxPayloadBytes')
    return await this.#exclusive(async () => this.#withLock(async () => {
      const { document } = await this.#load()
      let mutated = false
      for (;;) {
        const first = document.entries[0]
        if (!first) {
          if (mutated) await this.#persist(document)
          return undefined
        }
        const instance = structuredClone(first.instance)
        const instanceKey = stableInstanceKey(instance)
        const events: FactoryCloudEventV1[] = []
        for (const entry of document.entries) {
          if (events.length >= boundedLimit || stableInstanceKey(entry.instance) !== instanceKey) break
          const candidate = batch(instance, [...events, structuredClone(entry.event)])
          if (batchBytes(candidate) > boundedPayloadBytes) {
            if (events.length === 0) {
              // This event can never be delivered under the configured Cloud
              // request limit. Drop it durably so later events can progress.
              document.entries.shift()
              document.droppedEvents += 1
              mutated = true
            }
            break
          }
          events.push(structuredClone(entry.event))
        }
        if (events.length === 0) continue
        if (mutated) await this.#persist(document)
        return FactoryCloudEventBatchV1Schema.parse(batch(instance, events))
      }
    }))
  }

  async acknowledge(eventIds: readonly string[]): Promise<number> {
    if (eventIds.length === 0) return 0
    const ids = new Set(eventIds.map((id) => z.string().min(1).max(256).parse(id)))
    return await this.#exclusive(async () => this.#withLock(async () => {
      const { document } = await this.#load()
      const before = document.entries.length
      document.entries = document.entries.filter(({ event }) => !ids.has(event.id))
      const acknowledged = before - document.entries.length
      if (acknowledged > 0) await this.#persist(document)
      return acknowledged
    }))
  }

  async discard(eventIds: readonly string[]): Promise<number> {
    if (eventIds.length === 0) return 0
    const ids = new Set(eventIds.map((id) => z.string().min(1).max(256).parse(id)))
    return await this.#exclusive(async () => this.#withLock(async () => {
      const { document } = await this.#load()
      const before = document.entries.length
      document.entries = document.entries.filter(({ event }) => !ids.has(event.id))
      const discarded = before - document.entries.length
      if (discarded > 0) {
        document.droppedEvents += discarded
        await this.#persist(document)
      }
      return discarded
    }))
  }

  async stats(): Promise<FactoryCloudEventOutboxStats> {
    return await this.#exclusive(async () => {
      const { document } = await this.#load()
      return {
        pending: document.entries.length,
        droppedEvents: document.droppedEvents,
        ...(document.entries[0]?.event.occurredAt ? { oldestOccurredAt: document.entries[0].event.occurredAt } : {}),
      }
    })
  }

  async #load(): Promise<LoadedOutboxDocument> {
    try {
      const content = await readFile(this.#path, 'utf8')
      const document = outboxDocumentSchema.parse(JSON.parse(content))
      return {
        document,
        compactBytes: isCanonicalCompactDocument(content)
          ? Buffer.byteLength(content.endsWith('\n') ? content.slice(0, -1) : content, 'utf8')
          : documentBytes(document),
      }
    } catch (error) {
      if (isMissingFileError(error)) {
        const document = emptyDocument()
        return { document, compactBytes: documentBytes(document) }
      }
      throw error
    }
  }

  async #persist(document: OutboxDocument): Promise<void> {
    await mkdir(dirname(this.#path), { recursive: true, mode: 0o700 })
    const temporaryPath = `${this.#path}.${process.pid}.${randomUUID()}.tmp`
    try {
      const handle = await open(temporaryPath, 'wx', 0o600)
      try {
        await handle.writeFile(`${JSON.stringify(document)}\n`)
        await handle.sync()
      } finally {
        await handle.close()
      }
      await rename(temporaryPath, this.#path)
    } finally {
      await rm(temporaryPath, { force: true })
    }
  }

  async #withLock<T>(operation: () => Promise<T>): Promise<T> {
    await mkdir(dirname(this.#path), { recursive: true, mode: 0o700 })
    const release = await lockfile.lock(this.#path, {
      realpath: false,
      stale: OUTBOX_LOCK_STALE_MS,
      update: OUTBOX_LOCK_STALE_MS / 2,
      retries: {
        retries: this.#lockRetries,
        factor: 1.2,
        minTimeout: 10,
        maxTimeout: 50,
        randomize: true,
      },
    })
    try {
      return await operation()
    } finally {
      await release()
    }
  }

  async #exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#operation.then(operation, operation)
    this.#operation = result.then(() => undefined, () => undefined)
    return await result
  }
}

const emptyDocument = (): OutboxDocument => ({
  version: OUTBOX_DOCUMENT_VERSION,
  nextSequence: 1,
  droppedEvents: 0,
  entries: [],
})

function compactDocument(
  document: OutboxDocument,
  maxEvents: number,
  maxBytes: number,
  initialBytes: number,
): { droppedEvents: number; compactBytes: number } {
  if (document.entries.length <= maxEvents && initialBytes <= maxBytes) {
    return { droppedEvents: 0, compactBytes: initialBytes }
  }

  const ordinary: Array<{ index: number; bytes: number }> = []
  const critical: Array<{ index: number; bytes: number }> = []
  document.entries.forEach((entry, index) => {
    const candidate = { index, bytes: serializedBytes(entry) }
    if (isCriticalFactoryCloudEvent(entry.event)) critical.push(candidate)
    else ordinary.push(candidate)
  })

  const removed = new Set<number>()
  let compactBytes = initialBytes
  let remainingEntries = document.entries.length
  let droppedTotal = document.droppedEvents
  for (const candidate of [...ordinary, ...critical]) {
    if (remainingEntries <= maxEvents && compactBytes <= maxBytes) break
    // A JSON array loses exactly one comma whenever an entry is removed from
    // an array that still had at least two entries.
    compactBytes -= candidate.bytes + (remainingEntries > 1 ? 1 : 0)
    remainingEntries -= 1
    const previousCounterBytes = integerBytes(droppedTotal)
    droppedTotal += 1
    compactBytes += integerBytes(droppedTotal) - previousCounterBytes
    removed.add(candidate.index)
  }

  const droppedEvents = removed.size
  if (droppedEvents > 0) {
    document.entries = document.entries.filter((_entry, index) => !removed.has(index))
    document.droppedEvents = droppedTotal
  }
  return { droppedEvents, compactBytes }
}

const documentBytes = (document: OutboxDocument): number => Buffer.byteLength(JSON.stringify(document), 'utf8')
const serializedBytes = (value: unknown): number => Buffer.byteLength(JSON.stringify(value), 'utf8')
const integerBytes = (value: number): number => Buffer.byteLength(String(value), 'utf8')

/**
 * Files written by this version are canonical compact JSON plus one newline.
 * Legacy pretty-printed files fall back to one exact compact serialization on
 * first mutation, then are migrated by #persist. The scanner avoids trusting a
 * raw formatted file size as though it were the maxBytes accounting format.
 */
function isCanonicalCompactDocument(content: string): boolean {
  let inString = false
  let escaped = false
  const finalNewline = content.endsWith('\n') ? content.length - 1 : -1
  for (let index = 0; index < content.length; index += 1) {
    const character = content[index]
    if (index === finalNewline) continue
    if (inString) {
      if (escaped) escaped = false
      else if (character === '\\') escaped = true
      else if (character === '"') inString = false
      continue
    }
    if (character === '"') {
      inString = true
      continue
    }
    if (character === ' ' || character === '\t' || character === '\n' || character === '\r') return false
  }
  return true
}

const batch = (
  instance: FactoryCloudInstanceV1,
  events: FactoryCloudEventV1[],
): FactoryCloudEventBatchV1 => ({
  contract: FACTORY_CLOUD_EVENT_CONTRACT_V1,
  instance,
  events,
})

const batchBytes = (value: FactoryCloudEventBatchV1): number =>
  Buffer.byteLength(JSON.stringify(value), 'utf8')

const stableInstanceKey = (instance: FactoryCloudInstanceV1): string => JSON.stringify(instance)

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive safe integer`)
  return value
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a non-negative safe integer`)
  return value
}

const isMissingFileError = (error: unknown): boolean =>
  Boolean(error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT')
