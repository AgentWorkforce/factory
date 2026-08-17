import { describe, expect, it } from 'vitest'
import { DatabaseSync } from 'node:sqlite'

import type { WatchStateDocument } from '../state/document-store'
import {
  DurableObjectWatchStateService,
  SqliteDurableDocumentPersistence,
  diffWatchStateDocuments,
  flattenWatchStateDocument,
  inflateWatchStateDocument,
  type DurableDocumentChange,
  type DurableDocumentPersistence,
  type DurableDocumentRecord,
  type DurableDocumentSnapshot,
  type DurableObjectTransactionalStorage,
} from './document-state'

describe('DurableObjectWatchStateService', () => {
  it('fails closed while uninitialized and accepts exactly one explicit import', async () => {
    const persistence = new MemoryDocumentPersistence()
    const service = new DurableObjectWatchStateService(persistence)

    expect((await service.fetch(request('GET'))).status).toBe(503)

    const initialized = await service.fetch(request('PUT', {
      protocolVersion: 1,
      document: emptyDocument(),
    }))
    expect(initialized.status).toBe(201)
    expect(await initialized.json()).toEqual({ protocolVersion: 1, revision: 1 })
    expect((await service.fetch(request('PUT', {
      protocolVersion: 1,
      document: emptyDocument(),
    }))).status).toBe(409)

    const read = await service.fetch(request('GET'))
    expect(read.status).toBe(200)
    expect(await read.json()).toMatchObject({
      protocolVersion: 1,
      revision: 1,
      document: emptyDocument(),
    })
  })

  it('rejects a stale revision without losing the winning write', async () => {
    const persistence = new MemoryDocumentPersistence()
    const service = new DurableObjectWatchStateService(persistence)
    await service.fetch(request('PUT', { protocolVersion: 1, document: workspaceDocument() }))

    const changes: DurableDocumentChange[] = [{
      workspaceId: 'workspace-1',
      collection: 'discoverySweep',
      recordKey: 'state',
      value: { consecutiveOverloads: 1, backoffUntilMs: 10, lastEpoch: 0 },
    }]
    expect((await service.fetch(request('PATCH', {
      protocolVersion: 1,
      revision: 1,
      changes,
    }))).status).toBe(200)
    expect((await service.fetch(request('PATCH', {
      protocolVersion: 1,
      revision: 1,
      changes: [{ ...changes[0], value: { consecutiveOverloads: 2, backoffUntilMs: 20, lastEpoch: 0 } }],
    }))).status).toBe(409)

    const read = await (await service.fetch(request('GET'))).json() as {
      document: WatchStateDocument
    }
    expect(read.document.workspaces['workspace-1']?.discoverySweep).toMatchObject({
      consecutiveOverloads: 1,
      backoffUntilMs: 10,
    })
  })
})

describe('durable document normalization', () => {
  it('round-trips large discovery trees and patches lease metadata without rewriting them', () => {
    const previous = workspaceDocument(12_000)
    expect(inflateWatchStateDocument(flattenWatchStateDocument(previous))).toEqual(previous)

    const next = structuredClone(previous)
    next.workspaces['workspace-1']!.discoverySweep.lease = {
      owner: 'owner-a',
      epoch: 1,
      leaseUntilMs: 6_000,
    }
    next.workspaces['workspace-1']!.discoverySweep.lastEpoch = 1

    expect(diffWatchStateDocuments(previous, next)).toEqual([{
      workspaceId: 'workspace-1',
      collection: 'discoverySweep',
      recordKey: 'state',
      value: {
        consecutiveOverloads: 0,
        backoffUntilMs: 0,
        lastEpoch: 1,
        lease: { owner: 'owner-a', epoch: 1, leaseUntilMs: 6_000 },
      },
    }])
  })

  it('stores chunked records and revision CAS in a real SQLite transaction', () => {
    const database = new DatabaseSync(':memory:')
    try {
      const persistence = new SqliteDurableDocumentPersistence(nodeSqliteStorage(database))
      const document = workspaceDocument(12_000)
      expect(persistence.initialize(flattenWatchStateDocument(document))).toBe(1)
      expect(inflateWatchStateDocument(persistence.read().records)).toEqual(document)

      const next = structuredClone(document)
      next.workspaces['workspace-1']!.discoverySweep.backoffUntilMs = 5_000
      const changes = diffWatchStateDocuments(document, next)
      expect(persistence.compareAndSet(1, changes)).toBe(2)
      expect(persistence.compareAndSet(1, changes)).toBeUndefined()
      expect(inflateWatchStateDocument(persistence.read().records)).toEqual(next)
    } finally {
      database.close()
    }
  })

  it('does not split a Unicode surrogate pair across SQLite chunks', () => {
    const database = new DatabaseSync(':memory:')
    try {
      const persistence = new SqliteDurableDocumentPersistence(nodeSqliteStorage(database))
      const document = workspaceDocument()
      document.workspaces['workspace-1']!.discoverySweep.checkpoint!.trees['/large'] = [
        `${'x'.repeat(63_997)}🚀`,
      ]
      expect(persistence.initialize(flattenWatchStateDocument(document))).toBe(1)
      expect(inflateWatchStateDocument(persistence.read().records)).toEqual(document)
    } finally {
      database.close()
    }
  })
})

export class MemoryDocumentPersistence implements DurableDocumentPersistence {
  #initialized = false
  #revision = 0
  #records = new Map<string, DurableDocumentRecord>()

  read(): DurableDocumentSnapshot {
    return {
      initialized: this.#initialized,
      revision: this.#revision,
      records: structuredClone([...this.#records.values()]),
    }
  }

  initialize(records: DurableDocumentRecord[]): number | undefined {
    if (this.#initialized) return undefined
    this.#initialized = true
    this.#revision = 1
    this.#records = recordMap(records)
    return this.#revision
  }

  compareAndSet(revision: number, changes: DurableDocumentChange[]): number | undefined {
    if (!this.#initialized || revision !== this.#revision) return undefined
    for (const change of changes) {
      const key = keyFor(change)
      if (change.delete) this.#records.delete(key)
      else this.#records.set(key, structuredClone(change as DurableDocumentRecord))
    }
    this.#revision += 1
    return this.#revision
  }
}

const recordMap = (records: DurableDocumentRecord[]): Map<string, DurableDocumentRecord> =>
  new Map(records.map((record) => [keyFor(record), structuredClone(record)]))

const keyFor = (record: Omit<DurableDocumentRecord, 'value'>): string =>
  JSON.stringify([record.workspaceId, record.collection, record.recordKey])

const request = (method: string, body?: unknown): Request => new Request('https://state.internal/v1/document', {
  method,
  ...(body === undefined ? {} : {
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }),
})

const emptyDocument = (): WatchStateDocument => ({ version: 3, workspaces: {} })

const workspaceDocument = (treeSize = 2): WatchStateDocument => ({
  version: 3,
  workspaces: {
    'workspace-1': {
      githubIssueCommentWatches: {},
      waitingClarifications: {},
      babysitterSessions: {},
      babysitterGenerations: {},
      conversationSessions: {},
      dispatchLifecycles: {},
      discoverySweep: {
        checkpoint: {
          highWatermark: 'event-10',
          trees: {
            '/github/issues': Array.from({ length: treeSize }, (_, index) => `/github/issues/${index}.json`),
          },
          updatedAtMs: 1_000,
        },
        consecutiveOverloads: 0,
        backoffUntilMs: 0,
        lastEpoch: 0,
      },
    },
  },
})

const nodeSqliteStorage = (database: DatabaseSync): DurableObjectTransactionalStorage => ({
  sql: {
    exec<T>(query: string, ...bindings: unknown[]): Iterable<T> {
      if (bindings.length === 0 && !query.trimStart().toUpperCase().startsWith('SELECT')) {
        database.exec(query)
        return []
      }
      const statement = database.prepare(query)
      if (query.trimStart().toUpperCase().startsWith('SELECT')) {
        return statement.all(...bindings as never[]) as T[]
      }
      statement.run(...bindings as never[])
      return []
    },
  },
  transactionSync<T>(closure: () => T): T {
    database.exec('BEGIN IMMEDIATE')
    try {
      const result = closure()
      database.exec('COMMIT')
      return result
    } catch (error) {
      database.exec('ROLLBACK')
      throw error
    }
  },
})
