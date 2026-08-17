import type { WatchStateDocument } from '../state/document-store'
import { parseWatchStateDocument } from '../state/watch-state-document'

const PROTOCOL_VERSION = 1
const SQL_CHUNK_CHARACTERS = 64_000

export type DurableDocumentRecord = {
  workspaceId: string
  collection: string
  recordKey: string
  value: unknown
}

export type DurableDocumentChange = Omit<DurableDocumentRecord, 'value'> & {
  value?: unknown
  delete?: true
}

export type DurableDocumentSnapshot = {
  initialized: boolean
  revision: number
  records: DurableDocumentRecord[]
}

/** High-level persistence boundary used by the protocol service and its SQLite adapter. */
export interface DurableDocumentPersistence {
  read(): DurableDocumentSnapshot
  initialize(records: DurableDocumentRecord[]): number | undefined
  compareAndSet(revision: number, changes: DurableDocumentChange[]): number | undefined
}

export type DurableObjectSqlCursor<T> = Iterable<T>

export interface DurableObjectSqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): DurableObjectSqlCursor<T>
}

export interface DurableObjectTransactionalStorage {
  sql: DurableObjectSqlStorage
  transactionSync<T>(closure: () => T): T
}

/** SQLite persistence adapter intended to be constructed with `ctx.storage`. */
export class SqliteDurableDocumentPersistence implements DurableDocumentPersistence {
  readonly #storage: DurableObjectTransactionalStorage

  constructor(storage: DurableObjectTransactionalStorage) {
    this.#storage = storage
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS factory_state_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      schema_version INTEGER NOT NULL,
      document_version INTEGER NOT NULL,
      revision INTEGER NOT NULL,
      initialized INTEGER NOT NULL
    )`)
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS factory_state_chunks (
      workspace_id TEXT NOT NULL,
      collection TEXT NOT NULL,
      record_key TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      payload TEXT NOT NULL,
      PRIMARY KEY (workspace_id, collection, record_key, chunk_index)
    )`)
  }

  read(): DurableDocumentSnapshot {
    const meta = first(this.#storage.sql.exec<MetaRow>(
      'SELECT document_version, revision, initialized FROM factory_state_meta WHERE singleton = 1',
    ))
    if (!meta || meta.initialized !== 1) return { initialized: false, revision: 0, records: [] }
    if (meta.document_version !== 3) throw new Error(`Unsupported Factory state document version ${meta.document_version}`)

    const records: DurableDocumentRecord[] = []
    let currentKey: string | undefined
    let current: Omit<DurableDocumentRecord, 'value'> | undefined
    let payload = ''
    const flush = (): void => {
      if (!current) return
      records.push({ ...current, value: JSON.parse(payload) as unknown })
    }
    for (const row of this.#storage.sql.exec<ChunkRow>(
      `SELECT workspace_id, collection, record_key, chunk_index, payload
       FROM factory_state_chunks
       ORDER BY workspace_id, collection, record_key, chunk_index`,
    )) {
      const key = logicalKey(row.workspace_id, row.collection, row.record_key)
      if (key !== currentKey) {
        flush()
        currentKey = key
        current = {
          workspaceId: row.workspace_id,
          collection: row.collection,
          recordKey: row.record_key,
        }
        payload = ''
      }
      payload += row.payload
    }
    flush()
    return { initialized: true, revision: meta.revision, records }
  }

  initialize(records: DurableDocumentRecord[]): number | undefined {
    return this.#storage.transactionSync(() => {
      const current = first(this.#storage.sql.exec<MetaRow>(
        'SELECT document_version, revision, initialized FROM factory_state_meta WHERE singleton = 1',
      ))
      if (current?.initialized === 1) return undefined
      this.#storage.sql.exec('DELETE FROM factory_state_chunks')
      for (const record of records) this.#replaceRecord(record)
      this.#storage.sql.exec(
        `INSERT INTO factory_state_meta (singleton, schema_version, document_version, revision, initialized)
         VALUES (1, 1, 3, 1, 1)
         ON CONFLICT(singleton) DO UPDATE SET
           schema_version = excluded.schema_version,
           document_version = excluded.document_version,
           revision = excluded.revision,
           initialized = excluded.initialized`,
      )
      return 1
    })
  }

  compareAndSet(revision: number, changes: DurableDocumentChange[]): number | undefined {
    return this.#storage.transactionSync(() => {
      const current = first(this.#storage.sql.exec<MetaRow>(
        'SELECT document_version, revision, initialized FROM factory_state_meta WHERE singleton = 1',
      ))
      if (!current || current.initialized !== 1 || current.revision !== revision) return undefined
      for (const change of changes) {
        this.#storage.sql.exec(
          'DELETE FROM factory_state_chunks WHERE workspace_id = ? AND collection = ? AND record_key = ?',
          change.workspaceId,
          change.collection,
          change.recordKey,
        )
        if (!change.delete) this.#replaceRecord(change as DurableDocumentRecord)
      }
      const nextRevision = revision + 1
      this.#storage.sql.exec(
        'UPDATE factory_state_meta SET revision = ? WHERE singleton = 1 AND revision = ?',
        nextRevision,
        revision,
      )
      return nextRevision
    })
  }

  #replaceRecord(record: DurableDocumentRecord): void {
    const serialized = JSON.stringify(record.value)
    if (serialized === undefined) throw new Error('Factory durable state record cannot serialize undefined')
    const chunks = chunkString(serialized)
    for (let index = 0; index < chunks.length; index += 1) {
      this.#storage.sql.exec(
        `INSERT INTO factory_state_chunks
          (workspace_id, collection, record_key, chunk_index, payload)
         VALUES (?, ?, ?, ?, ?)`,
        record.workspaceId,
        record.collection,
        record.recordKey,
        index,
        chunks[index],
      )
    }
  }
}

/** HTTP/RPC protocol handler to delegate to from the bound Durable Object class. */
export class DurableObjectWatchStateService {
  readonly #persistence: DurableDocumentPersistence

  constructor(persistence: DurableDocumentPersistence) {
    this.#persistence = persistence
  }

  async fetch(request: Request): Promise<Response> {
    try {
      if (request.method === 'GET') return this.#read(request)
      if (request.method === 'PUT') return await this.#initialize(request)
      if (request.method === 'PATCH') return await this.#write(request)
      return jsonResponse({ error: 'method-not-allowed' }, 405, { allow: 'GET, PUT, PATCH' })
    } catch (error) {
      return jsonResponse({
        error: 'invalid-state-request',
        message: error instanceof Error ? error.message : String(error),
      }, 400)
    }
  }

  #read(request: Request): Response {
    const snapshot = this.#persistence.read()
    if (!snapshot.initialized) return jsonResponse({ error: 'state-uninitialized' }, 503)
    if (request.headers.get('if-none-match') === etag(snapshot.revision)) {
      return new Response(null, { status: 304, headers: { etag: etag(snapshot.revision) } })
    }
    const document = inflateWatchStateDocument(snapshot.records)
    return jsonResponse({
      protocolVersion: PROTOCOL_VERSION,
      revision: snapshot.revision,
      document,
    }, 200, { etag: etag(snapshot.revision) })
  }

  async #initialize(request: Request): Promise<Response> {
    const payload = await request.json() as unknown
    if (!isRecord(payload) || payload.protocolVersion !== PROTOCOL_VERSION) {
      return jsonResponse({ error: 'invalid-protocol' }, 400)
    }
    const document = parseWatchStateDocument(payload.document)
    const revision = this.#persistence.initialize(flattenWatchStateDocument(document))
    if (revision === undefined) return jsonResponse({ error: 'already-initialized' }, 409)
    return jsonResponse({ protocolVersion: PROTOCOL_VERSION, revision }, 201, { etag: etag(revision) })
  }

  async #write(request: Request): Promise<Response> {
    const payload = await request.json() as unknown
    if (
      !isRecord(payload) ||
      payload.protocolVersion !== PROTOCOL_VERSION ||
      !isRevision(payload.revision) ||
      !Array.isArray(payload.changes)
    ) return jsonResponse({ error: 'invalid-protocol' }, 400)
    const changes = payload.changes.map(parseChange)
    const revision = this.#persistence.compareAndSet(payload.revision, changes)
    if (revision === undefined) return jsonResponse({ error: 'revision-conflict' }, 409)
    return jsonResponse({ protocolVersion: PROTOCOL_VERSION, revision }, 200, { etag: etag(revision) })
  }
}

const MAP_COLLECTIONS = [
  'githubIssueCommentWatches',
  'waitingClarifications',
  'babysitterSessions',
  'babysitterGenerations',
  'conversationSessions',
  'dispatchLifecycles',
] as const

type MapCollection = typeof MAP_COLLECTIONS[number]

export const flattenWatchStateDocument = (document: WatchStateDocument): DurableDocumentRecord[] => {
  const records: DurableDocumentRecord[] = []
  for (const [workspaceId, workspace] of Object.entries(document.workspaces)) {
    for (const collection of MAP_COLLECTIONS) {
      for (const [recordKey, value] of Object.entries(workspace[collection])) {
        records.push({ workspaceId, collection, recordKey, value })
      }
    }
    const { checkpoint, ...state } = workspace.discoverySweep
    records.push({ workspaceId, collection: 'discoverySweep', recordKey: 'state', value: state })
    if (checkpoint) {
      const { trees, ...metadata } = checkpoint
      records.push({ workspaceId, collection: 'discoveryCheckpoint', recordKey: 'metadata', value: metadata })
      for (const [recordKey, value] of Object.entries(trees)) {
        records.push({ workspaceId, collection: 'discoveryTree', recordKey, value })
      }
    }
  }
  return records.sort(compareRecords)
}

export const inflateWatchStateDocument = (records: DurableDocumentRecord[]): WatchStateDocument => {
  const workspaces: Record<string, Record<string, unknown>> = {}
  for (const record of records) {
    const workspace = workspaces[record.workspaceId] ??= emptySerializedWorkspace()
    if (isMapCollection(record.collection)) {
      (workspace[record.collection] as Record<string, unknown>)[record.recordKey] = record.value
      continue
    }
    if (record.collection === 'discoverySweep' && record.recordKey === 'state') {
      const existing = asRecordOrEmpty(workspace.discoverySweep)
      workspace.discoverySweep = {
        ...asRecord(record.value),
        ...(existing.checkpoint === undefined ? {} : { checkpoint: existing.checkpoint }),
      }
      continue
    }
    if (record.collection === 'discoveryCheckpoint' && record.recordKey === 'metadata') {
      const sweep = asRecordOrEmpty(workspace.discoverySweep)
      const checkpoint = asRecordOrEmpty(sweep.checkpoint)
      sweep.checkpoint = {
        ...asRecord(record.value),
        ...checkpoint,
        trees: asRecordOrEmpty(checkpoint.trees),
      }
      workspace.discoverySweep = sweep
      continue
    }
    if (record.collection === 'discoveryTree') {
      const sweep = asRecordOrEmpty(workspace.discoverySweep)
      const checkpoint = asRecordOrEmpty(sweep.checkpoint)
      const trees = asRecordOrEmpty(checkpoint.trees)
      trees[record.recordKey] = record.value
      checkpoint.trees = trees
      sweep.checkpoint = checkpoint
      workspace.discoverySweep = sweep
      continue
    }
    throw new Error(`Factory durable state contains unknown collection ${record.collection}`)
  }
  return parseWatchStateDocument({ version: 3, workspaces })
}

export const diffWatchStateDocuments = (
  previous: WatchStateDocument,
  next: WatchStateDocument,
): DurableDocumentChange[] => {
  const before = new Map(flattenWatchStateDocument(previous).map((record) => [
    logicalKey(record.workspaceId, record.collection, record.recordKey),
    record,
  ]))
  const after = new Map(flattenWatchStateDocument(next).map((record) => [
    logicalKey(record.workspaceId, record.collection, record.recordKey),
    record,
  ]))
  const changes: DurableDocumentChange[] = []
  for (const [key, record] of before) {
    if (!after.has(key)) {
      changes.push({
        workspaceId: record.workspaceId,
        collection: record.collection,
        recordKey: record.recordKey,
        delete: true,
      })
    }
  }
  for (const [key, record] of after) {
    const prior = before.get(key)
    if (!prior || JSON.stringify(prior.value) !== JSON.stringify(record.value)) changes.push(record)
  }
  return changes.sort(compareRecords)
}

const parseChange = (value: unknown): DurableDocumentChange => {
  if (
    !isRecord(value) ||
    typeof value.workspaceId !== 'string' ||
    typeof value.collection !== 'string' ||
    typeof value.recordKey !== 'string' ||
    (value.delete !== undefined && value.delete !== true)
  ) throw new Error('Factory durable state change is invalid')
  if (value.delete === true) {
    return {
      workspaceId: value.workspaceId,
      collection: value.collection,
      recordKey: value.recordKey,
      delete: true,
    }
  }
  if (!('value' in value)) throw new Error('Factory durable state change has no value')
  return {
    workspaceId: value.workspaceId,
    collection: value.collection,
    recordKey: value.recordKey,
    value: value.value,
  }
}

const emptySerializedWorkspace = (): Record<string, unknown> => ({
  githubIssueCommentWatches: {},
  waitingClarifications: {},
  babysitterSessions: {},
  babysitterGenerations: {},
  conversationSessions: {},
  dispatchLifecycles: {},
  discoverySweep: {
    consecutiveOverloads: 0,
    backoffUntilMs: 0,
    lastEpoch: 0,
  },
})

const isMapCollection = (value: string): value is MapCollection =>
  MAP_COLLECTIONS.some((collection) => collection === value)

const logicalKey = (workspaceId: string, collection: string, recordKey: string): string =>
  JSON.stringify([workspaceId, collection, recordKey])

const compareRecords = (
  left: Omit<DurableDocumentRecord, 'value'>,
  right: Omit<DurableDocumentRecord, 'value'>,
): number => logicalKey(left.workspaceId, left.collection, left.recordKey)
  .localeCompare(logicalKey(right.workspaceId, right.collection, right.recordKey))

const chunkString = (value: string): string[] => {
  const chunks: string[] = []
  let offset = 0
  while (offset < value.length) {
    let end = Math.min(offset + SQL_CHUNK_CHARACTERS, value.length)
    if (
      end < value.length &&
      isHighSurrogate(value.charCodeAt(end - 1)) &&
      isLowSurrogate(value.charCodeAt(end))
    ) end -= 1
    chunks.push(value.slice(offset, end))
    offset = end
  }
  return chunks.length > 0 ? chunks : ['']
}

const isHighSurrogate = (value: number): boolean => value >= 0xD800 && value <= 0xDBFF
const isLowSurrogate = (value: number): boolean => value >= 0xDC00 && value <= 0xDFFF

const jsonResponse = (
  body: unknown,
  status: number,
  headers: Record<string, string> = {},
): Response => new Response(JSON.stringify(body), {
  status,
  headers: { 'content-type': 'application/json', ...headers },
})

const etag = (revision: number): string => `"${revision}"`

const isRevision = (value: unknown): value is number =>
  Number.isSafeInteger(value) && (value as number) >= 0

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const asRecord = (value: unknown): Record<string, unknown> => {
  if (!isRecord(value)) throw new Error('Factory durable state record is invalid')
  return value
}

const asRecordOrEmpty = (value: unknown): Record<string, unknown> =>
  isRecord(value) ? value : {}

const first = <T>(values: Iterable<T>): T | undefined => values[Symbol.iterator]().next().value

type MetaRow = {
  document_version: number
  revision: number
  initialized: number
}

type ChunkRow = {
  workspace_id: string
  collection: string
  record_key: string
  chunk_index: number
  payload: string
}
