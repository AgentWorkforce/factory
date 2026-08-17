import type { InMemoryStateStoreOptions } from './in-memory-state-store'
import { DocumentStateStore } from './file-state-store'
import { parseWatchStateDocument } from './watch-state-document'
import {
  WatchStateDocumentConflictError,
  type WatchStateDocument,
  type WatchStateDocumentStore,
} from './document-store'
import { diffWatchStateDocuments } from '../hosted/document-state'

export const FACTORY_STATE_BACKEND_ENV = 'FACTORY_STATE_BACKEND'
export const FACTORY_STATE_URL_ENV = 'FACTORY_STATE_URL'
export const CLOUDFLARE_STATE_BACKEND = 'cloudflare-do'

const PROTOCOL_VERSION = 1
const MAX_CONFLICT_RETRIES = 8

type FetchLike = typeof globalThis.fetch

export type CloudflareStateStoreOptions = InMemoryStateStoreOptions & {
  url: string
  fetch?: FetchLike
  /** Injectable for deterministic stale-process lease recovery tests. */
  isProcessAlive?: (pid: number) => boolean
}

export class CloudflareStateStore extends DocumentStateStore {
  constructor(options: CloudflareStateStoreOptions) {
    super({
      backend: CLOUDFLARE_STATE_BACKEND,
      batchSize: options.batchSize,
      isProcessAlive: options.isProcessAlive,
      documentStore: new CloudflareWatchStateDocumentStore({
        url: options.url,
        fetch: options.fetch,
      }),
    })
  }
}

export type CloudflareWatchStateDocumentStoreOptions = {
  url: string
  fetch?: FetchLike
}

type CachedDocument = {
  revision: number
  document: WatchStateDocument
}

/** Container-side client for the internal Worker → Durable Object bridge. */
export class CloudflareWatchStateDocumentStore implements WatchStateDocumentStore {
  readonly #url: string
  readonly #fetch: FetchLike
  readonly #baselines = new WeakMap<WatchStateDocument, CachedDocument>()
  #cached?: CachedDocument

  constructor(options: CloudflareWatchStateDocumentStoreOptions) {
    this.#url = parseStateUrl(options.url)
    this.#fetch = options.fetch ?? globalThis.fetch
  }

  async read(): Promise<WatchStateDocument> {
    const headers = new Headers({ accept: 'application/json' })
    if (this.#cached) headers.set('if-none-match', etag(this.#cached.revision))
    const response = await this.#request({ method: 'GET', headers })

    if (response.status === 304) {
      if (!this.#cached) throw protocolError('returned 304 without a cached document')
      return this.#copyForCaller(this.#cached)
    }
    if (!response.ok) throw await responseError(response, 'read')

    const payload = await readJson(response)
    if (!isRecord(payload) || payload.protocolVersion !== PROTOCOL_VERSION || !isRevision(payload.revision)) {
      throw protocolError('returned an invalid read envelope')
    }
    const document = parseWatchStateDocument(payload.document)
    this.#cached = { revision: payload.revision, document: structuredClone(document) }
    return this.#copyForCaller(this.#cached)
  }

  async write(document: WatchStateDocument): Promise<void> {
    const baseline = this.#baselines.get(document)
    if (!baseline) throw protocolError('write was not based on a document returned by read')
    const changes = diffWatchStateDocuments(baseline.document, document)
    if (changes.length === 0) return

    const response = await this.#request({
      method: 'PATCH',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({
        protocolVersion: PROTOCOL_VERSION,
        revision: baseline.revision,
        changes,
      }),
    })
    if (response.status === 409) throw new WatchStateDocumentConflictError()
    if (!response.ok) throw await responseError(response, 'write')

    const payload = await readJson(response)
    if (!isRecord(payload) || payload.protocolVersion !== PROTOCOL_VERSION || !isRevision(payload.revision)) {
      throw protocolError('returned an invalid write envelope')
    }
    this.#cached = { revision: payload.revision, document: structuredClone(document) }
  }

  async runMutation<T>(operation: () => Promise<T>): Promise<T> {
    for (let attempt = 0; attempt < MAX_CONFLICT_RETRIES; attempt += 1) {
      try {
        return await operation()
      } catch (error) {
        if (!(error instanceof WatchStateDocumentConflictError)) throw error
        this.#cached = undefined
      }
    }
    throw new WatchStateDocumentConflictError(
      `Factory state document remained contended after ${MAX_CONFLICT_RETRIES} attempts`,
    )
  }

  async assertReady(): Promise<void> {
    await this.read()
  }

  /** Explicit, create-only cutover operation. It is never called by normal startup. */
  async initialize(document: WatchStateDocument): Promise<number> {
    const parsed = parseWatchStateDocument(document)
    const response = await this.#request({
      method: 'PUT',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ protocolVersion: PROTOCOL_VERSION, document: parsed }),
    })
    if (!response.ok) throw await responseError(response, 'initialize')
    const payload = await readJson(response)
    if (!isRecord(payload) || payload.protocolVersion !== PROTOCOL_VERSION || !isRevision(payload.revision)) {
      throw protocolError('returned an invalid initialize envelope')
    }
    this.#cached = { revision: payload.revision, document: structuredClone(parsed) }
    return payload.revision
  }

  async #request(init: RequestInit): Promise<Response> {
    try {
      return await this.#fetch(this.#url, init)
    } catch (cause) {
      throw new Error(`Factory durable state backend is unreachable at ${this.#url}`, { cause })
    }
  }

  #copyForCaller(cached: CachedDocument): WatchStateDocument {
    const document = structuredClone(cached.document)
    this.#baselines.set(document, {
      revision: cached.revision,
      document: structuredClone(cached.document),
    })
    return document
  }
}

const parseStateUrl = (value: string): string => {
  let url: URL
  try {
    url = new URL(value)
  } catch (cause) {
    throw new Error(`${FACTORY_STATE_URL_ENV} must be an absolute HTTP(S) URL`, { cause })
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${FACTORY_STATE_URL_ENV} must use http: or https:`)
  }
  url.hash = ''
  url.search = ''
  return url.toString().replace(/\/$/, '')
}

const responseError = async (response: Response, operation: string): Promise<Error> => {
  const body = await response.text()
  const detail = body.slice(0, 500).replace(/\s+/g, ' ').trim()
  return new Error(
    `Factory durable state ${operation} failed with HTTP ${response.status}${detail ? `: ${detail}` : ''}`,
  )
}

const readJson = async (response: Response): Promise<unknown> => {
  try {
    return await response.json() as unknown
  } catch (cause) {
    throw protocolError('returned invalid JSON', cause)
  }
}

const protocolError = (detail: string, cause?: unknown): Error =>
  new Error(`Factory durable state backend ${detail}`, cause === undefined ? undefined : { cause })

const etag = (revision: number): string => `"${revision}"`

const isRevision = (value: unknown): value is number =>
  Number.isSafeInteger(value) && (value as number) >= 0

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
