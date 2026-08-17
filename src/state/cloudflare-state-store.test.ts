import { access, mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'

import type { DispatchLifecycle } from '../ports/state'
import {
  DurableObjectWatchStateService,
  type DurableDocumentChange,
  type DurableDocumentPersistence,
  type DurableDocumentRecord,
  type DurableDocumentSnapshot,
} from '../hosted/document-state'
import type { WatchStateDocument } from './document-store'
import {
  CloudflareStateStore,
  CloudflareWatchStateDocumentStore,
} from './cloudflare-state-store'

describe('CloudflareStateStore', () => {
  it('keeps a dispatch claim after process restart and complete local-disk loss', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cloudflare-ephemeral-'))
    const runtimeState = join(root, 'state')
    await mkdir(runtimeState)
    const durable = durableHarness()
    await durable.client.initialize(emptyDocument())

    const key = 'AR-448:uuid-448:/linear/issues/AR-448.json'
    const first = new CloudflareStateStore({ batchSize: 2, url: durable.url, fetch: durable.fetch })
    await expect(first.claimDispatchLifecycle(
      'workspace-1', key, dispatchLifecycle(448), 'owner-a', 1_000, 60_000,
    )).resolves.toMatchObject({ acquired: true, created: true })

    // This is the Cloudflare Container failure mode: every local byte is gone.
    await rm(root, { recursive: true, force: true })
    await expect(access(runtimeState)).rejects.toBeDefined()

    const restarted = new CloudflareStateStore({ batchSize: 2, url: durable.url, fetch: durable.fetch })
    await expect(restarted.claimDispatchLifecycle(
      'workspace-1', key, dispatchLifecycle(448), 'owner-b', 2_000, 60_000,
    )).resolves.toMatchObject({
      acquired: false,
      created: false,
      lifecycle: { lease: { owner: 'owner-a', epoch: 1 } },
    })
  })

  it('retries a revision conflict and leaves exactly one winner for a contested claim', async () => {
    const durable = durableHarness()
    await durable.client.initialize(emptyDocument())
    const first = new CloudflareStateStore({ batchSize: 2, url: durable.url, fetch: durable.fetch })
    const second = new CloudflareStateStore({ batchSize: 2, url: durable.url, fetch: durable.fetch })

    const claims = await Promise.all([
      first.markRunning('workspace-1', 'claim-1', 'agent-a', 1_000, 5_000),
      second.markRunning('workspace-1', 'claim-1', 'agent-b', 1_000, 5_000),
    ])

    expect(claims.filter(Boolean)).toHaveLength(1)
    await expect(new CloudflareStateStore({ batchSize: 2, url: durable.url, fetch: durable.fetch })
      .getBabysitterGeneration('workspace-1', 'claim-1')).resolves.toMatchObject({
      agentName: claims[0] ? 'agent-a' : 'agent-b',
      phase: 'claimed',
    })
  })

  it('fails loudly when the durable backend is unreachable', async () => {
    const fetch = vi.fn(async () => {
      throw new Error('connection refused')
    }) as unknown as typeof globalThis.fetch
    const store = new CloudflareWatchStateDocumentStore({
      url: 'http://factory-state.do/control/v1/document',
      fetch,
    })

    await expect(store.assertReady()).rejects.toThrow('Factory durable state backend is unreachable')
    expect(fetch).toHaveBeenCalledTimes(1)
  })
})

const durableHarness = (): {
  url: string
  fetch: typeof globalThis.fetch
  client: CloudflareWatchStateDocumentStore
} => {
  const service = new DurableObjectWatchStateService(new MemoryDocumentPersistence())
  const url = 'http://factory-state.do/control/v1/document'
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) =>
    await service.fetch(new Request(input, init))) as typeof globalThis.fetch
  return {
    url,
    fetch,
    client: new CloudflareWatchStateDocumentStore({ url, fetch }),
  }
}

class MemoryDocumentPersistence implements DurableDocumentPersistence {
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
    this.#records = new Map(records.map((record) => [keyFor(record), structuredClone(record)]))
    return this.#revision
  }

  compareAndSet(revision: number, changes: DurableDocumentChange[]): number | undefined {
    if (!this.#initialized || revision !== this.#revision) return undefined
    for (const change of changes) {
      if (change.delete) this.#records.delete(keyFor(change))
      else this.#records.set(keyFor(change), structuredClone(change as DurableDocumentRecord))
    }
    this.#revision += 1
    return this.#revision
  }
}

const keyFor = (record: Omit<DurableDocumentRecord, 'value'>): string =>
  JSON.stringify([record.workspaceId, record.collection, record.recordKey])

const emptyDocument = (): WatchStateDocument => ({ version: 3, workspaces: {} })

const dispatchLifecycle = (number: number): DispatchLifecycle => ({
  runId: `run-${number}`,
  issue: { key: `AR-${number}`, uuid: `uuid-${number}`, path: `/linear/issues/AR-${number}.json` },
  decision: {
    issue: { key: `AR-${number}`, uuid: `uuid-${number}`, path: `/linear/issues/AR-${number}.json` },
    routes: [{ repo: 'AgentWorkforce/factory', clonePath: '/work/factory', rationale: 'test' }],
    scope: 'single',
    implementers: [],
    reviewer: {
      name: `ar-${number}-review`,
      role: 'reviewer',
      capability: 'spawn:claude',
      task: 'review',
      repo: 'AgentWorkforce/factory',
    },
    thin: false,
    confidence: 'high',
    rationale: 'test',
  },
  dryRun: false,
  phase: 'dispatching',
  agents: [],
  invocationIds: [],
  updatedAtMs: 1_000,
})
