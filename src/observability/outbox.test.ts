import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import lockfile from 'proper-lockfile'
import { afterEach, describe, expect, it } from 'vitest'

import { createFactoryCloudEventV1, type FactoryCloudInstanceV1 } from './events'
import { FileFactoryCloudEventOutbox } from './outbox'

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('FileFactoryCloudEventOutbox', () => {
  it('survives restart, assigns durable sequence numbers, and dedupes first-write-wins IDs', async () => {
    const path = await outboxPath()
    const first = new FileFactoryCloudEventOutbox({ path })
    const original = event('event-1', 'instance.heartbeat')
    const stored = await first.enqueue(instance('boot-1'), original)
    const duplicate = await first.enqueue(instance('boot-1'), event('event-1', 'factory.failure'))
    await first.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat'))

    expect(stored).toMatchObject({ duplicate: false, event: { id: 'event-1', sequence: 1 } })
    expect(duplicate).toMatchObject({ duplicate: true, event: { id: 'event-1', type: 'instance.heartbeat', sequence: 1 } })

    const restarted = new FileFactoryCloudEventOutbox({ path })
    expect((await restarted.peekBatch())?.events.map(({ id, sequence }) => ({ id, sequence }))).toEqual([
      { id: 'event-1', sequence: 1 },
      { id: 'event-2', sequence: 2 },
    ])
    expect(await restarted.acknowledge(['event-1', 'event-1'])).toBe(1)
    expect(await restarted.acknowledge(['event-1'])).toBe(0)
    expect(await restarted.stats()).toMatchObject({ pending: 1, droppedEvents: 0 })
    expect((await stat(path)).mode & 0o777).toBe(0o600)
  })

  it('bounds the queue while preserving failure events ahead of ordinary progress', async () => {
    const outbox = new FileFactoryCloudEventOutbox({ path: await outboxPath(), maxEvents: 2 })
    await outbox.enqueue(instance('boot-1'), event('failure-1', 'factory.failure', { level: 'error' }))
    await outbox.enqueue(instance('boot-1'), event('heartbeat-1', 'instance.heartbeat'))
    const result = await outbox.enqueue(instance('boot-1'), event('heartbeat-2', 'instance.heartbeat'))

    expect(result.droppedEvents).toBe(1)
    expect((await outbox.peekBatch())?.events.map(({ id }) => id)).toEqual(['failure-1', 'heartbeat-2'])
    expect(await outbox.stats()).toMatchObject({ pending: 2, droppedEvents: 1 })
  })

  it('enforces exact compact maxBytes accounting across entry commas and sequence growth', async () => {
    const referencePath = await outboxPath()
    const reference = new FileFactoryCloudEventOutbox({ path: referencePath })
    await reference.enqueue(instance('boot-1'), event('event-1', 'instance.heartbeat'))
    await reference.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat'))
    const exactBytes = await compactFileBytes(referencePath)

    const exact = new FileFactoryCloudEventOutbox({ path: await outboxPath(), maxBytes: exactBytes })
    await exact.enqueue(instance('boot-1'), event('event-1', 'instance.heartbeat'))
    expect(await exact.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat')))
      .toMatchObject({ droppedEvents: 0, pending: 2 })

    const oneByteUnder = new FileFactoryCloudEventOutbox({
      path: await outboxPath(),
      maxBytes: exactBytes - 1,
    })
    await oneByteUnder.enqueue(instance('boot-1'), event('event-1', 'instance.heartbeat'))
    expect(await oneByteUnder.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat')))
      .toMatchObject({ droppedEvents: 1, pending: 1 })
    expect((await oneByteUnder.peekBatch())?.events.map(({ id }) => id)).toEqual(['event-2'])
  })

  it('uses compact maxBytes semantics when migrating a legacy pretty-printed file', async () => {
    const path = await outboxPath()
    const reference = new FileFactoryCloudEventOutbox({ path })
    await reference.enqueue(instance('boot-1'), event('event-1', 'instance.heartbeat'))
    await reference.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat'))
    await reference.enqueue(instance('boot-1'), event('event-3', 'instance.heartbeat'))
    const exactThreeEventBytes = await compactFileBytes(path)
    const parsed = JSON.parse(await readFile(path, 'utf8')) as unknown

    const legacyPath = await outboxPath()
    const legacyTwoEvents = {
      ...(parsed as Record<string, unknown>),
      nextSequence: 3,
      entries: (parsed as { entries: unknown[] }).entries.slice(0, 2),
    }
    await writeFile(legacyPath, `${JSON.stringify(legacyTwoEvents, null, 2)}\n`)
    const migrated = new FileFactoryCloudEventOutbox({ path: legacyPath, maxBytes: exactThreeEventBytes })

    expect(await migrated.enqueue(instance('boot-1'), event('event-3', 'instance.heartbeat')))
      .toMatchObject({ droppedEvents: 0, pending: 3 })
    expect(await compactFileBytes(legacyPath)).toBe(exactThreeEventBytes)
  })

  it('never mixes persisted boot identities in one request batch', async () => {
    const outbox = new FileFactoryCloudEventOutbox({ path: await outboxPath() })
    await outbox.enqueue(instance('boot-old'), event('event-old', 'instance.stopped'))
    await outbox.enqueue(instance('boot-new'), event('event-new', 'instance.started'))

    const oldBatch = await outbox.peekBatch()
    expect(oldBatch?.instance.bootId).toBe('boot-old')
    expect(oldBatch?.events.map(({ id }) => id)).toEqual(['event-old'])
    await outbox.acknowledge(['event-old'])
    expect((await outbox.peekBatch())?.instance.bootId).toBe('boot-new')
  })

  it('fails lock acquisition within a bounded window instead of stalling Factory', async () => {
    const path = await outboxPath()
    const release = await lockfile.lock(path, { realpath: false })
    try {
      const outbox = new FileFactoryCloudEventOutbox({ path, lockRetries: 0 })
      const startedAt = Date.now()
      await expect(outbox.enqueue(instance('boot-1'), event('event-locked', 'instance.heartbeat')))
        .rejects.toMatchObject({ code: 'ELOCKED' })
      expect(Date.now() - startedAt).toBeLessThan(500)
    } finally {
      await release()
    }
  })

  it('bounds serialized batches by both event count and request bytes', async () => {
    const outbox = new FileFactoryCloudEventOutbox({ path: await outboxPath() })
    await outbox.enqueue(instance('boot-1'), event('event-1', 'instance.heartbeat'))
    await outbox.enqueue(instance('boot-1'), event('event-2', 'instance.heartbeat'))
    const oneEvent = await outbox.peekBatch(1)
    if (!oneEvent) throw new Error('expected one persisted event')
    const maxPayloadBytes = Buffer.byteLength(JSON.stringify(oneEvent), 'utf8')

    const bounded = await outbox.peekBatch(100, maxPayloadBytes)
    expect(bounded?.events.map(({ id }) => id)).toEqual(['event-1'])
    expect(Buffer.byteLength(JSON.stringify(bounded), 'utf8')).toBeLessThanOrEqual(maxPayloadBytes)
  })

  it('drops an oversized head durably so later deliverable events are not poisoned', async () => {
    const outbox = new FileFactoryCloudEventOutbox({ path: await outboxPath() })
    await outbox.enqueue(instance('boot-1'), event('oversized', 'instance.heartbeat', {
      attributes: { nodeId: 'x'.repeat(256) },
    }))
    await outbox.enqueue(instance('boot-1'), event('deliverable', 'instance.heartbeat'))
    const all = await outbox.peekBatch()
    if (!all) throw new Error('expected persisted events')
    const deliverableOnly = { ...all, events: [all.events[1]] }
    const maxPayloadBytes = Buffer.byteLength(JSON.stringify(deliverableOnly), 'utf8')

    const batch = await outbox.peekBatch(100, maxPayloadBytes)
    expect(batch?.events.map(({ id }) => id)).toEqual(['deliverable'])
    expect(await outbox.stats()).toMatchObject({ pending: 1, droppedEvents: 1 })
  })
})

const instance = (bootId: string): FactoryCloudInstanceV1 => ({
  id: 'instance-1',
  bootId,
  version: '0.1.32',
  metadata: { backend: 'internal', mode: 'live' },
})

const event = (
  id: string,
  type: string,
  extra: Record<string, unknown> = {},
) => createFactoryCloudEventV1({ type, id, ...extra }, {
  now: () => new Date('2026-07-19T12:00:00.000Z'),
})

async function outboxPath(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-cloud-outbox-'))
  roots.push(root)
  return join(root, 'events.json')
}

async function compactFileBytes(path: string): Promise<number> {
  const content = await readFile(path, 'utf8')
  return Buffer.byteLength(content.endsWith('\n') ? content.slice(0, -1) : content, 'utf8')
}
