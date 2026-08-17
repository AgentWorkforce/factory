import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { describe, expect, it } from 'vitest'

import type { DispatchLifecycle } from '../ports/state'
import type { WatchStateDocument, WatchStateDocumentStore } from './document-store'
import { DocumentStateStore } from './file-state-store'

describe('DocumentStateStore durable restart boundary', () => {
  it('does not redispatch a claimed lifecycle after the process and local disk are replaced', async () => {
    const localDisk = await mkdtemp(join(tmpdir(), 'factory-ephemeral-state-'))
    let durableDocument: WatchStateDocument = { version: 3, workspaces: {} }
    const adapter = (): WatchStateDocumentStore => ({
      read: async () => structuredClone(durableDocument),
      write: async (document) => {
        durableDocument = structuredClone(document)
      },
      runMutation: async (operation) => await operation(),
      assertReady: async () => {},
    })

    try {
      const firstProcess = new DocumentStateStore({ batchSize: 2, documentStore: adapter() })
      const firstClaim = await firstProcess.claimDispatchLifecycle(
        'workspace',
        'AgentWorkforce/factory#268',
        lifecycle(),
        'first-owner',
        1_000,
        60_000,
      )
      expect(firstClaim).toMatchObject({ acquired: true, created: true })
      await writeFile(join(localDisk, 'state.json'), JSON.stringify({ ignored: true }))

      await rm(localDisk, { recursive: true, force: true })
      const restartedProcess = new DocumentStateStore({ batchSize: 2, documentStore: adapter() })
      const competingClaim = await restartedProcess.claimDispatchLifecycle(
        'workspace',
        'AgentWorkforce/factory#268',
        lifecycle(),
        'second-owner',
        1_001,
        60_000,
      )

      expect(competingClaim).toMatchObject({
        acquired: false,
        created: false,
        lifecycle: { lease: { owner: 'first-owner' } },
      })
    } finally {
      await rm(localDisk, { recursive: true, force: true })
    }
  })
})

const lifecycle = (): DispatchLifecycle => ({
  runId: 'run-268',
  issue: {
    uuid: 'AgentWorkforce/factory#268',
    key: '268',
    path: '/github/issues/AgentWorkforce__factory/268.json',
  },
  decision: {
    issue: {
      uuid: 'AgentWorkforce/factory#268',
      key: '268',
      path: '/github/issues/AgentWorkforce__factory/268.json',
    },
    routes: [{ repo: 'AgentWorkforce/factory', rationale: 'durability regression' }],
    scope: 'single',
    implementers: [],
    reviewer: {
      name: 'ar-268-review',
      role: 'reviewer',
      capability: 'spawn:codex',
      task: 'review',
      repo: 'AgentWorkforce/factory',
    },
    thin: false,
    confidence: 'high',
    rationale: 'durability regression',
  },
  dryRun: false,
  phase: 'dispatching',
  agents: [],
  invocationIds: [],
  updatedAtMs: 1_000,
})
