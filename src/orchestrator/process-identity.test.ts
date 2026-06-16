import { describe, expect, it } from 'vitest'

import { findAgentProcessByName, type ProcessIdentity } from './process-identity'

describe('process identity lookup', () => {
  it('matches an anchored agent-name token and rejects loose substrings', async () => {
    const identities = new Map<number, ProcessIdentity>([
      [101, { pid: 101, startTime: 'start-101', cmdline: 'node --agent-name ar-1-impl worker' }],
      [102, { pid: 102, startTime: 'start-102', cmdline: 'node --agent-name ar-1-impl-extra worker' }],
      [103, { pid: 103, startTime: 'start-103', cmdline: 'node --agent-name ar-10-impl worker' }],
    ])

    await expect(findAgentProcessByName('ar-1-impl', {
      listPidsByCommand: async () => [101, 102, 103],
      readProcessIdentity: async (pid) => identities.get(pid),
    })).resolves.toEqual({
      status: 'found',
      identity: identities.get(101),
    })
  })

  it('fails closed when no anchored agent-name process exists', async () => {
    await expect(findAgentProcessByName('ar-2-impl', {
      listPidsByCommand: async () => [201],
      readProcessIdentity: async (pid) => ({ pid, startTime: 'start-201', cmdline: 'node --agent-name ar-20-impl worker' }),
    })).resolves.toEqual({ status: 'missing' })
  })

  it('fails closed when multiple anchored agent-name processes match', async () => {
    await expect(findAgentProcessByName('ar-3-impl', {
      listPidsByCommand: async () => [301, 302],
      readProcessIdentity: async (pid) => ({ pid, startTime: `start-${pid}`, cmdline: 'node --agent-name ar-3-impl worker' }),
      readParentPid: async () => undefined,
    })).resolves.toEqual({ status: 'ambiguous' })
  })

  it('allows multiple anchored matches only when they form one agent tree and returns the launcher root', async () => {
    const identities = new Map<number, ProcessIdentity>([
      [501, { pid: 501, startTime: 'launcher-start', cmdline: 'node --agent-name ar-5-review launcher' }],
      [502, { pid: 502, startTime: 'worker-start', cmdline: 'node --agent-name ar-5-review worker' }],
    ])

    await expect(findAgentProcessByName('ar-5-review', {
      listPidsByCommand: async () => [501, 502],
      readProcessIdentity: async (pid) => identities.get(pid),
      readParentPid: async (pid) => pid === 502 ? 501 : undefined,
    })).resolves.toEqual({
      status: 'found',
      identity: identities.get(501),
    })
  })

  it('excludes protected PIDs from scan results', async () => {
    await expect(findAgentProcessByName('ar-4-impl', {
      protectedPids: [401],
      listPidsByCommand: async () => [401],
      readProcessIdentity: async (pid) => ({ pid, startTime: 'broker-start', cmdline: 'node --agent-name ar-4-impl broker' }),
    })).resolves.toEqual({ status: 'missing' })
  })
})
