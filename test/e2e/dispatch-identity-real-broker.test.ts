import { createHash, randomUUID } from 'node:crypto'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { HarnessDriverClient } from '@agent-relay/harness-driver'
import { AgentRelay } from '@agent-relay/sdk'
import { describe, expect, it } from 'vitest'

import { dispatchAgentIdentityKey } from '../../src/dispatch/work-unit-identity'

const workspaceKey = process.env.RELAY_WORKSPACE_KEY
  ?? process.env.AGENT_RELAY_WORKSPACE_KEY
  ?? process.env.RELAY_API_KEY
const runRealBroker = process.env.FACTORY_REAL_BROKER_IDENTITY_E2E === '1' && Boolean(workspaceKey)
const realBrokerIt = runRealBroker ? it : it.skip

describe('dispatch identity against a real Agent Relay broker', () => {
  realBrokerIt('reclaims the same work unit and rejects a different one using the same name', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-dispatch-identity-'))
    const name = `factory-identity-e2e-${process.pid}-${randomUUID().slice(0, 8)}`
    const issue = {
      uuid: `AgentWorkforce/factory#${process.pid}`,
      key: String(process.pid),
      path: `/github/repos/AgentWorkforce__factory/issues/by-id/${process.pid}.json`,
    }
    const ownerKey = dispatchAgentIdentityKey(issue, 'reviewer')
    const differentKey = dispatchAgentIdentityKey({
      ...issue,
      uuid: `AgentWorkforce/cloud#${process.pid}`,
      path: `/github/repos/AgentWorkforce__cloud/issues/by-id/${process.pid}.json`,
    }, 'reviewer')
    const relay = new AgentRelay({ workspaceKey: workspaceKey! })
    let active: HarnessDriverClient | undefined

    const spawnBroker = async (identityKey: string, attempt: number): Promise<HarnessDriverClient> =>
      await HarnessDriverClient.spawn({
        brokerName: name,
        workspaceKey: workspaceKey!,
        cwd: root,
        binaryArgs: {
          apiPort: 0,
          persist: true,
          stateDir: join(root, `attempt-${attempt}`),
        },
        env: {
          ...process.env,
          RELAY_AGENT_IDENTITY_KEY: identityKey,
          RELAY_AGENT_TYPE: 'agent',
          RELAY_STRICT_AGENT_NAME: '1',
          AGENT_RELAY_HANDSHAKE_TIMEOUT_MS: '15000',
          AGENT_RELAY_HANDSHAKE_ATTEMPTS: '3',
        },
        startupTimeoutMs: 60_000,
      })

    const killBroker = async (client: HarnessDriverClient): Promise<void> => {
      const pid = client.brokerPid
      if (!pid) throw new Error('real-broker identity test did not receive a broker PID')
      const exited = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error(`broker ${pid} did not exit after SIGKILL`)), 10_000)
        const unsubscribe = client.onBrokerExit(() => {
          clearTimeout(timeout)
          unsubscribe()
          resolve()
        })
      })
      process.kill(pid, 'SIGKILL')
      await exited
      client.disconnect()
    }

    try {
      active = await spawnBroker(ownerKey, 1)
      const first = await relay.messaging.agents.get(name)
      expect(first.metadata?.identity_key).toBe(createHash('sha256').update(ownerKey).digest('hex'))
      await killBroker(active)
      active = undefined
      await new Promise<void>((resolve) => setTimeout(resolve, 1_000))

      active = await spawnBroker(ownerKey, 2)
      const reclaimed = await relay.messaging.agents.get(name)
      expect(reclaimed.id).toBe(first.id)
      expect(reclaimed.metadata?.identity_key).toBe(first.metadata?.identity_key)
      await killBroker(active)
      active = undefined

      await expect(spawnBroker(differentKey, 3)).rejects.toThrow(
        /did not prove ownership of that identity|agent_identity_mismatch/iu,
      )
      const stillOwned = await relay.messaging.agents.get(name)
      expect(stillOwned.id).toBe(first.id)
      expect(stillOwned.metadata?.identity_key).toBe(first.metadata?.identity_key)
    } finally {
      if (active) await killBroker(active).catch(() => undefined)
      await relay.messaging.agents.delete(name).catch(() => undefined)
      await rm(root, { recursive: true, force: true })
    }
  }, 180_000)
})
