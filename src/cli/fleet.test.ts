import { describe, expect, it } from 'vitest'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import { LINEAR_STATE_IDS } from '../constants/linear'
import type { CloseProbePrInput } from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { parseFleetCommand, parseGlobalOptions, resolveBrokerConnectionPath, runFleetCli } from './fleet'

const issuePath = '/linear/issues/AR-77__uuid-77.json'

const config = {
  workspaceId: 'factory-cli-test',
  repos: {
    byLabel: { pear: 'AgentWorkforce/pear' },
    clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
    default: 'AgentWorkforce/pear',
  },
  stateIds: LINEAR_STATE_IDS,
}

const issueFile = {
  provider: 'linear',
  objectType: 'issue',
  objectId: 'uuid-77',
  payload: {
    id: 'uuid-77',
    identifier: 'AR-77',
    title: '[factory-e2e] CLI dry run',
    description: 'Implement a small fix in packages/factory-sdk/src/cli/fleet.ts and verify with tests. Ensure the fleet CLI parses arguments, calls the SDK facades, prints an IterationReport, and keeps dry-run execution free of writes or spawns.',
    url: 'https://linear.app/agent-relay/issue/AR-77/cli-dry-run',
    stateId: LINEAR_STATE_IDS.readyForAgent,
    labels: ['pear'],
    team: { key: 'AR', name: 'Agent Relay' },
    state: { id: LINEAR_STATE_IDS.readyForAgent, name: 'Ready for Agent' },
  },
}

describe('fleet CLI parsing', () => {
  it('parses spawn flags into a FleetClient spawn input shape', () => {
    expect(parseFleetCommand([
      'spawn',
      'spawn:codex',
      '--node',
      'self',
      '--name',
      'agent-a',
      '--task',
      'do work',
      '--model',
      'codex',
      '--cwd',
      '/work',
    ])).toEqual({
      kind: 'spawn',
      input: {
        capability: 'spawn:codex',
        node: 'self',
        name: 'agent-a',
        task: 'do work',
        model: 'codex',
        cwd: '/work',
        sessionRef: undefined,
      },
    })
  })

  it('parses global backend, config, and dry-run independently of subcommand position', () => {
    expect(parseGlobalOptions([
      'factory',
      'run-once',
      '--dry-run',
      '--backend',
      'relay',
      '--config',
      'factory.json',
    ])).toEqual({
      globals: { backend: 'relay', dryRun: true, config: 'factory.json' },
      args: ['factory', 'run-once'],
    })
  })

  it('parses manual probe close command', () => {
    expect(parseFleetCommand([
      'factory',
      'close-probe',
      '42',
      '--repo',
      'AgentWorkforce/pear',
      '--issue',
      'AR-77',
    ])).toEqual({
      kind: 'factory-close-probe',
      prNumber: 42,
      repo: 'AgentWorkforce/pear',
      issue: 'AR-77',
    })
  })

  it('resolves a broker connection path by walking up from the command cwd', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-broker-'))
    try {
      const connectionPath = join(root, '.agentworkforce', 'relay', 'connection.json')
      const nested = join(root, 'packages', 'factory-sdk')
      await mkdir(dirname(connectionPath), { recursive: true })
      await mkdir(nested, { recursive: true })
      await writeFile(connectionPath, JSON.stringify({ port: 3890 }))

      expect(resolveBrokerConnectionPath(nested)).toBe(connectionPath)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

describe('fleet CLI runtime', () => {
  it('runs factory run-once dry-run over FleetClient and MountClient fakes with zero writes and zero spawns', async () => {
    const fleet = new FakeFleetClient()
    const mount = new FakeMountClient({ [issuePath]: issueFile })
    const output = buffer()

    const code = await runFleetCli([
      'factory',
      'run-once',
      '--dry-run',
      '--config',
      'packages/factory-sdk/test/fixtures/factory.config.json',
    ], {
      fleet,
      mount,
      stdout: output,
      stderr: buffer(),
    })

    expect(code).toBe(0)
    const report = JSON.parse(output.text())
    expect(report).toMatchObject({
      dryRun: true,
      pulled: [{ key: 'AR-77' }],
      dispatched: [{ issue: { key: 'AR-77' }, dryRun: true, agents: [{ name: 'ar-77-impl' }, { name: 'ar-77-review' }] }],
    })
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
  })

  it('selects the RelayFleetClient stub when --backend relay is requested', async () => {
    const output = buffer()
    const errors = buffer()

    const code = await runFleetCli(['roster', '--backend', 'relay'], {
      stdout: output,
      stderr: errors,
    })

    expect(code).toBe(1)
    expect(errors.text()).toContain('RelayFleetClient not implemented')
  })

  it('refuses targeted factory dispatch for an issue outside factory-e2e scope', async () => {
    const fleet = new FakeFleetClient()
    const mount = new FakeMountClient({
      [issuePath]: {
        ...issueFile,
        payload: {
          ...issueFile.payload,
          title: 'Real ready AR issue without synthetic marker',
        },
      },
    })
    const errors = buffer()

    const code = await runFleetCli([
      'factory',
      'dispatch',
      'AR-77',
      '--config',
      'packages/factory-sdk/test/fixtures/factory.config.json',
    ], {
      fleet,
      mount,
      stdout: buffer(),
      stderr: errors,
    })

    expect(code).toBe(1)
    expect(errors.text()).toContain('not factory-e2e scope')
    expect(fleet.spawns).toEqual([])
    expect(mount.writes).toEqual([])
  })

  it('runs manual close-probe through the injectable probe closer', async () => {
    const output = buffer()
    const calls: unknown[] = []
    const code = await runFleetCli([
      'factory',
      'close-probe',
      '42',
      '--repo',
      'AgentWorkforce/pear',
      '--issue',
      'AR-77',
    ], {
      stdout: output,
      stderr: buffer(),
      probeCloser: async (input: Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey'>) => {
        calls.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    expect(code).toBe(0)
    expect(calls).toEqual([{ repo: 'AgentWorkforce/pear', prNumber: 42, expectedIssueKey: 'AR-77' }])
    expect(JSON.parse(output.text())).toEqual({ repo: 'AgentWorkforce/pear', prNumber: 42, state: 'CLOSED' })
  })
})

const buffer = () => {
  let value = ''
  return {
    write(chunk: string) {
      value += chunk
      return true
    },
    text() {
      return value
    },
  }
}
