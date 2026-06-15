import { describe, expect, it, vi } from 'vitest'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import { LINEAR_STATE_IDS } from '../constants/linear'
import type { CloseProbePrInput, Factory } from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { installFactoryStopSignalHandlers, parseFleetCommand, parseGlobalOptions, resolveBrokerConnectionPath, runFleetCli } from './fleet'

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

  it('parses the factory orphan reaper command', () => {
    expect(parseFleetCommand(['factory', 'reap-orphans'])).toEqual({
      kind: 'factory',
      action: 'reap-orphans',
    })
  })

  it('parses the factory live start command', () => {
    expect(parseFleetCommand(['factory', 'start', '--mode', 'live'])).toEqual({
      kind: 'factory',
      action: 'start',
      mode: 'live',
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
  it('uses real fleet and cloud mount for fixture-less factory configs on the operator path', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-real-default-'))
    try {
      const configPath = await writeConfig(root)
      const realFleet = new FakeFleetClient()
      const realMount = new FakeMountClient({ [issuePath]: issueFile })
      const dispose = vi.spyOn(realFleet, 'dispose')
      const createFleetCalls: unknown[] = []
      const cloudMountCalls: unknown[] = []
      const output = buffer()

      const code = await runFleetCli([
        'factory',
        'run-once',
        '--dry-run',
        '--config',
        configPath,
      ], {
        createFleet: (opts) => {
          createFleetCalls.push(opts)
          return realFleet
        },
        cloudMountFromConfig: async (opts) => {
          cloudMountCalls.push(opts)
          return realMount
        },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(createFleetCalls).toHaveLength(1)
      expect(cloudMountCalls).toHaveLength(1)
      expect(dispose).toHaveBeenCalledTimes(1)
      const report = JSON.parse(output.text())
      expect(report).toMatchObject({
        dryRun: true,
        pulled: [{ key: 'AR-77' }],
        dispatched: [{ issue: { key: 'AR-77' } }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps explicit fixtureFiles configs on Fake fleet and mount for harness runs', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-fixture-opt-in-'))
    try {
      const configPath = await writeConfig(root, {
        fixtureFiles: { [issuePath]: issueFile },
      })
      const output = buffer()

      const code = await runFleetCli([
        'factory',
        'run-once',
        '--dry-run',
        '--config',
        configPath,
      ], {
        createFleet: () => {
          throw new Error('real fleet should not be selected for fixture config')
        },
        cloudMountFromConfig: async () => {
          throw new Error('real mount should not be selected for fixture config')
        },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      const report = JSON.parse(output.text())
      expect(report).toMatchObject({
        dryRun: true,
        pulled: [{ key: 'AR-77' }],
        dispatched: [{ issue: { key: 'AR-77' } }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('disposes a one-shot fleet when event subscription setup throws during factory construction', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-connect-throw-'))
    try {
      const configPath = await writeConfig(root)
      const fleet = new FakeFleetClient()
      const dispose = vi.spyOn(fleet, 'dispose')
      vi.spyOn(fleet, 'onAgentExit').mockImplementation(() => {
        throw new Error('connect failed after opening event stream')
      })
      const stderr = buffer()

      const code = await runFleetCli([
        'factory',
        'run-once',
        '--config',
        configPath,
      ], {
        fleet,
        mount: new FakeMountClient(),
        stdout: buffer(),
        stderr,
      })

      expect(code).toBe(1)
      expect(stderr.text()).toContain('connect failed after opening event stream')
      expect(dispose).toHaveBeenCalledTimes(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

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
      dispatched: [{ issue: { key: 'AR-77' }, dryRun: true, agents: [{ name: 'ar-77-impl-pear' }, { name: 'ar-77-review' }] }],
    })
    expect(fleet.spawns).toEqual([])
    expect(fleet.messages).toEqual([])
    expect(fleet.inputs).toEqual([])
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

  it('runs factory loop through the bounded runner and emits a heartbeat-backed status', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-loop-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { maxIterations: 2, heartbeatPath, heartbeatStaleMs: 10_000 } })
      const fleet = new FakeFleetClient()
      const mount = new FakeMountClient({ [issuePath]: issueFile })
      const output = buffer()

      const code = await runFleetCli([
        'factory',
        'loop',
        '--dry-run',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      const result = JSON.parse(output.text())
      expect(result.reports).toHaveLength(2)
      expect(result.status.counters.loopIdle).toBe(1)
      const heartbeat = JSON.parse(await readFile(heartbeatPath, 'utf8'))
      expect(heartbeat).toMatchObject({ status: 'idle', iteration: 2, maxIterations: 2 })

      const statusOut = buffer()
      const statusCode = await runFleetCli([
        'factory',
        'loop-status',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        stdout: statusOut,
        stderr: buffer(),
      })
      expect(statusCode).toBe(0)
      expect(JSON.parse(statusOut.text())).toMatchObject({ ok: true, stale: false })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('starts the factory live daemon and waits for an injected stop signal boundary', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-'))
    try {
      const configPath = await writeConfig(root)
      const fleet = new FakeFleetClient()
      const mount = new FakeMountClient()
      const factory = {
        start: vi.fn(async () => {}),
        stop: vi.fn(async () => {}),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const waitForStopSignal = vi.fn(async () => undefined)
      const createFactory = vi.fn(() => factory)

      const code = await runFleetCli([
        'factory',
        'start',
        '--mode',
        'live',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        createFactory,
        waitForStopSignal,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(createFactory).toHaveBeenCalledTimes(1)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
      expect(factory.runLoop).not.toHaveBeenCalled()
      expect(waitForStopSignal).toHaveBeenCalledTimes(1)
      expect(factory.stop).toHaveBeenCalledTimes(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('factory start exits cleanly on SIGTERM after the signal handler stops the factory once', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-sigterm-'))
    try {
      const configPath = await writeConfig(root)
      const listeners = new Map<string, () => void>()
      const calls: string[] = []
      const processLike = {
        once(signal: string, listener: () => void) {
          listeners.set(signal, listener)
          return processLike
        },
        off(signal: string, listener: () => void) {
          if (listeners.get(signal) === listener) listeners.delete(signal)
          return processLike
        },
      }
      const factory = {
        start: vi.fn(async () => {}),
        stop: vi.fn(async () => {
          calls.push('stop')
        }),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const createFactory = vi.fn(() => factory)
      const daemonExits: number[] = []

      const run = runFleetCli([
        'factory',
        'start',
        '--mode',
        'live',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory,
        stopSignalProcessLike: processLike as unknown as Pick<NodeJS.Process, 'once' | 'off'>,
        flushDaemonOutput: async () => {
          calls.push('flush')
        },
        daemonExit: (code) => {
          calls.push('exit')
          daemonExits.push(code)
        },
        stdout: buffer(),
        stderr: buffer(),
      })
      await vi.waitFor(() => {
        expect(listeners.has('SIGTERM')).toBe(true)
      })
      listeners.get('SIGTERM')?.()

      await expect(run).resolves.toBe(0)
      expect(factory.stop).toHaveBeenCalledTimes(1)
      expect(calls).toEqual(['stop', 'flush', 'exit'])
      expect(daemonExits).toEqual([0])
      expect(listeners.size).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not force process exit for one-shot factory commands', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-one-shot-no-force-exit-'))
    try {
      const configPath = await writeConfig(root)
      const daemonExits: number[] = []
      const daemonFlushes: string[] = []
      const runOnceFactory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({ pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: true })),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory

      const runOnceCode = await runFleetCli([
        '--dry-run',
        'factory',
        'run-once',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => runOnceFactory,
        daemonExit: (code) => {
          daemonExits.push(code)
        },
        flushDaemonOutput: async () => {
          daemonFlushes.push('flush')
        },
        stdout: buffer(),
        stderr: buffer(),
      })

      const reapCode = await runFleetCli([
        'factory',
        'reap-orphans',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        daemonExit: (code) => {
          daemonExits.push(code)
        },
        flushDaemonOutput: async () => {
          daemonFlushes.push('flush')
        },
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(runOnceCode).toBe(0)
      expect(reapCode).toBe(0)
      expect(daemonExits).toEqual([])
      expect(daemonFlushes).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('factory kill-loop sends SIGTERM to the heartbeat pid', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-kill-'))
    const originalKill = process.kill
    const killed: Array<{ pid: number; signal?: NodeJS.Signals | number }> = []
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { maxIterations: 2, heartbeatPath, heartbeatStaleMs: 10_000 } })
      await writeFile(heartbeatPath, JSON.stringify({
        pid: 4242,
        status: 'running',
        iteration: 1,
        maxIterations: 2,
        updatedAt: new Date().toISOString(),
        updatedAtMs: Date.now(),
      }))
      process.kill = ((pid: number, signal?: NodeJS.Signals | number) => {
        killed.push({ pid, signal })
        return true
      }) as typeof process.kill

      const output = buffer()
      const code = await runFleetCli([
        'factory',
        'kill-loop',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(killed).toEqual([{ pid: 4242, signal: 'SIGTERM' }])
      expect(JSON.parse(output.text())).toEqual({ killed: 4242, signal: 'SIGTERM' })
    } finally {
      process.kill = originalKill
      await rm(root, { recursive: true, force: true })
    }
  })

  it('factory reap-orphans reports fresh heartbeat without killing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-reaper-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      const configPath = await writeConfig(root, { loop: { maxIterations: 2, heartbeatPath, registryPath, heartbeatStaleMs: 10_000 } })
      const fleet = new FakeFleetClient()
      const dispose = vi.spyOn(fleet, 'dispose')
      const createFleetCalls: unknown[] = []
      const cloudMountFromConfig = vi.fn(async () => new FakeMountClient())
      await writeFile(heartbeatPath, JSON.stringify({
        pid: 4242,
        status: 'running',
        iteration: 1,
        maxIterations: 2,
        updatedAt: new Date().toISOString(),
        updatedAtMs: Date.now(),
        registryPath,
      }))
      await writeFile(registryPath, JSON.stringify({ pid: 4242, updatedAt: new Date().toISOString(), updatedAtMs: Date.now(), agents: [] }))
      const output = buffer()

      const code = await runFleetCli([
        'factory',
        'reap-orphans',
        '--config',
        configPath,
      ], {
        createFleet: (opts) => {
          createFleetCalls.push(opts)
          return fleet
        },
        cloudMountFromConfig,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(createFleetCalls).toHaveLength(1)
      expect(cloudMountFromConfig).not.toHaveBeenCalled()
      expect(dispose).toHaveBeenCalledTimes(1)
      expect(JSON.parse(output.text())).toMatchObject({ stale: false, reaped: [], skipped: [] })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('signal handlers exit 0 after clean graceful stop and unregister themselves', async () => {
    const calls: string[] = []
    const listeners = new Map<string, () => void>()
    const processLike = {
      once(signal: string, listener: () => void) {
        listeners.set(signal, listener)
        return processLike
      },
      off(signal: string, listener: () => void) {
        if (listeners.get(signal) === listener) listeners.delete(signal)
        return processLike
      },
    }
    const factory = {
      stop: vi.fn(async () => {
        calls.push('stop')
      }),
    } as unknown as Factory
    const exits: number[] = []

    installFactoryStopSignalHandlers(factory, {
      processLike: processLike as unknown as Pick<NodeJS.Process, 'once' | 'off'>,
      exit: (code) => {
        calls.push('exit')
        exits.push(code)
      },
    })
    listeners.get('SIGTERM')?.()
    await flush()

    expect(factory.stop).toHaveBeenCalledTimes(1)
    expect(calls).toEqual(['stop', 'exit'])
    expect(exits).toEqual([0])
    expect(listeners.size).toBe(0)
  })

  it('signal handlers exit 1 when local teardown rejects', async () => {
    const calls: string[] = []
    const listeners = new Map<string, () => void>()
    const processLike = {
      once(signal: string, listener: () => void) {
        listeners.set(signal, listener)
        return processLike
      },
      off(signal: string, listener: () => void) {
        if (listeners.get(signal) === listener) listeners.delete(signal)
        return processLike
      },
    }
    const factory = {
      stop: vi.fn(async () => {
        calls.push('stop')
        throw new Error('dispose failed')
      }),
    } as unknown as Factory
    const exits: number[] = []

    installFactoryStopSignalHandlers(factory, {
      processLike: processLike as unknown as Pick<NodeJS.Process, 'once' | 'off'>,
      exit: (code) => {
        calls.push('exit')
        exits.push(code)
      },
    })
    listeners.get('SIGTERM')?.()
    await flush()

    expect(factory.stop).toHaveBeenCalledTimes(1)
    expect(calls).toEqual(['stop', 'exit'])
    expect(exits).toEqual([1])
    expect(listeners.size).toBe(0)
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

const writeConfig = async (root: string, overrides: Record<string, unknown> = {}): Promise<string> => {
  const path = join(root, 'factory.config.json')
  await writeFile(path, JSON.stringify({
    ...config,
    ...overrides,
  }))
  return path
}

const flush = async () => {
  await new Promise((resolve) => setTimeout(resolve, 0))
}
