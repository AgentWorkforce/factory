import { describe, expect, it, vi } from 'vitest'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import type { CloseProbePrInput, Factory, FactoryPorts, createFactory } from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { installFactoryStopSignalHandlers, parseFleetCommand, parseGlobalOptions, resolveBrokerConnectionPath, runFleetCli } from './fleet'

const issuePath = '/linear/issues/AR-77__uuid-77.json'

// Explicit state UUIDs for the CLI tests (pinned via config.stateIds, which the
// resolver uses directly without reading /linear/states).
const TEST_STATE_IDS = {
  readyForAgent: 'state-ready-for-agent',
  agentImplementing: 'state-agent-implementing',
  done: 'state-done',
  inPlanning: 'state-in-planning',
  humanReview: 'state-human-review',
}

const config = {
  workspaceId: 'factory-cli-test',
  repos: {
    byLabel: { pear: 'AgentWorkforce/pear' },
    clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
    default: 'AgentWorkforce/pear',
  },
  stateIds: TEST_STATE_IDS,
}

const issueFile = {
  provider: 'linear',
  objectType: 'issue',
  objectId: 'uuid-77',
  payload: {
    id: 'uuid-77',
    identifier: 'AR-77',
    title: '[factory-e2e] CLI dry run',
    description: 'Implement a small fix in src/cli/fleet.ts and verify with tests. Ensure the fleet CLI parses arguments, calls the SDK facades, prints an IterationReport, and keeps dry-run execution free of writes or spawns.',
    url: 'https://linear.app/agent-relay/issue/AR-77/cli-dry-run',
    stateId: TEST_STATE_IDS.readyForAgent,
    labels: ['pear'],
    team: { key: 'AR', name: 'Agent Relay' },
    state: { id: TEST_STATE_IDS.readyForAgent, name: 'Ready for Agent' },
  },
}

const githubIssueFile = (repo: string, number = 48) => ({
  provider: 'github',
  objectType: 'issue',
  objectId: `${repo}-${number}`,
  payload: {
    number,
    title: `GitHub-only ${repo} issue`,
    body: 'Dispatch the repository-qualified GitHub issue.',
    state: 'open',
    labels: [{ name: 'factory' }, { name: repo }],
    url: `https://github.com/AgentWorkforce/${repo}/issues/${number}`,
    repository: { name: repo, owner: { login: 'AgentWorkforce' } },
  },
})

describe('fleet CLI parsing', () => {
  it('parses spawn flags into a FleetClient spawn input shape', () => {
    expect(parseFleetCommand([
      'fleet',
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
      'run-once',
      '--dry-run',
      '--backend',
      'relay',
      '--config',
      'factory.json',
      '--agent-exit-timeout',
      '45000',
    ])).toEqual({
      globals: { backend: 'relay', dryRun: true, config: 'factory.json', agentExitTimeoutMs: 45_000 },
      args: ['run-once'],
    })
  })

  it('reads the owned-broker agent exit timeout from the environment and lets the CLI override it', () => {
    vi.stubEnv('FACTORY_AGENT_EXIT_TIMEOUT_MS', '30000')
    try {
      expect(parseGlobalOptions(['roster'])).toEqual({
        globals: { backend: 'internal', dryRun: false, agentExitTimeoutMs: 30_000 },
        args: ['roster'],
      })
      expect(parseGlobalOptions(['roster', '--agent-exit-timeout', '45000']).globals.agentExitTimeoutMs).toBe(45_000)

      vi.stubEnv('FACTORY_AGENT_EXIT_TIMEOUT_MS', 'invalid')
      expect(parseGlobalOptions(['roster'])).toEqual({
        globals: { backend: 'internal', dryRun: false },
        args: ['roster'],
      })
    } finally {
      vi.unstubAllEnvs()
    }
  })

  it('rejects invalid owned-broker agent exit timeouts', () => {
    expect(() => parseGlobalOptions(['roster', '--agent-exit-timeout', '0'])).toThrow(
      '--agent-exit-timeout must be a positive integer number of milliseconds',
    )
  })

  it('forwards the CLI agent exit timeout into fleet construction', async () => {
    const fleet = new FakeFleetClient()
    const createFleetDeps: unknown[] = []

    const code = await runFleetCli(['fleet', 'roster', '--agent-exit-timeout', '45000'], {
      createFleet: (_options, deps) => {
        createFleetDeps.push(deps)
        return fleet
      },
      stdout: buffer(),
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(createFleetDeps).toEqual([{ ownedBrokerAgentExitTimeoutMs: 45_000 }])
  })

  it('parses manual probe close command', () => {
    expect(parseFleetCommand([
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
    expect(parseFleetCommand(['reap-orphans'])).toEqual({
      kind: 'factory',
      action: 'reap-orphans',
    })
  })

  it('parses the factory live start command', () => {
    expect(parseFleetCommand(['start', '--mode', 'live'])).toEqual({
      kind: 'factory',
      action: 'start',
      mode: 'live',
    })
  })

  it('defaults factory start to live mode', () => {
    expect(parseFleetCommand(['start'])).toEqual({
      kind: 'factory',
      action: 'start',
      mode: 'live',
    })
  })

  it('rejects the removed nested factory namespace', () => {
    expect(() => parseFleetCommand(['factory', 'run-once'])).toThrow(/Unknown factory command: factory/)
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
  it('prints factory help for -h without requiring config or showing internal fleet as the binary', async () => {
    const output = buffer()
    const errors = buffer()

    const code = await runFleetCli(['-h'], {
      createFleet: () => {
        throw new Error('help should not construct a fleet')
      },
      stdout: output,
      stderr: errors,
    })

    expect(code).toBe(0)
    expect(errors.text()).toBe('')
    expect(output.text()).toContain('usage: factory <command> [options]')
    expect(output.text()).toContain('run-once')
    expect(output.text()).toContain('start')
    expect(output.text()).toContain('default: ./factory.config.json')
    expect(output.text()).toContain('fleet <command>')
    expect(output.text()).not.toContain('usage: fleet')
  })

  it('prints factory help for --help even when passed after the fleet namespace', async () => {
    const output = buffer()

    const code = await runFleetCli(['fleet', '--help'], {
      stdout: output,
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(output.text()).toContain('usage: factory <command> [options]')
    expect(output.text()).not.toContain('usage: fleet')
  })

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
      'run-once',
      '--dry-run',
      '--config',
      'test/fixtures/factory.config.json',
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

  it('auto-detects GitHub-only workspaces without resolving Linear states', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-only-'))
    try {
      const configPath = await writeConfig(root)
      const githubPath = '/github/repos/AgentWorkforce/pear/issues/by-id/48.json'
      const mount = new FakeMountClient({
        [githubPath]: {
          provider: 'github',
          objectType: 'issue',
          objectId: '48',
          payload: {
            number: 48,
            title: 'GitHub-only quickstart issue',
            body: 'Dispatch without a Linear connection.',
            state: 'open',
            labels: [{ name: 'factory' }],
            url: 'https://github.com/AgentWorkforce/pear/issues/48',
            repository: { name: 'pear', owner: { login: 'AgentWorkforce' } },
          },
        },
      })
      mount.setSubRoot('/linear/issues', 'absent')
      const output = buffer()

      const code = await runFleetCli([
        'run-once',
        '--dry-run',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount,
        resolveStates: async () => {
          throw new Error('Linear state resolution must not run')
        },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        pulled: [{ key: '48', path: githubPath }],
        dispatched: [{ issue: { key: '48' }, dryRun: true }],
      })
      expect(mount.writes.filter((write) => write.path.startsWith('/linear/'))).toEqual([])

      const dispatchOutput = buffer()
      const dispatchCode = await runFleetCli([
        'dispatch',
        '48',
        '--dry-run',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount,
        resolveStates: async () => {
          throw new Error('Linear state resolution must not run')
        },
        stdout: dispatchOutput,
        stderr: buffer(),
      })

      expect(dispatchCode).toBe(0)
      expect(JSON.parse(dispatchOutput.text())).toMatchObject({
        issue: { key: '48', path: githubPath },
        dryRun: true,
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects an ambiguous bare GitHub issue number across configured repositories', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-ambiguous-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear', cloud: 'AgentWorkforce/cloud' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear', 'AgentWorkforce/cloud': '/work/cloud' },
        },
      })
      const mount = new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/by-id/48.json': githubIssueFile('pear'),
        '/github/repos/AgentWorkforce/cloud/issues/by-id/48.json': githubIssueFile('cloud'),
      })
      const errors = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('matches multiple repositories (AgentWorkforce/cloud, AgentWorkforce/pear)')
      expect(errors.text()).toContain('set repos.default or pass a repo-qualified argument')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('uses repos.default to disambiguate a bare GitHub issue number', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-default-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear', cloud: 'AgentWorkforce/cloud' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear', 'AgentWorkforce/cloud': '/work/cloud' },
          default: 'AgentWorkforce/cloud',
        },
      })
      const cloudPath = '/github/repos/AgentWorkforce/cloud/issues/by-id/48.json'
      const mount = new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/by-id/48.json': githubIssueFile('pear'),
        [cloudPath]: githubIssueFile('cloud'),
      })
      const output = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: '48', path: cloudPath } })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('deduplicates alternate mounted paths for the same GitHub repository', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-dedupe-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
        },
      })
      const preferredPath = '/github/repos/AgentWorkforce/pear/issues/48/meta.json'
      const mount = new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/by-id/48.json': githubIssueFile('pear'),
        [preferredPath]: githubIssueFile('pear'),
      })
      const output = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: '48', path: preferredPath } })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('routes factory progress logs to stderr so run-once stdout stays JSON', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-logs-'))
    try {
      const configPath = await writeConfig(root)
      const stdout = buffer()
      const stderr = buffer()
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        dispose: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({
          pulled: [],
          triaged: [],
          dispatched: [],
          skipped: [],
          dryRun: false,
        })),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
        on: vi.fn(() => () => undefined),
      } as unknown as Factory
      const createFactoryWithLog = vi.fn((_config, ports: FactoryPorts) => {
        ports.logger?.info?.('[factory] run-once started', { dryRun: false })
        return factory
      }) as typeof createFactory

      const code = await runFleetCli([
        'run-once',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: createFactoryWithLog,
        stdout,
        stderr,
      })

      expect(code).toBe(0)
      expect(JSON.parse(stdout.text())).toMatchObject({ dryRun: false, pulled: [] })
      expect(stdout.text()).not.toContain('[factory]')
      expect(stderr.text()).toContain('[factory] run-once started {\"dryRun\":false}')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('prints factory status from the top-level status command', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-status-'))
    try {
      const configPath = await writeConfig(root)
      const output = buffer()
      const factoryStatus = { inFlight: [], queued: [], counters: { pulled: 0 } }
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(() => factoryStatus),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory

      const code = await runFleetCli([
        'status',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toEqual(factoryStatus)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('drives the real RelayFleetClient when --backend relay is requested', async () => {
    // Strip every relay credential so the lazily-built HTTP transport surfaces a
    // deterministic auth error instead of attempting a real network request.
    // Independent of host env (a developer's ambient RELAY_API_KEY).
    vi.stubEnv('RELAY_AGENT_TOKEN', '')
    vi.stubEnv('RELAY_WORKSPACE_KEY', '')
    vi.stubEnv('RELAY_API_KEY', '')
    try {
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli(['fleet', 'roster', '--backend', 'relay'], {
        stdout: output,
        stderr: errors,
      })

      // The relay backend is selected (no more 'not implemented' stub) and the
      // RelayFleetClient surfaces the relay auth error on first transport use.
      expect(code).toBe(1)
      expect(errors.text()).toContain('requires agentToken or workspaceKey')
      expect(output.text()).toBe('')
    } finally {
      vi.unstubAllEnvs()
    }
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
      'dispatch',
      'AR-77',
      '--config',
      'test/fixtures/factory.config.json',
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

  it('derives the workspace from the cloud session when config omits workspaceId and forwards the cloud UUID alias', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-derive-'))
    try {
      const path = join(root, 'factory.config.json')
      const { workspaceId: _omitted, ...withoutWorkspace } = config
      await writeFile(path, JSON.stringify(withoutWorkspace))

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
      const ensureLocalMount = vi.fn(async () => {})
      const resolveWorkspace = vi.fn(async () => ({
        workspaceId: 'rw_7ccfea89',
        cloudWorkspaceId: '50587328-441d-4acb-b8f3-dbe1b3c5de99',
      }))

      const code = await runFleetCli([
        'start',
        '--mode',
        'live',
        '--config',
        path,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: vi.fn(() => factory),
        ensureLocalMount,
        resolveWorkspace,
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(resolveWorkspace).toHaveBeenCalledTimes(1)
      expect(ensureLocalMount).toHaveBeenCalledWith('rw_7ccfea89', process.cwd(), {
        acceptableWorkspaceIds: ['50587328-441d-4acb-b8f3-dbe1b3c5de99'],
      })
      expect(ensureLocalMount).toHaveBeenCalledWith('rw_7ccfea89', '/work/pear', {
        acceptableWorkspaceIds: ['50587328-441d-4acb-b8f3-dbe1b3c5de99'],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not resolve the workspace when config pins workspaceId', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-pinned-'))
    try {
      const configPath = await writeConfig(root)
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
      const resolveWorkspace = vi.fn(async () => ({ workspaceId: 'rw_unused' }))

      const code = await runFleetCli([
        'start',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: vi.fn(() => factory),
        ensureLocalMount: vi.fn(async () => {}),
        resolveWorkspace,
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(resolveWorkspace).not.toHaveBeenCalled()
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
      const ensureLocalMount = vi.fn(async () => {})

      const code = await runFleetCli([
        'start',
        '--mode',
        'live',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        createFactory,
        ensureLocalMount,
        waitForStopSignal,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(ensureLocalMount).toHaveBeenCalledWith('factory-cli-test', process.cwd(), {
        acceptableWorkspaceIds: undefined,
      })
      expect(ensureLocalMount).toHaveBeenCalledWith('factory-cli-test', '/work/pear', {
        acceptableWorkspaceIds: undefined,
      })
      expect(createFactory).toHaveBeenCalledTimes(1)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
      expect(factory.runLoop).not.toHaveBeenCalled()
      expect(waitForStopSignal).toHaveBeenCalledTimes(1)
      expect(factory.stop).toHaveBeenCalledTimes(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('uses ./factory.config.json by default for factory commands', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-default-config-'))
    const previousCwd = process.cwd()
    try {
      await writeConfig(root)
      process.chdir(root)
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
      const code = await runFleetCli(['start'], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: vi.fn(() => factory),
        ensureLocalMount: vi.fn(async () => {}),
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
    } finally {
      process.chdir(previousCwd)
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
      const ensureLocalMount = vi.fn(async () => {})
      const daemonExits: number[] = []

      const run = runFleetCli([
        'start',
        '--mode',
        'live',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory,
        ensureLocalMount,
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
