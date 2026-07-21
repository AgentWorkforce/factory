import { describe, expect, it, vi } from 'vitest'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import type {
  CloseProbePrInput,
  Factory,
  FactoryCloudEventInputV1,
  FactoryEventReporter,
  FactoryIntegrationConnections,
  FactoryIntegrationProvider,
  FactoryPorts,
  createFactory,
} from '../index'
import { stateResolutionFromIds } from '../index'
import { FileStateStore } from '../state/file-state-store'
import { FakeFleetClient, FakeMountClient } from '../testing'
import type { GithubConnectionWrite, SpawnInput, SpawnResult } from '../ports'
import { formatLogArgs, installFactoryStopSignalHandlers, parseFleetCommand, parseGlobalOptions, resolveBrokerConnectionPath, runFleetCli } from './fleet'

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

class CompletingRemoteFleetClient extends FakeFleetClient {
  override readonly placementLocality = 'remote' as const
  readonly lifecycleOrder: string[] = []

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    const result = await super.spawn(input)
    if (input.name.includes('-impl-')) {
      setTimeout(() => this.emitAgentExit(input.name, 'exited'), 0)
    }
    return { ...result, node: 'sf-mini', locality: 'remote' }
  }

  override async release(name: string, reason?: string): Promise<void> {
    this.lifecycleOrder.push(`release:${name}`)
    await super.release(name, reason)
  }

  override async dispose(): Promise<void> {
    this.lifecycleOrder.push('dispose')
    await super.dispose()
  }
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

const mountWithIntegrationConnections = (
  files: Record<string, unknown>,
  integrationConnections: FactoryIntegrationConnections,
): FakeMountClient => Object.assign(new FakeMountClient(files), { integrationConnections })

const fakeIntegrationConnections = (
  getStatus: (provider: FactoryIntegrationProvider) => ReturnType<FactoryIntegrationConnections['getStatus']>,
): FactoryIntegrationConnections => ({
  getStatus: vi.fn(getStatus),
  connect: vi.fn(async (provider) => ({
    alreadyConnected: false,
    connectLink: `https://connect.example/${provider}`,
    connectionId: `conn-${provider}`,
  })),
  waitForConnection: vi.fn(async () => {}),
})

describe('fleet CLI logging', () => {
  it('keeps every argument when Errors and unserializable values reach the stream logger', () => {
    const circular: Record<string, unknown> = { sibling: 'kept' }
    circular.self = circular
    const throwingToJson = {
      sibling: 'also kept',
      toJSON: () => {
        throw new Error('must not run')
      },
    }

    const output = formatLogArgs([
      new Error('top-level failure'),
      { nested: new Error('nested failure') },
      circular,
      throwingToJson,
      42n,
      'tail',
    ])

    expect(output).toContain('"message":"top-level failure"')
    expect(output).toContain('"message":"nested failure"')
    expect(output).toContain('"self":"[Circular]"')
    expect(output).toContain('also kept')
    expect(output).toContain('"42n"')
    expect(output).toContain(' tail')
  })
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

  it('parses feature-map validation and base-drift options', () => {
    expect(parseFleetCommand([
      'featuremap',
      'check',
      '--manifest',
      'custom/manifest.yaml',
      '--base',
      'origin/main',
    ])).toEqual({
      kind: 'featuremap-check',
      manifestPath: 'custom/manifest.yaml',
      baseRef: 'origin/main',
    })
    expect(() => parseFleetCommand(['featuremap', 'sweep'])).toThrow(
      'factory featuremap requires the check command',
    )
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

  it('parses standalone babysit number and URL commands', () => {
    expect(parseFleetCommand(['babysit', '10'])).toEqual({
      kind: 'factory-babysit',
      prNumber: 10,
    })
    expect(parseFleetCommand(['babysit', 'https://github.com/AgentWorkforce/hoopsheet/pull/10'])).toEqual({
      kind: 'factory-babysit',
      repo: 'AgentWorkforce/hoopsheet',
      prNumber: 10,
      url: 'https://github.com/AgentWorkforce/hoopsheet/pull/10',
    })
    expect(() => parseFleetCommand(['babysit', 'not-a-pr'])).toThrow(/canonical/u)
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

      expect(resolveBrokerConnectionPath(nested, {})).toBe(connectionPath)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('prefers an explicit relay state directory without falling back to an ancestor broker', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-broker-state-'))
    try {
      const nested = join(root, 'packages', 'factory-sdk')
      const stateDir = join(root, 'isolated-relay-state')
      const ancestorConnectionPath = join(root, '.agentworkforce', 'relay', 'connection.json')
      await mkdir(dirname(ancestorConnectionPath), { recursive: true })
      await mkdir(nested, { recursive: true })
      await writeFile(ancestorConnectionPath, JSON.stringify({ port: 3890 }))

      expect(resolveBrokerConnectionPath(nested, { AGENT_RELAY_STATE_DIR: stateDir }))
        .toBe(join(stateDir, 'connection.json'))
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

describe('fleet CLI runtime', () => {
  it('runs the feature-map checker without loading config or constructing a fleet', async () => {
    const output = buffer()
    const featureMapCheck = vi.fn(async () => ({
      ok: true as const,
      manifestPath: '.agentworkforce/features/manifest.yaml',
      categoryCount: 2,
      featureCount: 8,
      baseRef: 'origin/main',
      mergeBase: 'abc123',
      advisories: [],
    }))

    const code = await runFleetCli(['featuremap', 'check', '--base', 'origin/main'], {
      createFleet: () => {
        throw new Error('featuremap check should not construct a fleet')
      },
      featureMapCheck,
      stdout: output,
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(featureMapCheck).toHaveBeenCalledWith({ baseRef: 'origin/main' })
    expect(JSON.parse(output.text())).toMatchObject({ ok: true, featureCount: 8 })
  })

  it('prompts and connects a missing GitHub integration before an interactive triage', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-connect-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const issue = '/github/repos/AgentWorkforce__pear/issues/48/meta.json'
      let githubChecks = 0
      const integrations = fakeIntegrationConnections(async () => (
        githubChecks++ === 0
          ? { ready: false, state: 'not_connected' }
          : { ready: true, state: 'ready' }
      ))
      const mount = mountWithIntegrationConnections({ [issue]: githubIssueFile('pear') }, integrations)
      const confirm = vi.fn(async () => true)
      const openUrl = vi.fn()

      const code = await runFleetCli(['triage', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        isInteractive: () => true,
        confirmIntegrationConnect: confirm,
        openIntegrationUrl: openUrl,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(confirm).toHaveBeenCalledWith('github')
      expect(integrations.connect).toHaveBeenCalledWith('github')
      expect(integrations.waitForConnection).toHaveBeenCalledWith('github', 'conn-github')
      expect(openUrl).toHaveBeenCalledWith('https://connect.example/github')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('requires both Linear and GitHub for a Linear-backed Factory run', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-linear-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'linear' })
      const integrations = fakeIntegrationConnections(async (provider) => provider === 'linear'
        ? { ready: true, state: 'ready' }
        : { ready: false, state: 'not_connected' })
      const errors = buffer()

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({}, integrations),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(integrations.getStatus).toHaveBeenCalledWith('linear')
      expect(integrations.getStatus).toHaveBeenCalledWith('github')
      expect(integrations.connect).not.toHaveBeenCalled()
      expect(errors.text()).toContain('dry-run will not start an OAuth flow')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not prompt or connect when a missing integration is checked without a TTY', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-headless-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const integrations = fakeIntegrationConnections(async () => ({
        ready: false,
        state: 'not_connected',
      }))
      const confirm = vi.fn(async () => true)
      const openUrl = vi.fn()
      const errors = buffer()

      const code = await runFleetCli(['triage', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({}, integrations),
        isInteractive: () => false,
        confirmIntegrationConnect: confirm,
        openIntegrationUrl: openUrl,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(confirm).not.toHaveBeenCalled()
      expect(openUrl).not.toHaveBeenCalled()
      expect(integrations.connect).not.toHaveBeenCalled()
      expect(errors.text()).toContain('invocation is non-interactive')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('treats canary as dry-run during integration preflight', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-canary-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const integrations = fakeIntegrationConnections(async () => ({
        ready: false,
        state: 'not_connected',
      }))
      const confirm = vi.fn(async () => true)
      const openUrl = vi.fn()
      const errors = buffer()

      const code = await runFleetCli(['canary', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({}, integrations),
        isInteractive: () => true,
        confirmIntegrationConnect: confirm,
        openIntegrationUrl: openUrl,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(confirm).not.toHaveBeenCalled()
      expect(openUrl).not.toHaveBeenCalled()
      expect(integrations.connect).not.toHaveBeenCalled()
      expect(errors.text()).toContain('dry-run will not start an OAuth flow')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('auto-selects GitHub only when Linear is authoritatively not connected', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-auto-'))
    try {
      const configPath = await writeConfig(root, {
        stateIds: {},
        issueSource: undefined,
      })
      const issue = '/github/repos/AgentWorkforce__pear/issues/48/meta.json'
      const integrations = fakeIntegrationConnections(async (provider) => provider === 'linear'
        ? { ready: false, state: 'not_connected' }
        : { ready: true, state: 'ready' })
      const output = buffer()

      const code = await runFleetCli(['triage', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({ [issue]: githubIssueFile('pear') }, integrations),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(integrations.getStatus).toHaveBeenNthCalledWith(1, 'linear')
      expect(integrations.getStatus).toHaveBeenCalledWith('github')
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: '48' } })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not mask an unreadable Linear connection check as a GitHub fallback', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-unknown-'))
    try {
      const configPath = await writeConfig(root, { issueSource: undefined })
      const integrations = fakeIntegrationConnections(async () => {
        throw Object.assign(new Error('invalid connection status payload'), { code: 'malformed_cloud_response' })
      })
      const errors = buffer()

      const code = await runFleetCli(['triage', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({}, integrations),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(integrations.getStatus).toHaveBeenCalledTimes(1)
      expect(integrations.getStatus).toHaveBeenCalledWith('linear')
      expect(errors.text()).toContain('refusing to assume it is disconnected')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

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

  it.each(['--version', '-V'])('prints the installed package version for %s without constructing a fleet', async (flag) => {
    const output = buffer()
    const errors = buffer()
    const manifest = JSON.parse(
      await readFile(join(process.cwd(), 'package.json'), 'utf8'),
    ) as { version: string }

    const code = await runFleetCli([flag], {
      createFleet: () => {
        throw new Error('version should not construct a fleet')
      },
      stdout: output,
      stderr: errors,
    })

    expect(code).toBe(0)
    expect(errors.text()).toBe('')
    expect(output.text()).toBe(`${manifest.version}\n`)
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

  it('passes Cloud reporting into Factory and flushes instance lifecycle events on exit', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-reporting-'))
    try {
      const configPath = await writeConfig(root)
      const events: FactoryCloudEventInputV1[] = []
      const close = vi.fn(async () => ({
        delivered: events.length,
        pending: 0,
        attempts: 1,
        stoppedReason: 'empty' as const,
      }))
      const reporter: FactoryEventReporter = {
        report: async (event) => { events.push(event) },
        flush: close,
        close,
      }
      const factory = {
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
      const createFactorySpy = vi.fn((_config, ports: FactoryPorts) => {
        expect(ports.reporter).toBe(reporter)
        return factory
      }) as typeof createFactory
      const cloudMountFromConfig = vi.fn(async (options) => {
        await options?.onLocalMountHealth?.({
          state: 'degraded',
          reason: 'mount_stale',
          degradedMounts: 1,
        })
        return new FakeMountClient()
      })

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        cloudMountFromConfig,
        reporter,
        createFactory: createFactorySpy,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(events.map((event) => event.type)).toEqual([
        'instance.started',
        'factory.anomaly',
        'instance.stopping',
        'instance.stopped',
      ])
      expect(events[0]).toMatchObject({ attributes: { backend: 'internal', mode: 'run-once' } })
      expect(events[1]).toMatchObject({
        level: 'error',
        attributes: {
          component: 'relayfile_mount',
          operation: 'supervise',
          errorCode: 'mount_stale',
          count: 1,
        },
      })
      expect(cloudMountFromConfig).toHaveBeenCalledWith(expect.objectContaining({
        logger: expect.any(Object),
        onLocalMountHealth: expect.any(Function),
      }))
      const cloudMount = await cloudMountFromConfig.mock.results[0]?.value
      expect(cloudMount?.writes).toContainEqual({
        path: '/factory/observability/mount-health/current.json',
        content: expect.objectContaining({
          schemaVersion: 'factory.mount-health.v1',
          type: 'factory.mount-health',
          workspaceId: 'factory-cli-test',
          state: 'degraded',
          reason: 'mount_stale',
          degradedMounts: 1,
          occurredAt: expect.any(String),
        }),
      })
      expect(close).toHaveBeenCalledWith({ deadlineMs: 2_000 })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('infers clonePath from cwd for internal dispatch and logs the checkout root', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-infer-'))
    try {
      const configPath = await writeConfig(root, {
        repos: {
          org: 'AgentWorkforce',
          names: ['pear'],
          default: 'AgentWorkforce/pear',
        },
      })
      const git = vi.fn(async (_cwd: string, args: string[]) => {
        if (args[0] === 'rev-parse') return '/checkout/pear\n'
        if (args[0] === 'remote' && args.length === 1) return 'origin\n'
        if (args[0] === 'remote' && args[1] === 'get-url') return 'git@github.com:AgentWorkforce/pear.git\n'
        throw new Error(`unexpected git args: ${args.join(' ')}`)
      })
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli([
        'dispatch',
        'AR-77',
        '--dry-run',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient({ [issuePath]: issueFile }),
        localClonePathOptions: { cwd: '/checkout/pear/src', git },
        stdout: output,
        stderr: errors,
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issue: { key: 'AR-77' },
        dryRun: true,
      })
      expect(errors.text()).toContain('[factory] clonePath inferred from cwd: /checkout/pear')
      expect(git).toHaveBeenCalledWith('/checkout/pear/src', ['rev-parse', '--show-toplevel'])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('fails fast for an invalid explicit clonePath before internal dispatch', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-invalid-clone-'))
    try {
      const missing = join(root, 'missing-checkout')
      const configPath = await writeConfig(root, {
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': missing },
          default: 'AgentWorkforce/pear',
        },
      })
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli([
        'dispatch',
        'AR-77',
        '--dry-run',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient({ [issuePath]: issueFile }),
        localClonePathOptions: { validateConfiguredCheckouts: true },
        stdout: output,
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain(`[factory] clonePath for AgentWorkforce/pear does not exist: ${missing}`)
      expect(output.text()).toBe('')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not inspect orchestrator-local checkouts for relay dispatch', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-relay-clone-'))
    try {
      const configPath = await writeConfig(root, {
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': join(root, 'worker-only-checkout') },
          default: 'AgentWorkforce/pear',
        },
      })
      const git = vi.fn(async () => {
        throw new Error('relay dispatch must not inspect local git state')
      })

      const code = await runFleetCli([
        'dispatch',
        'AR-77',
        '--dry-run',
        '--backend',
        'relay',
        '--config',
        configPath,
      ], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient({ [issuePath]: issueFile }),
        localClonePathOptions: { git, validateConfiguredCheckouts: true },
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(git).not.toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps relay dispatch ownership until the remote PR is published and the issue is parked', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-relay-owner-'))
    try {
      const configPath = await writeConfig(root, {
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const publishes: Parameters<GithubConnectionWrite['publishPullRequest']>[0][] = []
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => {
          publishes.push(input)
          return {
            repo: input.repo,
            number: 77,
            url: 'https://github.com/AgentWorkforce/pear/pull/77',
            headRef: input.headRef ?? 'unexpected-local-head',
          }
        },
        closePullRequest: async () => undefined,
      }
      const mount = new FakeMountClient({
        [issuePath]: issueFile,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
      }, githubWrite)
      const fleet = new CompletingRemoteFleetClient()
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli([
        'dispatch',
        'AR-77',
        '--backend',
        'relay',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        stdout: output,
        stderr: errors,
        probePrGhRunner: async () => ({ stdout: '[]' }),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: 'AR-77' }, dryRun: false })
      expect(publishes).toEqual([expect.objectContaining({
        repo: 'AgentWorkforce/pear',
        headRef: expect.stringMatching(/^factory\/ar-77-agentworkforce-pear-[0-9a-f]{8}$/u),
      })])
      expect(fleet.releases.map((release) => release.reason)).toEqual(['issue-human-review', 'issue-human-review'])
      const firstDispose = fleet.lifecycleOrder.indexOf('dispose')
      expect(firstDispose).toBeGreaterThanOrEqual(0)
      expect(fleet.lifecycleOrder.slice(0, firstDispose).every((event) => event.startsWith('release:'))).toBe(true)
      expect(fleet.lifecycleOrder.slice(firstDispose).every((event) => event === 'dispose')).toBe(true)
      expect(errors.text()).not.toContain('[factory] error')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not infer or preflight clone paths for a maintenance command', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-maintenance-clone-'))
    try {
      const configPath = await writeConfig(root, {
        repos: { org: 'AgentWorkforce', names: ['pear'] },
      })
      const git = vi.fn(async () => {
        throw new Error('status must not inspect local git state')
      })
      const factoryStatus = { inFlight: [], queued: [], counters: { pulled: 0 } }
      const factory = {
        status: vi.fn(() => factoryStatus),
      } as unknown as Factory
      const output = buffer()
      const integrations = fakeIntegrationConnections(async () => {
        throw new Error('maintenance command must not preflight integrations')
      })

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: mountWithIntegrationConnections({}, integrations),
        createFactory: () => factory,
        localClonePathOptions: { cwd: '/not/a/checkout', git, validateConfiguredCheckouts: true },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toEqual(factoryStatus)
      expect(git).not.toHaveBeenCalled()
      expect(integrations.getStatus).not.toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('auto-detects GitHub-only workspaces without resolving Linear states', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-only-'))
    try {
      const configPath = await writeConfig(root, {
        stateIds: undefined,
        repos: {
          org: 'AgentWorkforce',
          names: ['pear'],
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubPath = '/github/repos/AgentWorkforce/pear/issues/by-id/48.json'
      const mount = new (class extends FakeMountClient {
        override async ensureSubRoot(prefix: string): Promise<'ready' | 'absent'> {
          if (prefix === '/linear/issues') {
            throw new Error('Linear sub-root probe must not run for a GitHub-oriented config')
          }
          return super.ensureSubRoot(prefix)
        }
      })({
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
      const output = buffer()
      const errors = buffer()

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
        stderr: errors,
      })

      expect(code, errors.text()).toBe(0)
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

  it('keeps legacy Linear configs with repository routing on the Linear source', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-linear-with-repos-'))
    try {
      const configPath = await writeConfig(root, {
        repos: {
          org: 'AgentWorkforce',
          names: ['pear'],
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubPath = '/github/repos/AgentWorkforce__pear/issues/by-id/77.json'
      const mount = new FakeMountClient({
        [issuePath]: issueFile,
        [githubPath]: githubIssueFile('pear', 77),
      })
      const output = buffer()

      const code = await runFleetCli(['dispatch', 'AR-77', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: 'AR-77', path: issuePath } })
      expect(mount.reads).not.toContain(githubPath)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('honors an explicit Linear source for an otherwise GitHub-oriented config', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-explicit-linear-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'linear',
        stateIds: undefined,
        repos: {
          org: 'AgentWorkforce',
          names: ['pear'],
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const mount = new (class extends FakeMountClient {
        override async ensureSubRoot(): Promise<'ready' | 'absent'> {
          throw new Error('explicit issueSource must not probe providers')
        }
      })({
        [issuePath]: issueFile,
        '/github/repos/AgentWorkforce__pear/issues/by-id/77.json': githubIssueFile('pear', 77),
      })
      const output = buffer()

      const code = await runFleetCli(['dispatch', 'AR-77', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        resolveStates: async () => stateResolutionFromIds(TEST_STATE_IDS),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: 'AR-77', path: issuePath } })
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

  it('rejects non-positive GitHub issue numbers with source context', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-invalid-number-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const errors = buffer()

      const code = await runFleetCli(['dispatch', '0', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain(
        'Unable to resolve GitHub issue 0 (issueSource=github): expected a positive issue number',
      )
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
      const mount = new (class extends FakeMountClient {
        override async listTree(): Promise<string[]> {
          throw new Error('direct GitHub issue resolution must not list the mount')
        }
      })({
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

  it('resolves a case-insensitive bare repos.default through repos.byLabel without listing the mount', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-bare-default-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'PEAR',
        },
      })
      const compactPath = '/github/repos/AgentWorkforce__pear/issues/by-id/48.json'
      const mount = new (class extends FakeMountClient {
        override async listTree(): Promise<string[]> {
          throw new Error('direct GitHub issue resolution must not list the mount')
        }
      })({ [compactPath]: githubIssueFile('pear') })
      const output = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: '48', path: compactPath } })
      expect(mount.reads).toContain(compactPath)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it.each([
    '/github/repos/AgentWorkforce__pear/issues/48__scoped-fallback/meta.json',
    '/github/repos/AgentWorkforce/pear/issues/48__scoped-fallback/meta.json',
  ])('falls back to compact and nested repo-scoped issue roots for %s', async (issuePath) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-scoped-fallback-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const listed: string[] = []
      const mount = new (class extends FakeMountClient {
        override async listTree(prefix: string): Promise<string[]> {
          listed.push(prefix)
          if (prefix === '/github/repos') throw new Error('global GitHub tree scan is forbidden')
          return super.listTree(prefix)
        }
      })({ [issuePath]: githubIssueFile('pear') })
      const output = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ issue: { key: '48', path: issuePath } })
      expect(listed.sort()).toEqual([
        '/github/repos/AgentWorkforce/pear/issues',
        '/github/repos/AgentWorkforce__pear/issues',
      ])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not hide a provider failure behind GitHub direct-read fallback', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-provider-error-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const mount = new (class extends FakeMountClient {
        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path.endsWith('/48/meta.json')) {
            throw Object.assign(new Error('GitHub connection unauthorized'), { status: 401 })
          }
          return super.readFile(path)
        }
      })()
      const errors = buffer()

      const code = await runFleetCli(['dispatch', '48', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('Unable to read GitHub issue candidate')
      expect(errors.text()).toContain('GitHub connection unauthorized')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('names an auto-detected Linear source and suggests the GitHub override on resolution failure', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-linear-source-error-'))
    try {
      const configPath = await writeConfig(root)
      const errors = buffer()

      const code = await runFleetCli(['dispatch', '37', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain(
        'Unable to resolve issue 37 in Linear (issueSource=linear, auto-detected): found 0 matches',
      )
      expect(errors.text()).toContain("set issueSource: 'github' if these are GitHub issues")
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
    const output = buffer()
    const errors = buffer()

    // A hermetic env keeps the client from resolving ambient host credentials
    // (RELAY_WORKSPACE_KEY, the cloud workspace store) and reaching the network.
    const code = await runFleetCli(['fleet', 'roster', '--backend', 'relay'], {
      stdout: output,
      stderr: errors,
      env: {},
    })

    // The relay backend is selected (no more 'not implemented' stub) and the
    // RelayFleetClient surfaces the credential error on first use.
    expect(code).toBe(1)
    expect(errors.text()).toContain('requires a workspace key (rk_live_…) or agent token (at_live_…)')
    expect(output.text()).toBe('')
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
    const git = vi.fn(async () => {
      throw new Error('close-probe must not inspect local git state')
    })
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
      localClonePathOptions: { git, validateConfiguredCheckouts: true },
      probeCloser: async (input: Pick<CloseProbePrInput, 'repo' | 'prNumber' | 'expectedIssueKey'>) => {
        calls.push(input)
        return { repo: input.repo, prNumber: input.prNumber, state: 'CLOSED' }
      },
    })

    expect(code).toBe(0)
    expect(git).not.toHaveBeenCalled()
    expect(calls).toEqual([{ repo: 'AgentWorkforce/pear', prNumber: 42, expectedIssueKey: 'AR-77' }])
    expect(JSON.parse(output.text())).toEqual({ repo: 'AgentWorkforce/pear', prNumber: 42, state: 'CLOSED' })
  })

  it('spawns a standalone babysitter for a mounted open PR even when config automation is disabled', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-babysit-'))
    try {
      const clonePath = join(root, 'hoopsheet')
      await mkdir(clonePath)
      await mkdir(join(clonePath, '.agentworkforce/features/verify'), { recursive: true })
      await writeFile(join(clonePath, '.agentworkforce/features/manifest.yaml'), [
        'categories:',
        '  public-sites:',
        '    features:',
        '      - id: league-routing',
        '        name: League routing',
        '        location: src/routes/league.ts',
        '        verify_tier: 2',
        '',
      ].join('\n'))
      await writeFile(join(clonePath, '.agentworkforce/features/verify/procedures.md'), [
        '## Tier 2 — Config',
        '```bash',
        'npm run test:league-routing',
        '```',
        '',
      ].join('\n'))
      const configPath = await writeConfig(root, {
        repos: {
          org: 'AgentWorkforce',
          byLabel: { hoopsheet: 'AgentWorkforce/hoopsheet' },
          clonePaths: { 'AgentWorkforce/hoopsheet': clonePath },
          default: 'hoopsheet',
        },
        babysitter: { enabled: false },
        models: { babysitter: 'claude-sonnet-4-6' },
      })
      const integrations = fakeIntegrationConnections(async () => ({ ready: true, state: 'ready' }))
      const mount = mountWithIntegrationConnections({
        '/github/repos/AgentWorkforce__hoopsheet/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Add league subdomain routing',
            body: 'Full PR definition of done',
            state: 'open',
            draft: false,
            html_url: 'https://github.com/AgentWorkforce/hoopsheet/pull/10',
            head: { ref: 'codex/league-public-sites', sha: 'abc123', repo: { full_name: 'AgentWorkforce/hoopsheet' } },
            base: { ref: 'main' },
            files: [{ filename: 'src/routes/league.ts' }],
          },
        },
      }, integrations)
      const fleet = new FakeFleetClient()
      const output = buffer()
      const errors = buffer()
      const mountCalls: string[] = []
      const code = await runFleetCli(['babysit', '10', '--config', configPath], {
        fleet,
        mount,
        ensureLocalMount: async (_workspaceId, startDir) => {
          mountCalls.push(startDir)
          if (startDir === clonePath) throw new Error('mount unavailable')
        },
        babysitPrGhRunner: async () => { throw new Error('gh unavailable') },
        stdout: output,
        stderr: errors,
      })

      expect(code).toBe(0)
      expect(integrations.getStatus).toHaveBeenCalledWith('github')
      expect(mountCalls).toEqual([process.cwd(), clonePath])
      expect(errors.text()).toContain(`warning: could not start relayfile mount for standalone babysitter at ${clonePath}`)
      expect(fleet.spawns).toHaveLength(1)
      expect(fleet.preservedInfrastructure).toBe(1)
      expect(fleet.spawns[0]).toMatchObject({
        capability: 'spawn:claude',
        repo: 'AgentWorkforce/hoopsheet',
        clonePath,
        cwd: clonePath,
        model: 'claude-sonnet-4-6',
        invocationId: 'factory-babysit:AgentWorkforce/hoopsheet#10',
      })
      expect(fleet.spawns[0]?.task).toContain('standalone PR babysitter')
      expect(fleet.spawns[0]?.task).toContain('Full PR definition of done')
      expect(fleet.spawns[0]?.task).toContain('League routing (`league-routing`)')
      expect(fleet.spawns[0]?.task).toContain('npm run test:league-routing')
      expect(fleet.spawns[0]?.task).not.toContain('[factory-pr-ready]')
      expect(JSON.parse(output.text())).toMatchObject({
        status: 'spawned',
        repo: 'AgentWorkforce/hoopsheet',
        prNumber: 10,
        source: 'mount',
        specSource: 'pull-request',
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it.each([
    { state: 'closed', draft: false, message: 'state is CLOSED' },
    { state: 'open', draft: true, message: 'is a draft' },
    { state: 'unknown', draft: false, message: 'state is UNKNOWN' },
  ])('rejects an ineligible standalone PR before spawn: $message', async ({ state, draft, message }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-babysit-guard-'))
    try {
      const configPath = await writeConfig(root, {
        repos: { byLabel: { pear: 'AgentWorkforce/pear' }, default: 'AgentWorkforce/pear' },
      })
      const mount = new FakeMountClient({
        '/github/repos/AgentWorkforce__pear/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Guarded PR',
            body: 'Do not spawn when ineligible.',
            state,
            draft,
            head: { ref: 'guarded-pr', sha: 'guard-sha', repo: { full_name: 'AgentWorkforce/pear' } },
            base: { ref: 'main' },
          },
        },
      })
      const fleet = new FakeFleetClient()
      const errors = buffer()
      const code = await runFleetCli(['babysit', '10', '--config', configPath], {
        fleet,
        mount,
        ensureLocalMount: async () => undefined,
        babysitPrGhRunner: async () => { throw new Error('gh unavailable') },
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain(message)
      expect(fleet.spawns).toEqual([])
      expect(fleet.preservedInfrastructure).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses a cross-repository standalone PR before spawn', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-babysit-fork-'))
    try {
      const configPath = await writeConfig(root)
      const mount = new FakeMountClient({
        '/github/repos/AgentWorkforce__pear/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Fork PR',
            body: 'Untrusted fork',
            state: 'open',
            draft: false,
            head: { ref: 'fork-pr', sha: 'fork-sha', repo: { full_name: 'outside/pear' } },
            base: { ref: 'main' },
          },
        },
      })
      const fleet = new FakeFleetClient()
      const errors = buffer()
      const code = await runFleetCli(['babysit', '10', '--config', configPath], {
        fleet,
        mount,
        ensureLocalMount: async () => undefined,
        babysitPrGhRunner: async () => { throw new Error('gh unavailable') },
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('refuses cross-repository PRs')
      expect(fleet.spawns).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('honors an explicit default route override for numeric babysit dry-runs', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-babysit-override-'))
    try {
      const configPath = await writeConfig(root, {
        repos: {
          org: 'Acme',
          byLabel: { app: 'customer-fork/app' },
          default: 'app',
        },
      })
      const mount = new FakeMountClient({
        '/github/repos/customer-fork__app/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Override PR',
            body: 'Use the explicit route.',
            state: 'open',
            draft: false,
            head: { ref: 'override-pr', sha: 'override-sha', repo: { full_name: 'customer-fork/app' } },
            base: { ref: 'main' },
          },
        },
      })
      const output = buffer()
      const fleet = new FakeFleetClient()
      const code = await runFleetCli(['babysit', '10', '--dry-run', '--config', configPath], {
        fleet,
        mount,
        ensureLocalMount: async () => undefined,
        babysitPrGhRunner: async () => { throw new Error('gh unavailable') },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(fleet.spawns).toEqual([])
      expect(JSON.parse(output.text())).toMatchObject({
        status: 'dry-run',
        repo: 'customer-fork/app',
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects an ambiguous repo-name default for numeric babysit', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-babysit-ambiguous-'))
    try {
      const configPath = await writeConfig(root, {
        repos: {
          byLabel: {
            appProd: 'org-a/app',
            appFork: 'org-b/app',
          },
          default: 'app',
        },
      })
      const fleet = new FakeFleetClient()
      const errors = buffer()
      const code = await runFleetCli(['babysit', '10', '--dry-run', '--config', configPath], {
        fleet,
        mount: new FakeMountClient(),
        ensureLocalMount: async () => undefined,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('matches multiple configured repos (org-a/app, org-b/app)')
      expect(fleet.spawns).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('configures guarded GitHub writeback when close-probe creates a cloud mount', async () => {
    const output = buffer()
    const errors = buffer()
    const closes: Array<{ repo: string; number: number }> = []
    const integrations = fakeIntegrationConnections(async () => ({ ready: true, state: 'ready' }))
    const cloudMountFromConfig = vi.fn(async (opts) => {
      const mount = mountWithIntegrationConnections({}, integrations)
      mount.githubWrite = {
        publishPullRequest: async () => { throw new Error('unexpected publish') },
        closePullRequest: async (input) => {
          const path = `/github/repos/${input.repo}/pulls/${input.number}/close.json`
          const allowed = await opts?.isAllowedDraft?.(path, {}, { guarded: true })
          if (!allowed) throw new Error('GitHub close draft rejected by mount predicate')
          expect(await opts?.isAllowedDraft?.(
            `/github/repos/${input.repo}/refs/factory.json`,
            { ref: 'refs/heads/factory/77', sha: 'abc123' },
            { guarded: true },
          )).toBe(true)
          expect(await opts?.isAllowedDraft?.(
            `/github/repos/${input.repo}/refs/refs%2Fheads%2Ffactory%2F77.json`,
            { ref: 'refs/heads/factory/77', sha: 'abc123' },
            { guarded: true },
          )).toBe(true)
          expect(await opts?.isAllowedDraft?.(
            `/github/repos/${input.repo}/refs/refs%2Fheads%2Fmain.json`,
            { ref: 'refs/heads/main', sha: 'abc123' },
            { guarded: true },
          )).toBe(false)
          expect(await opts?.isAllowedDraft?.(
            `/github/repos/${input.repo}/refs/arbitrary.json`,
            { ref: 'refs/heads/factory/77', sha: 'abc123' },
            { guarded: true },
          )).toBe(false)
          closes.push(input)
        },
      }
      return mount
    })
    let readCount = 0

    const code = await runFleetCli([
      'close-probe',
      '42',
      '--repo',
      'AgentWorkforce/pear',
      '--issue',
      'AR-77',
    ], {
      stdout: output,
      stderr: errors,
      resolveWorkspace: async () => ({ workspaceId: 'rw_test' }),
      cloudMountFromConfig,
      probePrGhRunner: async () => ({
        stdout: JSON.stringify({
          state: readCount++ === 0 ? 'OPEN' : 'CLOSED',
          headRefName: 'factory-e2e/ar-77-probe',
          title: '[factory-e2e] AR-77 probe',
          body: 'Closes AR-77',
        }),
      }),
    })

    expect(code, errors.text()).toBe(0)
    expect(cloudMountFromConfig).toHaveBeenCalledWith(expect.objectContaining({
      workspaceId: 'rw_test',
      isAllowedDraft: expect.any(Function),
    }))
    expect(integrations.getStatus).toHaveBeenCalledWith('github')
    expect(closes).toEqual([{ repo: 'AgentWorkforce/pear', number: 42 }])
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

  it('uses the mount client SDK path without requiring a relayfile CLI', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-sdk-mount-'))
    try {
      const configPath = await writeConfig(root)
      const ensureSdkMount = vi.fn(async () => {})
      const disposeMount = vi.fn(async () => {})
      const mount = new FakeMountClient() as FakeMountClient & {
        ensureLocalMount: typeof ensureSdkMount
        dispose: typeof disposeMount
      }
      mount.ensureLocalMount = ensureSdkMount
      mount.dispose = disposeMount
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

      const code = await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: vi.fn(() => factory),
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(ensureSdkMount).toHaveBeenCalledWith(process.cwd(), {
        acceptableWorkspaceIds: undefined,
      })
      expect(ensureSdkMount).toHaveBeenCalledWith('/work/pear', {
        acceptableWorkspaceIds: undefined,
      })
      expect(disposeMount).toHaveBeenCalledTimes(1)
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
      expect(createFactory.mock.calls[0]?.[1].stateStore).toBeInstanceOf(FileStateStore)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
      expect(factory.runLoop).not.toHaveBeenCalled()
      expect(waitForStopSignal).toHaveBeenCalledTimes(1)
      expect(factory.stop).toHaveBeenCalledTimes(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('warms configured clone mounts with bounded concurrency without blocking live start', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-mount-concurrency-'))
    try {
      const clonePaths = Object.fromEntries(
        Array.from({ length: 9 }, (_, index) => [`AgentWorkforce/repo-${index}`, join(root, `repo-${index}`)]),
      )
      const configPath = await writeConfig(root, {
        repos: {
          byLabel: Object.fromEntries(Object.keys(clonePaths).map((repo) => [repo.split('/')[1], repo])),
          clonePaths,
          default: 'AgentWorkforce/repo-0',
        },
      })
      const mounted: string[] = []
      let mountedWhenFactoryStarted = -1
      const factory = {
        start: vi.fn(async () => { mountedWhenFactoryStarted = mounted.length }),
        stop: vi.fn(async () => {}),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      let active = 0
      let maxActive = 0
      const ensureLocalMount = vi.fn(async (_workspaceId: string, startDir: string) => {
        if (startDir === process.cwd()) return
        mounted.push(startDir)
        active += 1
        maxActive = Math.max(maxActive, active)
        await new Promise((resolve) => setTimeout(resolve, 10))
        active -= 1
      })

      await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: vi.fn(() => factory),
        ensureLocalMount,
        waitForStopSignal: vi.fn(async () => {
          await vi.waitFor(() => expect(mounted).toHaveLength(9))
        }),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(maxActive).toBe(4)
      expect(mounted.sort()).toEqual(Object.values(clonePaths).sort())
      expect(mountedWhenFactoryStarted).toBeLessThan(Object.keys(clonePaths).length)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
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
      const events: FactoryCloudEventInputV1[] = []
      const closeReporter = vi.fn(async () => ({
        delivered: events.length,
        pending: 0,
        attempts: 0,
        stoppedReason: 'empty' as const,
      }))
      const reporter: FactoryEventReporter = {
        report: async (event) => { events.push(event) },
        flush: closeReporter,
        close: closeReporter,
      }

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
        reporter,
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
      expect(calls).toEqual(['stop', 'flush'])
      expect(daemonExits).toEqual([])
      expect(events.map((event) => event.type)).toEqual([
        'instance.started',
        'instance.stopping',
        'instance.stopped',
      ])
      expect(closeReporter).toHaveBeenCalledWith({ deadlineMs: 2_000 })
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
