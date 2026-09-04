import { describe, expect, it, vi } from 'vitest'
import type { CloudSession } from '@agent-relay/cloud'
import { existsSync } from 'node:fs'
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

import type {
  CloseProbePrInput,
  Factory,
  FactoryCloudEventInputV1,
  FactoryConfig,
  FactoryEventReporter,
  FactoryIntegrationConnections,
  FactoryIntegrationProvider,
  FactoryPorts,
} from '../index'
import {
  createFactory,
  FactoryConfigSchema,
  FileFactoryCloudEventOutbox,
  LiveDispatchStateChangedError,
  stateResolutionFromIds,
} from '../index'
import { MountAuthScopeError, mountAuthRemediation } from '../mount/mount-auth-error'
import { DocumentStateStore, FileStateStore } from '../state/file-state-store'
import { FakeFleetClient, FakeMountClient, withDeadline } from '../testing'
import { GhCliIssuePublisher } from '../intake/notion'
import type { Capability, GithubConnectionRead, GithubConnectionWrite, GithubIssueLookup, GithubWriteback, LocalMountOptions, SpawnInput, SpawnResult } from '../ports'
import type { HarnessDriverClientLike } from '../fleet/internal-fleet-client'
import type { RelayMessaging } from '@agent-relay/sdk'
import { factoryGithubIssueCommentDraftName } from '../github/writeback-paths'
import { GhCliGithubWriteback } from '../writeback/github'
import { ensureLocalMount as runLocalMountPreflight } from '../mount/local-mount-preflight'
import { formatLogArgs, installFactoryStopSignalHandlers, parseFleetCommand, parseGithubIssueSelector, parseGlobalOptions, reportFactoryVersionDrift, resolveBrokerConnectionPath, resolveFactoryBrokerConnectionPath, runFleetCli } from './fleet'

// MITIGATION for this file only, not a fix (#442). Scoped here rather than set
// in vitest.config.ts because this repo deliberately configures no global
// `testTimeout` (see the note in sweep-counters.test.ts) and uses per-test
// budgets instead; a global raise would weaken hung-test detection across all
// 117 test files.
//
// The race that made "keeps relay dispatch ownership…" fail is fixed at its
// source, in `CompletingRemoteFleetClient`. This covers what is left: these CLI
// tests each drive a whole dispatch through real filesystem I/O and cost 2-10s
// apiece, so at Vitest's 5s default a loaded runner fails a rotating cast of
// them. Measured on 8 cores at load average ~155, one run failed 10 different
// tests, every one between 4.3s and 10.4s — the "each rerun fails a different
// test" signature reported on #442.
//
// Deliberately ABOVE REMOTE_AGENT_REGISTRATION_TIMEOUT_MS (30s,
// factory.ts:459). If the registration race ever regresses, the run should
// reach the production deadline and fail as `expect(code).toBe(0)` with a
// RemoteAgentRegistrationTimeoutError on stderr — the diagnostic failure. A
// budget under 30s would truncate that into a bare "Test timed out", which is
// the exact failure mode that hid this bug in the first place.
//
// This buys headroom; it makes no test cheaper. The real cure is for these
// tests to stop doing seconds of real I/O each.
vi.setConfig({ testTimeout: 40_000 })

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

const currentVersionInfo = {
  version: '0.1.58',
  installedAt: '2026-08-14T12:00:00.000Z',
  latestVersion: '0.1.58',
  versionsBehind: 0,
}

const staleVersionInfo = {
  version: '0.1.20',
  installedAt: '2026-07-17T12:00:00.000Z',
  latestVersion: '0.1.58',
  versionsBehind: 38,
}

const testDocumentStateStore = (options: {
  backend?: string
  assertReady?: () => Promise<void>
} = {}): DocumentStateStore => new DocumentStateStore({
  batchSize: 2,
  ...(options.backend ? { backend: options.backend } : {}),
  documentStore: {
    read: async () => ({ version: 3, workspaces: {} }),
    write: async () => {},
    runMutation: async (operation) => await operation(),
    assertReady: options.assertReady ?? (async () => {}),
  },
})

const fakeHarnessClient = (): HarnessDriverClientLike => ({
  async spawnPty(input) {
    return { name: input.name, sessionId: 'session' }
  },
  async release(name) {
    return { name }
  },
  async listAgents() {
    return []
  },
  async sendMessage(input) {
    return { event_id: 'event', targets: [input.to] }
  },
  async sendInput() {},
  async shutdown() {},
  disconnect() {},
})

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

/**
 * Remote fleet whose implementer exit is emitted by the test, not by a timer.
 *
 * Kept separate from `CompletingRemoteFleetClient`: that one completes itself,
 * asynchronously and without the test body saying when, while every user of
 * this class drives the exit from a specific point in its own mount fixture.
 * Collapsing the two behind a flag would put that self-completion in scope for
 * tests whose whole point is that no timer decides the ordering (#346 review,
 * cubic).
 *
 * Note that `CompletingRemoteFleetClient` no longer *races* dispatch: its exit
 * is armed at spawn and released by the registration probe. It used to fire on
 * a bare `setTimeout(..., 0)`, which raced the roster read and was the bug
 * behind #442 — do not restore that.
 */
class CompletingRemoteFleetBase extends FakeFleetClient {
  override readonly placementLocality = 'remote' as const

  override async roster() {
    const roster = await super.roster()
    return {
      agents: roster.agents.map((agent) => ({ ...agent, node: 'sf-mini' })),
      nodes: [{
        name: 'sf-mini',
        capabilities: ['spawn:codex' as const, 'spawn:claude' as const, 'workflow:run' as const],
        live: true,
      }],
    }
  }
}

class ControlledCompletingRemoteFleetClient extends CompletingRemoteFleetBase {
  implementerName?: string
  exitEmitted = false

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    const result = await super.spawn(input)
    if (input.name.includes('-impl-')) this.implementerName = input.name
    return { ...result, node: 'sf-mini', locality: 'remote' }
  }
}

class CompletingRemoteFleetClient extends CompletingRemoteFleetBase {
  readonly lifecycleOrder: string[] = []
  #exitOnRegistration?: string

  override async spawn(input: SpawnInput): Promise<SpawnResult> {
    const result = await super.spawn(input)
    // Arm the exit here, but let the registration probe below release it. A
    // bare `setTimeout(..., 0)` raced the remote-registration wait: this is a
    // `remote` placement, so dispatch polls until the agent is roster-visible,
    // and `emitAgentExit` *removes* the agent from the fake roster. Whenever
    // the timer won that race the placement became permanently unobservable,
    // so dispatch burned the full 30s startup deadline and exited 1. The
    // scheduler picked the winner, which is why an idle runner passed and a
    // loaded one failed (#442).
    if (input.name.includes('-impl-')) this.#exitOnRegistration = input.name
    return { ...result, node: 'sf-mini', locality: 'remote' }
  }

  /**
   * Dispatch prefers this probe over a roster read when it is present. Emitting
   * the exit only once it has answered `true` keeps the exit on a timer — the
   * ordering this class exists to provide, rather than one the test body drives
   * — while making it causally *after* registration instead of a coin flip
   * against it.
   */
  async isAgentRegistered(input: { name: string; node: string; capability: Capability }): Promise<boolean> {
    const roster = await this.roster()
    const registered = roster.agents.some((agent) => agent.name === input.name && agent.node === input.node)
      && roster.nodes.some((node) =>
        node.name === input.node && node.live && node.capabilities.includes(input.capability))
    if (registered && this.#exitOnRegistration === input.name) {
      this.#exitOnRegistration = undefined
      setTimeout(() => this.emitAgentExit(input.name, 'exited'), 0)
    }
    return registered
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

const githubIssueFile = (repo: string, number = 48, owner = 'AgentWorkforce') => ({
  provider: 'github',
  objectType: 'issue',
  objectId: `${repo}-${number}`,
  payload: {
    number,
    title: `GitHub-only ${repo} issue`,
    body: 'Dispatch the repository-qualified GitHub issue.',
    state: 'open',
    labels: [{ name: 'factory' }, { name: repo }],
    url: `https://github.com/${owner}/${repo}/issues/${number}`,
    repository: { name: repo, owner: { login: owner } },
  },
})

const githubConnectionIssue = (
  repo: string,
  number: number,
  content: unknown = githubIssueFile(repo, number),
) => ({
  repo: `AgentWorkforce/${repo}`,
  number,
  path: `/github/repos/AgentWorkforce__${repo}/issues/by-id/${number}.json`,
  content,
})

const githubIssueFound = (repo: string, number: number, content?: unknown): GithubIssueLookup =>
  ({ outcome: 'found', issue: githubConnectionIssue(repo, number, content) })

const githubIssueNotFound = (): GithubIssueLookup => ({ outcome: 'not-found' })

const githubIssueIndeterminate = (
  reason = 'repository not visible without authentication',
): GithubIssueLookup => ({ outcome: 'indeterminate', reason })

const fakeGithubConnectionRead = (
  resolveIssue: (repo: string, number: number) => ReturnType<GithubConnectionRead['getIssue']>,
): GithubConnectionRead => ({ getIssue: vi.fn(resolveIssue) })

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

  it.each([
    { name: 'warns for a meaningfully stale artifact', info: staleVersionInfo, warns: true },
    { name: 'stays quiet for the current artifact', info: currentVersionInfo, warns: false },
  ])('$name', async ({ info, warns }) => {
    const logger = { warn: vi.fn() }

    await reportFactoryVersionDrift(async () => info, logger)

    expect(logger.warn).toHaveBeenCalledTimes(warns ? 1 : 0)
    if (warns) {
      expect(logger.warn).toHaveBeenCalledWith(
        '[factory] version drift detected: running 0.1.20; published latest 0.1.58',
        { versionsBehind: 38, installedAt: staleVersionInfo.installedAt },
      )
    }
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

  it('requires and forwards a workflow path for workflow capability spawns', async () => {
    expect(() => parseFleetCommand([
      'fleet',
      'spawn',
      'workflow:run',
      '--name',
      'workflow-a',
    ])).toThrow('factory fleet spawn workflow:run requires --workflow <path>')

    expect(parseFleetCommand([
      'fleet',
      'spawn',
      'workflow:run',
      '--name',
      'workflow-a',
      '--workflow',
      'workflows/verify-target.ts',
    ])).toEqual({
      kind: 'spawn',
      input: {
        capability: 'workflow:run',
        node: undefined,
        name: 'workflow-a',
        task: undefined,
        workflow: 'workflows/verify-target.ts',
        model: undefined,
        cwd: undefined,
        sessionRef: undefined,
      },
    })

    const fleet = new FakeFleetClient()
    const code = await runFleetCli([
      'fleet',
      'spawn',
      'workflow:run',
      '--name',
      'workflow-a',
      '--workflow',
      'workflows/verify-target.ts',
    ], {
      fleet,
      stdout: buffer(),
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(fleet.spawns).toContainEqual(expect.objectContaining({
      capability: 'workflow:run',
      name: 'workflow-a',
      workflow: 'workflows/verify-target.ts',
    }))
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

  it('parses mounted Notion intake as a first-class source command', () => {
    expect(parseFleetCommand(['intake', 'notion', 'ops/notion.json'])).toEqual({
      kind: 'notion-intake',
      manifestPath: 'ops/notion.json',
    })
    expect(() => parseFleetCommand(['intake', 'linear', 'ops/linear.json'])).toThrow(
      'currently requires the notion source',
    )
    expect(() => parseFleetCommand(['intake', 'notion'])).toThrow(
      'requires a manifest path',
    )
    expect(parseFleetCommand([
      'intake',
      'notion',
      'generate',
      '--data-source',
      'collection://a7fb83ad-c667-4003-a1dc-132c6826aac1',
      '--worker-mount-transport',
      'relay-channel',
    ])).toEqual({
      kind: 'notion-manifest',
      dataSourceId: 'collection://a7fb83ad-c667-4003-a1dc-132c6826aac1',
      workerMountTransport: 'relay-channel',
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
    expect(parseFleetCommand(['reap-orphans', '--include-held'])).toEqual({
      kind: 'factory',
      action: 'reap-orphans',
      includeHeld: true,
    })
    expect(() => parseFleetCommand(['reap-orphans', '--unknown'])).toThrow(/Unknown factory reap-orphans option/u)
  })

  it('parses the factory live start command', () => {
    expect(parseFleetCommand(['start', '--mode', 'live'])).toEqual({
      kind: 'factory',
      action: 'start',
      mode: 'live',
    })
  })

  it('parses init with an explicit repository and workspace', () => {
    expect(parseFleetCommand(['init', 'Acme/widgets', '--workspace', 'rw_widgets'])).toEqual({
      kind: 'factory-init',
      repo: 'Acme/widgets',
      workspaceId: 'rw_widgets',
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

  it('stops at the nearest project workspace key instead of reusing an ancestor broker', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-broker-boundary-'))
    try {
      const project = join(root, 'projects', 'factory')
      const nested = join(project, 'packages', 'factory-sdk')
      const projectStateDir = join(project, '.agentworkforce', 'relay')
      const projectConnectionPath = join(projectStateDir, 'connection.json')
      const ancestorConnectionPath = join(root, '.agentworkforce', 'relay', 'connection.json')
      await mkdir(dirname(ancestorConnectionPath), { recursive: true })
      await mkdir(projectStateDir, { recursive: true })
      await mkdir(nested, { recursive: true })
      await writeFile(ancestorConnectionPath, JSON.stringify({ port: 3889 }))
      await writeFile(join(projectStateDir, 'workspace-key.json'), JSON.stringify({ workspace_key: 'rk_live_project' }))

      expect(resolveBrokerConnectionPath(nested, {})).toBe(projectConnectionPath)
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

  it('requires a separate explicit relay state directory when dedicated broker isolation is enabled', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-dedicated-broker-'))
    try {
      const projectStateDir = join(root, '.agentworkforce', 'relay')
      const dedicatedStateDir = join(root, '.agentworkforce', 'factory-relay')
      await mkdir(projectStateDir, { recursive: true })
      await writeFile(join(projectStateDir, 'connection.json'), JSON.stringify({ port: 3890 }))

      expect(() => resolveFactoryBrokerConnectionPath(root, {}, true))
        .toThrow(/requires AGENT_RELAY_STATE_DIR/u)
      expect(() => resolveFactoryBrokerConnectionPath(root, { AGENT_RELAY_STATE_DIR: projectStateDir }, true))
        .toThrow(/resolves to the project broker/u)
      expect(() => resolveFactoryBrokerConnectionPath(root, {
        AGENT_RELAY_STATE_DIR: join(projectStateDir, '..', 'relay'),
      }, true)).toThrow(/resolves to the project broker/u)
      expect(resolveFactoryBrokerConnectionPath(root, { AGENT_RELAY_STATE_DIR: dedicatedStateDir }, true))
        .toBe(join(dedicatedStateDir, 'connection.json'))
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects the shared project relay path before its connection file exists', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-empty-dedicated-broker-'))
    try {
      const projectStateDir = join(root, '.agentworkforce', 'relay')
      expect(() => resolveFactoryBrokerConnectionPath(root, { AGENT_RELAY_STATE_DIR: projectStateDir }, true))
        .toThrow(/resolves to the project broker/u)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects the project relay path even when broker discovery finds an ancestor first', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-ancestor-dedicated-broker-'))
    try {
      const project = join(root, 'project')
      const projectStateDir = join(project, '.agentworkforce', 'relay')
      const ancestorConnectionPath = join(root, '.agentworkforce', 'relay', 'connection.json')
      await mkdir(dirname(ancestorConnectionPath), { recursive: true })
      await mkdir(project, { recursive: true })
      await writeFile(ancestorConnectionPath, JSON.stringify({ port: 3890 }))

      expect(() => resolveFactoryBrokerConnectionPath(project, {
        AGENT_RELAY_STATE_DIR: projectStateDir,
      }, true)).toThrow(/resolves to the project broker/u)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects a symlink that resolves to the shared project relay path', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-symlink-dedicated-broker-'))
    try {
      const projectStateDir = join(root, '.agentworkforce', 'relay')
      const stateAlias = join(root, 'relay-alias')
      await mkdir(projectStateDir, { recursive: true })
      await symlink(projectStateDir, stateAlias, 'dir')

      expect(() => resolveFactoryBrokerConnectionPath(root, {
        AGENT_RELAY_STATE_DIR: stateAlias,
      }, true)).toThrow(/resolves to the project broker/u)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

describe('parseGithubIssueSelector normalization matrix', () => {
  // Three review rounds each found a different normalization gap in this
  // resolution (default-only collapse, org-only expansion, bare-label vs
  // normalized comparison). Every candidate and every configured entry must
  // pass through the same canonicalization before any comparison — this
  // table exercises the combinations that produced each of those bugs,
  // not just the one case each round happened to name.
  const buildConfig = (overrides: {
    org?: string
    byLabel?: Record<string, string>
    default?: string
  }): FactoryConfig => FactoryConfigSchema.parse({
    workspaceId: 'factory-cli-test',
    repos: {
      byLabel: overrides.byLabel ?? {},
      clonePaths: {},
      ...(overrides.org !== undefined ? { org: overrides.org } : {}),
      ...(overrides.default !== undefined ? { default: overrides.default } : {}),
    },
    stateIds: TEST_STATE_IDS,
  })

  it.each([
    [
      'qualified selector, bare label route, org set: resolves via org-prefixing',
      'work#5',
      { org: 'AgentWorkforce', byLabel: { work: 'factory' } },
      { number: 5, repo: 'AgentWorkforce/factory' },
    ],
    [
      'qualified selector, bare label route, org unset: cannot resolve without an owner',
      'work#5',
      { byLabel: { work: 'factory' } },
      undefined,
    ],
    [
      'qualified selector, cross-owner qualified label route, org set to a different owner: label route wins over org',
      'work#5',
      { org: 'AgentWorkforce', byLabel: { work: 'OtherOrg/partner-repo' } },
      { number: 5, repo: 'OtherOrg/partner-repo' },
    ],
    [
      'qualified selector, cross-owner qualified label route, org unset: resolves without needing org',
      'work#5',
      { byLabel: { work: 'OtherOrg/partner-repo' } },
      { number: 5, repo: 'OtherOrg/partner-repo' },
    ],
    [
      'qualified selector, already fully owner/repo-qualified: resolves directly regardless of byLabel',
      'AgentWorkforce/factory#5',
      { org: 'AgentWorkforce', byLabel: { work: 'factory' } },
      { number: 5, repo: 'AgentWorkforce/factory' },
    ],
    [
      'qualified selector, label match is case-insensitive',
      'WORK#5',
      { org: 'AgentWorkforce', byLabel: { work: 'factory' } },
      { number: 5, repo: 'AgentWorkforce/factory' },
    ],
    [
      'qualified selector, unconfigured label: rejected regardless of org',
      'unknown#5',
      { org: 'AgentWorkforce', byLabel: { work: 'factory' } },
      undefined,
    ],
    [
      'bare selector: never carries a repo, independent of org/byLabel shape',
      '5',
      { org: 'AgentWorkforce', byLabel: { work: 'factory' }, default: 'AgentWorkforce/factory' },
      { number: 5 },
    ],
    [
      'bare selector with no default and no org: still just the number',
      '5',
      { byLabel: { work: 'factory' } },
      { number: 5 },
    ],
  ] as const)('%s', (_name, key, overrides, expected) => {
    const config = buildConfig(overrides)
    if (expected === undefined) {
      expect(() => parseGithubIssueSelector(key, config)).toThrow(/is not one of the configured Factory routes/)
    } else {
      expect(parseGithubIssueSelector(key, config)).toEqual(expected)
    }
  })
})

describe('fleet CLI runtime', () => {
  it.each([
    { name: 'warns after spawning without a discovered connection', started: true, connection: false, warns: true },
    { name: 'stays quiet when reusing a broker', started: false, connection: false, warns: false },
    { name: 'stays quiet when a connection was discovered', started: true, connection: true, warns: false },
  ])('$name', async ({ started, connection, warns }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-broker-warning-'))
    const previousCwd = process.cwd()
    try {
      if (connection) {
        const connectionPath = join(root, '.agentworkforce', 'relay', 'connection.json')
        await mkdir(dirname(connectionPath), { recursive: true })
        await writeFile(connectionPath, JSON.stringify({ port: 3890 }))
      }
      process.chdir(root)
      const cwd = process.cwd()
      const errors = buffer()
      const ensureRelayBroker = vi.fn(async () => ({
        client: fakeHarnessClient(),
        started,
      }))

      const code = await runFleetCli(['fleet', 'roster'], {
        ensureRelayBroker,
        env: {},
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(0)
      expect(ensureRelayBroker).toHaveBeenCalledWith(expect.objectContaining({
        cwd,
        connectionPath: connection ? join(cwd, '.agentworkforce', 'relay', 'connection.json') : undefined,
      }))
      expect(errors.text().includes('no existing relay broker connection found')).toBe(warns)
      expect(errors.text().match(/starting a NEW broker/gu) ?? []).toHaveLength(warns ? 1 : 0)
    } finally {
      process.chdir(previousCwd)
      await rm(root, { recursive: true, force: true })
    }
  })

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

  it('plans mounted Notion intake without loading config, publishing, or constructing a fleet', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-notion-intake-'))
    try {
      const mountedPage = join(root, 'notion', 'pages', '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf')
      await mkdir(mountedPage, { recursive: true })
      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Verify intake',
        'Summary: Prove the mounted spec normalizes without writes.',
        'Recipe: team',
        'Repos: AgentWorkforce/cloud',
        '',
        'Implementation details.',
      ].join('\n'))
      const manifestPath = join(root, 'notion.json')
      await writeFile(manifestPath, JSON.stringify({
        version: 1,
        mountRoot: './notion',
        statePath: './state.json',
        tasks: [{ page: '3b36800c1c90801db1cfc8f2e1cff7cf' }],
      }))
      const output = buffer()

      const code = await runFleetCli(['intake', 'notion', manifestPath, '--dry-run'], {
        createFleet: () => {
          throw new Error('dry-run Notion intake should not construct a fleet')
        },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(existsSync(join(root, 'state.json'))).toBe(false)
      expect(JSON.parse(output.text())).toMatchObject({
        ok: true,
        dispatch: false,
        results: [{ status: 'ready', target: { repo: 'AgentWorkforce/cloud' } }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('blocks default-auto Notion repository intake before invoking local gh or reserving a claim', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-notion-no-gh-'))
    try {
      const mountedPage = join(root, 'notion', 'pages', '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf')
      await mkdir(mountedPage, { recursive: true })
      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Verify explicit local identity',
        'Summary: Default auto must not reach the CLI adapter.',
        'Recipe: single',
        'Repos: AgentWorkforce/cloud',
      ].join('\n'))
      const manifestPath = join(root, 'notion.json')
      await writeFile(manifestPath, JSON.stringify({
        version: 1,
        mountRoot: './notion',
        statePath: './state.json',
        tasks: [{ page: '3b36800c1c90801db1cfc8f2e1cff7cf' }],
      }))
      const ghLogPath = join(root, 'gh.log')
      const ghPath = join(root, 'gh')
      await writeFile(ghPath, [
        '#!/bin/sh',
        'printf \'%s\\n\' "$*" >> "$FACTORY_TEST_GH_LOG"',
        'if [ "$1 $2" = "issue list" ]; then printf \'[]\'; fi',
        'if [ "$1 $2" = "repo view" ]; then printf \'private\'; fi',
      ].join('\n'))
      await chmod(ghPath, 0o755)
      vi.stubEnv('PATH', root)
      vi.stubEnv('FACTORY_TEST_GH_LOG', ghLogPath)
      const notionClaims = {
        get: vi.fn(async () => undefined),
        findBySourcePrefix: vi.fn(async () => []),
        claim: vi.fn(async () => { throw new Error('claim must not be reserved') }),
        dispose: vi.fn(async () => undefined),
      }
      const output = buffer()

      const code = await runFleetCli(['intake', 'notion', manifestPath], {
        fleet: new FakeFleetClient(),
        notionClaims,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(1)
      expect(JSON.parse(output.text())).toMatchObject({
        ok: false,
        results: [{
          status: 'blocked',
          reason: expect.stringMatching(/GitHub identity "auto".*Notion intake.*local gh.*identity.*"user"/iu),
        }],
      })
      await expect(readFile(ghLogPath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' })
      expect(notionClaims.claim).not.toHaveBeenCalled()
    } finally {
      vi.unstubAllEnvs()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('honours github.identity "app" for Notion intake instead of writing as the local gh user', async () => {
    // The gate in GhCliIssuePublisher is worthless if this call site hardcodes
    // an identity: this is the only production caller of runNotionIntake, so
    // the refusal must be reachable from the resolved contract.
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-notion-identity-'))
    try {
      const mountedPage = join(root, 'notion', 'pages', '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf')
      await mkdir(mountedPage, { recursive: true })
      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Verify identity gating',
        'Summary: Prove intake refuses under an app identity.',
        'Recipe: single',
        'Repos: AgentWorkforce/cloud',
      ].join('\n'))
      const manifestPath = join(root, 'notion.json')
      await writeFile(manifestPath, JSON.stringify({
        version: 1,
        mountRoot: './notion',
        statePath: './state.json',
        tasks: [{ page: '3b36800c1c90801db1cfc8f2e1cff7cf' }],
      }))
      const configPath = join(root, 'factory.config.json')
      // Deliberately the SPLIT contract shape. A reader that only looked at a
      // flat `github` key would miss this and fall back to `auto`, silently
      // permitting the local-user write.
      await writeFile(configPath, JSON.stringify({
        workspaceConfig: { repos: { org: 'AgentWorkforce', names: ['cloud'] } },
        nodeConfig: { github: { identity: 'app' } },
      }))

      const durableClaims = new Map<string, { sourceKey: string; digest: string; claimedAt: string }>()
      const notionClaims = {
        get: vi.fn(async (sourceKey: string) => durableClaims.get(sourceKey)),
        findBySourcePrefix: vi.fn(async () => []),
        claim: vi.fn(async (claim: { sourceKey: string; digest: string; claimedAt: string }) => {
          durableClaims.set(claim.sourceKey, claim)
          return { status: 'claimed' as const, claim }
        }),
        dispose: vi.fn(async () => undefined),
      }
      const output = buffer()
      // Capture the identity the CLI resolved, and keep the publisher's reads
      // hermetic so the refusal is the only thing that can block the task.
      const resolved: string[] = []
      const notionGithub = (identity: string) => {
        resolved.push(identity)
        return {
          assertWritable: () => {
            if (identity === 'app') throw new Error('GitHub identity "app" refuses injected publisher writes')
          },
          repositoryVisibility: async () => 'private' as const,
          missingLabels: async () => [],
          findBySource: async () => undefined,
          createIssue: async () => ({ number: 42, url: 'https://github.test/issues/42' }),
          updateIssue: async () => undefined,
        }
      }

      const code = await runFleetCli(
        ['intake', 'notion', manifestPath, '--config', configPath],
        { fleet: new FakeFleetClient(), notionClaims, notionGithub, stdout: output, stderr: buffer() },
      )

      // The CLI read the SPLIT contract's node half, not a flat github key.
      expect(resolved).toEqual(['app'])
      expect(code).toBe(1)
      expect(JSON.parse(output.text())).toMatchObject({
        ok: false,
        results: [{
          status: 'blocked',
          reason: expect.stringContaining('GitHub identity "app"'),
        }],
      })
      // The refusal must not have consumed the exactly-once claim.
      expect(notionClaims.claim).not.toHaveBeenCalled()
      expect(durableClaims.size).toBe(0)

      // Split config is a shallow top-level merge. An explicitly present,
      // empty node github block replaces the workspace block, so the schema
      // default is `auto`; falling back to the workspace identity here would
      // disagree with loadFactoryConfig.
      await writeFile(configPath, JSON.stringify({
        workspaceConfig: {
          repos: { org: 'AgentWorkforce', names: ['cloud'] },
          github: { identity: 'app' },
        },
        nodeConfig: { github: {} },
      }))
      const permittedOutput = buffer()
      const permittedCode = await runFleetCli(
        ['intake', 'notion', manifestPath, '--config', configPath],
        { fleet: new FakeFleetClient(), notionClaims, notionGithub, stdout: permittedOutput, stderr: buffer() },
      )

      expect(resolved).toEqual(['app', 'auto'])
      expect(permittedCode).toBe(0)
      expect(JSON.parse(permittedOutput.text())).toMatchObject({
        ok: true,
        results: [{ status: 'dispatched' }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('returns after exact-path intake while preserving the spawned worker infrastructure', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-notion-dispatch-'))
    try {
      const mountedPage = join(root, 'notion', 'pages', '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf')
      const projectPath = join(root, 'project')
      await mkdir(mountedPage, { recursive: true })
      await mkdir(projectPath, { recursive: true })
      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Verify exact-path dispatch',
        'Summary: Prove intake returns while its worker continues.',
        'Recipe: single',
        `Project-Paths: ${projectPath}`,
      ].join('\n'))
      const manifestPath = join(root, 'notion.json')
      const manifest = {
        version: 1,
        mountRoot: './notion',
        statePath: './state.json',
        tasks: [{ page: '3b36800c1c90801db1cfc8f2e1cff7cf' }],
      }
      await writeFile(manifestPath, JSON.stringify(manifest))
      const output = buffer()
      const fleet = new FakeFleetClient()
      const durableClaims = new Map<string, { sourceKey: string; digest: string; claimedAt: string }>()
      const notionClaims = {
        get: vi.fn(async (sourceKey: string) => durableClaims.get(sourceKey)),
        findBySourcePrefix: vi.fn(async (sourceKeyPrefix: string) => [...durableClaims.values()]
          .filter((claim) => claim.sourceKey.startsWith(sourceKeyPrefix))),
        claim: vi.fn(async (claim: { sourceKey: string; digest: string; claimedAt: string }) => {
          const existing = durableClaims.get(claim.sourceKey)
          if (existing) return { status: 'existing' as const, claim: existing }
          durableClaims.set(claim.sourceKey, claim)
          return { status: 'claimed' as const, claim }
        }),
        dispose: vi.fn(async () => undefined),
      }

      const code = await runFleetCli(['intake', 'notion', manifestPath], {
        fleet,
        notionClaims,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(fleet.preservedInfrastructure).toBe(1)
      expect(JSON.parse(output.text())).toMatchObject({
        ok: true,
        results: [{ status: 'dispatched', target: { projectPath } }],
      })

      await writeFile(manifestPath, JSON.stringify({
        ...manifest,
        workerMountTransport: { kind: 'relay-channel' },
      }))
      const contracts = {
        publish: vi.fn(async () => ({
          kind: 'relay-channel' as const,
          channel: 'factory-notion-e1cff7cf-aabbccddee',
          messageIds: ['message-1'],
          encoding: 'base64-chunks-v1' as const,
        })),
        dispose: vi.fn(async () => { throw new Error('cleanup failed') }),
      }
      const migratedOutput = buffer()
      const migratedErrors = buffer()
      const migratedCode = await runFleetCli(['intake', 'notion', manifestPath], {
        fleet,
        notionClaims,
        notionContracts: contracts,
        env: {},
        stdout: migratedOutput,
        stderr: migratedErrors,
      })

      expect(migratedCode).toBe(0)
      expect(fleet.spawns).toHaveLength(1)
      expect(fleet.messages).toEqual([expect.objectContaining({
        to: expect.stringContaining('notion-e1cff7cf'),
        text: expect.stringContaining('factory-notion-e1cff7cf-aabbccddee'),
        mode: 'steer',
      })])
      expect(contracts.dispose).toHaveBeenCalledOnce()
      expect(notionClaims.dispose).toHaveBeenCalledTimes(2)
      expect(migratedErrors.text()).toContain('Notion contract publisher failed during shutdown')
      expect(JSON.parse(migratedOutput.text())).toMatchObject({
        ok: true,
        results: [{ status: 'already-dispatched', target: { projectPath } }],
      })

      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Changed exact-path dispatch',
        'Summary: This digest must not reuse the durable claim.',
        'Recipe: single',
        `Project-Paths: ${projectPath}`,
      ].join('\n'))
      await writeFile(manifestPath, JSON.stringify({
        ...manifest,
        statePath: './changed-state.json',
      }))
      const changedOutput = buffer()
      const changedCode = await runFleetCli(['intake', 'notion', manifestPath], {
        fleet,
        notionClaims,
        stdout: changedOutput,
        stderr: buffer(),
      })

      expect(changedCode).toBe(1)
      expect(fleet.spawns).toHaveLength(1)
      expect(JSON.parse(changedOutput.text())).toMatchObject({
        ok: false,
        results: [{
          status: 'blocked',
          reason: 'durable Notion claim digest does not match the mounted spec',
        }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not borrow an ambient workspace when an explicit intake environment has no key', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-notion-no-key-'))
    try {
      const mountedPage = join(root, 'notion', 'pages', '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf')
      await mkdir(mountedPage, { recursive: true })
      await writeFile(join(mountedPage, 'content.md'), [
        '# Chief Spec',
        'Status: ready',
        'Title: Verify isolated credentials',
        'Summary: Refuse ambient workspace credentials.',
        'Recipe: single',
        'Repos: AgentWorkforce/cloud',
      ].join('\n'))
      const manifestPath = join(root, 'notion.json')
      await writeFile(manifestPath, JSON.stringify({
        version: 1,
        mountRoot: './notion',
        workerMountTransport: { kind: 'relay-channel' },
        tasks: [{ page: '3b36800c1c90801db1cfc8f2e1cff7cf' }],
      }))
      const errors = buffer()

      const code = await runFleetCli(['intake', 'notion', manifestPath], {
        env: {},
        createFleet: () => { throw new Error('missing key must fail before fleet construction') },
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('requires an active Agent Relay workspace for its durable shared claim')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
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

  it('keeps a populated projection preferred over the API fallback when the connection is not ready', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-github-fallback-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const issue = '/github/repos/AgentWorkforce__pear/issues/48/meta.json'
      const integrations = fakeIntegrationConnections(async () => ({
        ready: false,
        state: 'degraded',
        initialSyncState: 'complete',
      }))
      const githubRead = fakeGithubConnectionRead(async () => {
        throw new Error('populated projection must remain preferred')
      })
      const mount = Object.assign(
        mountWithIntegrationConnections({ [issue]: githubIssueFile('pear') }, integrations),
        { githubRead },
      )
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli(['triage', '48', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: errors,
      })

      expect(code).toBe(0)
      expect(errors.text()).toContain('GitHub projection is not ready (degraded, complete)')
      expect(JSON.parse(output.text())).toMatchObject({
        issueResolution: { source: 'relayfile-projection' },
      })
      expect(githubRead.getIssue).not.toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('uses connected-not-ready status as the reported reason an empty projection cannot answer', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-integration-github-empty-fallback-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const integrations = fakeIntegrationConnections(async () => ({
        ready: false,
        state: 'degraded',
        initialSyncState: 'complete',
      }))
      const githubRead = fakeGithubConnectionRead(async (_repo, number) => githubIssueFound('pear', number))
      const mount = Object.assign(
        mountWithIntegrationConnections({}, integrations),
        {
          githubRead,
          getLocalMountHealth: () => ({ degraded: false }),
        },
      )
      const output = buffer()

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issueResolution: {
          source: 'github-api-fallback',
          detail: expect.stringContaining('GitHub projection connection is not ready (degraded, complete)'),
          projection: {
            githubConnection: { ready: false, state: 'degraded', initialSyncState: 'complete' },
          },
        },
      })
      expect(githubRead.getIssue).toHaveBeenCalledWith('AgentWorkforce/pear', 222)
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
        '--backend',
        'relay',
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

  // The plumbing that was missing: without it a cloud deployment can configure
  // `relay.agentName` and still register as the default, colliding with itself.
  it.each([
    ['forwards a configured relay agent name to fleet construction', { relay: { agentName: 'factory-cloud' } }, 'factory-cloud'],
    ['leaves the relay agent name unset when the config omits it', {}, undefined],
  ])('%s', async (_label, overrides, expected) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-relay-identity-'))
    try {
      const configPath = await writeConfig(root, overrides)
      const createFleetCalls: Array<{ relayAgentName?: string }> = []
      const output = buffer()

      const code = await runFleetCli([
        '--backend',
        'relay',
        'run-once',
        '--dry-run',
        '--config',
        configPath,
      ], {
        createFleet: (opts) => {
          createFleetCalls.push(opts as { relayAgentName?: string })
          return new FakeFleetClient()
        },
        cloudMountFromConfig: async () => new FakeMountClient({ [issuePath]: issueFile }),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(createFleetCalls).toHaveLength(1)
      expect(createFleetCalls[0]?.relayAgentName).toBe(expected)
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
      const probePrGhRunner = vi.fn(async () => ({ stdout: '[]' }))
      const createFactorySpy = vi.fn((_config, ports: FactoryPorts) => {
        expect(ports.reporter).toBe(reporter)
        expect(ports).not.toHaveProperty('probePrGhRunner')
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
        probePrGhRunner,
        createFactory: createFactorySpy,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(probePrGhRunner).not.toHaveBeenCalled()
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

  it('uses the hosted rotating path token for Cloud reporting without local login', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-hosted-reporting-'))
    try {
      const outboxPath = join(root, 'factory-cloud-events.json')
      const configPath = await writeConfig(root, {
        workspaceId: 'rw_7ccfea89',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
        },
        reporting: {
          enabled: true,
          instanceName: 'factory-khaliq-cloud',
          outboxPath,
          batchSize: 100,
          requestTimeoutMs: 1_000,
        },
      })
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({ pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: true })),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(async () => {}),
      } as unknown as Factory
      let capturedReporter: FactoryEventReporter | undefined
      const createFactorySpy = vi.fn((_config, ports: FactoryPorts) => {
        capturedReporter = ports.reporter
        return factory
      }) as typeof createFactory
      const cloudSessionProvider = vi.fn(async () => {
        throw new Error('local Cloud login must not be consulted')
      })
      const cloudAccessTokenFetch = vi.fn(async () =>
        Response.json({ accessToken: 'relay_pa_test' }))
      const batches: Array<Record<string, unknown>> = []
      const cloudReporterFetch = vi.fn(async (request: string | URL | Request, init?: RequestInit) => {
        expect(String(request)).toBe('https://cloud.example/api/v1/factory/events')
        expect(new Headers(init?.headers).get('authorization')).toBe(
          'Bearer relay_pa_test',
        )
        const batch = JSON.parse(String(init?.body)) as Record<string, unknown>
        batches.push(batch)
        const accepted = Array.isArray(batch.events) ? batch.events.length : 0
        return Response.json({ accepted, duplicates: 0 }, { status: 201 })
      })

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        env: {
          FACTORY_CLOUD_ACCESS_TOKEN_URL: 'http://factory-auth.do/factory-primary/v1/access',
          CLOUD_API_URL: 'https://cloud.example',
        },
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: createFactorySpy,
        cloudSessionProvider,
        cloudAccessTokenFetch: cloudAccessTokenFetch as unknown as typeof fetch,
        cloudReporterFetch: cloudReporterFetch as unknown as typeof fetch,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(capturedReporter).toBeDefined()
      expect(factory.runOnce).toHaveBeenCalledOnce()
      expect(cloudSessionProvider).not.toHaveBeenCalled()
      expect(cloudAccessTokenFetch).toHaveBeenCalledWith(
        new URL('http://factory-auth.do/factory-primary/v1/access'),
        expect.objectContaining({ method: 'GET', redirect: 'error' }),
      )
      expect(cloudReporterFetch).toHaveBeenCalled()
      const events = batches.flatMap((batch) => (
        Array.isArray(batch.events) ? batch.events as Array<{ type?: string }> : []
      ))
      expect(events.map((event) => event.type)).toEqual(expect.arrayContaining([
        'instance.started',
        'instance.stopping',
        'instance.stopped',
      ]))
      expect(batches[0]?.instance).toMatchObject({
        metadata: expect.objectContaining({ name: 'factory-khaliq-cloud' }),
      })
      await expect(new FileFactoryCloudEventOutbox({ path: outboxPath }).stats())
        .resolves.toMatchObject({ pending: 0 })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('cancels a hanging hosted credential request when the CLI shuts down', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-hosted-token-shutdown-'))
    try {
      const outboxPath = join(root, 'factory-cloud-events.json')
      const configPath = await writeConfig(root, {
        workspaceId: 'rw_7ccfea89',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
        },
        reporting: {
          enabled: true,
          outboxPath,
          batchSize: 100,
          requestTimeoutMs: 100,
        },
      })
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({ pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: true })),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(async () => {}),
      } as unknown as Factory
      const tokenSignals: Array<AbortSignal | undefined> = []
      // The private credential endpoint accepts the connection and never answers.
      const cloudAccessTokenFetch = vi.fn(async (_url: unknown, init?: RequestInit) => {
        tokenSignals.push(init?.signal ?? undefined)
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => {
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))
          }, { once: true })
        })
      })

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        env: {
          FACTORY_CLOUD_ACCESS_TOKEN_URL: 'http://factory-auth.do/factory-primary/v1/access',
          CLOUD_API_URL: 'https://cloud.example',
        },
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        cloudAccessTokenFetch: cloudAccessTokenFetch as unknown as typeof fetch,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(tokenSignals.length).toBeGreaterThan(0)
      // Shutdown must leave no referenced credential request behind, or the
      // process outlives the reporter's close() deadline. `aborted` alone does
      // not prove that: the reporter also aborts this same controller when the
      // flush deadline lapses, so an unbound request still reports aborted and
      // the assertion could not fail for the reason it guards. Assert the abort
      // *reason* instead — only the shutdown path produces it.
      expect(tokenSignals.map((signal) => (signal?.reason as Error | undefined)?.name)).toEqual(
        tokenSignals.map(() => 'FactoryCloudShutdownError'),
      )
    } finally {
      // The cancelled flush unwinds its outbox writes just after the CLI
      // returns; retry rmdir rather than race it.
      await rm(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 25 })
    }
  })

  it('falls back to the local Cloud session when the hosted credential URL is blank', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-hosted-blank-url-'))
    try {
      const outboxPath = join(root, 'factory-cloud-events.json')
      const configPath = await writeConfig(root, {
        workspaceId: 'rw_7ccfea89',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
        },
        reporting: {
          enabled: true,
          outboxPath,
          batchSize: 100,
          requestTimeoutMs: 1_000,
        },
      })
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({ pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: true })),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(async () => {}),
      } as unknown as Factory
      const sessionBatches: Array<Record<string, unknown>> = []
      const cloudSessionProvider = vi.fn(async () => ({
        auth: { apiUrl: 'https://cloud.example' },
        client: {
          snapshot: () => ({ accessToken: 'relay_pa_session' }),
          fetch: async (_path: string, init?: RequestInit) => {
            const batch = JSON.parse(String(init?.body)) as Record<string, unknown>
            sessionBatches.push(batch)
            const accepted = Array.isArray(batch.events) ? batch.events.length : 0
            return Response.json({ accepted, duplicates: 0 }, { status: 201 })
          },
        },
      }) as unknown as CloudSession)
      const cloudAccessTokenFetch = vi.fn(async () => {
        throw new Error('hosted credential endpoint must not be consulted')
      })

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        env: {
          FACTORY_CLOUD_ACCESS_TOKEN_URL: '   ',
          CLOUD_API_URL: 'https://cloud.example',
        },
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        resolveWorkspace: async () => ({ workspaceId: 'rw_7ccfea89' }),
        cloudSessionProvider,
        cloudAccessTokenFetch: cloudAccessTokenFetch as unknown as typeof fetch,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      // A blank URL is not hosted configuration: the local-session path owns
      // telemetry instead of `new URL('')` throwing the reporter away.
      expect(cloudSessionProvider).toHaveBeenCalledOnce()
      expect(cloudAccessTokenFetch).not.toHaveBeenCalled()
      expect(sessionBatches.length).toBeGreaterThan(0)
      await expect(new FileFactoryCloudEventOutbox({ path: outboxPath }).stats())
        .resolves.toMatchObject({ pending: 0 })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps dispatch successful and the event pending when hosted telemetry returns 503', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-hosted-reporting-fail-open-'))
    try {
      const outboxPath = join(root, 'factory-cloud-events.json')
      const configPath = await writeConfig(root, {
        workspaceId: 'rw_7ccfea89',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
        },
        reporting: {
          enabled: true,
          instanceName: 'factory-khaliq-cloud',
          outboxPath,
          batchSize: 100,
          requestTimeoutMs: 100,
        },
      })
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(async () => ({ pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: true })),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(async () => {}),
      } as unknown as Factory
      const cloudAccessTokenFetch = vi.fn(async () =>
        Response.json({ accessToken: 'relay_pa_test' }))
      const cloudReporterFetch = vi.fn(async () =>
        new Response('unavailable', { status: 503 }))

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        env: {
          FACTORY_CLOUD_ACCESS_TOKEN_URL: 'http://factory-auth.do/factory-primary/v1/access',
          CLOUD_API_URL: 'https://cloud.example',
        },
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        cloudAccessTokenFetch: cloudAccessTokenFetch as unknown as typeof fetch,
        cloudReporterFetch: cloudReporterFetch as unknown as typeof fetch,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(factory.runOnce).toHaveBeenCalledOnce()
      expect(cloudReporterFetch).toHaveBeenCalled()
      const stats = await new FileFactoryCloudEventOutbox({ path: outboxPath }).stats()
      expect(stats.pending).toBeGreaterThan(0)
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

  // factory#319. The state write becomes locally visible before provider
  // confirmation returns. Pin the post-spawn read in that interval and test
  // both outcomes: confirmation lets the completed dispatch converge; a
  // rejected confirmation MUST NOT excuse the visible park.
  it.each([
    { confirmation: 'acked' as const, expectedExit: 0 },
    { confirmation: 'failed' as const, expectedExit: 3 },
  ])('waits for its in-flight park confirmation ($confirmation)', async ({ confirmation, expectedExit }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-park-inflight-'))
    try {
      const configPath = await writeConfig(root, {
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => ({
          repo: input.repo,
          number: 79,
          url: 'https://github.com/AgentWorkforce/pear/pull/79',
          headRef: input.headRef ?? 'unexpected-local-head',
        }),
        closePullRequest: async () => undefined,
      }
      const fleet = new ControlledCompletingRemoteFleetClient()
      // `setState` writes `stateId` at the top level of the record; the initial
      // fixture carries it under `payload`. Read either.
      const stateOf = (entry: { content: unknown } | undefined): string | undefined => {
        const content = entry?.content as { stateId?: string; payload?: { stateId?: string } } | undefined
        return content?.stateId ?? content?.payload?.stateId
      }

      class ParkConfirmBlockingMount extends FakeMountClient {
        parkWritten = false
        releaseConfirm?: () => void
        reReadSeen = false

        override async writeFile(path: string, content: unknown, opts?: { guarded?: boolean }): Promise<void> {
          await super.writeFile(path, content, opts)
          if (path === issuePath && stateOf(this.files.get(path)) === TEST_STATE_IDS.humanReview) {
            this.parkWritten = true
          }
        }

        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path === issuePath && fleet.implementerName && !fleet.exitEmitted) {
            // This is the dispatch's post-spawn re-read. Start completion here
            // instead of hoping a timer wins the race, then hold this read
            // until completion has made the parked state visible. Completion's
            // own reads pass through because `exitEmitted` is already true.
            fleet.exitEmitted = true
            fleet.emitAgentExit(fleet.implementerName, 'exited')
            await vi.waitFor(() => expect(this.parkWritten).toBe(true), { timeout: 1_000 })
            this.reReadSeen = true
            queueMicrotask(() => this.releaseConfirm?.())
          }
          if (path === issuePath && this.parkWritten && confirmation === 'acked') {
            if (!this.releaseConfirm) {
              // First issue read after the park is `setState`'s own readback
              // confirmation. Hold it, so `setState` cannot return. Bounded so
              // a wrong assumption about ordering fails loudly rather than
              // hanging the suite.
              await new Promise<void>((resolve) => {
                this.releaseConfirm = resolve
                setTimeout(resolve, 400)
              })
            }
          }
          return super.readFile(path)
        }

        override async confirmWrite(path: string): Promise<'acked' | 'pending' | 'failed' | 'timeout'> {
          if (path === issuePath && this.parkWritten) return confirmation
          return super.confirmWrite(path)
        }
      }

      const mount = new ParkConfirmBlockingMount({
        [issuePath]: issueFile,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
      }, githubWrite)
      const output = buffer()

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
        stderr: buffer(),
        probePrGhRunner: async () => ({ stdout: '[]' }),
      })

      // Both must hold or the test is not exercising the window at all.
      expect(mount.parkWritten).toBe(true)
      expect(mount.reReadSeen).toBe(true)
      expect(code).toBe(expectedExit)
      const releaseReasons = fleet.releases.map((release) => release.reason)
      if (confirmation === 'acked') {
        expect(releaseReasons).not.toContain('live dispatch state changed')
      } else {
        expect(releaseReasons).toContain('live dispatch state changed')
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it.each([
    { receipt: 'explicit already-matched' as const, legacyVoid: false },
    { receipt: 'legacy void' as const, legacyVoid: true },
  ])('does not attribute a third-party GitHub park from an in-flight $receipt receipt', async ({ legacyVoid }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-foreign-park-inflight-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const githubPath = '/github/repos/AgentWorkforce__pear/issues/by-id/48.json'
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => ({
          repo: input.repo,
          number: 80,
          url: 'https://github.com/AgentWorkforce/pear/pull/80',
          headRef: input.headRef ?? 'unexpected-local-head',
        }),
        closePullRequest: async () => undefined,
      }
      const fleet = new ControlledCompletingRemoteFleetClient()
      let thirdPartyParked = false
      let terminalLabelProvisioned = false
      let terminalViews = 0
      let terminalEdits = 0
      let releaseConfirmation: (() => void) | undefined

      class ThirdPartyGithubParkMount extends FakeMountClient {
        postSpawnReadSawPark = false

        parkAsThirdParty(): void {
          const existing = this.files.get(githubPath)
          const content = existing?.content as ReturnType<typeof githubIssueFile>
          const labels = content.payload.labels
            .filter((label) => label.name !== 'factory:in-progress')
          labels.push({ name: 'factory:human-review' })
          this.files.set(githubPath, {
            ...existing,
            content: {
              ...content,
              payload: { ...content.payload, labels },
            },
          })
          thirdPartyParked = true
        }

        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path === githubPath && fleet.implementerName && !fleet.exitEmitted) {
            // This is dispatch's post-spawn read. Completion starts here and
            // registers its fence before the CLI adapter's first label read.
            fleet.exitEmitted = true
            fleet.emitAgentExit(fleet.implementerName, 'exited')
            await vi.waitFor(() => expect(thirdPartyParked).toBe(true), { timeout: 1_000 })
            const result = await super.readFile(path)
            this.postSpawnReadSawPark = true
            // Let the adapter's provider-confirmation read finish only after
            // the dispatch has observed the foreign state while the fence is
            // still registered.
            setTimeout(() => releaseConfirmation?.(), 0)
            return result
          }
          return super.readFile(path)
        }
      }

      const githubContent = githubIssueFile('pear')
      githubContent.payload.title = '[factory-e2e] preserve foreign GitHub lifecycle ownership'
      githubContent.payload.body = [
        'Fix the completion race without attributing a third-party lifecycle change to Factory.',
        '',
        'Acceptance criteria:',
        '- A skipped GitHub CLI label edit never stamps Factory authorship.',
        '- The live dispatch state-change guard still releases the remote agent.',
        '- Add a deterministic integration regression for the provider-read ordering.',
      ].join('\n')
      const mount = new ThirdPartyGithubParkMount({
        [githubPath]: githubContent,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
      }, githubWrite)
      const providerLabels = new Set(['factory', 'pear'])
      const githubWriteback = new GhCliGithubWriteback({
        runner: async (args) => {
          if (args[0] === 'label' && args[1] === 'create') {
            terminalLabelProvisioned = args[2] === 'factory:human-review'
            return { stdout: '' }
          }
          if (args[0] === 'issue' && args[1] === 'view') {
            if (terminalLabelProvisioned) {
              terminalViews += 1
              if (terminalViews === 1) {
                // The third party wins before the adapter's first read. The
                // target is present and the previous label absent, so the CLI
                // must skip `gh issue edit` and report already-matched.
                providerLabels.delete('factory:in-progress')
                providerLabels.add('factory:human-review')
                mount.parkAsThirdParty()
              } else if (terminalViews === 2 && !releaseConfirmation) {
                await new Promise<void>((resolve) => {
                  releaseConfirmation = resolve
                  setTimeout(resolve, 400)
                })
              }
            }
            return { stdout: JSON.stringify({ labels: [...providerLabels].map((name) => ({ name })) }) }
          }
          if (args[0] === 'issue' && args[1] === 'edit') {
            if (terminalLabelProvisioned) terminalEdits += 1
            const added = args[args.indexOf('--add-label') + 1]
            const removed = args[args.indexOf('--remove-label') + 1]
            if (args.includes('--add-label') && added) providerLabels.add(added)
            if (args.includes('--remove-label') && removed) providerLabels.delete(removed)
          }
          return { stdout: '' }
        },
      })
      const effectiveGithubWriteback: GithubWriteback = legacyVoid
        ? {
            getIssueStatus: async (target) => await githubWriteback.getIssueStatus(target),
            postComment: async (target, body) => await githubWriteback.postComment(target, body),
            setStatus: async (target, status) => {
              await githubWriteback.setStatus(target, status)
            },
            closeIssue: async (target, body) => await githubWriteback.closeIssue(target, body),
          }
        : githubWriteback
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli([
        'dispatch',
        '48',
        '--backend',
        'relay',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        createFactory: (factoryConfig, ports) => createFactory(factoryConfig, {
          ...ports,
          githubWriteback: effectiveGithubWriteback,
        }),
        stdout: output,
        stderr: errors,
        probePrGhRunner: async () => ({ stdout: '[]' }),
      })

      expect(thirdPartyParked).toBe(true)
      expect(mount.postSpawnReadSawPark).toBe(true)
      expect(terminalEdits).toBe(0)
      expect(code).toBe(3)
      expect(errors.text()).toContain('Live state changed before writeback for 48')
      expect(fleet.releases.map((release) => release.reason)).toContain('live dispatch state changed')
      if (legacyVoid) {
        expect(errors.text()).toContain('GitHub writeback returned no ownership receipt')
        expect(errors.text()).toContain('"method":"setStatus"')
      } else {
        expect(errors.text()).not.toContain('GitHub writeback returned no ownership receipt')
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it.each([
    { receipt: 'explicit acknowledged' as const, legacyVoid: false },
    { receipt: 'legacy void' as const, legacyVoid: true },
  ])('does not attribute a third-party GitHub close from an in-flight $receipt receipt', async ({ legacyVoid }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-foreign-close-inflight-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        terminalState: 'done',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const githubPath = '/github/repos/AgentWorkforce__pear/issues/by-id/48.json'
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => ({
          repo: input.repo,
          number: 80,
          url: 'https://github.com/AgentWorkforce/pear/pull/80',
          headRef: input.headRef ?? 'unexpected-local-head',
        }),
        closePullRequest: async () => undefined,
      }
      const fleet = new ControlledCompletingRemoteFleetClient()
      let thirdPartyClosed = false

      class ThirdPartyGithubCloseMount extends FakeMountClient {
        postSpawnReadSawClose = false

        closeAsThirdParty(): void {
          const existing = this.files.get(githubPath)
          const content = existing?.content as ReturnType<typeof githubIssueFile>
          this.files.set(githubPath, {
            ...existing,
            content: {
              ...content,
              payload: { ...content.payload, state: 'closed' },
            },
          })
          thirdPartyClosed = true
        }

        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path === githubPath && fleet.implementerName && !fleet.exitEmitted) {
            // Pin the dispatch's post-spawn read behind completion. The
            // provider close command returns idempotently after another actor
            // creates the terminal event, so only the audit receipt can tell
            // the dispatch that it must not claim ownership.
            fleet.exitEmitted = true
            fleet.emitAgentExit(fleet.implementerName, 'exited')
            await vi.waitFor(() => expect(thirdPartyClosed).toBe(true), { timeout: 1_000 })
            this.postSpawnReadSawClose = true
          }
          return super.readFile(path)
        }
      }

      const githubContent = githubIssueFile('pear')
      githubContent.payload.title = '[factory-e2e] preserve foreign GitHub close ownership'
      githubContent.payload.body = [
        'Keep terminal lifecycle ownership provider-authoritative during concurrent close operations.',
        '',
        'Acceptance criteria:',
        '- An idempotent close must not claim Factory ownership when another actor creates the close event.',
        '- The post-spawn live-state guard must abort the dispatch.',
        '- Remote agents must be released with the live-state-change reason.',
      ].join('\n')
      const mount = new ThirdPartyGithubCloseMount({
        [githubPath]: githubContent,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
        '/github/repos/AgentWorkforce/pear/pulls/80/metadata.json': {
          provider: 'github',
          objectType: 'pull_request',
          objectId: '80',
          payload: {
            number: 80,
            title: '[factory-e2e] preserve foreign GitHub close ownership',
            body: 'Fixes #48',
            head_ref: 'factory/48-agentworkforce-pear',
            isDraft: false,
            state: 'MERGED',
            merged: true,
          },
        },
      }, githubWrite)
      let providerState = 'OPEN'
      const stateEvents: string[] = []
      let closeCommands = 0
      const githubWriteback = new GhCliGithubWriteback({
        runner: async (args) => {
          if (args[0] === 'issue' && args[1] === 'view' && args.includes('state')) {
            return { stdout: JSON.stringify({ state: providerState }) }
          }
          if (args[0] === 'api' && args[1] === 'user') return { stdout: 'factory-bot\n' }
          if (args[0] === 'api' && args[1] === '--paginate') {
            return { stdout: stateEvents.join('\n') }
          }
          if (args[0] === 'issue' && args[1] === 'close') {
            closeCommands += 1
            providerState = 'CLOSED'
            stateEvents.push('1\tclosed\tother-user')
            mount.closeAsThirdParty()
          }
          return { stdout: '' }
        },
      })
      const effectiveGithubWriteback: GithubWriteback = legacyVoid
        ? {
            getIssueStatus: async (target) => await githubWriteback.getIssueStatus(target),
            postComment: async (target, body) => await githubWriteback.postComment(target, body),
            setStatus: async (target, status) => await githubWriteback.setStatus(target, status),
            closeIssue: async (target, body) => {
              await githubWriteback.closeIssue(target, body)
            },
          }
        : githubWriteback
      const output = buffer()
      const errors = buffer()

      const code = await runFleetCli([
        'dispatch',
        '48',
        '--backend',
        'relay',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        createFactory: (factoryConfig, ports) => createFactory(factoryConfig, {
          ...ports,
          githubWriteback: effectiveGithubWriteback,
        }),
        stdout: output,
        stderr: errors,
        probePrGhRunner: async () => ({
          stdout: JSON.stringify([{
            number: 80,
            title: '[factory-e2e] preserve foreign GitHub close ownership',
            body: 'Fixes #48',
            headRefName: 'factory/48-agentworkforce-pear',
            isDraft: false,
            state: 'MERGED',
          }]),
        }),
      })

      expect(thirdPartyClosed, JSON.stringify({
        code,
        output: output.text(),
        errors: errors.text(),
        releases: fleet.releases,
      })).toBe(true)
      expect(mount.postSpawnReadSawClose).toBe(true)
      expect(closeCommands).toBe(1)
      expect(code).toBe(3)
      expect(errors.text()).toContain('Live state changed before writeback for 48')
      expect(fleet.releases.map((release) => release.reason)).toContain('live dispatch state changed')
      if (legacyVoid) {
        expect(errors.text()).toContain('GitHub writeback returned no ownership receipt')
        expect(errors.text()).toContain('"method":"closeIssue"')
      } else {
        expect(errors.text()).not.toContain('GitHub writeback returned no ownership receipt')
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('serializes terminal completion that starts after the ready read against the dispatch claim', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-claim-completion-fence-'))
    let releaseClaim: (() => void) | undefined
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        terminalState: 'human-review',
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const githubPath = '/github/repos/AgentWorkforce__pear/issues/by-id/48.json'
      const fleet = new ControlledCompletingRemoteFleetClient()
      let resolveCompletionRead!: () => void
      const completionRead = new Promise<void>((resolve) => {
        resolveCompletionRead = resolve
      })
      class CompletionObservedMount extends FakeMountClient {
        completionReadSeen = false

        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path === githubPath && fleet.exitEmitted && !this.completionReadSeen) {
            this.completionReadSeen = true
            resolveCompletionRead()
          }
          return await super.readFile(path)
        }
      }
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => ({
          repo: input.repo,
          number: 80,
          url: 'https://github.com/AgentWorkforce/pear/pull/80',
          headRef: input.headRef ?? 'unexpected-local-head',
        }),
        closePullRequest: async () => undefined,
      }
      const githubContent = githubIssueFile('pear')
      githubContent.payload.title = '[factory-e2e] serialize dispatch claim with terminal completion'
      githubContent.payload.body = [
        'Prevent a stale post-spawn ready snapshot from overwriting a concurrent terminal writeback.',
        '',
        'Acceptance criteria:',
        '- Completion that starts after the ready read waits for the dispatch claim write.',
        '- The final provider state remains human review.',
        '- The agent is released with the terminal completion reason.',
      ].join('\n')
      const mount = new CompletionObservedMount({
        [githubPath]: githubContent,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
      }, githubWrite)
      let resolveClaimStarted!: () => void
      const claimStarted = new Promise<void>((resolve) => {
        resolveClaimStarted = resolve
      })
      const claimRelease = new Promise<void>((resolve) => {
        releaseClaim = resolve
      })
      const statusCalls: string[] = []
      let terminalStarted = false
      const output = buffer()
      const errors = buffer()
      const setMountedStatus = (status: 'in-progress' | 'human-review'): void => {
        const existing = mount.files.get(githubPath)
        const content = existing?.content as ReturnType<typeof githubIssueFile>
        const labels = content.payload.labels.filter((label) =>
          label.name !== 'factory:in-progress' && label.name !== 'factory:human-review')
        labels.push({ name: `factory:${status}` })
        mount.files.set(githubPath, {
          ...existing,
          content: { ...content, payload: { ...content.payload, labels } },
        })
      }
      const githubWriteback: GithubWriteback = {
        getIssueStatus: async () => undefined,
        postComment: async () => undefined,
        setStatus: async (_issue, status) => {
          statusCalls.push(`start:${status}`)
          if (status === 'in-progress') {
            resolveClaimStarted()
            if (!fleet.implementerName) throw new Error('implementer was not spawned before the dispatch claim')
            fleet.exitEmitted = true
            fleet.emitAgentExit(fleet.implementerName, 'exited')
            await claimRelease
          } else if (status === 'human-review') {
            terminalStarted = true
          }
          setMountedStatus(status)
          statusCalls.push(`applied:${status}`)
          return 'applied'
        },
        closeIssue: async () => 'acknowledged',
      }

      const run = runFleetCli([
        'dispatch',
        '48',
        '--backend',
        'relay',
        '--config',
        configPath,
      ], {
        fleet,
        mount,
        createFactory: (factoryConfig, ports) => createFactory(factoryConfig, {
          ...ports,
          githubWriteback,
        }),
        stdout: output,
        stderr: errors,
        probePrGhRunner: async () => ({ stdout: '[]' }),
      })

      try {
        await withDeadline(claimStarted, 1_000, 'dispatch claim did not start')
      } catch (error) {
        throw new Error(JSON.stringify({
          error: error instanceof Error ? error.message : String(error),
          output: output.text(),
          errors: errors.text(),
          statusCalls,
          spawns: fleet.spawns,
          releases: fleet.releases,
        }))
      }
      await withDeadline(completionRead, 1_000, 'completion did not reach its issue read')
      await new Promise<void>((resolve) => setTimeout(resolve, 10))
      const terminalStartedBeforeClaimReleased = terminalStarted
      releaseClaim()
      const code = await run

      expect(fleet.exitEmitted).toBe(true)
      expect(mount.completionReadSeen).toBe(true)
      expect(terminalStartedBeforeClaimReleased).toBe(false)
      expect(statusCalls).toEqual([
        'start:in-progress',
        'applied:in-progress',
        'start:human-review',
        'applied:human-review',
      ])
      expect(code).toBe(0)
      expect((mount.files.get(githubPath)?.content as ReturnType<typeof githubIssueFile>).payload.labels)
        .toContainEqual({ name: 'factory:human-review' })
      expect(fleet.releases.map((release) => release.reason)).toContain('issue-human-review')
    } finally {
      releaseClaim?.()
      await rm(root, { recursive: true, force: true })
    }
  })

  it('still aborts when a third party parks the issue and this dispatch did not', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-foreign-park-'))
    try {
      const configPath = await writeConfig(root, {
        loop: {
          heartbeatPath: join(root, 'heartbeat.json'),
          registryPath: join(root, 'registry.json'),
          heartbeatStaleMs: 10_000,
        },
      })
      const githubWrite: GithubConnectionWrite = {
        publishPullRequest: async (input) => ({
          repo: input.repo,
          number: 78,
          url: 'https://github.com/AgentWorkforce/pear/pull/78',
          headRef: input.headRef ?? 'unexpected-local-head',
        }),
        closePullRequest: async () => undefined,
      }
      // No agent ever exits here, so this record's lifecycle never advances
      // past `dispatching` — the park below is somebody else's.
      class ForeignParkMountClient extends FakeMountClient {
        parked = false
        override async readFile(path: string): Promise<{ content: unknown; revision?: string }> {
          if (path === issuePath && this.armedForPark && !this.parked) {
            this.parked = true
            const existing = this.files.get(issuePath)
            const content = existing?.content as { payload: Record<string, unknown> }
            this.files.set(issuePath, {
              ...existing,
              content: { ...content, payload: { ...content.payload, stateId: TEST_STATE_IDS.humanReview } },
            })
          }
          return super.readFile(path)
        }

        armedForPark = false
      }
      const mount = new ForeignParkMountClient({
        [issuePath]: issueFile,
        '/github/repos/AgentWorkforce/pear/meta.json': { default_branch: 'main' },
      }, githubWrite)
      const fleet = new FakeFleetClient()
      const spawn = fleet.spawn.bind(fleet)
      fleet.spawn = async (input: SpawnInput): Promise<SpawnResult> => {
        const result = await spawn(input)
        mount.armedForPark = true
        return result
      }

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
        stdout: buffer(),
        stderr: buffer(),
        probePrGhRunner: async () => ({ stdout: '[]' }),
      })

      // The guard must still fire for a change this dispatch did not make.
      // If this ever returns 0, the fix has stopped distinguishing writers and
      // has simply deleted the protection.
      expect(code).not.toBe(0)
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
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      const configPath = await writeConfig(root, {
        repos: { org: 'AgentWorkforce', names: ['pear'] },
        loop: { heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
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
        versionInfo: async () => currentVersionInfo,
        localClonePathOptions: { cwd: '/not/a/checkout', git, validateConfiguredCheckouts: true },
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toEqual({
        ...factoryStatus,
        ...currentVersionInfo,
        heldAgents: [],
        eventListener: {
          state: 'not-listening',
          reason: 'heartbeat missing',
        },
      })
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

  it('uses the GitHub API fallback after an empty projection and reports the existing health signals', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-fallback-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        loop: { heartbeatPath: join(root, 'missing-heartbeat.json'), heartbeatStaleMs: 10_000 },
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubRead = fakeGithubConnectionRead(async (repo, number) =>
        repo === 'AgentWorkforce/pear' && number === 222
          ? githubIssueFound('pear', number)
          : githubIssueNotFound(),
      )
      const mount = Object.assign(new FakeMountClient(), {
        githubRead,
        getLocalMountHealth: () => ({
          degraded: true,
          reason: 'last reconcile is stale',
          localDir: join(root, '.integrations'),
        }),
      })
      const output = buffer()

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issue: { key: '222' },
        routes: [{ repo: 'AgentWorkforce/pear' }],
        issueResolution: {
          source: 'github-api-fallback',
          repo: 'AgentWorkforce/pear',
          projection: {
            outcome: 'no-match',
            localMountDegraded: true,
            localMountDegradedReason: 'last reconcile is stale',
            eventListener: { state: 'not-listening' },
          },
        },
      })
      expect(githubRead.getIssue).toHaveBeenCalledWith('AgentWorkforce/pear', 222)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('keeps a populated Relayfile projection preferred and reports that source', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-projection-source-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const issuePath = '/github/repos/AgentWorkforce__pear/issues/by-id/222.json'
      const githubRead = fakeGithubConnectionRead(async () => {
        throw new Error('GitHub API must not run when the projection has a match')
      })
      const mount = Object.assign(new FakeMountClient({
        [issuePath]: githubIssueFile('pear', 222),
      }), { githubRead })
      const output = buffer()

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issue: { key: '222', path: issuePath },
        issueResolution: {
          source: 'relayfile-projection',
          projection: { outcome: 'matched' },
        },
      })
      expect(githubRead.getIssue).not.toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not manufacture a match when GitHub authoritatively has no issue', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-not-found-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubRead = fakeGithubConnectionRead(async () => githubIssueNotFound())
      const errors = buffer()

      const code = await runFleetCli(['triage', '999999', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('found 0 matches in the projection and GitHub API')
      expect(githubRead.getIssue).toHaveBeenCalledWith('AgentWorkforce/pear', 999999)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('reports that GitHub could not determine existence instead of a confident 0 matches', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-indeterminate-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubRead = fakeGithubConnectionRead(async () => githubIssueIndeterminate())
      const errors = buffer()

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('could not determine whether the issue exists')
      expect(errors.text()).not.toContain('found 0 matches')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('refuses to dispatch to a found match when another configured repo could not be checked (no silent misroute)', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-unconfirmed-unique-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: {
            factory: 'AgentWorkforce/factory',
            cloud: 'AgentWorkforce/cloud',
          },
          clonePaths: {
            'AgentWorkforce/factory': '/work/factory',
            'AgentWorkforce/cloud': '/work/cloud',
          },
        },
      })
      // A public repo (factory) answers with a match; a repo it cannot see
      // (cloud) is indeterminate. A same-numbered issue could exist there
      // too — dispatching to the found match alone would be a silent misroute.
      const githubRead = fakeGithubConnectionRead(async (repo, number) =>
        repo === 'AgentWorkforce/factory' ? githubIssueFound('factory', number) : githubIssueIndeterminate(),
      )
      const errors = buffer()

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('could not confirm it is unique')
      expect(errors.text()).toContain('AgentWorkforce/factory')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('re-reads an API fallback issue during dry-run dispatch and records the source', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-dispatch-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const content = githubIssueFile('pear', 222)
      content.payload.body = [
        'Restore dispatch through the GitHub API fallback without changing routing.',
        '',
        'Acceptance criteria:',
        '- Projection misses use the workspace connection.',
        '- Projection hits remain preferred.',
        '- Run the focused CLI regression checks.',
      ].join('\n')
      const githubRead = fakeGithubConnectionRead(async (_repo, number) =>
        githubIssueFound('pear', number, content),
      )
      const output = buffer()

      const code = await runFleetCli(['dispatch', '222', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issue: { key: '222' },
        issueResolution: { source: 'github-api-fallback' },
        comments: [expect.stringContaining('Issue resolution: github-api-fallback')],
      })
      expect(githubRead.getIssue).toHaveBeenCalledTimes(2)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not let the API fallback bypass the existing GitHub safety label gate', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-api-safety-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        safety: { requireLabel: 'factory', requireTitlePrefix: '[factory]', requireTeamKey: 'AR' },
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const unsafe = githubIssueFile('pear', 222)
      unsafe.payload.labels = [{ name: 'pear' }]
      unsafe.payload.title = 'Missing both configured safety markers'
      const githubRead = fakeGithubConnectionRead(async (_repo, number) =>
        githubIssueFound('pear', number, unsafe),
      )
      const errors = buffer()
      const fleet = new FakeFleetClient()

      const code = await runFleetCli(['dispatch', '222', '--dry-run', '--config', configPath], {
        fleet,
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('not factory-e2e scope')
      expect(fleet.spawns).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('treats a healthy projection miss as authoritative and does not call the fallback', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-healthy-miss-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        loop: { heartbeatPath, heartbeatStaleMs: 10_000 },
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
      })
      const githubRead = fakeGithubConnectionRead(async () => githubIssueFound('pear', 222))
      const errors = buffer()
      const now = Date.now()
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAt: new Date(now).toISOString(),
        updatedAtMs: now,
        eventListener: { state: 'subscribed' },
      }))

      const code = await runFleetCli(['triage', '222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: false }),
        }),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(errors.text()).toContain('found 0 matches in the healthy Relayfile projection')
      expect(errors.text()).toContain('fallback was not used')
      expect(githubRead.getIssue).not.toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('uses a configured repo-qualified reference to make one fallback lookup', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-qualified-fallback-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          org: 'AgentWorkforce',
          byLabel: {
            factory: 'AgentWorkforce/factory',
            workforce: 'AgentWorkforce/workforce',
          },
          clonePaths: {
            'AgentWorkforce/factory': '/work/factory',
            'AgentWorkforce/workforce': '/work/workforce',
          },
        },
      })
      const githubRead = fakeGithubConnectionRead(async (repo, number) =>
        repo === 'AgentWorkforce/factory' ? githubIssueFound('factory', number) : githubIssueNotFound(),
      )
      const output = buffer()

      const code = await runFleetCli(['triage', 'factory#222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issueResolution: { source: 'github-api-fallback', repo: 'AgentWorkforce/factory' },
      })
      expect(githubRead.getIssue).toHaveBeenCalledTimes(1)
      expect(githubRead.getIssue).toHaveBeenCalledWith('AgentWorkforce/factory', 222)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('validates a qualified selector against every configured route, not just repos.default', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-qualified-non-default-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          default: 'AgentWorkforce/factory',
          byLabel: {
            factory: 'AgentWorkforce/factory',
            cloud: 'AgentWorkforce/cloud',
          },
          clonePaths: {
            'AgentWorkforce/factory': '/work/factory',
            'AgentWorkforce/cloud': '/work/cloud',
          },
        },
      })
      const githubRead = fakeGithubConnectionRead(async (repo, number) =>
        repo === 'AgentWorkforce/cloud' ? githubIssueFound('cloud', number) : githubIssueNotFound(),
      )
      const output = buffer()

      const code = await runFleetCli(['triage', 'cloud#222', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issueResolution: { source: 'github-api-fallback', repo: 'AgentWorkforce/cloud' },
      })
      expect(githubRead.getIssue).toHaveBeenCalledWith('AgentWorkforce/cloud', 222)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('validates a qualified label selector routed outside repos.org', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-github-qualified-cross-owner-'))
    try {
      const configPath = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          org: 'AgentWorkforce',
          byLabel: {
            // Routed to a different owner than repos.org — the org expansion
            // alone (`${org}/${requested}`) cannot resolve this label.
            partner: 'OtherOrg/partner-repo',
          },
          clonePaths: {
            'OtherOrg/partner-repo': '/work/partner-repo',
          },
        },
      })
      const githubRead = fakeGithubConnectionRead(async (repo, number) =>
        repo === 'OtherOrg/partner-repo'
          ? {
              outcome: 'found',
              issue: {
                repo: 'OtherOrg/partner-repo',
                number,
                path: `/github/repos/OtherOrg__partner-repo/issues/by-id/${number}.json`,
                content: githubIssueFile('partner-repo', number, 'OtherOrg'),
              },
            }
          : githubIssueNotFound(),
      )
      const output = buffer()

      const code = await runFleetCli(['triage', 'partner#5', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), {
          githubRead,
          getLocalMountHealth: () => ({ degraded: true, reason: 'projection reconcile is stale' }),
        }),
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        issueResolution: { source: 'github-api-fallback', repo: 'OtherOrg/partner-repo' },
      })
      expect(githubRead.getIssue).toHaveBeenCalledWith('OtherOrg/partner-repo', 5)
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
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      const configPath = await writeConfig(root, { loop: { heartbeatPath, registryPath, heartbeatStaleMs: 10_000 } })
      const heldSinceAtMs = Date.now() - 5_000
      await writeFile(registryPath, JSON.stringify({
        pid: 4242,
        updatedAt: new Date().toISOString(),
        updatedAtMs: Date.now(),
        agents: [{
          name: 'ar-252-impl-factory',
          role: 'implementer',
          issue: { key: '252', uuid: 'uuid-252', path: '/linear/issues/252.json' },
          pids: [],
          heldSinceAtMs,
          holdDeadlineAtMs: heldSinceAtMs + 60_000,
          waitingForTerminalState: 'human-review',
          lifecyclePhase: 'running',
        }],
      }))
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
      const assertReady = vi.fn(async () => {})

      const code = await runFleetCli([
        'status',
        '--config',
        configPath,
      ], {
        stateStoreFactory: () => testDocumentStateStore({ backend: 'test-durable', assertReady }),
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        versionInfo: async () => staleVersionInfo,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(assertReady).toHaveBeenCalledTimes(1)
      expect(JSON.parse(output.text())).toMatchObject({
        ...factoryStatus,
        ...staleVersionInfo,
        stateStore: { backend: 'test-durable' },
        heldAgents: [{
          name: 'ar-252-impl-factory',
          issue: { key: '252' },
          lifecyclePhase: 'running',
          waitingForTerminalState: 'human-review',
          pastDeadline: false,
        }],
        eventListener: {
          state: 'not-listening',
          reason: 'heartbeat missing',
        },
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not eagerly read file state for a status command', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-file-state-compat-'))
    try {
      const registryPath = join(root, 'registry.json')
      const configPath = await writeConfig(root, { loop: { registryPath } })
      await writeFile(join(root, 'github-issue-comment-watches.json'), 'not json')
      const output = buffer()
      const factory = {
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
      } as unknown as Factory

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        inFlight: [],
        queued: [],
        counters: {},
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('surfaces degraded readiness and an open fleet circuit from the live daemon heartbeat', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-reconcile-status-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { heartbeatPath, heartbeatStaleMs: 10_000 } })
      const now = Date.now()
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAt: new Date(now).toISOString(),
        updatedAtMs: now,
        eventListener: { state: 'subscribed' },
        readinessReconcile: {
          state: 'degraded',
          consecutiveFailures: 3,
          failureThreshold: 3,
          lastFailureAtMs: now - 1_000,
          lastError: 'discovery sweep lease expired',
        },
        fleetControlPlane: {
          state: 'open',
          consecutiveFailures: 2,
          timeoutMs: 5_000,
          failureThreshold: 2,
          resetTimeoutMs: 60_000,
          lastFailureAtMs: now - 500,
          retryAtMs: now + 59_500,
          lastError: 'TimeoutError (FACTORY_FLEET_CONTROL_TIMEOUT)',
        },
        fleetConnect: {
          state: 'failed',
          attempts: 2,
          lastAttemptAtMs: now - 2_000,
          lastFailureAtMs: now - 400,
          lastError: 'RelayEventStreamDisconnected',
        },
      }))
      const output = buffer()
      const factory = {
        status: vi.fn(() => ({
          inFlight: [],
          queued: [],
          counters: {},
          readinessReconcile: {
            state: 'not-running' as const,
            consecutiveFailures: 0,
            failureThreshold: 3,
          },
          fleetControlPlane: {
            state: 'closed' as const,
            consecutiveFailures: 0,
            timeoutMs: 5_000,
            failureThreshold: 2,
            resetTimeoutMs: 60_000,
          },
          fleetConnect: {
            state: 'connected' as const,
            attempts: 1,
            firstEventAtMs: now - 5_000,
          },
        })),
      } as unknown as Factory

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        readinessReconcile: {
          state: 'degraded',
          consecutiveFailures: 3,
          failureThreshold: 3,
          lastError: 'discovery sweep lease expired',
        },
        fleetControlPlane: {
          state: 'open',
          consecutiveFailures: 2,
          lastError: 'TimeoutError (FACTORY_FLEET_CONTROL_TIMEOUT)',
        },
        fleetConnect: {
          state: 'failed',
          attempts: 2,
          lastError: 'RelayEventStreamDisconnected',
        },
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not report a fresh local closed circuit for a live older daemon heartbeat', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-legacy-circuit-status-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { heartbeatPath, heartbeatStaleMs: 10_000 } })
      const now = Date.now()
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAt: new Date(now).toISOString(),
        updatedAtMs: now,
        eventListener: { state: 'subscribed' },
      }))
      const output = buffer()
      const factory = {
        status: vi.fn(() => ({
          inFlight: [],
          queued: [],
          counters: {},
          fleetControlPlane: {
            state: 'closed' as const,
            consecutiveFailures: 0,
            timeoutMs: 5_000,
            failureThreshold: 2,
            resetTimeoutMs: 60_000,
          },
          fleetConnect: {
            state: 'connected' as const,
            attempts: 1,
          },
        })),
      } as unknown as Factory

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      const status = JSON.parse(output.text())
      expect(status).not.toHaveProperty('fleetControlPlane')
      expect(status).not.toHaveProperty('fleetConnect')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('lists registry-backed in-flight issues, agents, and degraded claims in factory status', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-registry-status-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const registryPath = join(root, 'registry.json')
      const configPath = await writeConfig(root, {
        loop: { heartbeatPath, registryPath, heartbeatStaleMs: 10_000 },
      })
      const output = buffer()
      const factory = {
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
      } as unknown as Factory
      await writeFile(registryPath, JSON.stringify({
        pid: 4242,
        heartbeatPath,
        updatedAt: '2026-08-14T13:30:00.000Z',
        updatedAtMs: Date.parse('2026-08-14T13:30:00.000Z'),
        agents: [
          {
            name: 'ar-242-impl-factory',
            role: 'implementer',
            issue: { uuid: 'AgentWorkforce/factory#242', key: '242', path: '/github/factory/242.json' },
            sessionRef: 'session-impl',
            pids: [],
            node: 'oslo-mini',
            dispatchClaim: {
              state: 'degraded',
              write: 'GitHub dispatch comment',
              attempts: 3,
              maxAttempts: 3,
              deadLettered: true,
              error: 'GitHub comment write unavailable',
              updatedAtMs: Date.parse('2026-08-14T13:29:00.000Z'),
            },
          },
          {
            name: 'ar-242-review',
            role: 'reviewer',
            issue: { uuid: 'AgentWorkforce/factory#242', key: '242', path: '/github/factory/242.json' },
            pids: [],
            dispatchClaim: {
              state: 'degraded',
              write: 'GitHub dispatch comment',
              attempts: 3,
              maxAttempts: 3,
              deadLettered: true,
              error: 'GitHub comment write unavailable',
              updatedAtMs: Date.parse('2026-08-14T13:29:00.000Z'),
            },
          },
        ],
      }))

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        inFlightDispatches: [{
          issue: { key: '242' },
          agents: [
            { name: 'ar-242-impl-factory', role: 'implementer', sessionRef: 'session-impl', node: 'oslo-mini' },
            { name: 'ar-242-review', role: 'reviewer' },
          ],
          claim: {
            state: 'degraded',
            write: 'GitHub dispatch comment',
            attempts: 3,
            deadLettered: true,
          },
        }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('surfaces a stale registered workspace mirror in factory status', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-stale-status-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { heartbeatPath, heartbeatStaleMs: 10_000 } })
      const output = buffer()
      const mirror = join(root, 'chief', '.integrations')
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const mount = Object.assign(new FakeMountClient(), {
        getLocalMountHealth: () => ({
          degraded: true,
          reason: 'last reconcile 5m ago',
          localDir: mirror,
        }),
      })
      const now = Date.now()
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAt: new Date(now).toISOString(),
        updatedAtMs: now,
        eventListener: { state: 'subscribed' },
      }))

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        eventListener: {
          state: 'subscribed',
        },
        localMountDegraded: true,
        localMountDegradedReason: 'last reconcile 5m ago',
        localMountRoot: mirror,
        localMountEventFeed: {
          state: 'degraded',
          livenessSignal: '.integrations/.relay/state.json',
          reason: 'last reconcile 5m ago',
          root: mirror,
        },
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('distinguishes a healthy quiet mount event feed from a daemon that is not listening', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-event-status-'))
    try {
      const heartbeatPath = join(root, 'heartbeat.json')
      const configPath = await writeConfig(root, { loop: { heartbeatPath, heartbeatStaleMs: 10_000 } })
      const output = buffer()
      const mirror = join(root, 'chief', '.integrations')
      const factory = {
        start: vi.fn(),
        stop: vi.fn(),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const mount = Object.assign(new FakeMountClient(), {
        getLocalMountHealth: () => ({ degraded: false, localDir: mirror }),
      })
      const now = Date.now()
      await writeFile(heartbeatPath, JSON.stringify({
        pid: process.pid,
        status: 'running',
        iteration: 0,
        maxIterations: 0,
        updatedAt: new Date(now).toISOString(),
        updatedAtMs: now,
        eventListener: { state: 'subscribed' },
      }))

      const code = await runFleetCli(['status', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: () => factory,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({
        eventListener: { state: 'subscribed' },
        localMountEventFeed: {
          state: 'healthy',
          livenessSignal: '.integrations/.relay/state.json',
          root: mirror,
        },
      })
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
        stdout: output,
        stderr: errors,
      })

      expect(code).toBe(0)
      expect(integrations.getStatus).toHaveBeenCalledWith('github')
      expect(mountCalls).toEqual([process.cwd()])
      expect(errors.text()).toBe('')
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
      const mount = mountWithIntegrationConnections({
        '/github/repos/AgentWorkforce__pear/pulls/by-id/42.json': {
          payload: {
            number: 42,
            state: 'OPEN',
            headRefName: 'factory-e2e/ar-77-probe',
            title: '[factory-e2e] AR-77 probe',
            body: 'Closes AR-77',
          },
        },
      }, integrations)
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

  it('scope-checks GitHub lifecycle drafts in the ordinary CLI cloud mount', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-cli-github-draft-scope-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const outOfScopeIssue = githubIssueFile('pear', 222)
      outOfScopeIssue.payload.labels = [{ name: 'bug' }]
      const closedIssue = githubIssueFile('pear', 223)
      closedIssue.payload.state = 'closed'
      const integrations = fakeIntegrationConnections(async () => ({ ready: true, state: 'ready' }))
      const mount = mountWithIntegrationConnections({
        '/github/repos/AgentWorkforce/pear/issues/by-id/221.json': githubIssueFile('pear', 221),
        '/github/repos/AgentWorkforce/pear/issues/by-id/222.json': outOfScopeIssue,
        '/github/repos/AgentWorkforce/pear/issues/by-id/223.json': closedIssue,
      }, integrations)
      let predicate: (
        path: string,
        content: unknown,
        opts?: { guarded?: boolean },
      ) => boolean | Promise<boolean> = () => false
      const cloudMountFromConfig = vi.fn(async (opts) => {
        predicate = opts?.isAllowedDraft ?? predicate
        return mount
      })
      const factory = {
        status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
      } as unknown as Factory

      const code = await runFleetCli(['status', '--config', configPath], {
        cloudMountFromConfig,
        fleet: new FakeFleetClient(),
        createFactory: () => factory,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { state: 'closed' },
        { guarded: true },
      )).resolves.toBe(true)
      // The status claim is a complete-label-set PATCH on the issue itself:
      // Relayfile's GitHub adapter routes no label resource, so if this guard
      // rejects the shape the claim never reaches the provider.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['factory', 'bug', 'factory:in-progress'] },
        { guarded: true },
      )).resolves.toBe(true)
      // One write must not assert two contradictory Factory claims, in any casing.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['factory', 'factory:in-progress', 'Factory:Human-Review'] },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['factory', '  '] },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: 'factory:in-progress' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['factory'], title: 'rewritten' },
        { guarded: true },
      )).resolves.toBe(false)
      // A complete-set PATCH can drop labels too. The safety opt-in is what
      // made this issue eligible, so a set that omits it must not be admitted
      // — otherwise the guard would allow silently un-scoping the issue.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: [] },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['bug', 'factory:in-progress'] },
        { guarded: true },
      )).resolves.toBe(false)
      // Casing is the provider's for labels Factory did not author.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['Factory', 'bug'] },
        { guarded: true },
      )).resolves.toBe(true)
      const commentBody = 'Factory dispatch'
      await expect(predicate(
        `/github/repos/AgentWorkforce/pear/issues/221/comments/${factoryGithubIssueCommentDraftName(commentBody)}`,
        { body: commentBody },
        { guarded: true },
      )).resolves.toBe(true)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221/comments/factory-abcdef012345abcdef012345.json',
        { body: commentBody },
        { guarded: true },
      )).resolves.toBe(false)
      // Both of these used to be admitted, and that is #431. Relayfile's
      // GitHub adapter routes no label resource, so neither path could ever
      // reach GitHub — it refused them with `Unsupported GitHub writeback
      // path`. Vouching for them here made every local layer report a healthy
      // write while the lifecycle label was silently never applied, and
      // several lanes read the resulting label-less issues as "dispatch never
      // started". Nothing authors either path since #434; the guard now fails
      // closed on them like any other unroutable shape.
      //
      // This assertion is inverted from allow to deny on purpose. It is the
      // safe direction — the routed expression of a label change is the
      // complete-set `/issues/{n}.json` PATCH asserted above, which is
      // unaffected.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/labels/factory-11111111-1111-4111-8111-111111111111.json',
        {
          name: 'factory:in-progress',
          color: '1d76db',
          description: 'Factory agents are working on this issue.',
        },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221/labels/factory-22222222-2222-4222-8222-222222222222.json',
        { operation: 'add', labels: ['factory:in-progress'] },
        { guarded: true },
      )).resolves.toBe(false)
      // Still rejected, but for a sharper reason than before: the guard now
      // admits a complete-label-set PATCH (the per-label path it used to force
      // is unroutable), and this set drops the `factory` safety opt-in that
      // made the issue eligible. `setStatus` preserves every non-Factory
      // label, so it never authors a set like this.
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221.json',
        { labels: ['factory:in-progress'] },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221/labels/factory-33333333-3333-4333-8333-333333333333.json',
        { operation: 'remove', label: 'bug' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221/comments/operator.json',
        { body: 'unscoped draft' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/221/comments/factory-abcdef012345.json',
        { body: 'wrong-length digest' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/222.json',
        { state: 'closed' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/AgentWorkforce/pear/issues/223.json',
        { state: 'closed' },
        { guarded: true },
      )).resolves.toBe(false)
      await expect(predicate(
        '/github/repos/OtherOrg/other/issues/221.json',
        { state: 'closed' },
        { guarded: true },
      )).resolves.toBe(false)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
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

  it('refreshes one stale registered workspace mirror once, regardless of routed clone count', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-stale-mounts-'))
    const previousCwd = process.cwd()
    try {
      const clonePaths = [join(root, 'pear'), join(root, 'relay')]
      const mirrorDir = join(root, 'chief', '.integrations')
      await Promise.all(clonePaths.map((clonePath) => mkdir(clonePath)))
      const configPath = await writeConfig(root, {
        repos: {
          ...config.repos,
          clonePaths: {
            'AgentWorkforce/pear': clonePaths[0],
            'AgentWorkforce/relay': clonePaths[1],
          },
        },
      })
      process.chdir(root)

      const writeMountState = async (startDir: string, lastReconcileAt: string): Promise<void> => {
        const stateDir = join(startDir, '.integrations', '.relay')
        await mkdir(stateDir, { recursive: true })
        await writeFile(join(stateDir, 'state.json'), JSON.stringify({
          workspaceId: config.workspaceId,
          lastReconcileAt,
          pid: process.pid,
        }))
      }
      const ensureLocalMount = async (
        workspaceId: string,
        startDir: string,
        options: LocalMountOptions = {},
      ): Promise<void> => runLocalMountPreflight(workspaceId, startDir, {
        ...options,
        stateWaitTimeoutMs: 100,
        stateWaitPollMs: 1,
        startMount: async () => writeMountState(startDir, new Date().toISOString()),
      })
      const runStart = async (debug: boolean) => {
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
        const errors = buffer()
        const mount = Object.assign(new FakeMountClient(), {
          getLocalMountRoot: () => mirrorDir,
        })
        const code = await runFleetCli(['start', '--config', configPath], {
          fleet: new FakeFleetClient(),
          mount,
          createFactory: vi.fn(() => factory),
          ensureLocalMount,
          waitForStopSignal: vi.fn(async () => {
            await vi.waitFor(() => {
              expect(errors.text()).toContain('[factory] refreshed 1 stale local mount(s)')
            })
          }),
          env: debug ? { FACTORY_LOG_LEVEL: 'debug' } : {},
          stdout: buffer(),
          stderr: errors,
        })
        expect(code, errors.text()).toBe(0)
        return errors.text()
      }

      const staleAt = Date.now()
      await writeMountState(dirname(mirrorDir), new Date(staleAt - 31 * 60 * 1000).toISOString())
      const normalOutput = await runStart(false)
      expect(normalOutput).toContain('[factory] refreshed 1 stale local mount(s) (last reconcile ~31m ago)')
      expect(normalOutput).not.toContain('local mount is stale')
      expect(normalOutput).not.toContain('[factory] debug:')
      expect(normalOutput.match(/stale local mount/gu)).toHaveLength(1)

      await writeMountState(dirname(mirrorDir), new Date(staleAt - 31 * 60 * 1000).toISOString())
      const debugOutput = await runStart(true)
      expect(debugOutput).toContain(`[factory] debug: refreshed stale local mount at ${mirrorDir}`)
      expect(debugOutput).toContain('[factory] refreshed 1 stale local mount(s)')
    } finally {
      process.chdir(previousCwd)
      await rm(root, { recursive: true, force: true })
    }
  })

  it('fails closed before Factory construction when an injected state backend is unreadable', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-durable-state-gate-'))
    try {
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const createFactorySpy = vi.fn()
      const assertReady = vi.fn(async () => {
        throw new Error('injected durable state is unreachable')
      })
      const errors = buffer()

      const code = await runFleetCli(['run-once', '--dry-run', '--config', configPath], {
        stateStoreFactory: () => testDocumentStateStore({ assertReady }),
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient(),
        createFactory: createFactorySpy as typeof createFactory,
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(assertReady).toHaveBeenCalledTimes(1)
      expect(createFactorySpy).not.toHaveBeenCalled()
      expect(errors.text()).toContain('injected durable state is unreachable')
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
      expect(ensureLocalMount).toHaveBeenCalledTimes(1)
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
      expect(ensureSdkMount).toHaveBeenCalledTimes(1)
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
      const stderr = buffer()

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
        versionInfo: async ({ includeRegistry } = {}) => includeRegistry
          ? staleVersionInfo
          : {
              version: staleVersionInfo.version,
              installedAt: staleVersionInfo.installedAt,
            },
        stdout: buffer(),
        stderr,
      })

      expect(code).toBe(0)
      expect(ensureLocalMount).toHaveBeenCalledWith('factory-cli-test', process.cwd(), {
        acceptableWorkspaceIds: undefined,
      })
      expect(ensureLocalMount).toHaveBeenCalledTimes(1)
      expect(createFactory).toHaveBeenCalledTimes(1)
      expect(createFactory.mock.calls[0]?.[1].stateStore).toBeInstanceOf(FileStateStore)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
      expect(factory.runLoop).not.toHaveBeenCalled()
      expect(waitForStopSignal).toHaveBeenCalledTimes(1)
      expect(factory.stop).toHaveBeenCalledTimes(1)
      expect(stderr.text()).toContain(
        '[factory] daemon starting {"version":"0.1.20","installedAt":"2026-07-17T12:00:00.000Z"}',
      )
      expect(stderr.text()).toContain(
        '[factory] version drift detected: running 0.1.20; published latest 0.1.58',
      )
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('warms one registered workspace mirror without blocking live start for sixteen routes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-start-mount-concurrency-'))
    try {
      const clonePaths = Object.fromEntries(
        Array.from({ length: 16 }, (_, index) => [`AgentWorkforce/repo-${index}`, join(root, `repo-${index}`)]),
      )
      const configPath = await writeConfig(root, {
        repos: {
          byLabel: Object.fromEntries(Object.keys(clonePaths).map((repo) => [repo.split('/')[1], repo])),
          clonePaths,
          default: 'AgentWorkforce/repo-0',
        },
      })
      const mounted: string[] = []
      const mirrorDir = join(root, 'chief', '.integrations')
      let releaseMount!: () => void
      const mountReleased = new Promise<void>((resolve) => { releaseMount = resolve })
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
      const ensureLocalMount = vi.fn(async (_workspaceId: string, startDir: string) => {
        await mountReleased
        mounted.push(startDir)
      })
      const mount = Object.assign(new FakeMountClient(), {
        getLocalMountRoot: () => mirrorDir,
      })

      await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: vi.fn(() => factory),
        ensureLocalMount,
        waitForStopSignal: vi.fn(async () => {
          releaseMount()
          await vi.waitFor(() => expect(mounted).toHaveLength(1))
        }),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(mounted).toEqual([dirname(mirrorDir)])
      expect(ensureLocalMount).toHaveBeenCalledTimes(1)
      expect(mountedWhenFactoryStarted).toBeLessThan(1)
      expect(factory.start).toHaveBeenCalledWith({ mode: 'live' })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('resolves an unknown workspace mirror before enabling a live factory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-resolve-mirror-before-start-'))
    try {
      const configPath = await writeConfig(root)
      const events: string[] = []
      let registeredRoot: string | undefined
      const mount = Object.assign(new FakeMountClient(), {
        getLocalMountRoot: () => registeredRoot,
      })
      const factory = {
        start: vi.fn(async () => { events.push('factory-start') }),
        stop: vi.fn(async () => {}),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const ensureLocalMount = vi.fn(async () => {
        events.push('mount-resolved')
        registeredRoot = join(root, 'chief', '.integrations')
      })

      const code = await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: vi.fn(() => factory),
        ensureLocalMount,
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(events).toEqual(['mount-resolved', 'factory-start'])
      expect(ensureLocalMount).toHaveBeenCalledTimes(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('handles SIGTERM gracefully while an unknown workspace mirror is still resolving', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-resolve-mirror-sigterm-'))
    try {
      const configPath = await writeConfig(root)
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
      const calls: string[] = []
      let registeredRoot: string | undefined
      let releaseMount!: () => void
      const mountReleased = new Promise<void>((resolve) => { releaseMount = resolve })
      const mount = Object.assign(new FakeMountClient(), {
        getLocalMountRoot: () => registeredRoot,
      })
      const factory = {
        start: vi.fn(async () => {}),
        stop: vi.fn(async () => { calls.push('stop') }),
        runLoop: vi.fn(async () => []),
        runOnce: vi.fn(),
        status: vi.fn(),
        triageIssue: vi.fn(),
        dispatch: vi.fn(),
        on: vi.fn(),
        dispose: vi.fn(),
      } as unknown as Factory
      const ensureLocalMount = vi.fn(async () => {
        await mountReleased
        registeredRoot = join(root, 'chief', '.integrations')
      })

      const run = runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: vi.fn(() => factory),
        ensureLocalMount,
        waitForStopSignal: vi.fn(async () => undefined),
        stopSignalProcessLike: processLike as unknown as Pick<NodeJS.Process, 'once' | 'off'>,
        flushDaemonOutput: async () => { calls.push('flush') },
        stdout: buffer(),
        stderr: buffer(),
      })

      await vi.waitFor(() => {
        expect(ensureLocalMount).toHaveBeenCalledTimes(1)
        expect(listeners.has('SIGTERM')).toBe(true)
      })
      listeners.get('SIGTERM')?.()
      await vi.waitFor(() => expect(calls).toEqual(['stop', 'flush']))
      releaseMount()

      await expect(run).resolves.toBe(0)
      expect(factory.start).not.toHaveBeenCalled()
      expect(factory.stop).toHaveBeenCalledTimes(1)
      expect(listeners.size).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('does not start a live factory when an unknown workspace mirror cannot be resolved', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-unresolved-mirror-'))
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
      const errors = buffer()

      const code = await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: Object.assign(new FakeMountClient(), { getLocalMountRoot: () => undefined }),
        createFactory: vi.fn(() => factory),
        ensureLocalMount: vi.fn(async () => { throw new Error('admission refused') }),
        waitForStopSignal: vi.fn(async () => undefined),
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(1)
      expect(factory.start).not.toHaveBeenCalled()
      expect(errors.text()).toContain('aborting startup: Relayfile workspace mirror could not be resolved')
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
        reapEnvironments: async () => ({ reaped: [], retained: [] }),
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
      const heldSinceAtMs = Date.now() - 30_000
      await writeFile(registryPath, JSON.stringify({
        pid: 4242,
        updatedAt: new Date().toISOString(),
        updatedAtMs: Date.now(),
        agents: [{
          name: 'ar-252-impl-factory',
          role: 'implementer',
          issue: { key: '252', uuid: 'uuid-252', path: '/linear/issues/252.json' },
          pids: [],
          heldSinceAtMs,
          holdDeadlineAtMs: heldSinceAtMs + 10_000,
          waitingForTerminalState: 'human-review',
          lifecyclePhase: 'running',
        }],
      }))
      const output = buffer()
      const reapEnvironments = vi.fn(async () => ({ reaped: [], retained: [] }))

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
        reapEnvironments,
        stdout: output,
        stderr: buffer(),
      })

      expect(code).toBe(0)
      expect(createFleetCalls).toHaveLength(1)
      expect(cloudMountFromConfig).not.toHaveBeenCalled()
      expect(dispose).toHaveBeenCalledTimes(1)
      expect(JSON.parse(output.text())).toMatchObject({
        stale: false,
        reaped: [],
        skipped: [],
        heldAgents: [{
          name: 'ar-252-impl-factory',
          issue: { key: '252' },
          pastDeadline: true,
          waitingForTerminalState: 'human-review',
        }],
        environments: {
          applicable: false,
          reason: 'kubernetes environment provider is not configured',
          reaped: [],
          retained: [],
        },
      })
      expect(reapEnvironments).not.toHaveBeenCalled()
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

// Every case below is a PAIR. The refusal arm proves the CLI reports failure;
// the success arm proves the code still discriminates — a command that always
// exited non-zero would satisfy the refusal arm alone and carry just as little
// information as one that always exited 0.
describe('fleet CLI exit-code contract', () => {
  const stubFactory = (overrides: Partial<Record<keyof Factory, unknown>> = {}): Factory => ({
    start: vi.fn(async () => {}),
    stop: vi.fn(async () => {}),
    runLoop: vi.fn(async () => []),
    runOnce: vi.fn(async () => ({
      pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: false,
    })),
    status: vi.fn(() => ({ inFlight: [], queued: [], counters: {} })),
    triageIssue: vi.fn(async (issue: { identifier?: string }) => ({
      issue: { key: issue.identifier ?? 'AR-77', uuid: 'uuid-77', path: issuePath },
      routes: [{ repo: 'AgentWorkforce/pear', clonePath: '/work/pear' }],
    })),
    dispatch: vi.fn(),
    waitForDispatchTerminal: vi.fn(async () => {}),
    on: vi.fn(),
    dispose: vi.fn(async () => {}),
    ...overrides,
  } as unknown as Factory)

  const dispatchCli = async (root: string, dispatch: () => Promise<unknown>): Promise<{
    code: number
    output: string
    errors: string
  }> => {
    const configPath = await writeConfig(root)
    const output = buffer()
    const errors = buffer()
    const code = await runFleetCli(['dispatch', 'AR-77', '--config', configPath], {
      fleet: new FakeFleetClient(),
      mount: new FakeMountClient({ [issuePath]: issueFile }),
      createFactory: () => stubFactory({ dispatch: vi.fn(dispatch) }),
      ensureLocalMount: async () => undefined,
      stdout: output,
      stderr: errors,
    })
    return { code, output: output.text(), errors: errors.text() }
  }

  const dispatchResult = (overrides: Record<string, unknown> = {}) => ({
    issue: { key: 'AR-77', uuid: 'uuid-77', path: issuePath },
    agents: [],
    dryRun: false,
    ...overrides,
  })

  // Class 1 — the issue argument never resolved to one issue.
  it('exits non-zero when the issue key is ambiguous, and zero when it resolves', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-ambiguous-'))
    try {
      const ambiguous = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear', cloud: 'AgentWorkforce/cloud' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear', 'AgentWorkforce/cloud': '/work/cloud' },
        },
      })
      const bothRepos = new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/by-id/48.json': githubIssueFile('pear'),
        '/github/repos/AgentWorkforce/cloud/issues/by-id/48.json': githubIssueFile('cloud'),
      })
      const refusalErrors = buffer()
      const refused = await runFleetCli(['dispatch', '48', '--dry-run', '--config', ambiguous], {
        fleet: new FakeFleetClient(),
        mount: bothRepos,
        stdout: buffer(),
        stderr: refusalErrors,
      })

      expect(refused).not.toBe(0)
      expect(refusalErrors.text()).toContain('matches multiple repositories')

      // Must-not-fire: the same command against a key that resolves to exactly
      // one repository is a completed dry run and must still exit 0.
      const unambiguous = await writeConfig(root, {
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
        },
      })
      const onlyPear = new FakeMountClient({
        '/github/repos/AgentWorkforce/pear/issues/by-id/48.json': githubIssueFile('pear'),
      })
      const performed = await runFleetCli(['dispatch', '48', '--dry-run', '--config', unambiguous], {
        fleet: new FakeFleetClient(),
        mount: onlyPear,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(performed).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // Class 2 — another writer won the race for the same work unit.
  it('exits retryable on a dispatch claim race, and zero when the dispatch lands', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-race-'))
    try {
      const raced = await dispatchCli(root, async () => {
        throw new LiveDispatchStateChangedError('AR-77')
      })

      expect(raced.code).toBe(3)
      expect(raced.errors).toContain('Live state changed before writeback for AR-77')

      const landed = await dispatchCli(root, async () => dispatchResult({
        agents: [{ name: 'ar-77-impl-factory', role: 'implementer' }],
      }))

      expect(landed.code).toBe(0)
      expect(JSON.parse(landed.output)).toMatchObject({
        agents: [{ name: 'ar-77-impl-factory' }],
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // Class 3 — the cloud session cannot mint the mount's filesystem scopes.
  it('exits refused when the mount lacks its filesystem scope, and zero when the mount is healthy', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-scope-'))
    try {
      const configPath = await writeConfig(root, {
        repos: { byLabel: { pear: 'AgentWorkforce/pear' }, default: 'AgentWorkforce/pear' },
      })
      const openPr = () => new FakeMountClient({
        '/github/repos/AgentWorkforce__pear/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Scoped PR',
            body: 'Babysit me.',
            state: 'open',
            draft: false,
            head: { ref: 'scoped-pr', sha: 'scope-sha', repo: { full_name: 'AgentWorkforce/pear' } },
            base: { ref: 'main' },
          },
        },
      })
      const deniedFleet = new FakeFleetClient()
      const deniedErrors = buffer()
      const denied = await runFleetCli(['babysit', '10', '--config', configPath], {
        fleet: deniedFleet,
        mount: openPr(),
        ensureLocalMount: async () => {
          throw new MountAuthScopeError(mountAuthRemediation({ missingScope: 'fs:read', detail: 'http 403' }), {
            missingScope: 'fs:read',
          })
        },
        stdout: buffer(),
        stderr: deniedErrors,
      })

      expect(denied).toBe(2)
      expect(deniedErrors.text()).toContain('lacks the filesystem scope the mount needs')
      // The refusal message promises no agents; assert the promise was kept.
      expect(deniedFleet.spawns).toEqual([])

      const grantedFleet = new FakeFleetClient()
      const granted = await runFleetCli(['babysit', '10', '--config', configPath], {
        fleet: grantedFleet,
        mount: openPr(),
        ensureLocalMount: async () => undefined,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(granted).toBe(0)
      expect(grantedFleet.spawns).toHaveLength(1)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // Class 3b — one scope-403, two verdicts, one mount-freshness apart.
  //
  // Both arms run the REAL preflight, so the wording and the exit code are read
  // off one mechanism instead of being asserted independently. That matters
  // here: the refusal text and the warning text were once the same string, so
  // an operator could not tell a startup that aborted from one that went on to
  // spawn. The pair pins that the refusal reaches the shell as non-zero and
  // says so, while the degraded branch exits 0 and never claims to refuse.
  it('reports a scope-403 refusal as non-zero and the degraded warning as zero', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-scope-verdict-'))
    const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)
    try {
      const configPath = await writeConfig(root, {
        repos: { byLabel: { pear: 'AgentWorkforce/pear' }, default: 'AgentWorkforce/pear' },
      })
      const openPr = () => new FakeMountClient({
        '/github/repos/AgentWorkforce__pear/pulls/by-id/10.json': {
          payload: {
            number: 10,
            title: 'Scoped PR',
            body: 'Babysit me.',
            state: 'open',
            draft: false,
            head: { ref: 'scoped-pr', sha: 'scope-sha', repo: { full_name: 'AgentWorkforce/pear' } },
            base: { ref: 'main' },
          },
        },
      })

      // A mirror recording a 403 scope shortfall. `lastReconcileAt` is the only
      // thing that differs between the two arms.
      const runWithMirror = async (label: string, lastReconcileAt: string) => {
        const mirrorRoot = join(root, label)
        const fleet = new FakeFleetClient()
        const injected = buffer()
        stderrSpy.mockClear()
        const code = await runFleetCli(['babysit', '10', '--config', configPath], {
          fleet,
          mount: openPr(),
          ensureLocalMount: async (workspaceId, _startDir, options) => {
            const stateDir = join(mirrorRoot, '.integrations', '.relay')
            await mkdir(stateDir, { recursive: true })
            await writeFile(join(stateDir, 'state.json'), JSON.stringify({
              workspaceId,
              lastReconcileAt,
              pid: process.pid,
              lastError: { message: 'http 403 forbidden: missing required scope: fs:read' },
            }), 'utf8')
            return runLocalMountPreflight(workspaceId, mirrorRoot, {
              ...options,
              startMount: async () => {},
            })
          },
          stdout: buffer(),
          stderr: injected,
        })
        // Two sinks: the refusal is raised and surfaced through the CLI's
        // injected stream, while the degraded branch writes straight to
        // process.stderr. Read both so neither arm can pass by looking at the
        // wrong one.
        const emitted = injected.text()
          + stderrSpy.mock.calls.map(([chunk]) => String(chunk)).join('')
        return { code, emitted, spawns: fleet.spawns }
      }

      const stale = await runWithMirror('stale', new Date(Date.now() - 30 * 60 * 1000).toISOString())
      expect(stale.code).toBe(2)
      // Refused, said so, and kept the promise.
      expect(stale.emitted).toContain('Factory will not spawn agents against a read-denied mirror')
      expect(stale.spawns).toEqual([])

      const fresh = await runWithMirror('fresh', new Date().toISOString())
      expect(fresh.code).toBe(0)
      expect(fresh.emitted).toContain('lacks the filesystem scope')
      expect(fresh.emitted).toContain('continuing')
      // The defect this splits: a branch that spawns must not print the
      // sentence promising it will not.
      expect(fresh.emitted).not.toContain('Factory will not spawn agents against a read-denied mirror')
      expect(fresh.spawns).toHaveLength(1)
    } finally {
      // This file installs no global mock restoration, so an un-restored spy on
      // process.stderr leaks into every later test in the suite. Restore it
      // here rather than leaving the next test to fail for our reasons.
      stderrSpy.mockRestore()
      await rm(root, { recursive: true, force: true })
    }
  })

  // Class 4 — `factory.dispatch()` returned normally having placed no agents.
  // This is the path the CLI hard-coded to 0 regardless of the result.
  it.each([
    { label: 'a capacity hold', result: { hold: { kind: 'capacity' } }, code: 3 },
    { label: 'a dependency hold', result: { hold: { kind: 'dependency', blockers: ['AR-70'] } }, code: 3 },
    { label: 'a dependency cycle', result: { hold: { kind: 'dependency-cycle', cycle: ['AR-77', 'AR-77'] } }, code: 2 },
    { label: 'a queued or escalated issue', result: {}, code: 2 },
  ])('exits non-zero when dispatch placed no agents: $label', async ({ result, code }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-nodispatch-'))
    try {
      const held = await dispatchCli(root, async () => dispatchResult(result))

      expect(held.code).toBe(code)
      // The receipt is still printed — the exit code adds a signal, it does not
      // replace the diagnostic.
      expect(JSON.parse(held.output)).toMatchObject({ issue: { key: 'AR-77' } })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('exits zero when dispatch placed at least one agent', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-dispatched-'))
    try {
      const placed = await dispatchCli(root, async () => dispatchResult({
        agents: [{ name: 'ar-77-impl-factory', role: 'implementer' }],
      }))

      expect(placed.code).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('exits zero for a dry run, whose requested action is the dry run itself', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-dryrun-'))
    try {
      const configPath = await writeConfig(root)
      const code = await runFleetCli(['dispatch', 'AR-77', '--dry-run', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount: new FakeMountClient({ [issuePath]: issueFile }),
        createFactory: () => stubFactory({
          dispatch: vi.fn(async () => dispatchResult({ dryRun: true })),
        }),
        ensureLocalMount: async () => undefined,
        stdout: buffer(),
        stderr: buffer(),
      })

      expect(code).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // A scope shortfall is one failure. `factory start` reaches it by two
  // different branches depending on whether a mirror is already registered,
  // and both must report the same code — a supervising script cannot act on a
  // code that depends on which branch happened to run.
  it.each([
    { label: 'no registered mirror (inline warm-up)', registeredRoot: undefined },
    { label: 'an existing mirror (background warm-up)', registeredRoot: '/registered/.integrations' },
  ])('reports one refusal code for a scope shortfall regardless of branch: $label', async ({ registeredRoot }) => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-startup-'))
    try {
      const configPath = await writeConfig(root)
      const mount = new FakeMountClient({ [issuePath]: issueFile })
      if (registeredRoot) {
        ;(mount as unknown as { getLocalMountRoot: () => string }).getLocalMountRoot = () => registeredRoot
      }
      const errors = buffer()
      const code = await runFleetCli(['start', '--config', configPath], {
        fleet: new FakeFleetClient(),
        mount,
        createFactory: () => stubFactory(),
        ensureLocalMount: async () => {
          throw new MountAuthScopeError(mountAuthRemediation({ missingScope: 'fs:read', detail: 'http 403' }), {
            missingScope: 'fs:read',
          })
        },
        stdout: buffer(),
        stderr: errors,
      })

      expect(code).toBe(2)
      expect(errors.text()).toContain('lacks the filesystem scope the mount needs')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // A relay dispatch waits for the durable lifecycle to reach terminal. A
  // dependency park, a triage escalation, and a label refusal all return
  // BEFORE the lifecycle claim, so there is no row that can ever become
  // terminal. The wait must not become an infinite poll: a command that never
  // returns produces no exit code at all, which is strictly worse than the
  // wrong one.
  it('returns an exit code instead of waiting forever when a relay dispatch created no lifecycle', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-nolifecycle-'))
    try {
      const configPath = await writeConfig(root)
      const waited: string[] = []
      const factory = stubFactory({
        dispatch: vi.fn(async () => dispatchResult({
          hold: { kind: 'dependency', blockers: ['AR-70'] },
        })),
        // Mirrors the real method once the no-row early return is in place.
        waitForDispatchTerminal: vi.fn(async (issue: { key: string }) => {
          waited.push(issue.key)
          return undefined
        }),
      })

      const code = await withDeadline(
        runFleetCli(['dispatch', 'AR-77', '--backend', 'relay', '--config', configPath], {
          fleet: new FakeFleetClient(),
          mount: new FakeMountClient({ [issuePath]: issueFile }),
          createFactory: () => factory,
          ensureLocalMount: async () => undefined,
          env: { RELAY_WORKSPACE_KEY: 'rk_live_test' },
          stdout: buffer(),
          stderr: buffer(),
        }),
        5_000,
        'relay dispatch never returned',
      )

      expect(code).toBe(3)
      expect(waited).toEqual(['AR-77'])
      expect(factory.stop).toHaveBeenCalled()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  // Sibling command — `run-once` had the same hard-coded 0.
  it('exits non-zero when a run-once cycle records an error, and zero when it does not', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-exit-runonce-'))
    try {
      const configPath = await writeConfig(root)
      const runOnceCli = async (report: Record<string, unknown>): Promise<number> =>
        runFleetCli(['run-once', '--config', configPath], {
          fleet: new FakeFleetClient(),
          mount: new FakeMountClient({ [issuePath]: issueFile }),
          createFactory: () => stubFactory({ runOnce: vi.fn(async () => report) }),
          ensureLocalMount: async () => undefined,
          stdout: buffer(),
          stderr: buffer(),
        })

      const clean = { pulled: [], triaged: [], dispatched: [], skipped: [], dryRun: false }

      expect(await runOnceCli({ ...clean, error: { message: 'discovery failed' } })).toBe(1)
      expect(await runOnceCli({ ...clean, discoveryDeferred: 'sweep-in-flight' })).toBe(3)
      // Must-not-fire: a sweep that examined issues and dispatched none of them
      // still did what it was asked.
      expect(await runOnceCli(clean)).toBe(0)
      expect(await runOnceCli({
        ...clean,
        skipped: [{ issue: { key: 'AR-77' }, reason: 'queued or escalated' }],
      })).toBe(0)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
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

// The discriminating test for factory-cloud#55.
//
// Every other test in this file injects `fleet` or `createFleet` and therefore
// never exercises the client that actually registers. These run the real
// `RelayFleetClient` through the real `buildFleet`, stubbing only the relay
// engine, and assert on the one thing the outage turned on: whether an agent
// row was created.
describe('read-only CLI commands leave no workspace identity', () => {
  const relayEnv = { RELAY_WORKSPACE_KEY: 'rk_live_test' }

  const recordingRelay = () => {
    const registered: Array<{ name: string }> = []
    const messaging = {
      agents: {
        register: async (input: { name: string }) => {
          registered.push(input)
          return { id: 'agent-1', name: input.name, token: 'at_live_minted', status: 'online' }
        },
        list: async () => [],
        presence: async () => [],
        me: async () => ({ name: 'factory' }),
      },
      nodes: { list: async () => [] },
      events: { connect: () => {}, disconnect: async () => {}, on: () => () => {} },
      commands: { available: () => false },
    } as unknown as RelayMessaging
    return { registered, createRelay: () => ({ messaging }) }
  }

  // The control arm. `fleet roster` needs an identity to read the roster and is
  // an explicit operator command, not a pre-daemon gate, so it stays live — and
  // that makes it the proof this harness can reach registration at all. Without
  // it the assertions below could pass because the fake never gets that far.
  it('still registers for a command that is not read-only', async () => {
    const relay = recordingRelay()

    const code = await runFleetCli(['fleet', 'roster', '--backend', 'relay'], {
      env: relayEnv,
      createRelay: relay.createRelay,
      stdout: buffer(),
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(relay.registered).toEqual([{ name: 'factory' }])
  })

  it('creates no agent row for the status preflight the cloud container runs on every boot', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-readonly-status-'))
    try {
      const configPath = await writeConfig(root)
      const relay = recordingRelay()
      const output = buffer()

      const code = await runFleetCli([
        'status', '--backend', 'relay', '--config', configPath,
      ], {
        env: relayEnv,
        createRelay: relay.createRelay,
        mount: new FakeMountClient(),
        stdout: output,
        stderr: buffer(),
      })

      expect(relay.registered).toEqual([])
      expect(code).toBe(0)
      expect(JSON.parse(output.text())).toMatchObject({ inFlight: [], queued: [] })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('creates no agent row for the run-once dry run that runs ahead of a start-disabled boot', async () => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-readonly-dryrun-'))
    try {
      // GitHub-backed, matching the cloud config template. It is not cosmetic:
      // that is the branch whose sweep reads the roster for orphan recovery, so
      // this is the path where skipping the constructor's event wiring is NOT
      // enough on its own and the client's own refusal to register is what
      // holds the line.
      const configPath = await writeConfig(root, { issueSource: 'github' })
      const relay = recordingRelay()
      const mount = mountWithIntegrationConnections(
        {},
        fakeIntegrationConnections(async () => ({ ready: true, state: 'ready' })),
      )

      await runFleetCli([
        'run-once', '--dry-run', '--backend', 'relay', '--config', configPath,
      ], {
        env: relayEnv,
        createRelay: relay.createRelay,
        mount,
        stdout: buffer(),
        stderr: buffer(),
      })

      // Asserted without gating on the exit code: a sweep that fails for an
      // unrelated reason must not let a registration through unnoticed.
      expect(relay.registered).toEqual([])
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})

// The enumeration, enforced. `status` was the command we found; it was never
// the only one that constructs a Factory before the daemon exists. Asserted
// through the real CLI so a new command that reaches the same construction site
// has to state which side of the line it is on.
describe('read-only classification of every Factory-constructing command', () => {
  const classify = async (argv: string[]): Promise<boolean | undefined> => {
    const root = await mkdtemp(join(tmpdir(), 'fleet-cli-readonly-classify-'))
    try {
      let readOnly: boolean | undefined
      const configPath = await writeConfig(root, { issueSource: 'github' })
      await runFleetCli([...argv, '--config', configPath], {
        createFleet: ((options) => {
          readOnly = options?.readOnly
          // Fail the command straight after classification: this test is about
          // which client gets built, not about running the command to the end.
          throw new Error('classified')
        }) as unknown as typeof createFleet,
        mount: mountWithIntegrationConnections(
          {},
          fakeIntegrationConnections(async () => ({ ready: true, state: 'ready' })),
        ),
        stdout: buffer(),
        stderr: buffer(),
      })
      return readOnly
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  }

  it.each([
    // The two the cloud container runs before the live daemon exists.
    ['status', ['status', '--backend', 'relay']],
    ['run-once --dry-run', ['run-once', '--dry-run', '--backend', 'relay']],
    ['loop --dry-run', ['loop', '--dry-run', '--backend', 'relay']],
    ['loop-status', ['loop-status', '--backend', 'relay']],
    ['kill-loop', ['kill-loop', '--backend', 'relay']],
    ['canary', ['canary', '48', '--backend', 'relay']],
    ['triage', ['triage', '48', '--backend', 'relay']],
    ['dispatch --dry-run', ['dispatch', '48', '--dry-run', '--backend', 'relay']],
  ])('builds a read-only fleet client for %s', async (_label, argv) => {
    await expect(classify(argv)).resolves.toBe(true)
  })

  it.each([
    ['start', ['start', '--backend', 'relay']],
    ['run-once', ['run-once', '--backend', 'relay']],
    ['loop', ['loop', '--backend', 'relay']],
    ['dispatch', ['dispatch', '48', '--backend', 'relay']],
    // Skips the relay dispatch-owner branch but still spawns a real team.
    ['dispatch --backend internal', ['dispatch', '48', '--backend', 'internal']],
  ])('keeps a registering fleet client for %s', async (_label, argv) => {
    await expect(classify(argv)).resolves.toBe(false)
  })
})
