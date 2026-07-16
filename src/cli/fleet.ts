import { existsSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'

import { ensureLocalMount } from '../mount/local-mount-preflight'
import {
  FactoryConfigSchema,
  FileStateStore,
  RelayfileCloudMountClient,
  checkFactoryLoopLiveness,
  closeProbePr,
  createFactory,
  createFleet,
  ensureRelayBroker,
  defaultGhRunner,
  explicitLinkedIssueKey,
  githubIssuePathParts,
  githubWatchStatePath,
  isInFactoryScope,
  parseGithubFactoryIssue,
  parseLinearIssue,
  parseOwnedBrokerAgentExitTimeoutMs,
  parseStandaloneBabysitTarget,
  readStandalonePullRequest,
  readLinearIssueWithCanonicalFallback,
  reapFactoryOrphansOnce,
  readFactoryLoopHeartbeat,
  resolveFactoryStates,
  stateResolutionFromIds,
  standaloneBabysitterAgentName,
  renderAgentTask,
  resolveFactoryWorkspace,
  type Capability,
  type Factory,
  type FactoryConfig,
  type IterationReport,
  type FleetBackend,
  type FleetClient,
  type GhRunner,
  type FactoryStateResolution,
  type Logger,
  type MountClient,
  type ProbeCloser,
  type RelayfileCloudMountClientConfig,
  type ResolvedFactoryWorkspace,
} from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'

interface FleetCliDeps {
  fleet?: FleetClient
  /** Hermetic credential environment for the relay backend (tests). */
  env?: NodeJS.ProcessEnv
  mount?: MountClient
  createFactory?: typeof createFactory
  createFleet?: typeof createFleet
  ensureRelayBroker?: typeof ensureRelayBroker
  cloudMountFromConfig?: (config?: RelayfileCloudMountClientConfig) => Promise<MountClient>
  resolveWorkspace?: () => Promise<ResolvedFactoryWorkspace>
  resolveStates?: (mount: MountClient, config: FactoryConfig) => Promise<FactoryStateResolution>
  ensureLocalMount?: (
    workspaceId: string,
    startDir: string,
    options?: { acceptableWorkspaceIds?: readonly string[] },
  ) => Promise<void>
  waitForStopSignal?: () => Promise<number | void>
  stdout?: Pick<NodeJS.WriteStream, 'write'>
  stderr?: Pick<NodeJS.WriteStream, 'write'>
  probeCloser?: ProbeCloser
  probePrGhRunner?: GhRunner
  babysitPrGhRunner?: GhRunner
  now?: () => number
  stopSignalProcessLike?: Pick<NodeJS.Process, 'once' | 'off'>
  daemonExit?: (code: number) => void
  flushDaemonOutput?: () => Promise<void>
}

interface GlobalOptions {
  backend: FleetBackend
  config?: string
  dryRun: boolean
  agentExitTimeoutMs?: number
}

interface LoadedConfig {
  config: FactoryConfig
  fixtureFiles?: Record<string, unknown>
}

type ParsedCommand =
  | { kind: 'spawn'; input: { capability: Capability; name?: string; node?: 'self' | string; task?: string; model?: string; sessionRef?: string; cwd?: string } }
  | { kind: 'roster' }
  | { kind: 'release'; name: string; reason?: string }
  | { kind: 'factory'; action: 'run-once' | 'loop' | 'status' | 'loop-status' | 'kill-loop' | 'reap-orphans' }
  | { kind: 'factory'; action: 'start'; mode?: 'live' }
  | { kind: 'factory-canary'; issue: string }
  | { kind: 'factory-triage'; issue: string }
  | { kind: 'factory-dispatch'; issue: string }
  | { kind: 'factory-babysit'; prNumber: number; repo?: string; url?: string }
  | { kind: 'factory-close-probe'; prNumber: number; repo: string; issue: string }

export async function runFleetCli(argv: string[], deps: FleetCliDeps = {}): Promise<number> {
  const out = deps.stdout ?? process.stdout
  const err = deps.stderr ?? process.stderr
  let fleet: FleetClient | undefined

  try {
    if (argv.some(isHelpFlag)) {
      out.write(helpText())
      return 0
    }
    const { globals, args } = parseGlobalOptions(argv)
    const command = parseFleetCommand(args)

    if (command.kind === 'factory-close-probe') {
      // Manual close-probe remains strict; the daemon relaxes the title marker only after issue-synthetic classification.
      const githubWrite = deps.probeCloser
        ? undefined
        : (deps.mount ?? await (deps.cloudMountFromConfig ?? RelayfileCloudMountClient.fromConfig)({
            workspaceId: (await (deps.resolveWorkspace ?? resolveFactoryWorkspace)()).workspaceId,
            isAllowedDraft: (path, _content, opts) => isAllowedFactoryGithubDraft(path, opts),
          })).githubWrite
      const result = await (deps.probeCloser ?? closeProbePr)({
        repo: command.repo,
        prNumber: command.prNumber,
        expectedIssueKey: command.issue,
        ...(githubWrite ? { githubWrite } : {}),
        ...(deps.probePrGhRunner ? { runner: deps.probePrGhRunner } : {}),
      })
      writeJson(out, result)
      return 0
    }

    const loaded = command.kind.startsWith('factory') ? await loadConfig(globals.config) : undefined
    fleet = await buildFleet(
      globals,
      loaded,
      deps,
      command.kind === 'factory-babysit' && !globals.dryRun,
    )

    switch (command.kind) {
      case 'spawn': {
        const name = command.input.name ?? defaultAgentName(command.input.capability, deps.now?.() ?? Date.now())
        if (command.input.sessionRef) {
          writeJson(out, await fleet.resume({
            name,
            sessionRef: command.input.sessionRef,
            node: command.input.node,
            capability: command.input.capability,
          }))
          return 0
        }

        writeJson(out, await fleet.spawn({
          name,
          capability: command.input.capability,
          node: command.input.node ?? 'self',
          task: command.input.task,
          model: command.input.model,
          cwd: command.input.cwd,
        }))
        return 0
      }
      case 'roster':
        writeJson(out, await fleet.roster())
        return 0
      case 'release':
        await fleet.release(command.name, command.reason)
        writeJson(out, { released: command.name })
        return 0
      case 'factory':
      case 'factory-canary':
      case 'factory-triage':
      case 'factory-dispatch':
      case 'factory-babysit': {
        if (!loaded) throw new Error('factory command requires config')
        if (command.kind === 'factory' && command.action === 'reap-orphans') {
          writeJson(out, await reapFactoryOrphansOnce({
            heartbeatPath: loaded.config.loop.heartbeatPath,
            registryPath: loaded.config.loop.registryPath,
            staleMs: loaded.config.loop.heartbeatStaleMs,
            fleet,
          }))
          return 0
        }
        // Derive the workspace from the cloud session when it isn't pinned in
        // config. resolveFactoryWorkspace() returns the relayfile handle plus
        // the cloud UUID for the same workspace; the UUID is forwarded to the
        // mount staleness check as an accepted alias.
        let acceptableMountIds: readonly string[] | undefined
        if (!loaded.config.workspaceId) {
          const workspace = await (deps.resolveWorkspace ?? resolveFactoryWorkspace)()
          loaded.config.workspaceId = workspace.workspaceId
          if (workspace.cloudWorkspaceId) acceptableMountIds = [workspace.cloudWorkspaceId]
        }
        const workspaceId = loaded.config.workspaceId
        if (!workspaceId) throw new Error('factory command could not resolve a workspaceId')
        const mount = await buildMount(loaded, deps)
        if (command.kind === 'factory-babysit') {
          return await runStandaloneBabysitCommand(
            command,
            mount,
            fleet,
            loaded.config,
            globals,
            out,
            deps,
            workspaceId,
            acceptableMountIds,
          )
        }
        // Resolve Linear workflow states only when Linear is the issue source.
        // A GitHub-only workspace has no /linear/states catalog and uses labels
        // for lifecycle state, so it must not depend on that provider at startup.
        const stateResolution = await resolveStatesForIssueSource(mount, loaded.config, deps.resolveStates)
        const logger = streamLogger(err)
        const stateStore = new FileStateStore({
          batchSize: loaded.config.batchSize,
          watchStatePath: githubWatchStatePath(loaded.config.loop.registryPath),
        })
        const factory = (deps.createFactory ?? createFactory)(loaded.config, {
          mount,
          fleet,
          stateStore,
          stateResolution,
          probePrGhRunner: deps.probePrGhRunner ?? defaultGhRunner,
          logger,
        })
        return await runFactoryCommand(command, factory, mount, fleet, loaded.config, globals, out, deps, workspaceId, acceptableMountIds)
      }
    }
    return 1
  } catch (error) {
    err.write(`${error instanceof Error ? error.message : String(error)}\n`)
    return 1
  } finally {
    await fleet?.dispose()
  }
}

export function parseFleetCommand(args: string[]): ParsedCommand {
  const [verb, ...rest] = args
  if (!verb) {
    throw new Error(usage())
  }

  if (isFactoryAction(verb)) {
    return parseFactoryCommand(args)
  }

  if (verb === 'fleet') {
    return parseFleetSubcommand(rest)
  }

  throw new Error(`Unknown factory command: ${verb}`)
}

function parseFleetSubcommand(args: string[]): ParsedCommand {
  const [verb, ...rest] = args
  if (!verb) {
    throw new Error('factory fleet requires a command')
  }

  if (verb === 'spawn') {
    const [capability, ...flags] = rest
    if (!isCapability(capability)) {
      throw new Error('factory fleet spawn requires capability spawn:codex, spawn:claude, or workflow:run')
    }
    const parsed = parseFlags(flags)
    return {
      kind: 'spawn',
      input: {
        capability,
        name: parsed.name,
        node: parsed.node,
        task: parsed.task,
        model: parsed.model,
        sessionRef: parsed.resume,
        cwd: parsed.cwd,
      },
    }
  }

  if (verb === 'roster' || verb === 'ls') {
    return { kind: 'roster' }
  }

  if (verb === 'release') {
    const [name, ...flags] = rest
    if (!name) throw new Error('factory fleet release requires agent name')
    return { kind: 'release', name, reason: parseFlags(flags).reason }
  }

  throw new Error(`Unknown factory fleet command: ${verb}`)
}

export function parseGlobalOptions(argv: string[]): { globals: GlobalOptions; args: string[] } {
  const args: string[] = []
  const envAgentExitTimeoutMs = parseOwnedBrokerAgentExitTimeoutMs(process.env.FACTORY_AGENT_EXIT_TIMEOUT_MS)
  const globals: GlobalOptions = {
    backend: 'internal',
    dryRun: false,
    ...(envAgentExitTimeoutMs === undefined ? {} : { agentExitTimeoutMs: envAgentExitTimeoutMs }),
  }
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (arg === '--backend') {
      const backend = argv[++index]
      if (backend !== 'internal' && backend !== 'relay') throw new Error(`Invalid --backend ${backend ?? ''}`)
      globals.backend = backend
    } else if (arg === '--config') {
      globals.config = requireValue(argv, ++index, '--config')
    } else if (arg === '--agent-exit-timeout') {
      const value = requireValue(argv, ++index, '--agent-exit-timeout')
      const timeoutMs = parseOwnedBrokerAgentExitTimeoutMs(value)
      if (timeoutMs === undefined) {
        throw new Error('--agent-exit-timeout must be a positive integer number of milliseconds')
      }
      globals.agentExitTimeoutMs = timeoutMs
    } else if (arg === '--dry-run') {
      globals.dryRun = true
    } else {
      args.push(arg)
    }
  }
  return { globals, args }
}

async function runFactoryCommand(
  command: Extract<ParsedCommand, { kind: 'factory' | 'factory-canary' | 'factory-triage' | 'factory-dispatch' }>,
  factory: Factory,
  mount: MountClient,
  fleet: FleetClient,
  config: FactoryConfig,
  globals: GlobalOptions,
  out: Pick<NodeJS.WriteStream, 'write'>,
  deps: FleetCliDeps = {},
  workspaceId: string = config.workspaceId ?? '',
  acceptableMountIds?: readonly string[],
): Promise<number> {
  if (command.kind === 'factory') {
    if (command.action === 'start') {
      await (deps.ensureLocalMount ?? ensureLocalMount)(workspaceId, process.cwd(), {
        acceptableWorkspaceIds: acceptableMountIds,
      })
      await ensureClonePathMounts(deps, workspaceId, config, acceptableMountIds)
      const waiter = createStopSignalWaiter()
      let stoppedBySignal = false
      const flushAndExit = async (code: number): Promise<void> => {
        try {
          await (deps.flushDaemonOutput ?? flushProcessOutput)()
        } finally {
          const daemonExit = deps.daemonExit ?? ((exitCode: number) => process.exit(exitCode))
          daemonExit(code)
          waiter.resolve(code)
        }
      }
      const removeSignalHandlers = installFactoryStopSignalHandlers(factory, {
        exit: (code) => {
          stoppedBySignal = true
          void flushAndExit(code)
        },
        processLike: deps.stopSignalProcessLike,
      })
      try {
        await factory.start({ mode: command.mode })
        const code = await (deps.waitForStopSignal?.() ?? waiter.promise)
        return typeof code === 'number' ? code : 0
      } finally {
        removeSignalHandlers()
        if (!stoppedBySignal) {
          await factory.stop()
        }
      }
    }
    if (command.action === 'run-once') {
      await ensureClonePathMounts(deps, workspaceId, config, acceptableMountIds)
      writeJson(out, await factory.runOnce({ dryRun: globals.dryRun }))
      return 0
    }
    if (command.action === 'status') {
      writeJson(out, factory.status())
      return 0
    }
    if (command.action === 'loop-status') {
      const heartbeat = await readFactoryLoopHeartbeat(config.loop.heartbeatPath)
      writeJson(out, checkFactoryLoopLiveness(heartbeat, { staleMs: config.loop.heartbeatStaleMs }))
      return 0
    }
    if (command.action === 'kill-loop') {
      const heartbeat = await readFactoryLoopHeartbeat(config.loop.heartbeatPath)
      if (!heartbeat?.pid) {
        throw new Error(`No factory loop heartbeat at ${config.loop.heartbeatPath}`)
      }
      process.kill(heartbeat.pid, 'SIGTERM')
      writeJson(out, { killed: heartbeat.pid, signal: 'SIGTERM' })
      return 0
    }
    if (command.action === 'reap-orphans') {
      writeJson(out, await reapFactoryOrphansOnce({
        heartbeatPath: config.loop.heartbeatPath,
        registryPath: config.loop.registryPath,
        staleMs: config.loop.heartbeatStaleMs,
        fleet,
      }))
      return 0
    }

    await ensureClonePathMounts(deps, workspaceId, config, acceptableMountIds)
    const removeSignalHandlers = installFactoryStopSignalHandlers(factory, {
      processLike: deps.stopSignalProcessLike,
    })
    try {
      const reports = await factory.runLoop({ dryRun: globals.dryRun })
      writeJson(out, { reports, status: factory.status() })
    } finally {
      removeSignalHandlers()
      await factory.stop()
    }
    return 0
  }

  if (command.kind === 'factory-canary') {
    const report = await factory.runOnce({ dryRun: true })
    const result = evaluateFactoryCanary(report, command.issue)
    writeJson(out, result)
    return result.ok ? 0 : 1
  }

  const issue = await readIssueArg(mount, command.issue, config)
  const decision = await factory.triageIssue(issue)
  if (command.kind === 'factory-triage') {
    writeJson(out, decision)
    return 0
  }

  writeJson(out, await factory.dispatch(decision, { dryRun: globals.dryRun }))
  return 0
}

async function runStandaloneBabysitCommand(
  command: Extract<ParsedCommand, { kind: 'factory-babysit' }>,
  mount: MountClient,
  fleet: FleetClient,
  config: FactoryConfig,
  globals: GlobalOptions,
  out: Pick<NodeJS.WriteStream, 'write'>,
  deps: FleetCliDeps,
  workspaceId: string,
  acceptableMountIds?: readonly string[],
): Promise<number> {
  const repo = resolveStandaloneBabysitRepo(command.repo, config)
  const clonePath = standaloneBabysitClonePath(repo, config)
  const mountFn = deps.ensureLocalMount ?? ensureLocalMount
  const mountOpts = { acceptableWorkspaceIds: acceptableMountIds }
  await ensureStandaloneBabysitMount(mountFn, workspaceId, process.cwd(), mountOpts)
  if (clonePath && resolve(clonePath) !== resolve(process.cwd())) {
    await ensureStandaloneBabysitMount(mountFn, workspaceId, clonePath, mountOpts)
  }

  const pr = await readStandalonePullRequest(
    mount,
    { repo, prNumber: command.prNumber, url: command.url },
    deps.babysitPrGhRunner ?? defaultGhRunner,
  )
  const state = pr.state?.trim().toUpperCase()
  if (pr.merged || state === 'MERGED') {
    throw new Error(`factory babysit requires an open PR; ${repo} PR #${pr.number} is merged`)
  }
  if (state !== 'OPEN') {
    throw new Error(`factory babysit requires an open PR; ${repo} PR #${pr.number} state is ${state ?? 'UNKNOWN'}`)
  }
  if (pr.draft) {
    throw new Error(`factory babysit skips draft PRs; ${repo} PR #${pr.number} is a draft`)
  }
  if (pr.crossRepository) {
    throw new Error(
      `factory babysit refuses cross-repository PRs by default; ${repo} PR #${pr.number} head is ${pr.headRepo ?? 'unknown'}`,
    )
  }

  const linkedIssueKey = explicitLinkedIssueKey(pr.body)
  let issue = {
    key: `${repo}#${pr.number}`,
    title: pr.title,
    description: pr.body || '(No PR description was provided.)',
  }
  let specSource: 'pull-request' | 'linked-issue' = 'pull-request'
  if (linkedIssueKey) {
    try {
      const linked = await readIssueArg(mount, linkedIssueKey, config)
      issue = {
        key: linked.key,
        title: linked.title,
        description: linked.description,
      }
      specSource = 'linked-issue'
    } catch {
      // The PR remains independently babysittable when its referenced issue is
      // not connected to this workspace. Its own title/body become the spec.
    }
  }

  const agentName = standaloneBabysitterAgentName(repo, pr.number)
  const task = renderAgentTask({
    issue,
    route: { repo, clonePath },
    role: 'babysitter',
    config: { mergePolicy: config.mergePolicy, terminalState: config.terminalState },
    reviewerName: '',
    pr: {
      number: pr.number,
      url: pr.url,
      headRef: pr.headRef,
      headSha: pr.headSha,
      baseRef: pr.baseRef,
      headRepo: pr.headRepo,
      crossRepository: pr.crossRepository,
      maintainerCanModify: pr.maintainerCanModify,
    },
    standaloneBabysitter: { specSource },
    integrationsMountRoot: resolve(process.cwd(), '.integrations'),
  })
  const receiptBase = {
    agent: agentName,
    repo,
    prNumber: pr.number,
    url: pr.url,
    source: pr.source,
    specSource,
    ...(specSource === 'linked-issue' ? { linkedIssue: issue.key } : {}),
  }

  if (globals.dryRun) {
    writeJson(out, { status: 'dry-run', ...receiptBase })
    return 0
  }

  const roster = await fleet.roster()
  if (roster.agents.some((agent) => agent.name === agentName)) {
    writeJson(out, { status: 'already-running', ...receiptBase })
    return 0
  }

  const spawned = await fleet.spawn({
    name: agentName,
    capability: 'spawn:claude',
    node: 'self',
    repo,
    clonePath,
    task,
    model: config.models.babysitter,
    cwd: clonePath,
    invocationId: `factory-babysit:${repo}#${pr.number}`,
  })
  writeJson(out, { status: 'spawned', ...receiptBase, agent: spawned.name })
  return 0
}

async function ensureStandaloneBabysitMount(
  mountFn: NonNullable<FleetCliDeps['ensureLocalMount']>,
  workspaceId: string,
  startDir: string,
  options: { acceptableWorkspaceIds?: readonly string[] },
): Promise<void> {
  try {
    await mountFn(workspaceId, startDir, options)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    process.stderr.write(
      `[factory] warning: could not start relayfile mount for standalone babysitter at ${resolve(startDir)}; ` +
      `the agent will use the GitHub CLI fallback: ${message}\n`,
    )
  }
}

function resolveStandaloneBabysitRepo(repo: string | undefined, config: FactoryConfig): string {
  const configured = repo ?? config.repos.default
  if (!configured) {
    throw new Error('factory babysit <PR-number> requires repos.default; pass a full GitHub PR URL instead')
  }
  if (configured.includes('/')) return configured
  const exactRoute = config.repos.byLabel[configured]
  if (exactRoute) return exactRoute
  const routed = [...new Set(Object.values(config.repos.byLabel).filter((candidate) =>
    candidate === configured || candidate.endsWith(`/${configured}`),
  ))]
  if (routed.length === 1) return routed[0]!
  if (routed.length > 1) {
    throw new Error(
      `Unable to resolve repository ${configured}: matches multiple configured repos (${routed.sort().join(', ')}); ` +
      'set repos.default to owner/repo or pass a full GitHub PR URL instead',
    )
  }
  if (config.repos.org) return `${config.repos.org}/${configured}`
  throw new Error(`Unable to resolve repository ${configured} to owner/repo; pass a full GitHub PR URL instead`)
}

function standaloneBabysitClonePath(repo: string, config: FactoryConfig): string | undefined {
  const configured = Object.entries(config.clonePaths).find(([candidate]) => candidate.toLowerCase() === repo.toLowerCase())?.[1]
    ?? Object.entries(config.repos.clonePaths).find(([candidate]) => candidate.toLowerCase() === repo.toLowerCase())?.[1]
  if (configured && existsSync(configured)) return configured
  return undefined
}

/**
 * Ensures the relayfile mount is running at each configured clone path so
 * spawned agents can resolve `.integrations` relative to their working
 * directory (the checkout path). The mount daemon started at the daemon CWD
 * is not automatically accessible from a different directory, and agents need
 * these paths for integration writebacks (Slack, GitHub, etc.).
 */
async function ensureClonePathMounts(
  deps: FleetCliDeps,
  workspaceId: string,
  config: FactoryConfig,
  acceptableMountIds?: readonly string[],
): Promise<void> {
  const mountFn = deps.ensureLocalMount ?? ensureLocalMount
  const mountOpts = { acceptableWorkspaceIds: acceptableMountIds }
  const daemonCwd = resolve(process.cwd())
  for (const clonePath of new Set(Object.values(config.clonePaths ?? {}))) {
    const resolved = resolve(clonePath)
    if (resolved !== daemonCwd) {
      try {
        await mountFn(workspaceId, resolved, mountOpts)
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        process.stderr.write(`[factory] warning: could not start relayfile mount at ${resolved}: ${message}\n`)
      }
    }
  }
}

function parseFactoryCommand(args: string[]): ParsedCommand {
  const [action, issueOrPr, ...flags] = args
  if (action === 'start') {
    return { kind: 'factory', action, ...parseFactoryStartFlags([issueOrPr, ...flags]) }
  }
  if (action === 'run-once' || action === 'loop' || action === 'status' || action === 'loop-status' || action === 'kill-loop' || action === 'reap-orphans') {
    return { kind: 'factory', action }
  }
  if (action === 'canary') {
    if (!issueOrPr) throw new Error('factory canary requires an issue key or path')
    return { kind: 'factory-canary', issue: issueOrPr }
  }
  if (action === 'triage') {
    if (!issueOrPr) throw new Error('factory triage requires an issue key or path')
    return { kind: 'factory-triage', issue: issueOrPr }
  }
  if (action === 'dispatch') {
    if (!issueOrPr) throw new Error('factory dispatch requires an issue key or path')
    return { kind: 'factory-dispatch', issue: issueOrPr }
  }
  if (action === 'babysit') {
    if (flags.length > 0) throw new Error(`Unexpected argument ${flags[0]}`)
    return { kind: 'factory-babysit', ...parseStandaloneBabysitTarget(issueOrPr) }
  }
  if (action === 'close-probe') {
    const prNumber = Number(issueOrPr)
    if (!Number.isInteger(prNumber) || prNumber <= 0) throw new Error('factory close-probe requires a PR number')
    const parsed = parseFlags(flags)
    if (!parsed.repo || !parsed.issue) throw new Error('factory close-probe requires --repo <owner/repo> --issue <KEY>')
    return { kind: 'factory-close-probe', prNumber, repo: parsed.repo, issue: parsed.issue }
  }
  throw new Error(`Unknown factory action: ${action ?? ''}`)
}

// Canary: assert a known "Ready for Agent" issue is classified dispatch-ready
// by the REAL triage path against the live mount. This is the regression
// detector for sync-fidelity drift (sparse records / stub primaries) — if it
// flips to skipped, the adapter/sync contract broke. Exits non-zero with the
// offending skip reason so CI/cron can alert.
function evaluateFactoryCanary(
  report: IterationReport,
  issueArg: string,
): { ok: boolean; issue: string; status: string; reason?: string } {
  const wantKey = issueArg.startsWith('/')
    ? (issueArg.split('/').at(-1) ?? '').replace(/\.json$/u, '').split('__')[0]
    : issueArg
  const matches = (ref: { key: string }): boolean => ref.key === wantKey
  if (report.dispatched.some((d) => matches(d.issue))) {
    return { ok: true, issue: wantKey, status: 'dispatched' }
  }
  if (report.triaged.some((t) => matches(t.issue))) {
    return { ok: true, issue: wantKey, status: 'triaged' }
  }
  const skipped = report.skipped.find((s) => matches(s.issue))
  if (skipped) {
    return { ok: false, issue: wantKey, status: 'skipped', reason: skipped.reason }
  }
  if (!report.pulled.some(matches)) {
    return {
      ok: false,
      issue: wantKey,
      status: 'not-found',
      reason: 'issue was not enumerated from the mount (sync may be missing it or it is in-flight)',
    }
  }
  return {
    ok: false,
    issue: wantKey,
    status: 'unknown',
    reason: 'issue pulled but neither dispatched, triaged, nor skipped',
  }
}

function parseFactoryStartFlags(args: Array<string | undefined>): { mode: 'live' } {
  let mode: 'live' = 'live'
  const flags = args.filter((arg): arg is string => Boolean(arg))
  for (let index = 0; index < flags.length; index += 1) {
    const flag = flags[index]
    if (flag === '--mode') {
      const value = requireValue(flags, ++index, '--mode')
      if (value !== 'live') throw new Error(`Invalid factory start mode: ${value}`)
      mode = value
      continue
    }
    throw new Error(`Unknown factory start option: ${flag}`)
  }
  return { mode }
}

async function loadConfig(path?: string): Promise<LoadedConfig> {
  const configPath = path ?? resolve(process.cwd(), 'factory.config.json')
  const raw = JSON.parse(await readFile(configPath, 'utf8')) as unknown
  const record = asRecord(raw)
  return {
    config: FactoryConfigSchema.parse(record.factoryConfig ?? record),
    fixtureFiles: record.fixtureFiles ? asRecord(record.fixtureFiles) : undefined,
  }
}

async function buildFleet(
  globals: GlobalOptions,
  loaded: LoadedConfig | undefined,
  deps: FleetCliDeps,
  preserveStartedBroker = false,
): Promise<FleetClient> {
  if (deps.fleet) return deps.fleet
  if (globals.backend === 'internal' && hasExplicitFixtureFiles(loaded)) return new FakeFleetClient()

  const cwd = process.cwd()
  const connectionPath = resolveBrokerConnectionPath(cwd)

  // An injected createFleet owns fleet construction entirely (tests), so skip the
  // real broker bootstrap.
  if (deps.createFleet) {
    return deps.createFleet(
      { backend: globals.backend, cwd, connectionPath },
      { ownedBrokerAgentExitTimeoutMs: globals.agentExitTimeoutMs },
    )
  }

  // The internal backend talks to a relay broker. Reuse the one already running
  // for this workspace, or start one if none is up, so `factory <run-once|loop>`
  // works without a separately-started broker. The relay backend manages its own
  // connection and needs no local broker.
  if (globals.backend === 'internal') {
    const stderr = deps.stderr ?? process.stderr
    const logger = streamLogger(stderr)
    const { client, started, workspaceKey } = await (deps.ensureRelayBroker ?? ensureRelayBroker)({ cwd, connectionPath, logger })
    return createFleet(
      { backend: 'internal', cwd, connectionPath },
      {
        harnessClient: client,
        // A standalone babysitter is intentionally fire-and-forget. If this
        // command had to start the broker, leave it running so a long CI/review
        // cycle is not killed by InternalFleetClient's owned-broker timeout.
        ownsBroker: started && !preserveStartedBroker,
        ownedBrokerAgentExitTimeoutMs: globals.agentExitTimeoutMs,
        workspaceKey,
        logger,
      },
    )
  }

  return createFleet({ backend: globals.backend, cwd, connectionPath }, { env: deps.env })
}

function streamLogger(stream: Pick<NodeJS.WriteStream, 'write'>): Logger {
  const write = (message: string, args: unknown[]) => {
    stream.write(`${message}${formatLogArgs(args)}\n`)
  }
  return {
    debug: (message, ...args) => write(message, args),
    info: (message, ...args) => write(message, args),
    warn: (message, ...args) => write(message, args),
    error: (message, ...args) => write(message, args),
  }
}

function formatLogArgs(args: unknown[]): string {
  if (args.length === 0) return ''
  try {
    return ` ${args.map((arg) => (typeof arg === 'string' ? arg : JSON.stringify(arg))).join(' ')}`
  } catch {
    return ''
  }
}

export function resolveBrokerConnectionPath(startCwd = process.cwd()): string | undefined {
  let current = resolve(startCwd)
  for (;;) {
    const candidate = join(current, '.agentworkforce', 'relay', 'connection.json')
    if (existsSync(candidate)) {
      return candidate
    }
    const parent = dirname(current)
    if (parent === current) {
      return undefined
    }
    current = parent
  }
}

async function defaultResolveStates(mount: MountClient, config: FactoryConfig): Promise<FactoryStateResolution> {
  return resolveFactoryStates(mount, {
    states: config.linear.states,
    statesByTeam: config.linear.statesByTeam,
    stateIds: config.stateIds,
    teams: config.subscription.teams,
  })
}

async function resolveStatesForIssueSource(
  mount: MountClient,
  config: FactoryConfig,
  resolveStates: FleetCliDeps['resolveStates'],
): Promise<FactoryStateResolution> {
  if (config.issueSource === 'github') {
    return stateResolutionFromIds(config.stateIds, config.linear.states)
  }
  if (!config.issueSource) {
    const linearReady = await mount.ensureSubRoot('/linear/issues', { timeoutMs: 90_000 })
    if (linearReady !== 'ready') {
      config.issueSource = 'github'
      return stateResolutionFromIds(config.stateIds, config.linear.states)
    }
    config.issueSource = 'linear'
  }
  return (resolveStates ?? defaultResolveStates)(mount, config)
}

async function buildMount(loaded: LoadedConfig, deps: FleetCliDeps): Promise<MountClient> {
  if (deps.mount) return deps.mount
  if (hasExplicitFixtureFiles(loaded)) return new FakeMountClient(loaded.fixtureFiles)
  let mount: MountClient
  mount = await (deps.cloudMountFromConfig ?? RelayfileCloudMountClient.fromConfig)({
    workspaceId: loaded.config.workspaceId,
    isAllowedDraft: (path, content, opts) => isAllowedFactoryDraft(path, content, opts, mount, loaded.config),
  })
  return mount
}

const hasExplicitFixtureFiles = (loaded: LoadedConfig | undefined): loaded is LoadedConfig & { fixtureFiles: Record<string, unknown> } =>
  loaded?.fixtureFiles !== undefined

async function isAllowedFactoryDraft(
  path: string,
  content: unknown,
  opts: { guarded?: boolean } | undefined,
  mount: MountClient,
  config: FactoryConfig,
): Promise<boolean> {
  if (!opts?.guarded) return false

  // Comment writeback nested under its issue: /linear/issues/<ref>/comments/<draft>.json.
  // Scope-check the owning issue (the draft content is a comment, not an issue).
  const nestedComment = /^\/linear\/issues\/([^/]+)\/comments\/[^/]+$/u.exec(path)
  if (nestedComment) {
    const issuePath = `/linear/issues/${nestedComment[1]}.json`
    try {
      const issue = await readLinearIssueWithCanonicalFallback(mount, issuePath)
      return isInFactoryScope(issue, config.safety)
    } catch {
      return false
    }
  }

  if (path.startsWith('/linear/issues/')) {
    if (isInFactoryScope(scopeIssueFromDraftContent(content), config.safety)) return true
    try {
      const issue = await readLinearIssueWithCanonicalFallback(mount, path)
      return isInFactoryScope(issue, config.safety)
    } catch {
      return false
    }
  }

  if (/^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)) {
    return true
  }

  if (isAllowedFactoryGithubDraft(path, opts)) {
    return true
  }

  return false
}

const isFactoryGithubWritebackPath = (path: string): boolean =>
  /^\/github\/repos\/[^/]+\/[^/]+\/(?:pull-requests\/factory-[^/]+\.json|refs\/refs%2Fheads%2F[^/]+\.json|pulls\/[1-9]\d*\/close\.json)$/iu.test(path)

const isAllowedFactoryGithubDraft = (
  path: string,
  opts: { guarded?: boolean } | undefined,
): boolean => opts?.guarded === true && isFactoryGithubWritebackPath(path)

const scopeIssueFromDraftContent = (content: unknown) => ({
  title: typeof asRecord(content)?.title === 'string' ? asRecord(content)?.title as string : '',
  team: typeof asRecord(asRecord(content)?.team)?.key === 'string'
    ? asRecord(asRecord(content)?.team)?.key as string
    : undefined,
  raw: asRecord(content),
})

async function readIssueArg(mount: MountClient, issueArg: string, config: FactoryConfig) {
  const path = issueArg.startsWith('/') ? issueArg : await findIssuePath(mount, issueArg, config)
  if (githubIssuePathParts(path)) {
    return parseGithubFactoryIssue(path, (await mount.readFile(path)).content)
  }
  return readLinearIssueWithCanonicalFallback(mount, path)
}

async function findIssuePath(mount: MountClient, key: string, config: FactoryConfig): Promise<string> {
  if (config.issueSource === 'github') {
    const number = Number(key.replace(/^#/, ''))
    if (!Number.isInteger(number) || number <= 0) {
      throw new Error(`Unable to resolve GitHub issue ${key}: expected a positive issue number`)
    }
    const defaultRepo = config.repos.default?.toLowerCase()
    const matches = (await mount.listTree('/github/repos'))
      .filter((path) => path.endsWith('.json'))
      .filter((path) => {
        const parts = githubIssuePathParts(path)
        if (!parts || parts.number !== number) return false
        return !defaultRepo || `${parts.owner}/${parts.repo}`.toLowerCase() === defaultRepo
      })
      .sort((left, right) => githubIssuePathPreference(left) - githubIssuePathPreference(right) || left.localeCompare(right))
    if (matches.length === 0) {
      throw new Error(`Unable to resolve GitHub issue ${key}: found 0 matches`)
    }
    const matchesByRepo = new Map<string, { repo: string; path: string }>()
    for (const path of matches) {
      const parts = githubIssuePathParts(path)!
      const repo = `${parts.owner}/${parts.repo}`
      if (!matchesByRepo.has(repo.toLowerCase())) {
        matchesByRepo.set(repo.toLowerCase(), { repo, path })
      }
    }
    if (!defaultRepo && matchesByRepo.size > 1) {
      const repos = [...matchesByRepo.values()].map((match) => match.repo).sort((left, right) => left.localeCompare(right))
      throw new Error(
        `Unable to resolve GitHub issue ${key}: matches multiple repositories (${repos.join(', ')}); ` +
        'set repos.default or pass a repo-qualified argument',
      )
    }
    return matchesByRepo.values().next().value!.path
  }
  const matches = (await mount.listTree('/linear/issues/'))
    .filter((path) => path.startsWith(`/linear/issues/${key}__`) || path === `/linear/issues/${key}.json`)
  if (matches.length !== 1) {
    throw new Error(`Unable to resolve issue ${key}: found ${matches.length} matches`)
  }
  return matches[0]
}

const githubIssuePathPreference = (path: string): number =>
  path.endsWith('/meta.json') ? 0 : path.includes('/by-id/') ? 1 : 2

function parseFlags(args: string[]): Record<string, string | undefined> {
  const flags: Record<string, string | undefined> = {}
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    if (!arg.startsWith('--')) throw new Error(`Unexpected argument ${arg}`)
    const key = arg.slice(2)
    flags[key] = requireValue(args, ++index, arg)
  }
  return flags
}

function requireValue(args: string[], index: number, flag: string): string {
  const value = args[index]
  if (!value || value.startsWith('--')) throw new Error(`${flag} requires a value`)
  return value
}

function writeJson(out: Pick<NodeJS.WriteStream, 'write'>, value: unknown): void {
  out.write(`${JSON.stringify(value, null, 2)}\n`)
}

async function flushProcessOutput(): Promise<void> {
  await Promise.all([
    flushWritable(process.stdout),
    flushWritable(process.stderr),
  ])
}

function flushWritable(stream: NodeJS.WriteStream): Promise<void> {
  if (stream.destroyed || stream.writableEnded || stream.writable === false) {
    return Promise.resolve()
  }

  return new Promise((resolve) => {
    stream.write('', () => resolve())
  })
}

function isCapability(value: string | undefined): value is Capability {
  return value === 'spawn:codex' || value === 'spawn:claude' || value === 'workflow:run'
}

function isFactoryAction(value: string): boolean {
  return value === 'start' ||
    value === 'run-once' ||
    value === 'loop' ||
    value === 'status' ||
    value === 'loop-status' ||
    value === 'kill-loop' ||
    value === 'reap-orphans' ||
    value === 'canary' ||
    value === 'triage' ||
    value === 'dispatch' ||
    value === 'babysit' ||
    value === 'close-probe'
}

function defaultAgentName(capability: Capability, now: number): string {
  return `fleet-${capability.replace('spawn:', '').replace(':', '-')}-${now}`
}

export function installFactoryStopSignalHandlers(
  factory: Factory,
  opts: {
    exit?: (code: number) => void
    processLike?: Pick<NodeJS.Process, 'once' | 'off'>
  } = {},
): () => void {
  const exit = opts.exit ?? ((code: number) => process.exit(code))
  const processLike = opts.processLike ?? process
  let stopping: Promise<void> | undefined
  let installed = true
  const remove = () => {
    if (!installed) return
    installed = false
    processLike.off('SIGINT', onSigint)
    processLike.off('SIGTERM', onSigterm)
  }
  const stopAndExit = () => {
    if (!stopping) {
      stopping = factory.stop()
    }
    void stopping.then(() => {
      remove()
      exit(0)
    }, () => {
      remove()
      exit(1)
    })
  }
  const onSigint = () => stopAndExit()
  const onSigterm = () => stopAndExit()
  processLike.once('SIGINT', onSigint)
  processLike.once('SIGTERM', onSigterm)
  return remove
}

function createStopSignalWaiter(): { promise: Promise<number>; resolve: (code: number) => void } {
  let resolve!: (code: number) => void
  const promise = new Promise<number>((innerResolve) => {
    resolve = innerResolve
  })
  return { promise, resolve }
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

function usage(): string {
  return 'usage: factory <command> [options]'
}

function helpText(): string {
  return `${usage()}

Commands:
  run-once              Run one discovery -> triage -> dispatch cycle
  start                 Run the live factory daemon
  status                Print current factory status as JSON
  loop                  Run the bounded loop configured in factory.config.json
  loop-status           Print heartbeat/liveness status for the loop
  kill-loop             Send SIGTERM to the heartbeat pid
  reap-orphans          Reap stale factory-owned agents
  canary <KEY|path>     Check that a known issue is dispatch-ready
  triage <KEY|path>     Triage one issue and print the decision
  dispatch <KEY|path>   Triage and dispatch one issue
  babysit <PR|URL>      Shepherd an existing open PR to green
  close-probe <PR>      Probe/close a PR for an issue
  fleet <command>       Low-level fleet commands: spawn, roster, release

Options:
  --config <path>       Factory config JSON path (default: ./factory.config.json)
  --dry-run             Discover and triage without writes or agent spawns
  --backend <backend>   Fleet backend: internal or relay
  --agent-exit-timeout <ms>
                        Max owned-broker wait for task-exit agents (default: 1800000;
                        env: FACTORY_AGENT_EXIT_TIMEOUT_MS)
  -h, --help            Show this help
`
}

function isHelpFlag(arg: string): boolean {
  return arg === '-h' || arg === '--help'
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const code = await runFleetCli(argv)
  process.exitCode = code
}
