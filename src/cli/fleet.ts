import { existsSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { randomUUID } from 'node:crypto'
import readline from 'node:readline/promises'
import { ensureCloudSession, type CloudSession } from '@agent-relay/cloud'

import { stringifyLogValue } from '../logging'
import { resolveLocalFactoryConfig, type LocalClonePathOptions } from '../config/local-clone-paths'
import { initializeFactory } from './init'
import {
  FileStateStore,
  RelayfileCloudMountClient,
  checkFactoryLoopLiveness,
  closeProbePr,
  createFactory,
  createFleet,
  createFactoryCloudEventV1,
  ensureRelayBroker,
  FactoryCloudReporter,
  FileFactoryCloudEventOutbox,
  defaultGhRunner,
  explicitLinkedIssueKey,
  githubIssuePathParts,
  githubWatchStatePath,
  isInFactoryScope,
  parseGithubFactoryIssue,
  parseLinearIssue,
  publishFactoryMountHealth,
  parseOwnedBrokerAgentExitTimeoutMs,
  parseStandaloneBabysitTarget,
  readStandalonePullRequest,
  readLinearIssueWithCanonicalFallback,
  reapFactoryOrphansOnce,
  reapFactoryEnvironmentsOnce,
  readFactoryLoopHeartbeat,
  resolveFactoryStates,
  stateResolutionFromIds,
  standaloneBabysitterAgentName,
  renderAgentTask,
  resolveFactoryWorkspace,
  type Capability,
  type Factory,
  type FactoryEventReporter,
  type FactoryConfig,
  type IterationReport,
  type FleetBackend,
  type FleetClient,
  type GhRunner,
  type FactoryStateResolution,
  type Logger,
  type LocalMountHealthEvent,
  type LocalMountOptions,
  type MountClient,
  type IssueResolution,
  type LinearIssue,
  type ProbeCloser,
  type RelayfileCloudMountClientConfig,
  type ResolvedFactoryWorkspace,
} from '../index'
import { resolveTestGuidance } from '../dispatch/test-guidance'
import { FakeFleetClient, FakeMountClient } from '../testing'
import { GitAgentWorktreeManager } from '../git/agent-worktree'
import { checkFeatureMap, type CheckFeatureMapOptions, type FeatureMapCheckReport } from '../featuremap'
import { loadOrCreateFactoryInstanceId, resolveFactoryInstanceName } from '../observability/instance-identity'
import {
  ensureFactoryIntegrations,
  inspectFactoryIntegration,
  openIntegrationUrl,
  type FactoryIntegrationObservation,
} from '../mount/relayfile-integration-preflight'
import type { FactoryIntegrationProvider } from '../ports'
import { checkMountStaleness } from '../mount/relayfile-binary'
import { MountAuthScopeError } from '../mount/mount-auth-error'
import { resolveRelayWorkspaceKey } from '../fleet/relay-workspace-key'
import {
  GhCliIssuePublisher,
  RelayChannelNotionClaimStore,
  RelayChannelNotionContractPublisher,
  loadNotionIntakeManifest,
  runNotionIntake,
  type NotionIntakeClaimStore,
  type NotionContractPublisher,
  type WorkspaceTaskDispatcher,
} from '../intake'

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
    options?: LocalMountOptions,
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
  /** Hermetic local-checkout probes for CLI integration tests. */
  localClonePathOptions?: LocalClonePathOptions
  reporter?: FactoryEventReporter
  cloudSessionProvider?: (options?: Parameters<typeof ensureCloudSession>[0]) => Promise<CloudSession>
  isInteractive?: () => boolean
  confirmIntegrationConnect?: (provider: FactoryIntegrationProvider) => Promise<boolean>
  openIntegrationUrl?: (url: string) => void | Promise<void>
  featureMapCheck?: (options?: CheckFeatureMapOptions) => Promise<FeatureMapCheckReport>
  /** Hermetic portable Notion contract publisher for intake tests and alternate runtimes. */
  notionContracts?: NotionContractPublisher
  /** Hermetic workspace-global Notion claim store for tests and alternate runtimes. */
  notionClaims?: NotionIntakeClaimStore
  /** Hermetic verification-environment sweep for CLI tests and alternate runtimes. */
  reapEnvironments?: typeof reapFactoryEnvironmentsOnce
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

const autoDetectedIssueSources = new WeakSet<FactoryConfig>()
type ParsedCommand =
  | { kind: 'spawn'; input: { capability: Capability; name?: string; node?: 'self' | string; task?: string; workflow?: string; model?: string; sessionRef?: string; cwd?: string } }
  | { kind: 'roster' }
  | { kind: 'release'; name: string; reason?: string }
  | { kind: 'factory'; action: 'run-once' | 'loop' | 'status' | 'loop-status' | 'kill-loop' | 'reap-orphans' }
  | { kind: 'factory'; action: 'start'; mode?: 'live' }
  | { kind: 'factory-canary'; issue: string }
  | { kind: 'factory-triage'; issue: string }
  | { kind: 'factory-dispatch'; issue: string }
  | { kind: 'factory-babysit'; prNumber: number; repo?: string; url?: string }
  | { kind: 'factory-close-probe'; prNumber: number; repo: string; issue: string }
  | { kind: 'featuremap-check'; manifestPath?: string; baseRef?: string }
  | { kind: 'factory-init'; repo?: string; workspaceId?: string }
  | { kind: 'notion-intake'; manifestPath: string }

export async function runFleetCli(argv: string[], deps: FleetCliDeps = {}): Promise<number> {
  const out = deps.stdout ?? process.stdout
  const err = deps.stderr ?? process.stderr
  let fleet: FleetClient | undefined
  let mount: MountClient | undefined
  let reporter: FactoryEventReporter | undefined
  let notionContracts: NotionContractPublisher | undefined
  let notionClaims: NotionIntakeClaimStore | undefined

  try {
    if (argv.some(isHelpFlag)) {
      out.write(helpText())
      return 0
    }
    if (argv.some(isVersionFlag)) {
      out.write(`${await readFactoryVersion()}\n`)
      return 0
    }
    const { globals, args } = parseGlobalOptions(argv)
    const command = parseFleetCommand(args)

    if (command.kind === 'featuremap-check') {
      const report = await (deps.featureMapCheck ?? checkFeatureMap)({
        ...(command.manifestPath ? { manifestPath: command.manifestPath } : {}),
        ...(command.baseRef ? { baseRef: command.baseRef } : {}),
      })
      writeJson(out, report)
      return 0
    }

    if (command.kind === 'factory-init') {
      await initializeFactory({ repo: command.repo, workspaceId: command.workspaceId, stdout: out, stderr: err })
      return 0
    }

    if (command.kind === 'notion-intake') {
      const manifest = await loadNotionIntakeManifest(command.manifestPath)
      if (!globals.dryRun) {
        notionClaims = deps.notionClaims
        notionContracts = deps.notionContracts
        const needsWorkspaceKey = !notionClaims ||
          (manifest.workerMountTransport.kind === 'relay-channel' && !notionContracts)
        const workspaceKey = needsWorkspaceKey
          ? resolveRelayWorkspaceKey({
            env: deps.env ?? process.env,
            ...(deps.env ? { activeWorkspaceKey: () => undefined } : {}),
          })
          : undefined
        if (needsWorkspaceKey && !workspaceKey) {
          throw new Error('Notion dispatch requires an active Agent Relay workspace for its durable shared claim')
        }
        notionClaims ??= new RelayChannelNotionClaimStore({ workspaceKey: workspaceKey! })
        if (manifest.workerMountTransport.kind === 'relay-channel') {
          notionContracts ??= new RelayChannelNotionContractPublisher({ workspaceKey: workspaceKey! })
        }
      }
      const workspace: WorkspaceTaskDispatcher = {
        find: async (name) => {
          fleet ??= await buildFleet(globals, undefined, deps)
          const running = (await fleet.roster()).agents.find((agent) => agent.name === name)
          return running ? { agent: running.name, node: running.node } : undefined
        },
        dispatch: async (task) => {
          fleet ??= await buildFleet(globals, undefined, deps)
          const running = (await fleet.roster()).agents.find((agent) => agent.name === task.name)
          if (running) return { agent: running.name, node: running.node, status: 'already-running' }
          const spawned = await fleet.spawn({
            name: task.name,
            capability: 'spawn:codex',
            node: task.node ?? 'self',
            task: task.task,
            cwd: task.projectPath,
            invocationId: task.invocationId,
          })
          fleet.preserveInfrastructureOnDispose?.()
          return { agent: spawned.name, node: spawned.node, status: 'spawned' }
        },
        redispatch: async (task) => {
          fleet ??= await buildFleet(globals, undefined, deps)
          const running = (await fleet.roster()).agents.find((agent) => agent.name === task.name)
          if (running) {
            await fleet.sendMessage({ to: `@${running.name}`, text: task.task, mode: 'steer' })
            return { agent: running.name, node: running.node, status: 'updated-running' }
          }
          const spawned = await fleet.spawn({
            name: task.name,
            capability: 'spawn:codex',
            node: task.node ?? 'self',
            task: task.task,
            cwd: task.projectPath,
            invocationId: task.invocationId,
          })
          fleet.preserveInfrastructureOnDispose?.()
          return { agent: spawned.name, node: spawned.node, status: 'respawned' }
        },
      }
      const report = await runNotionIntake({
        manifest,
        dispatch: !globals.dryRun,
        ...(!globals.dryRun ? {
          github: new GhCliIssuePublisher(),
          workspace,
          ...(notionClaims ? { claims: notionClaims } : {}),
          ...(notionContracts ? { contracts: notionContracts } : {}),
        } : {}),
      })
      writeJson(out, report)
      return report.ok ? 0 : 1
    }

    if (command.kind === 'factory-close-probe') {
      // Manual close-probe remains strict; the daemon relaxes the title marker only after issue-synthetic classification.
      let githubWrite
      if (!deps.probeCloser) {
        const workspaceId = (await (deps.resolveWorkspace ?? resolveFactoryWorkspace)()).workspaceId
        mount = deps.mount ?? await (deps.cloudMountFromConfig ?? RelayfileCloudMountClient.fromConfig)({
          workspaceId,
          isAllowedDraft: (path, _content, opts) => isAllowedFactoryGithubDraft(path, opts),
        })
        await prepareFactoryIntegrations(command, mount, undefined, globals, deps, workspaceId, err)
        githubWrite = mount.githubWrite
      }
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

    const resolvesLocalClonePaths = globals.backend === 'internal' && commandUsesLocalCheckout(command)
    const loaded = command.kind.startsWith('factory')
      ? await loadConfig(globals.config, {
          ...deps.localClonePathOptions,
          inferFromCwd: resolvesLocalClonePaths,
          logger: streamLogger(err),
          validateConfiguredCheckouts: resolvesLocalClonePaths && (
            deps.localClonePathOptions?.validateConfiguredCheckouts ?? !hasInjectedFactoryRuntime(deps)
          ),
        })
      : undefined
    fleet = await buildFleet(globals, loaded, deps)

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
          workflow: command.input.workflow,
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
          const processes = await reapFactoryOrphansOnce({
            heartbeatPath: loaded.config.loop.heartbeatPath,
            registryPath: loaded.config.loop.registryPath,
            staleMs: loaded.config.loop.heartbeatStaleMs,
            fleet,
          })
          writeJson(out, { ...processes, environments: await safeEnvironmentReap(deps.reapEnvironments) })
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
        const logger = streamLogger(err)
        const pendingMountHealthEvents: LocalMountHealthEvent[] = []
        const reportMountHealth = async (event: LocalMountHealthEvent): Promise<void> => {
          if (!mount) {
            pendingMountHealthEvents.push(event)
            return
          }
          if (reporter) {
            await reporter.report(createFactoryCloudEventV1({
              type: event.state === 'degraded' ? 'factory.anomaly' : 'factory.snapshot',
              level: event.state === 'degraded' ? 'error' : 'info',
              attributes: {
                component: 'relayfile_mount',
                operation: 'supervise',
                errorCode: event.reason,
                count: event.degradedMounts,
              },
            }))
          }
          try {
            await publishFactoryMountHealth(mount, workspaceId, event)
          } catch (error) {
            logger.warn?.('[factory] unable to publish Relayfile mount health signal', {
              errorClass: error instanceof Error ? error.name : 'Error',
            })
          }
        }
        mount = await buildMount(loaded, deps, {
          logger,
          onLocalMountHealth: reportMountHealth,
        })
        await prepareFactoryIntegrations(command, mount, loaded.config, globals, deps, workspaceId, err)
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
        const stateStore = new FileStateStore({
          batchSize: loaded.config.batchSize,
          watchStatePath: githubWatchStatePath(loaded.config.loop.registryPath),
        })
        reporter = deps.reporter ?? await buildFactoryCloudReporter({
          config: loaded.config,
          backend: globals.backend,
          mode: factoryReportingMode(command),
          logger,
          deps,
        })
        if (reporter) {
          await reporter.report(createFactoryCloudEventV1({
            type: 'instance.started',
            attributes: {
              backend: globals.backend,
              mode: factoryReportingMode(command),
              component: 'cli',
              operation: 'start',
            },
          }))
        }
        for (const event of pendingMountHealthEvents.splice(0)) {
          await reportMountHealth(event)
        }
        const factory = (deps.createFactory ?? createFactory)(loaded.config, {
          mount,
          fleet,
          stateStore,
          stateResolution,
          probePrGhRunner: deps.probePrGhRunner ?? defaultGhRunner,
          logger,
          reporter,
          worktrees: globals.backend === 'internal' ? new GitAgentWorktreeManager() : undefined,
        })
        return await runFactoryCommand(command, factory, mount, fleet, loaded.config, globals, out, deps, workspaceId, acceptableMountIds)
      }
    }
    return 1
  } catch (error) {
    if (reporter) {
      try {
        await reporter.report(createFactoryCloudEventV1({
          type: 'factory.failure',
          level: 'error',
          attributes: { component: 'cli', operation: 'command', errorCode: 'command_failed' },
        }))
      } catch {
        // An injected reporter may violate the no-reject port contract. The
        // original command failure still determines the CLI result.
      }
    }
    err.write(`${error instanceof Error ? error.message : String(error)}\n`)
    return 1
  } finally {
    try {
      try {
        await notionClaims?.dispose?.()
      } catch {
        err.write('[factory] warning: Notion claim store failed during shutdown\n')
      }
    } finally {
      try {
        try {
          await notionContracts?.dispose?.()
        } catch {
          err.write('[factory] warning: Notion contract publisher failed during shutdown\n')
        }
      } finally {
        try {
          await mount?.dispose?.()
        } finally {
          try {
            await fleet?.dispose()
          } finally {
            if (reporter) {
              try {
                await reporter.report(createFactoryCloudEventV1({
                  type: 'instance.stopping',
                  attributes: { component: 'cli', operation: 'stop' },
                }))
                await reporter.report(createFactoryCloudEventV1({
                  type: 'instance.stopped',
                  attributes: { component: 'cli', operation: 'stop' },
                }))
                await reporter.close?.({ deadlineMs: 2_000 })
              } catch {
                err.write('[factory] warning: Cloud progress reporter failed during shutdown\n')
              }
            }
          }
        }
      }
    }
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

  if (verb === 'featuremap') {
    return parseFeatureMapCommand(rest)
  }

  if (verb === 'intake') {
    return parseIntakeCommand(rest)
  }

  throw new Error(`Unknown factory command: ${verb}`)
}

function parseIntakeCommand(args: string[]): ParsedCommand {
  const [source, manifestPath, ...rest] = args
  if (source !== 'notion') throw new Error('factory intake currently requires the notion source')
  if (!manifestPath) throw new Error('factory intake notion requires a manifest path')
  if (rest.length > 0) throw new Error(`Unexpected factory intake argument: ${rest[0]}`)
  return { kind: 'notion-intake', manifestPath }
}

function parseFeatureMapCommand(args: string[]): ParsedCommand {
  const [action, ...flags] = args
  if (action !== 'check') {
    throw new Error('factory featuremap requires the check command')
  }
  const parsed = parseFlags(flags)
  const unexpected = Object.keys(parsed).find((key) => key !== 'manifest' && key !== 'base')
  if (unexpected) throw new Error(`Unknown factory featuremap option: --${unexpected}`)
  return {
    kind: 'featuremap-check',
    ...(parsed.manifest ? { manifestPath: parsed.manifest } : {}),
    ...(parsed.base ? { baseRef: parsed.base } : {}),
  }
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
    if (capability === 'workflow:run' && !parsed.workflow) {
      throw new Error('factory fleet spawn workflow:run requires --workflow <path>')
    }
    return {
      kind: 'spawn',
      input: {
        capability,
        name: parsed.name,
        node: parsed.node,
        task: parsed.task,
        ...(parsed.workflow ? { workflow: parsed.workflow } : {}),
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
  const mountFn = resolveLocalMountFn(deps, mount)
  const mountStderr = deps.stderr ?? process.stderr
  const debugMountRefreshes = (deps.env ?? process.env).FACTORY_LOG_LEVEL?.toLowerCase() === 'debug'
  if (command.kind === 'factory') {
    if (command.action === 'start') {
      const waiter = createStopSignalWaiter()
      let stoppedBySignal = false
      const flushAndResolve = async (code: number): Promise<void> => {
        try {
          await (deps.flushDaemonOutput ?? flushProcessOutput)()
        } finally {
          waiter.resolve(code)
        }
      }
      // Once a workspace mirror is known, retain background stale-mount
      // supervision so durable recovery is not serialized behind a refresh.
      // If there is no registered root yet, however, wait for the single
      // mount/admission fallback before Factory can dispatch: agents must not
      // receive a provisional checkout-local `.integrations` path.
      //
      // A MountAuthScopeError is the exception: it is terminal (the cloud
      // session lacks the filesystem scope the mount needs), so limping on
      // would only spawn agents against a read-denied mirror. Fail fast with the
      // remediation and resolve the command with a non-zero code.
      const warmMount = () => warmStartPathMounts(
        mount,
        mountFn,
        workspaceId,
        config,
        acceptableMountIds,
        mountStderr,
        debugMountRefreshes,
      )
      const handleWarmMountError = (error: unknown): void => {
        if (error instanceof MountAuthScopeError) {
          mountStderr.write(`${error.message}\n`)
          mountStderr.write('[factory] aborting startup: local mount cannot obtain its filesystem scopes.\n')
          void flushAndResolve(1)
          return
        }
        const message = error instanceof Error ? error.message : String(error)
        mountStderr.write(`[factory] warning: background relayfile mount warmup failed: ${message}\n`)
      }
      const removeSignalHandlers = installFactoryStopSignalHandlers(factory, {
        exit: (code) => {
          stoppedBySignal = true
          // Resolve the command instead of calling process.exit here. Returning
          // through runFleetCli's outer finally gives Cloud lifecycle reporting
          // its bounded shutdown opportunity before the caller applies `code`.
          void flushAndResolve(code)
        },
        processLike: deps.stopSignalProcessLike,
      })
      try {
        if (mount.getLocalMountRoot?.() === undefined) {
          try {
            const result = await warmMount()
            if (!result.mounted) {
              mountStderr.write('[factory] aborting startup: Relayfile workspace mirror could not be resolved.\n')
              if (stoppedBySignal) return await waiter.promise
              return 1
            }
          } catch (error) {
            handleWarmMountError(error)
            if (stoppedBySignal) return await waiter.promise
            return 1
          }
        } else {
          void warmMount().catch(handleWarmMountError)
        }
        if (stoppedBySignal) return await waiter.promise
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
      await ensureWorkspaceMount(
        mount,
        mountFn,
        workspaceId,
        acceptableMountIds,
        mountStderr,
      )
      writeJson(out, await factory.runOnce({ dryRun: globals.dryRun }))
      return 0
    }
    if (command.action === 'status') {
      writeJson(out, await factoryStatusWithMountHealth(factory, mount, config.loop.heartbeatPath, config.loop.heartbeatStaleMs))
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
    await ensureWorkspaceMount(
      mount,
      mountFn,
      workspaceId,
      acceptableMountIds,
      mountStderr,
    )
    const removeSignalHandlers = installFactoryStopSignalHandlers(factory, {
      processLike: deps.stopSignalProcessLike,
    })
    try {
      const reports = await factory.runLoop({ dryRun: globals.dryRun })
      writeJson(out, {
        reports,
        status: await factoryStatusWithMountHealth(factory, mount, config.loop.heartbeatPath, config.loop.heartbeatStaleMs),
      })
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

  const resolved = await resolveIssueArg(mount, command.issue, config, async () =>
    issueProjectionStatus(factory, mount, config),
  )
  const decision = {
    ...await factory.triageIssue(resolved.issue),
    issueResolution: resolved.resolution,
  }
  if (command.kind === 'factory-triage') {
    writeJson(out, decision)
    return 0
  }

  if (command.kind === 'factory-dispatch' && globals.backend === 'relay' && !globals.dryRun) {
    // A relay dispatch is not placement-only. This process becomes the durable
    // lifecycle publisher (or attaches to the current owner's durable row),
    // and stays through takeover/publication/writeback/release to terminal.
    await factory.start({ mode: 'dispatch-owner' })
    try {
      const result = await factory.dispatch(decision, { dryRun: false })
      writeJson(out, result)
      await factory.waitForDispatchTerminal(result.issue)
      return 0
    } finally {
      await factory.stop()
    }
  }

  writeJson(out, await factory.dispatch(decision, { dryRun: globals.dryRun }))
  return 0
}

async function warmStartPathMounts(
  mount: MountClient,
  mountFn: NonNullable<FleetCliDeps['ensureLocalMount']>,
  workspaceId: string,
  config: FactoryConfig,
  acceptableMountIds?: readonly string[],
  stderr: Pick<NodeJS.WriteStream, 'write'> = process.stderr,
  debug = process.env.FACTORY_LOG_LEVEL?.toLowerCase() === 'debug',
): Promise<WorkspaceMountPreflight> {
  const result = await ensureWorkspaceMount(
    mount,
    mountFn,
    workspaceId,
    acceptableMountIds,
    stderr,
  )
  writeMountRefreshSummary(result.refreshed ? [result.refreshed] : [], stderr, debug)
  stderr.write(
    `[factory] Relayfile workspace mirror preflight: mounted=${result.mounted ? 1 : 0} ` +
    `failed=${result.mounted ? 0 : 1} routedRepos=${new Set(Object.values(config.repos.byLabel)).size}\n`,
  )
  return result
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
  const mountFn = resolveLocalMountFn(deps, mount)
  await ensureWorkspaceMount(mount, mountFn, workspaceId, acceptableMountIds, deps.stderr)

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
  const testGuidance = await resolveTestGuidance({
    repoPath: clonePath,
    issue,
    changedFiles: pr.filesChanged,
  })
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
    integrationsMountRoot: resolveIntegrationsMountRoot(mount),
    testGuidance,
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
    capability: config.agentCapabilities.babysitter,
    node: 'self',
    repo,
    clonePath,
    task,
    model: config.models.babysitter,
    cwd: clonePath,
    invocationId: `factory-babysit:${repo}#${pr.number}`,
  })
  fleet.preserveInfrastructureOnDispose?.()
  writeJson(out, { status: 'spawned', ...receiptBase, agent: spawned.name })
  return 0
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
  const configured = Object.entries(config.clonePaths ?? {}).find(([candidate]) => candidate.toLowerCase() === repo.toLowerCase())?.[1]
    ?? Object.entries(config.repos.clonePaths ?? {}).find(([candidate]) => candidate.toLowerCase() === repo.toLowerCase())?.[1]
  if (configured && existsSync(configured)) return configured
  return undefined
}

/**
 * Ensures exactly one Relayfile mirror for the workspace.  Agents receive this
 * absolute path in their tasks, so routing more repositories never asks
 * Relayfile to re-home the mirror into each checkout.
 */
async function ensureWorkspaceMount(
  mount: MountClient,
  mountFn: NonNullable<FleetCliDeps['ensureLocalMount']>,
  workspaceId: string,
  acceptableMountIds?: readonly string[],
  stderr: Pick<NodeJS.WriteStream, 'write'> = process.stderr,
): Promise<WorkspaceMountPreflight> {
  const mountOpts = { acceptableWorkspaceIds: acceptableMountIds }
  const localDir = mount.getLocalMountRoot?.()
  return ensureMountPath(
    mountFn,
    workspaceId,
    localDir ? dirname(localDir) : process.cwd(),
    mountOpts,
    stderr,
    localDir,
  )
}

type RefreshedStaleMount = { path: string; reason?: string }
type WorkspaceMountPreflight = { mounted: boolean; refreshed?: RefreshedStaleMount }

async function ensureMountPath(
  mountFn: NonNullable<FleetCliDeps['ensureLocalMount']>,
  workspaceId: string,
  path: string,
  mountOpts: { acceptableWorkspaceIds?: readonly string[] },
  stderr: Pick<NodeJS.WriteStream, 'write'>,
  localDir = join(resolve(path), '.integrations'),
): Promise<WorkspaceMountPreflight> {
  const statePath = join(localDir, '.relay', 'state.json')
  const staleBefore = checkMountStaleness(statePath, workspaceId, mountOpts.acceptableWorkspaceIds)
  try {
    await mountFn(workspaceId, resolve(path), {
      ...mountOpts,
      ...(staleBefore.stale ? { suppressStaleRefreshLogs: true } : {}),
    })
    if (staleBefore.stale && !checkMountStaleness(statePath, workspaceId, mountOpts.acceptableWorkspaceIds).stale) {
      return { mounted: true, refreshed: { path: localDir, reason: staleBefore.reason } }
    }
    return { mounted: true }
  } catch (error) {
    // A scope shortfall is terminal and identical across every clone path;
    // propagate it so startup fails fast with one remediation instead of
    // logging the same unfixable warning per path.
    if (error instanceof MountAuthScopeError) throw error
    const message = error instanceof Error ? error.message : String(error)
    stderr.write(`[factory] warning: could not start Relayfile workspace mirror at ${localDir}: ${message}\n`)
  }
  return { mounted: false }
}

function resolveIntegrationsMountRoot(mount: MountClient): string {
  return mount.getLocalMountRoot?.() ?? resolve(process.cwd(), '.integrations')
}

async function factoryStatusWithMountHealth(
  factory: Factory,
  mount: MountClient,
  heartbeatPath: string,
  heartbeatStaleMs: number,
): Promise<ReturnType<Factory['status']> & {
  localMountDegraded?: boolean
  localMountDegradedReason?: string
  localMountRoot?: string
  /** Local mirror liveness, independently of whether the daemon is listening. */
  localMountEventFeed?: {
    state: 'healthy' | 'degraded'
    livenessSignal: '.integrations/.relay/state.json'
    reason?: string
    root?: string
  }
}> {
  const processStatus = factory.status()
  const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
  // `factory status` runs in a fresh CLI process, so its in-memory Factory has
  // no knowledge of counters accumulated by the live daemon. The heartbeat is
  // the daemon-owned status handoff; local counters win only when this helper
  // is used in the same process as active work.
  const status = {
    ...processStatus,
    counters: { ...(heartbeat?.counters ?? {}), ...processStatus.counters },
  }
  const liveness = checkFactoryLoopLiveness(heartbeat, { staleMs: heartbeatStaleMs })
  const eventListener = liveness.ok
    ? heartbeat?.eventListener ?? {
      state: 'unknown' as const,
      reason: 'running daemon heartbeat does not report event listener state',
    }
    : {
      state: 'not-listening' as const,
      reason: liveness.reason,
    }
  const health = mount.getLocalMountHealth?.()
  if (!health) return { ...status, eventListener }
  return {
    ...status,
    eventListener,
    localMountDegraded: health.degraded,
    ...(health.reason ? { localMountDegradedReason: health.reason } : {}),
    ...(health.localDir ? { localMountRoot: health.localDir } : {}),
    localMountEventFeed: {
      state: health.degraded ? 'degraded' : 'healthy',
      livenessSignal: '.integrations/.relay/state.json',
      ...(health.reason ? { reason: health.reason } : {}),
      ...(health.localDir ? { root: health.localDir } : {}),
    },
  }
}

function writeMountRefreshSummary(
  refreshedStaleMounts: readonly RefreshedStaleMount[],
  stderr: Pick<NodeJS.WriteStream, 'write'>,
  debug: boolean,
): void {
  if (debug) {
    for (const mount of [...refreshedStaleMounts].sort((left, right) => left.path.localeCompare(right.path))) {
      const suffix = mount.reason ? ` (${mount.reason})` : ''
      stderr.write(`[factory] debug: refreshed stale local mount at ${mount.path}${suffix}\n`)
    }
  }
  if (refreshedStaleMounts.length > 0) {
    const ages = refreshedStaleMounts
      .map(({ reason }) => reason?.match(/^last reconcile (\d+)m ago$/u)?.[1])
      .filter((age): age is string => age !== undefined)
      .map(Number)
    const ageSuffix = ages.length > 0 ? ` (last reconcile ~${Math.max(...ages)}m ago)` : ''
    stderr.write(`[factory] refreshed ${refreshedStaleMounts.length} stale local mount(s)${ageSuffix}\n`)
  }
}

function resolveLocalMountFn(
  deps: FleetCliDeps,
  mount: MountClient,
): NonNullable<FleetCliDeps['ensureLocalMount']> {
  if (deps.ensureLocalMount) return deps.ensureLocalMount
  if (mount.ensureLocalMount) {
    const ensureLocalMount = mount.ensureLocalMount.bind(mount)
    return async (_workspaceId, startDir, options) => ensureLocalMount(startDir, options)
  }
  return async () => {
    throw new Error('Mount client does not provide Relayfile SDK local-mount support')
  }
}

function parseFactoryCommand(args: string[]): ParsedCommand {
  const [action, issueOrPr, ...flags] = args
  if (action === 'init') {
    const values = [issueOrPr, ...flags].filter((value): value is string => Boolean(value))
    let repo: string | undefined
    let workspaceId: string | undefined
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index]
      if (value === '--workspace') {
        workspaceId = requireValue(values, ++index, '--workspace')
        continue
      }
      if (value.startsWith('-')) throw new Error(`Unknown factory init option: ${value}`)
      if (repo) throw new Error('factory init accepts at most one owner/repo argument')
      repo = value
    }
    return { kind: 'factory-init', repo, workspaceId }
  }
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

async function loadConfig(path?: string, options: LocalClonePathOptions = {}): Promise<LoadedConfig> {
  const configPath = path ?? resolve(process.cwd(), 'factory.config.json')
  const raw = JSON.parse(await readFile(configPath, 'utf8')) as unknown
  const record = asRecord(raw)
  return {
    config: await resolveLocalFactoryConfig(raw, {
      ...options,
      // fixtureFiles describe a hermetic in-memory run; their checkout paths
      // are intentionally synthetic and must not be preflighted on the host.
      validateConfiguredCheckouts: options.validateConfiguredCheckouts && !record.fixtureFiles,
    }),
    fixtureFiles: record.fixtureFiles ? asRecord(record.fixtureFiles) : undefined,
  }
}

function commandUsesLocalCheckout(command: ParsedCommand): boolean {
  switch (command.kind) {
    case 'factory':
      return command.action === 'run-once' || command.action === 'loop' || command.action === 'start'
    case 'factory-canary':
    case 'factory-triage':
    case 'factory-dispatch':
    case 'factory-babysit':
      return true
    default:
      return false
  }
}

function factoryReportingMode(command: Extract<ParsedCommand, { kind: `factory${string}` | 'factory' }>): string {
  return command.kind === 'factory' ? command.action : command.kind.slice('factory-'.length)
}

async function buildFactoryCloudReporter(input: {
  config: FactoryConfig
  backend: FleetBackend
  mode: string
  logger: Logger
  deps: FleetCliDeps
}): Promise<FactoryEventReporter | undefined> {
  if (!input.config.reporting.enabled) return undefined
  if (hasInjectedFactoryRuntime(input.deps) && !input.deps.cloudSessionProvider) return undefined

  try {
    const activeWorkspace = await (input.deps.resolveWorkspace ?? resolveFactoryWorkspace)()
    const activeWorkspaceIds = new Set([
      activeWorkspace.workspaceId,
      activeWorkspace.cloudWorkspaceId,
    ].filter((value): value is string => Boolean(value)))
    if (!input.config.workspaceId || !activeWorkspaceIds.has(input.config.workspaceId)) {
      input.logger.warn?.('[factory] Cloud progress reporting skipped because the active account workspace differs from Factory config')
      return undefined
    }
    const session = await (input.deps.cloudSessionProvider ?? ensureCloudSession)({ interactive: false })
    const outboxPath = input.config.reporting.outboxPath
      ?? join(dirname(input.config.loop.registryPath), 'factory-cloud-events.json')
    const instanceId = await loadOrCreateFactoryInstanceId(`${outboxPath}.instance-id`)
    const instanceName = resolveFactoryInstanceName(input.config)
    const cloudFetch: typeof fetch = async (_request, init) =>
      session.client.fetch('/api/v1/factory/events', init)
    return new FactoryCloudReporter({
      apiUrl: session.auth.apiUrl,
      instance: {
        id: instanceId,
        bootId: randomUUID(),
        version: await readFactoryVersion(),
        metadata: {
          ...(instanceName !== undefined ? { name: instanceName } : {}),
          backend: input.backend,
          mode: input.mode,
          runtime: 'node',
        },
      },
      outbox: new FileFactoryCloudEventOutbox({ path: outboxPath }),
      getAccessToken: async () => session.client.snapshot().accessToken,
      fetch: cloudFetch,
      logger: input.logger,
      batchSize: input.config.reporting.batchSize,
      requestTimeoutMs: input.config.reporting.requestTimeoutMs,
    })
  } catch (error) {
    input.logger.warn?.('[factory] Cloud progress reporting is unavailable', {
      errorClass: error instanceof Error ? error.name : 'Error',
    })
    return undefined
  }
}

function hasInjectedFactoryRuntime(deps: FleetCliDeps): boolean {
  return Boolean(deps.fleet || deps.mount || deps.createFactory || deps.createFleet || deps.cloudMountFromConfig)
}

async function buildFleet(
  globals: GlobalOptions,
  loaded: LoadedConfig | undefined,
  deps: FleetCliDeps,
): Promise<FleetClient> {
  if (deps.fleet) return deps.fleet
  if (globals.backend === 'internal' && hasExplicitFixtureFiles(loaded)) return new FakeFleetClient()

  const cwd = process.cwd()
  const env = deps.env ?? process.env
  const connectionPath = resolveBrokerConnectionPath(cwd, env)

  // An injected createFleet owns fleet construction entirely (tests), so skip the
  // real broker bootstrap.
  if (deps.createFleet) {
    return deps.createFleet(
      { backend: globals.backend, cwd, connectionPath, previewConfig: loaded?.config.preview },
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
    const { client, started, workspaceKey } = await (deps.ensureRelayBroker ?? ensureRelayBroker)({
      cwd,
      connectionPath,
      logger,
      env,
    })
    if (started && connectionPath === undefined) {
      stderr.write(
        `[factory] no existing relay broker connection found under ${cwd} (.agentworkforce/relay/connection.json);\n` +
        'starting a NEW broker. If you expected to attach to an already-running Factory, you are likely running\n' +
        'from the wrong directory — cd to the checkout that owns .agentworkforce/relay/ and retry.\n',
      )
    }
    return createFleet(
      { backend: 'internal', cwd, connectionPath, previewConfig: loaded?.config.preview },
      {
        harnessClient: client,
        ownsBroker: started,
        ownedBrokerAgentExitTimeoutMs: globals.agentExitTimeoutMs,
        workspaceKey,
        logger,
      },
    )
  }

  return createFleet({ backend: globals.backend, cwd, connectionPath, previewConfig: loaded?.config.preview }, { env: deps.env })
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

export function formatLogArgs(args: unknown[]): string {
  if (args.length === 0) return ''
  return ` ${args.map((arg) => (typeof arg === 'string' ? arg : stringifyLogValue(arg))).join(' ')}`
}

export function resolveBrokerConnectionPath(
  startCwd = process.cwd(),
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const explicitStateDir = env.AGENT_RELAY_STATE_DIR?.trim()
  if (explicitStateDir) {
    return join(explicitStateDir, 'connection.json')
  }

  let current = resolve(startCwd)
  for (;;) {
    const relayStateDir = join(current, '.agentworkforce', 'relay')
    const candidate = join(relayStateDir, 'connection.json')
    if (existsSync(candidate)) {
      return candidate
    }
    // A stored project workspace key is an ownership boundary. Do not keep
    // walking upward and attach this project to (for example) a machine-level
    // broker that belongs to a different Relay workspace. Returning the local
    // connection path even before it exists makes ensureRelayBroker start a
    // project-local broker that joins this key instead.
    if (existsSync(join(relayStateDir, 'workspace-key.json'))) {
      return candidate
    }
    const parent = dirname(current)
    if (parent === current) {
      return undefined
    }
    current = parent
  }
}

async function prepareFactoryIntegrations(
  command: ParsedCommand,
  mount: MountClient,
  config: FactoryConfig | undefined,
  globals: GlobalOptions,
  deps: FleetCliDeps,
  workspaceId: string,
  err: Pick<NodeJS.WriteStream, 'write'>,
): Promise<void> {
  const connections = mount.integrationConnections
  if (!connections) return

  const observed = new Map<FactoryIntegrationProvider, FactoryIntegrationObservation>()
  if (config && commandUsesIssueSource(command) && !config.issueSource) {
    autoDetectedIssueSources.add(config)
    if (shouldAutoDetectGithubSource(config)) {
      config.issueSource = 'github'
    } else {
      const linear = await inspectFactoryIntegration(connections, 'linear')
      observed.set('linear', linear)
      if (linear.kind === 'ready') {
        config.issueSource = 'linear'
      } else if (linear.kind === 'missing') {
        config.issueSource = 'github'
      } else {
        await ensureFactoryIntegrations({
          connections,
          providers: ['linear'],
          workspaceId,
          interactive: false,
          dryRun: globals.dryRun,
          observed,
          io: factoryIntegrationIO(deps, err),
        })
      }
    }
  }

  const providers = requiredIntegrationsForCommand(command, config)
  if (providers.length === 0) return
  const dryRun = globals.dryRun || command.kind === 'factory-canary'
  const providersToEnsure: FactoryIntegrationProvider[] = []
  for (const provider of providers) {
    const observation = observed.get(provider) ?? await inspectFactoryIntegration(connections, provider)
    observed.set(provider, observation)
    if (
      provider === 'github' &&
      observation.kind === 'connected-not-ready' &&
      mount.githubRead &&
      (command.kind === 'factory-triage' || command.kind === 'factory-dispatch')
    ) {
      const details = [observation.state, observation.initialSyncState]
        .filter((value): value is string => Boolean(value))
        .join(', ')
      err.write(
        `[factory] warning: GitHub projection is not ready${details ? ` (${details})` : ''}; ` +
        'targeted resolution remains projection-first and will use the GitHub API fallback only for a miss.\n',
      )
      continue
    }
    providersToEnsure.push(provider)
  }
  if (providersToEnsure.length === 0) return
  await ensureFactoryIntegrations({
    connections,
    providers: providersToEnsure,
    workspaceId,
    interactive: !dryRun && (deps.isInteractive?.() ?? Boolean(process.stdin.isTTY && process.stderr.isTTY)),
    dryRun,
    observed,
    io: factoryIntegrationIO(deps, err),
  })
}

function requiredIntegrationsForCommand(
  command: ParsedCommand,
  config: FactoryConfig | undefined,
): FactoryIntegrationProvider[] {
  if (command.kind === 'factory-close-probe' || command.kind === 'factory-babysit') {
    return ['github']
  }
  if (
    command.kind === 'factory-triage' ||
    command.kind === 'factory-dispatch' ||
    command.kind === 'factory-canary'
  ) {
    return config?.issueSource === 'linear' ? ['linear', 'github'] : ['github']
  }
  if (command.kind === 'factory' && (
    command.action === 'start' || command.action === 'run-once' || command.action === 'loop'
  )) {
    return config?.issueSource === 'linear' ? ['linear', 'github'] : ['github']
  }
  return []
}

function commandUsesIssueSource(command: ParsedCommand): boolean {
  return command.kind === 'factory-triage' ||
    command.kind === 'factory-dispatch' ||
    command.kind === 'factory-canary' ||
    (command.kind === 'factory' && (
      command.action === 'start' || command.action === 'run-once' || command.action === 'loop'
    ))
}

function factoryIntegrationIO(
  deps: FleetCliDeps,
  err: Pick<NodeJS.WriteStream, 'write'>,
) {
  return {
    info: (message: string) => err.write(`[factory] ${message}\n`),
    warn: (message: string) => err.write(`[factory] warning: ${message}\n`),
    confirm: deps.confirmIntegrationConnect ?? confirmIntegrationConnect,
    openUrl: deps.openIntegrationUrl ?? openIntegrationUrl,
  }
}

async function confirmIntegrationConnect(provider: FactoryIntegrationProvider): Promise<boolean> {
  const terminal = readline.createInterface({ input: process.stdin, output: process.stderr })
  try {
    const answer = (await terminal.question(`Connect ${provider} now? (opens browser) [Y/n] `)).trim().toLowerCase()
    return answer === '' || answer === 'y' || answer === 'yes'
  } finally {
    terminal.close()
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
    autoDetectedIssueSources.add(config)
    if (shouldAutoDetectGithubSource(config)) {
      config.issueSource = 'github'
      return stateResolutionFromIds(config.stateIds, config.linear.states)
    }
    const linearReady = await mount.ensureSubRoot('/linear/issues', { timeoutMs: 90_000 })
    config.issueSource = linearReady === 'ready' ? 'linear' : 'github'
    if (config.issueSource === 'github') {
      return stateResolutionFromIds(config.stateIds, config.linear.states)
    }
  }
  return (resolveStates ?? defaultResolveStates)(mount, config)
}

function shouldAutoDetectGithubSource(config: FactoryConfig): boolean {
  if (!config.repos.org || configuredGithubIssueRepos(config).length === 0) return false
  return Object.keys(config.stateIds).length === 0 &&
    hasDefaultLinearStateNames(config.linear.states) &&
    Object.keys(config.linear.statesByTeam).length === 0 &&
    Object.keys(config.linear.teamIds).length === 0 &&
    config.subscription.teams.length === 0 &&
    config.subscription.projects.length === 0 &&
    config.subscription.assignees.length === 0
}

function hasDefaultLinearStateNames(states: FactoryConfig['linear']['states']): boolean {
  return states.readyForAgent === 'Ready for Agent' &&
    states.agentImplementing === 'Agent Implementing' &&
    states.inPlanning === 'In Planning' &&
    states.done === 'Done' &&
    states.humanReview === 'In Human Review'
}

async function buildMount(
  loaded: LoadedConfig,
  deps: FleetCliDeps,
  observability: {
    logger?: Logger
    onLocalMountHealth?: (event: LocalMountHealthEvent) => Promise<void> | void
  } = {},
): Promise<MountClient> {
  if (deps.mount) return deps.mount
  if (hasExplicitFixtureFiles(loaded)) return new FakeMountClient(loaded.fixtureFiles)
  let mount: MountClient
  mount = await (deps.cloudMountFromConfig ?? RelayfileCloudMountClient.fromConfig)({
    workspaceId: loaded.config.workspaceId,
    localMountRoot: loaded.config.localMountRoot,
    logger: observability.logger,
    onLocalMountHealth: observability.onLocalMountHealth,
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
  /^\/github\/repos\/[^/]+\/[^/]+\/(?:pull-requests\/factory-[^/]+\.json|refs\/(?:factory\.json|refs%2Fheads%2Ffactory%2F[^/]+\.json)|pulls\/[1-9]\d*\/close\.json)$/iu.test(path)

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

async function readIssueArg(mount: MountClient, issueArg: string, config: FactoryConfig): Promise<LinearIssue> {
  return (await resolveIssueArg(mount, issueArg, config)).issue
}

type ResolvedIssueArg = { issue: LinearIssue; resolution: IssueResolution }

async function resolveIssueArg(
  mount: MountClient,
  issueArg: string,
  config: FactoryConfig,
  projectionStatus?: () => Promise<IssueResolution['projection']>,
): Promise<ResolvedIssueArg> {
  const explicitPath = issueArg.startsWith('/')
  const path = explicitPath ? issueArg : await findIssuePath(mount, issueArg, config)
  if (path) {
    const issue = githubIssuePathParts(path)
      ? parseGithubFactoryIssue(path, (await mount.readFile(path)).content)
      : await readLinearIssueWithCanonicalFallback(mount, path)
    return {
      issue,
      resolution: {
        source: 'relayfile-projection',
        detail: 'Resolved from the preferred Relayfile projection.',
        projection: { outcome: 'matched' },
      },
    }
  }

  const selector = parseGithubIssueSelector(issueArg, config)
  const projection = projectionStatus
    ? await projectionStatus()
    : projectionStatusFromMount(mount)
  const unavailableReason = githubProjectionUnavailableReason(projection)
  if (!unavailableReason) {
    throw new Error(
      `${githubIssueResolutionError(config, issueArg)}: found 0 matches in the healthy Relayfile projection; ` +
      'the GitHub API fallback was not used',
    )
  }
  const fallback = await findGithubIssueThroughConnection(mount, selector, config, issueArg)
  if (!fallback) {
    throw new Error(`${githubIssueResolutionError(config, issueArg)}: found 0 matches in the projection and GitHub API`)
  }
  return {
    issue: parseGithubFactoryIssue(fallback.path, fallback.content),
    resolution: {
      source: 'github-api-fallback',
      repo: fallback.repo,
      detail: `Relayfile projection could not answer (${unavailableReason}); resolved authoritatively through the GitHub API fallback.`,
      projection,
    },
  }
}

async function findIssuePath(mount: MountClient, key: string, config: FactoryConfig): Promise<string | undefined> {
  if (config.issueSource === 'github') {
    const selector = parseGithubIssueSelector(key, config)
    const number = selector.number
    const configuredRepos = selector.repo ? [selector.repo] : configuredGithubIssueRepos(config)
    if (configuredRepos.length === 0 && hasConfiguredGithubIssueRoutes(config)) {
      throw new Error(
        `${githubIssueResolutionError(config, key)}: configured repository routes do not resolve to owner/repo; ` +
        'set repos.default to owner/repo or map its label in repos.byLabel',
      )
    }
    if (configuredRepos.length === 1) {
      const directPath = await findDirectGithubIssuePath(mount, configuredRepos[0]!, number)
      if (directPath) return directPath
    }

    const roots = configuredRepos.length > 0
      ? configuredRepos.flatMap(githubIssueRoots)
      : ['/github/repos']
    const matches = (await Promise.all([...new Set(roots)].map((root) => mount.listTree(root))))
      .flat()
      .filter((path) => path.endsWith('.json'))
      .filter((path) => {
        const parts = githubIssuePathParts(path)
        if (!parts || parts.number !== number) return false
        return configuredRepos.length === 0 || configuredRepos.some((repo) =>
          repo.toLowerCase() === `${parts.owner}/${parts.repo}`.toLowerCase(),
        )
      })
      .sort((left, right) => githubIssuePathPreference(left) - githubIssuePathPreference(right) || left.localeCompare(right))
    if (matches.length === 0) {
      return undefined
    }
    const matchesByRepo = new Map<string, { repo: string; path: string }>()
    for (const path of matches) {
      const parts = githubIssuePathParts(path)!
      const repo = `${parts.owner}/${parts.repo}`
      if (!matchesByRepo.has(repo.toLowerCase())) {
        matchesByRepo.set(repo.toLowerCase(), { repo, path })
      }
    }
    if (!config.repos.default && matchesByRepo.size > 1) {
      const repos = [...matchesByRepo.values()].map((match) => match.repo).sort((left, right) => left.localeCompare(right))
      throw new Error(
        `${githubIssueResolutionError(config, key)}: matches multiple repositories (${repos.join(', ')}); ` +
        'set repos.default or pass a repo-qualified argument',
      )
    }
    return matchesByRepo.values().next().value!.path
  }
  const matches = (await mount.listTree('/linear/issues/'))
    .filter((path) => path.startsWith(`/linear/issues/${key}__`) || path === `/linear/issues/${key}.json`)
  if (matches.length !== 1) {
    const detected = autoDetectedIssueSources.has(config) ? ', auto-detected' : ''
    throw new Error(
      `Unable to resolve issue ${key} in Linear (issueSource=linear${detected}): found ${matches.length} matches; ` +
      `set issueSource: 'github' if these are GitHub issues`,
    )
  }
  return matches[0]
}

async function findGithubIssueThroughConnection(
  mount: MountClient,
  selector: GithubIssueSelector,
  config: FactoryConfig,
  issueArg: string,
) {
  const github = mount.githubRead
  if (!github) {
    throw new Error(
      `${githubIssueResolutionError(config, issueArg)}: projection cannot answer and the GitHub API fallback is unavailable`,
    )
  }
  const repos = selector.repo ? [selector.repo] : configuredGithubIssueRepos(config)
  if (repos.length === 0) {
    throw new Error(
      `${githubIssueResolutionError(config, issueArg)}: projection cannot answer and no configured owner/repo is available for the GitHub API fallback`,
    )
  }
  const lookups = await Promise.all(repos.map((repo) => github.getIssue(repo, selector.number)))
  const found = lookups.filter((lookup): lookup is Extract<typeof lookup, { outcome: 'found' }> =>
    lookup.outcome === 'found',
  )
  const indeterminate = lookups.some((lookup) => lookup.outcome === 'indeterminate')

  if (found.length === 0) {
    if (indeterminate) {
      throw new Error(
        `${githubIssueResolutionError(config, issueArg)}: the projection could not answer and the GitHub API fallback ` +
        'could not determine whether the issue exists (one or more configured repositories are not visible without authentication)',
      )
    }
    return undefined
  }
  if (!selector.repo && !config.repos.default) {
    if (found.length > 1) {
      const matchedRepos = found.map((match) => match.issue.repo).sort((left, right) => left.localeCompare(right))
      throw new Error(
        `${githubIssueResolutionError(config, issueArg)}: GitHub API matches multiple repositories (${matchedRepos.join(', ')}); ` +
        'set repos.default or pass a repo-qualified argument',
      )
    }
    if (indeterminate) {
      // A single confirmed match is not the same as a unique one: at least
      // one other configured repository could not be checked, so a same-
      // numbered issue could exist there too. Refuse rather than silently
      // dispatch to whichever repo happened to answer.
      throw new Error(
        `${githubIssueResolutionError(config, issueArg)}: GitHub API found a match in ${found[0]!.issue.repo} but could not ` +
        'confirm it is unique because one or more other configured repositories could not be checked without authentication; ' +
        'set repos.default or pass a repo-qualified argument',
      )
    }
  }
  return found[0]!.issue
}

async function issueProjectionStatus(
  factory: Factory,
  mount: MountClient,
  config: FactoryConfig,
): Promise<IssueResolution['projection']> {
  const status = await factoryStatusWithMountHealth(
    factory,
    mount,
    config.loop.heartbeatPath,
    config.loop.heartbeatStaleMs,
  )
  const githubConnection = mount.integrationConnections
    ? await mount.integrationConnections.getStatus('github')
    : undefined
  return {
    outcome: 'no-match',
    ...(status.localMountDegraded !== undefined ? { localMountDegraded: status.localMountDegraded } : {}),
    ...(status.localMountDegradedReason ? { localMountDegradedReason: status.localMountDegradedReason } : {}),
    ...(status.eventListener ? { eventListener: status.eventListener } : {}),
    ...(githubConnection ? { githubConnection } : {}),
  }
}

function projectionStatusFromMount(mount: MountClient): IssueResolution['projection'] {
  const health = mount.getLocalMountHealth?.()
  return {
    outcome: 'no-match',
    ...(health ? { localMountDegraded: health.degraded } : {}),
    ...(health?.reason ? { localMountDegradedReason: health.reason } : {}),
  }
}

function githubProjectionUnavailableReason(projection: IssueResolution['projection']): string | undefined {
  if (projection.githubConnection && !projection.githubConnection.ready) {
    const detail = [projection.githubConnection.state, projection.githubConnection.initialSyncState]
      .filter((value): value is string => Boolean(value))
      .join(', ')
    return `GitHub projection connection is not ready${detail ? ` (${detail})` : ''}`
  }
  if (projection.localMountDegraded) {
    return projection.localMountDegradedReason ?? 'local mount is degraded'
  }
  if (projection.eventListener && !['subscribed', 'polling'].includes(projection.eventListener.state)) {
    return projection.eventListener.reason ?? `event listener is ${projection.eventListener.state}`
  }
  return undefined
}

export type GithubIssueSelector = { number: number; repo?: string }

export function parseGithubIssueSelector(key: string, config: FactoryConfig): GithubIssueSelector {
  const qualified = key.match(/^([^#]+)#([1-9]\d*)$/u)
  const bare = key.match(/^#?([1-9]\d*)$/u)
  const number = Number(qualified?.[2] ?? bare?.[1])
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new Error(
      `${githubIssueResolutionError(config, key)}: expected a positive issue number or repo-qualified reference (repo#number)`,
    )
  }
  if (!qualified) return { number }

  const requested = qualified[1]!
  const configured = allConfiguredGithubIssueRepos(config)
  // Resolve what the caller typed through the exact same canonicalization
  // every configured route already goes through (label mapping, org
  // prefixing, canonical-route lookup) instead of a second, ad-hoc
  // expansion — three review rounds each found a different normalization
  // gap (default-only, org-only, bare-label-vs-normalized) in a hand-rolled
  // comparison here. Comparison is only ever normalized against normalized.
  const [resolved] = resolveGithubIssueRepoCandidates(config, [requested])
  const repo = resolved
    ? configured.find((candidate) => candidate.toLowerCase() === resolved.toLowerCase())
    : undefined
  if (!repo) {
    throw new Error(
      `${githubIssueResolutionError(config, key)}: repository ${requested} is not one of the configured Factory routes`,
    )
  }
  return { number, repo }
}

function resolveGithubIssueRepoCandidates(config: FactoryConfig, candidates: string[]): string[] {
  const repos = new Map<string, string>()
  const routedRepos = [
    ...Object.values(config.repos.byLabel),
    ...Object.values(config.repos.byProject),
    ...config.repos.keywordRules.map((rule) => rule.repo),
  ]
  for (const candidate of candidates) {
    const mapped = Object.entries(config.repos.byLabel).find(([label]) =>
      label.toLowerCase() === candidate.toLowerCase(),
    )?.[1]
    const canonicalRoute = routedRepos.find((route) =>
      route.includes('/') && route.toLowerCase() === candidate.toLowerCase(),
    )
    const repo = candidate.includes('/')
      ? canonicalRoute ?? candidate
      : mapped?.includes('/')
        ? mapped
        : config.repos.org ? `${config.repos.org}/${mapped ?? candidate}` : undefined
    if (!repo || !/^[^/]+\/[^/]+$/u.test(repo)) continue
    repos.set(repo.toLowerCase(), repo)
  }
  return [...repos.values()]
}

/** Bare-number resolution stays default-only: when repos.default is set, a
 *  bare issue number resolves against that single repo, not every route. */
function configuredGithubIssueRepos(config: FactoryConfig): string[] {
  const candidates = config.repos.default
    ? [config.repos.default]
    : [
        ...Object.values(config.repos.byLabel),
        ...Object.values(config.repos.byProject),
        ...config.repos.keywordRules.map((rule) => rule.repo),
      ]
  return resolveGithubIssueRepoCandidates(config, candidates)
}

/** Qualified `repo#number` selectors must validate against every configured
 *  route, not just repos.default — a route reachable only through byLabel,
 *  byProject, or keywordRules is still a valid dispatch target. */
function allConfiguredGithubIssueRepos(config: FactoryConfig): string[] {
  const candidates = [
    ...(config.repos.default ? [config.repos.default] : []),
    ...Object.values(config.repos.byLabel),
    ...Object.values(config.repos.byProject),
    ...config.repos.keywordRules.map((rule) => rule.repo),
  ]
  return resolveGithubIssueRepoCandidates(config, candidates)
}

function hasConfiguredGithubIssueRoutes(config: FactoryConfig): boolean {
  return Boolean(
    config.repos.default ||
    Object.keys(config.repos.byLabel).length > 0 ||
    Object.keys(config.repos.byProject).length > 0 ||
    config.repos.keywordRules.length > 0,
  )
}

async function findDirectGithubIssuePath(
  mount: MountClient,
  repo: string,
  number: number,
): Promise<string | undefined> {
  for (const path of githubIssueDirectPaths(repo, number)) {
    try {
      await mount.readFile(path)
      return path
    } catch (error) {
      if (!isMountFileNotFound(error)) {
        const message = error instanceof Error ? error.message : String(error)
        throw new Error(`Unable to read GitHub issue candidate ${path}: ${message}`, { cause: error })
      }
      // Direct reads are an optimization. A scoped tree scan below handles
      // slugged directories and older mount shapes when these paths are absent.
    }
  }
  return undefined
}

function isMountFileNotFound(error: unknown): boolean {
  const record = asRecord(error)
  const response = asRecord(record.response)
  const status = record.status ?? record.statusCode ?? response.status ?? response.statusCode
  const code = typeof record.code === 'string' ? record.code.toLowerCase() : undefined
  const message = error instanceof Error ? error.message : String(error)
  return status === 404 || status === '404' ||
    code === 'not_found' || code === 'file_not_found' ||
    /(?:file\s+not\s+found|\b404\b)/iu.test(message)
}

function githubIssueDirectPaths(repo: string, number: number): string[] {
  const [owner, name] = repo.split('/') as [string, string]
  const roots = [
    `/github/repos/${owner}__${name}/issues`,
    `/github/repos/${owner}/${name}/issues`,
  ]
  return [
    ...roots.map((root) => `${root}/${number}/meta.json`),
    ...roots.map((root) => `${root}/by-id/${number}.json`),
    ...roots.map((root) => `${root}/${number}.json`),
  ]
}

function githubIssueRoots(repo: string): string[] {
  const [owner, name] = repo.split('/') as [string, string]
  return [
    `/github/repos/${owner}__${name}/issues`,
    `/github/repos/${owner}/${name}/issues`,
  ]
}

function githubIssueResolutionError(config: FactoryConfig, key: string): string {
  const detected = autoDetectedIssueSources.has(config) ? ', auto-detected' : ''
  return `Unable to resolve GitHub issue ${key} (issueSource=github${detected})`
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

async function safeEnvironmentReap(
  reap: typeof reapFactoryEnvironmentsOnce = reapFactoryEnvironmentsOnce,
): Promise<Awaited<ReturnType<typeof reapFactoryEnvironmentsOnce>> | {
  reaped: never[]
  retained: never[]
  error: string
}> {
  try {
    return await reap()
  } catch (error) {
    // Process cleanup remains useful on hosts without kubectl or an active
    // cluster. Surface the skipped backstop without failing the whole command.
    return {
      reaped: [],
      retained: [],
      error: error instanceof Error ? error.message : String(error),
    }
  }
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
  return value === 'init' ||
    value === 'start' ||
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
  init [owner/repo]     Set up this checkout for GitHub-native issue dispatch
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
  featuremap check      Validate .agentworkforce/features/manifest.yaml
  intake notion <file>  Normalize mounted Notion specs into Factory work
  fleet <command>       Low-level fleet commands: spawn, roster, release

Options:
  --workspace <id>      Relay workspace to use with init (otherwise active workspace)
  --config <path>       Factory config JSON path (default: ./factory.config.json)
  --dry-run             Discover and triage without writes or agent spawns
  --backend <backend>   Fleet backend: internal or relay
  --agent-exit-timeout <ms>
                        Max owned-broker wait for task-exit agents (default: 1800000;
                        env: FACTORY_AGENT_EXIT_TIMEOUT_MS)
  -V, --version         Show the installed Factory version
  -h, --help            Show this help
`
}

function isHelpFlag(arg: string): boolean {
  return arg === '-h' || arg === '--help'
}

function isVersionFlag(arg: string): boolean {
  return arg === '-V' || arg === '--version'
}

async function readFactoryVersion(): Promise<string> {
  const manifest = JSON.parse(
    await readFile(new URL('../../package.json', import.meta.url), 'utf8'),
  ) as { version?: unknown }
  if (typeof manifest.version !== 'string' || manifest.version.length === 0) {
    throw new Error('Factory package version is missing')
  }
  return manifest.version
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const code = await runFleetCli(argv)
  process.exitCode = code
}
