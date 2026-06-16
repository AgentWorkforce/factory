import { existsSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'

import { ensureLocalMount } from '../mount/local-mount-preflight'
import {
  FactoryConfigSchema,
  RelayfileCloudMountClient,
  checkFactoryLoopLiveness,
  closeProbePr,
  createFactory,
  createFleet,
  defaultGhRunner,
  isInFactoryScope,
  parseLinearIssue,
  reapFactoryOrphansOnce,
  readFactoryLoopHeartbeat,
  type Capability,
  type Factory,
  type FactoryConfig,
  type FleetBackend,
  type FleetClient,
  type GhRunner,
  type MountClient,
  type ProbeCloser,
  type RelayfileCloudMountClientConfig,
} from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'

interface FleetCliDeps {
  fleet?: FleetClient
  mount?: MountClient
  createFactory?: typeof createFactory
  createFleet?: typeof createFleet
  cloudMountFromConfig?: (config?: RelayfileCloudMountClientConfig) => Promise<MountClient>
  ensureLocalMount?: (workspaceId: string, startDir: string) => Promise<void>
  waitForStopSignal?: () => Promise<number | void>
  stdout?: Pick<NodeJS.WriteStream, 'write'>
  stderr?: Pick<NodeJS.WriteStream, 'write'>
  probeCloser?: ProbeCloser
  probePrGhRunner?: GhRunner
  now?: () => number
  stopSignalProcessLike?: Pick<NodeJS.Process, 'once' | 'off'>
  daemonExit?: (code: number) => void
  flushDaemonOutput?: () => Promise<void>
}

interface GlobalOptions {
  backend: FleetBackend
  config?: string
  dryRun: boolean
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
  | { kind: 'factory-triage'; issue: string }
  | { kind: 'factory-dispatch'; issue: string }
  | { kind: 'factory-close-probe'; prNumber: number; repo: string; issue: string }

export async function runFleetCli(argv: string[], deps: FleetCliDeps = {}): Promise<number> {
  const out = deps.stdout ?? process.stdout
  const err = deps.stderr ?? process.stderr
  let fleet: FleetClient | undefined

  try {
    const { globals, args } = parseGlobalOptions(argv)
    const command = parseFleetCommand(args)

    if (command.kind === 'factory-close-probe') {
      // Manual close-probe remains strict; the daemon relaxes the title marker only after issue-synthetic classification.
      const result = await (deps.probeCloser ?? closeProbePr)({
        repo: command.repo,
        prNumber: command.prNumber,
        expectedIssueKey: command.issue,
      })
      writeJson(out, result)
      return 0
    }

    const loaded = command.kind.startsWith('factory') ? await loadConfig(globals.config) : undefined
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
      case 'factory-triage':
      case 'factory-dispatch': {
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
        const mount = await buildMount(loaded, deps)
        const factory = (deps.createFactory ?? createFactory)(loaded.config, {
          mount,
          fleet,
          probePrGhRunner: deps.probePrGhRunner ?? defaultGhRunner,
        })
        return await runFactoryCommand(command, factory, mount, fleet, loaded.config, globals, out, deps)
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

  if (verb === 'spawn') {
    const [capability, ...flags] = rest
    if (!isCapability(capability)) {
      throw new Error('fleet spawn requires capability spawn:codex or spawn:claude')
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
    if (!name) throw new Error('fleet release requires agent name')
    return { kind: 'release', name, reason: parseFlags(flags).reason }
  }

  if (verb === 'factory') {
    return parseFactoryCommand(rest)
  }

  throw new Error(`Unknown fleet command: ${verb}`)
}

export function parseGlobalOptions(argv: string[]): { globals: GlobalOptions; args: string[] } {
  const args: string[] = []
  const globals: GlobalOptions = { backend: 'internal', dryRun: false }
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (arg === '--backend') {
      const backend = argv[++index]
      if (backend !== 'internal' && backend !== 'relay') throw new Error(`Invalid --backend ${backend ?? ''}`)
      globals.backend = backend
    } else if (arg === '--config') {
      globals.config = requireValue(argv, ++index, '--config')
    } else if (arg === '--dry-run') {
      globals.dryRun = true
    } else {
      args.push(arg)
    }
  }
  return { globals, args }
}

async function runFactoryCommand(
  command: Extract<ParsedCommand, { kind: 'factory' | 'factory-triage' | 'factory-dispatch' }>,
  factory: Factory,
  mount: MountClient,
  fleet: FleetClient,
  config: FactoryConfig,
  globals: GlobalOptions,
  out: Pick<NodeJS.WriteStream, 'write'>,
  deps: FleetCliDeps = {},
): Promise<number> {
  if (command.kind === 'factory') {
    if (command.action === 'start') {
      await (deps.ensureLocalMount ?? ensureLocalMount)(config.workspaceId, process.cwd())
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

  const issue = await readIssueArg(mount, command.issue)
  const decision = await factory.triageIssue(issue)
  if (command.kind === 'factory-triage') {
    writeJson(out, decision)
    return 0
  }

  writeJson(out, await factory.dispatch(decision, { dryRun: globals.dryRun }))
  return 0
}

function parseFactoryCommand(args: string[]): ParsedCommand {
  const [action, issueOrPr, ...flags] = args
  if (action === 'start') {
    return { kind: 'factory', action, ...parseFactoryStartFlags([issueOrPr, ...flags]) }
  }
  if (action === 'run-once' || action === 'loop' || action === 'status' || action === 'loop-status' || action === 'kill-loop' || action === 'reap-orphans') {
    return { kind: 'factory', action }
  }
  if (action === 'triage') {
    if (!issueOrPr) throw new Error('fleet factory triage requires an issue key or path')
    return { kind: 'factory-triage', issue: issueOrPr }
  }
  if (action === 'dispatch') {
    if (!issueOrPr) throw new Error('fleet factory dispatch requires an issue key or path')
    return { kind: 'factory-dispatch', issue: issueOrPr }
  }
  if (action === 'close-probe') {
    const prNumber = Number(issueOrPr)
    if (!Number.isInteger(prNumber) || prNumber <= 0) throw new Error('fleet factory close-probe requires a PR number')
    const parsed = parseFlags(flags)
    if (!parsed.repo || !parsed.issue) throw new Error('fleet factory close-probe requires --repo <owner/repo> --issue <KEY>')
    return { kind: 'factory-close-probe', prNumber, repo: parsed.repo, issue: parsed.issue }
  }
  throw new Error(`Unknown fleet factory action: ${action ?? ''}`)
}

function parseFactoryStartFlags(args: Array<string | undefined>): { mode?: 'live' } {
  let mode: 'live' | undefined
  const flags = args.filter((arg): arg is string => Boolean(arg))
  for (let index = 0; index < flags.length; index += 1) {
    const flag = flags[index]
    if (flag === '--mode') {
      const value = requireValue(flags, ++index, '--mode')
      if (value !== 'live') throw new Error(`Invalid factory start mode: ${value}`)
      mode = value
      continue
    }
    throw new Error(`Unknown fleet factory start option: ${flag}`)
  }
  return { mode }
}

async function loadConfig(path?: string): Promise<LoadedConfig> {
  if (!path) throw new Error('factory commands require --config <path>')
  const raw = JSON.parse(await readFile(path, 'utf8')) as unknown
  const record = asRecord(raw)
  return {
    config: FactoryConfigSchema.parse(record.factoryConfig ?? record),
    fixtureFiles: record.fixtureFiles ? asRecord(record.fixtureFiles) : undefined,
  }
}

async function buildFleet(globals: GlobalOptions, loaded: LoadedConfig | undefined, deps: FleetCliDeps): Promise<FleetClient> {
  if (deps.fleet) return deps.fleet
  if (globals.backend === 'internal' && hasExplicitFixtureFiles(loaded)) return new FakeFleetClient()
  return (deps.createFleet ?? createFleet)({
    backend: globals.backend,
    cwd: process.cwd(),
    connectionPath: resolveBrokerConnectionPath(process.cwd()),
  })
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
      const issue = parseLinearIssue(issuePath, (await mount.readFile(issuePath)).content)
      return isInFactoryScope(issue, config.safety)
    } catch {
      return false
    }
  }

  if (path.startsWith('/linear/issues/')) {
    if (isInFactoryScope(scopeIssueFromDraftContent(content), config.safety)) return true
    try {
      const issue = parseLinearIssue(path, (await mount.readFile(path)).content)
      return isInFactoryScope(issue, config.safety)
    } catch {
      return false
    }
  }

  if (/^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)) {
    return true
  }

  return false
}

const scopeIssueFromDraftContent = (content: unknown) => ({
  title: typeof asRecord(content)?.title === 'string' ? asRecord(content)?.title as string : '',
  team: typeof asRecord(asRecord(content)?.team)?.key === 'string'
    ? asRecord(asRecord(content)?.team)?.key as string
    : undefined,
  raw: asRecord(content),
})

async function readIssueArg(mount: MountClient, issueArg: string) {
  const path = issueArg.startsWith('/') ? issueArg : await findIssuePath(mount, issueArg)
  const { content } = await mount.readFile(path)
  return parseLinearIssue(path, content)
}

async function findIssuePath(mount: MountClient, key: string): Promise<string> {
  const matches = (await mount.listTree('/linear/issues/'))
    .filter((path) => path.startsWith(`/linear/issues/${key}__`) || path === `/linear/issues/${key}.json`)
  if (matches.length !== 1) {
    throw new Error(`Unable to resolve issue ${key}: found ${matches.length} matches`)
  }
  return matches[0]
}

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
  return value === 'spawn:codex' || value === 'spawn:claude'
}

function defaultAgentName(capability: Capability, now: number): string {
  return `fleet-${capability.replace('spawn:', '')}-${now}`
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
  return 'usage: fleet <spawn|roster|ls|release|factory> [options]'
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const code = await runFleetCli(argv)
  process.exitCode = code
}
