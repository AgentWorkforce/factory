import { readFile } from 'node:fs/promises'

import {
  FactoryConfigSchema,
  RelayfileCloudMountClient,
  closeProbePr,
  createFactory,
  createFleet,
  parseLinearIssue,
  type Capability,
  type Factory,
  type FactoryConfig,
  type FleetBackend,
  type FleetClient,
  type MountClient,
  type ProbeCloser,
} from '../index'
import { FakeFleetClient, FakeMountClient } from '../testing'

interface FleetCliDeps {
  fleet?: FleetClient
  mount?: MountClient
  stdout?: Pick<NodeJS.WriteStream, 'write'>
  stderr?: Pick<NodeJS.WriteStream, 'write'>
  probeCloser?: ProbeCloser
  now?: () => number
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
  | { kind: 'factory'; action: 'run-once' | 'loop' | 'status' }
  | { kind: 'factory-triage'; issue: string }
  | { kind: 'factory-dispatch'; issue: string }
  | { kind: 'factory-close-probe'; prNumber: number; repo: string; issue: string }

export async function runFleetCli(argv: string[], deps: FleetCliDeps = {}): Promise<number> {
  const out = deps.stdout ?? process.stdout
  const err = deps.stderr ?? process.stderr

  try {
    const { globals, args } = parseGlobalOptions(argv)
    const command = parseFleetCommand(args)

    if (command.kind === 'factory-close-probe') {
      const result = await (deps.probeCloser ?? closeProbePr)({
        repo: command.repo,
        prNumber: command.prNumber,
        expectedIssueKey: command.issue,
      })
      writeJson(out, result)
      return 0
    }

    const loaded = command.kind.startsWith('factory') ? await loadConfig(globals.config) : undefined
    const fleet = await buildFleet(globals, loaded, deps)

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
        const mount = await buildMount(loaded, deps)
        const factory = createFactory(loaded.config, { mount, fleet })
        return await runFactoryCommand(command, factory, mount, globals, out)
      }
    }
  } catch (error) {
    err.write(`${error instanceof Error ? error.message : String(error)}\n`)
    return 1
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
  globals: GlobalOptions,
  out: Pick<NodeJS.WriteStream, 'write'>,
): Promise<number> {
  if (command.kind === 'factory') {
    if (command.action === 'run-once') {
      writeJson(out, await factory.runOnce({ dryRun: globals.dryRun }))
      return 0
    }
    if (command.action === 'status') {
      writeJson(out, factory.status())
      return 0
    }

    await factory.start()
    writeJson(out, factory.status())
    await waitForShutdown(factory)
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
  if (action === 'run-once' || action === 'loop' || action === 'status') {
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

async function loadConfig(path?: string): Promise<LoadedConfig> {
  if (!path) throw new Error('factory commands require --config <path>')
  const raw = JSON.parse(await readFile(path, 'utf8')) as unknown
  const record = asRecord(raw)
  return {
    config: FactoryConfigSchema.parse(record.factoryConfig ?? record),
    fixtureFiles: asRecord(record.fixtureFiles),
  }
}

async function buildFleet(globals: GlobalOptions, loaded: LoadedConfig | undefined, deps: FleetCliDeps): Promise<FleetClient> {
  if (deps.fleet) return deps.fleet
  if (globals.backend === 'internal' && loaded?.fixtureFiles) return new FakeFleetClient()
  return createFleet({ backend: globals.backend, cwd: process.cwd() })
}

async function buildMount(loaded: LoadedConfig, deps: FleetCliDeps): Promise<MountClient> {
  if (deps.mount) return deps.mount
  if (loaded.fixtureFiles) return new FakeMountClient(loaded.fixtureFiles)
  return RelayfileCloudMountClient.fromConfig({ workspaceId: loaded.config.workspaceId })
}

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

function isCapability(value: string | undefined): value is Capability {
  return value === 'spawn:codex' || value === 'spawn:claude'
}

function defaultAgentName(capability: Capability, now: number): string {
  return `fleet-${capability.replace('spawn:', '')}-${now}`
}

function waitForShutdown(factory: Factory): Promise<void> {
  return new Promise((resolve) => {
    const stop = async () => {
      process.off('SIGINT', stop)
      process.off('SIGTERM', stop)
      await factory.stop()
      resolve()
    }
    process.once('SIGINT', stop)
    process.once('SIGTERM', stop)
  })
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
