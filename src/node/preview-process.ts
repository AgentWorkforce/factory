import { createHash } from 'node:crypto'
import { spawn } from 'node:child_process'

import {
  findAgentProcessByName,
  readProcessIdentity,
  type AgentProcessFinder,
  type ProcessIdentity,
} from '../orchestrator/process-identity'
import {
  terminatePids,
  type TerminatePidsOptions,
  type TerminatePidsReport,
} from '../orchestrator/reaper'

const DEFAULT_IDENTITY_TIMEOUT_MS = 5_000
const DEFAULT_IDENTITY_POLL_INTERVAL_MS = 25
const PREVIEW_CWD_ARGUMENT = '--factory-preview-cwd'
const PREVIEW_COMMAND_ARGUMENT = '--factory-preview-command'

// Keep the wrapper process alive while its configured command runs so the
// stable, Factory-owned marker remains visible in the wrapper's command line.
// The command itself runs in a child shell and receives termination through the
// ordinary process-tree teardown as well as this forwarding trap.
const PREVIEW_PROCESS_WRAPPER = [
  'child=',
  'forward_signal() {',
  '  if [ -n "$child" ]; then',
  '    kill -TERM "$child" 2>/dev/null || true',
  '  fi',
  '}',
  'trap forward_signal TERM INT HUP',
  '/bin/sh -c "$6" &',
  'child=$!',
  'wait "$child"',
  'status=$?',
  'exit "$status"',
].join('\n')

export interface PreviewProcessStartInput {
  /** Provider preview ID; its digest becomes the deterministic recovery marker. */
  id: string
  command: string
  cwd: string
  port: number
}

export interface PreviewProcessIdentity {
  pid: number
  startTime: string
  cmdline: string
  cwd: string
  marker: string
}

export interface PreviewProcessSpawnOptions {
  cwd: string
  detached: true
  env: NodeJS.ProcessEnv
  stdio: 'ignore'
}

export interface PreviewProcessChild {
  pid?: number
  unref(): void
}

export type PreviewProcessSpawner = (
  file: string,
  args: string[],
  options: PreviewProcessSpawnOptions,
) => PreviewProcessChild

export interface PreviewProcessSupervisorOptions {
  shell?: string
  spawn?: PreviewProcessSpawner
  findProcess?: AgentProcessFinder
  readProcessIdentity?: (pid: number) => Promise<ProcessIdentity | undefined>
  terminatePids?: (pids: number[], options?: TerminatePidsOptions) => Promise<TerminatePidsReport>
  terminateOptions?: TerminatePidsOptions
  sleep?: (ms: number) => Promise<void>
  identityTimeoutMs?: number
  identityPollIntervalMs?: number
  env?: NodeJS.ProcessEnv
}

/**
 * Owns repository preview commands independently of an agent PTY or Factory
 * process. The persisted identity is exact enough to refuse recycled PIDs, and
 * its deterministic marker lets startup recovery find the wrapper after a
 * spawn/persist crash gap.
 */
export class PreviewProcessSupervisor {
  readonly #shell: string
  readonly #spawn: PreviewProcessSpawner
  readonly #findProcess: AgentProcessFinder
  readonly #readProcessIdentity: (pid: number) => Promise<ProcessIdentity | undefined>
  readonly #terminatePids: (pids: number[], options?: TerminatePidsOptions) => Promise<TerminatePidsReport>
  readonly #terminateOptions: TerminatePidsOptions
  readonly #sleep: (ms: number) => Promise<void>
  readonly #identityTimeoutMs: number
  readonly #identityPollIntervalMs: number
  readonly #env: NodeJS.ProcessEnv

  constructor(options: PreviewProcessSupervisorOptions = {}) {
    this.#shell = options.shell ?? '/bin/sh'
    this.#spawn = options.spawn ?? ((file, args, spawnOptions) =>
      spawn(file, args, spawnOptions) as PreviewProcessChild)
    this.#readProcessIdentity = options.readProcessIdentity ?? readProcessIdentity
    this.#findProcess = options.findProcess ?? ((marker, findOptions) => findAgentProcessByName(marker, {
      ...findOptions,
      readProcessIdentity: this.#readProcessIdentity,
    }))
    this.#terminatePids = options.terminatePids ?? terminatePids
    this.#terminateOptions = options.terminateOptions ?? {}
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)))
    this.#identityTimeoutMs = options.identityTimeoutMs ?? DEFAULT_IDENTITY_TIMEOUT_MS
    this.#identityPollIntervalMs = options.identityPollIntervalMs ?? DEFAULT_IDENTITY_POLL_INTERVAL_MS
    this.#env = options.env ?? process.env
  }

  async start(input: PreviewProcessStartInput): Promise<PreviewProcessIdentity> {
    validateStartInput(input)
    const marker = previewProcessMarker(input.id)
    const cwdToken = encodeArgument(input.cwd)
    const commandToken = digest(input.command)
    const existing = await this.find(input.id)
    if (existing) {
      if (
        existing.cwd !== input.cwd ||
        !commandTokenMatches(existing.cmdline, commandToken)
      ) {
        throw new Error(`Preview process marker ${marker} is already owned by a different command or checkout`)
      }
      return existing
    }

    const child = this.#spawn(this.#shell, [
      '-c',
      PREVIEW_PROCESS_WRAPPER,
      '--agent-name',
      marker,
      PREVIEW_CWD_ARGUMENT,
      cwdToken,
      PREVIEW_COMMAND_ARGUMENT,
      commandToken,
      input.command,
    ], {
      cwd: input.cwd,
      detached: true,
      env: { ...this.#env, PORT: String(input.port) },
      stdio: 'ignore',
    })
    const pid = child.pid
    if (!Number.isInteger(pid) || (pid ?? 0) <= 0) {
      throw new Error(`Unable to start preview process ${marker}: detached shell returned no PID`)
    }
    child.unref()

    const identity = await this.#awaitIdentity(pid!, marker, input.cwd, commandToken)
    if (!identity) {
      throw new Error(
        `Preview process ${marker} started as PID ${pid} but its exact identity was not observable; recover it with find(${JSON.stringify(input.id)})`,
      )
    }
    return identity
  }

  async isRunning(identity: PreviewProcessIdentity): Promise<boolean> {
    if (!validPersistedIdentity(identity)) return false
    const current = await this.#readProcessIdentity(identity.pid)
    return Boolean(current && processIdentityMatches(identity, current))
  }

  async find(id: string): Promise<PreviewProcessIdentity | undefined> {
    const marker = previewProcessMarker(id)
    const resolution = await this.#findProcess(marker)
    if (resolution.status === 'missing') return undefined
    if (resolution.status === 'ambiguous') {
      throw new Error(`Preview process marker ${marker} matched multiple process trees`)
    }
    const cwd = cwdFromCommandLine(resolution.identity.cmdline)
    if (!cwd || !markerMatches(resolution.identity.cmdline, marker)) {
      throw new Error(`Preview process marker ${marker} resolved to an invalid wrapper identity`)
    }
    return {
      ...resolution.identity,
      cwd,
      marker,
    }
  }

  async stop(identity: PreviewProcessIdentity): Promise<boolean> {
    if (!validPersistedIdentity(identity)) return false
    const current = await this.#readProcessIdentity(identity.pid)
    // Already absent is an idempotent success. A live but changed identity is
    // a recycled PID and must never be signalled.
    if (!current) return true
    if (!processIdentityMatches(identity, current)) {
      return false
    }

    const report = await this.#terminatePids([identity.pid], this.#terminateOptions)
    if (report.terminated.some((entry) => entry.pid === identity.pid)) {
      return true
    }
    // The wrapper can exit naturally between the identity check and the tree
    // terminator's liveness probe. Count only confirmed absence as success; a
    // recycled or changed identity remains a fail-closed false result.
    return (await this.#readProcessIdentity(identity.pid)) === undefined
  }

  async #awaitIdentity(
    pid: number,
    marker: string,
    cwd: string,
    commandToken: string,
  ): Promise<PreviewProcessIdentity | undefined> {
    const interval = Math.max(1, this.#identityPollIntervalMs)
    const attempts = Math.max(1, Math.ceil(Math.max(0, this.#identityTimeoutMs) / interval) + 1)
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const current = await this.#readProcessIdentity(pid)
      if (
        current &&
        markerMatches(current.cmdline, marker) &&
        cwdFromCommandLine(current.cmdline) === cwd &&
        commandTokenMatches(current.cmdline, commandToken)
      ) {
        return { ...current, cwd, marker }
      }
      if (attempt + 1 < attempts) await this.#sleep(interval)
    }
    return undefined
  }
}

export function previewProcessMarker(id: string): string {
  const normalized = id.trim()
  if (!normalized) throw new Error('Preview process id must be non-empty')
  return `factory-preview-${digest(normalized).slice(0, 32)}`
}

function validateStartInput(input: PreviewProcessStartInput): void {
  previewProcessMarker(input.id)
  if (!input.command.trim()) throw new Error('Preview process command must be non-empty')
  if (!input.cwd.trim()) throw new Error('Preview process cwd must be non-empty')
  if (!Number.isInteger(input.port) || input.port < 1 || input.port > 65_535) {
    throw new Error('Preview process port must be an integer between 1 and 65535')
  }
}

function validPersistedIdentity(identity: PreviewProcessIdentity): boolean {
  return Number.isInteger(identity.pid) &&
    identity.pid > 0 &&
    Boolean(identity.startTime) &&
    Boolean(identity.cmdline) &&
    Boolean(identity.cwd) &&
    /^factory-preview-[a-f0-9]{32}$/u.test(identity.marker) &&
    markerMatches(identity.cmdline, identity.marker) &&
    cwdFromCommandLine(identity.cmdline) === identity.cwd
}

function processIdentityMatches(expected: PreviewProcessIdentity, current: ProcessIdentity): boolean {
  return current.pid === expected.pid &&
    current.startTime === expected.startTime &&
    current.cmdline === expected.cmdline &&
    markerMatches(current.cmdline, expected.marker) &&
    cwdFromCommandLine(current.cmdline) === expected.cwd
}

function markerMatches(cmdline: string, marker: string): boolean {
  const escaped = escapeRegExp(marker)
  return new RegExp(`(^|\\s)--agent-name(\\s+|=)${escaped}(\\s|$)`, 'u').test(cmdline)
}

function commandTokenMatches(cmdline: string, token: string): boolean {
  return commandLineArgument(cmdline, PREVIEW_COMMAND_ARGUMENT) === token
}

function cwdFromCommandLine(cmdline: string): string | undefined {
  const encoded = commandLineArgument(cmdline, PREVIEW_CWD_ARGUMENT)
  if (!encoded) return undefined
  try {
    return Buffer.from(encoded, 'base64url').toString('utf8') || undefined
  } catch {
    return undefined
  }
}

function commandLineArgument(cmdline: string, name: string): string | undefined {
  const escaped = escapeRegExp(name)
  return new RegExp(`(?:^|\\s)${escaped}(?:\\s+|=)([^\\s]+)(?:\\s|$)`, 'u').exec(cmdline)?.[1]
}

function encodeArgument(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url')
}

function digest(value: string): string {
  return createHash('sha256').update(value).digest('hex')
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
}
