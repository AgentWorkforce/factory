import { randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { get } from 'node:http'
import { createServer } from 'node:net'
import { dirname } from 'node:path'
import { promisify } from 'node:util'
import lockfile from 'proper-lockfile'

import type { PreviewConfig } from '../config/schema'
import type {
  PreviewReference,
  PreviewStartInput,
  PreviewSweepInput,
  PreviewSweepResult,
} from '../ports/fleet'
import {
  PreviewProcessSupervisor,
  type PreviewProcessIdentity,
} from './preview-process'

const execFileAsync = promisify(execFile)
const REGISTRY_VERSION = 1
const MANAGED_BY = '@agent-relay/factory'
const PREVIEW_LOCK_STALE_MS = 30_000
const PREVIEW_PROVIDER_COMMAND_TIMEOUT_MS = 30_000
const PREVIEW_READY_TIMEOUT_MS = 60_000
const PREVIEW_READY_POLL_INTERVAL_MS = 250

type PreviewIdentity = Omit<PreviewReference, 'url' | 'process'> & {
  managedBy: typeof MANAGED_BY
  checkoutPath?: string
  process?: PreviewProcessIdentity
}
type PendingPreview = PreviewIdentity & { state: 'pending' }
type PersistedPreview = PreviewIdentity & {
  state: 'active'
  url: string
}
type RegistryPreview = PendingPreview | PersistedPreview
type PreviewRegistry = { version: typeof REGISTRY_VERSION; previews: RegistryPreview[] }
type CommandResult = { stdout: string; stderr: string }
export type PreviewCommandRunner = (file: string, args: string[]) => Promise<CommandResult>
export type PreviewPortProbe = (port: number) => Promise<boolean>
export type PreviewReadyProbe = (port: number) => Promise<boolean>

export interface PreviewManager {
  start(input: PreviewStartInput): Promise<PreviewReference>
  remove(preview: PreviewReference): Promise<boolean>
  sweep(input: PreviewSweepInput): Promise<PreviewSweepResult>
}

export interface TailscalePreviewManagerOptions {
  config: PreviewConfig
  run?: PreviewCommandRunner
  isPortAvailable?: PreviewPortProbe
  isReady?: PreviewReadyProbe
  processSupervisor?: Pick<PreviewProcessSupervisor, 'start' | 'isRunning' | 'find' | 'stop'>
  sleep?: (ms: number) => Promise<void>
  readyTimeoutMs?: number
  readyPollIntervalMs?: number
  now?: () => Date
}

/**
 * Thin lifecycle adapter around Tailscale Serve. `tailscaled` owns background
 * proxying, while this registry records the exact routes Factory may remove.
 * Every destructive operation checks the live Serve target before issuing
 * `off`, so a stale registry can never turn off a port repurposed by a human.
 */
export class TailscalePreviewManager implements PreviewManager {
  readonly #config: PreviewConfig
  readonly #run: PreviewCommandRunner
  readonly #isPortAvailable: PreviewPortProbe
  readonly #isReady: PreviewReadyProbe
  readonly #processes: Pick<PreviewProcessSupervisor, 'start' | 'isRunning' | 'find' | 'stop'>
  readonly #sleep: (ms: number) => Promise<void>
  readonly #readyTimeoutMs: number
  readonly #readyPollIntervalMs: number
  readonly #now: () => Date
  #operation: Promise<unknown> = Promise.resolve()

  constructor(options: TailscalePreviewManagerOptions) {
    this.#config = options.config
    this.#run = options.run ?? (async (file, args) => {
      const result = await execFileAsync(file, args, {
        encoding: 'utf8',
        timeout: PREVIEW_PROVIDER_COMMAND_TIMEOUT_MS,
      })
      return { stdout: result.stdout, stderr: result.stderr }
    })
    this.#isPortAvailable = options.isPortAvailable ?? portAvailable
    this.#isReady = options.isReady ?? httpReady
    this.#processes = options.processSupervisor ?? new PreviewProcessSupervisor()
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)))
    this.#readyTimeoutMs = options.readyTimeoutMs ?? PREVIEW_READY_TIMEOUT_MS
    this.#readyPollIntervalMs = options.readyPollIntervalMs ?? PREVIEW_READY_POLL_INTERVAL_MS
    this.#now = options.now ?? (() => new Date())
  }

  async start(input: PreviewStartInput): Promise<PreviewReference> {
    return await this.#exclusive(async () => {
      const registry = await this.#readRegistry()
      let status = await this.#serveStatus()
      const existing = registry.previews.find((preview) =>
        preview.namespace === input.namespace &&
        preview.owner === input.owner &&
        preview.service === input.service,
      )
      const existingMatchesRequest = existing &&
        existing.repo === input.repo &&
        (existing.configuredTargetPort ?? existing.targetPort) === input.targetPort &&
        (input.preferredHttpsPort === undefined || existing.httpsPort === input.preferredHttpsPort) &&
        existing.startCommand === input.startCommand &&
        (!existing.process || existing.process.cwd === input.checkoutPath)
      const existingRoute = existing
        ? liveRoute(status, existing.httpsPort, existing.targetPort)
        : undefined
      if (existingMatchesRequest && existingRoute && !existingRoute.funnel) {
        const process = await this.#ensureManagedProcess(existing, input.checkoutPath)
        await this.#awaitReady(existing.targetPort, process)
        const liveUrl = previewUrl(existingRoute.host, existing.httpsPort)
        const active = activatePreview({ ...existing, process }, liveUrl)
        if (
          existing.state === 'pending' ||
          existing.url !== liveUrl ||
          !sameProcess(existing.process, process)
        ) {
          registry.previews = registry.previews.map((preview) => preview.id === existing.id ? active : preview)
          await this.#writeRegistry(registry)
        }
        return referenceFrom(active)
      }
      if (existing) {
        // A changed service declaration replaces only the old route whose live
        // upstream still matches Factory's registry identity.
        if (liveRouteMatches(status, existing)) {
          await this.#disable(existing)
          status = await this.#serveStatus()
        }
        if (!await this.#stopManagedProcess(existing)) {
          throw new Error(`Unable to confirm teardown of replaced preview process ${existing.id}`)
        }
        registry.previews = registry.previews.filter((preview) => preview.id !== existing.id)
        await this.#writeRegistry(registry)
      }

      const serviceConfig = this.#config.services[input.service]
      const lastTargetPort = Math.min(65_535, input.targetPort + (serviceConfig?.portSpan ?? 100) - 1)
      const occupiedTargetPorts = new Set([
        ...registry.previews.map((preview) => preview.targetPort),
        ...liveUpstreamPorts(status),
      ])
      let targetPort: number | undefined
      for (const candidate of portRange(input.targetPort, lastTargetPort)) {
        if (occupiedTargetPorts.has(candidate)) continue
        if (await this.#isPortAvailable(candidate)) {
          targetPort = candidate
          break
        }
      }
      if (targetPort === undefined) {
        throw new Error(`Unable to allocate a local target port for ${input.service}`)
      }

      const occupied = new Set(registry.previews.map((preview) => preview.httpsPort))
      const candidates = input.preferredHttpsPort !== undefined
        ? [input.preferredHttpsPort]
        : portRange(this.#config.httpsPortRange[0], this.#config.httpsPortRange[1])
      let lastError: unknown
      for (const httpsPort of candidates) {
        if (occupied.has(httpsPort) || livePortConfigured(status, httpsPort)) continue
        const pending: PendingPreview = {
          id: randomUUID(),
          provider: 'tailscale-serve',
          namespace: input.namespace,
          owner: input.owner,
          service: input.service,
          repo: input.repo,
          configuredTargetPort: input.targetPort,
          targetPort,
          httpsPort,
          access: 'tailnet',
          lifetime: 'issue',
          createdAt: this.#now().toISOString(),
          startCommand: input.startCommand,
          checkoutPath: input.checkoutPath,
          ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
          managedBy: MANAGED_BY,
          state: 'pending',
        }
        registry.previews.push(pending)
        // Persist intent before starting either external resource. The
        // deterministic process marker closes the spawn/persist crash gap,
        // while the pending route identity closes the Serve API crash gap.
        await this.#writeRegistry(registry)
        let commandSucceeded = false
        try {
          const process = await this.#processes.start({
            id: pending.id,
            command: input.startCommand,
            cwd: input.checkoutPath,
            port: targetPort,
          })
          pending.process = process
          await this.#writeRegistry(registry)
          await this.#awaitReady(targetPort, process)
          await this.#run(this.#config.tailscaleBinary, [
            'serve',
            '--bg',
            '--yes',
            `--https=${httpsPort}`,
            '--set-path=/',
            `http://127.0.0.1:${targetPort}`,
          ])
          commandSucceeded = true
          const configured = await this.#serveStatus()
          const route = liveRoute(configured, httpsPort, targetPort)
          if (!route || route.funnel) {
            throw new Error(
              route?.funnel
                ? `tailscale serve reported public Funnel access on HTTPS port ${httpsPort}`
                : `tailscale serve did not report the configured route on HTTPS port ${httpsPort}`,
            )
          }
          const preview = activatePreview(pending, previewUrl(route.host, httpsPort))
          registry.previews = registry.previews.map((candidate) =>
            candidate.id === pending.id ? preview : candidate,
          )
          await this.#writeRegistry(registry)
          return referenceFrom(preview)
        } catch (error) {
          lastError = error
          let routeRollbackCompleted = false
          let processRollbackCompleted = false
          try {
            const cleanupStatus = await this.#serveStatus()
            if (liveRoute(cleanupStatus, httpsPort, targetPort)) {
              await this.#disable({ httpsPort })
              routeRollbackCompleted = true
            } else if (!commandSucceeded) {
              // A rejected command that left no exact route has no provider
              // mutation Factory can safely undo. Clear the pending intent;
              // genuinely ambiguous status failures retain it for sweeping.
              routeRollbackCompleted = true
            }
          } catch (cleanupError) {
            lastError = new AggregateError(
              [error, cleanupError],
              `Preview route setup and guarded rollback both failed on HTTPS port ${httpsPort}`,
            )
          }
          try {
            processRollbackCompleted = await this.#stopManagedProcess(pending)
          } catch (cleanupError) {
            lastError = new AggregateError(
              [lastError, cleanupError],
              `Preview setup and managed-process rollback both failed for ${pending.id}`,
            )
          }
          if (routeRollbackCompleted && processRollbackCompleted) {
            registry.previews = registry.previews.filter((candidate) => candidate.id !== pending.id)
            try {
              await this.#writeRegistry(registry)
            } catch (registryError) {
              lastError = new AggregateError(
                [lastError, registryError],
                `Preview route rollback succeeded but its pending registry entry could not be cleared on HTTPS port ${httpsPort}`,
              )
            }
          }
          // Status already selected a free port. A command or verification
          // failure is provider-wide, not a reason to hammer the whole range.
          break
        }
      }
      throw new Error(
        `Unable to allocate a Tailscale Serve HTTPS port for ${input.service}`,
        lastError === undefined ? undefined : { cause: lastError },
      )
    })
  }

  async remove(reference: PreviewReference): Promise<boolean> {
    return await this.#exclusive(async () => {
      const registry = await this.#readRegistry()
      const preview = registry.previews.find((candidate) =>
        candidate.managedBy === MANAGED_BY &&
        candidate.id === reference.id &&
        candidate.namespace === reference.namespace &&
        candidate.owner === reference.owner &&
        candidate.httpsPort === reference.httpsPort &&
        candidate.targetPort === reference.targetPort,
      )
      // Idempotent teardown: an absent ownership record means this exact
      // Factory route was already removed by a previous attempt or sweep.
      if (!preview) return true

      const status = await this.#serveStatus()
      if (!liveRouteMatches(status, preview)) {
        const processRemoved = await this.#stopManagedProcess(preview)
        if (livePortConfigured(status, preview.httpsPort)) {
          // The port definitively points at another route. Forget the stale
          // ownership record, but never disable the replacement.
          if (!processRemoved) return false
          registry.previews = registry.previews.filter((candidate) => candidate.id !== preview.id)
          await this.#writeRegistry(registry)
          return true
        }
        // An unconfigured port can be a transient status gap. Preserve the
        // identity so a later startup sweep can still remove a route that
        // becomes visible after this check.
        return false
      }
      await this.#disable(preview)
      if (!await this.#stopManagedProcess(preview)) return false
      registry.previews = registry.previews.filter((candidate) => candidate.id !== preview.id)
      await this.#writeRegistry(registry)
      return true
    })
  }

  async sweep(input: PreviewSweepInput): Promise<PreviewSweepResult> {
    return await this.#exclusive(async () => {
      const active = new Set(input.activeOwners)
      const activePreviewIds = input.activePreviewIds
        ? new Set(input.activePreviewIds)
        : undefined
      const registry = await this.#readRegistry()
      const status = await this.#serveStatus()
      const reaped: PreviewReference[] = []
      const skipped: PreviewSweepResult['skipped'] = []
      const retained: RegistryPreview[] = []

      for (const preview of registry.previews) {
        const authoritativeActive = active.has(preview.owner) &&
          (!activePreviewIds || activePreviewIds.has(preview.id))
        if (
          preview.managedBy !== MANAGED_BY ||
          preview.namespace !== input.namespace
        ) {
          retained.push(preview)
          continue
        }
        if (authoritativeActive) {
          try {
            const checkoutPath = preview.checkoutPath ?? preview.process?.cwd
            if (!checkoutPath) throw new Error('managed preview checkout is not recoverable')
            const process = await this.#ensureManagedProcess(preview, checkoutPath)
            await this.#awaitReady(preview.targetPort, process)
            retained.push({ ...preview, checkoutPath, process })
          } catch (error) {
            retained.push(preview)
            skipped.push({
              id: preview.id,
              reason: `active preview process recovery failed: ${error instanceof Error ? error.message : String(error)}`,
              node: preview.node,
            })
          }
          continue
        }
        const route = liveRoute(status, preview.httpsPort, preview.targetPort)
        if (!route) {
          const repurposed = livePortConfigured(status, preview.httpsPort)
          let processRemoved = false
          try {
            processRemoved = await this.#stopManagedProcess(preview)
          } catch (error) {
            skipped.push({
              id: preview.id,
              reason: `managed process teardown failed: ${error instanceof Error ? error.message : String(error)}`,
              node: preview.node,
            })
          }
          if (!repurposed || !processRemoved) retained.push(preview)
          skipped.push({
            id: preview.id,
            reason: repurposed
              ? 'live route identity mismatch'
              : 'live route is not currently observable; retained for retry',
            node: preview.node,
          })
          continue
        }
        try {
          await this.#disable(preview)
          if (!await this.#stopManagedProcess(preview)) {
            throw new Error('managed process identity could not be confirmed stopped')
          }
          reaped.push(referenceFrom(activatePreview(preview, previewUrl(route.host, preview.httpsPort))))
        } catch (error) {
          retained.push(preview)
          skipped.push({
            id: preview.id,
            reason: error instanceof Error ? error.message : String(error),
            node: preview.node,
          })
        }
      }

      registry.previews = retained
      await this.#writeRegistry(registry)
      return { reaped, skipped }
    })
  }

  async #awaitReady(port: number, process: PreviewProcessIdentity): Promise<void> {
    const interval = Math.max(1, this.#readyPollIntervalMs)
    const attempts = Math.max(1, Math.ceil(Math.max(0, this.#readyTimeoutMs) / interval) + 1)
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (!await this.#processes.isRunning(process)) {
        throw new Error(`Preview command exited before local HTTP port ${port} became ready`)
      }
      if (await this.#isReady(port)) return
      if (attempt + 1 < attempts) await this.#sleep(interval)
    }
    throw new Error(`Preview command did not make local HTTP port ${port} ready within ${this.#readyTimeoutMs}ms`)
  }

  async #stopManagedProcess(preview: Pick<RegistryPreview, 'id' | 'process'>): Promise<boolean> {
    if (preview.process && await this.#processes.stop(preview.process)) return true
    const recovered = await this.#processes.find(preview.id)
    if (!recovered) return true
    return await this.#processes.stop(recovered)
  }

  async #ensureManagedProcess(
    preview: Pick<RegistryPreview, 'id' | 'startCommand' | 'targetPort' | 'process'>,
    checkoutPath: string,
  ): Promise<PreviewProcessIdentity> {
    const recorded = preview.process && await this.#processes.isRunning(preview.process)
      ? preview.process
      : undefined
    const recovered = recorded ?? await this.#processes.find(preview.id)
    if (!recovered && !await this.#isPortAvailable(preview.targetPort)) {
      throw new Error(`Refusing to attach preview ${preview.id} to occupied unowned port ${preview.targetPort}`)
    }
    return await this.#processes.start({
      id: preview.id,
      command: preview.startCommand,
      cwd: checkoutPath,
      port: preview.targetPort,
    })
  }

  async #disable(preview: Pick<PreviewReference, 'httpsPort'>): Promise<void> {
    await this.#run(this.#config.tailscaleBinary, [
      'serve',
      '--yes',
      `--https=${preview.httpsPort}`,
      '--set-path=/',
      'off',
    ])
  }

  async #serveStatus(): Promise<unknown> {
    const result = await this.#run(this.#config.tailscaleBinary, ['serve', 'status', '--json'])
    try {
      return JSON.parse(result.stdout) as unknown
    } catch (error) {
      throw new Error('tailscale serve status returned invalid JSON', { cause: error })
    }
  }

  async #readRegistry(): Promise<PreviewRegistry> {
    try {
      const value = JSON.parse(await readFile(this.#config.registryPath, 'utf8')) as unknown
      if (!isRecord(value) || value.version !== REGISTRY_VERSION || !Array.isArray(value.previews)) {
        throw new Error('invalid preview registry')
      }
      return {
        version: REGISTRY_VERSION,
        previews: value.previews.filter(isRegistryPreview),
      }
    } catch (error) {
      if (isMissingFileError(error)) return { version: REGISTRY_VERSION, previews: [] }
      throw error
    }
  }

  async #writeRegistry(registry: PreviewRegistry): Promise<void> {
    await mkdir(dirname(this.#config.registryPath), { recursive: true })
    const temporary = `${this.#config.registryPath}.${process.pid}.${randomUUID()}.tmp`
    try {
      const handle = await open(temporary, 'wx', 0o600)
      try {
        await handle.writeFile(`${JSON.stringify(registry, null, 2)}\n`)
        await handle.sync()
      } finally {
        await handle.close()
      }
      await rename(temporary, this.#config.registryPath)
    } finally {
      await rm(temporary, { force: true })
    }
  }

  async #exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const withLock = async () => {
      await mkdir(dirname(this.#config.registryPath), { recursive: true, mode: 0o700 })
      const release = await lockfile.lock(this.#config.registryPath, {
        realpath: false,
        stale: PREVIEW_LOCK_STALE_MS,
        update: PREVIEW_LOCK_STALE_MS / 2,
        retries: {
          forever: true,
          factor: 1.2,
          minTimeout: 10,
          maxTimeout: 100,
          randomize: true,
        },
      })
      try {
        return await operation()
      } finally {
        await release()
      }
    }
    const result = this.#operation.then(withLock, withLock)
    this.#operation = result.then(() => undefined, () => undefined)
    return await result
  }
}

const activatePreview = (preview: RegistryPreview, url: string): PersistedPreview => ({
  ...preview,
  state: 'active',
  url,
})

const referenceFrom = (
  { managedBy: _managedBy, state: _state, checkoutPath: _checkoutPath, ...preview }: PersistedPreview,
): PreviewReference => preview

const sameProcess = (
  left: PreviewProcessIdentity | undefined,
  right: PreviewProcessIdentity | undefined,
): boolean => Boolean(left && right &&
  left.pid === right.pid &&
  left.startTime === right.startTime &&
  left.cmdline === right.cmdline &&
  left.cwd === right.cwd &&
  left.marker === right.marker)

const portRange = (start: number, end: number): number[] =>
  Array.from({ length: end - start + 1 }, (_, index) => start + index)

const livePortConfigured = (status: unknown, httpsPort: number): boolean => {
  for (const config of serveConfigs(status)) {
    const tcp = isRecord(config.TCP) ? config.TCP : undefined
    if (tcp && Object.hasOwn(tcp, String(httpsPort))) return true
  }
  return false
}

const liveUpstreamPorts = (status: unknown): Set<number> => {
  const ports = new Set<number>()
  for (const config of serveConfigs(status)) {
    const web = isRecord(config.Web) ? config.Web : undefined
    for (const rawServer of Object.values(web ?? {})) {
      if (!isRecord(rawServer)) continue
      const handlers = isRecord(rawServer.Handlers) ? rawServer.Handlers : undefined
      for (const rawHandler of Object.values(handlers ?? {})) {
        if (!isRecord(rawHandler) || typeof rawHandler.Proxy !== 'string') continue
        const match = rawHandler.Proxy.match(/^http:\/\/127\.0\.0\.1:(\d{1,5})(?:\/|$)/u)
        const port = Number(match?.[1])
        if (Number.isInteger(port) && port >= 1 && port <= 65_535) ports.add(port)
      }
    }
  }
  return ports
}

const liveRouteMatches = (
  status: unknown,
  preview: Pick<PreviewReference, 'httpsPort' | 'targetPort'>,
): boolean =>
  Boolean(liveRoute(status, preview.httpsPort, preview.targetPort))

const liveRoute = (
  status: unknown,
  httpsPort: number,
  targetPort: number,
): { host: string; funnel: boolean } | undefined => {
  const expectedProxy = `http://127.0.0.1:${targetPort}`
  for (const config of serveConfigs(status)) {
    const tcp = isRecord(config.TCP) ? config.TCP : undefined
    const tcpHandler: Record<string, unknown> | undefined = tcp && isRecord(tcp[String(httpsPort)])
      ? tcp[String(httpsPort)] as Record<string, unknown>
      : undefined
    if (tcpHandler?.HTTPS !== true) continue
    const web = isRecord(config.Web) ? config.Web : undefined
    for (const [hostPort, rawServer] of Object.entries(web ?? {})) {
      if (!hostPort.endsWith(`:${httpsPort}`) || !isRecord(rawServer)) continue
      const handlers = isRecord(rawServer.Handlers) ? rawServer.Handlers : undefined
      const root = handlers && isRecord(handlers['/']) ? handlers['/'] : undefined
      if (root?.Proxy === expectedProxy) {
        const allowFunnel = isRecord(config.AllowFunnel) ? config.AllowFunnel : undefined
        return {
          host: hostPort.slice(0, -String(httpsPort).length - 1),
          funnel: allowFunnel?.[hostPort] === true,
        }
      }
    }
  }
  return undefined
}

const serveConfigs = (status: unknown): Record<string, unknown>[] => {
  if (!isRecord(status)) return []
  const configs: Record<string, unknown>[] = []
  const visit = (config: Record<string, unknown>) => {
    configs.push(config)
    for (const nestedKey of ['Services', 'Foreground']) {
      const nested = isRecord(config[nestedKey]) ? config[nestedKey] : undefined
      for (const child of Object.values(nested ?? {}).filter(isRecord)) visit(child)
    }
  }
  visit(status)
  return configs
}

const previewUrl = (host: string, httpsPort: number): string =>
  `https://${host}${httpsPort === 443 ? '' : `:${httpsPort}`}/`

const isRegistryPreview = (value: unknown): value is RegistryPreview => {
  if (!isRecord(value)) return false
  const identity = value.managedBy === MANAGED_BY &&
    value.provider === 'tailscale-serve' &&
    value.access === 'tailnet' &&
    value.lifetime === 'issue' &&
    typeof value.id === 'string' &&
    typeof value.namespace === 'string' &&
    typeof value.owner === 'string' &&
    typeof value.service === 'string' &&
    typeof value.repo === 'string' &&
    typeof value.startCommand === 'string' &&
    typeof value.targetPort === 'number' &&
    typeof value.httpsPort === 'number' &&
    typeof value.createdAt === 'string'
  if (!identity) return false
  if (value.configuredTargetPort !== undefined && typeof value.configuredTargetPort !== 'number') return false
  if (value.checkoutPath !== undefined && typeof value.checkoutPath !== 'string') return false
  if (value.process !== undefined && !isPreviewProcessIdentity(value.process)) return false
  return value.state === 'pending' || (value.state === 'active' && typeof value.url === 'string')
}

const isPreviewProcessIdentity = (value: unknown): value is PreviewProcessIdentity =>
  isRecord(value) &&
  Number.isInteger(value.pid) &&
  Number(value.pid) > 0 &&
  typeof value.startTime === 'string' &&
  typeof value.cmdline === 'string' &&
  typeof value.cwd === 'string' &&
  typeof value.marker === 'string'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isMissingFileError = (error: unknown): boolean =>
  isRecord(error) && error.code === 'ENOENT'

const portAvailable: PreviewPortProbe = async (port) => await new Promise<boolean>((resolve) => {
  const server = createServer()
  let settled = false
  const finish = (available: boolean) => {
    if (settled) return
    settled = true
    resolve(available)
  }
  server.once('error', () => finish(false))
  server.listen({ host: '127.0.0.1', port, exclusive: true }, () => {
    server.close((error) => finish(!error))
  })
  server.unref()
})

const httpReady: PreviewReadyProbe = async (port) => await new Promise<boolean>((resolve) => {
  let settled = false
  const finish = (ready: boolean) => {
    if (settled) return
    settled = true
    resolve(ready)
  }
  const request = get({ host: '127.0.0.1', port, path: '/', timeout: 1_000 }, (response) => {
    response.resume()
    finish(true)
  })
  request.once('error', () => finish(false))
  request.once('timeout', () => {
    request.destroy()
    finish(false)
  })
})
