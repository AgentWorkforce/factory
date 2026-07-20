import { randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { mkdir, open, readFile, rename, rm } from 'node:fs/promises'
import { dirname } from 'node:path'
import { promisify } from 'node:util'

import type { PreviewConfig } from '../config/schema'
import type {
  PreviewReference,
  PreviewStartInput,
  PreviewSweepResult,
} from '../ports/fleet'

const execFileAsync = promisify(execFile)
const REGISTRY_VERSION = 1
const MANAGED_BY = '@agent-relay/factory'

type PersistedPreview = PreviewReference & { managedBy: typeof MANAGED_BY }
type PreviewRegistry = { version: typeof REGISTRY_VERSION; previews: PersistedPreview[] }
type CommandResult = { stdout: string; stderr: string }
export type PreviewCommandRunner = (file: string, args: string[]) => Promise<CommandResult>

export interface PreviewManager {
  start(input: PreviewStartInput): Promise<PreviewReference>
  remove(preview: PreviewReference): Promise<boolean>
  sweep(activeOwners: string[]): Promise<PreviewSweepResult>
}

export interface TailscalePreviewManagerOptions {
  config: PreviewConfig
  run?: PreviewCommandRunner
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
  readonly #now: () => Date
  #operation: Promise<unknown> = Promise.resolve()

  constructor(options: TailscalePreviewManagerOptions) {
    this.#config = options.config
    this.#run = options.run ?? (async (file, args) => {
      const result = await execFileAsync(file, args, { encoding: 'utf8' })
      return { stdout: result.stdout, stderr: result.stderr }
    })
    this.#now = options.now ?? (() => new Date())
  }

  async start(input: PreviewStartInput): Promise<PreviewReference> {
    return await this.#exclusive(async () => {
      const registry = await this.#readRegistry()
      let status = await this.#serveStatus()
      const existing = registry.previews.find((preview) =>
        preview.owner === input.owner && preview.service === input.service,
      )
      const existingMatchesRequest = existing &&
        existing.repo === input.repo &&
        existing.targetPort === input.targetPort &&
        (input.preferredHttpsPort === undefined || existing.httpsPort === input.preferredHttpsPort) &&
        existing.startCommand === input.startCommand
      if (existingMatchesRequest && liveRouteMatches(status, existing)) {
        return referenceFrom(existing)
      }
      if (existing) {
        // A changed service declaration replaces only the old route whose live
        // upstream still matches Factory's registry identity.
        if (liveRouteMatches(status, existing)) {
          await this.#disable(existing)
          status = await this.#serveStatus()
        }
        registry.previews = registry.previews.filter((preview) => preview.id !== existing.id)
      }

      const occupied = new Set(registry.previews.map((preview) => preview.httpsPort))
      const candidates = input.preferredHttpsPort !== undefined
        ? [input.preferredHttpsPort]
        : portRange(this.#config.httpsPortRange[0], this.#config.httpsPortRange[1])
      let lastError: unknown
      for (const httpsPort of candidates) {
        if (occupied.has(httpsPort) || liveHttpsPortConfigured(status, httpsPort)) continue
        let commandSucceeded = false
        try {
          await this.#run(this.#config.tailscaleBinary, [
            'serve',
            '--bg',
            '--yes',
            `--https=${httpsPort}`,
            `http://127.0.0.1:${input.targetPort}`,
          ])
          commandSucceeded = true
          const configured = await this.#serveStatus()
          const route = liveRoute(configured, httpsPort, input.targetPort)
          if (!route) {
            throw new Error(`tailscale serve did not report the configured route on HTTPS port ${httpsPort}`)
          }
          const preview: PersistedPreview = {
            id: randomUUID(),
            provider: 'tailscale-serve',
            owner: input.owner,
            service: input.service,
            repo: input.repo,
            url: `https://${route.host}${httpsPort === 443 ? '' : `:${httpsPort}`}/`,
            targetPort: input.targetPort,
            httpsPort,
            access: 'tailnet',
            lifetime: 'issue',
            createdAt: this.#now().toISOString(),
            ...(input.startCommand ? { startCommand: input.startCommand } : {}),
            ...(input.node && input.node !== 'self' ? { node: input.node } : {}),
            managedBy: MANAGED_BY,
          }
          registry.previews.push(preview)
          await this.#writeRegistry(registry)
          return referenceFrom(preview)
        } catch (error) {
          lastError = error
          if (commandSucceeded) {
            try {
              const cleanupStatus = await this.#serveStatus()
              if (liveRoute(cleanupStatus, httpsPort, input.targetPort)) {
                await this.#disable({ httpsPort })
              }
            } catch (cleanupError) {
              lastError = new AggregateError(
                [error, cleanupError],
                `Preview route setup and guarded rollback both failed on HTTPS port ${httpsPort}`,
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
        candidate.owner === reference.owner &&
        candidate.httpsPort === reference.httpsPort &&
        candidate.targetPort === reference.targetPort,
      )
      if (!preview) return false

      const status = await this.#serveStatus()
      if (!liveRouteMatches(status, preview)) {
        // The port no longer points at the route Factory created. Forget the
        // stale ownership record, but never disable the new route.
        registry.previews = registry.previews.filter((candidate) => candidate.id !== preview.id)
        await this.#writeRegistry(registry)
        return false
      }
      await this.#disable(preview)
      registry.previews = registry.previews.filter((candidate) => candidate.id !== preview.id)
      await this.#writeRegistry(registry)
      return true
    })
  }

  async sweep(activeOwners: string[]): Promise<PreviewSweepResult> {
    return await this.#exclusive(async () => {
      const active = new Set(activeOwners)
      const registry = await this.#readRegistry()
      const status = await this.#serveStatus()
      const reaped: PreviewReference[] = []
      const skipped: PreviewSweepResult['skipped'] = []
      const retained: PersistedPreview[] = []

      for (const preview of registry.previews) {
        if (preview.managedBy !== MANAGED_BY || active.has(preview.owner)) {
          retained.push(preview)
          continue
        }
        if (!liveRouteMatches(status, preview)) {
          skipped.push({ id: preview.id, reason: 'live route identity mismatch', node: preview.node })
          continue
        }
        try {
          await this.#disable(preview)
          reaped.push(referenceFrom(preview))
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

  async #disable(preview: Pick<PreviewReference, 'httpsPort'>): Promise<void> {
    await this.#run(this.#config.tailscaleBinary, [
      'serve',
      '--yes',
      `--https=${preview.httpsPort}`,
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
        previews: value.previews.filter(isPersistedPreview),
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
    const result = this.#operation.then(operation, operation)
    this.#operation = result.then(() => undefined, () => undefined)
    return await result
  }
}

const referenceFrom = ({ managedBy: _managedBy, ...preview }: PersistedPreview): PreviewReference => preview

const portRange = (start: number, end: number): number[] =>
  Array.from({ length: end - start + 1 }, (_, index) => start + index)

const liveHttpsPortConfigured = (status: unknown, httpsPort: number): boolean => {
  for (const config of serveConfigs(status)) {
    const tcp = isRecord(config.TCP) ? config.TCP : undefined
    const handler: Record<string, unknown> | undefined = tcp && isRecord(tcp[String(httpsPort)])
      ? tcp[String(httpsPort)] as Record<string, unknown>
      : undefined
    if (handler?.HTTPS === true) return true
  }
  return false
}

const liveRouteMatches = (status: unknown, preview: PreviewReference): boolean =>
  Boolean(liveRoute(status, preview.httpsPort, preview.targetPort))

const liveRoute = (
  status: unknown,
  httpsPort: number,
  targetPort: number,
): { host: string } | undefined => {
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
        return { host: hostPort.slice(0, -String(httpsPort).length - 1) }
      }
    }
  }
  return undefined
}

const serveConfigs = (status: unknown): Record<string, unknown>[] => {
  if (!isRecord(status)) return []
  const configs = [status]
  if (isRecord(status.Services)) {
    configs.push(...Object.values(status.Services).filter(isRecord))
  }
  return configs
}

const isPersistedPreview = (value: unknown): value is PersistedPreview => {
  if (!isRecord(value)) return false
  return value.managedBy === MANAGED_BY &&
    value.provider === 'tailscale-serve' &&
    value.access === 'tailnet' &&
    value.lifetime === 'issue' &&
    typeof value.id === 'string' &&
    typeof value.owner === 'string' &&
    typeof value.service === 'string' &&
    typeof value.repo === 'string' &&
    typeof value.url === 'string' &&
    typeof value.targetPort === 'number' &&
    typeof value.httpsPort === 'number' &&
    typeof value.createdAt === 'string'
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isMissingFileError = (error: unknown): boolean =>
  isRecord(error) && error.code === 'ENOENT'
