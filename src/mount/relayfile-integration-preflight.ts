import { platform } from 'node:os'
import { spawn } from 'node:child_process'

import type {
  FactoryIntegrationConnections,
  FactoryIntegrationProvider,
} from '../ports'

export type FactoryIntegrationObservation =
  | { kind: 'ready' }
  | { kind: 'connected-not-ready'; state?: string; initialSyncState?: string }
  | { kind: 'missing' }

export interface FactoryIntegrationPreflightIO {
  info(message: string): void
  warn(message: string): void
  confirm(provider: FactoryIntegrationProvider): Promise<boolean>
  openUrl(url: string): void | Promise<void>
}

export async function inspectFactoryIntegration(
  connections: FactoryIntegrationConnections,
  provider: FactoryIntegrationProvider,
): Promise<FactoryIntegrationObservation> {
  try {
    const status = await connections.getStatus(provider)
    if (status.ready) return { kind: 'ready' }
    if (status.state?.trim().toLowerCase() === 'not_connected') {
      return { kind: 'missing' }
    }
    return {
      kind: 'connected-not-ready',
      ...(status.state ? { state: status.state } : {}),
      ...(status.initialSyncState ? { initialSyncState: status.initialSyncState } : {}),
    }
  } catch (error) {
    if (errorCode(error) === 'provider_not_connected') {
      return { kind: 'missing' }
    }
    const reason = error instanceof Error ? error.message : String(error)
    throw new Error(
      `[factory] could not verify the ${provider} Relayfile connection; ` +
      `refusing to assume it is disconnected (${reason})`,
      { cause: error },
    )
  }
}

export async function ensureFactoryIntegrations(input: {
  connections: FactoryIntegrationConnections
  providers: readonly FactoryIntegrationProvider[]
  workspaceId: string
  interactive: boolean
  dryRun: boolean
  io: FactoryIntegrationPreflightIO
  observed?: ReadonlyMap<FactoryIntegrationProvider, FactoryIntegrationObservation>
}): Promise<void> {
  for (const provider of [...new Set(input.providers)]) {
    const observation = input.observed?.get(provider)
      ?? await inspectFactoryIntegration(input.connections, provider)
    if (observation.kind === 'ready') continue
    if (observation.kind === 'connected-not-ready') {
      throw new Error(notReadyMessage(provider, observation))
    }

    const retry = `Rerun this Factory command in an interactive terminal to connect ${provider}, then retry.`
    if (input.dryRun) {
      throw new Error(
        `[factory] ${provider} is not connected to Relayfile workspace ${input.workspaceId}; ` +
        `dry-run will not start an OAuth flow. ${retry}`,
      )
    }
    if (!input.interactive) {
      throw new Error(
        `[factory] ${provider} is not connected to Relayfile workspace ${input.workspaceId}; ` +
        `this invocation is non-interactive. ${retry}`,
      )
    }

    if (!(await input.io.confirm(provider))) {
      throw new Error(`[factory] ${provider} connection declined. ${retry}`)
    }

    const result = await input.connections.connect(provider)
    if (result.connectLink) {
      input.io.info(`Connecting ${provider}: ${result.connectLink}`)
      try {
        await input.io.openUrl(result.connectLink)
      } catch (error) {
        input.io.warn(
          `Could not open the browser automatically (${error instanceof Error ? error.message : String(error)}); ` +
          'open the printed URL manually.',
        )
      }
    }
    await input.connections.waitForConnection(provider, result.connectionId)

    const connected = await inspectFactoryIntegration(input.connections, provider)
    if (connected.kind !== 'ready') {
      if (connected.kind === 'connected-not-ready') {
        throw new Error(notReadyMessage(provider, connected))
      }
      throw new Error(`[factory] ${provider} is still not connected after the Relayfile connect flow`)
    }
    input.io.info(`${provider} connected.`)
  }
}

export function openIntegrationUrl(url: string): void {
  const command = platform() === 'darwin'
    ? 'open'
    : platform() === 'win32'
      ? 'cmd'
      : 'xdg-open'
  const args = platform() === 'win32' ? ['/c', 'start', '', url] : [url]
  const child = spawn(command, args, { stdio: 'ignore', detached: true })
  child.on('error', () => {})
  child.unref()
}

const notReadyMessage = (
  provider: FactoryIntegrationProvider,
  observation: Extract<FactoryIntegrationObservation, { kind: 'connected-not-ready' }>,
): string => {
  const details = [observation.state, observation.initialSyncState]
    .filter((value): value is string => Boolean(value))
    .join(', ')
  const state = observation.state?.trim().toLowerCase()
  const initialSyncState = observation.initialSyncState?.trim().toLowerCase()
  if (state === 'degraded' && initialSyncState === 'complete') {
    return `[factory] ${provider} is connected but degraded after its initial sync completed` +
      `${details ? ` (${details})` : ''}; ask an Agent Relay workspace owner to repair or reconnect ` +
      `${provider}, then retry.`
  }
  return `[factory] ${provider} is connected but not ready${details ? ` (${details})` : ''}; wait for its initial sync, then retry.`
}

const errorCode = (error: unknown): string | undefined => {
  if (error === null || typeof error !== 'object') return undefined
  const code = (error as { code?: unknown }).code
  return typeof code === 'string' ? code : undefined
}
