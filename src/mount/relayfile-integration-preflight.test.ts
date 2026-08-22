import { describe, expect, it, vi } from 'vitest'

import type { FactoryIntegrationConnections } from '../ports'
import {
  ensureFactoryIntegrations,
  inspectFactoryIntegration,
  type FactoryIntegrationPreflightIO,
} from './relayfile-integration-preflight'

const io = (): FactoryIntegrationPreflightIO => ({
  info: vi.fn(),
  warn: vi.fn(),
  confirm: vi.fn(async () => true),
  openUrl: vi.fn(),
})

const connections = (
  getStatus: FactoryIntegrationConnections['getStatus'],
): FactoryIntegrationConnections => ({
  getStatus: vi.fn(getStatus),
  connect: vi.fn(async () => ({
    alreadyConnected: false,
    connectLink: 'https://connect.example/session',
    connectionId: 'conn-1',
  })),
  waitForConnection: vi.fn(async () => {}),
})

describe('Relayfile integration preflight', () => {
  it('only classifies the authoritative not_connected state as missing', async () => {
    const missing = connections(async () => ({ ready: false, state: 'not_connected' }))
    const syncing = connections(async () => ({ ready: false, state: 'oauth_connected', initialSyncState: 'running' }))

    await expect(inspectFactoryIntegration(missing, 'linear')).resolves.toEqual({ kind: 'missing' })
    await expect(inspectFactoryIntegration(syncing, 'linear')).resolves.toEqual({
      kind: 'connected-not-ready',
      state: 'oauth_connected',
      initialSyncState: 'running',
    })
  })

  it('recognizes the SDK provider_not_connected error without swallowing other failures', async () => {
    const missing = connections(async () => {
      throw Object.assign(new Error('not connected'), { code: 'provider_not_connected' })
    })
    const malformed = connections(async () => {
      throw Object.assign(new Error('missing ready field'), { code: 'malformed_cloud_response' })
    })

    await expect(inspectFactoryIntegration(missing, 'github')).resolves.toEqual({ kind: 'missing' })
    await expect(inspectFactoryIntegration(malformed, 'github')).rejects.toThrow(
      /refusing to assume it is disconnected.*missing ready field/u,
    )
  })

  it.each([
    ['authentication', 'cloud_api_error'],
    ['transient timeout', 'cloud_timeout_error'],
    ['malformed response', 'malformed_cloud_response'],
  ])('does not misclassify a %s failure as a missing integration', async (reason, code) => {
    const relayfile = connections(async () => {
      throw Object.assign(new Error(reason), { code })
    })

    await expect(inspectFactoryIntegration(relayfile, 'linear')).rejects.toThrow(
      new RegExp(`refusing to assume it is disconnected.*${reason}`, 'u'),
    )
    expect(relayfile.connect).not.toHaveBeenCalled()
  })

  it('connects a genuinely missing provider through the SDK and waits until ready', async () => {
    let checks = 0
    const relayfile = connections(async () => (
      checks++ === 0
        ? { ready: false, state: 'not_connected' }
        : { ready: true, state: 'ready' }
    ))
    const terminal = io()

    await expect(ensureFactoryIntegrations({
      connections: relayfile,
      providers: ['github'],
      workspaceId: 'rw_test',
      interactive: true,
      dryRun: false,
      io: terminal,
    })).resolves.toBeUndefined()

    expect(terminal.confirm).toHaveBeenCalledWith('github')
    expect(relayfile.connect).toHaveBeenCalledWith('github')
    expect(terminal.openUrl).toHaveBeenCalledWith('https://connect.example/session')
    expect(relayfile.waitForConnection).toHaveBeenCalledWith('github', 'conn-1')
  })

  it.each([
    { dryRun: true, interactive: true, message: /dry-run will not start an OAuth flow/u },
    { dryRun: false, interactive: false, message: /invocation is non-interactive/u },
  ])('fails actionably with no side effects for $message', async ({ dryRun, interactive, message }) => {
    const relayfile = connections(async () => ({ ready: false, state: 'not_connected' }))
    const terminal = io()

    await expect(ensureFactoryIntegrations({
      connections: relayfile,
      providers: ['linear'],
      workspaceId: 'rw_test',
      interactive,
      dryRun,
      io: terminal,
    })).rejects.toThrow(message)
    expect(terminal.confirm).not.toHaveBeenCalled()
    expect(relayfile.connect).not.toHaveBeenCalled()
    expect(relayfile.waitForConnection).not.toHaveBeenCalled()
  })

  it('fails precisely for connected-but-not-ready providers without prompting', async () => {
    const relayfile = connections(async () => ({
      ready: false,
      state: 'oauth_connected',
      initialSyncState: 'running',
    }))
    const terminal = io()

    await expect(ensureFactoryIntegrations({
      connections: relayfile,
      providers: ['github'],
      workspaceId: 'rw_test',
      interactive: true,
      dryRun: false,
      io: terminal,
    })).rejects.toThrow(/connected but not ready \(oauth_connected, running\)/u)
    expect(terminal.confirm).not.toHaveBeenCalled()
    expect(relayfile.connect).not.toHaveBeenCalled()
  })

  it('directs completed-but-degraded integrations to an authorized repair instead of waiting', async () => {
    const relayfile = connections(async () => ({
      ready: false,
      state: 'degraded',
      initialSyncState: 'complete',
    }))
    const terminal = io()

    await expect(ensureFactoryIntegrations({
      connections: relayfile,
      providers: ['github'],
      workspaceId: 'rw_test',
      interactive: false,
      dryRun: true,
      io: terminal,
    })).rejects.toThrow(
      /degraded after its initial sync completed \(degraded, complete\).*workspace owner.*repair or reconnect github/u,
    )
    expect(relayfile.connect).not.toHaveBeenCalled()
    expect(relayfile.waitForConnection).not.toHaveBeenCalled()
  })

  it('classifies a degraded completed integration by its normalized state, echoing the reported values', async () => {
    const relayfile = connections(async () => ({
      ready: false,
      state: '  DeGraded ',
      initialSyncState: '\tComplete  ',
    }))
    const terminal = io()

    // Only `.trim().toLowerCase()` routes this to the repair branch; without it
    // the provider falls back to "wait for its initial sync", which is the
    // advice #203 is about. The details string deliberately echoes the values
    // as the provider reported them, so the operator can match them to what
    // the dashboard shows -- normalization decides the branch, not the text.
    await expect(ensureFactoryIntegrations({
      connections: relayfile,
      providers: ['github'],
      workspaceId: 'rw_test',
      interactive: false,
      dryRun: true,
      io: terminal,
    })).rejects.toThrow(
      /degraded after its initial sync completed \(  DeGraded ,\s+Complete  \).*workspace owner.*repair or reconnect github/u,
    )
    expect(relayfile.connect).not.toHaveBeenCalled()
    expect(relayfile.waitForConnection).not.toHaveBeenCalled()
  })
})
