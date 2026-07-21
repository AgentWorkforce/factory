import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it, vi } from 'vitest'

import type { PreviewStartInput } from '../ports/fleet'
import {
  previewProcessMarker,
  type PreviewListenerOwnership,
  type PreviewProcessIdentity,
} from './preview-process'
import {
  TailscalePreviewManager,
  type PreviewCommandRunner,
  type TailscalePreviewManagerOptions,
} from './tailscale-preview'

type Route = { targetPort: number; host: string }

function fakeTailscale() {
  const routes = new Map<number, Route>()
  const funnels = new Set<number>()
  const foreignTcp = new Map<number, Record<string, unknown>>()
  const foregroundTcp = new Map<number, Record<string, unknown>>()
  const humanWeb = new Map<number, Record<string, unknown>>()
  const calls: string[][] = []
  const run: PreviewCommandRunner = async (_file, args) => {
    calls.push(args)
    if (args.join(' ') === 'serve status --json') {
      const webPorts = new Set([...routes.keys(), ...humanWeb.keys()])
      return {
        stdout: JSON.stringify({
          TCP: Object.fromEntries([
            ...[...routes].map(([port]) => [port, { HTTPS: true }] as const),
            ...[...humanWeb].map(([port]) => [port, { HTTPS: true }] as const),
            ...foreignTcp,
          ]),
          Web: Object.fromEntries([...webPorts].map((port) => {
            const route = routes.get(port)
            return [
              `${route?.host ?? 'factory-node.example.ts.net'}:${port}`,
              {
                Handlers: {
                  ...humanWeb.get(port),
                  ...(route ? { '/': { Proxy: `http://127.0.0.1:${route.targetPort}` } } : {}),
                },
              },
            ]
          })),
          AllowFunnel: Object.fromEntries([...funnels].map((port) => [
            `${routes.get(port)?.host}:${port}`,
            true,
          ])),
          Foreground: foregroundTcp.size > 0
            ? { human: { TCP: Object.fromEntries(foregroundTcp) } }
            : undefined,
        }),
        stderr: '',
      }
    }
    const https = args.find((arg) => arg.startsWith('--https='))
    const httpsPort = Number(https?.slice('--https='.length))
    if (args.includes('--bg')) {
      const target = args.find((arg) => arg.startsWith('http://127.0.0.1:'))
      routes.set(httpsPort, {
        targetPort: Number(target?.slice('http://127.0.0.1:'.length)),
        host: 'factory-node.example.ts.net',
      })
      funnels.delete(httpsPort)
      return { stdout: '', stderr: '' }
    }
    if (args.at(-1) === 'off') {
      routes.delete(httpsPort)
      funnels.delete(httpsPort)
      return { stdout: '', stderr: '' }
    }
    throw new Error(`unexpected tailscale command: ${args.join(' ')}`)
  }
  return { calls, foreignTcp, foregroundTcp, funnels, humanWeb, routes, run }
}

function manager(
  run: PreviewCommandRunner,
  registryPath: string,
  options: Partial<TailscalePreviewManagerOptions> = {},
  listenerOwnership: () => Promise<PreviewListenerOwnership> = async () => 'owned',
) {
  const processes = new Map<string, PreviewProcessIdentity>()
  return new TailscalePreviewManager({
    config: {
      provider: 'tailscale-serve',
      access: 'tailnet',
      services: {},
      tailscaleBinary: 'tailscale-test',
      registryPath,
      httpsPortRange: [10_000, 10_002],
    },
    run,
    isPortAvailable: async () => true,
    isReady: async () => true,
    isPublishedReady: async () => true,
    processSupervisor: {
      async start(input) {
        const existing = processes.get(input.id)
        if (existing) return existing
        const marker = previewProcessMarker(input.id)
        const process = {
          pid: 10_000 + processes.size,
          startTime: `started-${input.id}`,
          cmdline: `/bin/sh --agent-name ${marker}`,
          cwd: input.cwd,
          marker,
        }
        processes.set(input.id, process)
        return process
      },
      async isRunning(process) {
        return [...processes.values()].some((candidate) => candidate.pid === process.pid)
      },
      listenerOwnership,
      async find(id) { return processes.get(id) },
      async stop(process) {
        for (const [id, candidate] of processes) {
          if (candidate.pid === process.pid) processes.delete(id)
        }
        return true
      },
    },
    now: () => new Date('2026-07-20T12:00:00.000Z'),
    ...options,
  })
}

function start(
  previewManager: TailscalePreviewManager,
  input: Omit<PreviewStartInput, 'startCommand' | 'checkoutPath'> &
    Partial<Pick<PreviewStartInput, 'startCommand' | 'checkoutPath'>>,
) {
  return previewManager.start({
    startCommand: 'npm run dev',
    checkoutPath: '/work/factory',
    ...input,
  })
}

describe('TailscalePreviewManager', () => {
  it('creates and reuses a background tailnet-only route for one issue owner', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const input = {
      namespace: 'factory-test',
      owner: 'AR-129:uuid:/linear/issues/129',
      issueKey: 'AR-129',
      service: 'factory',
      repo: 'AgentWorkforce/factory',
      targetPort: 3_000,
      startCommand: 'npm run dev',
    }

    const first = await start(previewManager, input)
    const second = await start(previewManager, input)

    expect(first).toEqual(second)
    expect(first).toMatchObject({
      provider: 'tailscale-serve',
      url: 'https://factory-node.example.ts.net:10000/',
      targetPort: 3_000,
      httpsPort: 10_000,
      access: 'tailnet',
      lifetime: 'issue',
      startCommand: 'npm run dev',
    })
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(1)
  })

  it('probes the published tailnet URL before returning a preview reference', async () => {
    const fake = fakeTailscale()
    const publishedProbe = vi.fn(async () => true)
    const previewManager = manager(
      fake.run,
      join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'),
      { isPublishedReady: publishedProbe },
    )

    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    expect(publishedProbe).toHaveBeenCalledWith(preview.url)
  })

  it.each(['unrelated', 'indeterminate'] as const)(
    'refuses to publish when local listener ownership is %s',
    async (ownership) => {
      const fake = fakeTailscale()
      const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
      const publishedProbe = vi.fn(async () => true)
      const previewManager = manager(
        fake.run,
        registryPath,
        { isPublishedReady: publishedProbe },
        async () => ownership,
      )

      await expect(start(previewManager, {
        namespace: 'factory-test',
        owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
      })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

      expect(fake.routes).toHaveLength(0)
      expect(publishedProbe).not.toHaveBeenCalled()
      expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
    },
  )

  it('rolls back the route and process when the published URL never becomes reachable', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const previewManager = manager(fake.run, registryPath, {
      isPublishedReady: async () => false,
      publishedReadyTimeoutMs: 0,
    })

    await expect(start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

    expect(fake.routes).toHaveLength(0)
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
  })

  it('bounds a slow readiness probe by one wall-clock deadline', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(
      fake.run,
      join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'),
      {
        isReady: async () => await new Promise((resolve) => setTimeout(() => resolve(false), 100)),
        readyTimeoutMs: 20,
        readyPollIntervalMs: 1,
      },
    )
    const startedAt = Date.now()

    await expect(start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

    expect(Date.now() - startedAt).toBeLessThan(250)
  })

  it('removes only an exact live route identity', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1',
      issueKey: 'AR-129',
      service: 'factory',
      repo: 'AgentWorkforce/factory',
      targetPort: 3_000,
    })

    fake.routes.set(preview.httpsPort, { targetPort: 9_999, host: 'factory-node.example.ts.net' })

    await expect(previewManager.remove(preview)).resolves.toBe(true)
    expect(fake.routes.get(preview.httpsPort)?.targetPort).toBe(9_999)
    expect(fake.calls.some((args) => args.at(-1) === 'off')).toBe(false)
  })

  it('clears an active registry record after its route and process are confirmed absent', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const previewManager = manager(fake.run, registryPath)
    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })
    fake.routes.delete(preview.httpsPort)

    await expect(previewManager.remove(preview)).resolves.toBe(true)

    expect(fake.calls.some((args) => args.at(-1) === 'off')).toBe(false)
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
  })

  it('removes only the Factory root mount and preserves human-owned paths', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1',
      issueKey: 'AR-129',
      service: 'factory',
      repo: 'AgentWorkforce/factory',
      targetPort: 3_000,
    })
    fake.humanWeb.set(preview.httpsPort, {
      '/human': { Proxy: 'http://127.0.0.1:9000' },
    })

    await expect(previewManager.remove(preview)).resolves.toBe(true)

    expect(fake.routes.has(preview.httpsPort)).toBe(false)
    expect(fake.humanWeb.get(preview.httpsPort)).toEqual({
      '/human': { Proxy: 'http://127.0.0.1:9000' },
    })
    expect(fake.calls.filter((args) => args.includes('--bg')).at(-1)).toContain('--set-path=/')
    expect(fake.calls.filter((args) => args.at(-1) === 'off').at(-1)).toContain('--set-path=/')
  })

  it('allocates distinct upstream ports for concurrent issues of the same service', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const first = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
    })
    const second = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-2', issueKey: 'AR-2', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    expect(first).toMatchObject({ configuredTargetPort: 3_000, targetPort: 3_000 })
    expect(second).toMatchObject({ configuredTargetPort: 3_000, targetPort: 3_001 })
    expect(fake.routes.get(first.httpsPort)?.targetPort).toBe(3_000)
    expect(fake.routes.get(second.httpsPort)?.targetPort).toBe(3_001)
  })

  it('never reuses an upstream port referenced by an unowned live Serve route', async () => {
    const fake = fakeTailscale()
    fake.humanWeb.set(9_999, {
      '/foreign': { Proxy: 'http://127.0.0.1:3000' },
    })
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))

    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1',
      issueKey: 'AR-1',
      service: 'factory',
      repo: 'factory',
      targetPort: 3_000,
    })

    expect(preview).toMatchObject({ configuredTargetPort: 3_000, targetPort: 3_001 })
  })

  it('serializes allocation across manager instances sharing one node registry', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const firstManager = manager(fake.run, registryPath)
    const secondManager = manager(fake.run, registryPath)

    const [first, second] = await Promise.all([
      start(firstManager, {
        namespace: 'factory-test',
        owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
      }),
      start(secondManager, {
        namespace: 'factory-test',
        owner: 'owner-2', issueKey: 'AR-2', service: 'factory', repo: 'factory', targetPort: 3_000,
      }),
    ])

    expect([first.targetPort, second.targetPort].sort()).toEqual([3_000, 3_001])
    expect([first.httpsPort, second.httpsPort].sort()).toEqual([10_000, 10_001])
  })

  it('does not claim HTTP, TCP-forward, or foreground ports configured by a human', async () => {
    const fake = fakeTailscale()
    fake.foreignTcp.set(10_000, { HTTP: true })
    fake.foregroundTcp.set(10_001, { TCPForward: '127.0.0.1:9000' })
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))

    const preview = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    expect(preview.httpsPort).toBe(10_002)
  })

  it('replaces a changed service target without leaving the old route behind', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const initial = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    const replaced = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 4_173,
    })

    expect(replaced.id).not.toBe(initial.id)
    expect(replaced.httpsPort).toBe(initial.httpsPort)
    expect(fake.routes.get(replaced.httpsPort)?.targetPort).toBe(4_173)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
  })

  it('never reuses a Factory route that was switched to public Funnel access', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const input = {
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    }
    const initial = await start(previewManager, input)
    fake.funnels.add(initial.httpsPort)

    const repaired = await start(previewManager, input)

    expect(repaired.id).not.toBe(initial.id)
    expect(repaired.httpsPort).toBe(initial.httpsPort)
    expect(fake.funnels.has(repaired.httpsPort)).toBe(false)
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(2)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
  })

  it('sweeps inactive Factory owners while preserving active issue routes', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const orphan = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-orphan', issueKey: 'AR-2', service: 'two', repo: 'two', targetPort: 3_002,
    })

    const report = await previewManager.sweep({ namespace: 'factory-test', activeOwners: ['owner-active'] })

    expect(report.reaped).toEqual([orphan])
    expect(report.skipped).toEqual([])
    expect(fake.routes.has(active.httpsPort)).toBe(true)
    expect(fake.routes.has(orphan.httpsPort)).toBe(false)
  })

  it('clears an inactive active-state record when its route is already absent', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const previewManager = manager(fake.run, registryPath)
    const orphan = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-orphan', issueKey: 'AR-2', service: 'two', repo: 'two', targetPort: 3_002,
    })
    fake.routes.delete(orphan.httpsPort)

    const report = await previewManager.sweep({ namespace: 'factory-test', activeOwners: [] })

    expect(report).toEqual({ reaped: [orphan], skipped: [] })
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
  })

  it('restores a missing route for an active preview without replacing another configured port', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    fake.routes.delete(active.httpsPort)

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [active.id],
    })

    expect(report).toEqual({ reaped: [], skipped: [] })
    expect(fake.routes.get(active.httpsPort)?.targetPort).toBe(active.targetPort)
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(2)
  })

  it('rechecks listener ownership during active sweep and disables an unsafe exact route', async () => {
    const fake = fakeTailscale()
    let ownership: PreviewListenerOwnership = 'owned'
    const previewManager = manager(
      fake.run,
      join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'),
      {},
      async () => ownership,
    )
    const active = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    ownership = 'unrelated'

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [active.id],
    })

    expect(report.reaped).toEqual([])
    expect(report.skipped).toEqual([expect.objectContaining({
      id: active.id,
      reason: expect.stringContaining('outside the managed preview tree'),
    })])
    expect(fake.routes.has(active.httpsPort)).toBe(false)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
  })

  it('switches an active exact route from public Funnel back to tailnet-only Serve', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    fake.funnels.add(active.httpsPort)

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [active.id],
    })

    expect(report).toEqual({ reaped: [], skipped: [] })
    expect(fake.routes.get(active.httpsPort)?.targetPort).toBe(active.targetPort)
    expect(fake.funnels.has(active.httpsPort)).toBe(false)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(2)
  })

  it('refuses to repair an active preview after its HTTPS port is repurposed', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    fake.routes.set(active.httpsPort, { targetPort: 9_999, host: 'factory-node.example.ts.net' })

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [active.id],
    })

    expect(report.reaped).toEqual([])
    expect(report.skipped).toEqual([expect.objectContaining({
      id: active.id,
      reason: expect.stringContaining('HTTPS port has been repurposed'),
    })])
    expect(fake.routes.get(active.httpsPort)?.targetPort).toBe(9_999)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(0)
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(1)
  })

  it('retains a recent preview for an active owner while its exact ID is not yet durable', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const provisioning = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [],
    })

    expect(report).toEqual({ reaped: [], skipped: [] })
    expect(fake.routes.has(provisioning.httpsPort)).toBe(true)
  })

  it('does not grant provisioning grace to a future-dated registry record', async () => {
    const fake = fakeTailscale()
    let now = new Date('2026-07-20T12:00:00.000Z')
    const previewManager = manager(
      fake.run,
      join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'),
      { now: () => now },
    )
    const futureDated = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    now = new Date('2026-07-20T11:59:00.000Z')

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [],
    })

    expect(report).toEqual({ reaped: [futureDated], skipped: [] })
    expect(fake.routes.has(futureDated.httpsPort)).toBe(false)
  })

  it('reaps duplicate references for an active owner when their ids are not durable', async () => {
    const fake = fakeTailscale()
    let now = new Date('2026-07-20T12:00:00.000Z')
    const previewManager = manager(
      fake.run,
      join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'),
      { now: () => now },
    )
    const durable = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const crashGapDuplicate = await start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'two', repo: 'two', targetPort: 3_002,
    })
    now = new Date('2026-07-20T12:11:00.000Z')

    const report = await previewManager.sweep({
      namespace: 'factory-test',
      activeOwners: ['owner-active'],
      activePreviewIds: [durable.id],
    })

    expect(report.reaped).toEqual([crashGapDuplicate])
    expect(fake.routes.has(durable.httpsPort)).toBe(true)
    expect(fake.routes.has(crashGapDuplicate.httpsPort)).toBe(false)
  })

  it('never sweeps previews owned by another Factory workspace namespace', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const workspaceA = await start(previewManager, {
      namespace: 'workspace-a',
      owner: 'owner-a', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const workspaceB = await start(previewManager, {
      namespace: 'workspace-b',
      owner: 'owner-b', issueKey: 'AR-2', service: 'two', repo: 'two', targetPort: 3_002,
    })

    const report = await previewManager.sweep({ namespace: 'workspace-b', activeOwners: [] })

    expect(report.reaped).toEqual([workspaceB])
    expect(fake.routes.has(workspaceA.httpsPort)).toBe(true)
    expect(fake.routes.has(workspaceB.httpsPort)).toBe(false)
  })

  it('reaps a live route left after an interrupted pending start', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    fake.routes.set(10_000, { targetPort: 3_000, host: 'factory-node.example.ts.net' })
    writeFileSync(registryPath, JSON.stringify({
      version: 1,
      previews: [{
        id: 'pending-preview',
        provider: 'tailscale-serve',
        namespace: 'factory-test',
        owner: 'owner-crashed',
        service: 'factory',
        repo: 'factory',
        configuredTargetPort: 3_000,
        targetPort: 3_000,
        httpsPort: 10_000,
        access: 'tailnet',
        lifetime: 'issue',
        createdAt: '2026-07-20T12:00:00.000Z',
        startCommand: 'npm run dev',
        managedBy: '@agent-relay/factory',
        state: 'pending',
      }],
    }))
    const previewManager = manager(fake.run, registryPath)

    const report = await previewManager.sweep({ namespace: 'factory-test', activeOwners: [] })

    expect(report.reaped).toEqual([expect.objectContaining({ id: 'pending-preview' })])
    expect(fake.routes.has(10_000)).toBe(false)
  })

  it('retains pending identity when a successful Serve command is not yet observable', async () => {
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    let routeVisible = false
    const run: PreviewCommandRunner = async (_file, args) => {
      if (args.join(' ') === 'serve status --json') {
        return {
          stdout: JSON.stringify(routeVisible
            ? {
                TCP: { 10_000: { HTTPS: true } },
                Web: {
                  'factory-node.example.ts.net:10000': {
                    Handlers: { '/': { Proxy: 'http://127.0.0.1:3000' } },
                  },
                },
              }
            : { TCP: {}, Web: {} }),
          stderr: '',
        }
      }
      if (args.includes('--bg')) return { stdout: '', stderr: '' }
      if (args.at(-1) === 'off') return { stdout: '', stderr: '' }
      throw new Error(`unexpected tailscale command: ${args.join(' ')}`)
    }
    const previewManager = manager(run, registryPath)

    await expect(start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-interrupted', issueKey: 'AR-3', service: 'factory', repo: 'factory', targetPort: 3_000,
    })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

    const registry = JSON.parse(readFileSync(registryPath, 'utf8')) as {
      previews: Array<{ owner: string; state: string }>
    }
    expect(registry.previews).toEqual([expect.objectContaining({
      owner: 'owner-interrupted',
      state: 'pending',
    })])

    await expect(previewManager.sweep({ namespace: 'factory-test', activeOwners: [] })).resolves.toMatchObject({
      reaped: [],
      skipped: [{ reason: 'live route is not currently observable; retained for retry' }],
    })
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toHaveLength(1)

    routeVisible = true
    await expect(previewManager.sweep({ namespace: 'factory-test', activeOwners: [] })).resolves.toMatchObject({
      reaped: [expect.objectContaining({ owner: 'owner-interrupted' })],
      skipped: [],
    })
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
  })

  it('rolls back a route when the Serve runner rejects after mutating provider state', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    let rejectAfterMutation = true
    const run: PreviewCommandRunner = async (file, args) => {
      const result = await fake.run(file, args)
      if (rejectAfterMutation && args.includes('--bg')) {
        rejectAfterMutation = false
        throw new Error('runner transport closed after provider mutation')
      }
      return result
    }
    const previewManager = manager(run, registryPath)

    await expect(start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-interrupted',
      issueKey: 'AR-4',
      service: 'factory',
      repo: 'factory',
      targetPort: 3_000,
    })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

    expect(fake.routes).toHaveLength(0)
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
    expect(fake.calls.filter((args) => args.at(-1) === 'off').at(-1)).toContain('--set-path=/')
  })

  it('clears pending intent when the Serve runner rejects before provider mutation', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const run: PreviewCommandRunner = async (file, args) => {
      if (args.includes('--bg')) throw new Error('serve rejected before mutation')
      return await fake.run(file, args)
    }
    const previewManager = manager(run, registryPath)

    await expect(start(previewManager, {
      namespace: 'factory-test',
      owner: 'owner-before-mutation',
      issueKey: 'AR-5',
      service: 'factory',
      repo: 'factory',
      targetPort: 3_000,
    })).rejects.toThrow('Unable to allocate a Tailscale Serve HTTPS port')

    expect(fake.routes).toHaveLength(0)
    expect((JSON.parse(readFileSync(registryPath, 'utf8')) as { previews: unknown[] }).previews).toEqual([])
  })
})
