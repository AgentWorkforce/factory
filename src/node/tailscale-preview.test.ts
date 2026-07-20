import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { TailscalePreviewManager, type PreviewCommandRunner } from './tailscale-preview'

type Route = { targetPort: number; host: string }

function fakeTailscale() {
  const routes = new Map<number, Route>()
  const funnels = new Set<number>()
  const foreignTcp = new Map<number, Record<string, unknown>>()
  const foregroundTcp = new Map<number, Record<string, unknown>>()
  const calls: string[][] = []
  const run: PreviewCommandRunner = async (_file, args) => {
    calls.push(args)
    if (args.join(' ') === 'serve status --json') {
      return {
        stdout: JSON.stringify({
          TCP: Object.fromEntries([
            ...[...routes].map(([port]) => [port, { HTTPS: true }] as const),
            ...foreignTcp,
          ]),
          Web: Object.fromEntries([...routes].map(([port, route]) => [
            `${route.host}:${port}`,
            { Handlers: { '/': { Proxy: `http://127.0.0.1:${route.targetPort}` } } },
          ])),
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
      return { stdout: '', stderr: '' }
    }
    if (args.at(-1) === 'off') {
      routes.delete(httpsPort)
      funnels.delete(httpsPort)
      return { stdout: '', stderr: '' }
    }
    throw new Error(`unexpected tailscale command: ${args.join(' ')}`)
  }
  return { calls, foreignTcp, foregroundTcp, funnels, routes, run }
}

function manager(run: PreviewCommandRunner, registryPath: string) {
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
    now: () => new Date('2026-07-20T12:00:00.000Z'),
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

    const first = await previewManager.start(input)
    const second = await previewManager.start(input)

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

  it('removes only an exact live route identity', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const preview = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-1',
      issueKey: 'AR-129',
      service: 'factory',
      repo: 'AgentWorkforce/factory',
      targetPort: 3_000,
    })

    fake.routes.set(preview.httpsPort, { targetPort: 9_999, host: 'factory-node.example.ts.net' })

    await expect(previewManager.remove(preview)).resolves.toBe(false)
    expect(fake.routes.get(preview.httpsPort)?.targetPort).toBe(9_999)
    expect(fake.calls.some((args) => args.at(-1) === 'off')).toBe(false)
  })

  it('allocates distinct upstream ports for concurrent issues of the same service', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const first = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
    })
    const second = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-2', issueKey: 'AR-2', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    expect(first).toMatchObject({ configuredTargetPort: 3_000, targetPort: 3_000 })
    expect(second).toMatchObject({ configuredTargetPort: 3_000, targetPort: 3_001 })
    expect(fake.routes.get(first.httpsPort)?.targetPort).toBe(3_000)
    expect(fake.routes.get(second.httpsPort)?.targetPort).toBe(3_001)
  })

  it('serializes allocation across manager instances sharing one node registry', async () => {
    const fake = fakeTailscale()
    const registryPath = join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json')
    const firstManager = manager(fake.run, registryPath)
    const secondManager = manager(fake.run, registryPath)

    const [first, second] = await Promise.all([
      firstManager.start({
        namespace: 'factory-test',
        owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
      }),
      secondManager.start({
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

    const preview = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-1', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    expect(preview.httpsPort).toBe(10_002)
  })

  it('replaces a changed service target without leaving the old route behind', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const initial = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    const replaced = await previewManager.start({
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
    const initial = await previewManager.start(input)
    fake.funnels.add(initial.httpsPort)

    const repaired = await previewManager.start(input)

    expect(repaired.id).not.toBe(initial.id)
    expect(repaired.httpsPort).toBe(initial.httpsPort)
    expect(fake.funnels.has(repaired.httpsPort)).toBe(false)
    expect(fake.calls.filter((args) => args.includes('--bg'))).toHaveLength(2)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
  })

  it('sweeps inactive Factory owners while preserving active issue routes', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const orphan = await previewManager.start({
      namespace: 'factory-test',
      owner: 'owner-orphan', issueKey: 'AR-2', service: 'two', repo: 'two', targetPort: 3_002,
    })

    const report = await previewManager.sweep({ namespace: 'factory-test', activeOwners: ['owner-active'] })

    expect(report.reaped).toEqual([orphan])
    expect(report.skipped).toEqual([])
    expect(fake.routes.has(active.httpsPort)).toBe(true)
    expect(fake.routes.has(orphan.httpsPort)).toBe(false)
  })

  it('never sweeps previews owned by another Factory workspace namespace', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const workspaceA = await previewManager.start({
      namespace: 'workspace-a',
      owner: 'owner-a', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const workspaceB = await previewManager.start({
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
    const run: PreviewCommandRunner = async (_file, args) => {
      if (args.join(' ') === 'serve status --json') {
        return { stdout: JSON.stringify({ TCP: {}, Web: {} }), stderr: '' }
      }
      if (args.includes('--bg')) return { stdout: '', stderr: '' }
      throw new Error(`unexpected tailscale command: ${args.join(' ')}`)
    }
    const previewManager = manager(run, registryPath)

    await expect(previewManager.start({
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
      skipped: [{ reason: 'live route identity mismatch' }],
    })
  })
})
