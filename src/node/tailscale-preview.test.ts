import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { TailscalePreviewManager, type PreviewCommandRunner } from './tailscale-preview'

type Route = { targetPort: number; host: string }

function fakeTailscale() {
  const routes = new Map<number, Route>()
  const calls: string[][] = []
  const run: PreviewCommandRunner = async (_file, args) => {
    calls.push(args)
    if (args.join(' ') === 'serve status --json') {
      return {
        stdout: JSON.stringify({
          TCP: Object.fromEntries([...routes].map(([port]) => [port, { HTTPS: true }])),
          Web: Object.fromEntries([...routes].map(([port, route]) => [
            `${route.host}:${port}`,
            { Handlers: { '/': { Proxy: `http://127.0.0.1:${route.targetPort}` } } },
          ])),
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
      return { stdout: '', stderr: '' }
    }
    throw new Error(`unexpected tailscale command: ${args.join(' ')}`)
  }
  return { calls, routes, run }
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
    now: () => new Date('2026-07-20T12:00:00.000Z'),
  })
}

describe('TailscalePreviewManager', () => {
  it('creates and reuses a background tailnet-only route for one issue owner', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const input = {
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

  it('replaces a changed service target without leaving the old route behind', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const initial = await previewManager.start({
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 3_000,
    })

    const replaced = await previewManager.start({
      owner: 'owner-1', issueKey: 'AR-129', service: 'factory', repo: 'factory', targetPort: 4_173,
    })

    expect(replaced.id).not.toBe(initial.id)
    expect(replaced.httpsPort).toBe(initial.httpsPort)
    expect(fake.routes.get(replaced.httpsPort)?.targetPort).toBe(4_173)
    expect(fake.calls.filter((args) => args.at(-1) === 'off')).toHaveLength(1)
  })

  it('sweeps inactive Factory owners while preserving active issue routes', async () => {
    const fake = fakeTailscale()
    const previewManager = manager(fake.run, join(mkdtempSync(join(tmpdir(), 'factory-preview-')), 'registry.json'))
    const active = await previewManager.start({
      owner: 'owner-active', issueKey: 'AR-1', service: 'one', repo: 'one', targetPort: 3_001,
    })
    const orphan = await previewManager.start({
      owner: 'owner-orphan', issueKey: 'AR-2', service: 'two', repo: 'two', targetPort: 3_002,
    })

    const report = await previewManager.sweep(['owner-active'])

    expect(report.reaped).toEqual([orphan])
    expect(report.skipped).toEqual([])
    expect(fake.routes.has(active.httpsPort)).toBe(true)
    expect(fake.routes.has(orphan.httpsPort)).toBe(false)
  })
})
