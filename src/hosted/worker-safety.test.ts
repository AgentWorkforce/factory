import { build } from 'esbuild'
import { describe, expect, it } from 'vitest'

describe('hosted Factory entrypoint', () => {
  it('bundles for a worker/browser runtime without Node built-ins', async () => {
    const result = await build({
      entryPoints: ['src/hosted/index.ts'],
      bundle: true,
      platform: 'browser',
      format: 'esm',
      target: 'es2022',
      write: false,
      logLevel: 'silent',
    })

    expect(result.outputFiles[0]?.text).toContain('HostedFactoryLoop')
  })
})
