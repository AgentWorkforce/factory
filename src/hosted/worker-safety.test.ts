import { build } from 'esbuild'
import { readFile } from 'node:fs/promises'
import { describe, expect, it } from 'vitest'

describe('hosted Factory Worker entrypoints', () => {
  it('bundles hosted orchestration and narrow telemetry for Workers nodejs_compat', async () => {
    const result = await build({
      entryPoints: {
        hosted: 'src/hosted/index.ts',
        telemetry: 'src/telemetry.ts',
      },
      bundle: true,
      platform: 'browser',
      format: 'esm',
      target: 'es2022',
      outdir: 'out',
      external: ['node:crypto'],
      metafile: true,
      write: false,
      logLevel: 'silent',
    })

    expect(result.outputFiles.find(({ path }) => path.endsWith('/hosted.js'))?.text)
      .toContain('HostedFactoryLoop')
    expect(result.outputFiles.find(({ path }) => path.endsWith('/telemetry.js'))?.text)
      .toContain('FactoryCloudEventInputV1Schema')
    expect(Object.keys(result.metafile.inputs)).not.toContain('src/observability/outbox.ts')
    expect(Object.keys(result.metafile.inputs)).not.toContain('src/observability/instance-identity.ts')

    const packageJson = JSON.parse(await readFile(
      new URL('../../package.json', import.meta.url),
      'utf8',
    )) as { exports: Record<string, unknown> }
    expect(packageJson.exports['./telemetry']).toEqual({
      types: './dist/telemetry.d.ts',
      import: './dist/telemetry.js',
    })
  })
})
