import { describe, expect, it } from 'vitest'

// Guards that the published dist is raw-Node ESM runnable. `tsc` emits
// extensionless relative imports (moduleResolution: Bundler); the build runs
// tsc-alias afterward to add `.js` extensions so Node's ESM loader can resolve
// them. Without that step these imports throw ERR_MODULE_NOT_FOUND. Requires a
// prior `npm run build` (CI builds before testing).
describe('published dist entrypoints', () => {
  it('are importable by Node ESM consumers', async () => {
    const main = await import('../../dist/index.js')
    const testing = await import('../../dist/testing/index.js')
    const writeback = await import('../../dist/writeback/index.js')

    expect(main.FactoryConfigSchema).toBeDefined()
    expect(main.createFactory).toBeTypeOf('function')
    expect(main.createFleet).toBeTypeOf('function')
    expect(testing.FakeFleetClient).toBeTypeOf('function')
    expect(writeback.MountLinearWriteback).toBeTypeOf('function')
  })
})
