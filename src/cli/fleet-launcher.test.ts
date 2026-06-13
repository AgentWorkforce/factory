import { execFile } from 'node:child_process'
import { tmpdir } from 'node:os'
import { promisify } from 'node:util'
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

const launcherPath = join(import.meta.dirname, '..', '..', 'bin', 'fleet.mjs')
const repoRoot = join(import.meta.dirname, '..', '..', '..', '..')
const execFileAsync = promisify(execFile)

describe('fleet.mjs launcher', () => {
  it('externalizes package dependencies and keys the cache on bundled input contents', async () => {
    const source = await readFile(launcherPath, 'utf8')

    expect(source).toContain("packages: 'external'")
    expect(source).toContain("join(repoRoot, 'node_modules', '.cache', 'pear-factory-sdk')")
    expect(source).toContain('const launcher = fileURLToPath(import.meta.url)')
    expect(source).toContain('metafile: true')
    expect(source).toContain('metafileBuild.metafile.inputs')
    expect(source).toContain('readFileSync(inputPath)')
  })

  it('rebundles when a non-entry transitive source changes', async () => {
    const fixture = await createLauncherFixture('first')
    try {
      expect(await runFixtureFleet(fixture.repoRoot)).toBe('first')

      await writeFactorySource(fixture.packageRoot, 'second')

      expect(await runFixtureFleet(fixture.repoRoot)).toBe('second')
    } finally {
      await rm(fixture.root, { recursive: true, force: true })
    }
  })

  it('supports force rebuilding with FLEET_FORCE_BUILD and --rebuild', async () => {
    const fixture = await createLauncherFixture('force')
    try {
      expect(await runFixtureFleet(fixture.repoRoot)).toBe('force')
      await poisonCachedBundle(fixture.repoRoot)
      expect(await runFixtureFleet(fixture.repoRoot)).toBe('poison')

      expect(await runFixtureFleet(fixture.repoRoot, [], { FLEET_FORCE_BUILD: '1' })).toBe('force')

      await poisonCachedBundle(fixture.repoRoot)
      expect(await runFixtureFleet(fixture.repoRoot, ['--rebuild'])).toBe('force')
    } finally {
      await rm(fixture.root, { recursive: true, force: true })
    }
  })
})

async function createLauncherFixture(marker: string): Promise<{ root: string; repoRoot: string; packageRoot: string }> {
  const root = await mkdtemp(join(tmpdir(), 'fleet-launcher-cache-'))
  const fixtureRepoRoot = join(root, 'repo')
  const packageRoot = join(fixtureRepoRoot, 'packages', 'factory-sdk')
  await mkdir(join(packageRoot, 'bin'), { recursive: true })
  await mkdir(join(packageRoot, 'src', 'cli'), { recursive: true })
  await mkdir(join(packageRoot, 'src', 'orchestrator'), { recursive: true })
  await mkdir(join(fixtureRepoRoot, 'node_modules'), { recursive: true })
  await symlink(join(repoRoot, 'node_modules', 'esbuild'), join(fixtureRepoRoot, 'node_modules', 'esbuild'), 'dir')
  await symlink(join(repoRoot, 'node_modules', '@esbuild'), join(fixtureRepoRoot, 'node_modules', '@esbuild'), 'dir')
  await copyFile(launcherPath, join(packageRoot, 'bin', 'fleet.mjs'))
  await writeFile(join(packageRoot, 'src', 'cli', 'fleet.ts'), [
    "import { marker } from '../orchestrator/factory'",
    '',
    'export async function main(args: string[] = []): Promise<void> {',
    "  process.stdout.write(`${args.includes('--rebuild') ? 'leaked-rebuild-flag' : marker}\\n`)",
    '}',
    '',
  ].join('\n'))
  await writeFactorySource(packageRoot, marker)
  return { root, repoRoot: fixtureRepoRoot, packageRoot }
}

async function writeFactorySource(packageRoot: string, marker: string): Promise<void> {
  await writeFile(
    join(packageRoot, 'src', 'orchestrator', 'factory.ts'),
    `export const marker = ${JSON.stringify(marker)}\n`,
  )
}

async function runFixtureFleet(
  fixtureRepoRoot: string,
  args: string[] = [],
  env: Record<string, string> = {},
): Promise<string> {
  const launcher = join(fixtureRepoRoot, 'packages', 'factory-sdk', 'bin', 'fleet.mjs')
  const { stdout } = await execFileAsync(process.execPath, [launcher, ...args], {
    cwd: fixtureRepoRoot,
    env: { ...process.env, ...env },
    encoding: 'utf8',
  })
  return stdout.trim()
}

async function poisonCachedBundle(fixtureRepoRoot: string): Promise<void> {
  const cacheDir = join(fixtureRepoRoot, 'node_modules', '.cache', 'pear-factory-sdk')
  const cacheFiles = (await readdir(cacheDir)).filter((file) => /^fleet-[a-f0-9]+\.mjs$/.test(file))
  expect(cacheFiles).toHaveLength(1)
  await writeFile(
    join(cacheDir, cacheFiles[0]),
    "export async function main() { process.stdout.write('poison\\n') }\n",
  )
}
