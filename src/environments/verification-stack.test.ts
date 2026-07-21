import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { loadVerificationStack } from './verification-stack'

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map(async (root) => await rm(root, { recursive: true, force: true })))
})

describe('loadVerificationStack', () => {
  it('resolves repository-relative files and duration safety controls', async () => {
    const root = await repositoryWith(`
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
deploy:
  manifests: [deploy/stack.yaml]
  endpoints:
    api: { service: api, port: 8080 }
e2e: { command: npm, args: [run, e2e], timeout: 45s }
load: { profile: load/profile.yaml, timeout: 1m }
timeouts: { overall: 10m, teardown: 90s }
`)

    await expect(loadVerificationStack(root)).resolves.toMatchObject({
      repositoryPath: root,
      provision: { namespacePrefix: 'factory-verify', ttlMs: 15 * 60_000 },
      deploy: {
        manifests: [{ path: join(root, 'deploy/stack.yaml') }],
        endpoints: { api: { service: 'api', port: 8080, scheme: 'http', portForward: true } },
      },
      e2e: { timeoutMs: 45_000 },
      load: { profilePath: join(root, 'load/profile.yaml'), timeoutMs: 60_000 },
      timeouts: { overallMs: 10 * 60_000, teardownMs: 90_000 },
    })
  })

  it('rejects files that escape the feature checkout', async () => {
    const root = await repositoryWith(`
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
deploy:
  manifests: [../cluster-admin.yaml]
  endpoints:
    api: { service: api, port: 8080 }
e2e: { command: npm }
load: { profile: load.yaml }
`)

    await expect(loadVerificationStack(root)).rejects.toThrow(/escapes the repository/)
  })
})

async function repositoryWith(descriptor: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-stack-'))
  roots.push(root)
  await mkdir(join(root, '.factory'), { recursive: true })
  await writeFile(join(root, '.factory', 'verification-stack.yaml'), descriptor, 'utf8')
  return root
}
