import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { loadVerificationGateStack } from './verification-stack'

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map(async (root) => await rm(root, { recursive: true, force: true })))
})

describe('loadVerificationGateStack', () => {
  it('normalizes live-gate stages from the repository-owned deployment descriptor', async () => {
    const root = await repositoryWith(gateDescriptor())

    await expect(loadVerificationGateStack(root)).resolves.toMatchObject({
      repositoryPath: root,
      environmentTtlMs: 120_000,
      loaded: {
        descriptor: {
          name: 'factory-test',
          source: { type: 'manifests', paths: ['deploy/stack.yaml'] },
          endpoints: [{ name: 'api', service: 'api', port: 8080 }],
        },
      },
      e2e: { command: 'npm', args: ['run', 'e2e'], timeoutMs: 45_000 },
      load: { profilePath: join(root, 'load/profile.yaml'), timeoutMs: 60_000 },
      timeouts: { overallMs: 600_000, teardownMs: 90_000 },
    })
  })

  it('fails closed when deployment endpoints or required gate stages are absent', async () => {
    const withoutGate = await repositoryWith(gateDescriptor().replace(/verification:[\s\S]*$/u, ''))
    await expect(loadVerificationGateStack(withoutGate)).rejects.toThrow(/required verification section/u)

    const withoutEndpoints = await repositoryWith(gateDescriptor().replace(
      /endpoints:[\s\S]*?verification:/u,
      'endpoints: []\nverification:',
    ))
    await expect(loadVerificationGateStack(withoutEndpoints)).rejects.toThrow(/at least one endpoint/u)
  })

  it('rejects descriptor traversal outside the repository', async () => {
    const root = await repositoryWith(gateDescriptor())
    await expect(loadVerificationGateStack(root, '../outside.yaml')).rejects.toThrow(/must stay inside the repository/u)
  })
})

async function repositoryWith(descriptor: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-verification-stack-'))
  roots.push(root)
  await mkdir(join(root, '.factory'), { recursive: true })
  await writeFile(join(root, '.factory', 'verification-stack.yaml'), descriptor, 'utf8')
  return root
}

function gateDescriptor(): string {
  return `apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: factory-test
source:
  type: manifests
  paths: [deploy/stack.yaml]
services:
  - name: api
    workload: { kind: deployment }
    readiness: { type: http, port: 8080 }
endpoints:
  - name: api
    service: api
    port: 8080
verification:
  environmentTtlSeconds: 120
  e2e:
    command: npm
    args: [run, e2e]
    timeoutSeconds: 45
  load:
    profile: load/profile.yaml
    timeoutSeconds: 60
  overallTimeoutSeconds: 600
  teardownTimeoutSeconds: 90
`
}
