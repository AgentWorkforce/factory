import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { loadKubernetesStackDescriptor } from './stack-descriptor'

describe('Kubernetes stack descriptor', () => {
  it('loads Helm, kustomize, and manifest deployments with BYOC as the safe default', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-stack-'))
    try {
      const path = join(root, 'verification-stack.yaml')
      await writeFile(path, [
        'name: api',
        'deployKind: kubernetes',
        'deployment:',
        '  strategy: helm',
        '  chart: deploy/chart',
        'endpoints:',
        '  - name: api',
        '    service: api',
        '    port: 8080',
      ].join('\n'))

      const descriptor = await loadKubernetesStackDescriptor(path)
      expect(descriptor).toMatchObject({
        apiVersion: 'factory.agentworkforce.dev/v1alpha1',
        deployKind: 'kubernetes',
        target: 'byoc',
        deployment: { strategy: 'helm', chart: 'deploy/chart' },
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })

  it('rejects checkout escapes and inline secret-shaped values with actionable paths', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-stack-invalid-'))
    try {
      const path = join(root, 'verification-stack.yaml')
      await writeFile(path, [
        'name: api',
        'deployKind: kubernetes',
        'deployment:',
        '  strategy: manifests',
        '  paths: [../prod.yaml]',
        'secrets:',
        '  - name: db',
        '    key: password',
        '    secretRef: "password=hunter2"',
      ].join('\n'))

      await expect(loadKubernetesStackDescriptor(path)).rejects.toThrow(/deployment.paths.0.*within the repository/)
      await expect(loadKubernetesStackDescriptor(path)).rejects.toThrow(/secrets.0.secretRef.*never inline/)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
