import { execFileSync } from 'node:child_process'
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  DEFAULT_VERIFICATION_STACK_PATH,
  VERIFICATION_STACK_JSON_SCHEMA_URL,
  loadVerificationStack,
  parseVerificationStack,
  resolveVerificationStackDescriptor,
  VerificationStackDescriptorError,
} from './stack-descriptor'

function descriptor(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    apiVersion: 'factory.agentworkforce.dev/v1alpha1',
    kind: 'VerificationStack',
    name: 'sample',
    source: { type: 'manifests', paths: ['k8s/stack.yaml'] },
    services: [{
      name: 'web',
      workload: { kind: 'deployment' },
      readiness: { type: 'http', port: 8080, path: '/health' },
    }],
    endpoints: [{ name: 'web', service: 'web', port: 8080, path: '/health' }],
    ...overrides,
  }
}

describe('verification-stack descriptor', () => {
  it('loads a typed descriptor and applies bounded probe defaults', () => {
    const loaded = loadVerificationStack(descriptor())

    expect(loaded.source).toEqual({ type: 'manifests', paths: ['k8s/stack.yaml'] })
    expect(loaded.services[0].readiness).toMatchObject({
      type: 'http',
      path: '/health',
      scheme: 'http',
      expectedStatuses: [200],
      timeoutSeconds: 120,
      intervalSeconds: 2,
    })
    expect(loaded.secrets).toEqual([])
    expect(loaded.seeds).toEqual([])
  })

  it('ships the public draft-2020-12 JSON Schema', async () => {
    const schema = JSON.parse(await readFile(VERIFICATION_STACK_JSON_SCHEMA_URL, 'utf8'))

    expect(schema.$schema).toBe('https://json-schema.org/draft/2020-12/schema')
    expect(schema.properties.source.$ref).toBe('#/$defs/source')
    expect(schema.properties.verification.$ref).toBe('#/$defs/verification')
    expect(schema.$defs.reference.properties).not.toHaveProperty('value')
  })

  it('validates and defaults the optional merge-gate stages', () => {
    const loaded = loadVerificationStack(descriptor({
      verification: {
        e2e: { command: 'npm', args: ['run', 'test:e2e'] },
        load: { profile: '.factory/load.yaml' },
      },
    }))

    expect(loaded.verification).toEqual({
      environmentTtlSeconds: 900,
      e2e: { command: 'npm', args: ['run', 'test:e2e'], env: {}, timeoutSeconds: 300 },
      load: { profile: '.factory/load.yaml', timeoutSeconds: 300 },
      overallTimeoutSeconds: 900,
      teardownTimeoutSeconds: 120,
    })
  })

  it('fails closed with paths for inline secrets, unknown keys, and dangling services', () => {
    expect(() => loadVerificationStack(descriptor({
      secrets: [{ name: 'database', data: { PASSWORD: 'inline-secret' } }],
    }), 'fixture.yaml')).toThrowError(/fixture\.yaml:[\s\S]*secrets\.0\.data\.PASSWORD/u)

    expect(() => loadVerificationStack(descriptor({ surprise: true }))).toThrowError(/<root>.*Unrecognized key/u)

    expect(() => loadVerificationStack(descriptor({
      endpoints: [{ name: 'api', service: 'missing', port: 80 }],
    }))).toThrowError(/endpoints\.0\.service.*undeclared service "missing"/u)

    expect(() => loadVerificationStack(descriptor({
      config: [{ name: 'application', data: { 'invalid/key': { ref: 'config://application/key' } } }],
    }))).toThrowError(/config\.0\.data\.invalid\/key.*letters, numbers, dots/u)
  })

  it('reports malformed YAML without obscuring the descriptor source', () => {
    expect(() => parseVerificationStack('services: [', 'broken.yaml')).toThrowError(
      /Could not parse verification-stack descriptor broken\.yaml/u,
    )
  })

  it('resolves the default path and an explicit repository-relative override', async () => {
    const repo = await mkdtemp(join(tmpdir(), 'factory-stack-'))
    await mkdir(join(repo, '.factory'), { recursive: true })
    await mkdir(join(repo, 'ops'), { recursive: true })
    await writeFile(join(repo, DEFAULT_VERIFICATION_STACK_PATH), yamlDescriptor('default-stack'))
    await writeFile(join(repo, 'ops/verify.yaml'), yamlDescriptor('override-stack'))

    const defaultStack = await resolveVerificationStackDescriptor({ repoPath: repo })
    const overrideStack = await resolveVerificationStackDescriptor({
      repoPath: repo,
      descriptorPath: 'ops/verify.yaml',
    })

    expect(defaultStack.descriptor.name).toBe('default-stack')
    expect(defaultStack.descriptorPath).toBe(DEFAULT_VERIFICATION_STACK_PATH)
    expect(overrideStack.descriptor.name).toBe('override-stack')
    await expect(resolveVerificationStackDescriptor({
      repoPath: repo,
      descriptorPath: '../outside.yaml',
    })).rejects.toThrow(/must stay inside the repository/u)
  })

  it('selects the descriptor committed at the requested branch or SHA', async () => {
    const repo = await mkdtemp(join(tmpdir(), 'factory-stack-git-'))
    await mkdir(join(repo, '.factory'), { recursive: true })
    execFileSync('git', ['init', '-q', repo])
    execFileSync('git', ['-C', repo, 'config', 'user.name', 'Factory Test'])
    execFileSync('git', ['-C', repo, 'config', 'user.email', 'factory@example.test'])
    await writeFile(join(repo, DEFAULT_VERIFICATION_STACK_PATH), yamlDescriptor('committed-stack'))
    execFileSync('git', ['-C', repo, 'add', DEFAULT_VERIFICATION_STACK_PATH])
    execFileSync('git', ['-C', repo, 'commit', '-qm', 'fixture'])
    await writeFile(join(repo, DEFAULT_VERIFICATION_STACK_PATH), yamlDescriptor('working-tree-stack'))

    const selected = await resolveVerificationStackDescriptor({ repoPath: repo, ref: 'HEAD' })

    expect(selected.descriptor.name).toBe('committed-stack')
    expect(selected.ref).toMatch(/^[a-f0-9]{40}$/u)
  })

  it('uses a dedicated actionable error type', () => {
    expect(() => loadVerificationStack(null)).toThrow(VerificationStackDescriptorError)
  })
})

function yamlDescriptor(name: string): string {
  return `apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: ${name}
source:
  type: manifests
  paths: [k8s/stack.yaml]
services:
  - name: web
    workload:
      kind: deployment
    readiness:
      type: tcp
      port: 8080
`
}
