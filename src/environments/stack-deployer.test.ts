import { access } from 'node:fs/promises'

import { describe, expect, it } from 'vitest'

import type { Environment } from '../ports/environment'
import type { CommandRunner, RunCommandOptions } from './kubernetes-command'
import {
  StackDeploymentError,
  VerificationStackDeployer,
  type ManagedPortForward,
  type PortForwarder,
} from './stack-deployer'
import { loadVerificationStack, type VerificationStackDescriptor } from './stack-descriptor'

class RecordingRunner implements CommandRunner {
  readonly calls: Array<{ command: string; args: string[]; options?: RunCommandOptions }> = []

  async run(command: string, args: string[], options?: RunCommandOptions) {
    this.calls.push({ command, args, options })
    return { stdout: command === 'kompose' ? 'apiVersion: v1\nkind: Service\n' : '', stderr: '' }
  }
}

class FakePortForwarder implements PortForwarder {
  readonly calls: string[] = []
  closed = 0

  async forward(input: Parameters<PortForwarder['forward']>[0]): Promise<ManagedPortForward> {
    this.calls.push(`${input.service}:${input.remotePort}`)
    return {
      localPort: 41_234,
      close: async () => { this.closed += 1 },
    }
  }
}

function environment(): Environment {
  return {
    id: 'env-1',
    status: 'ready',
    createdAt: new Date().toISOString(),
    ttl: 60_000,
    endpoints: {},
    bindings: {},
    target: { type: 'kubernetes', namespace: 'factory-env-1', context: 'kind-test' },
  }
}

function descriptor(source: VerificationStackDescriptor['source'] = {
  type: 'manifests', paths: ['k8s/stack.yaml'],
}): VerificationStackDescriptor {
  return loadVerificationStack({
    apiVersion: 'factory.agentworkforce.dev/v1alpha1',
    kind: 'VerificationStack',
    name: 'sample',
    source,
    secrets: [{
      name: 'database',
      data: { PASSWORD: { ref: 'resource://database/password' } },
    }],
    services: [{
      name: 'web',
      workload: { kind: 'deployment' },
      readiness: { type: 'http', port: 8080, path: '/health', timeoutSeconds: 2 },
    }],
    seeds: [{
      type: 'exec',
      name: 'seed-database',
      service: 'web',
      command: ['app', 'seed'],
    }],
    endpoints: [{ name: 'web', service: 'web', port: 8080, path: '/health' }],
  })
}

describe('VerificationStackDeployer', () => {
  it('materializes references, applies, gates readiness, seeds, and resolves endpoints', async () => {
    const runner = new RecordingRunner()
    const forwards = new FakePortForwarder()
    const env = environment()
    const deployer = new VerificationStackDeployer({
      commandRunner: runner,
      portForwarder: forwards,
      referenceResolver: { resolve: async () => 'resolved-password' },
      fetch: async () => new Response('healthy', { status: 200 }),
    })

    const deployment = await deployer.deploy({
      descriptor: descriptor(),
      descriptorPath: '.factory/verification-stack.yaml',
      rootDir: '/repo',
    }, env)

    expect(deployment.endpoints).toEqual({ web: 'http://127.0.0.1:41234/health' })
    expect(env.endpoints).toEqual(deployment.endpoints)
    expect(forwards.calls).toEqual(['web:8080'])
    const secret = runner.calls.find((call) => call.options?.input?.includes('"kind":"Secret"'))
    expect(secret).toBeDefined()
    expect(JSON.parse(secret!.options!.input!).data.PASSWORD).toBe(
      Buffer.from('resolved-password').toString('base64'),
    )
    expect(runner.calls.some((call) => call.args.includes('/repo/k8s/stack.yaml'))).toBe(true)
    expect(runner.calls.some((call) => call.args.includes('seed-database'))).toBe(false)
    expect(runner.calls.some((call) => call.args.includes('app') && call.args.includes('seed'))).toBe(true)

    await deployment.dispose()
    await deployment.dispose()
    expect(forwards.closed).toBe(1)
  })

  it('fails before apply with a precise missing-secret diagnostic', async () => {
    const runner = new RecordingRunner()
    const deployer = new VerificationStackDeployer({
      commandRunner: runner,
      referenceResolver: { resolve: async () => undefined },
    })

    await expect(deployer.deploy(descriptor(), environment())).rejects.toMatchObject({
      name: 'StackDeploymentError',
      stage: 'references',
      message: expect.stringContaining(
        'Missing required secret reference "resource://database/password" for database.PASSWORD',
      ),
    })
    expect(runner.calls).toEqual([])
  })

  it('allows unresolved references only when every entry is optional', async () => {
    const runner = new RecordingRunner()
    const stack = descriptor()
    stack.secrets[0].data.PASSWORD.optional = true
    stack.services[0].readiness = {
      type: 'exec', command: ['true'], timeoutSeconds: 1, intervalSeconds: 0.1,
    }
    stack.seeds = []
    stack.endpoints = []

    await expect(new VerificationStackDeployer({ commandRunner: runner }).deploy(
      stack,
      environment(),
    )).resolves.toBeDefined()
    const secret = runner.calls.find((call) => call.options?.input?.includes('"kind":"Secret"'))
    expect(JSON.parse(secret!.options!.input!).data).toEqual({})
  })

  it('materializes descriptor assets from the selected Git ref and removes the checkout', async () => {
    const runner = new RecordingRunner()
    const stack = descriptor()
    stack.secrets = []
    stack.services[0].readiness = {
      type: 'exec', command: ['true'], timeoutSeconds: 1, intervalSeconds: 0.1,
    }
    stack.seeds = []
    stack.endpoints = []

    await new VerificationStackDeployer({ commandRunner: runner }).deploy({
      descriptor: stack,
      descriptorPath: '.factory/verification-stack.yaml',
      rootDir: '/repo',
      ref: '0123456789abcdef0123456789abcdef01234567',
    }, environment())

    expect(runner.calls[0]).toMatchObject({
      command: 'git',
      args: ['clone', '--quiet', '--shared', '--no-checkout', '/repo', expect.any(String)],
    })
    const checkout = runner.calls[0].args.at(-1)!
    expect(runner.calls[1].args).toEqual([
      '-C', checkout, 'checkout', '--quiet', '--detach',
      '0123456789abcdef0123456789abcdef01234567',
    ])
    expect(runner.calls.some((call) => call.args.includes(`${checkout}/k8s/stack.yaml`))).toBe(true)
    await expect(access(checkout)).rejects.toThrow()
  })

  it('bounds an unsatisfiable health probe and names the failed service', async () => {
    const runner = new RecordingRunner()
    const forwards = new FakePortForwarder()
    const unhealthy = descriptor()
    unhealthy.secrets = []
    unhealthy.services[0].readiness.timeoutSeconds = 1
    unhealthy.services[0].readiness.intervalSeconds = 0.1
    const deployer = new VerificationStackDeployer({
      commandRunner: runner,
      portForwarder: forwards,
      fetch: async () => new Response('no', { status: 503 }),
    })
    const started = Date.now()

    await expect(deployer.deploy(unhealthy, environment())).rejects.toEqual(expect.objectContaining({
      name: 'StackDeploymentError',
      stage: 'readiness',
      service: 'web',
      message: expect.stringMatching(/Service web readiness probe never became ready within 1s.*HTTP 503/u),
    }))
    expect(Date.now() - started).toBeLessThan(2_500)
    expect(forwards.closed).toBe(1)
  })

  it.each([
    ['manifests', { type: 'manifests', paths: ['k8s/all.yaml'] }],
    ['kustomize', { type: 'kustomize', path: 'k8s/overlays/test' }],
    ['helm', { type: 'helm', chart: 'charts/app', release: 'verify', valuesFiles: ['values.test.yaml'] }],
    ['docker-compose', { type: 'docker-compose', path: 'docker-compose.yml' }],
  ] as const)('applies a %s source without repository-specific logic', async (_name, source) => {
    const runner = new RecordingRunner()
    const stack = descriptor(source as VerificationStackDescriptor['source'])
    stack.secrets = []
    stack.services[0].readiness = {
      type: 'exec', command: ['true'], timeoutSeconds: 1, intervalSeconds: 0.1,
    }
    stack.seeds = []
    stack.endpoints = []

    const deployment = await new VerificationStackDeployer({ commandRunner: runner }).deploy({
      descriptor: stack,
      descriptorPath: '.factory/verification-stack.yaml',
      rootDir: '/repo',
    }, environment())

    const commands = runner.calls.map((call) => call.command)
    if (source.type === 'helm') expect(commands).toContain('helm')
    else if (source.type === 'docker-compose') expect(commands).toEqual(expect.arrayContaining(['kompose', 'kubectl']))
    else expect(commands).toContain('kubectl')
    await deployment.dispose()
  })

  it('rejects a non-Kubernetes environment clearly', async () => {
    const env = { ...environment(), target: { type: 'cloudflare' } }

    await expect(new VerificationStackDeployer().deploy(descriptor(), env)).rejects.toBeInstanceOf(
      StackDeploymentError,
    )
  })
})
