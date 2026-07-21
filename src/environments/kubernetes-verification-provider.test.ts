import { describe, expect, it } from 'vitest'

import type { CommandRunner, RunCommandOptions } from './kubernetes-command'
import { KubernetesVerificationEnvironmentProvider } from './kubernetes-verification-provider'

class FakeRunner implements CommandRunner {
  calls: Array<{ command: string; args: string[]; options?: RunCommandOptions }> = []

  constructor(readonly inventory = '{"items":[]}', readonly phase = 'Active') {}

  async run(command: string, args: string[], options?: RunCommandOptions) {
    this.calls.push({ command, args, options })
    if (args.includes('--output') && args.includes('json')) return { stdout: this.inventory, stderr: '' }
    return { stdout: args.includes('jsonpath={.status.phase}') ? this.phase : '', stderr: '' }
  }
}

describe('KubernetesVerificationEnvironmentProvider', () => {
  it('provisions a namespace, reports status, and destroys idempotently', async () => {
    const runner = new FakeRunner()
    const provider = new KubernetesVerificationEnvironmentProvider({
      context: 'kind-factory',
      namespacePrefix: 'factory-test',
      defaultTtl: 12_345,
      commandRunner: runner,
    })

    const environment = await provider.provision({
      id: 'Issue-143',
      labels: { purpose: 'verification' },
      bindings: { owner: 'run-1' },
    })

    expect(environment).toMatchObject({
      id: 'issue-143',
      namespace: 'factory-test-issue-143',
      status: 'ready',
      ttl: 12_345,
      bindings: { owner: 'run-1' },
      target: {
        type: 'kubernetes',
        namespace: 'factory-test-issue-143',
        context: 'kind-factory',
      },
    })
    const apply = runner.calls.find((call) => call.args.includes('apply'))
    expect(JSON.parse(apply?.options?.input ?? '{}')).toMatchObject({
      metadata: {
        labels: { 'factory.agent-relay.dev/managed': 'true' },
        annotations: { 'factory.agent-relay.dev/expires-at': expect.any(String) },
      },
    })
    await expect(provider.status(environment.id)).resolves.toBe('ready')
    await provider.destroy(environment.id)
    await provider.destroy(environment.id)

    const deleteCalls = runner.calls.filter((call) => call.args.includes('delete'))
    expect(deleteCalls).toHaveLength(2)
    expect(deleteCalls[0].args).toContain('--ignore-not-found=true')
  })

  it('reports a terminating namespace as destroying', async () => {
    const provider = new KubernetesVerificationEnvironmentProvider({
      commandRunner: new FakeRunner('{"items":[]}', 'Terminating'),
    })

    await expect(provider.status('terminating')).resolves.toBe('destroying')
  })

  it('fails before namespace creation when the cluster-wide environment cap is reached', async () => {
    const runner = new FakeRunner(JSON.stringify({
      items: [
        { metadata: { name: 'factory-test-live' } },
        { metadata: { name: 'factory-test-deleting', deletionTimestamp: '2026-07-21T00:00:00Z' } },
      ],
    }))
    const provider = new KubernetesVerificationEnvironmentProvider({
      maxActiveEnvironments: 1,
      commandRunner: runner,
    })

    await expect(provider.provision({ id: 'capped' })).rejects.toThrow(
      'verification environment concurrency cap reached (1/1)',
    )
    expect(runner.calls.some((call) => call.args.includes('apply'))).toBe(false)
  })

  it('rejects invalid environment capacity configuration', () => {
    expect(() => new KubernetesVerificationEnvironmentProvider({ maxActiveEnvironments: 0 })).toThrow(
      'maxActiveEnvironments must be a positive integer',
    )
  })
})
