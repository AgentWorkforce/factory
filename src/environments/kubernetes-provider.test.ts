import { describe, expect, it } from 'vitest'

import type { CommandRunner, RunCommandOptions } from './kubernetes-command'
import { KubernetesEnvironmentProvider } from './kubernetes-provider'

class FakeRunner implements CommandRunner {
  calls: Array<{ command: string; args: string[]; options?: RunCommandOptions }> = []

  async run(command: string, args: string[], options?: RunCommandOptions) {
    this.calls.push({ command, args, options })
    return { stdout: args.includes('jsonpath={.status.phase}') ? 'Active' : '', stderr: '' }
  }
}

describe('KubernetesEnvironmentProvider', () => {
  it('provisions a namespace, reports status, and destroys idempotently', async () => {
    const runner = new FakeRunner()
    const provider = new KubernetesEnvironmentProvider({
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
    await expect(provider.status(environment.id)).resolves.toBe('ready')
    await provider.destroy(environment.id)
    await provider.destroy(environment.id)

    const deleteCalls = runner.calls.filter((call) => call.args.includes('delete'))
    expect(deleteCalls).toHaveLength(2)
    expect(deleteCalls[0].args).toContain('--ignore-not-found=true')
  })
})
