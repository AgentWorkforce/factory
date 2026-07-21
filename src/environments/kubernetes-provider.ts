import { randomUUID } from 'node:crypto'

import type {
  Environment,
  EnvironmentProvider,
  EnvironmentSpec,
  EnvironmentStatus,
} from '../ports/environment.js'
import {
  CommandExecutionError,
  type CommandRunner,
  type KubernetesConnection,
  ProcessCommandRunner,
  kubectlConnectionArgs,
} from './kubernetes-command.js'

export interface KubernetesEnvironmentProviderOptions extends KubernetesConnection {
  namespacePrefix?: string
  defaultTtl?: number
  commandRunner?: CommandRunner
}

/** A small Kubernetes EnvironmentProvider used by verification-stack deployments. */
export class KubernetesEnvironmentProvider implements EnvironmentProvider {
  private readonly runner: CommandRunner
  private readonly namespacePrefix: string
  private readonly defaultTtl: number
  private readonly connection: KubernetesConnection
  private readonly environments = new Map<string, Environment>()

  constructor(options: KubernetesEnvironmentProviderOptions = {}) {
    this.runner = options.commandRunner ?? new ProcessCommandRunner()
    this.namespacePrefix = normalizeDnsLabel(options.namespacePrefix ?? 'factory-verification')
    this.defaultTtl = options.defaultTtl ?? 30 * 60_000
    this.connection = { kubeconfig: options.kubeconfig, context: options.context }
  }

  async provision(spec: EnvironmentSpec = {}): Promise<Environment> {
    const id = normalizeDnsLabel(spec.id ?? randomUUID())
    const namespace = `${this.namespacePrefix}-${id}`.slice(0, 63).replace(/-+$/u, '')
    const labels = {
      'app.kubernetes.io/managed-by': 'factory',
      'factory.agentworkforce.dev/environment-id': id,
      ...spec.labels,
    }

    await this.runner.run('kubectl', [
      ...kubectlConnectionArgs(this.connection),
      'create', 'namespace', namespace,
    ])

    try {
      for (const [key, value] of Object.entries(labels)) {
        await this.runner.run('kubectl', [
          ...kubectlConnectionArgs(this.connection),
          'label', 'namespace', namespace, `${key}=${value}`, '--overwrite',
        ])
      }
    } catch (error) {
      await this.deleteNamespace(namespace).catch(() => undefined)
      throw error
    }

    const environment: Environment = {
      id,
      status: 'ready',
      createdAt: new Date().toISOString(),
      ttl: spec.ttl ?? this.defaultTtl,
      endpoints: {},
      bindings: { ...spec.bindings },
      namespace,
      target: {
        type: 'kubernetes',
        namespace,
        ...this.connection,
      },
    }
    this.environments.set(id, environment)
    return environment
  }

  async status(id: string): Promise<EnvironmentStatus> {
    const environment = this.environments.get(normalizeDnsLabel(id))
    const namespace = environment?.namespace ?? this.namespaceFor(id)
    try {
      const result = await this.runner.run('kubectl', [
        ...kubectlConnectionArgs(this.connection),
        'get', 'namespace', namespace, '-o', 'jsonpath={.status.phase}',
      ])
      return result.stdout.trim() === 'Active' ? 'ready' : 'provisioning'
    } catch (error) {
      if (error instanceof CommandExecutionError && /not found/iu.test(error.stderr)) return 'destroyed'
      throw error
    }
  }

  async endpoints(id: string): Promise<Record<string, string>> {
    return { ...(this.environments.get(normalizeDnsLabel(id))?.endpoints ?? {}) }
  }

  async destroy(id: string): Promise<void> {
    const normalizedId = normalizeDnsLabel(id)
    const environment = this.environments.get(normalizedId)
    const namespace = environment?.namespace ?? this.namespaceFor(normalizedId)
    if (environment) environment.status = 'destroying'
    await this.deleteNamespace(namespace)
    if (environment) environment.status = 'destroyed'
    this.environments.delete(normalizedId)
  }

  private namespaceFor(id: string): string {
    return `${this.namespacePrefix}-${normalizeDnsLabel(id)}`.slice(0, 63).replace(/-+$/u, '')
  }

  private async deleteNamespace(namespace: string): Promise<void> {
    await this.runner.run('kubectl', [
      ...kubectlConnectionArgs(this.connection),
      'delete', 'namespace', namespace,
      '--ignore-not-found=true', '--wait=true', '--timeout=2m',
    ], { timeoutMs: 125_000 })
  }
}

function normalizeDnsLabel(value: string): string {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/gu, '-')
    .replace(/^-+|-+$/gu, '')
  if (!normalized) throw new Error(`Environment id ${JSON.stringify(value)} is not a valid DNS label`)
  return normalized.slice(0, 40).replace(/-+$/u, '')
}
