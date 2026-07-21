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
  maxActiveEnvironments?: number
  commandRunner?: CommandRunner
}

export const FACTORY_ENVIRONMENT_MANAGED_LABEL = 'factory.agent-relay.dev/managed'
export const FACTORY_ENVIRONMENT_ID_LABEL = 'factory.agent-relay.dev/environment'
export const FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION = 'factory.agent-relay.dev/expires-at'
export const FACTORY_ENVIRONMENT_REPOSITORY_ANNOTATION = 'factory.agentworkforce.dev/repository'

/** A small Kubernetes EnvironmentProvider used by verification-stack deployments. */
export class KubernetesEnvironmentProvider implements EnvironmentProvider {
  private readonly runner: CommandRunner
  private readonly namespacePrefix: string
  private readonly defaultTtl: number
  private readonly maxActiveEnvironments: number
  private readonly connection: KubernetesConnection
  private readonly environments = new Map<string, Environment>()

  constructor(options: KubernetesEnvironmentProviderOptions = {}) {
    this.runner = options.commandRunner ?? new ProcessCommandRunner()
    this.namespacePrefix = normalizeDnsLabel(options.namespacePrefix ?? 'factory-verification')
    this.defaultTtl = options.defaultTtl ?? 30 * 60_000
    this.maxActiveEnvironments = positiveInteger(options.maxActiveEnvironments ?? 2, 'maxActiveEnvironments')
    this.connection = { kubeconfig: options.kubeconfig, context: options.context }
  }

  async provision(spec: EnvironmentSpec = {}): Promise<Environment> {
    const listed = await this.runner.run('kubectl', [
      ...kubectlConnectionArgs(this.connection),
      'get', 'namespaces',
      '--selector', `${FACTORY_ENVIRONMENT_MANAGED_LABEL}=true`,
      '--output', 'json',
    ], { timeoutMs: 30_000, signal: spec.signal })
    const active = activeNamespaceCount(listed.stdout)
    if (active >= this.maxActiveEnvironments) {
      throw new Error(
        `verification environment concurrency cap reached (${active}/${this.maxActiveEnvironments})`,
      )
    }

    const id = normalizeDnsLabel(spec.id ?? randomUUID())
    const namespace = `${this.namespacePrefix}-${id}`.slice(0, 63).replace(/-+$/u, '')
    const ttl = spec.ttl ?? this.defaultTtl
    const createdAt = new Date()
    const expiresAt = new Date(createdAt.getTime() + ttl).toISOString()
    const labels = {
      'app.kubernetes.io/managed-by': 'factory',
      'factory.agentworkforce.dev/environment-id': id,
      ...spec.labels,
      [FACTORY_ENVIRONMENT_MANAGED_LABEL]: 'true',
      [FACTORY_ENVIRONMENT_ID_LABEL]: id,
    }
    const annotations = {
      ...spec.annotations,
      [FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION]: expiresAt,
    }

    try {
      await this.runner.run('kubectl', [
        ...kubectlConnectionArgs(this.connection),
        'apply', '--filename', '-',
      ], {
        input: JSON.stringify({
          apiVersion: 'v1',
          kind: 'Namespace',
          metadata: { name: namespace, labels, annotations },
        }),
        timeoutMs: 30_000,
        signal: spec.signal,
      })
    } catch (error) {
      await this.deleteNamespace(namespace).catch(() => undefined)
      throw error
    }

    const environment: Environment = {
      id,
      status: 'ready',
      createdAt: createdAt.toISOString(),
      ttl,
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
      const phase = result.stdout.trim()
      if (phase === 'Active') return 'ready'
      if (phase === 'Terminating') return 'destroying'
      return 'provisioning'
    } catch (error) {
      if (error instanceof CommandExecutionError && /not found/iu.test(error.stderr)) return 'destroyed'
      throw error
    }
  }

  async endpoints(id: string): Promise<Record<string, string>> {
    return { ...(this.environments.get(normalizeDnsLabel(id))?.endpoints ?? {}) }
  }

  async destroy(id: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    const normalizedId = normalizeDnsLabel(id)
    const environment = this.environments.get(normalizedId)
    const namespace = environment?.namespace ?? this.namespaceFor(normalizedId)
    if (environment) environment.status = 'destroying'
    await this.deleteNamespace(namespace, options.signal)
    if (environment) environment.status = 'destroyed'
    this.environments.delete(normalizedId)
  }

  private namespaceFor(id: string): string {
    return `${this.namespacePrefix}-${normalizeDnsLabel(id)}`.slice(0, 63).replace(/-+$/u, '')
  }

  private async deleteNamespace(namespace: string, signal?: AbortSignal): Promise<void> {
    await this.runner.run('kubectl', [
      ...kubectlConnectionArgs(this.connection),
      'delete', 'namespace', namespace,
      '--ignore-not-found=true', '--wait=true', '--timeout=2m',
    ], { timeoutMs: 125_000, signal })
  }
}

function activeNamespaceCount(stdout: string): number {
  try {
    const value = JSON.parse(stdout) as { items?: Array<{ metadata?: { deletionTimestamp?: string } }> }
    if (!Array.isArray(value.items)) throw new Error('missing items array')
    return value.items.filter((item) => !item.metadata?.deletionTimestamp).length
  } catch (error) {
    throw new Error(`kubectl returned invalid namespace inventory: ${error instanceof Error ? error.message : String(error)}`)
  }
}

function positiveInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${field} must be a positive integer`)
  return value
}

function normalizeDnsLabel(value: string): string {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/gu, '-')
    .replace(/^-+|-+$/gu, '')
  if (!normalized) throw new Error(`Environment id ${JSON.stringify(value)} is not a valid DNS label`)
  return normalized.slice(0, 40).replace(/-+$/u, '')
}
