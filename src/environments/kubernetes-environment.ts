import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { readFile } from 'node:fs/promises'

import { parseAllDocuments } from 'yaml'

import {
  DEFAULT_MANAGED_FIDELITY_CAVEAT,
  KubernetesGuardrailDefaultsSchema,
  type ResolvedKubernetesConnection,
} from './connection-registry.js'
import { createKubernetesGuardrailResources } from './kubernetes-provider.js'
import type {
  DeployEnvironmentInput,
  ProvisionEnvironmentInput,
  VerificationEnvironment,
  VerificationEnvironmentProvider,
} from '../ports/environment.js'

export const FACTORY_ENVIRONMENT_MANAGED_LABEL = 'factory.agent-relay.dev/managed'
export const FACTORY_ENVIRONMENT_ID_LABEL = 'factory.agent-relay.dev/environment'
export const FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION = 'factory.agent-relay.dev/expires-at'
export const FACTORY_ENVIRONMENT_REPOSITORY_ANNOTATION = 'factory.agent-relay.dev/repository'

const MAX_OUTPUT_BYTES = 1024 * 1024

export interface KubectlEnvironmentCommandOptions {
  input?: string
  signal?: AbortSignal
}

export interface KubectlEnvironmentCommandResult {
  stdout: string
  stderr: string
}

export type KubectlEnvironmentCommandRunner = (
  args: string[],
  options?: KubectlEnvironmentCommandOptions,
) => Promise<KubectlEnvironmentCommandResult>

export interface KubectlEnvironmentProviderOptions {
  executable?: string
  runner?: KubectlEnvironmentCommandRunner
}

export function defaultKubectlEnvironmentRunner(
  executable = 'kubectl',
): KubectlEnvironmentCommandRunner {
  return async (args, options = {}) => await new Promise((resolve, reject) => {
    const child = spawn(executable, args, { stdio: ['pipe', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    let settled = false
    let forceTimer: ReturnType<typeof setTimeout> | undefined

    const finish = (error?: Error): void => {
      if (settled) return
      settled = true
      options.signal?.removeEventListener('abort', abort)
      if (!error && forceTimer) clearTimeout(forceTimer)
      if (error) reject(error)
      else resolve({ stdout, stderr })
    }
    const stop = (): void => {
      if (child.exitCode !== null) return
      child.kill('SIGTERM')
      forceTimer = setTimeout(() => child.kill('SIGKILL'), 2_000)
      forceTimer.unref()
    }
    const abort = (): void => {
      stop()
      finish(new VerificationEnvironmentAbortError())
    }
    options.signal?.addEventListener('abort', abort, { once: true })
    if (options.signal?.aborted) return abort()

    child.stdout.setEncoding('utf8')
    child.stderr.setEncoding('utf8')
    child.stdout.on('data', (chunk: string) => { stdout = boundedAppend(stdout, chunk) })
    child.stderr.on('data', (chunk: string) => { stderr = boundedAppend(stderr, chunk) })
    child.once('error', (error) => finish(error))
    child.once('close', (code) => {
      if (forceTimer) clearTimeout(forceTimer)
      if (code === 0) finish()
      else finish(new Error(
        `${executable} ${args.join(' ')} exited with ${code ?? 'no status'}: ${stderr.trim().slice(-4_000) || 'no stderr'}`,
      ))
    })
    if (!options.signal?.aborted) child.stdin.end(options.input)
  })
}

export class VerificationEnvironmentAbortError extends Error {
  constructor() {
    super('verification environment operation aborted')
    this.name = 'VerificationEnvironmentAbortError'
  }
}

export class KubectlEnvironmentProvider implements VerificationEnvironmentProvider {
  readonly #executable: string
  readonly #run: KubectlEnvironmentCommandRunner
  readonly #portForwards = new Map<string, ChildProcessWithoutNullStreams[]>()

  constructor(options: KubectlEnvironmentProviderOptions = {}) {
    this.#executable = options.executable ?? 'kubectl'
    this.#run = options.runner ?? defaultKubectlEnvironmentRunner(this.#executable)
  }

  async provision(input: ProvisionEnvironmentInput): Promise<VerificationEnvironment> {
    const context = contextArgs(input.kubeContext)
    const listed = await this.#run([
      ...context,
      'get', 'namespaces',
      '--selector', `${FACTORY_ENVIRONMENT_MANAGED_LABEL}=true`,
      '--output', 'json',
    ], { signal: input.signal })
    const active = activeNamespaceCount(listed.stdout)
    if (active >= input.maxActiveEnvironments) {
      throw new Error(
        `verification environment concurrency cap reached (${active}/${input.maxActiveEnvironments})`,
      )
    }

    const id = dnsLabel(`${input.namespacePrefix}-${input.runId}`).slice(0, 63)
    const expiresAt = new Date(Date.now() + input.ttlMs).toISOString()
    const namespace = {
      apiVersion: 'v1',
      kind: 'Namespace',
      metadata: {
        name: id,
        labels: {
          [FACTORY_ENVIRONMENT_MANAGED_LABEL]: 'true',
          [FACTORY_ENVIRONMENT_ID_LABEL]: dnsLabel(input.runId).slice(0, 63),
          'pod-security.kubernetes.io/enforce': 'restricted',
          'pod-security.kubernetes.io/enforce-version': 'latest',
          'pod-security.kubernetes.io/audit': 'restricted',
          'pod-security.kubernetes.io/warn': 'restricted',
        },
        annotations: {
          [FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION]: expiresAt,
          [FACTORY_ENVIRONMENT_REPOSITORY_ANNOTATION]: input.repository,
        },
      },
    }
    try {
      // A collision must fail; applying here could adopt and relabel an
      // unrelated namespace selected by a caller-controlled run id.
      await this.#run([...context, 'create', '--filename', '-'], {
        input: JSON.stringify(namespace),
        signal: input.signal,
      })
      const connection = verificationKubernetesConnection({
        context: input.kubeContext,
        repository: input.repository,
      })
      const guardrails = createKubernetesGuardrailResources(
        id,
        connection,
        KubernetesGuardrailDefaultsSchema.parse({}),
      )
      await this.#run([
        ...context,
        '--namespace', id,
        'apply', '--filename', '-',
      ], {
        input: JSON.stringify({ apiVersion: 'v1', kind: 'List', items: guardrails }),
        signal: input.signal,
      })
    } catch (error) {
      await this.#run([
        ...context,
        'delete', 'namespace', id,
        '--ignore-not-found=true',
        '--wait=false',
      ]).catch(() => undefined)
      throw error
    }
    return {
      id,
      namespace: id,
      endpoints: {},
      internalEndpoints: {},
      ...(input.kubeContext ? { kubeContext: input.kubeContext } : {}),
      expiresAt,
    }
  }

  async deploy(
    environment: VerificationEnvironment,
    input: DeployEnvironmentInput,
  ): Promise<VerificationEnvironment> {
    const resources = (await Promise.all(input.manifests.map(async ({ path }) =>
      resourcesFromManifest(path, environment.namespace),
    ))).flat()
    await this.#run([
      ...contextArgs(environment.kubeContext),
      '--namespace', environment.namespace,
      'apply', '--filename', '-',
    ], {
      input: JSON.stringify({ apiVersion: 'v1', kind: 'List', items: resources }),
      signal: input.signal,
    })

    for (const check of input.readiness) {
      await this.#run([
        ...contextArgs(environment.kubeContext),
        '--namespace', environment.namespace,
        'wait', check.resource,
        `--for=condition=${check.condition}`,
        `--timeout=${Math.max(1, Math.ceil(check.timeoutMs / 1_000))}s`,
      ], { signal: input.signal })
    }

    const endpoints: Record<string, string> = {}
    const internalEndpoints: Record<string, string> = {}
    for (const [name, endpoint] of Object.entries(input.endpoints)) {
      const suffix = normalizeEndpointPath(endpoint.path)
      internalEndpoints[name] = `${endpoint.scheme}://${endpoint.service}.${environment.namespace}.svc.cluster.local:${endpoint.port}${suffix}`
      endpoints[name] = endpoint.portForward
        ? `${endpoint.scheme}://127.0.0.1:${await this.#startPortForward(environment, endpoint.service, endpoint.port, input.signal)}${suffix}`
        : internalEndpoints[name]
    }
    return { ...environment, endpoints, internalEndpoints }
  }

  async teardown(environment: VerificationEnvironment, options: { signal?: AbortSignal } = {}): Promise<void> {
    this.#stopPortForwards(environment.id)
    await this.#run([
      ...contextArgs(environment.kubeContext),
      'delete', 'namespace', environment.namespace,
      '--ignore-not-found=true',
      '--wait=true',
    ], { signal: options.signal })
  }

  async #startPortForward(
    environment: VerificationEnvironment,
    service: string,
    port: number,
    signal?: AbortSignal,
  ): Promise<number> {
    return await new Promise((resolve, reject) => {
      const child = spawn(this.#executable, [
        ...contextArgs(environment.kubeContext),
        '--namespace', environment.namespace,
        'port-forward', `service/${service}`, `:${port}`,
        '--address', '127.0.0.1',
      ], { stdio: ['pipe', 'pipe', 'pipe'] })
      const forwards = this.#portForwards.get(environment.id) ?? []
      forwards.push(child)
      this.#portForwards.set(environment.id, forwards)
      let output = ''
      let settled = false
      const timeout = setTimeout(() => finish(new Error(`timed out starting port-forward for service/${service}`)), 30_000)
      const abort = (): void => finish(new VerificationEnvironmentAbortError())
      const finish = (error?: Error, localPort?: number): void => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        signal?.removeEventListener('abort', abort)
        if (error) {
          stopChildProcess(child)
          reject(error)
        } else {
          resolve(localPort!)
        }
      }
      const inspect = (chunk: Buffer | string): void => {
        if (settled) return
        output = boundedAppend(output, chunk.toString())
        const match = /Forwarding from 127\.0\.0\.1:(\d+)\s+->/u.exec(output)
        if (match) {
          output = ''
          finish(undefined, Number(match[1]))
        }
      }
      child.stdout.on('data', inspect)
      child.stderr.on('data', inspect)
      child.once('error', (error) => finish(error))
      child.once('close', (code) => {
        if (!settled) finish(new Error(
          `port-forward for service/${service} exited with ${code ?? 'no status'}: ${output.trim().slice(-4_000)}`,
        ))
      })
      signal?.addEventListener('abort', abort, { once: true })
      if (signal?.aborted) abort()
    })
  }

  #stopPortForwards(environmentId: string): void {
    for (const child of this.#portForwards.get(environmentId) ?? []) {
      stopChildProcess(child)
    }
    this.#portForwards.delete(environmentId)
  }
}

export function verificationKubernetesConnection(input: {
  repository: string
  context?: string
  kubeconfig?: string
}): ResolvedKubernetesConnection {
  return {
    id: 'factory-verification-gate',
    target: 'managed',
    customers: ['*'],
    repositories: [input.repository],
    credentialKind: 'kubeconfig',
    kubeconfigPath: input.kubeconfig ?? '',
    ...(input.context ? { context: input.context } : {}),
    protectedNamespaces: ['default', 'prod', 'production'],
    allowClusterScopedResources: false,
    allowedClusterScopedKinds: [],
    nodeSelector: {},
    tolerations: [],
    fidelityCaveat: DEFAULT_MANAGED_FIDELITY_CAVEAT,
  }
}

function stopChildProcess(child: ChildProcessWithoutNullStreams): void {
  if (child.exitCode !== null) return
  child.kill('SIGTERM')
  const force = setTimeout(() => child.kill('SIGKILL'), 2_000)
  force.unref()
  child.once('close', () => clearTimeout(force))
}

async function resourcesFromManifest(path: string, namespace: string): Promise<Array<Record<string, unknown>>> {
  const documents = parseAllDocuments(await readFile(path, 'utf8'))
  const resources: Array<Record<string, unknown>> = []
  for (const document of documents) {
    if (document.errors.length > 0) {
      throw new Error(`Invalid Kubernetes manifest ${path}: ${document.errors.map((error) => error.message).join('; ')}`)
    }
    const value = document.toJSON()
    if (value === null || value === undefined) continue
    for (const resource of flattenResource(value)) {
      const kind = typeof resource.kind === 'string' ? resource.kind : ''
      if (!kind || typeof resource.apiVersion !== 'string') {
        throw new Error(`Kubernetes manifest ${path} contains a resource without apiVersion/kind`)
      }
      if (CLUSTER_SCOPED_KINDS.has(kind)) {
        throw new Error(`Verification manifests may not create cluster-scoped ${kind} resources`)
      }
      const metadata = asRecord(resource.metadata)
      resource.metadata = { ...metadata, namespace }
      resources.push(resource)
    }
  }
  return resources
}

function flattenResource(value: unknown): Array<Record<string, unknown>> {
  const resource = asRecord(value)
  if (resource.kind === 'List' && Array.isArray(resource.items)) {
    return resource.items.flatMap(flattenResource)
  }
  return [resource]
}

const CLUSTER_SCOPED_KINDS = new Set([
  'APIService',
  'ClusterRole',
  'ClusterRoleBinding',
  'CustomResourceDefinition',
  'MutatingWebhookConfiguration',
  'Namespace',
  'Node',
  'PersistentVolume',
  'PriorityClass',
  'StorageClass',
  'ValidatingWebhookConfiguration',
])

function activeNamespaceCount(stdout: string): number {
  try {
    const value = JSON.parse(stdout) as { items?: Array<{ metadata?: { deletionTimestamp?: string } }> }
    return (value.items ?? []).filter((item) => !item.metadata?.deletionTimestamp).length
  } catch (error) {
    throw new Error(`kubectl returned invalid namespace inventory: ${error instanceof Error ? error.message : String(error)}`)
  }
}

const contextArgs = (context: string | undefined): string[] => context ? ['--context', context] : []

const normalizeEndpointPath = (path: string): string => !path ? '' : path.startsWith('/') ? path : `/${path}`

const dnsLabel = (value: string): string => value
  .toLowerCase()
  .replace(/[^a-z0-9-]+/gu, '-')
  .replace(/^-+|-+$/gu, '') || 'verification'

const boundedAppend = (current: string, chunk: string): string =>
  `${current}${chunk}`.slice(-MAX_OUTPUT_BYTES)

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
