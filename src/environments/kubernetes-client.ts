import { spawn } from 'node:child_process'
import { readFile, realpath } from 'node:fs/promises'
import { isAbsolute, relative, resolve, sep } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { parseAllDocuments } from 'yaml'

import type { ResolvedKubernetesConnection } from './connection-registry.js'
import type { KubernetesDeployment } from './stack-descriptor.js'

export type KubernetesResource = Record<string, unknown> & {
  apiVersion?: string
  kind?: string
  metadata?: {
    name?: string
    namespace?: string
    labels?: Record<string, string>
    annotations?: Record<string, string>
    deletionTimestamp?: string
  }
}

export interface KubernetesPortForward {
  url: string
  stop(): Promise<void>
}

export interface KubernetesClient {
  createNamespace(resource: KubernetesResource, connection: ResolvedKubernetesConnection): Promise<void>
  createClusterResource(resource: KubernetesResource, connection: ResolvedKubernetesConnection): Promise<void>
  apply(resources: KubernetesResource[], namespace: string, connection: ResolvedKubernetesConnection): Promise<void>
  render(
    deployment: KubernetesDeployment,
    namespace: string,
    repoRoot: string,
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource[]>
  waitForReady(namespace: string, connection: ResolvedKubernetesConnection, timeoutMs: number): Promise<void>
  getNamespace(name: string, connection: ResolvedKubernetesConnection): Promise<KubernetesResource | undefined>
  listNamespaces(connection: ResolvedKubernetesConnection, labelSelector: string): Promise<KubernetesResource[]>
  deleteNamespace(name: string, connection: ResolvedKubernetesConnection): Promise<void>
  getClusterResource(
    resource: KubernetesResource,
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource | undefined>
  deleteClusterResource(resource: KubernetesResource, connection: ResolvedKubernetesConnection): Promise<void>
  portForwardService(
    namespace: string,
    service: string,
    port: number,
    protocol: 'http' | 'https',
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesPortForward>
}

export interface CommandResult {
  stdout: string
  stderr: string
}

export interface CommandOptions {
  cwd?: string
  env?: NodeJS.ProcessEnv
  input?: string
}

export type KubernetesCommandRunner = (
  executable: string,
  args: string[],
  options?: CommandOptions,
) => Promise<CommandResult>

export class KubernetesCommandError extends Error {
  readonly executable: string
  readonly args: string[]
  readonly exitCode: number | null
  readonly stderr: string

  constructor(executable: string, args: string[], exitCode: number | null, stderr: string) {
    super(`${executable} ${safeArguments(args).join(' ')} exited with ${exitCode ?? 'no status'}: ${stderr.trim().slice(-4_000) || 'no stderr'}`)
    this.name = 'KubernetesCommandError'
    this.executable = executable
    this.args = args
    this.exitCode = exitCode
    this.stderr = stderr
  }
}

export const defaultKubernetesCommandRunner: KubernetesCommandRunner = async (
  executable,
  args,
  options = {},
) => await new Promise((resolvePromise, reject) => {
  const child = spawn(executable, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ['pipe', 'pipe', 'pipe'],
  })
  let stdout = ''
  let stderr = ''
  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', (chunk: string) => { stdout += chunk })
  child.stderr.on('data', (chunk: string) => { stderr += chunk })
  child.once('error', reject)
  child.once('close', (code) => {
    if (code === 0) resolvePromise({ stdout, stderr })
    else reject(new KubernetesCommandError(executable, args, code, stderr))
  })
  child.stdin.end(options.input)
})

export interface KubectlKubernetesClientOptions {
  kubectlExecutable?: string
  helmExecutable?: string
  pollIntervalMs?: number
  commandRunner?: KubernetesCommandRunner
}

const MAX_PORT_FORWARD_DIAGNOSTICS = 4_000

/** Thin kubectl/Helm integration; all policy decisions remain in the provider. */
export class KubectlKubernetesClient implements KubernetesClient {
  readonly #kubectl: string
  readonly #helm: string
  readonly #pollIntervalMs: number
  readonly #run: KubernetesCommandRunner

  constructor(options: KubectlKubernetesClientOptions = {}) {
    this.#kubectl = options.kubectlExecutable ?? 'kubectl'
    this.#helm = options.helmExecutable ?? 'helm'
    this.#pollIntervalMs = options.pollIntervalMs ?? 1_000
    this.#run = options.commandRunner ?? defaultKubernetesCommandRunner
  }

  async createNamespace(resource: KubernetesResource, connection: ResolvedKubernetesConnection): Promise<void> {
    await this.#kubectlRun(connection, ['create', '--filename', '-'], JSON.stringify(resource))
  }

  async createClusterResource(
    resource: KubernetesResource,
    connection: ResolvedKubernetesConnection,
  ): Promise<void> {
    await this.#kubectlRun(connection, ['create', '--filename', '-'], JSON.stringify(resource))
  }

  async apply(
    resources: KubernetesResource[],
    namespace: string,
    connection: ResolvedKubernetesConnection,
  ): Promise<void> {
    if (resources.length === 0) return
    await this.#kubectlRun(connection, [
      '--namespace', namespace,
      'apply', '--filename', '-',
    ], JSON.stringify({ apiVersion: 'v1', kind: 'List', items: resources }))
  }

  async render(
    deployment: KubernetesDeployment,
    namespace: string,
    repoRoot: string,
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource[]> {
    const canonicalRoot = await realpath(repoRoot)
    let source: string
    if (deployment.strategy === 'helm') {
      const chart = await resolveRepoPath(canonicalRoot, deployment.chart)
      const valuesFiles = await Promise.all(deployment.valuesFiles.map(
        async (path) => await resolveRepoPath(canonicalRoot, path),
      ))
      const args = [
        'template', deployment.release ?? 'factory-verification', chart,
        '--namespace', namespace,
        '--include-crds',
      ]
      for (const valuesFile of valuesFiles) args.push('--values', valuesFile)
      for (const [key, value] of Object.entries(deployment.values)) {
        args.push('--set-string', `${key}=${String(value)}`)
      }
      source = (await this.#run(this.#helm, args, { env: commandEnvironment(connection) })).stdout
    } else if (deployment.strategy === 'kustomize') {
      const path = await resolveRepoPath(canonicalRoot, deployment.path)
      source = (await this.#run(this.#kubectl, ['kustomize', path], {
        env: commandEnvironment(connection),
      })).stdout
    } else {
      const paths = await Promise.all(deployment.paths.map(async (path) => await resolveRepoPath(canonicalRoot, path)))
      source = (await Promise.all(paths.map(async (path) => await readFile(path, 'utf8')))).join('\n---\n')
    }
    return parseKubernetesResources(source)
  }

  async waitForReady(
    namespace: string,
    connection: ResolvedKubernetesConnection,
    timeoutMs: number,
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs
    let lastReason = 'workloads have not reported ready'
    while (Date.now() < deadline) {
      const result = await this.#kubectlRun(connection, [
        '--namespace', namespace,
        'get', 'deployments,statefulsets,daemonsets,jobs,pods',
        '--output', 'json',
        '--ignore-not-found=true',
      ])
      const workloadList = JSON.parse(result.stdout) as { items?: KubernetesWorkload[] }
      const readiness = workloadsReady(workloadList.items ?? [])
      if (readiness.ready) return
      lastReason = readiness.reason
      await delay(Math.min(this.#pollIntervalMs, Math.max(1, deadline - Date.now())))
    }
    throw new Error(`Timed out after ${timeoutMs}ms waiting for Kubernetes namespace ${namespace}: ${lastReason}`)
  }

  async getNamespace(
    name: string,
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource | undefined> {
    try {
      const { stdout } = await this.#kubectlRun(connection, ['get', 'namespace', name, '--output', 'json'])
      return JSON.parse(stdout) as KubernetesResource
    } catch (error) {
      if (isNotFound(error)) return undefined
      throw error
    }
  }

  async listNamespaces(
    connection: ResolvedKubernetesConnection,
    labelSelector: string,
  ): Promise<KubernetesResource[]> {
    const { stdout } = await this.#kubectlRun(connection, [
      'get', 'namespaces', '--selector', labelSelector, '--output', 'json',
    ])
    const list = JSON.parse(stdout) as { items?: KubernetesResource[] }
    return list.items ?? []
  }

  async deleteNamespace(name: string, connection: ResolvedKubernetesConnection): Promise<void> {
    await this.#kubectlRun(connection, [
      'delete', 'namespace', name,
      '--ignore-not-found=true',
      '--wait=true',
      '--timeout=5m',
    ])
  }

  async getClusterResource(
    resource: KubernetesResource,
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource | undefined> {
    const identity = resourceIdentity(resource)
    try {
      const { stdout } = await this.#kubectlRun(
        connection,
        ['get', '--filename', '-', '--output', 'json'],
        JSON.stringify(identity),
      )
      return JSON.parse(stdout) as KubernetesResource
    } catch (error) {
      if (isNotFound(error)) return undefined
      throw error
    }
  }

  async deleteClusterResource(
    resource: KubernetesResource,
    connection: ResolvedKubernetesConnection,
  ): Promise<void> {
    await this.#kubectlRun(
      connection,
      ['delete', '--filename', '-', '--ignore-not-found=true', '--wait=true', '--timeout=5m'],
      JSON.stringify(resourceIdentity(resource)),
    )
  }

  async portForwardService(
    namespace: string,
    service: string,
    port: number,
    protocol: 'http' | 'https',
    connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesPortForward> {
    const args = [
      ...kubectlConnectionArgs(connection),
      '--namespace', namespace,
      'port-forward', `service/${service}`, `:${port}`,
      '--address', '127.0.0.1',
    ]
    return await new Promise<KubernetesPortForward>((resolvePromise, reject) => {
      const child = spawn(this.#kubectl, args, {
        env: commandEnvironment(connection),
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let settled = false
      let diagnostics = ''
      const timeout = setTimeout(() => {
        if (settled) return
        settled = true
        child.kill('SIGTERM')
        reject(new Error(`Timed out starting port-forward for ${namespace}/${service}:${port}: ${diagnostics.slice(-2_000)}`))
      }, 15_000)
      const observe = (chunk: Buffer | string): void => {
        if (settled) return
        diagnostics = `${diagnostics}${chunk.toString()}`.slice(-MAX_PORT_FORWARD_DIAGNOSTICS)
        const match = /Forwarding from 127\.0\.0\.1:(\d+)/u.exec(diagnostics)
        if (!match) return
        settled = true
        clearTimeout(timeout)
        const localPort = Number(match[1])
        diagnostics = ''
        resolvePromise({
          url: `${protocol}://127.0.0.1:${localPort}`,
          stop: async () => {
            if (child.exitCode !== null || child.signalCode !== null) return
            await new Promise<void>((resolveStop) => {
              const force = setTimeout(() => {
                child.kill('SIGKILL')
                resolveStop()
              }, 2_000)
              child.once('close', () => {
                clearTimeout(force)
                resolveStop()
              })
              child.kill('SIGTERM')
            })
          },
        })
      }
      child.stdout.on('data', observe)
      child.stderr.on('data', observe)
      child.once('error', (error) => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        reject(error)
      })
      child.once('close', (code) => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        reject(new KubernetesCommandError(this.#kubectl, args, code, diagnostics))
      })
    })
  }

  async #kubectlRun(
    connection: ResolvedKubernetesConnection,
    args: string[],
    input?: string,
  ): Promise<CommandResult> {
    return await this.#run(this.#kubectl, [...kubectlConnectionArgs(connection), ...args], {
      env: commandEnvironment(connection),
      input,
    })
  }
}

export function parseKubernetesResources(source: string): KubernetesResource[] {
  const resources: KubernetesResource[] = []
  for (const document of parseAllDocuments(source)) {
    if (document.errors.length > 0) {
      throw new Error(`Invalid rendered Kubernetes YAML: ${document.errors.map((error) => error.message).join('; ')}`)
    }
    const value = document.toJSON() as KubernetesResource | null
    if (!value) continue
    if (value.kind === 'List' && Array.isArray((value as { items?: unknown[] }).items)) {
      resources.push(...((value as { items: KubernetesResource[] }).items))
    } else {
      resources.push(value)
    }
  }
  return resources
}

interface KubernetesWorkload extends KubernetesResource {
  spec?: Record<string, unknown>
  status?: Record<string, unknown>
}

function workloadsReady(items: KubernetesWorkload[]): { ready: boolean; reason: string } {
  if (items.length === 0) return { ready: true, reason: 'no workloads declared' }
  for (const item of items) {
    const name = item.metadata?.name ?? '<unnamed>'
    const spec = item.spec ?? {}
    const status = item.status ?? {}
    if (item.metadata?.deletionTimestamp) return { ready: false, reason: `${item.kind}/${name} is terminating` }
    if (item.kind === 'Deployment' || item.kind === 'StatefulSet') {
      const desired = numberField(spec.replicas, 1)
      const ready = numberField(status.readyReplicas, 0)
      if (ready < desired) return { ready: false, reason: `${item.kind}/${name} has ${ready}/${desired} ready replicas` }
    } else if (item.kind === 'DaemonSet') {
      const desired = numberField(status.desiredNumberScheduled, 0)
      const ready = numberField(status.numberReady, 0)
      if (ready < desired) return { ready: false, reason: `DaemonSet/${name} has ${ready}/${desired} ready pods` }
    } else if (item.kind === 'Job') {
      const desired = numberField(spec.completions, 1)
      const succeeded = numberField(status.succeeded, 0)
      if (succeeded < desired) return { ready: false, reason: `Job/${name} has ${succeeded}/${desired} completions` }
    } else if (item.kind === 'Pod') {
      if (status.phase === 'Succeeded') continue
      const conditions = Array.isArray(status.conditions) ? status.conditions as Array<Record<string, unknown>> : []
      if (!conditions.some((condition) => condition.type === 'Ready' && condition.status === 'True')) {
        return { ready: false, reason: `Pod/${name} is not ready (${String(status.phase ?? 'unknown')})` }
      }
    }
  }
  return { ready: true, reason: 'all workloads ready' }
}

function kubectlConnectionArgs(connection: ResolvedKubernetesConnection): string[] {
  return [
    '--kubeconfig', connection.kubeconfigPath,
    ...(connection.context ? ['--context', connection.context] : []),
    '--request-timeout=30s',
  ]
}

const commandEnvironment = (connection: ResolvedKubernetesConnection): NodeJS.ProcessEnv => ({
  ...process.env,
  ...(connection.environment ?? {}),
  KUBECONFIG: connection.kubeconfigPath,
})

async function resolveRepoPath(root: string, requested: string): Promise<string> {
  if (isAbsolute(requested)) throw new Error(`Kubernetes deployment path must be relative: ${requested}`)
  const target = resolve(root, requested)
  const relativePath = relative(root, target)
  if (relativePath === '..' || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath)) {
    throw new Error(`Kubernetes deployment path escapes repository checkout: ${requested}`)
  }
  const canonicalTarget = await realpath(target)
  const canonicalRelative = relative(root, canonicalTarget)
  if (canonicalRelative === '..' || canonicalRelative.startsWith(`..${sep}`) || isAbsolute(canonicalRelative)) {
    throw new Error(`Kubernetes deployment path resolves outside repository checkout: ${requested}`)
  }
  return canonicalTarget
}

const isNotFound = (error: unknown): boolean =>
  error instanceof KubernetesCommandError && /\bnotfound\b|\bnot found\b/iu.test(error.stderr)

const numberField = (value: unknown, fallback: number): number =>
  typeof value === 'number' && Number.isFinite(value) ? value : fallback

function resourceIdentity(resource: KubernetesResource): KubernetesResource {
  if (!resource.apiVersion || !resource.kind || !resource.metadata?.name) {
    throw new Error('Kubernetes resource identity requires apiVersion, kind, and metadata.name')
  }
  return {
    apiVersion: resource.apiVersion,
    kind: resource.kind,
    metadata: { name: resource.metadata.name },
  }
}

function safeArguments(args: string[]): string[] {
  const safe = [...args]
  for (let index = 0; index < safe.length; index += 1) {
    if (safe[index] === '--set-string' && index + 1 < safe.length) safe[index + 1] = '[Redacted]'
  }
  return safe
}
