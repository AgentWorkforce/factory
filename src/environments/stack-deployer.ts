import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { connect } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Environment, KubernetesEnvironmentTarget } from '../ports/environment.js'
import {
  type CommandRunner,
  helmConnectionArgs,
  type KubernetesConnection,
  kubectlConnectionArgs,
  ProcessCommandRunner,
} from './kubernetes-command.js'
import {
  type LoadedVerificationStack,
  resolveVerificationStackAsset,
  type VerificationProbe,
  type VerificationStackDescriptor,
  type VerificationStackReferenceGroup,
  type VerificationStackService,
} from './stack-descriptor.js'

export interface ReferenceResolutionContext {
  kind: 'secret' | 'config'
  resource: string
  key: string
  optional: boolean
}

export interface VerificationStackReferenceResolver {
  resolve(reference: string, context: ReferenceResolutionContext): Promise<string | undefined>
}

export interface ManagedPortForward {
  localPort: number
  close(): Promise<void>
}

export interface PortForwarder {
  forward(input: {
    connection: KubernetesConnection
    namespace: string
    service: string
    remotePort: string | number
    timeoutMs: number
  }): Promise<ManagedPortForward>
}

export interface StackDeployerOptions {
  commandRunner?: CommandRunner
  referenceResolver?: VerificationStackReferenceResolver
  portForwarder?: PortForwarder
  fetch?: typeof globalThis.fetch
}

export interface StackDeployment {
  endpoints: Record<string, string>
  dispose(): Promise<void>
}

export class StackDeploymentError extends Error {
  constructor(
    message: string,
    public readonly stage: 'references' | 'apply' | 'readiness' | 'seed' | 'endpoints',
    public readonly service?: string,
    options?: ErrorOptions,
  ) {
    super(message, options)
    this.name = 'StackDeploymentError'
  }
}

/** Deploys a repository-owned verification stack into a provisioned Kubernetes environment. */
export class VerificationStackDeployer {
  private readonly runner: CommandRunner
  private readonly referenceResolver?: VerificationStackReferenceResolver
  private readonly portForwarder: PortForwarder
  private readonly fetchImpl: typeof globalThis.fetch

  constructor(options: StackDeployerOptions = {}) {
    this.runner = options.commandRunner ?? new ProcessCommandRunner()
    this.referenceResolver = options.referenceResolver
    this.portForwarder = options.portForwarder ?? new KubectlPortForwarder()
    this.fetchImpl = options.fetch ?? globalThis.fetch
  }

  async deploy(
    stack: LoadedVerificationStack | VerificationStackDescriptor,
    environment: Environment,
  ): Promise<StackDeployment> {
    const descriptor = 'descriptor' in stack ? stack.descriptor : stack
    const target = kubernetesTarget(environment)
    const tunnels = new Map<string, ManagedPortForward>()
    let preparedRoot: PreparedStackRoot | undefined

    try {
      preparedRoot = await this.prepareStackRoot(stack)
      await this.applyReferences(descriptor.secrets, 'secret', target)
      await this.applyReferences(descriptor.config, 'config', target)
      await this.applySource(descriptor, preparedRoot.rootDir, target)

      for (const service of descriptor.services) {
        await this.waitForService(service, target, tunnels)
      }
      for (const seed of descriptor.seeds) {
        await this.runSeed(seed, descriptor, preparedRoot.rootDir, target)
      }

      const endpoints: Record<string, string> = {}
      for (const endpoint of descriptor.endpoints) {
        const tunnel = await this.tunnelFor(
          tunnels,
          target,
          endpoint.service,
          endpoint.port,
          30_000,
        )
        endpoints[endpoint.name] = `${endpoint.protocol}://127.0.0.1:${tunnel.localPort}${endpoint.path}`
      }
      environment.endpoints = { ...endpoints }

      let disposed = false
      return {
        endpoints,
        dispose: async () => {
          if (disposed) return
          disposed = true
          await closeTunnels(tunnels)
        },
      }
    } catch (error) {
      await closeTunnels(tunnels)
      if (error instanceof StackDeploymentError) throw error
      throw new StackDeploymentError(
        `Failed to deploy verification stack ${descriptor.name}: ${errorMessage(error)}`,
        'apply',
        undefined,
        { cause: error },
      )
    } finally {
      await preparedRoot?.cleanup().catch(() => undefined)
    }
  }

  private async prepareStackRoot(
    stack: LoadedVerificationStack | VerificationStackDescriptor,
  ): Promise<PreparedStackRoot> {
    if (!('descriptor' in stack)) {
      return { rootDir: process.cwd(), cleanup: async () => undefined }
    }
    if (!stack.ref) {
      return { rootDir: stack.rootDir, cleanup: async () => undefined }
    }

    const checkout = await mkdtemp(join(tmpdir(), 'factory-verification-stack-'))
    try {
      await this.runner.run('git', [
        'clone', '--quiet', '--shared', '--no-checkout', stack.rootDir, checkout,
      ], { timeoutMs: 120_000 })
      await this.runner.run('git', [
        '-C', checkout, 'checkout', '--quiet', '--detach', stack.ref,
      ], { timeoutMs: 120_000 })
      return {
        rootDir: checkout,
        cleanup: async () => await rm(checkout, { recursive: true, force: true }),
      }
    } catch (cause) {
      await rm(checkout, { recursive: true, force: true }).catch(() => undefined)
      throw new StackDeploymentError(
        `Could not materialize verification-stack assets at Git ref ${stack.ref}: ${errorMessage(cause)}`,
        'apply',
        undefined,
        { cause },
      )
    }
  }

  private async applyReferences(
    groups: VerificationStackReferenceGroup[],
    kind: 'secret' | 'config',
    target: KubernetesEnvironmentTarget,
  ): Promise<void> {
    const requiresResolver = groups.some((group) => (
      Object.values(group.data).some((reference) => !reference.optional)
    ))
    if (requiresResolver && !this.referenceResolver) {
      throw new StackDeploymentError(
        `Cannot materialize required ${kind} references: no reference resolver was configured`,
        'references',
      )
    }

    for (const group of groups) {
      const data: Record<string, string> = {}
      for (const [key, requirement] of Object.entries(group.data)) {
        let value: string | undefined
        try {
          value = await this.referenceResolver?.resolve(requirement.ref, {
            kind,
            resource: group.name,
            key,
            optional: requirement.optional,
          })
        } catch (cause) {
          throw new StackDeploymentError(
            `Could not resolve ${kind} reference ${JSON.stringify(requirement.ref)} for ${group.name}.${key}: ${errorMessage(cause)}`,
            'references',
            undefined,
            { cause },
          )
        }
        if (value === undefined) {
          if (requirement.optional) continue
          throw new StackDeploymentError(
            `Missing required ${kind} reference ${JSON.stringify(requirement.ref)} for ${group.name}.${key}`,
            'references',
          )
        }
        data[key] = value
      }

      const resource = kind === 'secret'
        ? {
            apiVersion: 'v1',
            kind: 'Secret',
            metadata: { name: group.name, namespace: target.namespace },
            type: 'Opaque',
            data: Object.fromEntries(Object.entries(data).map(([key, value]) => [
              key,
              Buffer.from(value).toString('base64'),
            ])),
          }
        : {
            apiVersion: 'v1',
            kind: 'ConfigMap',
            metadata: { name: group.name, namespace: target.namespace },
            data,
          }

      try {
        await this.runner.run('kubectl', [
          ...kubectlConnectionArgs(target),
          'apply', '-f', '-',
        ], { input: JSON.stringify(resource), timeoutMs: 30_000 })
      } catch (cause) {
        throw new StackDeploymentError(
          `Failed to materialize ${kind} ${group.name} in namespace ${target.namespace}: ${errorMessage(cause)}`,
          'references',
          undefined,
          { cause },
        )
      }
    }
  }

  private async applySource(
    descriptor: VerificationStackDescriptor,
    rootDir: string,
    target: KubernetesEnvironmentTarget,
  ): Promise<void> {
    const source = descriptor.source
    try {
      if (source.type === 'manifests') {
        for (const path of source.paths) {
          await this.runner.run('kubectl', [
            ...kubectlConnectionArgs(target),
            '--namespace', target.namespace,
            'apply', '-f', resolveVerificationStackAsset(rootDir, path),
          ], { timeoutMs: 120_000 })
        }
        return
      }
      if (source.type === 'kustomize') {
        await this.runner.run('kubectl', [
          ...kubectlConnectionArgs(target),
          '--namespace', target.namespace,
          'apply', '-k', resolveVerificationStackAsset(rootDir, source.path),
        ], { timeoutMs: 120_000 })
        return
      }
      if (source.type === 'helm') {
        const chart = isRemoteChart(source.chart)
          ? source.chart
          : resolveVerificationStackAsset(rootDir, source.chart)
        await this.runner.run('helm', [
          ...helmConnectionArgs(target),
          'upgrade', '--install', source.release ?? descriptor.name, chart,
          '--namespace', target.namespace,
          ...source.valuesFiles.flatMap((path) => [
            '--values', resolveVerificationStackAsset(rootDir, path),
          ]),
        ], { timeoutMs: 180_000 })
        return
      }

      const compose = await this.runner.run('kompose', [
        'convert', '--file', resolveVerificationStackAsset(rootDir, source.path), '--stdout',
      ], { timeoutMs: 120_000 })
      await this.runner.run('kubectl', [
        ...kubectlConnectionArgs(target),
        '--namespace', target.namespace,
        'apply', '-f', '-',
      ], { input: compose.stdout, timeoutMs: 120_000 })
    } catch (cause) {
      throw new StackDeploymentError(
        `Failed to apply ${source.type} source for stack ${descriptor.name} in namespace ${target.namespace}: ${errorMessage(cause)}`,
        'apply',
        undefined,
        { cause },
      )
    }
  }

  private async waitForService(
    service: VerificationStackService,
    target: KubernetesEnvironmentTarget,
    tunnels: Map<string, ManagedPortForward>,
  ): Promise<void> {
    const workload = `${service.workload.kind}/${service.workload.name ?? service.name}`
    try {
      await this.runner.run('kubectl', [
        ...kubectlConnectionArgs(target),
        '--namespace', target.namespace,
        'rollout', 'status', workload,
        `--timeout=${service.readiness.timeoutSeconds}s`,
      ], { timeoutMs: service.readiness.timeoutSeconds * 1_000 + 5_000 })
    } catch (cause) {
      throw new StackDeploymentError(
        `Service ${service.name} workload ${workload} never became ready within ${service.readiness.timeoutSeconds}s: ${errorMessage(cause)}`,
        'readiness',
        service.name,
        { cause },
      )
    }

    await this.waitForProbe(service, service.readiness, 'readiness', target, tunnels)
    if (service.health) await this.waitForProbe(service, service.health, 'health', target, tunnels)
  }

  private async waitForProbe(
    service: VerificationStackService,
    probe: VerificationProbe,
    label: 'readiness' | 'health',
    target: KubernetesEnvironmentTarget,
    tunnels: Map<string, ManagedPortForward>,
  ): Promise<void> {
    const timeoutMs = probe.timeoutSeconds * 1_000
    const deadline = Date.now() + timeoutMs
    let lastError = 'probe returned unhealthy'

    while (Date.now() < deadline) {
      try {
        const remaining = Math.max(1, deadline - Date.now())
        if (probe.type === 'exec') {
          const workload = probe.target ?? `${service.workload.kind}/${service.workload.name ?? service.name}`
          await this.runner.run('kubectl', [
            ...kubectlConnectionArgs(target),
            '--namespace', target.namespace,
            'exec', workload,
            ...(probe.container ? ['--container', probe.container] : []),
            '--', ...probe.command,
          ], { timeoutMs: Math.min(remaining, 10_000) })
          return
        }

        const serviceName = probe.service ?? service.name
        const tunnel = await this.tunnelFor(
          tunnels,
          target,
          serviceName,
          probe.port,
          Math.min(remaining, 15_000),
        )
        if (probe.type === 'tcp') {
          await checkTcp(tunnel.localPort, Math.min(remaining, 5_000))
          return
        }

        const controller = new AbortController()
        const timer = setTimeout(() => controller.abort(), Math.min(remaining, 5_000))
        try {
          const response = await this.fetchImpl(
            `${probe.scheme}://127.0.0.1:${tunnel.localPort}${probe.path}`,
            { signal: controller.signal },
          )
          await response.body?.cancel()
          if (probe.expectedStatuses.includes(response.status)) return
          lastError = `HTTP ${response.status}; expected ${probe.expectedStatuses.join(', ')}`
        } finally {
          clearTimeout(timer)
        }
      } catch (error) {
        lastError = errorMessage(error)
      }

      const delay = Math.min(probe.intervalSeconds * 1_000, Math.max(0, deadline - Date.now()))
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
    }

    throw new StackDeploymentError(
      `Service ${service.name} ${label} probe never became ready within ${probe.timeoutSeconds}s (last error: ${lastError})`,
      'readiness',
      service.name,
    )
  }

  private async runSeed(
    seed: VerificationStackDescriptor['seeds'][number],
    descriptor: VerificationStackDescriptor,
    rootDir: string,
    target: KubernetesEnvironmentTarget,
  ): Promise<void> {
    try {
      if (seed.type === 'job') {
        await this.runner.run('kubectl', [
          ...kubectlConnectionArgs(target),
          '--namespace', target.namespace,
          'apply', '-f', resolveVerificationStackAsset(rootDir, seed.manifest),
        ], { timeoutMs: 30_000 })
        await this.runner.run('kubectl', [
          ...kubectlConnectionArgs(target),
          '--namespace', target.namespace,
          'wait', `job/${seed.job}`, '--for=condition=complete',
          `--timeout=${seed.timeoutSeconds}s`,
        ], { timeoutMs: seed.timeoutSeconds * 1_000 + 5_000 })
        return
      }

      const service = descriptor.services.find((candidate) => candidate.name === seed.service)
      if (!service) throw new Error(`service ${seed.service} is not declared`)
      const workload = `${service.workload.kind}/${service.workload.name ?? service.name}`
      await this.runner.run('kubectl', [
        ...kubectlConnectionArgs(target),
        '--namespace', target.namespace,
        'exec', workload,
        ...(seed.container ? ['--container', seed.container] : []),
        '--', ...seed.command,
      ], { timeoutMs: seed.timeoutSeconds * 1_000 })
    } catch (cause) {
      throw new StackDeploymentError(
        `Seed step ${seed.name} failed in namespace ${target.namespace}: ${errorMessage(cause)}`,
        'seed',
        seed.type === 'exec' ? seed.service : undefined,
        { cause },
      )
    }
  }

  private async tunnelFor(
    tunnels: Map<string, ManagedPortForward>,
    target: KubernetesEnvironmentTarget,
    service: string,
    port: string | number,
    timeoutMs: number,
  ): Promise<ManagedPortForward> {
    const key = `${service}:${port}`
    const existing = tunnels.get(key)
    if (existing) return existing
    try {
      const tunnel = await this.portForwarder.forward({
        connection: target,
        namespace: target.namespace,
        service,
        remotePort: port,
        timeoutMs,
      })
      tunnels.set(key, tunnel)
      return tunnel
    } catch (cause) {
      throw new StackDeploymentError(
        `Could not expose service ${service} port ${port} in namespace ${target.namespace}: ${errorMessage(cause)}`,
        'endpoints',
        service,
        { cause },
      )
    }
  }
}

export async function deployVerificationStack(
  stack: LoadedVerificationStack | VerificationStackDescriptor,
  environment: Environment,
  options: StackDeployerOptions = {},
): Promise<StackDeployment> {
  return await new VerificationStackDeployer(options).deploy(stack, environment)
}

export class KubectlPortForwarder implements PortForwarder {
  async forward(input: {
    connection: KubernetesConnection
    namespace: string
    service: string
    remotePort: string | number
    timeoutMs: number
  }): Promise<ManagedPortForward> {
    const child = spawn('kubectl', [
      ...kubectlConnectionArgs(input.connection),
      '--namespace', input.namespace,
      'port-forward', '--address', '127.0.0.1',
      `service/${input.service}`, `:${input.remotePort}`,
    ], { env: process.env, stdio: ['pipe', 'pipe', 'pipe'] })
    child.stdin.end()

    try {
      const localPort = await waitForForwardedPort(child, input.timeoutMs)
      let closed = false
      return {
        localPort,
        close: async () => {
          if (closed) return
          closed = true
          await stopChild(child)
        },
      }
    } catch (error) {
      await stopChild(child)
      throw error
    }
  }
}

function kubernetesTarget(environment: Environment): KubernetesEnvironmentTarget {
  if (environment.target?.type === 'kubernetes') {
    return environment.target as KubernetesEnvironmentTarget
  }
  if (environment.namespace) {
    return { type: 'kubernetes', namespace: environment.namespace }
  }
  throw new StackDeploymentError(
    `Environment ${environment.id} is not a Kubernetes environment with a namespace`,
    'apply',
  )
}

async function closeTunnels(tunnels: Map<string, ManagedPortForward>): Promise<void> {
  const active = [...tunnels.values()]
  tunnels.clear()
  await Promise.allSettled(active.map(async (tunnel) => await tunnel.close()))
}

function isRemoteChart(chart: string): boolean {
  return /^(?:https?:|oci:)/u.test(chart)
}

function checkTcp(port: number, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: '127.0.0.1', port })
    const timer = setTimeout(() => {
      socket.destroy()
      reject(new Error(`TCP probe timed out after ${timeoutMs}ms`))
    }, timeoutMs)
    socket.once('connect', () => {
      clearTimeout(timer)
      socket.destroy()
      resolve()
    })
    socket.once('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
  })
}

function waitForForwardedPort(child: ChildProcessWithoutNullStreams, timeoutMs: number): Promise<number> {
  return new Promise((resolve, reject) => {
    let output = ''
    let settled = false
    const onData = (chunk: Buffer | string) => {
      output = `${output}${chunk.toString()}`.slice(-8_000)
      const match = /Forwarding from 127\.0\.0\.1:(\d+)/u.exec(output)
      if (match) finish(undefined, Number(match[1]))
    }
    const onError = (error: Error) => finish(error)
    const onClose = (code: number | null, signal: NodeJS.Signals | null) => {
      finish(new Error(
        `kubectl port-forward exited with ${code ?? signal ?? 'unknown status'}${output.trim() ? `: ${output.trim()}` : ''}`,
      ))
    }
    const timer = setTimeout(() => {
      finish(new Error(`kubectl port-forward did not become ready within ${timeoutMs}ms${output.trim() ? `: ${output.trim()}` : ''}`))
    }, timeoutMs)
    const finish = (error?: Error, port?: number) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      child.stdout.off('data', onData)
      child.stderr.off('data', onData)
      child.off('error', onError)
      child.off('close', onClose)
      if (error) reject(error)
      else resolve(port!)
    }

    child.stdout.on('data', onData)
    child.stderr.on('data', onData)
    child.once('error', onError)
    child.once('close', onClose)
  })
}

function stopChild(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve()
  return new Promise((resolve) => {
    const force = setTimeout(() => child.kill('SIGKILL'), 2_000)
    child.once('close', () => {
      clearTimeout(force)
      resolve()
    })
    child.kill('SIGTERM')
  })
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

interface PreparedStackRoot {
  rootDir: string
  cleanup(): Promise<void>
}
