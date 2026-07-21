import { randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'

import { normalizeLogger } from '../logging.js'
import type { FactoryEventReporter } from '../ports/observability.js'
import type { Logger } from '../ports/system.js'
import type {
  ProvisionEnvironmentInput,
  VerificationEnvironment,
  VerificationEnvironmentProvider,
} from '../ports/environment.js'
import { createFactoryCloudEventV1 } from '../observability/events.js'
import type { LoadMeasurements, LoadSloViolation } from './load-harness.js'
import { runLoad } from './load-harness.js'
import { loadLoadProfile } from './load-profile.js'
import { KubectlEnvironmentProvider } from './kubernetes-environment.js'
import {
  CommandExecutionError,
  ProcessCommandRunner,
  kubectlConnectionArgs,
} from './kubernetes-command.js'
import {
  VerificationStackDeployer,
  type StackDeployment,
} from './verification-stack-deployer.js'
import { loadVerificationGateStack, type ResolvedVerificationStack } from './verification-stack.js'

export const VERIFICATION_EVIDENCE_CONTRACT = 'factory.verification.evidence.v1' as const
export const DEFAULT_VERIFICATION_DESCRIPTOR = '.factory/verification-stack.yaml'

export type VerificationStageStatus = 'pass' | 'fail' | 'skipped' | 'timed_out'

export interface VerificationStageEvidence {
  status: VerificationStageStatus
  durationMs: number
  error?: string
}

export interface VerificationE2eEvidence extends VerificationStageEvidence {
  exitCode?: number
}

export interface VerificationLoadEvidence extends VerificationStageEvidence {
  measured?: LoadMeasurements
  violations?: LoadSloViolation[]
}

export interface VerificationEvidence {
  contract: typeof VERIFICATION_EVIDENCE_CONTRACT
  runId: string
  repository: string
  repositoryPath: string
  descriptorPath: string
  expectedHeadSha?: string
  environmentId?: string
  namespace?: string
  startedAt: string
  completedAt: string
  timedOut: boolean
  stages: {
    resolve: VerificationStageEvidence
    provision: VerificationStageEvidence
    deploy: VerificationStageEvidence
    e2e: VerificationE2eEvidence
    load: VerificationLoadEvidence
    evaluate: VerificationStageEvidence
    teardown: VerificationStageEvidence
  }
}

export interface VerificationVerdict {
  status: 'pass' | 'fail'
  passed: boolean
  reason: string
  evidence: VerificationEvidence
}

export interface VerificationGateInput {
  repository: string
  repositoryPath: string
  issueKey?: string
  expectedHeadSha?: string
  descriptorPath?: string
  runId?: string
}

export interface VerificationGate {
  verify(input: VerificationGateInput): Promise<VerificationVerdict>
}

export interface E2eCommandInput {
  image: string
  environment: VerificationEnvironment
  command: string
  args: string[]
  cwd: string
  env: Record<string, string>
  timeoutMs: number
  signal: AbortSignal
}

export interface E2eCommandResult {
  exitCode: number
  stdout: string
  stderr: string
  durationMs: number
}

export type E2eCommandRunner = (input: E2eCommandInput) => Promise<E2eCommandResult>

export type VerificationRevisionResolver = (repositoryPath: string) => Promise<string>

export interface VerificationLoadResult {
  status: 'pass' | 'fail'
  measured: LoadMeasurements
  violations: LoadSloViolation[]
}

export type VerificationLoadRunner = (
  environment: VerificationEnvironment,
  stack: ResolvedVerificationStack,
  runId: string,
  signal?: AbortSignal,
) => Promise<VerificationLoadResult>

export interface VerificationStackDeployRunner {
  deploy(
    stack: ResolvedVerificationStack['loaded'],
    environment: VerificationEnvironment,
    options?: { signal?: AbortSignal },
  ): Promise<StackDeployment>
}

export type VerificationLeaseProvider = Pick<
  VerificationEnvironmentProvider,
  'provision' | 'teardown'
>

export interface VerificationPipelineOptions {
  descriptorPath?: string
  environmentProvider?: VerificationLeaseProvider
  stackDeployer?: VerificationStackDeployRunner
  e2eRunner?: E2eCommandRunner
  loadRunner?: VerificationLoadRunner
  revisionResolver?: VerificationRevisionResolver
  reporter?: FactoryEventReporter
  logger?: Logger
  maxConcurrentEnvironments?: number
  maxRunTimeoutMs?: number
  maxEnvironmentTtlMs?: number
  maxTeardownTimeoutMs?: number
  now?: () => Date
  runId?: () => string
}

export class VerificationPipeline implements VerificationGate {
  readonly #descriptorPath: string
  readonly #environmentProvider: VerificationLeaseProvider
  readonly #stackDeployer: VerificationStackDeployRunner
  readonly #e2eRunner: E2eCommandRunner
  readonly #loadRunner: VerificationLoadRunner
  readonly #revisionResolver: VerificationRevisionResolver
  readonly #reporter?: FactoryEventReporter
  readonly #logger: Logger
  readonly #maxConcurrentEnvironments: number
  readonly #maxRunTimeoutMs: number
  readonly #maxEnvironmentTtlMs: number
  readonly #maxTeardownTimeoutMs: number
  readonly #now: () => Date
  readonly #runId: () => string
  readonly #semaphore: Semaphore

  constructor(options: VerificationPipelineOptions = {}) {
    this.#descriptorPath = options.descriptorPath ?? DEFAULT_VERIFICATION_DESCRIPTOR
    this.#environmentProvider = options.environmentProvider ?? new KubectlEnvironmentProvider()
    this.#stackDeployer = options.stackDeployer ?? new VerificationStackDeployer()
    this.#e2eRunner = options.e2eRunner ?? runE2eCommand
    this.#loadRunner = options.loadRunner ?? defaultLoadRunner
    this.#revisionResolver = options.revisionResolver ?? resolveGitHeadRevision
    this.#reporter = options.reporter
    this.#logger = normalizeLogger(options.logger ?? console)
    this.#maxConcurrentEnvironments = positiveInteger(options.maxConcurrentEnvironments ?? 2, 'maxConcurrentEnvironments')
    this.#maxRunTimeoutMs = positiveInteger(options.maxRunTimeoutMs ?? 30 * 60_000, 'maxRunTimeoutMs')
    this.#maxEnvironmentTtlMs = positiveInteger(options.maxEnvironmentTtlMs ?? 60 * 60_000, 'maxEnvironmentTtlMs')
    this.#maxTeardownTimeoutMs = positiveInteger(options.maxTeardownTimeoutMs ?? 5 * 60_000, 'maxTeardownTimeoutMs')
    this.#now = options.now ?? (() => new Date())
    this.#runId = options.runId ?? randomUUID
    this.#semaphore = new Semaphore(this.#maxConcurrentEnvironments)
  }

  async verify(input: VerificationGateInput): Promise<VerificationVerdict> {
    const runId = dnsLabel(input.runId ?? this.#runId()).slice(0, 24)
    const startedAt = this.#now()
    const stages = emptyStages()
    const descriptorPath = input.descriptorPath ?? this.#descriptorPath
    let stack: ResolvedVerificationStack | undefined
    let environment: VerificationEnvironment | undefined
    let deployedEnvironment: VerificationEnvironment | undefined
    let deployment: StackDeployment | undefined
    let release: (() => void) | undefined
    let functionalPass = false
    let timedOut = false
    let reason = 'verification did not run'
    const controller = new AbortController()
    let overallTimer: ReturnType<typeof setTimeout> | undefined
    const armOverallTimeout = (timeoutMs: number): void => {
      if (overallTimer) clearTimeout(overallTimer)
      overallTimer = setTimeout(() => {
        timedOut = true
        controller.abort(new VerificationTimeoutError(`verification timed out after ${timeoutMs}ms`))
      }, timeoutMs)
    }
    armOverallTimeout(this.#maxRunTimeoutMs)

    try {
      const resolveStarted = Date.now()
      try {
        stack = await withinTimeout(
          loadVerificationGateStack(input.repositoryPath, descriptorPath),
          this.#maxRunTimeoutMs,
          'verification resolution',
          controller.signal,
        )
        if (input.expectedHeadSha) {
          const checkoutHeadSha = await this.#revisionResolver(stack.repositoryPath)
          if (checkoutHeadSha.toLowerCase() !== input.expectedHeadSha.toLowerCase()) {
            throw new Error(
              `feature checkout head mismatch: expected ${input.expectedHeadSha}, found ${checkoutHeadSha}`,
            )
          }
        }
        stages.resolve = passStage(resolveStarted)
      } catch (error) {
        stages.resolve = failStage(resolveStarted, error)
        reason = `verification resolution failed: ${message(error)}`
        return await this.#finish(
          input, runId, startedAt, descriptorPath, stages, false, reason, undefined,
          controller.signal.aborted || error instanceof VerificationTimeoutError,
        )
      }

      const overallMs = Math.min(stack.timeouts.overallMs, this.#maxRunTimeoutMs)
      const remainingOverallMs = Math.max(1, overallMs - (Date.now() - startedAt.getTime()))
      armOverallTimeout(remainingOverallMs)

      const acquireStarted = Date.now()
      try {
        release = await this.#semaphore.acquire(controller.signal)
      } catch (error) {
        stages.provision = timeoutOrFailure(acquireStarted, error, controller.signal)
        reason = controller.signal.aborted ? 'verification timed out waiting for environment capacity' : message(error)
        return await this.#finish(input, runId, startedAt, stack.descriptorPath, stages, false, reason, undefined, controller.signal.aborted)
      }

      const provisionStarted = Date.now()
      try {
        const provisionInput: ProvisionEnvironmentInput = {
          runId,
          repository: input.repository,
          namespacePrefix: 'factory-verification',
          ttlMs: Math.min(stack.environmentTtlMs, this.#maxEnvironmentTtlMs),
          maxActiveEnvironments: this.#maxConcurrentEnvironments,
          signal: controller.signal,
        }
        environment = await this.#environmentProvider.provision(provisionInput)
        stages.provision = passStage(provisionStarted)
      } catch (error) {
        stages.provision = timeoutOrFailure(provisionStarted, error, controller.signal)
        reason = stageReason('provision', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, deployment, controller.signal.aborted,
        )
      }

      const deployStarted = Date.now()
      try {
        deployment = await withinTimeout(
          this.#stackDeployer.deploy(stack.loaded, environment, { signal: controller.signal }),
          remainingTimeout(stack, this.#maxRunTimeoutMs, startedAt),
          'deploy stage',
          controller.signal,
        )
        deployedEnvironment = asVerificationEnvironment(environment, stack, deployment.endpoints)
        stages.deploy = passStage(deployStarted)
      } catch (error) {
        stages.deploy = timeoutOrFailure(deployStarted, error, controller.signal)
        reason = stageReason('deploy', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, deployment, controller.signal.aborted,
        )
      }

      const e2eStarted = Date.now()
      try {
        const result = await withinTimeout(
          this.#e2eRunner({
            image: stack.e2e.image,
            environment: deployedEnvironment,
            command: stack.e2e.command,
            args: stack.e2e.args,
            cwd: stack.repositoryPath,
            env: {
              ...stack.e2e.env,
              FACTORY_VERIFICATION_RUN_ID: runId,
              FACTORY_ENVIRONMENT_ID: deployedEnvironment.id,
              FACTORY_ENVIRONMENT_NAMESPACE: deployedEnvironment.namespace,
              ...endpointEnvironment(deployedEnvironment),
            },
            timeoutMs: stack.e2e.timeoutMs,
            signal: controller.signal,
          }),
          stack.e2e.timeoutMs,
          'E2E stage',
          controller.signal,
        )
        stages.e2e = {
          status: result.exitCode === 0 ? 'pass' : 'fail',
          durationMs: result.durationMs,
          exitCode: result.exitCode,
          ...(result.exitCode === 0 ? {} : { error: result.stderr.trim().slice(-4_000) || `exited with ${result.exitCode}` }),
        }
        if (result.exitCode !== 0) {
          reason = `E2E stage failed with exit code ${result.exitCode}`
          return await this.#finishWithTeardown(
            input, runId, startedAt, stack, stages, false, reason, environment, deployment, false,
          )
        }
      } catch (error) {
        stages.e2e = timeoutOrFailure(e2eStarted, error, controller.signal)
        reason = stageReason('E2E', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, deployment, controller.signal.aborted || error instanceof VerificationTimeoutError,
        )
      }

      const loadStarted = Date.now()
      try {
        const result = await withinTimeout(
          this.#loadRunner(deployedEnvironment, stack, runId, controller.signal),
          stack.load.timeoutMs,
          'load stage',
          controller.signal,
        )
        stages.load = {
          status: result.status,
          durationMs: elapsed(loadStarted),
          measured: result.measured,
          violations: result.violations,
          ...(result.status === 'pass' ? {} : { error: 'load SLO thresholds were violated' }),
        }
        if (result.status === 'fail') {
          reason = 'load stage violated one or more SLO thresholds'
          return await this.#finishWithTeardown(
            input, runId, startedAt, stack, stages, false, reason, environment, deployment, false,
          )
        }
      } catch (error) {
        stages.load = timeoutOrFailure(loadStarted, error, controller.signal)
        reason = stageReason('load', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, deployment, controller.signal.aborted || error instanceof VerificationTimeoutError,
        )
      }

      functionalPass = true
      reason = 'provision, deploy, E2E, and load SLO verification passed'
      return await this.#finishWithTeardown(
        input, runId, startedAt, stack, stages, functionalPass, reason, environment, deployment, timedOut,
      )
    } finally {
      if (overallTimer) clearTimeout(overallTimer)
      release?.()
    }
  }

  async #finishWithTeardown(
    input: VerificationGateInput,
    runId: string,
    startedAt: Date,
    stack: ResolvedVerificationStack,
    stages: VerificationEvidence['stages'],
    functionalPass: boolean,
    reason: string,
    environment: VerificationEnvironment | undefined,
    deployment: StackDeployment | undefined,
    timedOut: boolean,
  ): Promise<VerificationVerdict> {
    const evaluateStarted = Date.now()
    stages.evaluate = {
      status: functionalPass ? 'pass' : 'fail',
      durationMs: elapsed(evaluateStarted),
      ...(functionalPass ? {} : { error: reason }),
    }

    if (environment) {
      const teardownStarted = Date.now()
      const teardownController = new AbortController()
      const teardownMs = Math.min(stack.timeouts.teardownMs, this.#maxTeardownTimeoutMs)
      const teardownDeadline = Date.now() + teardownMs
      try {
        const errors: unknown[] = []
        try {
          await withinTimeout(
            deployment?.dispose() ?? Promise.resolve(),
            Math.max(1, Math.min(5_000, Math.floor(teardownMs / 4))),
            'deployment cleanup',
          )
        } catch (error) {
          errors.push(error)
        }
        try {
          await withinTimeout(
            this.#environmentProvider.teardown(environment, { signal: teardownController.signal }),
            Math.max(1, teardownDeadline - Date.now()),
            'environment teardown',
          )
        } catch (error) {
          errors.push(error)
        }
        if (errors.length > 0) throw new AggregateError(errors, errors.map(message).join('; '))
        stages.teardown = passStage(teardownStarted)
      } catch (error) {
        teardownController.abort(error)
        stages.teardown = error instanceof VerificationTimeoutError
          ? { status: 'timed_out', durationMs: elapsed(teardownStarted), error: message(error) }
          : failStage(teardownStarted, error)
        functionalPass = false
        reason = `verification teardown failed; environment reaper retained responsibility: ${message(error)}`
      }
    } else {
      stages.teardown = { status: 'skipped', durationMs: 0 }
    }
    return await this.#finish(
      input, runId, startedAt, stack.descriptorPath, stages, functionalPass, reason, environment, timedOut,
    )
  }

  async #finish(
    input: VerificationGateInput,
    runId: string,
    startedAt: Date,
    descriptorPath: string,
    stages: VerificationEvidence['stages'],
    passed: boolean,
    reason: string,
    environment: VerificationEnvironment | undefined,
    timedOut: boolean,
  ): Promise<VerificationVerdict> {
    if (stages.evaluate.status === 'skipped') {
      stages.evaluate = { status: 'fail', durationMs: 0, error: reason }
    }
    const evidence: VerificationEvidence = {
      contract: VERIFICATION_EVIDENCE_CONTRACT,
      runId,
      repository: input.repository,
      repositoryPath: input.repositoryPath,
      descriptorPath,
      ...(input.expectedHeadSha ? { expectedHeadSha: input.expectedHeadSha } : {}),
      ...(environment ? {
        environmentId: environment.id,
        ...(environment.namespace ? { namespace: environment.namespace } : {}),
      } : {}),
      startedAt: startedAt.toISOString(),
      completedAt: this.#now().toISOString(),
      timedOut,
      stages,
    }
    const verdict: VerificationVerdict = { status: passed ? 'pass' : 'fail', passed, reason, evidence }
    if (this.#reporter) {
      try {
        await this.#reporter.report(createFactoryCloudEventV1({
          type: 'verification.completed',
          level: passed ? 'info' : 'error',
          runId,
          phase: 'verification',
          status: passed ? 'succeeded' : 'failed',
          run: {
            source: 'verification-gate',
            repository: input.repository,
            ...(input.issueKey ? { issueKey: input.issueKey } : {}),
          },
          verification: observabilityEvidence(verdict),
        }))
      } catch (error) {
        try {
          this.#logger.warn?.('[factory-verification] failed to report completion evidence', error)
        } catch {
          // Reporting and logging are best-effort and must not change the gate verdict.
        }
      }
    }
    return verdict
  }
}

const execFileAsync = promisify(execFile)

export async function resolveGitHeadRevision(repositoryPath: string): Promise<string> {
  let stdout: string
  try {
    ({ stdout } = await execFileAsync('git', ['rev-parse', '--verify', 'HEAD'], {
      cwd: repositoryPath,
      encoding: 'utf8',
      maxBuffer: 64 * 1024,
    }))
  } catch (error) {
    throw new Error(`could not resolve feature checkout HEAD: ${message(error)}`)
  }
  const revision = stdout.trim()
  if (!/^[a-f0-9]{40,64}$/u.test(revision)) {
    throw new Error('feature checkout HEAD was not a valid Git object id')
  }
  try {
    ({ stdout } = await execFileAsync('git', ['status', '--porcelain=v1', '--untracked-files=all'], {
      cwd: repositoryPath,
      encoding: 'utf8',
      maxBuffer: 1024 * 1024,
    }))
  } catch (error) {
    throw new Error(`could not inspect feature checkout state: ${message(error)}`)
  }
  if (stdout.trim()) {
    throw new Error('feature checkout contains uncommitted changes')
  }
  return revision
}

export async function runE2eCommand(input: E2eCommandInput): Promise<E2eCommandResult> {
  const started = Date.now()
  input.signal.throwIfAborted()
  const runner = new ProcessCommandRunner()
  const connection = { context: input.environment.kubeContext }
  const kubectl = kubectlConnectionArgs(connection)
  const pod = dnsLabel(`factory-e2e-${input.env.FACTORY_VERIFICATION_RUN_ID ?? randomUUID()}`)
    .slice(0, 63)
  const temporaryDirectory = await mkdtemp(join(tmpdir(), 'factory-verification-e2e-'))
  const archivePath = join(temporaryDirectory, 'source.tar')
  const environmentEntries = Object.entries(input.env)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(key)) {
        throw new Error(`invalid E2E environment variable name ${JSON.stringify(key)}`)
      }
      return `${key}=${value}`
    })
  const podResource = {
    apiVersion: 'v1',
    kind: 'Pod',
    metadata: {
      name: pod,
      namespace: input.environment.namespace,
      labels: {
        'app.kubernetes.io/managed-by': 'factory',
        'factory.agentworkforce.dev/environment-id': input.environment.id,
      },
    },
    spec: {
      restartPolicy: 'Never',
      terminationGracePeriodSeconds: 1,
      activeDeadlineSeconds: Math.max(60, Math.ceil(input.timeoutMs / 1_000) + 120),
      serviceAccountName: 'factory-guardrail-workload',
      automountServiceAccountToken: false,
      securityContext: {
        runAsNonRoot: true,
        runAsUser: 1_000,
        runAsGroup: 1_000,
        fsGroup: 1_000,
        seccompProfile: { type: 'RuntimeDefault' },
      },
      containers: [{
        name: 'runner',
        image: input.image,
        imagePullPolicy: 'IfNotPresent',
        command: ['sh', '-c', 'while true; do sleep 3600; done'],
        securityContext: {
          allowPrivilegeEscalation: false,
          readOnlyRootFilesystem: true,
          capabilities: { drop: ['ALL'] },
        },
        resources: {
          requests: { cpu: '25m', memory: '64Mi' },
          limits: { cpu: '1', memory: '1Gi' },
        },
        volumeMounts: [
          { name: 'workspace', mountPath: '/workspace' },
          { name: 'tmp', mountPath: '/tmp' },
        ],
      }],
      volumes: [
        { name: 'workspace', emptyDir: {} },
        { name: 'tmp', emptyDir: {} },
      ],
    },
  }

  try {
    await execFileAsync('git', [
      '-C', input.cwd,
      'archive', '--format=tar', `--output=${archivePath}`, 'HEAD',
    ], { encoding: 'utf8', maxBuffer: 1024 * 1024 })
    await runner.run('kubectl', [
      ...kubectl,
      '--namespace', input.environment.namespace,
      'create', '--filename', '-',
    ], {
      input: JSON.stringify(podResource),
      timeoutMs: 30_000,
      signal: input.signal,
    })
    await runner.run('kubectl', [
      ...kubectl,
      '--namespace', input.environment.namespace,
      'wait', `pod/${pod}`, '--for=condition=Ready', '--timeout=120s',
    ], { timeoutMs: 125_000, signal: input.signal })
    await runner.run('kubectl', [
      ...kubectl,
      '--namespace', input.environment.namespace,
      'cp', archivePath, `${pod}:/tmp/source.tar`,
    ], { timeoutMs: 120_000, signal: input.signal })
    await runner.run('kubectl', [
      ...kubectl,
      '--namespace', input.environment.namespace,
      'exec', pod, '--',
      'tar', '-xf', '/tmp/source.tar', '-C', '/workspace',
    ], { timeoutMs: 120_000, signal: input.signal })

    try {
      const result = await runner.run('kubectl', [
        ...kubectl,
        '--namespace', input.environment.namespace,
        'exec', pod, '--',
        'env', ...environmentEntries,
        'sh', '-c', 'cd /workspace && exec "$@"',
        'factory-e2e', input.command, ...input.args,
      ], { timeoutMs: input.timeoutMs, signal: input.signal })
      return { exitCode: 0, ...result, durationMs: elapsed(started) }
    } catch (error) {
      if (input.signal.aborted || /timed out after/iu.test(message(error))) {
        throw new VerificationTimeoutError(`E2E stage timed out after ${input.timeoutMs}ms`)
      }
      if (error instanceof CommandExecutionError) {
        return {
          exitCode: 1,
          stdout: error.stdout,
          stderr: error.stderr || error.message,
          durationMs: elapsed(started),
        }
      }
      throw error
    }
  } finally {
    await runner.run('kubectl', [
      ...kubectl,
      '--namespace', input.environment.namespace,
      'delete', 'pod', pod,
      '--ignore-not-found=true', '--wait=true', '--grace-period=0', '--force',
    ], { timeoutMs: 60_000 }).catch(() => undefined)
    await rm(temporaryDirectory, { recursive: true, force: true })
  }
}

const defaultLoadRunner: VerificationLoadRunner = async (environment, stack, runId, signal) => {
  const profile = await loadLoadProfile(stack.load.profilePath)
  const result = await runLoad({
    id: environment.id,
    namespace: environment.namespace,
    kubeContext: environment.kubeContext,
    endpoints: environment.internalEndpoints,
  }, profile, {
    namespace: environment.namespace,
    kubeContext: environment.kubeContext,
    timeoutMs: stack.load.timeoutMs,
    k6Image: stack.load.k6Image,
    runId,
    signal,
  })
  return { status: result.status, measured: result.measured, violations: result.violations }
}

function endpointEnvironment(environment: VerificationEnvironment): Record<string, string> {
  const values: Record<string, string> = {}
  for (const [name, endpoint] of Object.entries(environment.endpoints)) {
    const key = name.toUpperCase().replace(/[^A-Z0-9]+/gu, '_').replace(/^_+|_+$/gu, '')
    values[`FACTORY_EXTERNAL_ENDPOINT_${key}`] = endpoint
  }
  for (const [name, endpoint] of Object.entries(environment.internalEndpoints)) {
    const key = name.toUpperCase().replace(/[^A-Z0-9]+/gu, '_').replace(/^_+|_+$/gu, '')
    values[`FACTORY_ENDPOINT_${key}`] = endpoint
    values[`FACTORY_INTERNAL_ENDPOINT_${key}`] = endpoint
  }
  return values
}

function observabilityEvidence(verdict: VerificationVerdict) {
  const { evidence } = verdict
  return {
    contract: evidence.contract,
    environmentId: evidence.environmentId,
    namespace: evidence.namespace,
    verdict: verdict.status,
    timedOut: evidence.timedOut,
    stages: {
      resolve: evidence.stages.resolve.status,
      provision: evidence.stages.provision.status,
      deploy: evidence.stages.deploy.status,
      e2e: evidence.stages.e2e.status,
      load: evidence.stages.load.status,
      evaluate: evidence.stages.evaluate.status,
      teardown: evidence.stages.teardown.status,
    },
    e2e: {
      durationMs: evidence.stages.e2e.durationMs,
      exitCode: evidence.stages.e2e.exitCode,
    },
    load: evidence.stages.load.measured ? {
      durationMs: evidence.stages.load.durationMs,
      measured: evidence.stages.load.measured,
      violations: evidence.stages.load.violations ?? [],
    } : undefined,
  }
}

class Semaphore {
  readonly #capacity: number
  #active = 0
  readonly #waiters: Array<{
    resolve: (release: () => void) => void
    reject: (error: Error) => void
    signal: AbortSignal
    abort: () => void
  }> = []

  constructor(capacity: number) {
    this.#capacity = capacity
  }

  async acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) throw new VerificationTimeoutError('verification aborted before environment admission')
    if (this.#active < this.#capacity) return this.#claim()
    return await new Promise((resolve, reject) => {
      const waiter = {
        resolve,
        reject,
        signal,
        abort: () => {
          const index = this.#waiters.indexOf(waiter)
          if (index >= 0) this.#waiters.splice(index, 1)
          reject(new VerificationTimeoutError('verification aborted waiting for environment admission'))
        },
      }
      this.#waiters.push(waiter)
      signal.addEventListener('abort', waiter.abort, { once: true })
    })
  }

  #claim(): () => void {
    this.#active += 1
    let released = false
    return () => {
      if (released) return
      released = true
      this.#active -= 1
      this.#drain()
    }
  }

  #drain(): void {
    while (this.#active < this.#capacity && this.#waiters.length > 0) {
      const waiter = this.#waiters.shift()!
      waiter.signal.removeEventListener('abort', waiter.abort)
      if (waiter.signal.aborted) {
        waiter.reject(new VerificationTimeoutError('verification aborted waiting for environment admission'))
        continue
      }
      waiter.resolve(this.#claim())
    }
  }
}

export class VerificationTimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'VerificationTimeoutError'
  }
}

async function withinTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
  signal?: AbortSignal,
): Promise<T> {
  if (signal?.aborted) throw new VerificationTimeoutError(`${label} aborted`)
  let timer: ReturnType<typeof setTimeout> | undefined
  let abort: (() => void) | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new VerificationTimeoutError(`${label} timed out after ${timeoutMs}ms`)), timeoutMs)
    if (signal) {
      abort = () => reject(new VerificationTimeoutError(`${label} aborted`))
      signal.addEventListener('abort', abort, { once: true })
    }
  })
  try {
    return await Promise.race([operation, timeout])
  } finally {
    if (timer) clearTimeout(timer)
    if (abort) signal?.removeEventListener('abort', abort)
    void operation.catch(() => undefined)
  }
}

const emptyStage = (): VerificationStageEvidence => ({ status: 'skipped', durationMs: 0 })
const emptyStages = (): VerificationEvidence['stages'] => ({
  resolve: emptyStage(),
  provision: emptyStage(),
  deploy: emptyStage(),
  e2e: emptyStage(),
  load: emptyStage(),
  evaluate: emptyStage(),
  teardown: emptyStage(),
})

const passStage = (started: number): VerificationStageEvidence => ({ status: 'pass', durationMs: elapsed(started) })
const failStage = (started: number, error: unknown): VerificationStageEvidence => ({
  status: 'fail',
  durationMs: elapsed(started),
  error: message(error),
})
const timeoutOrFailure = (
  started: number,
  error: unknown,
  signal?: AbortSignal,
): VerificationStageEvidence => ({
  status: signal?.aborted || error instanceof VerificationTimeoutError ? 'timed_out' : 'fail',
  durationMs: elapsed(started),
  error: message(error),
})
const stageReason = (stage: string, error: unknown, signal?: AbortSignal): string =>
  signal?.aborted || error instanceof VerificationTimeoutError
    ? `${stage} stage timed out: ${message(error)}`
    : `${stage} stage failed: ${message(error)}`

const elapsed = (started: number): number => Math.max(0, Date.now() - started)
const message = (error: unknown): string => error instanceof Error ? error.message : String(error)
const dnsLabel = (value: string): string => value
  .toLowerCase()
  .replace(/[^a-z0-9-]+/gu, '-')
  .replace(/^-+|-+$/gu, '') || 'verification'
const positiveInteger = (value: number, field: string): number => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${field} must be a positive integer`)
  return value
}

function asVerificationEnvironment(
  environment: VerificationEnvironment,
  stack: ResolvedVerificationStack,
  endpoints: Record<string, string>,
): VerificationEnvironment {
  const namespace = environment.namespace
  const internalEndpoints = Object.fromEntries(stack.loaded.descriptor.endpoints.map((endpoint) => [
    endpoint.name,
    `${endpoint.protocol}://${endpoint.service}.${namespace}.svc.cluster.local:${endpoint.port}${endpoint.path}`,
  ]))
  return {
    id: environment.id,
    namespace,
    endpoints,
    internalEndpoints,
    ...(environment.kubeContext ? { kubeContext: environment.kubeContext } : {}),
    expiresAt: environment.expiresAt,
  }
}

const remainingTimeout = (
  stack: ResolvedVerificationStack,
  maxRunTimeoutMs: number,
  startedAt: Date,
): number => Math.max(1, Math.min(stack.timeouts.overallMs, maxRunTimeoutMs) - (Date.now() - startedAt.getTime()))
