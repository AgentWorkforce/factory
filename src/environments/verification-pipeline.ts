import { randomUUID } from 'node:crypto'
import { execFile, spawn } from 'node:child_process'
import { promisify } from 'node:util'

import type { FactoryEventReporter } from '../ports/observability.js'
import type { VerificationEnvironment, VerificationEnvironmentProvider } from '../ports/environment.js'
import { createFactoryCloudEventV1 } from '../observability/events.js'
import type { LoadMeasurements, LoadSloViolation } from './load-harness.js'
import { runLoad } from './load-harness.js'
import { loadLoadProfile } from './load-profile.js'
import { KubectlEnvironmentProvider } from './kubernetes-environment.js'
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
) => Promise<VerificationLoadResult>

export interface VerificationPipelineOptions {
  descriptorPath?: string
  environmentProvider?: VerificationEnvironmentProvider
  e2eRunner?: E2eCommandRunner
  loadRunner?: VerificationLoadRunner
  revisionResolver?: VerificationRevisionResolver
  reporter?: FactoryEventReporter
  maxConcurrentEnvironments?: number
  maxRunTimeoutMs?: number
  maxEnvironmentTtlMs?: number
  maxTeardownTimeoutMs?: number
  now?: () => Date
  runId?: () => string
}

export class VerificationPipeline implements VerificationGate {
  readonly #descriptorPath: string
  readonly #environmentProvider: VerificationEnvironmentProvider
  readonly #e2eRunner: E2eCommandRunner
  readonly #loadRunner: VerificationLoadRunner
  readonly #revisionResolver: VerificationRevisionResolver
  readonly #reporter?: FactoryEventReporter
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
    this.#e2eRunner = options.e2eRunner ?? runE2eCommand
    this.#loadRunner = options.loadRunner ?? defaultLoadRunner
    this.#revisionResolver = options.revisionResolver ?? resolveGitHeadRevision
    this.#reporter = options.reporter
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
    let release: (() => void) | undefined
    let functionalPass = false
    let timedOut = false
    let reason = 'verification did not run'
    const controller = new AbortController()
    let overallTimer: ReturnType<typeof setTimeout> | undefined

    try {
      const resolveStarted = Date.now()
      try {
        stack = await loadVerificationGateStack(input.repositoryPath, descriptorPath)
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
        return await this.#finish(input, runId, startedAt, descriptorPath, stages, false, reason, undefined, false)
      }

      const overallMs = Math.min(stack.timeouts.overallMs, this.#maxRunTimeoutMs)
      overallTimer = setTimeout(() => {
        timedOut = true
        controller.abort(new VerificationTimeoutError(`verification timed out after ${overallMs}ms`))
      }, overallMs)

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
        environment = await this.#environmentProvider.provision({
          runId,
          repository: input.repository,
          namespacePrefix: stack.provision.namespacePrefix,
          ttlMs: Math.min(stack.provision.ttlMs, this.#maxEnvironmentTtlMs),
          maxActiveEnvironments: this.#maxConcurrentEnvironments,
          ...(stack.provision.kubeContext ? { kubeContext: stack.provision.kubeContext } : {}),
          signal: controller.signal,
        })
        stages.provision = passStage(provisionStarted)
      } catch (error) {
        stages.provision = timeoutOrFailure(provisionStarted, error, controller.signal)
        reason = stageReason('provision', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, controller.signal.aborted,
        )
      }

      const deployStarted = Date.now()
      try {
        environment = await this.#environmentProvider.deploy(environment, {
          repositoryPath: stack.repositoryPath,
          manifests: stack.deploy.manifests,
          readiness: stack.deploy.readiness,
          endpoints: stack.deploy.endpoints,
          signal: controller.signal,
        })
        stages.deploy = passStage(deployStarted)
      } catch (error) {
        stages.deploy = timeoutOrFailure(deployStarted, error, controller.signal)
        reason = stageReason('deploy', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, controller.signal.aborted,
        )
      }

      const e2eStarted = Date.now()
      try {
        const result = await withinTimeout(
          this.#e2eRunner({
            command: stack.e2e.command,
            args: stack.e2e.args,
            cwd: stack.repositoryPath,
            env: {
              ...stack.e2e.env,
              FACTORY_VERIFICATION_RUN_ID: runId,
              FACTORY_ENVIRONMENT_ID: environment.id,
              FACTORY_ENVIRONMENT_NAMESPACE: environment.namespace,
              ...endpointEnvironment(environment),
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
            input, runId, startedAt, stack, stages, false, reason, environment, false,
          )
        }
      } catch (error) {
        stages.e2e = timeoutOrFailure(e2eStarted, error, controller.signal)
        reason = stageReason('E2E', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, controller.signal.aborted || error instanceof VerificationTimeoutError,
        )
      }

      const loadStarted = Date.now()
      try {
        const result = await withinTimeout(
          this.#loadRunner(environment, stack, runId),
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
            input, runId, startedAt, stack, stages, false, reason, environment, false,
          )
        }
      } catch (error) {
        stages.load = timeoutOrFailure(loadStarted, error, controller.signal)
        reason = stageReason('load', error, controller.signal)
        return await this.#finishWithTeardown(
          input, runId, startedAt, stack, stages, false, reason, environment, controller.signal.aborted || error instanceof VerificationTimeoutError,
        )
      }

      functionalPass = true
      reason = 'provision, deploy, E2E, and load SLO verification passed'
      return await this.#finishWithTeardown(
        input, runId, startedAt, stack, stages, functionalPass, reason, environment, timedOut,
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
      try {
        await withinTimeout(
          this.#environmentProvider.teardown(environment, { signal: teardownController.signal }),
          teardownMs,
          'teardown stage',
        )
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
      ...(environment ? { environmentId: environment.id, namespace: environment.namespace } : {}),
      startedAt: startedAt.toISOString(),
      completedAt: this.#now().toISOString(),
      timedOut,
      stages,
    }
    const verdict: VerificationVerdict = { status: passed ? 'pass' : 'fail', passed, reason, evidence }
    await this.#reporter?.report(createFactoryCloudEventV1({
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
  return await new Promise((resolve, reject) => {
    const child = spawn(input.command, input.args, {
      cwd: input.cwd,
      env: { ...process.env, ...input.env },
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: process.platform !== 'win32',
    })
    let stdout = ''
    let stderr = ''
    let settled = false
    const timeout = setTimeout(() => finish(new VerificationTimeoutError(`E2E stage timed out after ${input.timeoutMs}ms`)), input.timeoutMs)
    const abort = (): void => finish(new VerificationTimeoutError('E2E stage aborted'))
    const stop = (): void => {
      if (child.pid && process.platform !== 'win32') {
        try { process.kill(-child.pid, 'SIGTERM') } catch { child.kill('SIGTERM') }
      } else {
        child.kill('SIGTERM')
      }
    }
    const finish = (error?: Error, exitCode?: number): void => {
      if (settled) return
      settled = true
      clearTimeout(timeout)
      input.signal.removeEventListener('abort', abort)
      if (error) {
        stop()
        reject(error)
      } else {
        resolve({ exitCode: exitCode ?? 1, stdout, stderr, durationMs: elapsed(started) })
      }
    }
    child.stdout.setEncoding('utf8')
    child.stderr.setEncoding('utf8')
    child.stdout.on('data', (chunk: string) => { stdout = boundedAppend(stdout, chunk) })
    child.stderr.on('data', (chunk: string) => { stderr = boundedAppend(stderr, chunk) })
    child.once('error', (error) => finish(error))
    child.once('close', (code) => finish(undefined, code ?? 1))
    input.signal.addEventListener('abort', abort, { once: true })
    if (input.signal.aborted) abort()
  })
}

const defaultLoadRunner: VerificationLoadRunner = async (environment, stack, runId) => {
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
  })
  return { status: result.status, measured: result.measured, violations: result.violations }
}

function endpointEnvironment(environment: VerificationEnvironment): Record<string, string> {
  const values: Record<string, string> = {}
  for (const [name, endpoint] of Object.entries(environment.endpoints)) {
    const key = name.toUpperCase().replace(/[^A-Z0-9]+/gu, '_').replace(/^_+|_+$/gu, '')
    values[`FACTORY_ENDPOINT_${key}`] = endpoint
  }
  for (const [name, endpoint] of Object.entries(environment.internalEndpoints)) {
    const key = name.toUpperCase().replace(/[^A-Z0-9]+/gu, '_').replace(/^_+|_+$/gu, '')
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
const boundedAppend = (current: string, chunk: string): string => `${current}${chunk}`.slice(-64 * 1024)
const dnsLabel = (value: string): string => value
  .toLowerCase()
  .replace(/[^a-z0-9-]+/gu, '-')
  .replace(/^-+|-+$/gu, '') || 'verification'
const positiveInteger = (value: number, field: string): number => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${field} must be a positive integer`)
  return value
}
