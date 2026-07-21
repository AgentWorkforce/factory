import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import type { FactoryCloudEventInputV1 } from '../observability/events'
import type { FactoryEventReporter, FactoryEventReportResult } from '../ports/observability'
import type {
  ProvisionEnvironmentInput,
  VerificationEnvironment,
} from '../ports/environment'
import type { LoadedVerificationStack } from './verification-stack-descriptor'
import type { StackDeployment } from './verification-stack-deployer'
import {
  VerificationPipeline,
  type E2eCommandResult,
  type VerificationLeaseProvider,
  type VerificationLoadResult,
  type VerificationStackDeployRunner,
} from './verification-pipeline'

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map(async (root) => await rm(root, { recursive: true, force: true })))
})

class RecordingEnvironmentProvider implements VerificationLeaseProvider, VerificationStackDeployRunner {
  readonly calls: string[] = []
  teardownError?: Error

  async provision(input: ProvisionEnvironmentInput): Promise<VerificationEnvironment> {
    this.calls.push('provision')
    return {
      id: `env-${input.runId}`,
      namespace: `env-${input.runId}`,
      endpoints: {},
      internalEndpoints: {},
      expiresAt: new Date(Date.now() + input.ttlMs).toISOString(),
    }
  }

  async deploy(
    _stack: LoadedVerificationStack,
    _environment: VerificationEnvironment,
  ): Promise<StackDeployment> {
    this.calls.push('deploy')
    return {
      endpoints: { api: 'http://127.0.0.1:1234/health' },
      dispose: async () => undefined,
    }
  }

  async teardown(): Promise<void> {
    this.calls.push('teardown')
    if (this.teardownError) throw this.teardownError
  }
}

class RecordingReporter implements FactoryEventReporter {
  readonly events: FactoryCloudEventInputV1[] = []

  async report(event: FactoryCloudEventInputV1): Promise<void> {
    this.events.push(event)
  }

  async flush(): Promise<FactoryEventReportResult> {
    return { delivered: this.events.length, pending: 0, attempts: 1, stoppedReason: 'empty' }
  }
}

describe('VerificationPipeline', () => {
  it('runs provision -> deploy -> E2E -> load -> evaluate -> teardown and reports evidence', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    const reporter = new RecordingReporter()
    const calls: string[] = []
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      reporter,
      runId: () => 'green-run',
      revisionResolver: async () => 'abc123',
      e2eRunner: async (input): Promise<E2eCommandResult> => {
        calls.push('e2e')
        expect(input.env.FACTORY_ENDPOINT_API).toContain('.svc.cluster.local')
        expect(input.env.FACTORY_EXTERNAL_ENDPOINT_API).toBe('http://127.0.0.1:1234/health')
        return { exitCode: 0, stdout: 'ok', stderr: '', durationMs: 12 }
      },
      loadRunner: async (deployed): Promise<VerificationLoadResult> => {
        calls.push('load')
        expect(deployed.internalEndpoints.api).toContain('.svc.cluster.local')
        return passingLoad()
      },
    })

    const verdict = await pipeline.verify({
      repository: 'AgentWorkforce/factory',
      repositoryPath: root,
      issueKey: '145',
      expectedHeadSha: 'abc123',
    })

    expect(verdict).toMatchObject({
      status: 'pass',
      passed: true,
      evidence: {
        environmentId: 'env-green-run',
        expectedHeadSha: 'abc123',
        stages: {
          resolve: { status: 'pass' },
          provision: { status: 'pass' },
          deploy: { status: 'pass' },
          e2e: { status: 'pass', exitCode: 0 },
          load: { status: 'pass' },
          evaluate: { status: 'pass' },
          teardown: { status: 'pass' },
        },
      },
    })
    expect([...environment.calls.slice(0, 2), ...calls, environment.calls.at(-1)]).toEqual([
      'provision', 'deploy', 'e2e', 'load', 'teardown',
    ])
    expect(reporter.events).toHaveLength(1)
    expect(reporter.events[0]).toMatchObject({
      type: 'verification.completed',
      status: 'succeeded',
      verification: {
        environmentId: 'env-green-run',
        verdict: 'pass',
        stages: { e2e: 'pass', load: 'pass', teardown: 'pass' },
      },
    })
  })

  it('preserves a green verdict when completion evidence cannot be serialized', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    const reporter = new RecordingReporter()
    const warnings: Array<[string, ...unknown[]]> = []
    const load = passingLoad()
    load.measured.histogram = [{ upperBoundMs: null, count: 100 }]
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      reporter,
      logger: { warn: (message, ...details) => warnings.push([message, ...details]) },
      runId: () => 'invalid-evidence',
      e2eRunner: async () => ({ exitCode: 0, stdout: '', stderr: '', durationMs: 1 }),
      loadRunner: async () => load,
    })

    const verdict = await pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })

    expect(verdict.passed).toBe(true)
    expect(verdict.evidence.stages.teardown.status).toBe('pass')
    expect(reporter.events).toEqual([])
    expect(warnings).toContainEqual([
      '[factory-verification] failed to report completion evidence',
      expect.anything(),
    ])
  })

  it('fails closed before provisioning when the feature checkout does not match the reviewed head', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      runId: () => 'stale-checkout',
      revisionResolver: async () => 'stale-head',
      e2eRunner: async () => ({ exitCode: 0, stdout: '', stderr: '', durationMs: 1 }),
      loadRunner: async () => passingLoad(),
    })

    const verdict = await pipeline.verify({
      repository: 'AgentWorkforce/factory',
      repositoryPath: root,
      expectedHeadSha: 'reviewed-head',
    })

    expect(verdict).toMatchObject({
      passed: false,
      reason: expect.stringContaining('feature checkout head mismatch'),
      evidence: {
        stages: {
          resolve: { status: 'fail' },
          provision: { status: 'skipped' },
          teardown: { status: 'skipped' },
        },
      },
    })
    expect(environment.calls).toEqual([])
  })

  it('fails closed on E2E failure, skips load, and still tears down', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    let loadCalls = 0
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      runId: () => 'red-e2e',
      e2eRunner: async () => ({ exitCode: 23, stdout: '', stderr: 'assertion failed', durationMs: 3 }),
      loadRunner: async () => {
        loadCalls += 1
        return passingLoad()
      },
    })

    const verdict = await pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })

    expect(verdict.passed).toBe(false)
    expect(verdict.evidence.stages.e2e).toMatchObject({ status: 'fail', exitCode: 23 })
    expect(verdict.evidence.stages.load.status).toBe('skipped')
    expect(verdict.evidence.stages.teardown.status).toBe('pass')
    expect(loadCalls).toBe(0)
    expect(environment.calls).toEqual(['provision', 'deploy', 'teardown'])
  })

  it('retains load measurements and violations on an SLO failure', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    const failedLoad = passingLoad()
    failedLoad.status = 'fail'
    failedLoad.violations = [{
      metric: 'p95LatencyMs',
      actual: 501,
      threshold: 500,
      operator: 'at-most',
    }]
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      runId: () => 'red-load',
      e2eRunner: async () => ({ exitCode: 0, stdout: '', stderr: '', durationMs: 1 }),
      loadRunner: async () => failedLoad,
    })

    const verdict = await pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })

    expect(verdict).toMatchObject({
      passed: false,
      evidence: {
        stages: {
          e2e: { status: 'pass' },
          load: {
            status: 'fail',
            measured: { requestCount: 100 },
            violations: [{ metric: 'p95LatencyMs', actual: 501 }],
          },
          teardown: { status: 'pass' },
        },
      },
    })
  })

  it('bounds a stuck E2E stage and tears down with a fresh cleanup budget', async () => {
    const root = await fixtureRepository({ e2eTimeoutSeconds: 1, overallTimeoutSeconds: 2 })
    const environment = new RecordingEnvironmentProvider()
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      runId: () => 'timeout',
      e2eRunner: async () => await new Promise(() => undefined),
      loadRunner: async () => passingLoad(),
    })

    const verdict = await pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })

    expect(verdict.passed).toBe(false)
    expect(verdict.evidence.timedOut).toBe(true)
    expect(verdict.evidence.stages.e2e.status).toBe('timed_out')
    expect(verdict.evidence.stages.teardown.status).toBe('pass')
    expect(environment.calls).toEqual(['provision', 'deploy', 'teardown'])
  })

  it('turns teardown failure into a red verdict for the reaper backstop', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    environment.teardownError = new Error('namespace deletion unavailable')
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      runId: () => 'teardown-red',
      e2eRunner: async () => ({ exitCode: 0, stdout: '', stderr: '', durationMs: 1 }),
      loadRunner: async () => passingLoad(),
    })

    const verdict = await pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })

    expect(verdict.passed).toBe(false)
    expect(verdict.evidence.stages.evaluate.status).toBe('pass')
    expect(verdict.evidence.stages.teardown.status).toBe('fail')
    expect(verdict.reason).toContain('reaper retained responsibility')
  })

  it('admits no more than the configured number of live environments', async () => {
    const root = await fixtureRepository()
    const environment = new RecordingEnvironmentProvider()
    let run = 0
    let e2eCalls = 0
    let releaseFirst!: () => void
    let signalFirst!: () => void
    const firstStarted = new Promise<void>((resolve) => { signalFirst = resolve })
    const firstReleased = new Promise<void>((resolve) => { releaseFirst = resolve })
    const pipeline = new VerificationPipeline({
      environmentProvider: environment,
      stackDeployer: environment,
      maxConcurrentEnvironments: 1,
      runId: () => `concurrent-${++run}`,
      e2eRunner: async () => {
        e2eCalls += 1
        if (e2eCalls === 1) {
          signalFirst()
          await firstReleased
        }
        return { exitCode: 0, stdout: '', stderr: '', durationMs: 1 }
      },
      loadRunner: async () => passingLoad(),
    })

    const first = pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })
    await firstStarted
    const second = pipeline.verify({ repository: 'AgentWorkforce/factory', repositoryPath: root })
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(environment.calls.filter((call) => call === 'provision')).toHaveLength(1)

    releaseFirst()
    await expect(Promise.all([first, second])).resolves.toMatchObject([{ passed: true }, { passed: true }])
    expect(environment.calls.filter((call) => call === 'provision')).toHaveLength(2)
  })
})

async function fixtureRepository(options: { e2eTimeoutSeconds?: number; overallTimeoutSeconds?: number } = {}): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-verification-'))
  roots.push(root)
  await mkdir(join(root, '.factory'), { recursive: true })
  await writeFile(join(root, '.factory', 'verification-stack.yaml'), `
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: factory-test
source:
  type: manifests
  paths: [kubernetes.yaml]
services:
  - name: api
    workload: { kind: deployment }
    readiness: { type: http, port: 8080 }
endpoints:
  - name: api
    service: api
    port: 8080
    path: /health
verification:
  environmentTtlSeconds: 120
  e2e:
    command: node
    args: [test-e2e.mjs]
    timeoutSeconds: ${options.e2eTimeoutSeconds ?? 30}
  load:
    profile: load.yaml
    timeoutSeconds: 30
  overallTimeoutSeconds: ${options.overallTimeoutSeconds ?? 120}
  teardownTimeoutSeconds: 30
`, 'utf8')
  return root
}

function passingLoad(): VerificationLoadResult {
  return {
    status: 'pass',
    measured: {
      requestCount: 100,
      errorCount: 0,
      errorRate: 0,
      throughputRps: 50,
      durationMs: 2_000,
      latency: {
        minMs: 1,
        averageMs: 3,
        medianMs: 2,
        maxMs: 10,
        p95Ms: 5,
        p99Ms: 8,
      },
      histogram: [
        { upperBoundMs: 5, count: 95 },
        { upperBoundMs: null, count: 5 },
      ],
    },
    violations: [],
  }
}
