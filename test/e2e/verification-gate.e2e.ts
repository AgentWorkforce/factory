import assert from 'node:assert/strict'
import { execFile, spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'

import {
  FactoryConfigSchema,
  FactoryCloudReporter,
  FileFactoryCloudEventOutbox,
  KubernetesVerificationEnvironmentProvider,
  VerificationPipeline,
  createFactory,
  defaultKubectlEnvironmentRunner,
  parseLinearIssue,
  reapFactoryEnvironmentsOnce,
  type FactoryConfig,
  type VerificationTargetEnvironment,
  type VerificationTargetProvider,
  type VerificationTargetSpec,
  type EnvironmentStatus,
  type FactoryCloudEventBatchV1,
  type FactoryEventReporter,
  type GithubMergeGate,
  type GithubMergeInput,
  type LinearIssue,
  type LinearWriteback,
  type TriageDecision,
  type TriageEngine,
  type VerificationGate,
  type VerificationGateInput,
  type VerificationVerdict,
} from '../../src/index.ts'
import { FakeFleetClient, FakeMountClient } from '../../src/testing/index.ts'

const artifactDirectory = join(process.cwd(), 'artifacts', 'verification-gate-e2e')
const execFileAsync = promisify(execFile)

class SignalingEnvironmentProvider implements VerificationTargetProvider {
  readonly #delegate = new KubernetesVerificationEnvironmentProvider({ maxActiveEnvironments: 2 })

  constructor(readonly signalPath: string) {}

  async provision(input: VerificationTargetSpec): Promise<VerificationTargetEnvironment> {
    const environment = await this.#delegate.provision(input)
    await writeFile(this.signalPath, environment.namespace, 'utf8')
    return environment
  }

  async status(id: string): Promise<EnvironmentStatus> { return await this.#delegate.status(id) }
  async endpoints(id: string): Promise<Record<string, string>> { return await this.#delegate.endpoints(id) }
  async destroy(id: string, options?: { signal?: AbortSignal }): Promise<void> {
    await this.#delegate.destroy(id, options)
  }
}

async function leakChild(root: string, signalPath: string): Promise<void> {
  const pipeline = new VerificationPipeline({
    environmentProvider: new SignalingEnvironmentProvider(signalPath),
    runId: () => `killed-${process.pid}`,
  })
  await pipeline.verify({
    repository: 'AgentWorkforce/factory',
    repositoryPath: root,
    descriptorPath: '.factory/leak.yaml',
  })
}

async function main(): Promise<void> {
  const childMarker = process.argv.indexOf('--leak-child')
  if (childMarker >= 0) {
    await leakChild(process.argv[childMarker + 1]!, process.argv[childMarker + 2]!)
    return
  }

  const root = await fixtureRepository()
  const cloudBatches: FactoryCloudEventBatchV1[] = []
  const reporter = cloudReporter(root, cloudBatches)
  const expectedHeadSha = await gitHead(root)
  const verdicts: Record<string, VerificationVerdict> = {}
  const mergeAttempts: string[] = []
  try {
    verdicts.green = await gateThroughFactory(root, reporter, 'green', expectedHeadSha, mergeAttempts)
    assert.equal(verdicts.green.passed, true, verdicts.green.reason)
    await assertNamespaceGone(verdicts.green.evidence.namespace!)

    verdicts.redE2e = await gateThroughFactory(root, reporter, 'red-e2e', expectedHeadSha, mergeAttempts)
    assert.equal(verdicts.redE2e.passed, false)
    assert.equal(verdicts.redE2e.evidence.stages.e2e.status, 'fail')
    assert.equal(verdicts.redE2e.evidence.stages.load.status, 'skipped')
    await assertNamespaceGone(verdicts.redE2e.evidence.namespace!)

    verdicts.redLoad = await gateThroughFactory(root, reporter, 'red-load', expectedHeadSha, mergeAttempts)
    assert.equal(verdicts.redLoad.passed, false)
    assert.equal(verdicts.redLoad.evidence.stages.e2e.status, 'pass')
    assert.equal(verdicts.redLoad.evidence.stages.load.status, 'fail')
    assert.ok((verdicts.redLoad.evidence.stages.load.violations ?? []).some(
      (violation) => violation.metric === 'p95LatencyMs',
    ))
    await assertNamespaceGone(verdicts.redLoad.evidence.namespace!)

    verdicts.timeout = await pipelineGate(root, reporter, 'timeout', expectedHeadSha)
    assert.equal(verdicts.timeout.passed, false)
    assert.equal(verdicts.timeout.evidence.stages.e2e.status, 'timed_out')
    assert.equal(verdicts.timeout.evidence.stages.teardown.status, 'pass')
    await assertNamespaceGone(verdicts.timeout.evidence.namespace!)

    const killedNamespace = await killMidFlightAndReap(root)
    await assertNamespaceGone(killedNamespace)

    const delivery = await reporter.flush()
    assert.deepEqual(delivery, { delivered: 4, pending: 0, attempts: 1, stoppedReason: 'empty' })
    const reported = cloudBatches
      .flatMap((batch) => batch.events)
      .filter((event) => event.type === 'verification.completed')
    assert.equal(reported.length, 4, 'every completed gate must report one verdict')
    assert.deepEqual(reported.map((event) => event.verification?.verdict), ['pass', 'fail', 'fail', 'fail'])
    for (const event of reported) {
      assert.ok(event.verification?.environmentId, 'reported evidence must include environment id')
      assert.equal(event.verification?.stages.teardown, 'pass')
    }
    assert.deepEqual(mergeAttempts, ['green'], 'only a green live-stack verdict may reach the merge operation')

    await mkdir(artifactDirectory, { recursive: true })
    await writeFile(join(artifactDirectory, 'verdicts.json'), `${JSON.stringify({
      verdicts,
      reported,
      killedNamespace,
    }, null, 2)}\n`, 'utf8')
    process.stdout.write(`${JSON.stringify({
      green: verdicts.green.status,
      redE2e: verdicts.redE2e.status,
      redLoad: verdicts.redLoad.status,
      timeout: verdicts.timeout.status,
      killedNamespace,
      mergeAttempts,
      reportedEvents: reported.length,
    }, null, 2)}\n`)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
}

async function pipelineGate(
  root: string,
  reporter: FactoryEventReporter,
  scenario: string,
  expectedHeadSha: string,
): Promise<VerificationVerdict> {
  return await new VerificationPipeline({
    reporter,
    runId: () => `${scenario}-${process.pid}`,
    maxConcurrentEnvironments: 2,
  }).verify({
    repository: 'AgentWorkforce/factory',
    repositoryPath: root,
    descriptorPath: `.factory/${scenario}.yaml`,
    issueKey: '145',
    expectedHeadSha,
  })
}

async function gateThroughFactory(
  root: string,
  reporter: FactoryEventReporter,
  scenario: string,
  expectedHeadSha: string,
  mergeAttempts: string[],
): Promise<VerificationVerdict> {
  const issuePath = `/linear/issues/AR-145__verification-${scenario}.json`
  const issueRecord = verificationIssue(scenario)
  const mount = new FakeMountClient({
    [issuePath]: issueRecord,
    '/github/repos/AgentWorkforce__factory/pulls/by-id/145.json': verificationPullRequest(scenario),
  })
  const pipeline = new VerificationPipeline({
    reporter,
    descriptorPath: `.factory/${scenario}.yaml`,
    runId: () => `${scenario}-${process.pid}`,
    maxConcurrentEnvironments: 2,
  })
  const verificationGate = new RecordingVerificationGate(pipeline)
  const mergeGate = new RecordingMergeGate(expectedHeadSha, scenario, mergeAttempts)
  const factory = createFactory(verificationFactoryConfig(root, scenario), {
    mount,
    fleet: new FakeFleetClient(),
    triage: new VerificationTriage(root),
    linear: stateOnlyLinear(mount),
    mergeGate,
    verificationGate,
    logger: { info() {}, warn() {}, error() {} },
  })

  const issue = parseLinearIssue(issuePath, issueRecord)
  let status: ReturnType<typeof factory.status>
  try {
    await factory.dispatch(await factory.triageIssue(issue))
    await factory.runLoop({ maxIterations: 1 })
    status = factory.status()
  } finally {
    await factory.dispose()
  }

  assert.ok(
    verificationGate.verdict,
    `Factory did not run verification for ${scenario}: ${JSON.stringify(status!)}`,
  )
  if (verificationGate.verdict.passed) {
    assert.equal(mergeGate.inputs.length, 1, `green ${scenario} verdict did not reach the merge port`)
    assert.equal(status!.inFlight.length, 0, `green ${scenario} run did not complete the Factory workflow`)
  } else {
    assert.equal(mergeGate.inputs.length, 0, `red ${scenario} verdict reached the merge port`)
    assert.equal(status!.inFlight.length, 1, `red ${scenario} run incorrectly completed the Factory workflow`)
  }
  return verificationGate.verdict
}

class RecordingVerificationGate implements VerificationGate {
  verdict?: VerificationVerdict

  constructor(readonly delegate: VerificationGate) {}

  async verify(input: VerificationGateInput): Promise<VerificationVerdict> {
    this.verdict = await this.delegate.verify(input)
    return this.verdict
  }
}

class RecordingMergeGate implements GithubMergeGate {
  readonly inputs: GithubMergeInput[] = []

  constructor(
    readonly expectedHeadSha: string,
    readonly scenario: string,
    readonly attempts: string[],
  ) {}

  async check() {
    return {
      verdict: 'READY' as const,
      ready: true,
      reason: 'fixture checks and review are green',
      live: {
        mergeable: 'MERGEABLE',
        mergeStateStatus: 'CLEAN',
        headRefOid: this.expectedHeadSha,
        reviewDecision: 'APPROVED',
        checkStates: ['SUCCESS'],
      },
    }
  }

  async merge(input: GithubMergeInput) {
    this.inputs.push(input)
    this.attempts.push(this.scenario)
    return { merged: true, reason: 'fixture merge recorded' }
  }
}

class VerificationTriage implements TriageEngine {
  constructor(readonly root: string) {}

  async triage(issue: LinearIssue): Promise<TriageDecision> {
    const common = {
      repo: 'AgentWorkforce/factory',
      clonePath: this.root,
      node: 'self' as const,
    }
    return {
      issue: { uuid: issue.uuid, key: issue.key, path: issue.path },
      routes: [{ repo: common.repo, clonePath: common.clonePath, rationale: 'verification fixture' }],
      scope: 'single',
      implementers: [{
        ...common,
        name: 'ar-145-impl-verification',
        role: 'implementer',
        capability: 'spawn:codex',
        model: 'codex',
        task: 'Fixture implementation is complete.',
      }],
      reviewer: {
        ...common,
        name: 'ar-145-review-verification',
        role: 'reviewer',
        capability: 'spawn:claude',
        model: 'claude',
        task: 'Fixture review is complete.',
      },
      thin: false,
      confidence: 'high',
      rationale: 'verification fixture',
    }
  }
}

function verificationFactoryConfig(root: string, scenario: string): FactoryConfig {
  return FactoryConfigSchema.parse({
    workspaceId: `verification-gate-${scenario}`,
    repos: {
      byLabel: { factory: 'AgentWorkforce/factory' },
      clonePaths: { 'AgentWorkforce/factory': root },
      default: 'AgentWorkforce/factory',
    },
    triage: { maxImplementers: 1 },
    batchSize: 1,
    mergePolicy: 'on-green-with-review',
    verification: { enabled: false },
    safety: { requireTitlePrefix: 'Real', requireLabel: 'factory', requireTeamKey: 'AR' },
    stateIds: {
      readyForAgent: 'state-ready',
      agentImplementing: 'state-implementing',
      done: 'state-done',
      inPlanning: 'state-planning',
    },
    loop: {
      maxIterations: 1,
      heartbeatPath: join(tmpdir(), `factory-verification-${scenario}-${process.pid}-heartbeat.json`),
      registryPath: join(tmpdir(), `factory-verification-${scenario}-${process.pid}-registry.json`),
    },
  })
}

function verificationIssue(scenario: string) {
  return {
    provider: 'linear',
    objectType: 'issue',
    objectId: `verification-${scenario}`,
    payload: {
      id: `verification-${scenario}`,
      identifier: 'AR-145',
      title: `Real verification gate ${scenario}`,
      description: 'Exercise the live verification merge gate.',
      stateId: 'state-ready',
      url: `https://linear.app/agent-relay/issue/AR-145/verification-gate-${scenario}`,
      labels: [{ name: 'factory' }],
      team: { key: 'AR', name: 'Agent Relay' },
      state: { id: 'state-ready', name: 'Ready for Agent' },
    },
  }
}

function verificationPullRequest(scenario: string) {
  return {
    provider: 'github',
    objectType: 'pull_request',
    objectId: '145',
    payload: {
      number: 145,
      title: `AR-145: verification gate ${scenario}`,
      body: 'Linear: AR-145',
      head_ref: `verification-${scenario}`,
      state: 'OPEN',
      isDraft: false,
    },
  }
}

const stateOnlyLinear = (mount: FakeMountClient): LinearWriteback => ({
  async setState(issue, stateId) { await mount.writeFile(issue.path, { stateId }) },
  async postComment() {},
  async createIssue() { throw new Error('not used by verification fixture') },
  async verify() { return true },
})

function cloudReporter(root: string, batches: FactoryCloudEventBatchV1[]): FactoryCloudReporter {
  return new FactoryCloudReporter({
    endpoint: 'https://factory.invalid/api/v1/factory/events',
    instance: {
      id: 'verification-gate-e2e',
      bootId: `kind-${process.pid}`,
      version: 'e2e',
    },
    outbox: new FileFactoryCloudEventOutbox({ path: join(root, 'cloud-outbox.json') }),
    getAccessToken: () => 'e2e-access-token',
    autoFlush: false,
    maxAttempts: 1,
    fetch: async (input, init) => {
      const url = input instanceof Request ? input.url : String(input)
      assert.equal(url, 'https://factory.invalid/api/v1/factory/events')
      assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer e2e-access-token')
      assert.equal(typeof init?.body, 'string')
      const batch = JSON.parse(init!.body as string) as FactoryCloudEventBatchV1
      batches.push(batch)
      return new Response(JSON.stringify({
        accepted: batch.events.length,
        duplicates: 0,
      }), {
        status: 201,
        headers: { 'content-type': 'application/json' },
      })
    },
  })
}

async function killMidFlightAndReap(root: string): Promise<string> {
  const signalPath = join(root, 'leaked-namespace.txt')
  const tsx = join(process.cwd(), 'node_modules', '.bin', 'tsx')
  const child = spawn(tsx, [import.meta.filename, '--leak-child', root, signalPath], {
    stdio: ['ignore', 'ignore', 'inherit'],
  })
  const namespace = await waitForFile(signalPath, 30_000)
  child.kill('SIGKILL')
  await once(child, 'exit')

  const report = await reapFactoryEnvironmentsOnce({ nowMs: Date.now() + 60_000 })
  assert.ok(report.reaped.includes(namespace), `reaper did not claim killed environment ${namespace}`)
  return namespace
}

async function waitForFile(path: string, timeoutMs: number): Promise<string> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const value = (await readFile(path, 'utf8')).trim()
      if (value) return value
    } catch {
      // Provision has not completed yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for child environment signal ${path}`)
}

async function assertNamespaceGone(namespace: string): Promise<void> {
  const kubectl = defaultKubectlEnvironmentRunner()
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    try {
      await kubectl(['get', 'namespace', namespace])
    } catch {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  throw new Error(`verification namespace leaked: ${namespace}`)
}

async function fixtureRepository(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'factory-verification-gate-e2e-'))
  await mkdir(join(root, '.factory'), { recursive: true })
  await writeFile(join(root, 'stack.yaml'), stackManifest, 'utf8')
  await writeFile(join(root, 'e2e.mjs'), e2eProgram, 'utf8')
  await writeFile(join(root, 'load-pass.yaml'), loadProfile(false), 'utf8')
  await writeFile(join(root, 'load-fail.yaml'), loadProfile(true), 'utf8')
  await writeFile(join(root, '.gitignore'), 'cloud-outbox.json\nleaked-namespace.txt\n', 'utf8')
  await Promise.all([
    writeDescriptor(root, 'green', 'pass', 'load-pass.yaml', '30s', '2m'),
    writeDescriptor(root, 'red-e2e', 'fail', 'load-pass.yaml', '30s', '2m'),
    writeDescriptor(root, 'red-load', 'pass', 'load-fail.yaml', '30s', '2m'),
    writeDescriptor(root, 'timeout', 'hang', 'load-pass.yaml', '1s', '20s'),
    writeDescriptor(root, 'leak', 'hang', 'load-pass.yaml', '5m', '5m', '1s'),
  ])
  await execFileAsync('git', ['init', '--quiet'], { cwd: root })
  await execFileAsync('git', ['add', '--all'], { cwd: root })
  await execFileAsync('git', [
    '-c', 'user.name=Factory E2E',
    '-c', 'user.email=factory-e2e@invalid.example',
    'commit', '--quiet', '--message', 'verification gate fixture',
  ], { cwd: root })
  return root
}

async function gitHead(root: string): Promise<string> {
  const { stdout } = await execFileAsync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' })
  return stdout.trim()
}

async function writeDescriptor(
  root: string,
  name: string,
  e2eMode: string,
  profile: string,
  e2eTimeout: string,
  overallTimeout: string,
  ttl = '5m',
): Promise<void> {
  await writeFile(join(root, '.factory', `${name}.yaml`), `
apiVersion: factory.agentworkforce.dev/v1alpha1
kind: VerificationStack
name: factory-gate-${name}
source:
  type: manifests
  paths: [stack.yaml]
services:
  - name: sample-api
    workload: { kind: deployment }
    readiness:
      type: http
      port: 5678
      path: /
      timeoutSeconds: 120
      intervalSeconds: 1
endpoints:
  - name: api
    service: sample-api
    port: 5678
    path: /
verification:
  environmentTtlSeconds: ${durationSeconds(ttl)}
  e2e:
    command: node
    args: [e2e.mjs, ${e2eMode}]
    timeoutSeconds: ${durationSeconds(e2eTimeout)}
  load:
    profile: ${profile}
    timeoutSeconds: 120
  overallTimeoutSeconds: ${durationSeconds(overallTimeout)}
  teardownTimeoutSeconds: 120
`, 'utf8')
}

function durationSeconds(value: string): number {
  const match = /^(\d+)(s|m)$/u.exec(value)
  if (!match) throw new Error(`unsupported E2E duration ${value}`)
  return Number(match[1]) * (match[2] === 'm' ? 60 : 1)
}

const stackManifest = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sample-api
spec:
  replicas: 1
  selector:
    matchLabels: { app: factory-gate-sample }
  template:
    metadata:
      labels: { app: factory-gate-sample }
    spec:
      containers:
        - name: http-echo
          image: hashicorp/http-echo:1.0.0@sha256:fcb75f691c8b0414d670ae570240cbf95502cc18a9ba57e982ecac589760a186
          args: ['-listen=:5678', '-text={"status":"healthy"}']
          ports: [{ name: http, containerPort: 5678 }]
          readinessProbe:
            httpGet: { path: /, port: http }
            periodSeconds: 1
          resources:
            requests: { cpu: 10m, memory: 8Mi }
            limits: { cpu: 200m, memory: 64Mi }
---
apiVersion: v1
kind: Service
metadata:
  name: sample-api
spec:
  selector: { app: factory-gate-sample }
  ports: [{ name: http, port: 5678, targetPort: http }]
`

const e2eProgram = `
const mode = process.argv[2]
if (mode === 'hang') await new Promise(() => setInterval(() => undefined, 1_000))
const response = await fetch(process.env.FACTORY_ENDPOINT_API)
if (!response.ok) throw new Error('fixture endpoint returned ' + response.status)
const payload = await response.json()
if (payload.status !== 'healthy') throw new Error('fixture endpoint was not healthy')
if (mode === 'fail') throw new Error('intentional E2E fixture failure')
`

const loadProfile = (strict: boolean): string => `
name: ${strict ? 'strict-slo-failure' : 'healthy-stack'}
targets:
  - name: health
    endpoint: api
    path: /
vus: 3
duration: 2s
thresholds:
  maxP95LatencyMs: ${strict ? 0.001 : 2000}
  maxErrorRate: 0.01
  minThroughputRps: 0.1
`

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`)
  process.exitCode = 1
})
