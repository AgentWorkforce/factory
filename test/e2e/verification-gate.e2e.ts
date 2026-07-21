import assert from 'node:assert/strict'
import { execFile, spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'

import {
  FactoryCloudReporter,
  FileFactoryCloudEventOutbox,
  KubectlEnvironmentProvider,
  VerificationPipeline,
  defaultKubectlEnvironmentRunner,
  reapFactoryEnvironmentsOnce,
  type DeployEnvironmentInput,
  type FactoryCloudEventBatchV1,
  type FactoryEventReporter,
  type ProvisionEnvironmentInput,
  type VerificationEnvironment,
  type VerificationEnvironmentProvider,
  type VerificationVerdict,
} from '../../src/index.ts'

const artifactDirectory = join(process.cwd(), 'artifacts', 'verification-gate-e2e')
const execFileAsync = promisify(execFile)

class SignalingEnvironmentProvider implements VerificationEnvironmentProvider {
  readonly #delegate = new KubectlEnvironmentProvider()

  constructor(readonly signalPath: string) {}

  async provision(input: ProvisionEnvironmentInput): Promise<VerificationEnvironment> {
    const environment = await this.#delegate.provision(input)
    await writeFile(this.signalPath, environment.namespace, 'utf8')
    return environment
  }

  async deploy(environment: VerificationEnvironment, input: DeployEnvironmentInput): Promise<VerificationEnvironment> {
    return await this.#delegate.deploy(environment, input)
  }

  async teardown(environment: VerificationEnvironment, options?: { signal?: AbortSignal }): Promise<void> {
    await this.#delegate.teardown(environment, options)
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
    verdicts.green = await gate(root, reporter, 'green', expectedHeadSha)
    assert.equal(verdicts.green.passed, true, verdicts.green.reason)
    assert.equal(await attemptMerge(verdicts.green, 'green', mergeAttempts), true, 'green verification must allow merge')
    await assertNamespaceGone(verdicts.green.evidence.namespace!)

    verdicts.redE2e = await gate(root, reporter, 'red-e2e', expectedHeadSha)
    assert.equal(verdicts.redE2e.passed, false)
    assert.equal(verdicts.redE2e.evidence.stages.e2e.status, 'fail')
    assert.equal(verdicts.redE2e.evidence.stages.load.status, 'skipped')
    assert.equal(await attemptMerge(verdicts.redE2e, 'red-e2e', mergeAttempts), false, 'red E2E must block merge')
    await assertNamespaceGone(verdicts.redE2e.evidence.namespace!)

    verdicts.redLoad = await gate(root, reporter, 'red-load', expectedHeadSha)
    assert.equal(verdicts.redLoad.passed, false)
    assert.equal(verdicts.redLoad.evidence.stages.e2e.status, 'pass')
    assert.equal(verdicts.redLoad.evidence.stages.load.status, 'fail')
    assert.ok((verdicts.redLoad.evidence.stages.load.violations ?? []).some(
      (violation) => violation.metric === 'p95LatencyMs',
    ))
    assert.equal(await attemptMerge(verdicts.redLoad, 'red-load', mergeAttempts), false, 'load SLO violation must block merge')
    await assertNamespaceGone(verdicts.redLoad.evidence.namespace!)

    verdicts.timeout = await gate(root, reporter, 'timeout', expectedHeadSha)
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

async function gate(
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

async function attemptMerge(
  verdict: VerificationVerdict,
  scenario: string,
  attempts: string[],
): Promise<boolean> {
  if (!verdict.passed) return false
  attempts.push(scenario)
  return true
}

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
provision:
  namespacePrefix: factory-gate
  ttl: ${ttl}
deploy:
  manifests: [stack.yaml]
  readiness:
    - resource: deployment/sample-api
      condition: Available
      timeout: 2m
  endpoints:
    api:
      service: sample-api
      port: 5678
      portForward: true
e2e:
  command: node
  args: [e2e.mjs, ${e2eMode}]
  timeout: ${e2eTimeout}
load:
  profile: ${profile}
  timeout: 2m
timeouts:
  overall: ${overallTimeout}
  teardown: 2m
`, 'utf8')
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
