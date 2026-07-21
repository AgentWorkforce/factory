import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'

import {
  defaultKubectlCommandRunner,
  runLoad,
  type LoadEnvironment,
  type LoadProfile,
} from '../../src/index.ts'

const kubectl = defaultKubectlCommandRunner()
const namespace = `factory-load-e2e-${process.pid}`
const evidenceDirectory = 'artifacts/load-e2e'

const apply = async (resources: Array<Record<string, unknown>>): Promise<void> => {
  await kubectl(
    ['--namespace', namespace, 'apply', '--filename', '-'],
    JSON.stringify({ apiVersion: 'v1', kind: 'List', items: resources }),
  )
}

const provisionEnvironment = async (): Promise<LoadEnvironment> => {
  await kubectl(['create', 'namespace', namespace])
  return {
    id: namespace,
    namespace,
    endpoints: {
      api: `http://sample-api.${namespace}.svc.cluster.local:5678`,
    },
  }
}

const deploySampleService = async (): Promise<void> => {
  const labels = { app: 'factory-load-sample' }
  await apply([
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: { name: 'sample-api', namespace },
      spec: {
        replicas: 2,
        selector: { matchLabels: labels },
        template: {
          metadata: { labels },
          spec: {
            containers: [{
              name: 'http-echo',
              image: 'hashicorp/http-echo:1.0.0@sha256:fcb75f691c8b0414d670ae570240cbf95502cc18a9ba57e982ecac589760a186',
              args: ['-listen=:5678', '-text={"status":"healthy"}'],
              ports: [{ name: 'http', containerPort: 5678 }],
              readinessProbe: {
                httpGet: { path: '/', port: 'http' },
                initialDelaySeconds: 1,
                periodSeconds: 1,
              },
              resources: {
                requests: { cpu: '20m', memory: '16Mi' },
                limits: { cpu: '250m', memory: '64Mi' },
              },
            }],
          },
        },
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: { name: 'sample-api', namespace },
      spec: {
        selector: labels,
        ports: [{ name: 'http', port: 5678, targetPort: 'http' }],
      },
    },
  ])
  await kubectl([
    '--namespace', namespace,
    'wait', 'deployment/sample-api',
    '--for=condition=Available',
    '--timeout=120s',
  ])
}

const passingProfile = (): LoadProfile => ({
  name: 'healthy-service-pass',
  targets: [{ name: 'health', endpoint: 'api', path: '/' }],
  vus: Number(process.env.FACTORY_LOAD_E2E_VUS ?? 50),
  duration: process.env.FACTORY_LOAD_E2E_DURATION ?? '30s',
  ramp: { up: '2s', down: '1s' },
  thresholds: {
    maxP95LatencyMs: 2_000,
    maxP99LatencyMs: 5_000,
    maxErrorRate: 0.01,
    minThroughputRps: 1,
  },
})

const strictProfile = (): LoadProfile => ({
  name: 'healthy-service-strict-fail',
  targets: [{ name: 'health', endpoint: 'api', path: '/' }],
  vus: 5,
  duration: '3s',
  thresholds: {
    maxP95LatencyMs: 0.001,
    maxErrorRate: 0.01,
    minThroughputRps: 1,
  },
})

const main = async (): Promise<void> => {
  await mkdir(evidenceDirectory, { recursive: true })
  let environment: LoadEnvironment | undefined
  try {
    environment = await provisionEnvironment()
    await deploySampleService()

    const passing = await runLoad(environment, passingProfile(), {
      cleanup: false,
      evidencePath: `${evidenceDirectory}/pass.json`,
    })
    assert.equal(passing.status, 'pass', passing.evidenceJson)
    assert.equal(passing.job.completed, true)
    assert.ok(passing.measured.requestCount > 0, 'pass run must emit a real request count')
    assert.ok(passing.measured.throughputRps > 0, 'pass run must emit real throughput')
    assert.ok(passing.measured.latency.p95Ms > 0, 'pass run must emit a real p95')
    assert.ok(passing.measured.histogram.some((bucket) => bucket.count > 0), 'latency histogram must not be empty')
    assert.deepEqual(JSON.parse(passing.evidenceJson), passing.evidence)

    const failing = await runLoad(environment, strictProfile(), {
      cleanup: false,
      evidencePath: `${evidenceDirectory}/fail.json`,
    })
    assert.equal(failing.status, 'fail', 'strict SLO must reject a completed load run')
    assert.equal(failing.job.completed, true)
    assert.ok(failing.violations.some((violation) => violation.metric === 'p95LatencyMs'))
    assert.ok(failing.measured.requestCount > 0, 'failed gate must retain measured evidence')

    process.stdout.write(`${JSON.stringify({
      passing: passing.evidence,
      failing: failing.evidence,
    }, null, 2)}\n`)
  } finally {
    if (environment) {
      await kubectl(['delete', 'namespace', namespace, '--wait=true', '--ignore-not-found=true'])
    }
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`)
  process.exitCode = 1
})
