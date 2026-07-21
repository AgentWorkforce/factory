import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import {
  evaluateLoadSlo,
  parseK6LoadMeasurements,
  runLoad,
  type LoadMeasurements,
} from './load-harness'
import {
  createK6LoadJobResources,
  K6_EVIDENCE_PREFIX,
  k6ScenarioFor,
  type KubernetesLoadJobClient,
} from './k6-job'
import { loadLoadProfile, LoadProfileSchema, type LoadProfile } from './load-profile'

const measurements: LoadMeasurements = {
  requestCount: 1_000,
  errorCount: 2,
  errorRate: 0.002,
  throughputRps: 99.5,
  durationMs: 10_050,
  latency: {
    minMs: 2,
    averageMs: 20,
    medianMs: 15,
    maxMs: 200,
    p95Ms: 45,
    p99Ms: 90,
  },
  histogram: [
    { upperBoundMs: 50, count: 960 },
    { upperBoundMs: null, count: 40 },
  ],
}

const profile: LoadProfile = {
  name: 'healthy-api',
  targets: [{ name: 'health', endpoint: 'api', path: '/health' }],
  vus: 10,
  duration: '10s',
  thresholds: {
    maxP95LatencyMs: 100,
    maxP99LatencyMs: 150,
    maxErrorRate: 0.01,
    minThroughputRps: 50,
  },
}

class FakeLoadJobClient implements KubernetesLoadJobClient {
  applied: Array<Record<string, unknown>> = []
  deleted: string[] = []
  waited: string[] = []

  constructor(private readonly output: LoadMeasurements = measurements) {}

  async apply(resources: Array<Record<string, unknown>>): Promise<void> {
    this.applied = resources
  }

  async waitForCompletion(jobName: string): Promise<void> {
    this.waited.push(jobName)
  }

  async logs(): Promise<string> {
    return `k6 output\n${K6_EVIDENCE_PREFIX}${JSON.stringify(this.output)}\n`
  }

  async delete(jobName: string, configMapName: string): Promise<void> {
    this.deleted.push(jobName, configMapName)
  }
}

describe('load SLO gate', () => {
  it('passes when every measured value is within its declarative threshold', () => {
    expect(evaluateLoadSlo(measurements, profile.thresholds)).toEqual({
      passed: true,
      violations: [],
    })
  })

  it('fails with every violated metric and measured value', () => {
    const result = evaluateLoadSlo(measurements, {
      maxP95LatencyMs: 1,
      maxP99LatencyMs: 2,
      maxErrorRate: 0.001,
      minThroughputRps: 200,
    })

    expect(result.passed).toBe(false)
    expect(result.violations).toEqual([
      { metric: 'p95LatencyMs', actual: 45, threshold: 1, operator: 'at-most' },
      { metric: 'p99LatencyMs', actual: 90, threshold: 2, operator: 'at-most' },
      { metric: 'errorRate', actual: 0.002, threshold: 0.001, operator: 'at-most' },
      { metric: 'throughputRps', actual: 99.5, threshold: 200, operator: 'at-least' },
    ])
  })

  it('fails closed when a completed generator emitted no requests', () => {
    const empty: LoadMeasurements = {
      requestCount: 0,
      errorCount: 0,
      errorRate: 1,
      throughputRps: 0,
      durationMs: 1_000,
      latency: {
        minMs: 0,
        averageMs: 0,
        medianMs: 0,
        maxMs: 0,
        p95Ms: 0,
        p99Ms: 0,
      },
      histogram: [
        { upperBoundMs: 50, count: 0 },
        { upperBoundMs: null, count: 0 },
      ],
    }

    expect(evaluateLoadSlo(empty, { maxP95LatencyMs: 2_000 })).toEqual({
      passed: false,
      violations: [
        { metric: 'requestCount', actual: 0, threshold: 1, operator: 'at-least' },
      ],
    })
  })

  it('extracts valid summaries and rejects missing or internally inconsistent evidence', () => {
    expect(parseK6LoadMeasurements(
      `noise\n${K6_EVIDENCE_PREFIX}${JSON.stringify(measurements)}\n`,
    )).toEqual(measurements)
    expect(() => parseK6LoadMeasurements('ordinary k6 output')).toThrow(/without Factory evidence JSON/u)
    expect(() => parseK6LoadMeasurements(
      `${K6_EVIDENCE_PREFIX}${JSON.stringify({ ...measurements, errorCount: 1_001 })}\n`,
    )).toThrow(/errorCount: must not exceed requestCount/u)
  })
})

describe('k6 in-cluster Job', () => {
  it('renders weighted request shapes, a ramp, and hardened Job resources', () => {
    const resolvedProfile = LoadProfileSchema.parse({
      ...profile,
      rps: 50,
      maxVus: 100,
      ramp: { up: '2s', down: '1s' },
      targets: [{
        name: 'create',
        endpoint: 'api',
        path: '/items',
        method: 'POST',
        body: { value: 42 },
        weight: 3,
      }],
    })
    const resources = createK6LoadJobResources(
      { id: 'env-1', endpoints: { api: 'http://api.verify.svc.cluster.local:8080' } },
      resolvedProfile,
      { jobName: 'factory-load-test', namespace: 'verify' },
    )

    expect(k6ScenarioFor(resolvedProfile)).toMatchObject({
      executor: 'ramping-arrival-rate',
      stages: [
        { duration: '2s', target: 50 },
        { duration: '10s', target: 50 },
        { duration: '1s', target: 0 },
      ],
    })
    expect(resources.resources.map((resource) => resource.kind)).toEqual(['ConfigMap', 'Job'])
    expect(JSON.stringify(resources.resources)).toContain('http://api.verify.svc.cluster.local:8080/items')
    expect(JSON.stringify(resources.resources)).toContain("'factory_latency_bucket_' + index")
    expect(JSON.stringify(resources.resources)).toContain('automountServiceAccountToken')
  })

  it('runs the Job, returns inspectable evidence, and cleans up', async () => {
    const client = new FakeLoadJobClient()
    const moments = [new Date('2026-07-21T10:00:00.000Z'), new Date('2026-07-21T10:00:10.050Z')]
    const result = await runLoad(
      {
        id: 'env-1',
        namespace: 'verify',
        endpoints: { api: 'http://api.verify.svc.cluster.local:8080' },
      },
      profile,
      {
        client,
        runId: 'unit-test',
        now: () => moments.shift() as Date,
      },
    )

    expect(result).toMatchObject({
      status: 'pass',
      passed: true,
      measured: measurements,
      job: { namespace: 'verify', completed: true },
      evidence: {
        contract: 'factory.load.evidence.v1',
        environmentId: 'env-1',
        gate: { status: 'pass', violations: [] },
      },
    })
    expect(JSON.parse(result.evidenceJson)).toEqual(result.evidence)
    expect(client.applied).toHaveLength(2)
    expect(client.waited).toHaveLength(1)
    expect(client.deleted).toHaveLength(2)
  })

  it('returns a failed gate without treating the completed Job as an execution error', async () => {
    const client = new FakeLoadJobClient()
    const result = await runLoad(
      { id: 'env-1', endpoints: { api: 'http://api:8080' } },
      {
        ...profile,
        thresholds: { maxP95LatencyMs: 1 },
      },
      { client, runId: 'strict-slo' },
    )

    expect(result.status).toBe('fail')
    expect(result.violations).toContainEqual({
      metric: 'p95LatencyMs',
      actual: 45,
      threshold: 1,
      operator: 'at-most',
    })
  })
})

describe('load profile sidecar', () => {
  const temporaryDirectories: string[] = []

  afterEach(async () => {
    await Promise.all(temporaryDirectories.splice(0).map(async (path) => await rm(path, { recursive: true })))
  })

  it('loads YAML and applies safe profile defaults', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'factory-load-profile-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'load.yaml')
    await writeFile(path, `
name: smoke
vus: 2
duration: 3s
targets:
  - name: health
    endpoint: api
    path: /health
thresholds:
  maxP95LatencyMs: 1000
  maxErrorRate: 0.01
  minThroughputRps: 1
`, 'utf8')

    const loaded = await loadLoadProfile(path)
    expect(loaded.targets[0]).toMatchObject({ method: 'GET', weight: 1 })
    expect(loaded.histogramBucketsMs).toContain(1_000)
  })

  it('rejects an arrival-rate profile whose maxVus is below its default preallocation', () => {
    expect(() => LoadProfileSchema.parse({
      ...profile,
      vus: undefined,
      rps: 100,
      maxVus: 50,
    })).toThrow(/preallocated vus/u)
  })
})
