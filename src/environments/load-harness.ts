import { randomUUID } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

import { z } from 'zod'

import {
  createK6LoadJobResources,
  K6_EVIDENCE_PREFIX,
  KubectlLoadJobClient,
  type KubernetesLoadJobClient,
  type LoadEnvironment,
} from './k6-job.js'
import {
  LoadProfileSchema,
  LoadThresholdsSchema,
  type LoadProfile,
  type ResolvedLoadProfile,
  type LoadThresholds,
} from './load-profile.js'

export const LOAD_EVIDENCE_CONTRACT = 'factory.load.evidence.v1' as const

const LatencyHistogramBucketSchema = z.object({
  upperBoundMs: z.number().finite().positive().nullable(),
  count: z.number().int().nonnegative(),
}).strict()

export const LoadMeasurementsSchema = z.object({
  requestCount: z.number().int().nonnegative(),
  errorCount: z.number().int().nonnegative(),
  errorRate: z.number().finite().min(0).max(1),
  throughputRps: z.number().finite().nonnegative(),
  durationMs: z.number().finite().nonnegative(),
  latency: z.object({
    minMs: z.number().finite().nonnegative(),
    averageMs: z.number().finite().nonnegative(),
    medianMs: z.number().finite().nonnegative(),
    maxMs: z.number().finite().nonnegative(),
    p95Ms: z.number().finite().nonnegative(),
    p99Ms: z.number().finite().nonnegative(),
  }).strict(),
  histogram: z.array(LatencyHistogramBucketSchema).min(2),
}).strict().superRefine((measured, context) => {
  if (measured.errorCount > measured.requestCount) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['errorCount'],
      message: 'must not exceed requestCount',
    })
  }

  const expectedErrorRate = measured.requestCount === 0
    ? 1
    : measured.errorCount / measured.requestCount
  if (Math.abs(measured.errorRate - expectedErrorRate) > 1e-12) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['errorRate'],
      message: 'must equal errorCount / requestCount (or 1 when no requests ran)',
    })
  }

  const latencyOrder = [
    measured.latency.minMs,
    measured.latency.medianMs,
    measured.latency.p95Ms,
    measured.latency.p99Ms,
    measured.latency.maxMs,
  ]
  if (latencyOrder.some((value, index) => index > 0 && value < latencyOrder[index - 1])) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['latency'],
      message: 'must satisfy min <= median <= p95 <= p99 <= max',
    })
  }
  if (measured.latency.averageMs < measured.latency.minMs ||
      measured.latency.averageMs > measured.latency.maxMs) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['latency', 'averageMs'],
      message: 'must be between minMs and maxMs',
    })
  }

  const finalBucketIndex = measured.histogram.length - 1
  measured.histogram.forEach((bucket, index) => {
    const previous = measured.histogram[index - 1]
    if (index === finalBucketIndex && bucket.upperBoundMs !== null) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['histogram', index, 'upperBoundMs'],
        message: 'the final overflow bucket must have a null upper bound',
      })
    } else if (index < finalBucketIndex && bucket.upperBoundMs === null) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['histogram', index, 'upperBoundMs'],
        message: 'only the final overflow bucket may have a null upper bound',
      })
    }
    if (bucket.count > measured.requestCount) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['histogram', index, 'count'],
        message: 'must not exceed requestCount',
      })
    }
    if (previous !== undefined && previous.upperBoundMs !== null && bucket.upperBoundMs !== null) {
      if (bucket.upperBoundMs <= previous.upperBoundMs) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['histogram', index, 'upperBoundMs'],
          message: 'finite upper bounds must be strictly increasing',
        })
      }
      if (bucket.count < previous.count) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['histogram', index, 'count'],
          message: 'cumulative bucket counts must be nondecreasing',
        })
      }
    }
  })

  const lastFiniteCount = measured.histogram[finalBucketIndex - 1]?.count ?? 0
  const overflowCount = measured.histogram[finalBucketIndex]?.count ?? 0
  if (lastFiniteCount + overflowCount !== measured.requestCount) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['histogram'],
      message: 'last finite bucket plus overflow bucket must equal requestCount',
    })
  }
})

export type LoadMeasurements = z.infer<typeof LoadMeasurementsSchema>
export type LatencyHistogramBucket = z.infer<typeof LatencyHistogramBucketSchema>

export type LoadSloMetric =
  | 'requestCount'
  | 'p95LatencyMs'
  | 'p99LatencyMs'
  | 'errorRate'
  | 'throughputRps'

export interface LoadSloViolation {
  metric: LoadSloMetric
  actual: number
  threshold: number
  operator: 'at-most' | 'at-least'
}

export interface LoadSloEvaluation {
  passed: boolean
  violations: LoadSloViolation[]
}

export interface LoadEvidence {
  contract: typeof LOAD_EVIDENCE_CONTRACT
  environmentId: string
  profile: {
    name: string
    vus?: number
    maxVus?: number
    rps?: number
    duration: string
    ramp?: { up: string; down?: string }
    targets: Array<{
      name: string
      endpoint?: string
      method: string
      path?: string
    }>
  }
  job: {
    name: string
    namespace: string
    completed: true
  }
  startedAt: string
  completedAt: string
  thresholds: LoadThresholds
  measured: LoadMeasurements
  gate: {
    status: 'pass' | 'fail'
    violations: LoadSloViolation[]
  }
}

export interface LoadResult {
  status: 'pass' | 'fail'
  passed: boolean
  measured: LoadMeasurements
  violations: LoadSloViolation[]
  evidence: LoadEvidence
  evidenceJson: string
  job: LoadEvidence['job']
}

export interface RunLoadOptions {
  client?: KubernetesLoadJobClient
  namespace?: string
  kubeContext?: string
  k6Image?: string
  timeoutMs?: number
  cleanup?: boolean
  evidencePath?: string
  runId?: string
  now?: () => Date
}

export function evaluateLoadSlo(
  measuredInput: LoadMeasurements,
  thresholdsInput: LoadThresholds,
): LoadSloEvaluation {
  const measured = LoadMeasurementsSchema.parse(measuredInput)
  const thresholds = LoadThresholdsSchema.parse(thresholdsInput)
  const violations: LoadSloViolation[] = []

  const atMost = (metric: LoadSloMetric, actual: number, threshold: number | undefined): void => {
    if (threshold !== undefined && actual > threshold) {
      violations.push({ metric, actual, threshold, operator: 'at-most' })
    }
  }
  const atLeast = (metric: LoadSloMetric, actual: number, threshold: number | undefined): void => {
    if (threshold !== undefined && actual < threshold) {
      violations.push({ metric, actual, threshold, operator: 'at-least' })
    }
  }

  // A completed generator with no traffic is never evidence that an SLO passed,
  // even when the caller did not declare a throughput floor.
  atLeast('requestCount', measured.requestCount, 1)
  atMost('p95LatencyMs', measured.latency.p95Ms, thresholds.maxP95LatencyMs)
  atMost('p99LatencyMs', measured.latency.p99Ms, thresholds.maxP99LatencyMs)
  atMost('errorRate', measured.errorRate, thresholds.maxErrorRate)
  atLeast('throughputRps', measured.throughputRps, thresholds.minThroughputRps)
  return { passed: violations.length === 0, violations }
}

export function parseK6LoadMeasurements(logs: string): LoadMeasurements {
  const markerIndex = logs.lastIndexOf(K6_EVIDENCE_PREFIX)
  if (markerIndex < 0) {
    throw new Error('k6 Job completed without Factory evidence JSON')
  }
  const jsonLine = logs
    .slice(markerIndex + K6_EVIDENCE_PREFIX.length)
    .split(/\r?\n/u, 1)[0]
    .trim()
  let value: unknown
  try {
    value = JSON.parse(jsonLine)
  } catch (error) {
    throw new Error(`k6 Job emitted invalid Factory evidence JSON: ${error instanceof Error ? error.message : String(error)}`)
  }
  const parsed = LoadMeasurementsSchema.safeParse(value)
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '<root>'}: ${issue.message}`)
      .join('; ')
    throw new Error(`k6 Job evidence did not match the expected schema: ${issues}`)
  }
  return parsed.data
}

export function serializeLoadEvidence(evidence: LoadEvidence): string {
  return `${JSON.stringify(evidence, null, 2)}\n`
}

const evidenceProfile = (profile: ResolvedLoadProfile): LoadEvidence['profile'] => ({
  name: profile.name,
  ...(profile.vus === undefined ? {} : { vus: profile.vus }),
  ...(profile.maxVus === undefined ? {} : { maxVus: profile.maxVus }),
  ...(profile.rps === undefined ? {} : { rps: profile.rps }),
  duration: profile.duration,
  ...(profile.ramp === undefined ? {} : { ramp: profile.ramp }),
  targets: profile.targets.map((target) => ({
    name: target.name,
    ...(target.endpoint === undefined ? {} : { endpoint: target.endpoint }),
    method: target.method,
    ...(target.path === undefined ? {} : { path: target.path }),
  })),
})

export async function runLoad(
  environment: LoadEnvironment,
  profileInput: LoadProfile,
  options: RunLoadOptions = {},
): Promise<LoadResult> {
  const profile = LoadProfileSchema.parse(profileInput)
  const namespace = options.namespace ?? environment.namespace ?? 'default'
  const requestedRunId = (options.runId ?? randomUUID())
    .replace(/[^a-zA-Z0-9]+/gu, '-')
    .replace(/^-+|-+$/gu, '')
    .slice(0, 12)
  const runId = requestedRunId || randomUUID().replaceAll('-', '').slice(0, 12)
  const jobResources = createK6LoadJobResources(environment, profile, {
    jobName: `factory-load-${profile.name.slice(0, 14)}-${runId}`,
    namespace,
    image: options.k6Image,
    timeoutMs: options.timeoutMs,
  })
  const client = options.client ?? new KubectlLoadJobClient({
    context: options.kubeContext ?? environment.kubeContext,
  })
  const now = options.now ?? (() => new Date())
  const startedAt = now().toISOString()
  let mainError: unknown

  try {
    await client.apply(jobResources.resources, namespace)
    await client.waitForCompletion(jobResources.jobName, namespace, jobResources.timeoutMs)
    const logs = await client.logs(jobResources.jobName, namespace)
    const measured = parseK6LoadMeasurements(logs)
    const gate = evaluateLoadSlo(measured, profile.thresholds)
    const status = gate.passed ? 'pass' : 'fail'
    const job: LoadEvidence['job'] = {
      name: jobResources.jobName,
      namespace,
      completed: true,
    }
    const evidence: LoadEvidence = {
      contract: LOAD_EVIDENCE_CONTRACT,
      environmentId: environment.id,
      profile: evidenceProfile(profile),
      job,
      startedAt,
      completedAt: now().toISOString(),
      thresholds: profile.thresholds,
      measured,
      gate: { status, violations: gate.violations },
    }
    const evidenceJson = serializeLoadEvidence(evidence)
    if (options.evidencePath) {
      await mkdir(dirname(options.evidencePath), { recursive: true })
      await writeFile(options.evidencePath, evidenceJson, 'utf8')
    }
    return {
      status,
      passed: gate.passed,
      measured,
      violations: gate.violations,
      evidence,
      evidenceJson,
      job,
    }
  } catch (error) {
    mainError = error
    throw error
  } finally {
    if (options.cleanup !== false) {
      try {
        await client.delete(jobResources.jobName, jobResources.configMapName, namespace)
      } catch (cleanupError) {
        if (mainError === undefined) throw cleanupError
      }
    }
  }
}

export type {
  KubernetesLoadJobClient,
  LoadEnvironment,
  LoadProfile,
  LoadThresholds,
}
