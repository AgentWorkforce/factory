import { spawn } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'

import type { ResolvedLoadProfile, ResolvedLoadTargetProfile } from './load-profile.js'
import { durationToMilliseconds } from './load-profile.js'

export const DEFAULT_K6_IMAGE = 'grafana/k6:1.7.1@sha256:4fd3a694926b064d3491d9b02b01cde886583c4931f1223816e3d9a7bdfa7e0f'
export const K6_EVIDENCE_PREFIX = 'FACTORY_LOAD_EVIDENCE_JSON='

export interface LoadEnvironment {
  id: string
  endpoints: Record<string, string>
  namespace?: string
  kubeContext?: string
}

export interface ResolvedLoadTarget {
  name: string
  url: string
  method: ResolvedLoadTargetProfile['method']
  headers: Record<string, string>
  body: string | null
  expectedStatuses: number[] | null
  weight: number
}

export interface K6LoadJobResources {
  jobName: string
  configMapName: string
  namespace: string
  timeoutMs: number
  resources: Array<Record<string, unknown>>
}

export interface KubernetesLoadJobClient {
  apply(resources: Array<Record<string, unknown>>, namespace: string, signal?: AbortSignal): Promise<void>
  waitForCompletion(jobName: string, namespace: string, timeoutMs: number, signal?: AbortSignal): Promise<void>
  logs(jobName: string, namespace: string, signal?: AbortSignal): Promise<string>
  delete(jobName: string, configMapName: string, namespace: string, signal?: AbortSignal): Promise<void>
}

export interface KubectlCommandResult {
  stdout: string
  stderr: string
}

export type KubectlCommandRunner = (
  args: string[],
  input?: string,
  signal?: AbortSignal,
) => Promise<KubectlCommandResult>

export interface KubectlLoadJobClientOptions {
  context?: string
  executable?: string
  pollIntervalMs?: number
  runner?: KubectlCommandRunner
}

const commandError = (
  executable: string,
  args: string[],
  code: number | null,
  stderr: string,
): Error => new Error(
  `${executable} ${args.join(' ')} exited with ${code ?? 'no status'}: ${stderr.trim().slice(-4_000) || 'no stderr'}`,
)

export const defaultKubectlCommandRunner = (
  executable = 'kubectl',
): KubectlCommandRunner => async (args, input, signal) => await new Promise((resolve, reject) => {
  const child = spawn(executable, args, { stdio: ['pipe', 'pipe', 'pipe'] })
  let stdout = ''
  let stderr = ''
  let settled = false
  let aborted = false
  let forceKillTimer: ReturnType<typeof setTimeout> | undefined
  const abort = (): void => {
    aborted = true
    child.kill('SIGTERM')
    forceKillTimer = setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
    }, 2_000)
    forceKillTimer.unref()
  }
  const finish = (error?: Error, result?: KubectlCommandResult): void => {
    if (settled) return
    settled = true
    if (forceKillTimer) clearTimeout(forceKillTimer)
    signal?.removeEventListener('abort', abort)
    if (error) reject(error)
    else resolve(result!)
  }

  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', (chunk: string) => { stdout += chunk })
  child.stderr.on('data', (chunk: string) => { stderr += chunk })
  child.once('error', (error) => finish(error))
  child.once('close', (code) => {
    if (aborted) {
      finish(new Error('kubectl command aborted'))
      return
    }
    if (code === 0) finish(undefined, { stdout, stderr })
    else finish(commandError(executable, args, code, stderr))
  })
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) {
    abort()
  } else {
    child.stdin.end(input)
  }
})

export class KubectlLoadJobClient implements KubernetesLoadJobClient {
  private readonly contextArgs: string[]
  private readonly pollIntervalMs: number
  private readonly runner: KubectlCommandRunner

  constructor(options: KubectlLoadJobClientOptions = {}) {
    this.contextArgs = options.context ? ['--context', options.context] : []
    this.pollIntervalMs = options.pollIntervalMs ?? 1_000
    this.runner = options.runner ?? defaultKubectlCommandRunner(options.executable)
  }

  async apply(resources: Array<Record<string, unknown>>, namespace: string, signal?: AbortSignal): Promise<void> {
    await this.runner([
      ...this.contextArgs,
      '--namespace', namespace,
      'apply', '--filename', '-',
    ], JSON.stringify({ apiVersion: 'v1', kind: 'List', items: resources }), signal)
  }

  async waitForCompletion(jobName: string, namespace: string, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const { stdout } = await this.runner([
        ...this.contextArgs,
        '--namespace', namespace,
        'get', 'job', jobName,
        '--output', 'json',
      ], undefined, signal)
      const job = JSON.parse(stdout) as {
        status?: {
          succeeded?: number
          failed?: number
          conditions?: Array<{ type?: string; status?: string; reason?: string; message?: string }>
        }
      }
      if ((job.status?.succeeded ?? 0) > 0) return
      if ((job.status?.failed ?? 0) > 0 || job.status?.conditions?.some(
        (condition) => condition.type === 'Failed' && condition.status === 'True',
      )) {
        const failure = job.status?.conditions?.find(
          (condition) => condition.type === 'Failed' && condition.status === 'True',
        )
        throw new Error(
          `k6 Job ${namespace}/${jobName} failed${failure?.reason ? ` (${failure.reason})` : ''}${failure?.message ? `: ${failure.message}` : ''}`,
        )
      }
      await delay(Math.min(this.pollIntervalMs, Math.max(1, deadline - Date.now())), undefined, { signal })
    }
    throw new Error(`Timed out after ${timeoutMs}ms waiting for k6 Job ${namespace}/${jobName}`)
  }

  async logs(jobName: string, namespace: string, signal?: AbortSignal): Promise<string> {
    const { stdout } = await this.runner([
      ...this.contextArgs,
      '--namespace', namespace,
      'logs', `job/${jobName}`,
      '--container', 'k6',
    ], undefined, signal)
    return stdout
  }

  async delete(jobName: string, configMapName: string, namespace: string, signal?: AbortSignal): Promise<void> {
    await this.runner([
      ...this.contextArgs,
      '--namespace', namespace,
      'delete',
      `job/${jobName}`,
      `configmap/${configMapName}`,
      '--ignore-not-found=true',
      '--wait=false',
    ], undefined, signal)
  }
}

export function resolveLoadTargets(
  environment: LoadEnvironment,
  profile: ResolvedLoadProfile,
): ResolvedLoadTarget[] {
  return profile.targets.map((target) => {
    const baseUrl = target.url ?? environment.endpoints[target.endpoint as string]
    if (!baseUrl) {
      throw new Error(
        `Load target ${target.name} references unknown environment endpoint ${target.endpoint}`,
      )
    }

    let url: URL
    try {
      const normalizedBase = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`
      url = new URL(target.path ?? '', normalizedBase)
    } catch {
      throw new Error(`Load target ${target.name} resolved to an invalid URL`)
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error(`Load target ${target.name} must resolve to http or https`)
    }

    const headers = { ...(target.headers ?? {}) }
    let body: string | null = null
    if (target.body !== undefined) {
      if (typeof target.body === 'string') {
        body = target.body
      } else {
        try {
          body = JSON.stringify(target.body)
        } catch {
          throw new Error(`Load target ${target.name} body is not JSON serializable`)
        }
        const hasContentType = Object.keys(headers).some((header) => header.toLowerCase() === 'content-type')
        if (!hasContentType) headers['content-type'] = 'application/json'
      }
    }

    return {
      name: target.name,
      url: url.toString(),
      method: target.method,
      headers,
      body,
      expectedStatuses: target.expectedStatuses ?? null,
      weight: target.weight,
    }
  })
}

export function k6ScenarioFor(profile: ResolvedLoadProfile): Record<string, unknown> {
  const target = profile.rps ?? profile.vus as number
  if (profile.rps !== undefined) {
    const preAllocatedVUs = profile.vus ?? Math.max(1, Math.ceil(profile.rps))
    const maxVUs = profile.maxVus ?? Math.max(preAllocatedVUs, Math.ceil(preAllocatedVUs * 2))
    if (profile.ramp) {
      return {
        executor: 'ramping-arrival-rate',
        startRate: 0,
        timeUnit: '1s',
        preAllocatedVUs,
        maxVUs,
        stages: [
          { duration: profile.ramp.up, target },
          { duration: profile.duration, target },
          ...(profile.ramp.down ? [{ duration: profile.ramp.down, target: 0 }] : []),
        ],
        gracefulStop: '5s',
      }
    }
    return {
      executor: 'constant-arrival-rate',
      rate: profile.rps,
      timeUnit: '1s',
      duration: profile.duration,
      preAllocatedVUs,
      maxVUs,
      gracefulStop: '5s',
    }
  }

  if (profile.ramp) {
    return {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: profile.ramp.up, target },
        { duration: profile.duration, target },
        ...(profile.ramp.down ? [{ duration: profile.ramp.down, target: 0 }] : []),
      ],
      gracefulRampDown: '5s',
    }
  }
  return {
    executor: 'constant-vus',
    vus: profile.vus,
    duration: profile.duration,
    gracefulStop: '5s',
  }
}

export function renderK6Script(
  environment: LoadEnvironment,
  profile: ResolvedLoadProfile,
): string {
  const targets = resolveLoadTargets(environment, profile)
  const scenario = k6ScenarioFor(profile)
  return `import http from 'k6/http';
import { Counter } from 'k6/metrics';

const targets = ${JSON.stringify(targets)};
const histogramBucketsMs = ${JSON.stringify(profile.histogramBucketsMs)};
const requests = new Counter('factory_requests');
const errors = new Counter('factory_errors');
const latencyMetricNames = histogramBucketsMs.map((_, index) => 'factory_latency_bucket_' + index);
const latencyCounters = latencyMetricNames.map((name) => new Counter(name));
const latencyOverflow = new Counter('factory_latency_overflow');
const totalWeight = targets.reduce((sum, target) => sum + target.weight, 0);

export const options = ${JSON.stringify({
    scenarios: { factory_load: scenario },
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  })};

function selectTarget() {
  let selection = Math.random() * totalWeight;
  for (const target of targets) {
    selection -= target.weight;
    if (selection < 0) return target;
  }
  return targets[targets.length - 1];
}

function metric(data, name, key) {
  const value = data.metrics[name] && data.metrics[name].values[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

export default function () {
  const target = selectTarget();
  const response = http.request(target.method, target.url, target.body, {
    headers: target.headers,
    tags: { factory_target: target.name },
  });
  const latency = response.timings.duration;
  const failed = target.expectedStatuses === null
    ? response.status < 200 || response.status >= 400
    : !target.expectedStatuses.includes(response.status);
  requests.add(1);
  errors.add(failed ? 1 : 0);
  for (let index = 0; index < histogramBucketsMs.length; index += 1) {
    latencyCounters[index].add(latency <= histogramBucketsMs[index] ? 1 : 0);
  }
  latencyOverflow.add(latency > histogramBucketsMs[histogramBucketsMs.length - 1] ? 1 : 0);
}

export function handleSummary(data) {
  const requestCount = metric(data, 'factory_requests', 'count');
  const errorCount = metric(data, 'factory_errors', 'count');
  const summary = {
    requestCount,
    errorCount,
    errorRate: requestCount > 0 ? errorCount / requestCount : 1,
    throughputRps: metric(data, 'factory_requests', 'rate'),
    durationMs: data.state && typeof data.state.testRunDurationMs === 'number'
      ? data.state.testRunDurationMs
      : 0,
    latency: {
      minMs: metric(data, 'http_req_duration', 'min'),
      averageMs: metric(data, 'http_req_duration', 'avg'),
      medianMs: metric(data, 'http_req_duration', 'med'),
      maxMs: metric(data, 'http_req_duration', 'max'),
      p95Ms: metric(data, 'http_req_duration', 'p(95)'),
      p99Ms: metric(data, 'http_req_duration', 'p(99)'),
    },
    histogram: histogramBucketsMs.map((bucket, index) => ({
      upperBoundMs: bucket,
      count: metric(data, latencyMetricNames[index], 'count'),
    })).concat([{
      upperBoundMs: null,
      count: metric(data, 'factory_latency_overflow', 'count'),
    }]),
  };
  return { stdout: '${K6_EVIDENCE_PREFIX}' + JSON.stringify(summary) + '\\n' };
}
`
}

const dnsLabel = (value: string): string => {
  const normalized = value.toLowerCase()
    .replace(/[^a-z0-9-]+/gu, '-')
    .replace(/^-+|-+$/gu, '')
    .slice(0, 40)
  return normalized || 'load'
}

export interface CreateK6LoadJobOptions {
  jobName: string
  namespace: string
  image?: string
  timeoutMs?: number
}

export function createK6LoadJobResources(
  environment: LoadEnvironment,
  profile: ResolvedLoadProfile,
  options: CreateK6LoadJobOptions,
): K6LoadJobResources {
  const jobName = dnsLabel(options.jobName).slice(0, 63)
  const configMapName = `${jobName}-script`.slice(0, 63)
  const workloadDurationMs = durationToMilliseconds(profile.duration) +
    (profile.ramp ? durationToMilliseconds(profile.ramp.up) : 0) +
    (profile.ramp?.down ? durationToMilliseconds(profile.ramp.down) : 0)
  const timeoutMs = options.timeoutMs ?? workloadDurationMs + 120_000
  const labels = {
    'app.kubernetes.io/name': 'factory-load',
    'app.kubernetes.io/component': 'load-generator',
    'factory.agent-relay.dev/environment': dnsLabel(environment.id),
    'factory.agent-relay.dev/profile': dnsLabel(profile.name),
  }

  return {
    jobName,
    configMapName,
    namespace: options.namespace,
    timeoutMs,
    resources: [
      {
        apiVersion: 'v1',
        kind: 'ConfigMap',
        metadata: { name: configMapName, namespace: options.namespace, labels },
        immutable: true,
        data: { 'load.js': renderK6Script(environment, profile) },
      },
      {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: { name: jobName, namespace: options.namespace, labels },
        spec: {
          backoffLimit: 0,
          activeDeadlineSeconds: Math.max(1, Math.ceil(timeoutMs / 1_000)),
          ttlSecondsAfterFinished: 600,
          template: {
            metadata: { labels },
            spec: {
              restartPolicy: 'Never',
              automountServiceAccountToken: false,
              enableServiceLinks: false,
              securityContext: {
                runAsNonRoot: true,
                seccompProfile: { type: 'RuntimeDefault' },
              },
              containers: [{
                name: 'k6',
                image: options.image ?? DEFAULT_K6_IMAGE,
                imagePullPolicy: 'IfNotPresent',
                args: ['run', '--quiet', '/scripts/load.js'],
                env: [{ name: 'K6_NO_USAGE_REPORT', value: 'true' }],
                securityContext: {
                  allowPrivilegeEscalation: false,
                  capabilities: { drop: ['ALL'] },
                },
                resources: {
                  requests: { cpu: '100m', memory: '64Mi' },
                  limits: { cpu: '1', memory: '512Mi' },
                },
                volumeMounts: [{ name: 'script', mountPath: '/scripts', readOnly: true }],
              }],
              volumes: [{
                name: 'script',
                configMap: { name: configMapName },
              }],
            },
          },
        },
      },
    ],
  }
}
