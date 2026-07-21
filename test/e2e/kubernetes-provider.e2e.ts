import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { resolve } from 'node:path'

import {
  KUBERNETES_ENVIRONMENT_ID_LABEL,
  KUBERNETES_EXPIRES_AT_LABEL,
  KUBERNETES_CONNECTION_ID_ANNOTATION,
  KUBERNETES_MANAGED_BY_LABEL,
  KubernetesConnectionRegistry,
  KubernetesEnvironmentProvider,
  StackDeployer,
  loadKubernetesStackDescriptor,
  type Environment,
} from '../../src/index.js'

const repoRoot = resolve(import.meta.dirname, '../..')
const kubeconfig = process.env.KUBECONFIG
if (!kubeconfig) throw new Error('KUBECONFIG must point to the stand-in customer kind cluster')
process.env.FACTORY_E2E_POSTGRES_PASSWORD ??= 'factory-e2e-not-production'

interface Result { code: number | null; stdout: string; stderr: string }

const kubectl = async (args: string[], input?: string): Promise<Result> => await new Promise((resolvePromise, reject) => {
  const child = spawn('kubectl', ['--kubeconfig', kubeconfig, ...args], { stdio: ['pipe', 'pipe', 'pipe'] })
  let stdout = ''
  let stderr = ''
  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', (chunk: string) => { stdout += chunk })
  child.stderr.on('data', (chunk: string) => { stderr += chunk })
  child.once('error', reject)
  child.once('close', (code) => resolvePromise({ code, stdout, stderr }))
  child.stdin.end(input)
})

const mustKubectl = async (args: string[], input?: string): Promise<Result> => {
  const result = await kubectl(args, input)
  if (result.code !== 0) {
    throw new Error(`kubectl ${args.join(' ')} failed (${result.code}): ${result.stderr || result.stdout}`)
  }
  return result
}

const apply = async (resources: unknown, namespace?: string): Promise<void> => {
  const args = [...(namespace ? ['--namespace', namespace] : []), 'apply', '--filename', '-']
  await mustKubectl(args, JSON.stringify(resources))
}

const create = async (resources: unknown, namespace?: string): Promise<void> => {
  const args = [...(namespace ? ['--namespace', namespace] : []), 'create', '--filename', '-']
  await mustKubectl(args, JSON.stringify(resources))
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const eventuallyFetch = async (url: string): Promise<Response> => {
  const deadline = Date.now() + 30_000
  let lastError: unknown
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url)
      if (response.ok) return response
      lastError = new Error(`HTTP ${response.status}`)
    } catch (error) {
      lastError = error
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 500))
  }
  throw new Error(`Endpoint ${url} never became reachable: ${String(lastError)}`)
}

const namespaceExists = async (name: string): Promise<boolean> =>
  (await kubectl(['get', 'namespace', name, '--output', 'name'])).code === 0

const objectUid = async (kind: string, name: string, namespace?: string): Promise<string | undefined> => {
  const args = [...(namespace ? ['--namespace', namespace] : []), 'get', `${kind}/${name}`, '--output', 'jsonpath={.metadata.uid}']
  const result = await kubectl(args)
  return result.code === 0 ? result.stdout.trim() || undefined : undefined
}

const deleteFixtureNamespace = async (name: string, expectedUid: string | undefined): Promise<void> => {
  if (!expectedUid) return
  const currentUid = await objectUid('namespace', name)
  if (!currentUid) return
  assert(currentUid === expectedUid, `refusing to clean up namespace ${name}: fixture identity changed`)
  await mustKubectl(['delete', 'namespace', name, '--wait=true', '--timeout=3m'])
}

const suffix = `${process.pid}-${randomUUID().replaceAll('-', '').slice(0, 8)}`
const prodNamespace = `prod-customer-${suffix}`
const controlNamespace = `network-control-${suffix}`
const orphanNamespace = `factory-orphan-${suffix}`
let environment: Environment | undefined
let prodNamespaceUid: string | undefined
let prodDeploymentUid: string | undefined
let controlNamespaceUid: string | undefined
let orphanNamespaceUid: string | undefined

const registry = new KubernetesConnectionRegistry({
  connections: [{
    id: 'customer-eks-sim',
    target: 'byoc',
    customers: ['stand-in-customer'],
    repositories: ['AgentWorkforce/factory'],
    credential: { kind: 'kubeconfig', secretRef: 'env:KUBECONFIG' },
    protectedNamespaces: [prodNamespace, 'default', 'kube-system'],
  }],
})
const provider = new KubernetesEnvironmentProvider({ registry })
const deployer = new StackDeployer({ kubernetes: provider })

try {
  await create({
    apiVersion: 'v1', kind: 'Namespace',
    metadata: { name: prodNamespace, labels: { 'factory.agentworkforce.dev/production': 'true' } },
  })
  prodNamespaceUid = await objectUid('namespace', prodNamespace)
  assert(prodNamespaceUid, 'production fixture namespace has no UID')
  await create({
    apiVersion: 'v1', kind: 'Namespace',
    metadata: { name: controlNamespace, labels: { 'factory.agentworkforce.dev/network-control': 'true' } },
  })
  controlNamespaceUid = await objectUid('namespace', controlNamespace)
  assert(controlNamespaceUid, 'network control namespace has no UID')
  await apply({
    apiVersion: 'v1', kind: 'List', items: [
      {
        apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'prod-api' },
        spec: {
          replicas: 1,
          selector: { matchLabels: { app: 'prod-api' } },
          template: {
            metadata: { labels: { app: 'prod-api' } },
            spec: { containers: [{ name: 'api', image: 'hashicorp/http-echo:1.0.0', args: ['-listen=:5678', '-text=prod'] }] },
          },
        },
      },
      {
        apiVersion: 'v1', kind: 'Service', metadata: { name: 'prod-api' },
        spec: { selector: { app: 'prod-api' }, ports: [{ port: 5678 }] },
      },
    ],
  }, prodNamespace)
  await mustKubectl(['--namespace', prodNamespace, 'rollout', 'status', 'deployment/prod-api', '--timeout=180s'])
  prodDeploymentUid = await objectUid('deployment', 'prod-api', prodNamespace)
  assert(prodDeploymentUid, 'production fixture deployment has no UID')
  const prodServiceIp = (await mustKubectl([
    '--namespace', prodNamespace, 'get', 'service/prod-api', '--output', 'jsonpath={.spec.clusterIP}',
  ])).stdout.trim()
  const prodEndpointIp = (await mustKubectl([
    '--namespace', prodNamespace, 'get', 'endpoints/prod-api', '--output', 'jsonpath={.subsets[0].addresses[0].ip}',
  ])).stdout.trim()
  assert(/^\d{1,3}(?:\.\d{1,3}){3}$/u.test(prodServiceIp), 'production fixture has no routable ClusterIP')
  assert(/^\d{1,3}(?:\.\d{1,3}){3}$/u.test(prodEndpointIp), 'production fixture has no ready endpoint')

  // Prove the destination is reachable from another namespace before asserting
  // that Factory's NetworkPolicy blocks the exact same address. Without this
  // positive control, a broken CNI or dead Service could make isolation green.
  await create({
    apiVersion: 'v1', kind: 'Pod', metadata: { name: 'prod-connectivity-control' },
    spec: {
      restartPolicy: 'Never',
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{
        name: 'probe', image: 'curlimages/curl:8.12.1',
        command: ['curl', '-fsS', '--max-time', '10', `http://${prodServiceIp}:5678/`],
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
      }],
    },
  }, controlNamespace)
  await mustKubectl([
    '--namespace', controlNamespace,
    'wait', 'pod/prod-connectivity-control', '--for=jsonpath={.status.phase}=Succeeded', '--timeout=120s',
  ])
  const controlLogs = (await mustKubectl([
    '--namespace', controlNamespace, 'logs', 'pod/prod-connectivity-control',
  ])).stdout
  assert(controlLogs.trim() === 'prod', `positive network control returned unexpected output: ${controlLogs}`)

  const descriptor = await loadKubernetesStackDescriptor(
    resolve(repoRoot, 'test/fixtures/kubernetes/verification-stack.yaml'),
  )
  environment = await deployer.deploy(descriptor, {
    customerId: 'stand-in-customer',
    repository: 'AgentWorkforce/factory',
    ownerId: 'kubernetes-provider-e2e',
    repoRoot,
    ttl: 10 * 60_000,
  })
  assert(environment.dispatchNamespace !== prodNamespace, 'provider reused the production namespace')
  assert(environment.dispatchNamespace.startsWith('factory-'), 'provider did not generate a Factory namespace')
  assert(await provider.status(environment.id) === 'ready', 'provider status did not become ready')
  const response = await eventuallyFetch(environment.endpoints.api)
  assert(await response.text() === 'factory-kubernetes-ok\n', 'forwarded endpoint returned unexpected body')
  console.log(`provision/reachable: ${environment.id} ${environment.endpoints.api}`)

  const serviceAccount = `system:serviceaccount:${environment.id}:factory-guardrail-deployer`
  const namespacedPermission = await kubectl([
    'auth', 'can-i', 'create', 'deployments.apps', '--namespace', environment.id, '--as', serviceAccount,
  ])
  assert(namespacedPermission.code === 0 && namespacedPermission.stdout.trim() === 'yes',
    `scoped service account cannot manage its namespace: ${namespacedPermission.stderr || namespacedPermission.stdout}`)
  const clusterAdminPermission = await kubectl([
    'auth', 'can-i', 'create', 'clusterroles.rbac.authorization.k8s.io', '--as', serviceAccount,
  ])
  assert(clusterAdminPermission.code !== 0 && clusterAdminPermission.stdout.trim() === 'no',
    'scoped service account unexpectedly has a cluster-admin capability')
  console.log('rbac: namespace create allowed; cluster role create denied')

  await apply({
    apiVersion: 'v1', kind: 'Pod', metadata: { name: 'isolation-probe' },
    spec: {
      restartPolicy: 'Never',
      serviceAccountName: 'factory-guardrail-workload',
      automountServiceAccountToken: false,
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{
        name: 'probe', image: 'curlimages/curl:8.12.1',
        // Use the verified ClusterIP directly so a DNS failure cannot create a
        // false green for NetworkPolicy isolation.
        command: ['sh', '-c', `if curl -fsS --max-time 5 http://${prodServiceIp}:5678/; then echo guardrail-missing; exit 42; else exit 0; fi`],
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
      }],
    },
  }, environment.id)
  const isolation = await kubectl([
    '--namespace', environment.id,
    'wait', 'pod/isolation-probe', '--for=jsonpath={.status.phase}=Succeeded', '--timeout=90s',
  ])
  if (isolation.code !== 0) {
    const logs = await kubectl(['--namespace', environment.id, 'logs', 'pod/isolation-probe'])
    throw new Error(`NetworkPolicy isolation did not bind: ${isolation.stderr}\n${logs.stdout}\n${logs.stderr}`)
  }
  console.log('isolation: verification pod could not reach prod service')

  const quotaBuster = {
    apiVersion: 'v1', kind: 'Pod', metadata: { name: 'quota-buster', namespace: environment.id },
    spec: {
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{
        name: 'quota-buster', image: 'busybox:1.36', command: ['sh', '-c', 'sleep 600'],
        // This stays within the per-container LimitRange (2 CPU) but exceeds
        // the namespace's remaining requests.cpu quota. The assertion below
        // therefore proves ResourceQuota admission specifically.
        resources: { requests: { cpu: '2', memory: '16Mi' }, limits: { cpu: '2', memory: '16Mi' } },
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
      }],
    },
  }
  const quotaResult = await kubectl(
    ['--namespace', environment.id, 'create', '--filename', '-'],
    JSON.stringify(quotaBuster),
  )
  assert(
    quotaResult.code !== 0 && /exceeded quota: factory-guardrail-quota/iu.test(quotaResult.stderr),
    `quota-busting pod was not rejected: ${quotaResult.stdout}\n${quotaResult.stderr}`)
  console.log('quota: oversized workload rejected by admission')

  const k6Script = `
import http from 'k6/http';
import { check } from 'k6';
export const options = { vus: 2, duration: '5s' };
export default function () {
  const response = http.get('http://fixture-api.${environment.id}.svc.cluster.local:5678/');
  check(response, { healthy: (r) => r.status === 200 });
}
export function handleSummary(data) {
  const duration = data.metrics.http_req_duration.values;
  const requests = data.metrics.http_reqs.values;
  const failed = data.metrics.http_req_failed.values;
  return { stdout: 'FACTORY_K6_EVIDENCE=' + JSON.stringify({
    requestCount: requests.count,
    errorRate: failed.rate,
    p95LatencyMs: duration['p(95)']
  }) + '\\n' };
}`
  await apply({
    apiVersion: 'v1', kind: 'List', items: [
      {
        apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'factory-k6' },
        data: { 'script.js': k6Script },
      },
      {
        apiVersion: 'batch/v1', kind: 'Job', metadata: { name: 'factory-k6' },
        spec: {
          backoffLimit: 0,
          template: {
            metadata: { labels: { app: 'factory-k6' } },
            spec: {
              restartPolicy: 'Never',
              serviceAccountName: 'factory-guardrail-workload',
              automountServiceAccountToken: false,
              securityContext: { runAsNonRoot: true, runAsUser: 12345, seccompProfile: { type: 'RuntimeDefault' } },
              containers: [{
                name: 'k6', image: 'grafana/k6:1.7.1', args: ['run', '/scripts/script.js'],
                volumeMounts: [{ name: 'script', mountPath: '/scripts' }],
                securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
              }],
              volumes: [{ name: 'script', configMap: { name: 'factory-k6' } }],
            },
          },
        },
      },
    ],
  }, environment.id)
  await mustKubectl([
    '--namespace', environment.id, 'wait', 'job/factory-k6', '--for=condition=complete', '--timeout=180s',
  ])
  const k6Logs = (await mustKubectl(['--namespace', environment.id, 'logs', 'job/factory-k6'])).stdout
  const evidenceMatch = /FACTORY_K6_EVIDENCE=(\{[^\n]+\})/u.exec(k6Logs)
  assert(evidenceMatch, `k6 did not emit SLO evidence:\n${k6Logs}`)
  const evidence = JSON.parse(evidenceMatch[1]) as { requestCount: number; errorRate: number; p95LatencyMs: number }
  assert(evidence.requestCount > 0, 'k6 made no requests')
  assert(evidence.errorRate === 0, `k6 error-rate SLO failed: ${evidence.errorRate}`)
  assert(evidence.p95LatencyMs < 2_000, `k6 p95 SLO failed: ${evidence.p95LatencyMs}ms`)
  console.log(`load: ${evidence.requestCount} requests, errorRate=${evidence.errorRate}, p95=${evidence.p95LatencyMs}ms`)

  await create({
    apiVersion: 'v1', kind: 'Namespace',
    metadata: {
      name: orphanNamespace,
      labels: {
        [KUBERNETES_MANAGED_BY_LABEL]: 'factory',
        [KUBERNETES_ENVIRONMENT_ID_LABEL]: orphanNamespace,
        [KUBERNETES_EXPIRES_AT_LABEL]: '1',
      },
      annotations: { [KUBERNETES_CONNECTION_ID_ANNOTATION]: 'customer-eks-sim' },
    },
  })
  orphanNamespaceUid = await objectUid('namespace', orphanNamespace)
  assert(orphanNamespaceUid, 'orphan fixture namespace has no UID')
  await provider.destroy(environment.id)
  assert(!await namespaceExists(environment.id), 'destroy left the provisioned namespace behind')
  const reaped = await provider.reap()
  assert(reaped.reaped.some((entry) => entry.id === orphanNamespace), 'TTL reaper did not report the orphan')
  assert(!await namespaceExists(orphanNamespace), 'TTL reaper left the orphaned namespace behind')
  assert(await objectUid('namespace', prodNamespace) === prodNamespaceUid,
    'provider teardown or reaper deleted/recreated the production namespace')
  assert(await objectUid('deployment', 'prod-api', prodNamespace) === prodDeploymentUid,
    'provider teardown or reaper deleted/recreated the production workload')
  assert(await objectUid('namespace', controlNamespace) === controlNamespaceUid,
    'provider teardown or reaper touched the network-control namespace')
  console.log('reaper: provisioned and orphaned namespaces deleted; prod/control identities untouched')
} finally {
  if (environment) await provider.destroy(environment.id).catch(() => undefined)
  await deleteFixtureNamespace(orphanNamespace, orphanNamespaceUid)
  await deleteFixtureNamespace(controlNamespace, controlNamespaceUid)
  await deleteFixtureNamespace(prodNamespace, prodNamespaceUid)
}
