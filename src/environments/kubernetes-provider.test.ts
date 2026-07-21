import { describe, expect, it, vi } from 'vitest'

import {
  DEFAULT_MANAGED_FIDELITY_CAVEAT,
  EnvironmentKubernetesCredentialResolver,
  KubernetesConnectionRegistry,
  type KubernetesCredentialResolver,
  type ResolvedKubernetesConnection,
} from './connection-registry'
import type {
  KubernetesClient,
  KubernetesPortForward,
  KubernetesResource,
} from './kubernetes-client'
import {
  KUBERNETES_ENVIRONMENT_ID_LABEL,
  KUBERNETES_EXPIRES_AT_LABEL,
  KUBERNETES_CONNECTION_ID_ANNOTATION,
  KUBERNETES_CLUSTER_RESOURCES_ANNOTATION,
  KUBERNETES_MANAGED_BY_LABEL,
  KubernetesEnvironmentProvider,
} from './kubernetes-provider'
import { StackDeployer } from './stack-deployer'
import type { KubernetesDeployment } from './stack-descriptor'

const credentialResolver = (): KubernetesCredentialResolver => ({
  resolve: vi.fn(async (reference: string) => ({ kubeconfigPath: `/resolved/${reference.replaceAll(':', '-')}` })),
})

const registry = (resolver = credentialResolver()): KubernetesConnectionRegistry => new KubernetesConnectionRegistry({
  connections: [
    {
      id: 'customer-eks',
      target: 'byoc',
      customers: ['customer-a'],
      repositories: ['AgentWorkforce/factory'],
      credential: { kind: 'irsa', secretRef: 'aws-sm:customer-a/verification' },
      protectedNamespaces: ['payments-prod'],
      nodeSelector: { 'factory.agentworkforce.dev/pool': 'verification' },
      tolerations: [{ key: 'verification', operator: 'Exists', effect: 'NoSchedule' }],
    },
    {
      id: 'managed-eks',
      target: 'managed',
      credential: { kind: 'kubeconfig', secretRef: 'env:MANAGED_KUBECONFIG' },
    },
  ],
}, resolver)

class FakeKubernetesClient implements KubernetesClient {
  readonly namespaces = new Map<string, KubernetesResource>()
  readonly clusterResources = new Map<string, KubernetesResource>()
  readonly applies: Array<{ namespace: string; resources: KubernetesResource[] }> = []
  readonly deletes: string[] = []
  readonly clusterDeletes: string[] = []
  readonly forwards: string[] = []
  readonly stoppedForwards: string[] = []
  failForwardFor: string | undefined
  failClusterCreateAfterPersistFor: string | undefined
  rendered: KubernetesResource[] = [
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: { name: 'api' },
      spec: { template: { spec: { containers: [{ name: 'api', image: 'example.test/api' }] } } },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: { name: 'api' },
      spec: { selector: { app: 'api' }, ports: [{ port: 8080 }] },
    },
  ]

  async createNamespace(resource: KubernetesResource): Promise<void> {
    const name = resource.metadata?.name
    if (!name) throw new Error('namespace name required')
    if (this.namespaces.has(name)) throw new Error('AlreadyExists')
    this.namespaces.set(name, structuredClone(resource))
  }

  async createClusterResource(resource: KubernetesResource): Promise<void> {
    const key = clusterResourceKey(resource)
    if (this.clusterResources.has(key)) throw new Error('AlreadyExists')
    this.clusterResources.set(key, structuredClone(resource))
    if (key === this.failClusterCreateAfterPersistFor) throw new Error('connection dropped after create')
  }

  async apply(resources: KubernetesResource[], namespace: string): Promise<void> {
    this.applies.push({ namespace, resources: structuredClone(resources) })
  }

  async render(
    _deployment: KubernetesDeployment,
    _namespace: string,
    _repoRoot: string,
    _connection: ResolvedKubernetesConnection,
  ): Promise<KubernetesResource[]> {
    return structuredClone(this.rendered)
  }

  async waitForReady(): Promise<void> {}

  async getNamespace(name: string): Promise<KubernetesResource | undefined> {
    const namespace = this.namespaces.get(name)
    return namespace ? structuredClone(namespace) : undefined
  }

  async listNamespaces(): Promise<KubernetesResource[]> {
    return [...this.namespaces.values()]
      .filter((namespace) => namespace.metadata?.labels?.[KUBERNETES_MANAGED_BY_LABEL] === 'factory')
      .map((namespace) => structuredClone(namespace))
  }

  async deleteNamespace(name: string): Promise<void> {
    this.deletes.push(name)
    this.namespaces.delete(name)
  }

  async getClusterResource(resource: KubernetesResource): Promise<KubernetesResource | undefined> {
    const current = this.clusterResources.get(clusterResourceKey(resource))
    return current ? structuredClone(current) : undefined
  }

  async deleteClusterResource(resource: KubernetesResource): Promise<void> {
    const key = clusterResourceKey(resource)
    this.clusterDeletes.push(key)
    this.clusterResources.delete(key)
  }

  async portForwardService(
    namespace: string,
    service: string,
    port: number,
  ): Promise<KubernetesPortForward> {
    this.forwards.push(`${namespace}/${service}:${port}`)
    if (service === this.failForwardFor) throw new Error(`port-forward failed for ${service}`)
    return {
      url: 'http://127.0.0.1:43123',
      stop: vi.fn(async () => { this.stoppedForwards.push(service) }),
    }
  }
}

const clusterResourceKey = (resource: KubernetesResource): string =>
  `${resource.apiVersion}/${resource.kind}/${resource.metadata?.name}`

const stack = (target: 'byoc' | 'managed' = 'byoc') => ({
  descriptor: {
    name: 'factory-fixture',
    deployKind: 'kubernetes' as const,
    target,
    deployment: { strategy: 'helm' as const, chart: 'test/fixtures/kubernetes/chart' },
    endpoints: [{ name: 'api', service: 'api', port: 8080 }],
    secrets: [{ name: 'database', key: 'password', secretRef: 'vault:customer-a/db' }],
  },
  repoRoot: process.cwd(),
})

describe('KubernetesConnectionRegistry', () => {
  it('resolves a kubeconfig path without returning unrelated process secrets', async () => {
    const resolver = new EnvironmentKubernetesCredentialResolver({
      KUBECONFIG_PATH: '/run/secrets/customer-kubeconfig',
      UNRELATED_SECRET: 'must-not-escape',
    })

    await expect(resolver.resolve('env:KUBECONFIG_PATH')).resolves.toEqual({
      kubeconfigPath: '/run/secrets/customer-kubeconfig',
    })
  })

  it('defaults selection to BYOC and resolves only the referenced credential', async () => {
    const resolver = credentialResolver()
    const connections = registry(resolver)

    const connection = await connections.resolve({
      customerId: 'customer-a',
      repository: 'AgentWorkforce/factory',
    })

    expect(connection).toMatchObject({
      id: 'customer-eks',
      target: 'byoc',
      credentialKind: 'irsa',
      kubeconfigPath: '/resolved/aws-sm-customer-a/verification',
    })
    expect(resolver.resolve).toHaveBeenCalledWith('aws-sm:customer-a/verification', 'irsa')
  })

  it('documents the fidelity difference on every managed connection', async () => {
    const connection = await registry().resolve({
      customerId: 'customer-without-access',
      repository: 'some/repo',
      target: 'managed',
    })

    expect(connection.fidelityCaveat).toBe(DEFAULT_MANAGED_FIDELITY_CAVEAT)
  })

  it('fails closed on ambiguous equally-specific cluster mappings', async () => {
    const connections = new KubernetesConnectionRegistry({
      connections: [
        { id: 'one', credential: { kind: 'kubeconfig', secretRef: 'env:ONE' } },
        { id: 'two', credential: { kind: 'kubeconfig', secretRef: 'env:TWO' } },
      ],
    }, credentialResolver())

    await expect(connections.resolve({ customerId: 'a', repository: 'r' })).rejects.toThrow(/Ambiguous/)
  })

  it('rejects duplicate connection identities', () => {
    expect(() => new KubernetesConnectionRegistry({
      connections: [
        { id: 'duplicate', credential: { kind: 'kubeconfig', secretRef: 'env:ONE' } },
        { id: 'duplicate', target: 'managed', credential: { kind: 'kubeconfig', secretRef: 'env:TWO' } },
      ],
    }, credentialResolver())).toThrow(/duplicate Kubernetes connection id duplicate/)
  })
})

describe('KubernetesEnvironmentProvider', () => {
  it('provisions a generated namespace with binding guardrails, scoped stack resources, and referenced secrets', async () => {
    const client = new FakeKubernetesClient()
    const resolveSecret = vi.fn(async (reference: string) => `resolved-${reference}`)
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(),
      client,
      secretResolver: { resolve: resolveSecret },
      now: () => new Date('2026-07-21T10:00:00.000Z'),
      randomId: () => 'abc123',
    })

    const environment = await provider.provision({
      customerId: 'customer-a',
      repository: 'AgentWorkforce/factory',
      ownerId: 'run/147',
      ttl: 120_000,
      target: 'byoc',
      stack: stack(),
    })

    expect(environment).toMatchObject({
      id: 'factory-factory-abc123',
      provider: 'kubernetes',
      dispatchNamespace: 'factory-factory-abc123',
      status: 'ready',
      endpoints: { api: 'http://127.0.0.1:43123/' },
      bindings: {
        'kubernetes.connection': 'customer-eks',
        'kubernetes.target': 'byoc',
      },
    })
    const namespace = client.namespaces.get(environment.id)
    expect(namespace?.metadata?.labels).toMatchObject({
      [KUBERNETES_MANAGED_BY_LABEL]: 'factory',
      [KUBERNETES_ENVIRONMENT_ID_LABEL]: environment.id,
      [KUBERNETES_EXPIRES_AT_LABEL]: String(Date.parse('2026-07-21T10:02:00.000Z') / 1_000),
      'pod-security.kubernetes.io/enforce': 'restricted',
    })

    const guardrails = client.applies[0].resources
    expect(guardrails.map((resource) => resource.kind)).toEqual(expect.arrayContaining([
      'ServiceAccount', 'Role', 'RoleBinding', 'ResourceQuota', 'LimitRange', 'NetworkPolicy',
    ]))
    const role = guardrails.find((resource) => resource.kind === 'Role') as KubernetesResource & { rules: Array<{ resources: string[] }> }
    expect(role.rules.flatMap((rule) => rule.resources)).not.toContain('clusterroles')
    expect(role.rules.flatMap((rule) => rule.resources)).not.toContain('networkpolicies')
    const policies = guardrails.filter((resource) => resource.kind === 'NetworkPolicy')
    expect(policies).toHaveLength(4)
    expect(JSON.stringify(policies)).toContain('payments-prod')
    expect(JSON.stringify(policies)).toContain('169.254.0.0/16')

    const secret = client.applies[1].resources[0] as KubernetesResource & { data: Record<string, string> }
    expect(resolveSecret).toHaveBeenCalledWith('vault:customer-a/db')
    expect(secret.data.password).toBe(Buffer.from('resolved-vault:customer-a/db').toString('base64'))
    expect(JSON.stringify(secret)).not.toContain('vault:customer-a/db')

    const deployment = client.applies[2].resources.find((resource) => resource.kind === 'Deployment') as KubernetesResource & {
      spec: { template: { spec: Record<string, unknown> } }
    }
    expect(deployment.metadata?.namespace).toBe(environment.id)
    expect(deployment.metadata?.labels?.[KUBERNETES_ENVIRONMENT_ID_LABEL]).toBe(environment.id)
    expect(deployment.spec.template.spec).toMatchObject({
      serviceAccountName: 'factory-guardrail-workload',
      automountServiceAccountToken: false,
      nodeSelector: { 'factory.agentworkforce.dev/pool': 'verification' },
      tolerations: [{ key: 'verification', operator: 'Exists', effect: 'NoSchedule' }],
      securityContext: {
        runAsNonRoot: true,
        seccompProfile: { type: 'RuntimeDefault' },
      },
    })
    expect((deployment.spec.template.spec.containers as Array<Record<string, unknown>>)[0].securityContext)
      .toMatchObject({ allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } })

    await provider.destroy(environment.id)
    await provider.destroy(environment.id)
    expect(client.deletes).toEqual([environment.id])
    expect(await provider.status(environment.id)).toBe('destroyed')
  })

  it('propagates the managed target and its explicit fidelity caveat', async () => {
    const client = new FakeKubernetesClient()
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(), client, randomId: () => 'managed1',
      secretResolver: { resolve: async () => 'secret' },
    })
    const deployer = new StackDeployer({ kubernetes: provider })

    const environment = await deployer.deploy(stack('managed').descriptor, {
      customerId: 'no-cluster-access',
      repository: 'some/repo',
      ownerId: 'run-managed',
      repoRoot: process.cwd(),
    })

    expect(environment.bindings['kubernetes.target']).toBe('managed')
    expect(environment.bindings['kubernetes.fidelityCaveat']).toContain('may differ')
  })

  it('creates opted-in cluster resources without adoption and deletes them before the namespace', async () => {
    const client = new FakeKubernetesClient()
    client.rendered = [{
      apiVersion: 'scheduling.k8s.io/v1',
      kind: 'PriorityClass',
      metadata: { name: 'factory-e2e-low-priority' },
      value: -10,
      globalDefault: false,
    }]
    const connections = new KubernetesConnectionRegistry({
      connections: [{
        id: 'opted-cluster',
        customers: ['customer-a'],
        repositories: ['AgentWorkforce/factory'],
        credential: { kind: 'kubeconfig', secretRef: 'env:OPTED' },
        allowClusterScopedResources: true,
        allowedClusterScopedKinds: ['PriorityClass'],
      }],
    }, credentialResolver())
    const provider = new KubernetesEnvironmentProvider({
      registry: connections, client, randomId: () => 'clustered',
      secretResolver: { resolve: async () => 'secret' },
    })
    const descriptor = { ...stack().descriptor, allowClusterScopedResources: true }

    const environment = await provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run',
      stack: { descriptor, repoRoot: process.cwd() },
    })

    const key = 'scheduling.k8s.io/v1/PriorityClass/factory-e2e-low-priority'
    expect(client.clusterResources.get(key)?.metadata?.labels?.[KUBERNETES_ENVIRONMENT_ID_LABEL])
      .toBe(environment.id)
    expect(client.namespaces.get(environment.id)?.metadata?.annotations?.[KUBERNETES_CLUSTER_RESOURCES_ANNOTATION])
      .toContain('factory-e2e-low-priority')

    await provider.destroy(environment.id)
    expect(client.clusterDeletes).toEqual([key])
    expect(client.namespaces.has(environment.id)).toBe(false)
  })

  it('cleans up a cluster resource when the create result is ambiguous', async () => {
    const client = new FakeKubernetesClient()
    const priorityClass: KubernetesResource = {
      apiVersion: 'scheduling.k8s.io/v1',
      kind: 'PriorityClass',
      metadata: { name: 'factory-e2e-ambiguous-create' },
      value: -10,
      globalDefault: false,
    }
    client.rendered = [priorityClass]
    client.failClusterCreateAfterPersistFor = clusterResourceKey(priorityClass)
    const connections = new KubernetesConnectionRegistry({
      connections: [{
        id: 'opted-cluster', credential: { kind: 'kubeconfig', secretRef: 'env:OPTED' },
        allowClusterScopedResources: true, allowedClusterScopedKinds: ['PriorityClass'],
      }],
    }, credentialResolver())
    const provider = new KubernetesEnvironmentProvider({
      registry: connections, client, randomId: () => 'ambiguous',
      secretResolver: { resolve: async () => 'secret' },
    })

    await expect(provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run',
      stack: {
        descriptor: { ...stack().descriptor, allowClusterScopedResources: true },
        repoRoot: process.cwd(),
      },
    })).rejects.toThrow('connection dropped after create')

    expect(client.clusterResources.size).toBe(0)
    expect(client.namespaces.size).toBe(0)
  })

  it('deletes owned cluster resources even when the namespace disappeared externally', async () => {
    const client = new FakeKubernetesClient()
    client.rendered = [{
      apiVersion: 'scheduling.k8s.io/v1', kind: 'PriorityClass',
      metadata: { name: 'factory-e2e-external-namespace-delete' },
      value: -10, globalDefault: false,
    }]
    const connections = new KubernetesConnectionRegistry({
      connections: [{
        id: 'opted-cluster', credential: { kind: 'kubeconfig', secretRef: 'env:OPTED' },
        allowClusterScopedResources: true, allowedClusterScopedKinds: ['PriorityClass'],
      }],
    }, credentialResolver())
    const provider = new KubernetesEnvironmentProvider({
      registry: connections, client, randomId: () => 'external-delete',
      secretResolver: { resolve: async () => 'secret' },
    })
    const environment = await provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run',
      stack: {
        descriptor: { ...stack().descriptor, allowClusterScopedResources: true },
        repoRoot: process.cwd(),
      },
    })
    client.namespaces.delete(environment.id)

    await provider.destroy(environment.id)

    expect(client.clusterResources.size).toBe(0)
    expect(await provider.status(environment.id)).toBe('destroyed')
  })

  it('never adopts an existing cluster-scoped resource', async () => {
    const client = new FakeKubernetesClient()
    const priorityClass: KubernetesResource = {
      apiVersion: 'scheduling.k8s.io/v1', kind: 'PriorityClass',
      metadata: { name: 'customer-owned-priority', labels: { purpose: 'production' } },
      value: 1000,
    }
    client.rendered = [priorityClass]
    client.clusterResources.set(clusterResourceKey(priorityClass), structuredClone(priorityClass))
    const connections = new KubernetesConnectionRegistry({
      connections: [{
        id: 'opted-cluster', credential: { kind: 'kubeconfig', secretRef: 'env:OPTED' },
        allowClusterScopedResources: true, allowedClusterScopedKinds: ['PriorityClass'],
      }],
    }, credentialResolver())
    const provider = new KubernetesEnvironmentProvider({
      registry: connections, client, randomId: () => 'cluster-collision',
      secretResolver: { resolve: async () => 'secret' },
    })

    await expect(provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run',
      stack: {
        descriptor: { ...stack().descriptor, allowClusterScopedResources: true },
        repoRoot: process.cwd(),
      },
    })).rejects.toThrow('AlreadyExists')

    expect(client.clusterDeletes).toEqual([])
    expect(client.clusterResources.get(clusterResourceKey(priorityClass))?.metadata?.labels)
      .toEqual({ purpose: 'production' })
    expect(client.namespaces.size).toBe(0)
  })

  it('stops partial port-forwards and tears down when endpoint resolution fails', async () => {
    const client = new FakeKubernetesClient()
    client.failForwardFor = 'missing-api'
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(), client, randomId: () => 'endpoint-failure',
      secretResolver: { resolve: async () => 'secret' },
    })
    const descriptor = {
      ...stack().descriptor,
      endpoints: [
        { name: 'api', service: 'api', port: 8080 },
        { name: 'missing', service: 'missing-api', port: 8080 },
      ],
    }

    await expect(provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run',
      stack: { descriptor, repoRoot: process.cwd() },
    })).rejects.toThrow(/port-forward failed/)

    expect(client.stoppedForwards).toEqual(['api'])
    expect(client.namespaces.size).toBe(0)
  })

  it('never adopts or deletes an existing namespace when a generated name collides', async () => {
    const client = new FakeKubernetesClient()
    const collidingName = 'factory-factory-collision'
    client.namespaces.set(collidingName, {
      apiVersion: 'v1', kind: 'Namespace',
      metadata: { name: collidingName, labels: { purpose: 'customer-owned' } },
    })
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(), client, randomId: () => 'collision',
      secretResolver: { resolve: async () => 'secret' },
    })

    await expect(provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run', stack: stack(),
    })).rejects.toThrow('AlreadyExists')

    expect(client.deletes).toEqual([])
    expect(client.namespaces.get(collidingName)?.metadata?.labels).toEqual({ purpose: 'customer-owned' })
  })

  it('rejects cluster-scoped and cross-namespace customer resources before creating a namespace', async () => {
    for (const unsafe of [
      { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: { name: 'admin' }, rules: [] },
      { apiVersion: 'v1', kind: 'Service', metadata: { name: 'prod', namespace: 'payments-prod' } },
      {
        apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', metadata: { name: 'allow-all' },
        spec: { podSelector: {}, policyTypes: ['Egress'], egress: [{}] },
      },
      {
        apiVersion: 'v1', kind: 'Pod', metadata: { name: 'host-breakout' },
        spec: { hostNetwork: true, containers: [{ name: 'shell', image: 'busybox' }] },
      },
      {
        apiVersion: 'v1', kind: 'Secret', metadata: { name: 'inline-secret' },
        stringData: { password: 'not-allowed-inline' },
      },
      {
        apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'Role', metadata: { name: 'token-mint' },
        rules: [{ apiGroups: [''], resources: ['serviceaccounts/token'], verbs: ['create'] }],
      },
      {
        apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: { name: 'deployer-access' },
        roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: 'factory-guardrail-deployer' },
        subjects: [{ kind: 'ServiceAccount', name: 'app', namespace: 'factory-factory-unsafe0' }],
      },
      {
        apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: { name: 'global-user-access' },
        roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: 'app' },
        subjects: [{ kind: 'Group', name: 'system:authenticated' }],
      },
      {
        apiVersion: 'v1', kind: 'Pod', metadata: { name: 'deployer-token' },
        spec: {
          serviceAccountName: 'factory-guardrail-deployer',
          automountServiceAccountToken: true,
          containers: [{ name: 'shell', image: 'busybox' }],
        },
      },
    ]) {
      const client = new FakeKubernetesClient()
      client.rendered = [unsafe]
      const provider = new KubernetesEnvironmentProvider({
        registry: registry(), client, randomId: () => `unsafe${client.deletes.length}`,
        secretResolver: { resolve: async () => 'secret' },
      })
      await expect(provider.provision({
        customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run', stack: stack(),
      })).rejects.toThrow(/denied|only generated namespace|may not add egress|bypasses namespace isolation|deployer service account|may bind|escalate privileges/)
      expect(client.deletes).toHaveLength(0)
      expect(client.namespaces.size).toBe(0)
    }
  })

  it('refuses teardown when namespace ownership labels do not match', async () => {
    const client = new FakeKubernetesClient()
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(), client, randomId: () => 'identity',
      secretResolver: { resolve: async () => 'secret' },
    })
    const environment = await provider.provision({
      customerId: 'customer-a', repository: 'AgentWorkforce/factory', ownerId: 'run', stack: stack(),
    })
    const namespace = client.namespaces.get(environment.id)
    if (!namespace?.metadata?.labels) throw new Error('fixture namespace missing')
    namespace.metadata.labels[KUBERNETES_ENVIRONMENT_ID_LABEL] = 'some-other-environment'

    await expect(provider.destroy(environment.id)).rejects.toThrow(/ownership identity/)
    expect(client.namespaces.has(environment.id)).toBe(true)
  })

  it('reaps expired owned namespaces and leaves production or mismatched namespaces untouched', async () => {
    const client = new FakeKubernetesClient()
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(), client,
      now: () => new Date('2026-07-21T12:00:00.000Z'),
      secretResolver: { resolve: async () => 'secret' },
    })
    client.namespaces.set('factory-orphan-deadbeef', {
      apiVersion: 'v1', kind: 'Namespace',
      metadata: {
        name: 'factory-orphan-deadbeef',
        labels: {
          [KUBERNETES_MANAGED_BY_LABEL]: 'factory',
          [KUBERNETES_ENVIRONMENT_ID_LABEL]: 'factory-orphan-deadbeef',
          [KUBERNETES_EXPIRES_AT_LABEL]: String(Date.parse('2026-07-21T11:59:00.000Z') / 1_000),
        },
        annotations: {
          [KUBERNETES_CONNECTION_ID_ANNOTATION]: 'customer-eks',
          [KUBERNETES_CLUSTER_RESOURCES_ANNOTATION]: JSON.stringify([{
            apiVersion: 'scheduling.k8s.io/v1', kind: 'PriorityClass', metadata: { name: 'orphan-priority' },
          }]),
        },
      },
    })
    client.clusterResources.set('scheduling.k8s.io/v1/PriorityClass/orphan-priority', {
      apiVersion: 'scheduling.k8s.io/v1', kind: 'PriorityClass',
      metadata: {
        name: 'orphan-priority',
        labels: { [KUBERNETES_ENVIRONMENT_ID_LABEL]: 'factory-orphan-deadbeef' },
      },
    })
    client.namespaces.set('payments-prod', {
      apiVersion: 'v1', kind: 'Namespace',
      metadata: { name: 'payments-prod', labels: { purpose: 'production' } },
    })
    client.namespaces.set('factory-label-spoof', {
      apiVersion: 'v1', kind: 'Namespace',
      metadata: {
        name: 'factory-label-spoof',
        labels: {
          [KUBERNETES_MANAGED_BY_LABEL]: 'factory',
          [KUBERNETES_ENVIRONMENT_ID_LABEL]: 'different-id',
          [KUBERNETES_EXPIRES_AT_LABEL]: '1',
        },
      },
    })

    const report = await provider.reap()

    expect(report.reaped).toContainEqual({
      id: 'factory-orphan-deadbeef', connectionId: 'customer-eks', reason: 'ttl-expired',
    })
    expect(report.skipped).toContainEqual({
      id: 'factory-label-spoof', connectionId: 'customer-eks', reason: 'ownership identity mismatch',
    })
    expect(client.namespaces.has('factory-orphan-deadbeef')).toBe(false)
    expect(client.clusterResources.has('scheduling.k8s.io/v1/PriorityClass/orphan-priority')).toBe(false)
    expect(client.namespaces.has('factory-label-spoof')).toBe(true)
    expect(client.namespaces.has('payments-prod')).toBe(true)
  })

  it('continues reaping reachable clusters when another credential is unavailable', async () => {
    const client = new FakeKubernetesClient()
    const resolver: KubernetesCredentialResolver = {
      resolve: vi.fn(async (reference: string) => {
        if (reference === 'env:MANAGED_KUBECONFIG') throw new Error('secret mount unavailable')
        return { kubeconfigPath: '/resolved/customer' }
      }),
    }
    const provider = new KubernetesEnvironmentProvider({
      registry: registry(resolver), client,
      now: () => new Date('2026-07-21T12:00:00.000Z'),
      secretResolver: { resolve: async () => 'secret' },
    })
    client.namespaces.set('factory-reachable-orphan', {
      apiVersion: 'v1', kind: 'Namespace',
      metadata: {
        name: 'factory-reachable-orphan',
        labels: {
          [KUBERNETES_MANAGED_BY_LABEL]: 'factory',
          [KUBERNETES_ENVIRONMENT_ID_LABEL]: 'factory-reachable-orphan',
          [KUBERNETES_EXPIRES_AT_LABEL]: '1',
        },
        annotations: { [KUBERNETES_CONNECTION_ID_ANNOTATION]: 'customer-eks' },
      },
    })

    const report = await provider.reap()

    expect(report.reaped).toContainEqual({
      id: 'factory-reachable-orphan', connectionId: 'customer-eks', reason: 'ttl-expired',
    })
    expect(report.skipped).toContainEqual({
      connectionId: 'managed-eks',
      reason: 'credential resolution failed: secret mount unavailable',
    })
  })
})
