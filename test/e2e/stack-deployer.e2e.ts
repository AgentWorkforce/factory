import assert from 'node:assert/strict'
import { resolve } from 'node:path'

import {
  KubernetesEnvironmentProvider,
  ProcessCommandRunner,
  StackDeploymentError,
  VerificationStackDeployer,
  resolveVerificationStackDescriptor,
  type StackDeployment,
  type VerificationStackDescriptor,
} from '../../src/index.js'

const root = resolve(import.meta.dirname, '../..')
const context = process.env.FACTORY_E2E_KUBE_CONTEXT
const kubeconfig = process.env.KUBECONFIG
const suffix = `${process.pid}-${Date.now().toString(36)}`
const provider = new KubernetesEnvironmentProvider({
  context,
  kubeconfig,
  namespacePrefix: 'factory-stack-e2e',
  defaultTtl: 10 * 60_000,
})
const runner = new ProcessCommandRunner()
const deployer = new VerificationStackDeployer({
  referenceResolver: {
    resolve: async (reference) => {
      if (reference === 'fixture://postgres/password') return 'factory-e2e-password'
      return undefined
    },
  },
})

let successEnvironmentId: string | undefined
let failureEnvironmentId: string | undefined
let successDeployment: StackDeployment | undefined

try {
  const loaded = await resolveVerificationStackDescriptor({
    repoPath: root,
    descriptorPath: 'test/fixtures/verification-stack/verification-stack.yaml',
  })

  const successEnvironment = await provider.provision({ id: `healthy-${suffix}` })
  successEnvironmentId = successEnvironment.id
  assert.equal(await provider.status(successEnvironment.id), 'ready')

  successDeployment = await deployer.deploy(loaded, successEnvironment)
  assert.deepEqual(Object.keys(successDeployment.endpoints), ['web'])
  for (const [name, url] of Object.entries(successDeployment.endpoints)) {
    const response = await fetch(url)
    assert.equal(response.status, 200, `${name} returned HTTP ${response.status}`)
    assert.match(await response.text(), /healthy/u, `${name} did not return its health body`)
  }

  const namespace = successEnvironment.namespace!
  const seeded = await runner.run('kubectl', [
    ...(kubeconfig ? ['--kubeconfig', kubeconfig] : []),
    ...(context ? ['--context', context] : []),
    '--namespace', namespace,
    'exec', 'deployment/postgres', '--',
    'psql', '-U', 'factory', '-d', 'factory', '-tAc',
    'SELECT value FROM verification_seed WHERE id = 1',
  ], { timeoutMs: 30_000 })
  assert.equal(seeded.stdout.trim(), 'ran', 'the declared Postgres seed step did not run')

  const failureEnvironment = await provider.provision({ id: `unready-${suffix}` })
  failureEnvironmentId = failureEnvironment.id
  const impossible = structuredClone(loaded.descriptor) as VerificationStackDescriptor
  const web = impossible.services.find((service) => service.name === 'web')!
  web.readiness = {
    type: 'http',
    port: 5999,
    path: '/never-ready',
    scheme: 'http',
    expectedStatuses: [200],
    timeoutSeconds: 4,
    intervalSeconds: 0.5,
  }
  impossible.seeds = []
  impossible.endpoints = []
  const started = Date.now()
  let readinessFailure: unknown
  try {
    await deployer.deploy({ ...loaded, descriptor: impossible }, failureEnvironment)
  } catch (error) {
    readinessFailure = error
  }
  const elapsed = Date.now() - started
  assert(readinessFailure instanceof StackDeploymentError, 'unready stack unexpectedly deployed')
  assert.equal(readinessFailure.stage, 'readiness')
  assert.equal(readinessFailure.service, 'web')
  assert.match(readinessFailure.message, /Service web readiness probe never became ready within 4s/u)
  assert(elapsed < 15_000, `unready stack failure was not bounded (elapsed ${elapsed}ms)`)

  process.stdout.write(
    `stack-deployer e2e passed: ${Object.keys(successDeployment.endpoints).length} endpoint(s), seed verified, bounded failure in ${elapsed}ms\n`,
  )
} finally {
  await successDeployment?.dispose()
  if (failureEnvironmentId) await provider.destroy(failureEnvironmentId)
  if (successEnvironmentId) await provider.destroy(successEnvironmentId)
}
