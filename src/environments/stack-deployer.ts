import type { Environment } from '../ports/environment.js'
import type { KubernetesEnvironmentProvider, KubernetesProvisionSpec } from './kubernetes-provider.js'
import {
  KubernetesStackDescriptorSchema,
  type KubernetesStackDescriptorInput,
} from './stack-descriptor.js'

export interface DeployKubernetesStackInput {
  customerId: string
  repository: string
  ownerId: string
  repoRoot: string
  ttl?: number
}

/**
 * Substrate-neutral callers select a descriptor; this deployer routes the
 * Kubernetes kind and preserves its explicit BYOC/managed target.
 */
export class StackDeployer {
  readonly #kubernetes: KubernetesEnvironmentProvider

  constructor(providers: { kubernetes: KubernetesEnvironmentProvider }) {
    this.#kubernetes = providers.kubernetes
  }

  async deploy(
    descriptorInput: KubernetesStackDescriptorInput,
    input: DeployKubernetesStackInput,
  ): Promise<Environment> {
    const descriptor = KubernetesStackDescriptorSchema.parse(descriptorInput)
    const spec: KubernetesProvisionSpec = {
      customerId: input.customerId,
      repository: input.repository,
      ownerId: input.ownerId,
      ...(input.ttl === undefined ? {} : { ttl: input.ttl }),
      target: descriptor.target,
      stack: {
        descriptor,
        repoRoot: input.repoRoot,
      },
    }
    return await this.#kubernetes.provision(spec)
  }
}
