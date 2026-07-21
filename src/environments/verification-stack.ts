import { resolve } from 'node:path'

import {
  resolveVerificationStackAsset,
  resolveVerificationStackDescriptor,
  type LoadedVerificationStack,
} from './verification-stack-descriptor.js'

export interface ResolvedVerificationStack {
  descriptorPath: string
  repositoryPath: string
  loaded: LoadedVerificationStack
  environmentTtlMs: number
  e2e: {
    image: string
    command: string
    args: string[]
    env: Record<string, string>
    timeoutMs: number
  }
  load: {
    profilePath: string
    timeoutMs: number
    k6Image?: string
  }
  timeouts: {
    overallMs: number
    teardownMs: number
  }
}

/**
 * Resolve the repository-owned deployment descriptor and require the live-gate
 * section used by the provision → deploy → E2E → load pipeline.
 */
export async function loadVerificationGateStack(
  repositoryPath: string,
  descriptorPath = '.factory/verification-stack.yaml',
): Promise<ResolvedVerificationStack> {
  const root = resolve(repositoryPath)
  const loaded = await resolveVerificationStackDescriptor({
    repoPath: root,
    descriptorPath,
  })
  const gate = loaded.descriptor.verification
  if (!gate) {
    throw new Error(
      `Verification stack ${loaded.descriptorPath} does not declare the required verification section`,
    )
  }
  if (loaded.descriptor.endpoints.length === 0) {
    throw new Error(
      `Verification stack ${loaded.descriptorPath} must expose at least one endpoint for E2E and load stages`,
    )
  }

  return {
    descriptorPath: loaded.descriptorPath,
    repositoryPath: root,
    loaded,
    environmentTtlMs: gate.environmentTtlSeconds * 1_000,
    e2e: {
      image: gate.e2e.image,
      command: gate.e2e.command,
      args: gate.e2e.args,
      env: gate.e2e.env,
      timeoutMs: gate.e2e.timeoutSeconds * 1_000,
    },
    load: {
      profilePath: resolveVerificationStackAsset(loaded.rootDir, gate.load.profile),
      timeoutMs: gate.load.timeoutSeconds * 1_000,
      ...(gate.load.k6Image ? { k6Image: gate.load.k6Image } : {}),
    },
    timeouts: {
      overallMs: gate.overallTimeoutSeconds * 1_000,
      teardownMs: gate.teardownTimeoutSeconds * 1_000,
    },
  }
}
