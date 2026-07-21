import { readFile } from 'node:fs/promises'

import { parse as parseYaml } from 'yaml'
import { z } from 'zod'

const relativePathSchema = z.string().trim().min(1).superRefine((value, context) => {
  if (value.startsWith('/') || value.split(/[\\/]/u).includes('..')) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: 'must stay within the repository checkout' })
  }
})

const helmDeploymentSchema = z.object({
  strategy: z.literal('helm'),
  chart: relativePathSchema,
  release: z.string().trim().min(1).max(53).optional(),
  valuesFiles: z.array(relativePathSchema).default([]),
  values: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).default({}),
}).strict()

const kustomizeDeploymentSchema = z.object({
  strategy: z.literal('kustomize'),
  path: relativePathSchema,
}).strict()

const manifestsDeploymentSchema = z.object({
  strategy: z.literal('manifests'),
  paths: z.array(relativePathSchema).min(1),
}).strict()

export const KubernetesDeploymentSchema = z.discriminatedUnion('strategy', [
  helmDeploymentSchema,
  kustomizeDeploymentSchema,
  manifestsDeploymentSchema,
])

export const KubernetesEndpointSchema = z.object({
  name: z.string().trim().min(1).max(63),
  service: z.string().trim().min(1).max(253),
  port: z.number().int().min(1).max(65_535),
  protocol: z.enum(['http', 'https']).default('http'),
  path: z.string().startsWith('/').default('/'),
  ingressUrl: z.string().url().optional(),
}).strict()

export const ReferencedKubernetesSecretSchema = z.object({
  name: z.string().trim().min(1).max(63),
  key: z.string().trim().min(1).max(253),
  secretRef: z.string().trim().min(1).max(512),
}).strict().superRefine((secret, context) => {
  if (!/^[a-z][a-z0-9+.-]*:\S+$/iu.test(secret.secretRef)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['secretRef'],
      message: 'must be an opaque scheme-prefixed secret-manager reference',
    })
  }
  if (/-----BEGIN|password\s*=|token\s*=|\{\s*"/iu.test(secret.secretRef)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['secretRef'],
      message: 'must be an opaque secret-manager reference, never inline secret data',
    })
  }
})

/** Kubernetes slice of the repository-owned verification-stack contract. */
export const KubernetesStackDescriptorSchema = z.object({
  apiVersion: z.literal('factory.agentworkforce.dev/v1alpha1').default('factory.agentworkforce.dev/v1alpha1'),
  name: z.string().trim().min(1).max(63),
  deployKind: z.literal('kubernetes'),
  target: z.enum(['byoc', 'managed']).default('byoc'),
  deployment: KubernetesDeploymentSchema,
  endpoints: z.array(KubernetesEndpointSchema).default([]),
  secrets: z.array(ReferencedKubernetesSecretSchema).default([]),
  allowClusterScopedResources: z.boolean().default(false),
  readinessTimeoutMs: z.number().int().min(1_000).max(60 * 60_000).optional(),
}).strict()

export type KubernetesDeployment = z.output<typeof KubernetesDeploymentSchema>
export type KubernetesEndpoint = z.output<typeof KubernetesEndpointSchema>
export type ReferencedKubernetesSecret = z.output<typeof ReferencedKubernetesSecretSchema>
export type KubernetesStackDescriptor = z.output<typeof KubernetesStackDescriptorSchema>
export type KubernetesStackDescriptorInput = z.input<typeof KubernetesStackDescriptorSchema>

export async function loadKubernetesStackDescriptor(path: string): Promise<KubernetesStackDescriptor> {
  const source = await readFile(path, 'utf8')
  let value: unknown
  try {
    value = parseYaml(source)
  } catch (error) {
    throw new Error(`Could not parse verification stack descriptor ${path}: ${errorMessage(error)}`)
  }

  const parsed = KubernetesStackDescriptorSchema.safeParse(value)
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '<root>'}: ${issue.message}`)
      .join('; ')
    throw new Error(`Invalid verification stack descriptor ${path}: ${issues}`)
  }
  return parsed.data
}

const errorMessage = (error: unknown): string => error instanceof Error ? error.message : String(error)
