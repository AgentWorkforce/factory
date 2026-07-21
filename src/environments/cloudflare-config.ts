import { z } from 'zod'

const resourceReferenceSchema = z.string().trim().regex(
  /^Resource\.[A-Za-z][A-Za-z0-9_]*$/u,
  'must be a Resource.<name> secret reference',
)

export const CloudflareEnvironmentLimitsSchema = z.object({
  maxActiveEnvironments: z.number().int().min(1).max(100).default(5),
  maxWorkersPerEnvironment: z.number().int().min(1).max(100).default(20),
  maxContainersPerEnvironment: z.number().int().min(0).max(50).default(5),
  workerCpuMs: z.number().int().min(1).max(1_000).default(50),
  workerSubrequests: z.number().int().min(1).max(1_000).default(50),
}).strict().default({})

/**
 * Cloudflare credentials are references, never credential values. The runtime
 * adapter resolves these from the host's injected Resource.* secret map.
 */
export const CloudflareEnvironmentConfigSchema = z.object({
  accountId: resourceReferenceSchema,
  apiToken: resourceReferenceSchema,
  namespacePrefix: z.string().trim().min(1).max(24).regex(
    /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u,
    'must be a lowercase DNS label',
  ).default('factory'),
  ttlMs: z.number().int().min(1_000).max(24 * 60 * 60_000).default(15 * 60_000),
  minTtlMs: z.number().int().min(1_000).max(24 * 60 * 60_000).default(1_000),
  maxTtlMs: z.number().int().min(1_000).max(7 * 24 * 60 * 60_000).default(24 * 60 * 60_000),
  limits: CloudflareEnvironmentLimitsSchema,
}).strict().superRefine((config, context) => {
  if (config.minTtlMs > config.maxTtlMs) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['minTtlMs'],
      message: 'must be less than or equal to maxTtlMs',
    })
  }
  if (config.ttlMs < config.minTtlMs || config.ttlMs > config.maxTtlMs) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['ttlMs'],
      message: 'must be between minTtlMs and maxTtlMs',
    })
  }
})

export type CloudflareEnvironmentConfig = z.infer<typeof CloudflareEnvironmentConfigSchema>
export type CloudflareEnvironmentLimits = z.infer<typeof CloudflareEnvironmentLimitsSchema>
