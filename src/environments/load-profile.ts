import { readFile } from 'node:fs/promises'

import { parse as parseYaml } from 'yaml'
import { z } from 'zod'

const duration = z.string().trim().regex(
  /^\d+(?:\.\d+)?(?:ms|s|m|h)$/u,
  'must be a k6 duration such as 500ms, 30s, 5m, or 1h',
).refine((value) => !/^0+(?:\.0+)?(?:ms|s|m|h)$/u.test(value), 'must be greater than zero')

const positiveFinite = z.number().finite().positive()
const nonNegativeFinite = z.number().finite().nonnegative()

export const LoadThresholdsSchema = z.object({
  maxP95LatencyMs: positiveFinite.optional(),
  maxP99LatencyMs: positiveFinite.optional(),
  maxErrorRate: z.number().finite().min(0).max(1).optional(),
  minThroughputRps: nonNegativeFinite.optional(),
}).strict().refine(
  (thresholds) => Object.values(thresholds).some((value) => value !== undefined),
  { message: 'at least one SLO threshold is required' },
)

export type LoadThresholds = z.infer<typeof LoadThresholdsSchema>

export const LoadTargetSchema = z.object({
  name: z.string().trim().min(1).max(80),
  endpoint: z.string().trim().min(1).optional(),
  url: z.string().url().optional(),
  path: z.string().optional(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS']).default('GET'),
  headers: z.record(z.string()).optional(),
  body: z.unknown().optional(),
  expectedStatuses: z.array(z.number().int().min(100).max(599)).min(1).optional(),
  weight: z.number().int().positive().max(10_000).default(1),
}).strict().superRefine((target, context) => {
  if ((target.endpoint === undefined) === (target.url === undefined)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['endpoint'],
      message: 'set exactly one of endpoint or url',
    })
  }
  if (target.url !== undefined && !/^https?:\/\//u.test(target.url)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['url'],
      message: 'only http and https targets are supported',
    })
  }
})

export type LoadTarget = z.input<typeof LoadTargetSchema>
export type ResolvedLoadTargetProfile = z.output<typeof LoadTargetSchema>

export const LoadProfileSchema = z.object({
  name: z.string().trim().min(1).max(80),
  targets: z.array(LoadTargetSchema).min(1),
  vus: z.number().int().positive().max(100_000).optional(),
  maxVus: z.number().int().positive().max(100_000).optional(),
  rps: z.number().int().positive().max(1_000_000).optional(),
  duration,
  ramp: z.object({
    up: duration,
    down: duration.optional(),
  }).strict().optional(),
  thresholds: LoadThresholdsSchema,
  histogramBucketsMs: z.array(positiveFinite).min(1).max(50)
    .default([1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000]),
}).strict().superRefine((profile, context) => {
  if (profile.vus === undefined && profile.rps === undefined) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['vus'],
      message: 'set vus, rps, or both',
    })
  }
  if (profile.maxVus !== undefined && profile.rps === undefined) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['maxVus'],
      message: 'maxVus is only used with an rps profile',
    })
  }
  const preAllocatedVus = profile.rps === undefined
    ? undefined
    : (profile.vus ?? profile.rps)
  if (profile.maxVus !== undefined && preAllocatedVus !== undefined && profile.maxVus < preAllocatedVus) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['maxVus'],
      message: 'maxVus must be greater than or equal to the preallocated vus',
    })
  }
  const sortedBuckets = [...profile.histogramBucketsMs].sort((left, right) => left - right)
  if (sortedBuckets.some((bucket, index) => bucket !== profile.histogramBucketsMs[index])) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['histogramBucketsMs'],
      message: 'histogram buckets must be in ascending order',
    })
  }
  if (new Set(profile.histogramBucketsMs).size !== profile.histogramBucketsMs.length) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['histogramBucketsMs'],
      message: 'histogram buckets must be unique',
    })
  }
})

/** Declarative profile input. Schema defaults make method, weight, and histogram buckets optional. */
export type LoadProfile = z.input<typeof LoadProfileSchema>
export type ResolvedLoadProfile = z.output<typeof LoadProfileSchema>

export async function loadLoadProfile(path: string): Promise<ResolvedLoadProfile> {
  const source = await readFile(path, 'utf8')
  let value: unknown
  try {
    value = parseYaml(source)
  } catch (error) {
    throw new Error(`Could not parse load profile ${path}: ${error instanceof Error ? error.message : String(error)}`)
  }

  const parsed = LoadProfileSchema.safeParse(value)
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '<root>'}: ${issue.message}`)
      .join('; ')
    throw new Error(`Invalid load profile ${path}: ${issues}`)
  }
  return parsed.data
}

export function durationToMilliseconds(value: string): number {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/u.exec(value)
  if (!match) throw new Error(`Invalid duration: ${value}`)
  const amount = Number(match[1])
  const multiplier = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
  }[match[2] as 'ms' | 's' | 'm' | 'h']
  return amount * multiplier
}
