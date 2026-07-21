import { readFile } from 'node:fs/promises'
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path'

import { parse as parseYaml } from 'yaml'
import { z } from 'zod'

import { ProcessCommandRunner, type CommandRunner } from './kubernetes-command.js'

export const DEFAULT_VERIFICATION_STACK_PATH = '.factory/verification-stack.yaml'
export const VERIFICATION_STACK_API_VERSION = 'factory.agentworkforce.dev/v1alpha1'
export const VERIFICATION_STACK_KIND = 'VerificationStack'
/** File URL that works from both the source checkout and the published dist. */
export const VERIFICATION_STACK_JSON_SCHEMA_URL = new URL(
  '../../schemas/verification-stack.schema.json',
  import.meta.url,
)

const dnsNameSchema = z.string().trim().min(1).max(63).regex(
  /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u,
  'must be a lowercase DNS label (letters, numbers, and hyphens)',
)
const relativePathSchema = z.string().trim().min(1).refine(
  (value) => !isAbsolute(value) && !value.split(/[\\/]/u).includes('..'),
  'must be a path inside the repository',
)
const durationFields = {
  timeoutSeconds: z.number().int().min(1).max(3_600).default(120),
  intervalSeconds: z.number().min(0.1).max(60).default(2),
}

const httpProbeSchema = z.object({
  type: z.literal('http'),
  service: dnsNameSchema.optional(),
  port: z.number().int().min(1).max(65_535),
  path: z.string().startsWith('/').default('/'),
  scheme: z.enum(['http', 'https']).default('http'),
  expectedStatuses: z.array(z.number().int().min(100).max(599)).min(1).default([200]),
  ...durationFields,
}).strict()

const tcpProbeSchema = z.object({
  type: z.literal('tcp'),
  service: dnsNameSchema.optional(),
  port: z.number().int().min(1).max(65_535),
  ...durationFields,
}).strict()

const execProbeSchema = z.object({
  type: z.literal('exec'),
  target: z.string().trim().min(1).optional(),
  container: dnsNameSchema.optional(),
  command: z.array(z.string().min(1)).min(1),
  ...durationFields,
}).strict()

export const VerificationProbeSchema = z.discriminatedUnion('type', [
  httpProbeSchema,
  tcpProbeSchema,
  execProbeSchema,
])

const helmSourceSchema = z.object({
  type: z.literal('helm'),
  chart: z.string().trim().min(1),
  release: dnsNameSchema.optional(),
  valuesFiles: z.array(relativePathSchema).default([]),
}).strict()

const kustomizeSourceSchema = z.object({
  type: z.literal('kustomize'),
  path: relativePathSchema,
}).strict()

const manifestsSourceSchema = z.object({
  type: z.literal('manifests'),
  paths: z.array(relativePathSchema).min(1),
}).strict()

const composeSourceSchema = z.object({
  type: z.literal('docker-compose'),
  path: relativePathSchema,
}).strict()

export const VerificationStackSourceSchema = z.discriminatedUnion('type', [
  helmSourceSchema,
  kustomizeSourceSchema,
  manifestsSourceSchema,
  composeSourceSchema,
])

const referenceSchema = z.object({
  ref: z.string().trim().min(1),
  optional: z.boolean().default(false),
}).strict()

const referenceDataKeySchema = z.string().min(1).max(253).regex(
  /^[A-Za-z0-9._-]+$/u,
  'must contain only letters, numbers, dots, underscores, and hyphens',
)

const materializedReferenceSchema = z.object({
  name: dnsNameSchema,
  data: z.record(referenceDataKeySchema, referenceSchema).refine(
    (data) => Object.keys(data).length > 0,
    'must declare at least one referenced key',
  ),
}).strict()

const serviceSchema = z.object({
  name: dnsNameSchema,
  workload: z.object({
    kind: z.enum(['deployment', 'statefulset', 'daemonset']),
    name: dnsNameSchema.optional(),
  }).strict(),
  readiness: VerificationProbeSchema,
  health: VerificationProbeSchema.optional(),
}).strict()

const execSeedSchema = z.object({
  type: z.literal('exec'),
  name: dnsNameSchema,
  service: dnsNameSchema,
  container: dnsNameSchema.optional(),
  command: z.array(z.string().min(1)).min(1),
  timeoutSeconds: z.number().int().min(1).max(3_600).default(120),
}).strict()

const jobSeedSchema = z.object({
  type: z.literal('job'),
  name: dnsNameSchema,
  manifest: relativePathSchema,
  job: dnsNameSchema,
  timeoutSeconds: z.number().int().min(1).max(3_600).default(120),
}).strict()

const seedSchema = z.discriminatedUnion('type', [execSeedSchema, jobSeedSchema])

const endpointSchema = z.object({
  name: dnsNameSchema,
  service: dnsNameSchema,
  port: z.number().int().min(1).max(65_535),
  protocol: z.enum(['http', 'https']).default('http'),
  path: z.string().startsWith('/').default('/'),
}).strict()

const verificationGateSchema = z.object({
  environmentTtlSeconds: z.number().int().min(1).max(24 * 60 * 60).default(15 * 60),
  e2e: z.object({
    command: z.string().trim().min(1),
    args: z.array(z.string()).default([]),
    env: z.record(z.string()).default({}),
    timeoutSeconds: z.number().int().min(1).max(60 * 60).default(5 * 60),
  }).strict(),
  load: z.object({
    profile: relativePathSchema,
    timeoutSeconds: z.number().int().min(1).max(60 * 60).default(5 * 60),
    k6Image: z.string().trim().min(1).optional(),
  }).strict(),
  overallTimeoutSeconds: z.number().int().min(1).max(2 * 60 * 60).default(15 * 60),
  teardownTimeoutSeconds: z.number().int().min(1).max(30 * 60).default(2 * 60),
}).strict()

export const VerificationStackDescriptorSchema = z.object({
  apiVersion: z.literal(VERIFICATION_STACK_API_VERSION),
  kind: z.literal(VERIFICATION_STACK_KIND),
  name: dnsNameSchema,
  source: VerificationStackSourceSchema,
  secrets: z.array(materializedReferenceSchema).default([]),
  config: z.array(materializedReferenceSchema).default([]),
  services: z.array(serviceSchema).min(1),
  seeds: z.array(seedSchema).default([]),
  endpoints: z.array(endpointSchema).default([]),
  /** Required when this descriptor participates in Factory's live merge gate. */
  verification: verificationGateSchema.optional(),
}).strict().superRefine((descriptor, context) => {
  uniqueNames(descriptor.services, 'services', context)
  uniqueNames(descriptor.secrets, 'secrets', context)
  uniqueNames(descriptor.config, 'config', context)
  uniqueNames(descriptor.seeds, 'seeds', context)
  uniqueNames(descriptor.endpoints, 'endpoints', context)

  const services = new Set(descriptor.services.map((service) => service.name))
  descriptor.seeds.forEach((seed, index) => {
    if (seed.type === 'exec' && !services.has(seed.service)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['seeds', index, 'service'],
        message: `references undeclared service ${JSON.stringify(seed.service)}`,
      })
    }
  })
  descriptor.endpoints.forEach((endpoint, index) => {
    if (!services.has(endpoint.service)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['endpoints', index, 'service'],
        message: `references undeclared service ${JSON.stringify(endpoint.service)}`,
      })
    }
  })
})

export type VerificationProbe = z.infer<typeof VerificationProbeSchema>
export type VerificationStackSource = z.infer<typeof VerificationStackSourceSchema>
export type VerificationStackDescriptor = z.infer<typeof VerificationStackDescriptorSchema>
export type VerificationStackService = VerificationStackDescriptor['services'][number]
export type VerificationStackSeed = VerificationStackDescriptor['seeds'][number]
export type VerificationStackEndpoint = VerificationStackDescriptor['endpoints'][number]
export type VerificationStackReferenceGroup = VerificationStackDescriptor['secrets'][number]
export type VerificationGateDescriptor = NonNullable<VerificationStackDescriptor['verification']>

export interface LoadedVerificationStack {
  descriptor: VerificationStackDescriptor
  descriptorPath: string
  rootDir: string
  ref?: string
}

export interface ResolveVerificationStackOptions {
  repoPath: string
  ref?: string
  descriptorPath?: string
  commandRunner?: CommandRunner
}

export class VerificationStackDescriptorError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'VerificationStackDescriptorError'
  }
}

export function loadVerificationStack(input: unknown, source = '<input>'): VerificationStackDescriptor {
  const parsed = VerificationStackDescriptorSchema.safeParse(input)
  if (parsed.success) return parsed.data

  const details = parsed.error.issues.map((issue) => {
    const path = issue.path.length ? issue.path.join('.') : '<root>'
    return `- ${path}: ${issue.message}`
  }).join('\n')
  throw new VerificationStackDescriptorError(
    `Invalid verification-stack descriptor ${source}:\n${details}`,
    { cause: parsed.error },
  )
}

export function parseVerificationStack(text: string, source = '<input>'): VerificationStackDescriptor {
  let input: unknown
  try {
    input = parseYaml(text)
  } catch (cause) {
    throw new VerificationStackDescriptorError(
      `Could not parse verification-stack descriptor ${source}: ${errorMessage(cause)}`,
      { cause },
    )
  }
  return loadVerificationStack(input, source)
}

export async function loadVerificationStackFile(filePath: string): Promise<LoadedVerificationStack> {
  const absolutePath = resolve(filePath)
  let text: string
  try {
    text = await readFile(absolutePath, 'utf8')
  } catch (cause) {
    throw new VerificationStackDescriptorError(
      `Could not read verification-stack descriptor ${absolutePath}: ${errorMessage(cause)}`,
      { cause },
    )
  }
  return {
    descriptor: parseVerificationStack(text, absolutePath),
    descriptorPath: absolutePath,
    rootDir: dirname(absolutePath),
  }
}

/** Resolve the default or overridden descriptor from a checkout or an arbitrary Git ref. */
export async function resolveVerificationStackDescriptor(
  options: ResolveVerificationStackOptions,
): Promise<LoadedVerificationStack> {
  const repoPath = resolve(options.repoPath)
  const descriptorPath = validateDescriptorPath(options.descriptorPath ?? DEFAULT_VERIFICATION_STACK_PATH)

  if (!options.ref) {
    const absolutePath = resolveInside(repoPath, descriptorPath)
    const loaded = await loadVerificationStackFile(absolutePath)
    return { ...loaded, rootDir: repoPath, descriptorPath }
  }

  const runner = options.commandRunner ?? new ProcessCommandRunner()
  let commit: string
  try {
    const result = await runner.run('git', [
      '-C', repoPath, 'rev-parse', '--verify', `${options.ref}^{commit}`,
    ])
    commit = result.stdout.trim()
  } catch (cause) {
    throw new VerificationStackDescriptorError(
      `Could not resolve verification-stack Git ref ${JSON.stringify(options.ref)} in ${repoPath}: ${errorMessage(cause)}`,
      { cause },
    )
  }

  try {
    const result = await runner.run('git', ['-C', repoPath, 'show', `${commit}:${descriptorPath}`])
    return {
      descriptor: parseVerificationStack(result.stdout, `${options.ref}:${descriptorPath}`),
      descriptorPath,
      rootDir: repoPath,
      ref: commit,
    }
  } catch (cause) {
    if (cause instanceof VerificationStackDescriptorError) throw cause
    throw new VerificationStackDescriptorError(
      `Could not read verification-stack descriptor ${descriptorPath} at ${JSON.stringify(options.ref)}: ${errorMessage(cause)}`,
      { cause },
    )
  }
}

function uniqueNames(
  values: readonly { name: string }[],
  field: string,
  context: z.RefinementCtx,
): void {
  const seen = new Set<string>()
  values.forEach((value, index) => {
    if (seen.has(value.name)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: [field, index, 'name'],
        message: `duplicate name ${JSON.stringify(value.name)}`,
      })
    }
    seen.add(value.name)
  })
}

function validateDescriptorPath(value: string): string {
  if (!value || isAbsolute(value) || value.split(/[\\/]/u).includes('..')) {
    throw new VerificationStackDescriptorError(
      `verification-stack descriptor path must stay inside the repository: ${JSON.stringify(value)}`,
    )
  }
  return value.replaceAll('\\', '/')
}

export function resolveVerificationStackAsset(rootDir: string, assetPath: string): string {
  return resolveInside(rootDir, assetPath)
}

function resolveInside(root: string, child: string): string {
  const absolute = resolve(root, child)
  const fromRoot = relative(root, absolute)
  if (fromRoot === '..' || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) {
    throw new VerificationStackDescriptorError(
      `verification-stack asset path escapes the repository: ${JSON.stringify(child)}`,
    )
  }
  return absolute
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
