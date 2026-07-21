import { readFile } from 'node:fs/promises'
import { dirname, isAbsolute, relative, resolve } from 'node:path'

import { parse as parseYaml } from 'yaml'
import { z } from 'zod'

import { durationToMilliseconds } from './load-profile.js'

const duration = z.string().trim().regex(
  /^\d+(?:\.\d+)?(?:ms|s|m|h)$/u,
  'must be a duration such as 500ms, 30s, 5m, or 1h',
).refine((value) => durationToMilliseconds(value) > 0, 'must be greater than zero')

const relativePath = z.string().trim().min(1).refine((value) => !isAbsolute(value), 'must be relative to the repository')

const EndpointSchema = z.object({
  service: z.string().trim().min(1).max(253),
  port: z.number().int().min(1).max(65_535),
  scheme: z.enum(['http', 'https']).default('http'),
  path: z.string().default(''),
  portForward: z.boolean().default(true),
}).strict()

export const VerificationStackSchema = z.object({
  apiVersion: z.literal('factory.agentworkforce.dev/v1alpha1'),
  kind: z.literal('VerificationStack'),
  provision: z.object({
    namespacePrefix: z.string().trim().min(1).max(40).default('factory-verify'),
    ttl: duration.default('15m'),
    kubeContext: z.string().trim().min(1).optional(),
  }).strict().default({}),
  deploy: z.object({
    manifests: z.array(relativePath).min(1),
    readiness: z.array(z.object({
      resource: z.string().trim().min(1),
      condition: z.string().trim().min(1).default('Available'),
      timeout: duration.default('2m'),
    }).strict()).default([]),
    endpoints: z.record(EndpointSchema).refine(
      (endpoints) => Object.keys(endpoints).length > 0,
      'at least one endpoint is required',
    ),
  }).strict(),
  e2e: z.object({
    command: z.string().trim().min(1),
    args: z.array(z.string()).default([]),
    env: z.record(z.string()).default({}),
    timeout: duration.default('5m'),
  }).strict(),
  load: z.object({
    profile: relativePath,
    timeout: duration.default('5m'),
    k6Image: z.string().trim().min(1).optional(),
  }).strict(),
  timeouts: z.object({
    overall: duration.default('15m'),
    teardown: duration.default('2m'),
  }).strict().default({}),
}).strict()

export type VerificationStack = z.output<typeof VerificationStackSchema>

export interface ResolvedVerificationStack {
  descriptorPath: string
  repositoryPath: string
  provision: {
    namespacePrefix: string
    ttlMs: number
    kubeContext?: string
  }
  deploy: {
    manifests: Array<{ path: string }>
    readiness: Array<{ resource: string; condition: string; timeoutMs: number }>
    endpoints: VerificationStack['deploy']['endpoints']
  }
  e2e: {
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

export async function loadVerificationGateStack(
  repositoryPath: string,
  descriptor = '.factory/verification-stack.yaml',
): Promise<ResolvedVerificationStack> {
  const root = resolve(repositoryPath)
  const descriptorPath = resolveWithin(root, descriptor, 'descriptor')
  let value: unknown
  try {
    value = parseYaml(await readFile(descriptorPath, 'utf8'))
  } catch (error) {
    throw new Error(`Could not read verification stack ${descriptorPath}: ${message(error)}`)
  }
  const parsed = VerificationStackSchema.safeParse(value)
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '<root>'}: ${issue.message}`)
      .join('; ')
    throw new Error(`Invalid verification stack ${descriptorPath}: ${issues}`)
  }

  const stack = parsed.data
  return {
    descriptorPath,
    repositoryPath: root,
    provision: {
      namespacePrefix: stack.provision.namespacePrefix,
      ttlMs: durationToMilliseconds(stack.provision.ttl),
      ...(stack.provision.kubeContext ? { kubeContext: stack.provision.kubeContext } : {}),
    },
    deploy: {
      manifests: stack.deploy.manifests.map((path) => ({
        path: resolveWithin(root, path, 'deploy manifest'),
      })),
      readiness: stack.deploy.readiness.map((check) => ({
        resource: check.resource,
        condition: check.condition,
        timeoutMs: durationToMilliseconds(check.timeout),
      })),
      endpoints: stack.deploy.endpoints,
    },
    e2e: {
      command: stack.e2e.command,
      args: stack.e2e.args,
      env: stack.e2e.env,
      timeoutMs: durationToMilliseconds(stack.e2e.timeout),
    },
    load: {
      profilePath: resolveWithin(root, stack.load.profile, 'load profile'),
      timeoutMs: durationToMilliseconds(stack.load.timeout),
      ...(stack.load.k6Image ? { k6Image: stack.load.k6Image } : {}),
    },
    timeouts: {
      overallMs: durationToMilliseconds(stack.timeouts.overall),
      teardownMs: durationToMilliseconds(stack.timeouts.teardown),
    },
  }
}

function resolveWithin(root: string, path: string, label: string): string {
  if (isAbsolute(path)) throw new Error(`${label} path must be relative to the repository`)
  const resolved = resolve(root, path)
  const traversal = relative(root, resolved)
  if (traversal === '..' || traversal.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`)) {
    throw new Error(`${label} path escapes the repository: ${path}`)
  }
  return resolved
}

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)
