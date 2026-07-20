import { readFileSync } from 'node:fs'
import { hostname } from 'node:os'
import { basename, extname, join, resolve, sep } from 'node:path'

import { action, defineNode, type FleetActionContext, type FleetCapabilityValue, type FleetNodeDefinition } from '@agent-relay/fleet'
import type { AgentSpec, JsonValue, RestartPolicy, SpawnMode } from '@agent-relay/harness-driver/protocol'
import type { SpawnPtyInput } from '@agent-relay/harness-driver'
import { runScriptWorkflow, runWorkflow } from '@relayflows/core'
import type { WorkflowRunRow } from '@relayflows/core'
import { z } from 'zod'

import { loadFactoryConfig, NodeConfigSchema, type NodeConfig } from '../config/schema'
import {
  buildRelayMcpHarnessConfig,
  capabilityCli,
  resolveAgentRelayMcpCommand,
  type AgentRelayMcpCommand,
} from '../fleet/internal-fleet-client'
import type { PreviewConfig } from '../config/schema'
import type { Capability, PreviewReference } from '../ports/fleet'
import { TailscalePreviewManager, type PreviewManager } from './tailscale-preview'

export const FACTORY_NODE_CONFIG_ENV = 'FACTORY_NODE_CONFIG'
export const AGENT_RELAY_FACTORY_NODE_CONFIG_ENV = 'AGENT_RELAY_FACTORY_NODE_CONFIG'
export const DEFAULT_FACTORY_NODE_CONFIG_PATH = 'factory.node.json'

const factoryNodeCapabilities = ['spawn:claude', 'spawn:codex', 'workflow:run', 'preview:tailscale-serve'] as const
type FactoryNodeCapability = (typeof factoryNodeCapabilities)[number]

const knownFactoryNodeCapabilities = new Set<string>(factoryNodeCapabilities)
let workflowEnvLock: Promise<void> = Promise.resolve()

const restartPolicySchema: z.ZodType<RestartPolicy> = z.object({
  enabled: z.boolean().optional(),
  max_restarts: z.number().int().min(0).optional(),
  cooldown_ms: z.number().int().min(0).optional(),
  max_consecutive_failures: z.number().int().min(0).optional(),
}).passthrough()

// Mirrors the broker's accepted lifecycle modes; z.ZodType<SpawnMode> keeps the
// enum from drifting ahead of the harness-driver union.
const spawnModeSchema: z.ZodType<SpawnMode> = z.enum(['interactive', 'task_exit', 'task-exit', 'single_shot', 'single-shot'])

const spawnCapabilityInputSchema = z.object({
  name: z.string().min(1).optional(),
  agent: z.string().min(1).optional(),
  repo: z.string().min(1).optional(),
  clone_path: z.string().min(1).optional(),
  clonePath: z.string().min(1).optional(),
  cwd: z.string().min(1).optional(),
  task: z.string().optional(),
  model: z.string().min(1).optional(),
  session_ref: z.string().min(1).optional(),
  sessionRef: z.string().min(1).optional(),
  invocationId: z.string().min(1).optional(),
  invocation_id: z.string().min(1).optional(),
  channels: z.array(z.string().min(1)).optional(),
  channel: z.string().min(1).optional(),
  args: z.array(z.string()).optional(),
  restart_policy: restartPolicySchema.optional(),
  restartPolicy: restartPolicySchema.optional(),
  skip_relay_prompt: z.boolean().optional(),
  spawn_mode: spawnModeSchema.optional(),
  spawnMode: spawnModeSchema.optional(),
  exit_after_task: z.boolean().optional(),
  exitAfterTask: z.boolean().optional(),
}).passthrough().transform((input) => ({
  ...input,
  name: input.name ?? input.agent,
  clonePath: input.clonePath ?? input.clone_path,
  sessionRef: input.sessionRef ?? input.session_ref,
  invocationId: input.invocationId ?? input.invocation_id,
  channels: input.channels ?? (input.channel ? [input.channel] : undefined),
  restartPolicy: input.restartPolicy ?? input.restart_policy,
  spawnMode: input.spawnMode ?? input.spawn_mode,
  exitAfterTask: input.exitAfterTask ?? input.exit_after_task,
}))

const workflowCapabilityInputSchema = z.object({
  name: z.string().min(1).optional(),
  agent: z.string().min(1).optional(),
  repo: z.string().min(1).optional(),
  clone_path: z.string().min(1).optional(),
  clonePath: z.string().min(1).optional(),
  cwd: z.string().min(1).optional(),
  workflow: z.string().min(1),
  inputs: z.record(z.unknown()).default({}),
  invocationId: z.string().min(1).optional(),
  invocation_id: z.string().min(1).optional(),
}).passthrough().transform((input) => ({
  ...input,
  name: input.name ?? input.agent,
  clonePath: input.clonePath ?? input.clone_path,
  invocationId: input.invocationId ?? input.invocation_id,
}))

const previewReferenceSchema: z.ZodType<PreviewReference> = z.object({
  id: z.string().min(1),
  provider: z.literal('tailscale-serve'),
  namespace: z.string().min(1),
  owner: z.string().min(1),
  service: z.string().min(1),
  repo: z.string().min(1),
  url: z.string().url(),
  configuredTargetPort: z.number().int().min(1).max(65_535).optional(),
  targetPort: z.number().int().min(1).max(65_535),
  httpsPort: z.number().int().min(1).max(65_535),
  access: z.literal('tailnet'),
  lifetime: z.literal('issue'),
  createdAt: z.string().min(1),
  startCommand: z.string().min(1).optional(),
  node: z.string().min(1).optional(),
})

const previewCapabilityInputSchema = z.discriminatedUnion('operation', [
  z.object({
    operation: z.literal('start'),
    namespace: z.string().min(1),
    owner: z.string().min(1),
    issueKey: z.string().min(1),
    service: z.string().min(1),
    repo: z.string().min(1),
    targetPort: z.number().int().min(1).max(65_535),
    preferredHttpsPort: z.number().int().min(1).max(65_535).optional(),
    startCommand: z.string().min(1).optional(),
  }),
  z.object({ operation: z.literal('remove'), preview: previewReferenceSchema }),
  z.object({
    operation: z.literal('sweep'),
    namespace: z.string().min(1),
    activeOwners: z.array(z.string().min(1)),
  }),
])

type SpawnCapabilityInput = z.output<typeof spawnCapabilityInputSchema>
type WorkflowCapabilityInput = z.output<typeof workflowCapabilityInputSchema>
type PreviewCapabilityInput = z.output<typeof previewCapabilityInputSchema>
type CheckoutPathInput = {
  repo?: string
  clonePath?: string
  cwd?: string
  inputs?: Record<string, unknown>
}

export interface FactoryNodeDefinitionOptions {
  config: NodeConfig
  name?: string
  maxAgents?: number
  tags?: string[]
  version?: string
  workflowRunner?: WorkflowRunner
  resolveAgentRelayMcpCommand?: () => AgentRelayMcpCommand | undefined
  previewManager?: PreviewManager
}

export interface WorkflowRunnerInput {
  workflow: string
  cwd: string
  inputs: Record<string, unknown>
  invocationId?: string
}

export interface WorkflowRunnerResult {
  cwd: string
  runner: '@relayflows/core'
  status: WorkflowRunRow['status'] | 'completed'
  runId?: string
  workflowName?: string
  result?: WorkflowRunRow
}

export type WorkflowRunner = (input: WorkflowRunnerInput) => Promise<WorkflowRunnerResult>

export interface FactoryNodeInventoryAgent {
  agent_id?: string
  name: string
  invocationId?: string
  session_ref?: string
}

export interface FactoryNodeInventorySync {
  type: 'inventory.sync'
  workspaceId?: string
  clonePaths: Record<string, string>
  agents: FactoryNodeInventoryAgent[]
}

export function createFactoryNodeDefinition(options: FactoryNodeDefinitionOptions): FleetNodeDefinition {
  const config = options.config
  const capabilities = normalizeCapabilities([
    ...config.capabilities,
    ...(config.preview ? ['preview:tailscale-serve'] : []),
  ])
  const metadata = nodeCapabilityMetadata(config)
  const handlers: Record<string, FleetCapabilityValue> = {}
  const previewManager = config.preview
    ? options.previewManager ?? new TailscalePreviewManager({ config: config.preview })
    : undefined

  for (const capability of capabilities) {
    if (capability === 'preview:tailscale-serve') {
      if (!previewManager) {
        throw new Error('preview:tailscale-serve requires NodeConfig.preview')
      }
      handlers[capability] = action<PreviewCapabilityInput>({
        input: previewCapabilityInputSchema,
        metadata: { ...metadata, handler: 'tailscale-serve', access: 'tailnet' },
      }, async (input, ctx) => runPreviewCapability(input, ctx, previewManager, config.preview!)) as unknown as FleetCapabilityValue
      continue
    }
    if (capability === 'workflow:run') {
      handlers[capability] = action<WorkflowCapabilityInput>({
        input: workflowCapabilityInputSchema,
        metadata: { ...metadata, handler: 'relayflows' },
      }, async (input, ctx) => runWorkflowCapability(input, ctx, {
        config,
        workflowRunner: options.workflowRunner ?? runRelayflowsWorkflow,
      })) as unknown as FleetCapabilityValue
      continue
    }

    handlers[capability] = action<SpawnCapabilityInput>({
      input: spawnCapabilityInputSchema,
      metadata: { ...metadata, handler: 'harness', cli: capabilityCli[capability] },
    }, async (input, ctx) => runSpawnCapability(capability, input, ctx, {
      config,
      resolveAgentRelayMcpCommand: options.resolveAgentRelayMcpCommand ?? resolveAgentRelayMcpCommand,
    })) as unknown as FleetCapabilityValue
  }

  return defineNode({
    name: options.name ?? defaultFactoryNodeName(),
    ...(options.maxAgents !== undefined ? { maxAgents: options.maxAgents } : {}),
    capabilities: handlers,
    tags: options.tags ?? defaultFactoryNodeTags(config),
    version: options.version ?? 'factory-node-v1',
  })
}

async function runPreviewCapability(
  input: PreviewCapabilityInput,
  ctx: FleetActionContext,
  manager: PreviewManager,
  config: PreviewConfig,
): Promise<unknown> {
  if (input.operation === 'start') {
    const configured = config.services[input.service]
    if (!configured) {
      throw new Error(`preview service is not configured on this node: ${input.service}`)
    }
    if (
      input.targetPort !== configured.port ||
      input.preferredHttpsPort !== configured.httpsPort ||
      input.startCommand !== configured.startCommand
    ) {
      throw new Error(`preview request does not match this node's configuration for ${input.service}`)
    }
    const { operation: _operation, ...request } = input
    const preview = await manager.start({ ...request, node: ctx.node.name })
    return { operation: input.operation, preview: { ...preview, node: ctx.node.name } }
  }
  if (input.operation === 'remove') {
    return { operation: input.operation, removed: await manager.remove(input.preview) }
  }
  return {
    operation: input.operation,
    ...await manager.sweep({ namespace: input.namespace, activeOwners: input.activeOwners }),
  }
}

export function readFactoryNodeConfigSync(configPath = resolveFactoryNodeConfigPath()): NodeConfig {
  const raw = readFileSync(configPath, 'utf8')
  const parsed = JSON.parse(raw) as unknown
  return parseFactoryNodeConfig(parsed)
}

export function resolveFactoryNodeConfigPath(env: NodeJS.ProcessEnv = process.env): string {
  return resolve(
    env[AGENT_RELAY_FACTORY_NODE_CONFIG_ENV] ??
    env[FACTORY_NODE_CONFIG_ENV] ??
    env.FACTORY_CONFIG ??
    DEFAULT_FACTORY_NODE_CONFIG_PATH,
  )
}

export function parseFactoryNodeConfig(input: unknown): NodeConfig {
  const record = asRecord(input)
  if ('nodeConfig' in record) {
    return NodeConfigSchema.parse(record.nodeConfig)
  }
  if ('factoryConfig' in record || 'workspaceConfig' in record || 'repos' in record) {
    return loadFactoryConfig(input).nodeConfig
  }
  return NodeConfigSchema.parse(input)
}

/**
 * Builds the `inventory.sync` payload a node emits on reconnect so the cloud can
 * reconcile live agents against the node's advertised clone paths (AC3/AC5).
 *
 * Provisional surface: the wire-level reconnect/drain handshake is owned by the
 * installed `agent-relay` sidecar, not this package — the sidecar calls this
 * builder via the default node export to produce the reconcile payload. It is
 * exported (and unit-proven) here so the payload shape stays pinned to NodeConfig
 * even though no in-package handler invokes it directly.
 */
export function factoryNodeInventorySync(config: NodeConfig, agents: FactoryNodeInventoryAgent[]): FactoryNodeInventorySync {
  return {
    type: 'inventory.sync',
    workspaceId: config.workspaceId,
    clonePaths: { ...config.clonePaths },
    agents: agents.map((agent) => ({ ...agent })),
  }
}

async function runSpawnCapability(
  capability: Extract<FactoryNodeCapability, 'spawn:claude' | 'spawn:codex'>,
  input: SpawnCapabilityInput,
  ctx: FleetActionContext,
  deps: Pick<FactoryNodeDefinitionOptions, 'config' | 'resolveAgentRelayMcpCommand'>,
): Promise<unknown> {
  if (!input.name) {
    throw new Error(`${capability} requires name or agent`)
  }

  const cwd = resolveCheckoutPath(deps.config, input)
  const cli = capabilityCli[capability]
  const command = deps.resolveAgentRelayMcpCommand?.()
  const spawnInput: SpawnPtyInput = {
    name: input.name,
    cli,
    args: input.args,
    channels: input.channels,
    task: input.task,
    model: input.model,
    cwd,
    continueFrom: input.sessionRef,
    restartPolicy: input.restartPolicy,
  }
  const harnessConfig = command ? buildRelayMcpHarnessConfig(spawnInput, command) : undefined
  // spawn_mode/exit_after_task ride the agent spec onto the broker's spawn
  // input; the broker ends the agent when its task completes.
  const agent: AgentSpec = {
    name: input.name,
    runtime: 'pty',
    cli,
    ...(input.model ? { model: input.model } : {}),
    ...(input.args && input.args.length > 0 ? { args: input.args } : {}),
    ...(input.channels ? { channels: input.channels } : {}),
    cwd,
    ...(input.sessionRef ? { session_id: input.sessionRef } : {}),
    ...(input.restartPolicy ? { restart_policy: input.restartPolicy } : {}),
    ...(harnessConfig ? { harness_config: harnessConfig } : {}),
    ...(input.spawnMode ? { spawn_mode: input.spawnMode } : {}),
    ...(input.exitAfterTask !== undefined ? { exit_after_task: input.exitAfterTask } : {}),
  }

  const output = await ctx.spawnAgent({
    agent,
    ...(input.task !== undefined ? { initialTask: input.task } : {}),
    skipRelayPrompt: input.skip_relay_prompt ?? false,
    invocationId: input.invocationId ?? ctx.invocationId,
  })

  return {
    name: input.name,
    capability,
    cwd,
    node: ctx.node.name,
    invocationId: input.invocationId ?? ctx.invocationId,
    agent: output,
  }
}

async function runWorkflowCapability(
  input: WorkflowCapabilityInput,
  ctx: FleetActionContext,
  deps: Pick<FactoryNodeDefinitionOptions, 'config' | 'workflowRunner'>,
): Promise<unknown> {
  const cwd = resolveCheckoutPath(deps.config, input)
  const result = await deps.workflowRunner!({
    workflow: input.workflow,
    cwd,
    inputs: input.inputs,
    invocationId: input.invocationId ?? ctx.invocationId,
  })

  return {
    name: input.name ?? input.workflow,
    capability: 'workflow:run',
    workflow: input.workflow,
    invocationId: input.invocationId ?? ctx.invocationId,
    ...result,
    cwd,
  }
}

export async function runRelayflowsWorkflow(input: WorkflowRunnerInput): Promise<WorkflowRunnerResult> {
  const workflowPath = resolve(input.cwd, input.workflow)
  const env = {
    RELAYFLOWS_INPUTS_JSON: JSON.stringify(input.inputs),
    FACTORY_WORKFLOW_INPUTS_JSON: JSON.stringify(input.inputs),
    ...(input.invocationId ? { RELAY_INVOCATION_ID: input.invocationId } : {}),
  }
  const ext = extname(workflowPath).toLowerCase()

  if (ext === '.yaml' || ext === '.yml') {
    const result = await withProcessEnv(env, () => runWorkflow(workflowPath, {
      cwd: input.cwd,
      vars: input.inputs,
    }))
    if (result.status !== 'completed' && result.status !== 'needs_human') {
      throw new Error(`Relayflows workflow ${input.workflow} ${result.status}${result.error ? `: ${result.error}` : ''}`)
    }
    return {
      cwd: input.cwd,
      runner: '@relayflows/core',
      status: result.status,
      runId: result.id,
      workflowName: result.workflowName,
      result,
    }
  }

  if (ext === '.ts' || ext === '.tsx' || ext === '.py') {
    await withProcessEnv(env, () => runScriptWorkflowWithCwd(workflowPath, input.cwd))
    return {
      cwd: input.cwd,
      runner: '@relayflows/core',
      status: 'completed',
    }
  }

  throw new Error(`Unsupported workflow file type: ${ext || '(none)'}. Use .yaml, .yml, .ts, .tsx, or .py`)
}

function runScriptWorkflowWithCwd(workflowPath: string, cwd: string): Promise<void> {
  const runner = runScriptWorkflow as (filePath: string, options?: { cwd?: string }) => Promise<void>
  return runner(workflowPath, { cwd })
}

async function withProcessEnv<T>(
  values: Record<string, string>,
  fn: () => Promise<T>,
): Promise<T> {
  const run = async (): Promise<T> => {
    const previous = new Map<string, string | undefined>()
    for (const [key, value] of Object.entries(values)) {
      previous.set(key, process.env[key])
      process.env[key] = value
    }
    try {
      return await fn()
    } finally {
      for (const [key, value] of previous) {
        if (value === undefined) {
          delete process.env[key]
        } else {
          process.env[key] = value
        }
      }
    }
  }

  const result = workflowEnvLock.then(run, run)
  workflowEnvLock = result.then(
    () => undefined,
    () => undefined,
  )
  return result
}

function resolveCheckoutPath(
  config: NodeConfig,
  input: CheckoutPathInput,
): string {
  const clonePaths = config.clonePaths
  const configuredPaths = new Set(Object.values(clonePaths).map((path) => resolve(path)))
  const explicitPath = input.clonePath ?? input.cwd
  if (explicitPath) {
    const resolved = resolve(explicitPath)
    // Only honor a cloud-supplied path when it is advertised: it must be one of
    // the configured clonePaths, or live under the configured cloneRoot. Empty
    // clonePaths must NOT be treated as "allow all" — that would let the cloud
    // direct spawns into arbitrary local directories (see F4).
    if (configuredPaths.has(resolved) || isWithinCloneRoot(config, resolved)) {
      return resolved
    }
    throw new Error(`checkout path is not advertised by this node: ${explicitPath}`)
  }

  const repo = input.repo ?? repoFromInputs(input.inputs)
  if (repo) {
    const mapped = checkoutForRepo(config, repo)
    if (mapped) return mapped
  }

  const paths = Object.values(clonePaths)
  if (paths.length === 1) {
    return resolve(paths[0]!)
  }

  throw new Error(repo
    ? `no checkout path configured for repo ${repo}`
    : 'spawn requires repo, clonePath, cwd, or a NodeConfig with exactly one clonePath')
}

function isWithinCloneRoot(config: NodeConfig, resolvedPath: string): boolean {
  if (!config.cloneRoot) return false
  const root = resolve(config.cloneRoot)
  if (resolvedPath === root) return true
  const prefix = root.endsWith(sep) ? root : root + sep
  return resolvedPath.startsWith(prefix)
}

function checkoutForRepo(config: NodeConfig, repo: string): string | undefined {
  const candidates = new Set([
    repo,
    repo.replace(/^\/+|\/+$/gu, ''),
    basename(repo),
  ])
  for (const [key, clonePath] of Object.entries(config.clonePaths)) {
    if (candidates.has(key) || candidates.has(basename(key))) {
      return resolve(clonePath)
    }
  }
  if (config.cloneRoot) {
    return resolve(join(config.cloneRoot, basename(repo)))
  }
  return undefined
}

function repoFromInputs(inputs: Record<string, unknown> | undefined): string | undefined {
  if (!inputs) return undefined
  for (const key of ['repo', 'repository', 'repoLabel', 'repo_label']) {
    const value = inputs[key]
    if (typeof value === 'string' && value.trim()) {
      return value.trim()
    }
  }
  return undefined
}

function normalizeCapabilities(capabilities: string[]): FactoryNodeCapability[] {
  const normalized = capabilities
    .map((capability) => capability.trim())
    .filter((capability): capability is FactoryNodeCapability => knownFactoryNodeCapabilities.has(capability))
  const unique = [...new Set(normalized)]
  if (unique.length === 0) {
    throw new Error('NodeConfig.capabilities must include at least one factory node capability')
  }
  return unique
}

function nodeCapabilityMetadata(config: NodeConfig): Record<string, JsonValue> {
  return {
    factoryNode: true,
    ...(config.workspaceId ? { workspaceId: config.workspaceId } : {}),
    cloneRoot: config.cloneRoot ?? null,
    clonePaths: jsonRecord(config.clonePaths),
    dryRun: config.dryRun,
    ...(config.preview ? {
      preview: {
        provider: config.preview.provider,
        access: config.preview.access,
        httpsPortRange: [...config.preview.httpsPortRange],
      },
    } : {}),
  }
}

function jsonRecord(input: Record<string, string>): Record<string, JsonValue> {
  return Object.fromEntries(Object.entries(input).map(([key, value]) => [key, value]))
}

function defaultFactoryNodeTags(config: NodeConfig): string[] {
  return [
    'factory',
    ...(config.workspaceId ? [`workspace:${config.workspaceId}`] : []),
    ...Object.keys(config.clonePaths).map((repo) => `repo:${repo}`),
  ]
}

function defaultFactoryNodeName(): string {
  return process.env.AGENT_RELAY_NODE_NAME || process.env.FACTORY_NODE_NAME || `factory-${hostname()}`
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>
  }
  throw new Error('factory node config must be a JSON object')
}
