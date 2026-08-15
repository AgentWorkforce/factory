import { isAbsolute, join, resolve } from 'node:path'

import { loadFactoryConfig, type FactoryConfig } from '../config/schema'

export interface PrepareCloudNodeConfigInput {
  source: unknown
  cloneRoot: string
  runtimeRoot: string
  workspaceId?: string
  instanceName?: string
  configPath: string
}

export interface PreparedCloudNodeConfig {
  config: FactoryConfig
  commands: {
    status: string[]
    dryRun: string[]
    start: string[]
  }
}

/**
 * Resolve a portable Factory contract into one self-contained cloud-node copy.
 *
 * Node-local paths from the source host are deliberately discarded. The
 * returned config has one explicit workspace, checkout root, state directory,
 * reporting identity, and config path so no command can accidentally fall back
 * to a config from its current working directory.
 */
export function prepareCloudNodeConfig(input: PrepareCloudNodeConfigInput): PreparedCloudNodeConfig {
  assertAbsolutePath(input.cloneRoot, 'cloneRoot')
  assertAbsolutePath(input.runtimeRoot, 'runtimeRoot')
  assertAbsolutePath(input.configPath, 'configPath')

  // Normalize lexical `.`/`..` segments once so the path written into the
  // config and the path printed in every command are byte-for-byte identical.
  // realpath() is intentionally not used: the output and runtime directories
  // are allowed not to exist yet.
  const cloneRoot = resolve(input.cloneRoot)
  const runtimeRoot = resolve(input.runtimeRoot)
  const configPath = resolve(input.configPath)

  const source = asRecord(structuredClone(input.source), 'factory config')
  const workspaceId = input.workspaceId?.trim() || configuredWorkspaceId(source)
  if (!workspaceId) {
    throw new Error('cloud-node config requires a resolved workspaceId')
  }

  const heartbeatPath = join(runtimeRoot, 'factory-loop-heartbeat.json')
  const registryPath = join(runtimeRoot, 'factory-loop-registry.json')
  const outboxPath = join(runtimeRoot, 'factory-cloud-events.json')
  const previewRegistryPath = join(runtimeRoot, 'tailscale-previews.json')
  const instanceName = input.instanceName?.trim() || 'factory-cloud-node'

  const overrides = {
    workspaceId,
    cloneRoot,
    heartbeatPath,
    registryPath,
    outboxPath,
    previewRegistryPath,
    instanceName,
  }
  const rewritten = hasSplitConfig(source)
    ? rewriteSplitConfig(source, overrides)
    : source.factoryConfig !== undefined
      ? { factoryConfig: rewriteCombinedConfig(asRecord(source.factoryConfig, 'factoryConfig'), overrides) }
      : rewriteCombinedConfig(source, overrides)

  const config = loadFactoryConfig(rewritten).factoryConfig
  if (config.mergePolicy !== 'never') {
    throw new Error('cloud-node migration requires mergePolicy "never"')
  }

  return {
    config,
    commands: {
      status: factoryCommand('status', configPath),
      dryRun: factoryCommand('run-once', configPath, '--dry-run'),
      start: factoryCommand('start', configPath, '--mode', 'live', '--backend', 'relay'),
    },
  }
}

interface NodePathOverrides {
  workspaceId: string
  cloneRoot: string
  heartbeatPath: string
  registryPath: string
  outboxPath: string
  previewRegistryPath: string
  instanceName: string
}

function rewriteCombinedConfig(
  source: Record<string, unknown>,
  overrides: NodePathOverrides,
): Record<string, unknown> {
  const repos = asOptionalRecord(source.repos, 'repos')
  const loop = asOptionalRecord(source.loop, 'loop')
  const reporting = asOptionalRecord(source.reporting, 'reporting')
  const preview = source.preview === undefined ? undefined : asRecord(source.preview, 'preview')
  return {
    ...source,
    workspaceId: overrides.workspaceId,
    cloneRoot: overrides.cloneRoot,
    clonePaths: {},
    factoryLoopHeartbeatPath: overrides.heartbeatPath,
    factoryLoopRegistryPath: overrides.registryPath,
    repos: {
      ...repos,
      cloneRoot: overrides.cloneRoot,
      clonePaths: {},
    },
    loop: {
      ...loop,
      heartbeatPath: overrides.heartbeatPath,
      registryPath: overrides.registryPath,
    },
    reporting: {
      ...reporting,
      instanceName: overrides.instanceName,
      outboxPath: overrides.outboxPath,
    },
    ...(preview ? {
      preview: {
        ...preview,
        registryPath: overrides.previewRegistryPath,
      },
    } : {}),
  }
}

function rewriteSplitConfig(
  source: Record<string, unknown>,
  overrides: NodePathOverrides,
): Record<string, unknown> {
  const workspace = asRecord(source.workspaceConfig, 'workspaceConfig')
  const node = asRecord(source.nodeConfig, 'nodeConfig')
  const reporting = asOptionalRecord(workspace.reporting, 'workspaceConfig.reporting')
  const workspacePreview = workspace.preview === undefined
    ? undefined
    : asRecord(workspace.preview, 'workspaceConfig.preview')
  const nodePreview = node.preview === undefined ? undefined : asRecord(node.preview, 'nodeConfig.preview')
  const preview = workspacePreview || nodePreview
    ? {
        ...workspacePreview,
        ...nodePreview,
        services: {
          ...asOptionalRecord(workspacePreview?.services, 'workspaceConfig.preview.services'),
          ...asOptionalRecord(nodePreview?.services, 'nodeConfig.preview.services'),
        },
        registryPath: overrides.previewRegistryPath,
      }
    : undefined
  return {
    workspaceConfig: {
      ...workspace,
      workspaceId: overrides.workspaceId,
      reporting: {
        ...reporting,
        instanceName: overrides.instanceName,
        outboxPath: overrides.outboxPath,
      },
    },
    nodeConfig: {
      ...node,
      workspaceId: overrides.workspaceId,
      cloneRoot: overrides.cloneRoot,
      clonePaths: {},
      factoryLoopHeartbeatPath: overrides.heartbeatPath,
      factoryLoopRegistryPath: overrides.registryPath,
      ...(preview ? {
        preview,
      } : {}),
    },
  }
}

function factoryCommand(action: string, configPath: string, ...args: string[]): string[] {
  return ['node', 'bin/factory.mjs', action, '--config', resolve(configPath), ...args]
}

function hasSplitConfig(source: Record<string, unknown>): boolean {
  return Object.prototype.hasOwnProperty.call(source, 'workspaceConfig') ||
    Object.prototype.hasOwnProperty.call(source, 'nodeConfig')
}

function configuredWorkspaceId(source: Record<string, unknown>): string | undefined {
  const value = hasSplitConfig(source)
    ? asOptionalRecord(source.nodeConfig, 'nodeConfig').workspaceId ??
      asOptionalRecord(source.workspaceConfig, 'workspaceConfig').workspaceId
    : source.factoryConfig !== undefined
      ? asRecord(source.factoryConfig, 'factoryConfig').workspaceId
    : source.workspaceId
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function assertAbsolutePath(value: string, field: string): void {
  if (!isAbsolute(value)) throw new Error(`cloud-node ${field} must be an absolute path`)
}

function asRecord(value: unknown, field: string): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>
  }
  throw new Error(`${field} must be a JSON object`)
}

function asOptionalRecord(value: unknown, field: string): Record<string, unknown> {
  return value === undefined ? {} : asRecord(value, field)
}
