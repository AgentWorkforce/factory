import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { invokeNodeHandler, nodeManifest, type FleetActionContext, type FleetSpawnAgentInput } from '@agent-relay/fleet'

import {
  createFactoryNodeDefinition,
  factoryNodeInventorySync,
  FACTORY_NODE_CONFIG_ENV,
  parseFactoryNodeConfig,
  resolveFactoryNodeConfigPath,
  type WorkflowRunnerInput,
} from './factory-node'
import type { NodeConfig } from '../config/schema'

function nodeConfig(overrides: Partial<NodeConfig> = {}): NodeConfig {
  return {
    workspaceId: 'workspace-1',
    capabilities: ['spawn:claude', 'spawn:codex', 'workflow:run'],
    cloneRoot: '/work',
    clonePaths: {
      'AgentWorkforce/factory': '/work/factory',
      relay: '/work/relay',
    },
    dryRun: false,
    ...overrides,
  }
}

function fakeContext(invocationId = 'inv-1') {
  const spawns: FleetSpawnAgentInput[] = []
  const ctx: FleetActionContext = {
    node: {
      name: 'factory-node',
      capabilities: ['spawn:codex', 'spawn:claude', 'workflow:run'],
    },
    invocationId,
    relay: {
      async sendMessage(input) {
        return { input }
      },
    },
    async spawnAgent(input) {
      spawns.push(input)
      return {
        agent_name: input.agent.name,
        session_ref: 'session-1',
        pid: 123,
      }
    },
  }
  return { ctx, spawns }
}

describe('factory node definition', () => {
  it('advertises configured capabilities and clone paths in the node manifest', () => {
    const definition = createFactoryNodeDefinition({
      config: nodeConfig({ capabilities: ['spawn:codex', 'workflow:run'] }),
      name: 'mac-mini',
      maxAgents: 4,
      version: 'test-version',
    })

    expect(nodeManifest(definition)).toEqual({
      name: 'mac-mini',
      max_agents: 4,
      version: 'test-version',
      tags: ['factory', 'workspace:workspace-1', 'repo:AgentWorkforce/factory', 'repo:relay'],
      capabilities: [
        {
          name: 'spawn:codex',
          kind: 'action',
          metadata: {
            factoryNode: true,
            workspaceId: 'workspace-1',
            cloneRoot: '/work',
            clonePaths: {
              'AgentWorkforce/factory': '/work/factory',
              relay: '/work/relay',
            },
            dryRun: false,
            handler: 'harness',
            cli: 'codex',
          },
        },
        {
          name: 'workflow:run',
          kind: 'action',
          metadata: {
            factoryNode: true,
            workspaceId: 'workspace-1',
            cloneRoot: '/work',
            clonePaths: {
              'AgentWorkforce/factory': '/work/factory',
              relay: '/work/relay',
            },
            dryRun: false,
            handler: 'relayflows',
          },
        },
      ],
    })
  })

  it('spawns codex through the broker in the mapped checkout with MCP harness wiring', async () => {
    const { ctx, spawns } = fakeContext('spawn-inv')
    const definition = createFactoryNodeDefinition({
      config: nodeConfig(),
      name: 'factory-node',
      resolveAgentRelayMcpCommand: () => ({ command: '/usr/local/bin/node', args: ['/repo/agent-relay.js', 'mcp'] }),
    })

    await expect(invokeNodeHandler(definition, 'spawn:codex', {
      name: 'ar-13-impl',
      repo: 'AgentWorkforce/factory',
      task: 'implement p13',
      model: 'gpt-5',
      channel: 'wf-factory-p13',
      session_ref: 'resume-me',
      restart_policy: { max_restarts: 2 },
    }, ctx)).resolves.toMatchObject({
      name: 'ar-13-impl',
      capability: 'spawn:codex',
      cwd: '/work/factory',
      invocationId: 'spawn-inv',
    })

    expect(spawns).toHaveLength(1)
    expect(spawns[0]).toMatchObject({
      initialTask: 'implement p13',
      invocationId: 'spawn-inv',
      skipRelayPrompt: false,
      agent: {
        name: 'ar-13-impl',
        runtime: 'pty',
        cli: 'codex',
        model: 'gpt-5',
        cwd: '/work/factory',
        channels: ['wf-factory-p13'],
        session_id: 'resume-me',
        restart_policy: { max_restarts: 2 },
        harness_config: {
          runtime: 'pty',
          command: 'codex',
          cwd: '/work/factory',
          metadata: {
            factoryRelayMcp: true,
            relayMcpCommand: '/usr/local/bin/node',
            relayMcpArgs: ['/repo/agent-relay.js', 'mcp'],
          },
        },
      },
    })
    expect(spawns[0]?.agent.harness_config?.runtime).toBe('pty')
    if (spawns[0]?.agent.harness_config?.runtime !== 'pty') throw new Error('expected pty harness config')
    expect(spawns[0].agent.harness_config.args.join('\n')).toContain('mcp_servers.agent-relay.command="/usr/local/bin/node"')
  })

  it('runs workflow:run by shelling out to relayflows in the mapped checkout', async () => {
    const calls: WorkflowRunnerInput[] = []
    const { ctx } = fakeContext('workflow-inv')
    const definition = createFactoryNodeDefinition({
      config: nodeConfig(),
      workflowRunner: async (input) => {
        calls.push(input)
        return {
          command: 'relayflows',
          args: ['run', input.workflow],
          cwd: input.cwd,
          exitCode: 0,
          stdout: 'ok',
          stderr: '',
        }
      },
    })

    await expect(invokeNodeHandler(definition, 'workflow:run', {
      workflow: 'workflows/factory/linear-issue.ts',
      inputs: { issue: 'AR-13', repo: 'relay' },
    }, ctx)).resolves.toEqual({
      name: 'workflows/factory/linear-issue.ts',
      capability: 'workflow:run',
      workflow: 'workflows/factory/linear-issue.ts',
      cwd: '/work/relay',
      invocationId: 'workflow-inv',
      command: 'relayflows',
      args: ['run', 'workflows/factory/linear-issue.ts'],
      exitCode: 0,
      stdout: 'ok',
      stderr: '',
    })
    expect(calls).toEqual([{
      workflow: 'workflows/factory/linear-issue.ts',
      cwd: '/work/relay',
      inputs: { issue: 'AR-13', repo: 'relay' },
      invocationId: 'workflow-inv',
    }])
  })

  it('rejects an action that asks for a checkout not advertised by NodeConfig', async () => {
    const { ctx } = fakeContext()
    const definition = createFactoryNodeDefinition({ config: nodeConfig() })

    await expect(invokeNodeHandler(definition, 'spawn:claude', {
      name: 'ar-13-review',
      cwd: '/tmp/not-advertised',
    }, ctx)).rejects.toThrow('checkout path is not advertised by this node')
  })

  it('rejects an explicit checkout when clonePaths is empty and no cloneRoot is advertised', async () => {
    const { ctx } = fakeContext()
    const definition = createFactoryNodeDefinition({
      config: nodeConfig({ capabilities: ['spawn:claude'], cloneRoot: undefined, clonePaths: {} }),
    })

    await expect(invokeNodeHandler(definition, 'spawn:claude', {
      name: 'ar-13-review',
      cwd: '/tmp/arbitrary-local-dir',
    }, ctx)).rejects.toThrow('checkout path is not advertised by this node')
  })

  it('accepts an explicit checkout under the advertised cloneRoot even with empty clonePaths', async () => {
    const { ctx, spawns } = fakeContext()
    const definition = createFactoryNodeDefinition({
      config: nodeConfig({ capabilities: ['spawn:claude'], cloneRoot: '/work', clonePaths: {} }),
    })

    await expect(invokeNodeHandler(definition, 'spawn:claude', {
      name: 'ar-13-review',
      cwd: '/work/relay',
    }, ctx)).resolves.toMatchObject({ cwd: '/work/relay' })
    expect(spawns[0]?.agent.cwd).toBe('/work/relay')

    await expect(invokeNodeHandler(definition, 'spawn:claude', {
      name: 'ar-13-review',
      cwd: '/work-sibling/relay',
    }, ctx)).rejects.toThrow('checkout path is not advertised by this node')
  })

  it('parses node-only, split, and legacy combined config shapes', () => {
    expect(parseFactoryNodeConfig({
      workspaceId: 'workspace-1',
      capabilities: ['spawn:codex'],
      clonePaths: { relay: '/work/relay' },
    })).toMatchObject({
      workspaceId: 'workspace-1',
      capabilities: ['spawn:codex'],
      clonePaths: { relay: '/work/relay' },
    })

    expect(parseFactoryNodeConfig({
      workspaceConfig: {
        workspaceId: 'workspace-1',
        repos: { org: 'AgentWorkforce', names: ['relay'], cloneRoot: '/ignored-workspace-root' },
      },
      nodeConfig: {
        workspaceId: 'workspace-1',
        capabilities: ['workflow:run'],
        clonePaths: { 'AgentWorkforce/relay': '/work/relay' },
      },
    })).toMatchObject({
      workspaceId: 'workspace-1',
      capabilities: ['workflow:run'],
      clonePaths: { 'AgentWorkforce/relay': '/work/relay' },
    })

    expect(parseFactoryNodeConfig({
      workspaceId: 'workspace-1',
      capabilities: ['spawn:claude'],
      repos: { org: 'AgentWorkforce', names: ['factory'], cloneRoot: '/work' },
    })).toMatchObject({
      workspaceId: 'workspace-1',
      capabilities: ['spawn:claude'],
      cloneRoot: '/work',
      clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
    })
  })

  it('builds an inventory.sync payload for reconnect reconcile without changing live agents', () => {
    expect(factoryNodeInventorySync(nodeConfig(), [
      { agent_id: 'agent-1', name: 'ar-13-impl', invocationId: 'inv-1', session_ref: 'session-1' },
    ])).toEqual({
      type: 'inventory.sync',
      workspaceId: 'workspace-1',
      clonePaths: {
        'AgentWorkforce/factory': '/work/factory',
        relay: '/work/relay',
      },
      agents: [
        { agent_id: 'agent-1', name: 'ar-13-impl', invocationId: 'inv-1', session_ref: 'session-1' },
      ],
    })
  })

  it('resolves the default config path from the node config environment', () => {
    expect(resolveFactoryNodeConfigPath({
      AGENT_RELAY_FACTORY_NODE_CONFIG: '/configs/factory-node.json',
    } as NodeJS.ProcessEnv)).toBe('/configs/factory-node.json')
  })

  it('exposes the node builder through the published package root entry', async () => {
    const pkg = await import('../index')
    expect(typeof pkg.createFactoryNodeDefinition).toBe('function')
    expect(typeof pkg.readFactoryNodeConfigSync).toBe('function')

    const definition = pkg.createFactoryNodeDefinition({
      config: nodeConfig({ capabilities: ['spawn:claude', 'workflow:run'] }),
      name: 'published-node',
      version: 'test-version',
    })
    const manifest = nodeManifest(definition)
    expect(manifest.name).toBe('published-node')
    expect(manifest.capabilities.map((cap) => cap.name)).toEqual(['spawn:claude', 'workflow:run'])
  })

  it('resolves a FleetNodeDefinition from the packaged default node export', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'factory-node-def-'))
    const configPath = join(dir, 'factory.node.json')
    writeFileSync(configPath, JSON.stringify({
      workspaceId: 'workspace-published',
      capabilities: ['spawn:claude'],
      clonePaths: { relay: '/work/relay' },
    }))
    const previous = process.env[FACTORY_NODE_CONFIG_ENV]
    process.env[FACTORY_NODE_CONFIG_ENV] = configPath
    try {
      const mod = await import('./factory.node')
      const manifest = nodeManifest(mod.default)
      expect(manifest.capabilities.map((cap) => cap.name)).toEqual(['spawn:claude'])
      expect(manifest.tags).toContain('workspace:workspace-published')
    } finally {
      if (previous === undefined) delete process.env[FACTORY_NODE_CONFIG_ENV]
      else process.env[FACTORY_NODE_CONFIG_ENV] = previous
    }
  })
})
