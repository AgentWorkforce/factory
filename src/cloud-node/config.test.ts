import { describe, expect, it } from 'vitest'

import { prepareCloudNodeConfig } from './config'

const source = (overrides: Record<string, unknown> = {}) => ({
  issueSource: 'github',
  repos: {
    org: 'AgentWorkforce',
    names: ['factory', 'relayfile'],
    cloneRoot: '/Users/operator/AgentWorkforce',
    clonePaths: {
      'AgentWorkforce/factory': '/Users/operator/custom/factory',
    },
  },
  safety: {
    requireLabel: 'factory',
    requireTitlePrefix: '[factory]',
  },
  mergePolicy: 'never',
  preview: {
    services: {},
    registryPath: '/Users/operator/.factory/previews.json',
  },
  reporting: {
    instanceName: 'factory-laptop',
    outboxPath: '/Users/operator/.factory/events.json',
  },
  ...overrides,
})

describe('prepareCloudNodeConfig', () => {
  it('discards source-host paths and emits exact cloud-node commands', () => {
    const prepared = prepareCloudNodeConfig({
      source: source(),
      workspaceId: 'rw_factory',
      cloneRoot: '/srv/agent-workforce',
      runtimeRoot: '/var/lib/factory',
      configPath: '/etc/factory/factory.khaliq.config.json',
      instanceName: 'factory-khaliq-cloud',
    })

    expect(prepared.config).toMatchObject({
      workspaceId: 'rw_factory',
      issueSource: 'github',
      mergePolicy: 'never',
      cloneRoot: '/srv/agent-workforce',
      clonePaths: {
        'AgentWorkforce/factory': '/srv/agent-workforce/factory',
        'AgentWorkforce/relayfile': '/srv/agent-workforce/relayfile',
      },
      repos: {
        clonePaths: {
          'AgentWorkforce/factory': '/srv/agent-workforce/factory',
          'AgentWorkforce/relayfile': '/srv/agent-workforce/relayfile',
        },
      },
      loop: {
        heartbeatPath: '/var/lib/factory/factory-loop-heartbeat.json',
        registryPath: '/var/lib/factory/factory-loop-registry.json',
      },
      reporting: {
        instanceName: 'factory-khaliq-cloud',
        outboxPath: '/var/lib/factory/factory-cloud-events.json',
      },
      preview: {
        registryPath: '/var/lib/factory/tailscale-previews.json',
      },
    })
    expect(JSON.stringify(prepared.config)).not.toContain('/Users/operator')
    expect(prepared.commands).toEqual({
      status: ['node', 'bin/factory.mjs', 'status', '--config', '/etc/factory/factory.khaliq.config.json'],
      dryRun: [
        'node',
        'bin/factory.mjs',
        'run-once',
        '--config',
        '/etc/factory/factory.khaliq.config.json',
        '--dry-run',
      ],
      start: [
        'node',
        'bin/factory.mjs',
        'start',
        '--config',
        '/etc/factory/factory.khaliq.config.json',
        '--mode',
        'live',
        '--backend',
        'relay',
      ],
    })
  })

  it('resolves split configs without retaining the laptop node half', () => {
    const prepared = prepareCloudNodeConfig({
      source: {
        workspaceConfig: source({ workspaceId: 'rw_factory' }),
        nodeConfig: {
          workspaceId: 'rw_factory',
          cloneRoot: '/Users/operator/AgentWorkforce',
          clonePaths: { 'AgentWorkforce/factory': '/tmp/wrong' },
        },
      },
      cloneRoot: '/workspace/repos',
      runtimeRoot: '/workspace/state',
      configPath: '/workspace/config/factory.json',
    })

    expect(prepared.config.workspaceId).toBe('rw_factory')
    expect(prepared.config.clonePaths['AgentWorkforce/factory']).toBe('/workspace/repos/factory')
    expect(JSON.stringify(prepared.config)).not.toContain('/Users/operator')
    expect(JSON.stringify(prepared.config)).not.toContain('/tmp/wrong')
  })

  it('resolves a factoryConfig envelope', () => {
    const prepared = prepareCloudNodeConfig({
      source: { factoryConfig: source({ workspaceId: 'rw_factory' }) },
      cloneRoot: '/workspace/repos',
      runtimeRoot: '/workspace/state',
      configPath: '/workspace/config/factory.json',
    })

    expect(prepared.config.workspaceId).toBe('rw_factory')
    expect(prepared.config.clonePaths['AgentWorkforce/factory']).toBe('/workspace/repos/factory')
    expect(JSON.stringify(prepared.config)).not.toContain('/Users/operator')
  })

  it('normalizes absolute paths before writing config values and commands', () => {
    const prepared = prepareCloudNodeConfig({
      source: source(),
      workspaceId: 'rw_factory',
      cloneRoot: '/srv/temporary/../agent-workforce',
      runtimeRoot: '/var/lib/factory/./runtime',
      configPath: '/etc/factory/staging/../factory.khaliq.config.json',
    })

    expect(prepared.config.cloneRoot).toBe('/srv/agent-workforce')
    expect(prepared.config.loop.heartbeatPath).toBe('/var/lib/factory/runtime/factory-loop-heartbeat.json')
    expect(prepared.commands.status).toEqual([
      'node',
      'bin/factory.mjs',
      'status',
      '--config',
      '/etc/factory/factory.khaliq.config.json',
    ])
  })

  it('fails closed without an explicit workspace, absolute paths, or mergePolicy never', () => {
    const base = {
      source: source(),
      cloneRoot: '/workspace/repos',
      runtimeRoot: '/workspace/state',
      configPath: '/workspace/config/factory.json',
    }
    expect(() => prepareCloudNodeConfig(base)).toThrow(/resolved workspaceId/u)
    expect(() => prepareCloudNodeConfig({ ...base, workspaceId: 'rw_factory', cloneRoot: 'repos' }))
      .toThrow(/cloneRoot must be an absolute path/u)
    expect(() => prepareCloudNodeConfig({
      ...base,
      workspaceId: 'rw_factory',
      source: source({ mergePolicy: 'on-green-with-review' }),
    })).toThrow(/mergePolicy "never"/u)
  })
})
