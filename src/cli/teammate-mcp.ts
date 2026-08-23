import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  createAgentRelayMcpServer,
  optionsFromEnv,
  resolveStdioBootstrapOptions,
  type AgentRelayMcpServerOptions,
} from 'agent-relay/mcp'
import { z } from 'zod'

import type { FleetClient, TeammateAgent } from '../ports/fleet'
import { RelayFleetClient } from '../fleet/relay-fleet-client'
import { askTeammate } from '../fleet/teammates'

export interface FactoryTeammateMcpDependencies {
  fleet?: FleetClient
}

/**
 * Extend the same Agent Relay MCP server already injected into Factory workers
 * with directory-backed, bounded teammate tools. Keeping one server preserves
 * every existing Relay tool and identity setting while making #139 reachable
 * from the spawned worker rather than only from Factory's library API.
 */
export function createFactoryTeammateMcpServer(
  options: AgentRelayMcpServerOptions,
  dependencies: FactoryTeammateMcpDependencies = {},
): { server: McpServer; fleet: FleetClient } {
  const server = createAgentRelayMcpServer(options)
  const fleet = dependencies.fleet ?? new RelayFleetClient({
    workspaceKey: options.workspaceKey ?? options.apiKey,
    agentToken: options.agentToken,
    agentName: options.agentName,
    baseUrl: options.baseUrl,
    env: {},
    registerLifecycleAction: false,
  })

  server.registerTool('discover_teammates', {
    title: 'Discover Teammates',
    description: 'Find Relaycast agents by exact skill/tag or free-text card query before asking one for help.',
    inputSchema: {
      skill: z.string().trim().min(1).optional(),
      tag: z.string().trim().min(1).optional(),
      q: z.string().trim().min(1).optional(),
    },
    annotations: {
      readOnlyHint: true,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: true,
    },
  }, async ({ skill, tag, q }) => {
    if (!skill && !tag && !q) {
      throw new Error('discover_teammates requires skill, tag, or q')
    }
    return jsonToolResult(await fleet.discoverTeammates({ skill, tag, q }))
  })

  server.registerTool('ask_teammate', {
    title: 'Ask a Teammate',
    description: 'Discover or address one teammate, send a question over Relay, and wait for its reply within a bounded deadline.',
    inputSchema: {
      question: z.string().trim().min(1),
      to: z.string().trim().min(1).optional(),
      skill: z.string().trim().min(1).optional(),
      tag: z.string().trim().min(1).optional(),
      q: z.string().trim().min(1).optional(),
      timeoutMs: z.number().int().positive().max(120_000).optional(),
    },
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: false,
      openWorldHint: true,
    },
  }, async ({ question, to, skill, tag, q, timeoutMs }) => {
    if (!to && !skill && !tag && !q) {
      throw new Error('ask_teammate requires to, skill, tag, or q')
    }
    const from = requiredAgentName(options.agentName)
    const result = await askTeammate(fleet, {
      from,
      question,
      ...(to ? { teammate: directTeammate(to) } : { skill, tag, q }),
      timeoutMs,
    })
    return jsonToolResult(result)
  })

  return { server, fleet }
}

/** Start the worker-facing Factory + Agent Relay MCP server on stdio. */
export async function startFactoryTeammateMcpStdio(
  options: AgentRelayMcpServerOptions = optionsFromEnv(),
): Promise<void> {
  const resolved = await resolveStdioBootstrapOptions(options)
  const { server } = createFactoryTeammateMcpServer(resolved)
  await server.connect(new StdioServerTransport())
}

function directTeammate(address: string): TeammateAgent {
  return {
    name: address,
    address,
    description: 'Direct Relay teammate target supplied by the worker.',
    skills: [],
    tags: [],
    kind: 'native',
    url: `relay://${encodeURIComponent(address)}`,
  }
}

function requiredAgentName(value: string | undefined): string {
  const name = value?.trim()
  if (!name) throw new Error('ask_teammate requires the spawned worker identity (RELAY_AGENT_NAME)')
  return name
}

function jsonToolResult(value: unknown) {
  return {
    content: [{ type: 'text' as const, text: JSON.stringify(value, null, 2) }],
  }
}
