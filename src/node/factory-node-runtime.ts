import { startServeNode } from '@agent-relay/fleet'

import type { FleetNodeInfo, RunningNode, ServeNodeOptions } from '@agent-relay/fleet'

import type { AgentCardPublisher, PublishedAgentCard } from './factory-persona-card'
import type { FactoryNodeDefinition } from './factory-node'

export interface StartFactoryNodeOptions extends Omit<ServeNodeOptions, 'definition' | 'onRegistered'> {
  definition: FactoryNodeDefinition
  cardPublisher?: AgentCardPublisher
  onRegistered?: (info: FleetNodeInfo) => void
  /** Test seam; production uses @agent-relay/fleet startServeNode. */
  serve?: (options: ServeNodeOptions) => RunningNode
}

export interface RunningFactoryNode extends RunningNode {
  /** Settles only after the online registration hook publishes the persona card. */
  cardPublished: Promise<PublishedAgentCard | undefined>
}

/** Start a Factory node and publish its persona card on the node-online edge. */
export function startFactoryNode(options: StartFactoryNodeOptions): RunningFactoryNode {
  let resolvePublication!: (value: PublishedAgentCard | undefined) => void
  let rejectPublication!: (reason: unknown) => void
  const cardPublished = new Promise<PublishedAgentCard | undefined>((resolve, reject) => {
    resolvePublication = resolve
    rejectPublication = reject
  })
  let registered = false
  const serve = options.serve ?? startServeNode
  const running = serve({
    definition: options.definition,
    connection: options.connection,
    ...(options.providerName ? { providerName: options.providerName } : {}),
    ...(options.triggers ? { triggers: options.triggers } : {}),
    ...(options.nameOverride ? { nameOverride: options.nameOverride } : {}),
    ...(options.maxAgentsOverride !== undefined ? { maxAgentsOverride: options.maxAgentsOverride } : {}),
    ...(options.reconnect !== undefined ? { reconnect: options.reconnect } : {}),
    ...(options.signal ? { signal: options.signal } : {}),
    ...(options.logger ? { logger: options.logger } : {}),
    ...(options.log ? { log: options.log } : {}),
    ...(options.warn ? { warn: options.warn } : {}),
    onRegistered(info) {
      options.onRegistered?.(info)
      if (registered) return
      registered = true
      const card = options.definition.agentCard
      if (!card) {
        resolvePublication(undefined)
        return
      }
      if (!options.cardPublisher) {
        rejectPublication(new Error('Factory persona node came online without an AgentCardPublisher'))
        return
      }
      void options.cardPublisher.publishAgentCard(card).then(resolvePublication, rejectPublication)
    },
  })
  return {
    stop: () => running.stop(),
    done: running.done,
    cardPublished,
  }
}
