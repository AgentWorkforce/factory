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
  let publicationCompleted = false
  let publicationInFlight = false
  let publicationRetryRequested = false
  let publicationTerminal = false
  const serve = options.serve ?? startServeNode
  const publishCard = () => {
    const card = options.definition.agentCard
    if (!card || !options.cardPublisher || publicationCompleted || publicationInFlight || publicationTerminal) return
    publicationInFlight = true
    void options.cardPublisher.publishAgentCard(card).then((published) => {
      publicationInFlight = false
      publicationRetryRequested = false
      publicationCompleted = true
      resolvePublication(published)
    }, (error) => {
      publicationInFlight = false
      options.warn?.(
        `Factory persona card publication failed; ${
          publicationRetryRequested ? 'a registration arrived during publication, retrying now' : 'the next node registration will retry'
        }: ${error instanceof Error ? error.message : String(error)}`,
      )
      if (publicationRetryRequested) {
        publicationRetryRequested = false
        publishCard()
      }
    })
  }
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
      if (publicationCompleted || publicationTerminal) return
      if (publicationInFlight) {
        // A reconnect can finish while the previous registration's HTTP write
        // is still pending. Preserve that edge so a later failure retries
        // immediately instead of waiting for a third registration.
        publicationRetryRequested = true
        return
      }
      const card = options.definition.agentCard
      if (!card) {
        publicationCompleted = true
        resolvePublication(undefined)
        return
      }
      if (!options.cardPublisher) {
        publicationTerminal = true
        rejectPublication(new Error('Factory persona node came online without an AgentCardPublisher'))
        return
      }
      publishCard()
    },
  })
  return {
    stop: () => running.stop(),
    done: running.done,
    cardPublished,
  }
}
