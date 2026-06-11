import type { Factory, FactoryPorts } from './types'
import type { FactoryConfig } from './config/schema'

export function createFactory(_config: FactoryConfig, _ports: FactoryPorts): Factory {
  throw new Error('not implemented')
}

export type { FactoryConfig } from './config/schema'
export { FactoryConfigSchema } from './config/schema'
export {
  LINEAR_STATE_IDS,
  linearByStatePath,
  linearCommentPath,
  linearIssuePath,
} from './constants/linear'
export {
  slackMessagePath,
  slackReplyPath,
} from './constants/slack'
export { RelayFleetClient } from './fleet/relay-fleet-client'
export { createFleet } from './ports'
export type {
  Capability,
  ChangeEvent,
  Clock,
  EventPage,
  MountClient,
  SubscribeOptions,
  Subscription,
  AgentSpec,
  FleetClient,
  RestartPolicy,
  RosterEntry,
  SendInput,
  SpawnInput,
  SpawnResult,
  GithubRead,
  LinearWriteback,
  Logger,
  SlackWriteback,
} from './ports'
export type {
  DispatchResult,
  Factory,
  FactoryEventPayload,
  FactoryPorts,
  FactoryStatus,
  IssueRef,
  IterationReport,
  LinearIssue,
  PrSummary,
  RepoMapEntry,
  TriageContext,
  TriageDecision,
  TriageEngine,
} from './types'
