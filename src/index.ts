import type { Factory, FactoryPorts } from './types'
import type { FactoryConfig } from './config/schema'
import { MountGithubRead, MountLinearWriteback, MountSlackWriteback } from './writeback'

const notImplemented = (method: string) => new Error(`Factory.${method} not implemented`)

export function createFactory(config: FactoryConfig, ports: FactoryPorts): Factory {
  const resolvedPorts: FactoryPorts = {
    ...ports,
    linear: ports.linear ?? MountLinearWriteback(ports.mount, config.stateIds),
    slack: ports.slack ?? MountSlackWriteback(ports.mount, config.slack),
    github: ports.github ?? MountGithubRead(ports.mount),
  }

  void resolvedPorts

  return {
    async start(): Promise<void> {
      throw notImplemented('start')
    },
    async stop(): Promise<void> {
      throw notImplemented('stop')
    },
    async runOnce(): Promise<never> {
      throw notImplemented('runOnce')
    },
    async triageIssue(): Promise<never> {
      throw notImplemented('triageIssue')
    },
    async dispatch(): Promise<never> {
      throw notImplemented('dispatch')
    },
    status() {
      return { inFlight: [], queued: [], counters: {} }
    },
    on() {
      return () => {}
    },
  }
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
export {
  HeuristicTriage,
  LlmTriage,
  TieredTriage,
  TriageDecisionSchema,
} from './triage'
export type {
  HeuristicTriageOptions,
  LlmTriageOptions,
} from './triage'
export {
  linearCommentName,
  MountGithubRead,
  MountLinearWriteback,
  MountSlackWriteback,
} from './writeback'
export type {
  LinearCommentPayload,
  LinearStateIds,
  MountSlackWritebackConfig,
} from './writeback'
export {
  canonicalMountPaths,
  createWorkspaceScopedEventClient,
  deliveryTargetsFor,
  eventPathGlobsForIntegration,
  filesystemEventToChangeEvent,
  filterLinearPredicateSpecs,
  globMatchesPath,
  globSegmentMatches,
  hasLinearPredicates,
  integrationRelayFileSyncOptions,
  isLinearIssueEventPath,
  linearIssueMatchesPredicates,
  linearRecordCandidates,
  linearScopePredicates,
  normalizeChangePath,
  relayfileSdkPathFiltersFor,
  slackListenDms,
  subscriptionSpecsFor,
} from './subscriptions'
export type {
  ConnectedIntegrationLike,
  DeliveryTargets,
  FilesystemEventLike,
  IntegrationRelayFileSyncOptionsInput,
  LinearPredicateSubscriptionSpec,
  LinearScopePredicates,
  LocalMountRoot,
  RelayfileEventClient,
  RelayFileSyncFactory,
  RelayFileSyncLike,
  SubscriptionSpec,
  TokenProvider,
  WatchRegistration,
  WorkspaceEventClientSource,
  WorkspaceScopedEventClientOptions,
  WorkspaceScopedSubscribeOptions,
  ChangeEvent as SubscriptionChangeEvent,
} from './subscriptions'
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
  TelemetrySink,
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
