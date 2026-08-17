import type {
  BabysitterGenerationRecord,
  BabysitterSessionState,
  ConversationMessage,
  ConversationSessionState,
  DiscoverySweepState,
  DispatchLifecycle,
  GithubIssueCommentWatchState,
  WaitingClarification,
} from '../ports/state'
import type { PersistedWorkspaceState, WatchStateDocument } from './document-store'

export const parseWatchStateDocument = (value: unknown): WatchStateDocument => {
  if (!isRecord(value) || !isRecord(value.workspaces)) {
    throw invalidDocument()
  }
  if (value.version === 3) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, rawWorkspace] of Object.entries(value.workspaces)) {
      if (!isRecord(rawWorkspace)) throw invalidDocument()
      const watches = rawWorkspace.githubIssueCommentWatches
      const clarifications = rawWorkspace.waitingClarifications
      const babysitters = rawWorkspace.babysitterSessions
      const generations = rawWorkspace.babysitterGenerations
      const conversations = rawWorkspace.conversationSessions
      const lifecycles = rawWorkspace.dispatchLifecycles
      const discoverySweep = rawWorkspace.discoverySweep
      if (
        !isRecord(watches) ||
        !isRecord(clarifications) ||
        (babysitters !== undefined && !isRecord(babysitters)) ||
        (generations !== undefined && !isRecord(generations)) ||
        (conversations !== undefined && !isRecord(conversations)) ||
        (lifecycles !== undefined && !isRecord(lifecycles)) ||
        (discoverySweep !== undefined && !isRecord(discoverySweep))
      ) throw invalidDocument()
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: clarifications as Record<string, WaitingClarification>,
        babysitterSessions: parseBabysitterSessions(babysitters ?? {}),
        babysitterGenerations: parseBabysitterGenerations(generations ?? {}),
        conversationSessions: parseConversationSessions(conversations ?? {}),
        dispatchLifecycles: (lifecycles ?? {}) as Record<string, DispatchLifecycle>,
        discoverySweep: parseDiscoverySweepState(discoverySweep),
      }
    }
    return { version: 3, workspaces }
  }
  if (value.version === 2) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, rawWorkspace] of Object.entries(value.workspaces)) {
      if (!isRecord(rawWorkspace)) throw invalidDocument()
      const watches = rawWorkspace.githubIssueCommentWatches
      const clarifications = rawWorkspace.waitingClarifications
      const babysitters = rawWorkspace.babysitterSessions
      if (!isRecord(watches) || !isRecord(clarifications) || (babysitters !== undefined && !isRecord(babysitters))) {
        throw invalidDocument()
      }
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: clarifications as Record<string, WaitingClarification>,
        babysitterSessions: parseBabysitterSessions(babysitters ?? {}),
        babysitterGenerations: {},
        conversationSessions: {},
        dispatchLifecycles: {},
        discoverySweep: emptyDiscoverySweepState(),
      }
    }
    return { version: 3, workspaces }
  }
  if (value.version === 1) {
    const workspaces: Record<string, PersistedWorkspaceState> = {}
    for (const [workspaceId, watches] of Object.entries(value.workspaces)) {
      if (!isRecord(watches)) throw invalidDocument()
      workspaces[workspaceId] = {
        githubIssueCommentWatches: watches as Record<string, GithubIssueCommentWatchState>,
        waitingClarifications: {},
        babysitterSessions: {},
        babysitterGenerations: {},
        conversationSessions: {},
        dispatchLifecycles: {},
        discoverySweep: emptyDiscoverySweepState(),
      }
    }
    return { version: 3, workspaces }
  }
  throw invalidDocument()
}

export const emptyDiscoverySweepState = (): DiscoverySweepState => ({
  consecutiveOverloads: 0,
  backoffUntilMs: 0,
  lastEpoch: 0,
})

const parseDiscoverySweepState = (value: unknown): DiscoverySweepState => {
  if (value === undefined) return emptyDiscoverySweepState()
  if (!isRecord(value)) throw invalidDocument()
  const checkpoint = value.checkpoint
  const lease = value.lease
  const lastEpoch = value.lastEpoch ?? (isRecord(lease) ? lease.epoch : 0)
  if (
    !Number.isSafeInteger(value.consecutiveOverloads) || (value.consecutiveOverloads as number) < 0 ||
    !Number.isSafeInteger(value.backoffUntilMs) ||
    !Number.isSafeInteger(lastEpoch) || (lastEpoch as number) < 0 ||
    (lease !== undefined && (!isRecord(lease) || typeof lease.owner !== 'string' ||
      !Number.isSafeInteger(lease.epoch) || !Number.isSafeInteger(lease.leaseUntilMs))) ||
    (checkpoint !== undefined && (!isRecord(checkpoint) || !isRecord(checkpoint.trees) ||
      !Number.isSafeInteger(checkpoint.updatedAtMs) ||
      (checkpoint.highWatermark !== undefined && typeof checkpoint.highWatermark !== 'string') ||
      !Object.values(checkpoint.trees).every((paths) =>
        Array.isArray(paths) && paths.every((path) => typeof path === 'string'))))
  ) throw invalidDocument()
  return structuredClone({ ...value, lastEpoch }) as DiscoverySweepState
}

const parseConversationSessions = (
  value: Record<string, unknown>,
): Record<string, ConversationSessionState> => {
  const sessions: Record<string, ConversationSessionState> = {}
  for (const [conversationId, candidate] of Object.entries(value)) {
    if (!isRecord(candidate) || !isRecord(candidate.issue) || !isRecord(candidate.agent) || !isRecord(candidate.context)) {
      throw invalidDocument()
    }
    const issue = candidate.issue
    const agent = candidate.agent
    const delivery = candidate.delivery
    if (
      typeof issue.uuid !== 'string' || typeof issue.key !== 'string' || typeof issue.path !== 'string' ||
      typeof candidate.provider !== 'string' || typeof candidate.externalId !== 'string' ||
      !Object.values(candidate.context).every((entry) => typeof entry === 'string') ||
      typeof agent.name !== 'string' || typeof agent.sessionRef !== 'string' ||
      !validConversationMessages(candidate.history) || !validConversationMessages(candidate.pending) ||
      (candidate.processedMessageIds !== undefined && !validConversationMessageIds(candidate.processedMessageIds)) ||
      (delivery !== undefined && !validConversationDelivery(delivery) && !validLegacyConversationDelivery(delivery))
    ) throw invalidDocument()
    const session = structuredClone(candidate) as unknown as ConversationSessionState
    if (delivery !== undefined && validLegacyConversationDelivery(delivery)) {
      session.pending = [...structuredClone(delivery.messages), ...session.pending]
      delete session.delivery
    }
    session.processedMessageIds = candidate.processedMessageIds === undefined
      ? [...new Set([
          ...session.history,
          ...session.pending,
          ...(session.delivery?.messages ?? []),
        ].map((message) => message.id))]
      : [...candidate.processedMessageIds as string[]]
    sessions[conversationId] = session
  }
  return sessions
}

const validConversationMessages = (value: unknown): value is ConversationMessage[] =>
  Array.isArray(value) && value.every((message) => isRecord(message) &&
    typeof message.id === 'string' && typeof message.text === 'string' &&
    typeof message.receivedAtMs === 'number' &&
    (message.providerSequence === undefined || typeof message.providerSequence === 'string') &&
    (message.author === undefined || typeof message.author === 'string'))

const validConversationDelivery = (value: unknown): boolean => isRecord(value) &&
  typeof value.claimId === 'string' && typeof value.owner === 'string' &&
  typeof value.claimedAtMs === 'number' && typeof value.attempts === 'number' &&
  validConversationMessages(value.messages) && isRecord(value.agent) &&
  typeof value.agent.name === 'string' && typeof value.agent.sessionRef === 'string'

const validLegacyConversationDelivery = (value: unknown): value is {
  owner: string
  claimedAtMs: number
  attempts: number
  messages: ConversationMessage[]
} => isRecord(value) && value.claimId === undefined && value.agent === undefined &&
  typeof value.owner === 'string' && typeof value.claimedAtMs === 'number' &&
  typeof value.attempts === 'number' && validConversationMessages(value.messages)

const validConversationMessageIds = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((id) => typeof id === 'string')

const parseBabysitterSessions = (value: Record<string, unknown>): Record<string, BabysitterSessionState> => {
  const sessions: Record<string, BabysitterSessionState> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (!isRecord(candidate) || !isRecord(candidate.issue)) throw invalidDocument()
    const issue = candidate.issue
    if (
      typeof issue.uuid !== 'string' ||
      typeof issue.key !== 'string' ||
      typeof issue.path !== 'string' ||
      typeof candidate.repo !== 'string' ||
      !Number.isSafeInteger(candidate.prNumber) ||
      (candidate.prNumber as number) < 1 ||
      typeof candidate.agentName !== 'string' ||
      (candidate.path !== undefined && typeof candidate.path !== 'string') ||
      typeof candidate.critical !== 'boolean' ||
      !Array.isArray(candidate.pendingKinds) ||
      !candidate.pendingKinds.every((kind) => typeof kind === 'string') ||
      (candidate.pendingDeliveryClaims !== undefined && (
        !Array.isArray(candidate.pendingDeliveryClaims) ||
        !candidate.pendingDeliveryClaims.every((claim) =>
          isRecord(claim) && typeof claim.deliveryId === 'string' && typeof claim.claimToken === 'string'
        )
      )) ||
      (candidate.resourceSubscription !== undefined && (
        !isRecord(candidate.resourceSubscription) ||
        typeof candidate.resourceSubscription.subscriptionId !== 'string' ||
        typeof candidate.resourceSubscription.provider !== 'string' ||
        typeof candidate.resourceSubscription.resourceRef !== 'string' ||
        typeof candidate.resourceSubscription.subscriberId !== 'string' ||
        typeof candidate.resourceSubscription.ownerId !== 'string' ||
        typeof candidate.resourceSubscription.expiresAt !== 'string' ||
        (candidate.resourceSubscription.terminal !== undefined && typeof candidate.resourceSubscription.terminal !== 'boolean')
      ))
    ) throw invalidDocument()
    sessions[key] = {
      issue: { uuid: issue.uuid, key: issue.key, path: issue.path },
      repo: candidate.repo,
      prNumber: candidate.prNumber as number,
      agentName: candidate.agentName,
      ...(candidate.path === undefined ? {} : { path: candidate.path }),
      critical: candidate.critical,
      pendingKinds: [...candidate.pendingKinds] as string[],
      ...(candidate.resourceSubscription === undefined ? {} : {
        resourceSubscription: {
          subscriptionId: candidate.resourceSubscription.subscriptionId as string,
          provider: candidate.resourceSubscription.provider as string,
          resourceRef: candidate.resourceSubscription.resourceRef as string,
          subscriberId: candidate.resourceSubscription.subscriberId as string,
          ownerId: candidate.resourceSubscription.ownerId as string,
          expiresAt: candidate.resourceSubscription.expiresAt as string,
          ...(candidate.resourceSubscription.terminal === undefined ? {} : { terminal: candidate.resourceSubscription.terminal as boolean }),
        },
      }),
      ...(candidate.pendingDeliveryClaims === undefined ? {} : {
        pendingDeliveryClaims: (candidate.pendingDeliveryClaims as Array<Record<string, unknown>>).map((claim) => ({
          deliveryId: claim.deliveryId as string,
          claimToken: claim.claimToken as string,
        })),
      }),
    }
  }
  return sessions
}

const parseBabysitterGenerations = (
  value: Record<string, unknown>,
): Record<string, BabysitterGenerationRecord> => {
  const generations: Record<string, BabysitterGenerationRecord> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (
      !isRecord(candidate) ||
      typeof candidate.generationId !== 'string' ||
      typeof candidate.agentName !== 'string' ||
      !Number.isSafeInteger(candidate.claimedAtMs) ||
      !Number.isSafeInteger(candidate.leaseUntilMs) ||
      (candidate.phase !== 'claimed' && candidate.phase !== 'completed')
    ) throw invalidDocument()
    generations[key] = {
      generationId: candidate.generationId,
      agentName: candidate.agentName,
      claimedAtMs: candidate.claimedAtMs as number,
      leaseUntilMs: candidate.leaseUntilMs as number,
      phase: candidate.phase,
    }
  }
  return generations
}

const invalidDocument = (): Error => new Error('Factory GitHub watch state file is invalid')

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
