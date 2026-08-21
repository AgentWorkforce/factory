import type {
  DispatchLifecycleAgent,
  BabysitterGenerationRecord,
  BabysitterSessionState,
  ConversationMessage,
  ConversationSessionState,
  DiscoverySweepState,
  DispatchLifecycle,
  GithubIssueCommentWatchState,
  SlackThreadWatchState,
  WaitingClarification,
} from '../ports/state'
import type { AgentSpec, SpawnResult } from '../ports/fleet'
import type { TrackedAgent } from '../orchestrator/batch-tracker'
import type { TriageDecision } from '../types'
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
      const slackWatches = rawWorkspace.slackThreadWatches
      const clarifications = rawWorkspace.waitingClarifications
      const babysitters = rawWorkspace.babysitterSessions
      const generations = rawWorkspace.babysitterGenerations
      const conversations = rawWorkspace.conversationSessions
      const lifecycles = rawWorkspace.dispatchLifecycles
      const discoverySweep = rawWorkspace.discoverySweep
      if (
        !isRecord(watches) ||
        (slackWatches !== undefined && !isRecord(slackWatches)) ||
        !isRecord(clarifications) ||
        (babysitters !== undefined && !isRecord(babysitters)) ||
        (generations !== undefined && !isRecord(generations)) ||
        (conversations !== undefined && !isRecord(conversations)) ||
        (lifecycles !== undefined && !isRecord(lifecycles)) ||
        (discoverySweep !== undefined && !isRecord(discoverySweep))
      ) throw invalidDocument()
      workspaces[workspaceId] = {
        githubIssueCommentWatches: parseGithubIssueCommentWatches(watches),
        slackThreadWatches: parseSlackThreadWatches(slackWatches ?? {}),
        waitingClarifications: parseWaitingClarifications(clarifications),
        babysitterSessions: parseBabysitterSessions(babysitters ?? {}),
        babysitterGenerations: parseBabysitterGenerations(generations ?? {}),
        conversationSessions: parseConversationSessions(conversations ?? {}),
        dispatchLifecycles: parseDispatchLifecycles(lifecycles ?? {}),
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
        githubIssueCommentWatches: parseGithubIssueCommentWatches(watches),
        slackThreadWatches: {},
        waitingClarifications: parseWaitingClarifications(clarifications),
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
        githubIssueCommentWatches: parseGithubIssueCommentWatches(watches),
        slackThreadWatches: {},
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
    if (!isRecord(candidate) || !isRecord(candidate.issue) || !isRecord(candidate.context)) {
      throw invalidDocument()
    }
    const issue = candidate.issue
    const agent = candidate.agent
    const delivery = candidate.delivery
    if (
      typeof issue.uuid !== 'string' || typeof issue.key !== 'string' || typeof issue.path !== 'string' ||
      typeof candidate.provider !== 'string' || typeof candidate.externalId !== 'string' ||
      !Object.values(candidate.context).every((entry) => typeof entry === 'string') ||
      (agent !== undefined && (!isRecord(agent) || typeof agent.name !== 'string' || typeof agent.sessionRef !== 'string')) ||
      !validConversationMessages(candidate.history) || !validConversationMessages(candidate.pending) ||
      (candidate.processedMessageIds !== undefined && !validConversationMessageIds(candidate.processedMessageIds)) ||
      (candidate.acknowledgedMessageIds !== undefined && !validConversationMessageIds(candidate.acknowledgedMessageIds)) ||
      (candidate.acknowledgementClaims !== undefined && !validConversationAcknowledgementClaims(candidate.acknowledgementClaims)) ||
      (candidate.terminalReceipt !== undefined && !validConversationTerminalReceipt(candidate.terminalReceipt)) ||
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
    session.acknowledgedMessageIds = candidate.acknowledgedMessageIds === undefined
      ? session.history.map((message) => message.id)
      : [...candidate.acknowledgedMessageIds as string[]]
    if (candidate.acknowledgementClaims !== undefined) {
      session.acknowledgementClaims = structuredClone(candidate.acknowledgementClaims) as ConversationSessionState['acknowledgementClaims']
    }
    if (candidate.terminalReceipt !== undefined) {
      session.terminalReceipt = structuredClone(candidate.terminalReceipt) as ConversationSessionState['terminalReceipt']
    }
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

const validConversationAcknowledgementClaims = (value: unknown): boolean =>
  isRecord(value) && Object.values(value).every((claim) => isRecord(claim) &&
    typeof claim.claimId === 'string' && typeof claim.claimedAtMs === 'number')

const validConversationTerminalReceipt = (value: unknown): boolean =>
  isRecord(value) && typeof value.claimId === 'string' && typeof value.claimedAtMs === 'number' &&
  (value.posted === undefined || typeof value.posted === 'boolean')

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

const parseGithubIssueCommentWatches = (
  value: Record<string, unknown>,
): Record<string, GithubIssueCommentWatchState> => {
  const watches: Record<string, GithubIssueCommentWatchState> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (
      !isRecord(candidate) || !validIssueRef(candidate.issue) || !isRecord(candidate.source) ||
      typeof candidate.source.owner !== 'string' || typeof candidate.source.repo !== 'string' ||
      !Number.isSafeInteger(candidate.source.number) || (candidate.source.number as number) < 1 ||
      typeof candidate.source.url !== 'string' || !Array.isArray(candidate.pending) ||
      !candidate.pending.every(validGithubWatchPending) ||
      !validOptionalBoolean(candidate.detectAgentQuestions) ||
      !validOptionalString(candidate.sinceCommentId) || !validOptionalString(candidate.lastSeenCommentId) ||
      !validOptionalStringArray(candidate.processedCommentIds)
    ) throw invalidDocument()
    watches[key] = structuredClone(candidate) as unknown as GithubIssueCommentWatchState
  }
  return watches
}

const parseSlackThreadWatches = (
  value: Record<string, unknown>,
): Record<string, SlackThreadWatchState> => {
  const watches: Record<string, SlackThreadWatchState> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (
      !isRecord(candidate) || !validIssueRef(candidate.issue) || !validTriageDecision(candidate.decision) ||
      typeof candidate.threadId !== 'string' ||
      (candidate.kind !== 'triage' && candidate.kind !== 'terminal-grace') ||
      (candidate.kind === 'terminal-grace' && (
        !validNumber(candidate.expiresAtMs) || !validOptionalNumber(candidate.retiredAtMs)
      ))
    ) throw invalidDocument()
    watches[key] = structuredClone(candidate) as unknown as SlackThreadWatchState
  }
  return watches
}

const validGithubWatchPending = (value: unknown): boolean => isRecord(value) &&
  typeof value.correlationId === 'string' &&
  (value.kind === 'triage' || value.kind === 'agent-question') &&
  typeof value.authorizedAuthor === 'string' &&
  (value.decision === undefined || validTriageDecision(value.decision)) &&
  validOptionalString(value.claimedByCommentId) && validOptionalString(value.replyAfterCommentId)

const parseWaitingClarifications = (
  value: Record<string, unknown>,
): Record<string, WaitingClarification> => {
  const clarifications: Record<string, WaitingClarification> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (
      !isRecord(candidate) || !validIssueRef(candidate.issue) || !validTriageDecision(candidate.decision) ||
      typeof candidate.dryRun !== 'boolean' || typeof candidate.askerName !== 'string' ||
      typeof candidate.question !== 'string' || !validNumber(candidate.askedAtMs) ||
      !Array.isArray(candidate.agents) || !candidate.agents.every(validWaitingClarificationAgent) ||
      !validOptionalString(candidate.threadId) ||
      (candidate.questionSource !== undefined && candidate.questionSource !== 'github' && candidate.questionSource !== 'slack') ||
      !validOptionalNumber(candidate.questionPostedAtMs) || !validOptionalNumber(candidate.parkedAtMs) ||
      !validOptionalNumber(candidate.escalatedAtMs) || !validOptionalStringArray(candidate.releasedAgents) ||
      (candidate.questionDelivery !== undefined && !validOwnerClaim(candidate.questionDelivery)) ||
      (candidate.escalation !== undefined && !validOwnerClaim(candidate.escalation)) ||
      (candidate.reply !== undefined && !validClarificationReply(candidate.reply)) ||
      (candidate.wake !== undefined && !validClarificationWake(candidate.wake))
    ) throw invalidDocument()
    clarifications[key] = structuredClone(candidate) as unknown as WaitingClarification
  }
  return clarifications
}

const validWaitingClarificationAgent = (value: unknown): boolean => isRecord(value) &&
  typeof value.name === 'string' && validTrackedAgent(value.tracked)

const validOwnerClaim = (value: unknown): boolean => isRecord(value) &&
  typeof value.owner === 'string' && validNumber(value.claimedAtMs) && validNumber(value.attempts)

const validClarificationReply = (value: unknown): boolean => isRecord(value) &&
  typeof value.id === 'string' && typeof value.text === 'string' && validNumber(value.receivedAtMs) &&
  (value.source === undefined || value.source === 'slack' || value.source === 'github') &&
  validOptionalString(value.author)

const validClarificationWake = (value: unknown): boolean => validOwnerClaim(value) && isRecord(value) &&
  Array.isArray(value.injectedAgents) && value.injectedAgents.every((agent) => typeof agent === 'string')

const parseDispatchLifecycles = (value: Record<string, unknown>): Record<string, DispatchLifecycle> => {
  const lifecycles: Record<string, DispatchLifecycle> = {}
  for (const [key, candidate] of Object.entries(value)) {
    if (!validDispatchLifecycle(candidate)) throw invalidDocument()
    lifecycles[key] = structuredClone(candidate)
  }
  return lifecycles
}

const DISPATCH_LIFECYCLE_PHASES = new Set([
  'queued',
  'dispatching',
  'retryable',
  'abandoning',
  'running',
  'parking',
  'waiting-for-human',
  'publishing',
  'published',
  'writeback-applied',
  'releasing',
  'complete',
  'abandoned',
])

const validDispatchLifecycle = (value: unknown): value is DispatchLifecycle => isRecord(value) &&
  typeof value.runId === 'string' && validIssueRef(value.issue) && validTriageDecision(value.decision) &&
  typeof value.dryRun === 'boolean' && typeof value.phase === 'string' &&
  DISPATCH_LIFECYCLE_PHASES.has(value.phase) &&
  Array.isArray(value.agents) && value.agents.every(validDispatchLifecycleAgent) &&
  Array.isArray(value.invocationIds) && value.invocationIds.every((id) => typeof id === 'string') &&
  validNumber(value.updatedAtMs) && validOptionalNumber(value.heldSinceAtMs) &&
  validOptionalNumber(value.slotHeldSinceAtMs) &&
  validOptionalString(value.releaseReason) &&
  (value.lease === undefined || validLifecycleLease(value.lease)) &&
  (value.result === undefined || validDispatchResult(value.result)) &&
  (value.dispatchClaim === undefined || validDispatchClaimStatus(value.dispatchClaim)) &&
  (value.pullRequest === undefined || validPullRequest(value.pullRequest)) &&
  (value.pullRequests === undefined || (Array.isArray(value.pullRequests) && value.pullRequests.every(validPullRequest))) &&
  (value.ticketDispatchNotification === undefined || validTicketDispatchNotification(value.ticketDispatchNotification)) &&
  (value.cost === undefined || validRunCostTotal(value.cost))

const validDispatchLifecycleAgent = (value: unknown): value is DispatchLifecycleAgent => isRecord(value) &&
  typeof value.name === 'string' && validTrackedAgent(value.tracked) &&
  validOptionalNumber(value.releasedAtMs) &&
  (value.costUsage === undefined || (Array.isArray(value.costUsage) && value.costUsage.every(validAgentUsage)))

const validTrackedAgent = (value: unknown): value is TrackedAgent => isRecord(value) &&
  validAgentSpec(value.spec) && (value.result === undefined || validSpawnResult(value.result)) &&
  validOptionalString(value.sessionRef) && validOptionalString(value.unreachableWakeResumedSessionRef) &&
  validOptionalNumber(value.releasedAtMs)

const validSpawnResult = (value: unknown): value is SpawnResult => isRecord(value) &&
  typeof value.name === 'string' && validOptionalString(value.sessionRef) && validOptionalNumber(value.pid) &&
  (value.pids === undefined || (Array.isArray(value.pids) && value.pids.every(validNumber))) &&
  validOptionalString(value.node) &&
  (value.locality === undefined || value.locality === 'local' || value.locality === 'remote')

const validAgentSpec = (value: unknown): value is AgentSpec => isRecord(value) &&
  typeof value.name === 'string' &&
  (value.role === 'implementer' || value.role === 'reviewer' || value.role === 'babysitter' || value.role === 'workflow') &&
  (value.capability === 'spawn:codex' || value.capability === 'spawn:claude' || value.capability === 'workflow:run') &&
  typeof value.task === 'string' && typeof value.repo === 'string' &&
  validOptionalString(value.principal) && validOptionalString(value.owner) && validOptionalString(value.model) &&
  validOptionalString(value.workflow) && validOptionalString(value.baseClonePath) &&
  validOptionalString(value.clonePath) && validOptionalString(value.channel) && validOptionalString(value.node) &&
  validOptionalString(value.sessionRef) && validOptionalString(value.invocationId) &&
  validOptionalString(value.branch) && validOptionalBoolean(value.existingPullRequestBranch) &&
  (value.inputs === undefined || isRecord(value.inputs)) &&
  (value.ownedPullRequest === undefined || validOwnedPullRequest(value.ownedPullRequest)) &&
  (value.pendingPullRequestWake === undefined || validPendingPullRequestWake(value.pendingPullRequestWake)) &&
  (value.preview === undefined || validPreviewReference(value.preview))

const validPreviewReference = (value: unknown): boolean => isRecord(value) &&
  typeof value.id === 'string' && value.provider === 'tailscale-serve' &&
  typeof value.namespace === 'string' && typeof value.owner === 'string' &&
  typeof value.service === 'string' && typeof value.repo === 'string' &&
  typeof value.url === 'string' && validOptionalPort(value.configuredTargetPort) &&
  validPort(value.targetPort) && validPort(value.httpsPort) &&
  value.access === 'tailnet' && value.lifetime === 'issue' &&
  typeof value.createdAt === 'string' && typeof value.startCommand === 'string' &&
  (value.process === undefined || validPreviewProcessReference(value.process)) &&
  validOptionalString(value.node)

const validPreviewProcessReference = (value: unknown): boolean => isRecord(value) &&
  Number.isSafeInteger(value.pid) && (value.pid as number) > 0 &&
  typeof value.startTime === 'string' && typeof value.cmdline === 'string' &&
  typeof value.cwd === 'string' && typeof value.marker === 'string'

const validOwnedPullRequest = (value: unknown): boolean => isRecord(value) &&
  typeof value.repo === 'string' && Number.isSafeInteger(value.number) && (value.number as number) > 0 &&
  validOptionalString(value.path)

const validPendingPullRequestWake = (value: unknown): boolean => isRecord(value) &&
  typeof value.repo === 'string' && Number.isSafeInteger(value.number) && (value.number as number) > 0 &&
  Array.isArray(value.kinds) && value.kinds.every((kind) => typeof kind === 'string')

// A scope this validator does not know rejects the entire document, so one
// swarm-scoped issue would cost every watch in the workspace on restart. Keyed
// on the union itself, a new scope is a compile error here instead.
const triageScopes: Record<TriageDecision['scope'], true> = {
  single: true,
  workflow: true,
  team: true,
  swarm: true,
}

const validTriageDecision = (value: unknown): value is TriageDecision => isRecord(value) &&
  validIssueRef(value.issue) && Array.isArray(value.routes) && value.routes.every(validRoute) &&
  (typeof value.scope === 'string' && Object.hasOwn(triageScopes, value.scope)) &&
  Array.isArray(value.implementers) && value.implementers.every(validAgentSpec) &&
  (value.workflow === undefined || validAgentSpec(value.workflow)) && validAgentSpec(value.reviewer) &&
  typeof value.thin === 'boolean' && (value.confidence === 'high' || value.confidence === 'low') &&
  typeof value.rationale === 'string' &&
  (value.issueResolution === undefined || isRecord(value.issueResolution))

const validRoute = (value: unknown): boolean => isRecord(value) &&
  typeof value.repo === 'string' && typeof value.rationale === 'string' && validOptionalString(value.clonePath)

const validDispatchResult = (value: unknown): boolean => isRecord(value) && validIssueRef(value.issue) &&
  (value.issueResolution === undefined || validIssueResolution(value.issueResolution)) &&
  Array.isArray(value.agents) && value.agents.every((agent) => isRecord(agent) &&
    typeof agent.name === 'string' && validAgentRole(agent.role)) &&
  validOptionalStringArray(value.comments) && validOptionalString(value.stateId) &&
  (value.previews === undefined || (Array.isArray(value.previews) && value.previews.every(validPreviewReference))) &&
  typeof value.dryRun === 'boolean' && (value.hold === undefined || validDispatchHold(value.hold))

const validDispatchHold = (value: unknown): boolean => isRecord(value) &&
  (value.kind === 'capacity' || value.kind === 'dependency' || value.kind === 'dependency-cycle') &&
  validOptionalStringArray(value.blockers) && validOptionalStringArray(value.cycle)

const validIssueResolution = (value: unknown): boolean => isRecord(value) &&
  (value.source === 'relayfile-projection' || value.source === 'github-api-fallback') &&
  validOptionalString(value.repo) && typeof value.detail === 'string' &&
  isRecord(value.projection) &&
  (value.projection.outcome === 'matched' || value.projection.outcome === 'no-match') &&
  validOptionalBoolean(value.projection.localMountDegraded) &&
  validOptionalString(value.projection.localMountDegradedReason) &&
  (value.projection.eventListener === undefined || validEventListenerStatus(value.projection.eventListener)) &&
  (value.projection.githubConnection === undefined || validGithubConnection(value.projection.githubConnection))

const validEventListenerStatus = (value: unknown): boolean => isRecord(value) &&
  (value.state === 'starting' || value.state === 'subscribed' || value.state === 'polling' ||
    value.state === 'not-listening' || value.state === 'unknown') &&
  validOptionalString(value.reason)

const validGithubConnection = (value: unknown): boolean => isRecord(value) &&
  typeof value.ready === 'boolean' && validOptionalString(value.state) &&
  validOptionalString(value.initialSyncState)

const validDispatchClaimStatus = (value: unknown): boolean => isRecord(value) &&
  (value.state === 'pending' || value.state === 'verified' || value.state === 'degraded') &&
  validNumber(value.updatedAtMs) && validOptionalString(value.write) && validOptionalString(value.error) &&
  validOptionalNumber(value.attempts) && validOptionalNumber(value.maxAttempts) &&
  validOptionalBoolean(value.deadLettered)

const validPullRequest = (value: unknown): boolean => isRecord(value) &&
  typeof value.repo === 'string' && Number.isSafeInteger(value.number) && (value.number as number) > 0 &&
  typeof value.url === 'string' && typeof value.headRef === 'string' &&
  validOptionalString(value.headSha) && validOptionalString(value.author)

const validTicketDispatchNotification = (value: unknown): boolean => isRecord(value) &&
  typeof value.workUnitId === 'string' && validNumber(value.claimedAtMs)

const validLifecycleLease = (value: unknown): boolean => isRecord(value) &&
  typeof value.owner === 'string' && Number.isSafeInteger(value.epoch) && validNumber(value.leaseUntilMs)

const validAgentUsage = (value: unknown): boolean => isRecord(value) && typeof value.model === 'string' &&
  validNullableNumber(value.inputTokens) && validNullableNumber(value.outputTokens)

const validRunCostTotal = (value: unknown): boolean => isRecord(value) && typeof value.runId === 'string' &&
  validNullableNumber(value.inputTokens) && validNullableNumber(value.outputTokens) &&
  validNullableNumber(value.usd) && Array.isArray(value.byRole) && value.byRole.every(validCostRoleBreakdown)

const validCostRoleBreakdown = (value: unknown): boolean => isRecord(value) &&
  validCostLedgerRole(value.role) && validNullableNumber(value.inputTokens) &&
  validNullableNumber(value.outputTokens) && validNullableNumber(value.usd) &&
  Array.isArray(value.byModel) && value.byModel.every(validCostModelBreakdown)

const validCostModelBreakdown = (value: unknown): boolean => isRecord(value) &&
  typeof value.model === 'string' && validNullableNumber(value.inputTokens) &&
  validNullableNumber(value.outputTokens) && validNullableNumber(value.usd)

const validAgentRole = (value: unknown): boolean => value === 'implementer' || value === 'reviewer' ||
  value === 'babysitter' || value === 'workflow'

const validCostLedgerRole = (value: unknown): boolean => validAgentRole(value) || value === 'triage'

const validIssueRef = (value: unknown): boolean => isRecord(value) &&
  typeof value.uuid === 'string' && typeof value.key === 'string' && typeof value.path === 'string'

const validNumber = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value)
const validNullableNumber = (value: unknown): boolean => value === null || validNumber(value)
const validOptionalNumber = (value: unknown): boolean => value === undefined || validNumber(value)
const validOptionalString = (value: unknown): boolean => value === undefined || typeof value === 'string'
const validOptionalBoolean = (value: unknown): boolean => value === undefined || typeof value === 'boolean'
const validOptionalStringArray = (value: unknown): boolean => value === undefined ||
  (Array.isArray(value) && value.every((entry) => typeof entry === 'string'))
const validPort = (value: unknown): boolean => Number.isSafeInteger(value) &&
  (value as number) > 0 && (value as number) <= 65_535
const validOptionalPort = (value: unknown): boolean => value === undefined || validPort(value)

const invalidDocument = (): Error => new Error('Factory GitHub watch state file is invalid')

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
