import { createHash, randomUUID } from 'node:crypto'

import type {
  RelayfileCredentials,
  WorkforceCtx,
  WorkforceEvent,
} from '@agentworkforce/runtime'
import { defineAgent } from '@agentworkforce/runtime'
import {
  RelayFileApiError,
  RelayFileClient,
  RevisionConflictError,
  type OperationStatusResponse,
} from '@relayfile/sdk'
import {
  githubClient,
  slackClient,
  type GithubClient,
  type SlackClient,
  type WritebackResult,
} from '@relayfile/relay-helpers'

export const GUARDIAN_CONVERSATION_STATE_PATH =
  '/memory/workspace/factory-feature-guardian/conversations.json'
export const GUARDIAN_CONFIRMATIONS_PATH =
  '/memory/workspace/factory-feature-guardian/confirmations'
export const GUARDIAN_REMEDIATIONS_PATH =
  '/memory/workspace/factory-feature-guardian/remediations'

export const GUARDIAN_STATE_TIMEOUT_MS = 5_000
export const GUARDIAN_WRITEBACK_TIMEOUT_MS = 15_000
export const GUARDIAN_WRITEBACK_POLL_MS = 250

const MAX_STATE_BYTES = 256 * 1024
const MAX_CONVERSATIONS = 512
const MAX_SEEN_EVENTS = 128
const MAX_EVIDENCE_ITEMS = 32
const MAX_TURNS = 12
const UTF8_ENCODER = new TextEncoder()

export type GuardianConversationStatus =
  | 'asked'
  | 'discussing'
  | 'verifying'
  | 'confirmation-recorded'
  | 'remediation-recorded'
  | 'deferred-recorded'
  | 'confirmed'
  | 'remediation-open'
  | 'deferred'

export type GuardianResponseKind =
  | 'affirmative'
  | 'failure'
  | 'untested'
  | 'ambiguous'
  | 'deferred'

export type GuardianDefectKind =
  | 'implementation'
  | 'test'
  | 'manifest'
  | 'procedure'
  | 'documentation'

export type GuardianProcedureResultKind = 'passed' | 'failed' | 'skip' | 'manual'

export interface GuardianFeatureSnapshot {
  id: string
  name: string
  category: string
  cli?: string
  api?: string
  description: string
  locations: string[]
  procedure: string
  tier: number
  criticality: string
}

export interface GuardianEvidence {
  source: 'actor' | 'procedure' | 'system'
  result: 'positive' | 'negative' | 'skip' | 'manual' | 'unknown'
  summary: string
  commands?: string[]
  positiveAssertions?: string[]
  negativeAssertions?: string[]
  tests?: { passed: number; failed: number }
  cleanup?: string[]
}

export interface GuardianProcedureResult extends GuardianEvidence {
  source: 'procedure'
  outcome: GuardianProcedureResultKind
  verifier: string
}

export interface GuardianQuestion {
  feature: GuardianFeatureSnapshot
  manifestRevision: string
  manifestVersion: string
  procedureRevision: string
  generation: number
  channelId: string
  threadTs: string
  askedAt: string
}

export interface GuardianPendingTurn {
  eventId: string
  eventOrder: string
  eventOccurredAt: string
  actorId: string
  response: GuardianResponseKind
  text: string
  reaction?: string
  evidence?: GuardianEvidence
  verification?: GuardianProcedureResult
  defectKind?: GuardianDefectKind
}

export interface GuardianIssueReceipt {
  repository: string
  number: number
  url: string
  dedupeKey: string
}

export interface GuardianConversationRecord extends GuardianQuestion {
  id: string
  status: GuardianConversationStatus
  turnCount: number
  clarificationCount: number
  seenEventIds: string[]
  evidence: GuardianEvidence[]
  pending?: GuardianPendingTurn
  confirmationPath?: string
  issue?: GuardianIssueReceipt
  finalReplyTs?: string
  lastProcessedEventOrder?: string
  updatedAt: string
}

export interface GuardianConversationState {
  kind: 'feature-guardian:conversations'
  version: 1
  records: GuardianConversationRecord[]
}

export interface GuardianConfirmationRecord {
  kind: 'feature-guardian:confirmation'
  version: 1
  featureId: string
  manifestRevision: string
  manifestVersion: string
  procedureRevision: string
  generation: number
  result: 'confirmed'
  actor?: { id: string }
  verifier: string
  timestamp: string
  evidence: GuardianEvidence[]
  commands: string[]
  tests: { passed: number; failed: number }
  slack: {
    channelId: string
    threadTs: string
    questionTs: string
  }
}

export interface GuardianManifestCatalog {
  manifestRevision: string
  manifestVersion: string
  procedureRevision: string
  features: GuardianFeatureSnapshot[]
}

export interface GuardianProcedure {
  name: string
  path: string
  prerequisites: string
  body: string
}

export interface GuardianTierGate {
  outcome: 'available' | 'skip' | 'manual'
  reason: string
}

export interface GuardianIssuePolicy {
  repository: string
  title: string
  body: string
  labels: string[]
  defectKind: GuardianDefectKind
  dedupeKey: string
}

export interface FeatureGuardianAdapters {
  loadCatalog(ctx: WorkforceCtx): Promise<GuardianManifestCatalog>
  resolveProcedure(
    ctx: WorkforceCtx,
    feature: GuardianFeatureSnapshot,
  ): Promise<GuardianProcedure>
  gateTier(
    ctx: WorkforceCtx,
    feature: GuardianFeatureSnapshot,
    procedure: GuardianProcedure,
  ): Promise<GuardianTierGate>
  runProcedure(
    ctx: WorkforceCtx,
    feature: GuardianFeatureSnapshot,
    procedure: GuardianProcedure,
    runKey: string,
  ): Promise<GuardianProcedureResult>
  isAuthorizedConfirmer(ctx: WorkforceCtx, actorId: string): boolean
  clarification(
    feature: GuardianFeatureSnapshot,
    procedure: GuardianProcedure,
    turn: GuardianPendingTurn,
    turnNumber: number,
  ): string
  classifyDefect(
    feature: GuardianFeatureSnapshot,
    turn: GuardianPendingTurn,
    evidence: readonly GuardianEvidence[],
  ): GuardianDefectKind
  repositoryForFeature(feature: GuardianFeatureSnapshot): string
  issuePolicy(input: {
    feature: GuardianFeatureSnapshot
    conversation: GuardianConversationRecord
    defectKind: GuardianDefectKind
    slackBacklink: string
  }): GuardianIssuePolicy
}

export interface ConversationSnapshot {
  state: GuardianConversationState
  revision: string
}

export interface ConversationStore {
  load(): Promise<ConversationSnapshot | null>
  save(
    state: GuardianConversationState,
    expected: ConversationSnapshot | null,
  ): Promise<ConversationSnapshot>
}

export interface ImmutableConfirmationStore {
  append(record: GuardianConfirmationRecord): Promise<string>
}

export interface GuardianIssueWriter {
  upsert(policy: GuardianIssuePolicy): Promise<GuardianIssueReceipt>
}

export interface GuardianConversationDependencies {
  conversationStore?: ConversationStore
  confirmationStore?: ImmutableConfirmationStore
  issueWriter?: GuardianIssueWriter
  createSlackClient?: typeof slackClient
  now?: () => Date
}

export interface DefineFeatureGuardianAgentOptions {
  adapters: FeatureGuardianAdapters
  scheduled: (ctx: WorkforceCtx, event: WorkforceEvent) => Promise<void> | void
  channelInput?: string
  schedules?: ReadonlyArray<{ name: string; cron: string; tz?: string }>
}

export class GuardianStateConflictError extends Error {
  constructor(message = 'feature guardian conversation state revision conflict') {
    super(message)
    this.name = 'GuardianStateConflictError'
  }
}

/**
 * Reusable feature-guardian factory. Repository adopters provide their catalog,
 * procedures, gates, prompt, authority, and issue policy; this factory owns the
 * scoped Slack listeners and dispatches every response through the shared state
 * machine.
 */
export function defineFeatureGuardianAgent(options: DefineFeatureGuardianAgentOptions) {
  const channelInput = options.channelInput ?? 'SLACK_CHANNEL'
  const paths = [`/slack/channels/\${${channelInput}}/messages/**`]
  return defineAgent({
    triggers: {
      slack: [
        { on: 'message.created', paths, maxConcurrency: 1 },
        { on: 'reaction.added', paths, maxConcurrency: 1 },
      ],
    },
    schedules: options.schedules ?? [
      { name: 'hourly-check', cron: '0 * * * *', tz: 'America/New_York' },
    ],
    handler: async (ctx, event) => {
      if (event.type === 'cron.tick') {
        await options.scheduled(ctx, event)
        return
      }
      await runGuardianConversationTurn(ctx, event, options.adapters)
    },
  })
}

type FetchLike = typeof fetch

/** A stable content revision suitable for binding a question to exact source text. */
export function guardianContentRevision(content: string): string {
  return `sha256:${createHash('sha256').update(content).digest('hex')}`
}

/** Stable identity for one question in one manifest generation. */
export function guardianConversationId(question: GuardianQuestion): string {
  return [
    question.feature.id,
    question.manifestRevision,
    `g${question.generation}`,
    question.channelId,
    question.threadTs,
  ].join(':')
}

/** Stable key shared by retries of one threaded response. */
export function guardianReplyIdempotencyKey(
  record: Pick<GuardianConversationRecord, 'id'>,
  eventId: string,
  purpose: string,
): string {
  return `feature-guardian:${record.id}:${eventId}:${purpose}`
}

/** Immutable confirmation path. The filename is a hash so provider IDs never become paths. */
export function guardianConfirmationPath(record: GuardianConfirmationRecord): string {
  const identity = [
    record.featureId,
    record.manifestRevision,
    `g${record.generation}`,
    record.slack.channelId,
    record.slack.threadTs,
  ].join(':')
  return `${GUARDIAN_CONFIRMATIONS_PATH}/${createHash('sha256').update(identity).digest('hex')}.json`
}

/** Parse checkmark/wrench/question reactions and conversational text without an LLM guess. */
export function classifyGuardianResponse(input: {
  text?: string
  reaction?: string
}): GuardianResponseKind {
  const reaction = (input.reaction ?? '').trim().toLowerCase().replaceAll('-', '_')
  if (['white_check_mark', 'heavy_check_mark', 'ballot_box_with_check', 'check'].includes(reaction)) {
    return 'affirmative'
  }
  if (['wrench', 'hammer_and_wrench', 'hammer', 'x'].includes(reaction)) return 'failure'
  if (['question', 'grey_question', 'interrobang'].includes(reaction)) return 'untested'

  const text = (input.text ?? '').trim().toLowerCase()
  if (!text) return 'ambiguous'
  if (/\b(defer(?:red)?|manual|skip(?:ped)?|cannot run|can't run|blocked by credentials)\b/u.test(text)) {
    return 'deferred'
  }
  if (/\b(untested|not tested|haven't tested|have not tested|unknown|unsure|don't know|do not know)\b/u.test(text)) {
    return 'untested'
  }
  if (
    /\b(?:does(?:n't| not)|is(?:n't| not)|not)\s+(?:work(?:ing)?|pass(?:ing)?)\b/u.test(text)
  ) {
    return 'failure'
  }
  if (/^(?:not broken|not failing|no problems?|no issues?|works? for me)[.!\s]*$/u.test(text)) {
    return 'affirmative'
  }
  if (/\b(broken|breaks?|failing|fails?|failed|regression|drift(?:ed)?|wrong|bug|problem|off)\b/u.test(text)) {
    return 'failure'
  }
  if (/^(?:yes|yep|yeah|confirmed|works?|working|passes?|passed|all good|looks good|✅)[.!\s]*$/u.test(text)) {
    return 'affirmative'
  }
  if (/\b(tested|verified|confirmed)\b/u.test(text) && /\b(works?|working|passes?|passed|expected|good)\b/u.test(text)) {
    return 'affirmative'
  }
  return 'ambiguous'
}

interface ParsedSlackResponse {
  eventId: string
  eventOrder: string
  occurredAt: string
  channelId: string
  threadTs: string
  messageTs: string
  actorId: string
  text: string
  reaction?: string
  isReaction: boolean
  isBot: boolean
}

/** Expand and fail-closed parse one Slack reply/reaction event. */
export async function parseGuardianSlackEvent(
  event: WorkforceEvent,
): Promise<ParsedSlackResponse | null> {
  if (!event.type.startsWith('slack.')) return null
  const envelopeId = nonEmptyString(event.id)
  if (!envelopeId || typeof event.expand !== 'function') return null
  const expanded = await event.expand('full')
  const root = asRecord(expanded?.data)
  const payload = findSlackPayload(root)
  if (!payload) return null

  const item = asRecord(payload.item)
  const isReaction = event.type.includes('reaction') || nonEmptyString(payload.reaction) !== undefined
  const channelId =
    nonEmptyString(payload.channel) ??
    nonEmptyString(payload.channel_id) ??
    nonEmptyString(item?.channel) ??
    channelFromPath(event.resource?.path)
  const messageTs =
    nonEmptyString(payload.ts) ??
    nonEmptyString(payload.message_ts) ??
    nonEmptyString(item?.ts) ??
    messageTsFromPath(event.resource?.path)
  const threadTs = isReaction
    ? messageTs
    : nonEmptyString(payload.thread_ts) ?? nonEmptyString(payload.threadTs) ?? ''
  const actorId =
    nonEmptyString(payload.user) ??
    nonEmptyString(payload.user_id) ??
    nonEmptyString(event.summary?.actor?.id)
  if (!channelId || !messageTs || !threadTs || !actorId) return null
  if (!isReaction && messageTs === threadTs) return null
  const occurredAt =
    canonicalTimestamp(event.occurredAt) ??
    timestampFromSlackTs(nonEmptyString(payload.event_ts) ?? messageTs)
  if (!occurredAt) return null
  const providerIdentity = isReaction
    ? nonEmptyString(payload.event_ts) ?? envelopeId
    : messageTs
  const eventId = `slack:${createHash('sha256')
    .update(
      [
        channelId,
        threadTs,
        messageTs,
        actorId,
        nonEmptyString(payload.reaction) ?? '',
        providerIdentity,
      ].join(':'),
    )
    .digest('hex')}`
  const eventOrder = [occurredAt, messageTs, eventId].join(':')

  return {
    eventId,
    eventOrder,
    occurredAt,
    channelId,
    threadTs,
    messageTs,
    actorId,
    text: nonEmptyString(payload.text) ?? '',
    ...(nonEmptyString(payload.reaction) ? { reaction: nonEmptyString(payload.reaction) } : {}),
    isReaction,
    isBot:
      payload.user_is_bot === true ||
      Boolean(nonEmptyString(payload.bot_id)) ||
      nonEmptyString(payload.subtype) === 'bot_message',
  }
}

/** Register a provider-confirmed question before the scheduled cycle advances. */
export async function registerGuardianQuestion(
  ctx: WorkforceCtx,
  question: GuardianQuestion,
  dependencies: Pick<GuardianConversationDependencies, 'conversationStore' | 'now'> = {},
): Promise<GuardianConversationRecord> {
  const store = dependencies.conversationStore ?? createConversationStore(ctx)
  const now = (dependencies.now ?? (() => new Date()))().toISOString()
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const snapshot = await store.load()
    const state = snapshot?.state ?? emptyConversationState()
    const id = guardianConversationId(question)
    const existing = state.records.find((record) => record.id === id)
    if (existing) {
      if (!sameQuestion(existing, question)) {
        throw new Error('guardian question identity collided with different question data')
      }
      return existing
    }
    const record: GuardianConversationRecord = {
      ...question,
      id,
      status: 'asked',
      turnCount: 0,
      clarificationCount: 0,
      seenEventIds: [],
      evidence: [],
      updatedAt: now,
    }
    const retained = retainConversationCapacity(state.records, record)
    try {
      await store.save({ ...state, records: [...retained, record] }, snapshot)
      return record
    } catch (error) {
      if (!(error instanceof GuardianStateConflictError) || attempt === 2) throw error
    }
  }
  throw new Error('unreachable guardian question registration retry')
}

/** Own one correlated Slack response through a durable or explicitly deferred outcome. */
export async function runGuardianConversationTurn(
  ctx: WorkforceCtx,
  event: WorkforceEvent,
  adapters: FeatureGuardianAdapters,
  dependencies: GuardianConversationDependencies = {},
): Promise<void> {
  const parsed = await parseGuardianSlackEvent(event)
  if (!parsed || parsed.isBot) return
  const configuredChannel = resolvedInput(ctx, 'SLACK_CHANNEL')
  if (!configuredChannel || parsed.channelId !== configuredChannel) {
    ctx.log('warn', 'feature-guardian.event-rejected', {
      reason: 'unauthorized-channel',
      channel: parsed.channelId,
    })
    return
  }
  if (parsed.actorId === resolvedInput(ctx, 'SLACK_BOT_USER')) {
    ctx.log('info', 'feature-guardian.event-ignored', { reason: 'configured-bot-actor' })
    return
  }

  const store = dependencies.conversationStore ?? createConversationStore(ctx)
  const confirmationStore = dependencies.confirmationStore ?? createConfirmationStore(ctx)
  const issueWriter = dependencies.issueWriter ?? createGithubIssueWriter(ctx)
  const createSlackClient = dependencies.createSlackClient ?? slackClient
  const now = dependencies.now ?? (() => new Date())

  let snapshot = await store.load()
  if (!snapshot) return
  const correlated = findCorrelatedConversation(snapshot.state, parsed)
  if (!correlated) {
    ctx.log('info', 'feature-guardian.event-ignored', {
      reason: 'unrelated-thread',
      channel: parsed.channelId,
      threadTs: parsed.threadTs,
      messageTs: parsed.messageTs,
    })
    return
  }
  let record: GuardianConversationRecord = correlated
  if (isTerminal(record.status)) return
  if (Date.parse(parsed.occurredAt) < Date.parse(record.askedAt)) {
    ctx.log('info', 'feature-guardian.event-ignored', {
      reason: 'event-predates-question',
      eventId: parsed.eventId,
      conversationId: record.id,
    })
    return
  }

  // A durable pending turn always owns the conversation. Resume it before
  // considering a later delivery so a restart or out-of-order event cannot
  // overwrite procedure evidence, a confirmation write, or an issue receipt.
  const pendingBeforeResume = record.pending
  if (pendingBeforeResume) {
    await resumePendingTurn()
    if (isTerminal(record.status) || record.pending) return
    if (pendingBeforeResume.eventId === parsed.eventId || record.seenEventIds.includes(parsed.eventId)) {
      return
    }
    if (Date.parse(parsed.occurredAt) <= Date.parse(pendingBeforeResume.eventOccurredAt)) {
      ctx.log('info', 'feature-guardian.event-ignored', {
        reason: 'superseded-out-of-order-event',
        eventId: parsed.eventId,
        conversationId: record.id,
      })
      return
    }
  }
  if (record.seenEventIds.includes(parsed.eventId)) return
  if (
    record.lastProcessedEventOrder &&
    parsed.eventOrder <= record.lastProcessedEventOrder
  ) {
    ctx.log('info', 'feature-guardian.event-ignored', {
      reason: 'delayed-event-precedes-conversation-watermark',
      eventId: parsed.eventId,
      conversationId: record.id,
    })
    return
  }

  const catalog = await adapters.loadCatalog(ctx)
  if (
    catalog.manifestRevision !== record.manifestRevision ||
    catalog.procedureRevision !== record.procedureRevision
  ) {
    const turn = pendingTurn(parsed, 'deferred', {
      source: 'system',
      result: 'manual',
      summary: 'The manifest or procedure changed after this question was posted; refusing to confirm a different revision.',
    })
    snapshot = await checkpointPending(snapshot, record, turn, 'deferred-recorded')
    record = requireRecordById(snapshot.state, record.id)
    await finishDeferred(record, turn.evidence?.summary ?? 'Revision changed before verification.')
    return
  }

  const response = classifyGuardianResponse({ text: parsed.text, reaction: parsed.reaction })
  const actorEvidence: GuardianEvidence = {
    source: 'actor',
    result:
      response === 'affirmative'
        ? 'positive'
        : response === 'failure'
          ? 'negative'
          : response === 'deferred'
            ? 'manual'
            : 'unknown',
    summary: parsed.reaction
      ? `Slack reaction :${parsed.reaction}: from ${parsed.actorId}`
      : `Slack reply from ${parsed.actorId}: ${bounded(parsed.text, 1_000)}`,
  }
  const turn = pendingTurn(parsed, response, actorEvidence)

  if (response === 'ambiguous') {
    await clarify(turn)
    return
  }

  if ((response === 'failure' || response === 'untested') && record.turnCount === 0) {
    await clarify(turn)
    return
  }

  if (response === 'affirmative' && adapters.isAuthorizedConfirmer(ctx, parsed.actorId)) {
    snapshot = await checkpointPending(snapshot, record, turn, 'confirmation-recorded', true)
    record = requireRecordById(snapshot.state, record.id)
    await finishConfirmation(record, turn)
    return
  }

  if (response === 'deferred') {
    snapshot = await checkpointPending(snapshot, record, turn, 'deferred-recorded', true)
    record = requireRecordById(snapshot.state, record.id)
    await finishDeferred(record, actorEvidence.summary)
    return
  }

  const procedure = await adapters.resolveProcedure(ctx, record.feature)
  const gate = await adapters.gateTier(ctx, record.feature, procedure)
  if (gate.outcome !== 'available') {
    const gateEvidence: GuardianEvidence = {
      source: 'system',
      result: gate.outcome,
      summary: gate.reason,
    }
    const deferredTurn = { ...turn, response: 'deferred' as const, evidence: gateEvidence }
    snapshot = await checkpointPending(snapshot, record, deferredTurn, 'deferred-recorded', true)
    record = requireRecordById(snapshot.state, record.id)
    await finishDeferred(record, gate.reason)
    return
  }

  snapshot = await checkpointPending(snapshot, record, turn, 'verifying', true)
  record = requireRecordById(snapshot.state, record.id)
  await resumePendingTurn(procedure)

  async function resumePendingTurn(preResolvedProcedure?: GuardianProcedure): Promise<void> {
    if (!snapshot || !record.pending) return
    const pending = record.pending
    if (record.status === 'confirmation-recorded') {
      await finishConfirmation(record, pending)
      return
    }
    if (record.status === 'remediation-recorded') {
      if (!record.issue) throw new Error('remediation-recorded state is missing its issue receipt')
      await finishRemediation(record, record.issue)
      return
    }
    if (record.status === 'deferred-recorded') {
      await finishDeferred(record, pending.evidence?.summary ?? 'Verification deferred.')
      return
    }
    if (record.status !== 'verifying') return

    const procedure = preResolvedProcedure ?? await adapters.resolveProcedure(ctx, record.feature)
    const result = pending.verification ?? await adapters.runProcedure(
      ctx,
      record.feature,
      procedure,
      guardianReplyIdempotencyKey(record, pending.eventId, 'procedure'),
    )
    if (!pending.verification) {
      const next = replaceRecord(snapshot.state, record.id, {
        ...record,
        pending: { ...pending, verification: result },
        evidence: appendEvidence(record.evidence, result),
        updatedAt: now().toISOString(),
      })
      snapshot = await store.save(next, snapshot)
      record = requireRecordById(snapshot.state, record.id)
    }
    const evidence = record.evidence
    if (result.outcome === 'passed') {
      if (pending.response === 'failure') {
        const reportedDefect = adapters.classifyDefect(record.feature, pending, evidence)
        if (
          record.clarificationCount >= 1 &&
          ['test', 'manifest', 'procedure', 'documentation'].includes(reportedDefect)
        ) {
          await recordRemediation(result, reportedDefect)
          return
        }
        const clarificationNumber = record.clarificationCount + 1
        const text = adapters.clarification(
          record.feature,
          procedure,
          { ...pending, evidence: result, verification: result },
          clarificationNumber,
        )
        const receipt = await postThreadReply(
          createSlackClient,
          record,
          text,
          guardianReplyIdempotencyKey(record, pending.eventId, `post-check-clarification-${clarificationNumber}`),
        )
        const next = replaceRecord(snapshot.state, record.id, {
          ...record,
          status: 'discussing',
          clarificationCount: clarificationNumber,
          seenEventIds: appendSeen(record.seenEventIds, pending.eventId),
          lastProcessedEventOrder: pending.eventOrder,
          pending: undefined,
          finalReplyTs: receipt,
          updatedAt: now().toISOString(),
        })
        snapshot = await store.save(next, snapshot)
        record = requireRecordById(snapshot.state, record.id)
        return
      }
      const next = replaceRecord(snapshot.state, record.id, {
        ...record,
        status: 'confirmation-recorded',
        updatedAt: now().toISOString(),
      })
      snapshot = await store.save(next, snapshot)
      record = requireRecordById(snapshot.state, record.id)
      await finishConfirmation(record, pending)
      return
    }
    if (result.outcome === 'skip' || result.outcome === 'manual') {
      const next = replaceRecord(snapshot.state, record.id, {
        ...record,
        status: 'deferred-recorded',
        pending: { ...pending, response: 'deferred', evidence: result },
        updatedAt: now().toISOString(),
      })
      snapshot = await store.save(next, snapshot)
      record = requireRecordById(snapshot.state, record.id)
      await finishDeferred(record, result.summary)
      return
    }

    const defectKind = adapters.classifyDefect(record.feature, pending, evidence)
    await recordRemediation(result, defectKind)
  }

  async function recordRemediation(
    result: GuardianProcedureResult,
    defectKind: GuardianDefectKind,
  ): Promise<void> {
    if (!snapshot || !record.pending) return
    const pending = record.pending
    const evidence = record.evidence
    const policy = adapters.issuePolicy({
      feature: record.feature,
      conversation: { ...record, evidence },
      defectKind,
      slackBacklink: slackThreadBacklink(record.channelId, record.threadTs),
    })
    const routedRepository = adapters.repositoryForFeature(record.feature)
    if (policy.repository !== routedRepository) {
      throw new Error(
        `guardian issue policy routed ${policy.repository}, expected ${routedRepository}`,
      )
    }
    const receipt = await issueWriter.upsert(policy)
    if (receipt.repository !== routedRepository || receipt.dedupeKey !== policy.dedupeKey) {
      throw new Error('guardian issue receipt did not match the authorized repository/dedupe key')
    }
    const next = replaceRecord(snapshot.state, record.id, {
      ...record,
      status: 'remediation-recorded',
      issue: receipt,
      pending: { ...pending, defectKind, evidence: result },
      updatedAt: now().toISOString(),
    })
    snapshot = await store.save(next, snapshot)
    record = requireRecordById(snapshot.state, record.id)
    await finishRemediation(record, receipt)
  }

  async function clarify(turn: GuardianPendingTurn): Promise<void> {
    if (!snapshot) return
    if (record.turnCount >= MAX_TURNS) {
      snapshot = await checkpointPending(snapshot, record, {
        ...turn,
        response: 'deferred',
        evidence: {
          source: 'system',
          result: 'manual',
          summary: `Clarification stopped after the bounded ${MAX_TURNS}-turn limit.`,
        },
      }, 'deferred-recorded', true)
      record = requireRecordById(snapshot.state, record.id)
      await finishDeferred(record, `Clarification stopped after ${MAX_TURNS} turns.`)
      return
    }
    const procedure = await adapters.resolveProcedure(ctx, record.feature)
    const clarificationNumber = record.clarificationCount + 1
    const text = adapters.clarification(record.feature, procedure, turn, clarificationNumber)
    const receipt = await postThreadReply(
      createSlackClient,
      record,
      text,
      guardianReplyIdempotencyKey(record, turn.eventId, `clarification-${clarificationNumber}`),
    )
    const nextRecord: GuardianConversationRecord = {
      ...record,
      status: 'discussing',
      turnCount: record.turnCount + 1,
      clarificationCount: clarificationNumber,
      seenEventIds: appendSeen(record.seenEventIds, turn.eventId),
      lastProcessedEventOrder: turn.eventOrder,
      evidence: appendEvidence(record.evidence, turn.evidence),
      finalReplyTs: receipt,
      updatedAt: now().toISOString(),
    }
    snapshot = await store.save(replaceRecord(snapshot.state, record.id, nextRecord), snapshot)
    record = requireRecordById(snapshot.state, record.id)
  }

  async function finishConfirmation(
    current: GuardianConversationRecord,
    turn: GuardianPendingTurn,
  ): Promise<void> {
    if (!snapshot) return
    // Bind the immutable timestamp to the provider event, not the retry wall
    // clock, so a post-before-checkpoint retry produces byte-identical evidence.
    const confirmation = confirmationRecord(current, turn, turn.eventOccurredAt)
    const confirmationPath = await confirmationStore.append(confirmation)
    if (current.confirmationPath !== confirmationPath) {
      const next = replaceRecord(snapshot.state, current.id, {
        ...current,
        confirmationPath,
        updatedAt: now().toISOString(),
      })
      snapshot = await store.save(next, snapshot)
      current = requireRecordById(snapshot.state, current.id)
      record = current
    }
    const procedureEvidence = [...current.evidence]
      .reverse()
      .find((entry) => entry.source === 'procedure')
    const summary = procedureEvidence
      ? `✅ Confirmed *${current.feature.name}* for manifest \`${shortRevision(current.manifestRevision)}\`. ${procedureEvidence.summary}`
      : `✅ Confirmed *${current.feature.name}* for manifest \`${shortRevision(current.manifestRevision)}\` from ${turn.actorId}'s response.`
    const replyTs = await postThreadReply(
      createSlackClient,
      current,
      summary,
      guardianReplyIdempotencyKey(current, turn.eventId, 'confirmed'),
    )
    const terminal: GuardianConversationRecord = {
      ...current,
      status: 'confirmed',
      pending: undefined,
      seenEventIds: appendSeen(current.seenEventIds, turn.eventId),
      lastProcessedEventOrder: turn.eventOrder,
      finalReplyTs: replyTs,
      updatedAt: now().toISOString(),
    }
    snapshot = await store.save(replaceRecord(snapshot.state, current.id, terminal), snapshot)
    record = requireRecordById(snapshot.state, current.id)
  }

  async function finishRemediation(
    current: GuardianConversationRecord,
    issue: GuardianIssueReceipt,
  ): Promise<void> {
    if (!snapshot || !current.pending) return
    const replyTs = await postThreadReply(
      createSlackClient,
      current,
      `🔧 Verification established a ${current.pending.defectKind ?? 'feature'} defect for *${current.feature.name}*. Remediation: ${issue.url}`,
      guardianReplyIdempotencyKey(current, current.pending.eventId, 'remediation'),
    )
    const terminal: GuardianConversationRecord = {
      ...current,
      status: 'remediation-open',
      pending: undefined,
      seenEventIds: appendSeen(current.seenEventIds, current.pending.eventId),
      lastProcessedEventOrder: current.pending.eventOrder,
      finalReplyTs: replyTs,
      updatedAt: now().toISOString(),
    }
    snapshot = await store.save(replaceRecord(snapshot.state, current.id, terminal), snapshot)
    record = requireRecordById(snapshot.state, current.id)
  }

  async function finishDeferred(
    current: GuardianConversationRecord,
    reason: string,
  ): Promise<void> {
    if (!snapshot || !current.pending) return
    const evidenceResult = current.pending.verification?.result ?? current.pending.evidence?.result
    const outcome = evidenceResult === 'skip' ? 'SKIP' : 'MANUAL'
    const replyTs = await postThreadReply(
      createSlackClient,
      current,
      `🟡 *${current.feature.name}* remains unconfirmed: ${reason} Result: ${outcome}.`,
      guardianReplyIdempotencyKey(current, current.pending.eventId, 'deferred'),
    )
    const terminal: GuardianConversationRecord = {
      ...current,
      status: 'deferred',
      pending: undefined,
      seenEventIds: appendSeen(current.seenEventIds, current.pending.eventId),
      lastProcessedEventOrder: current.pending.eventOrder,
      finalReplyTs: replyTs,
      updatedAt: now().toISOString(),
    }
    snapshot = await store.save(replaceRecord(snapshot.state, current.id, terminal), snapshot)
    record = requireRecordById(snapshot.state, current.id)
  }

  async function checkpointPending(
    currentSnapshot: ConversationSnapshot,
    current: GuardianConversationRecord,
    turn: GuardianPendingTurn,
    status: GuardianConversationStatus,
    countTurn = false,
  ): Promise<ConversationSnapshot> {
    const nextRecord: GuardianConversationRecord = {
      ...current,
      status,
      turnCount: current.turnCount + (countTurn ? 1 : 0),
      pending: turn,
      lastProcessedEventOrder: turn.eventOrder,
      evidence: appendEvidence(current.evidence, turn.evidence),
      updatedAt: now().toISOString(),
    }
    return store.save(replaceRecord(currentSnapshot.state, current.id, nextRecord), currentSnapshot)
  }
}

/** Exact-revision store used by the reusable state machine. */
export function createSdkConversationStore(
  credentials: RelayfileCredentials,
  options: { fetchImpl?: FetchLike; timeoutMs?: number } = {},
): ConversationStore {
  const client = relayfileClient(credentials, options.fetchImpl)
  const timeoutMs = options.timeoutMs ?? GUARDIAN_STATE_TIMEOUT_MS
  return exactStore({
    client,
    credentials,
    path: GUARDIAN_CONVERSATION_STATE_PATH,
    timeoutMs,
    parse: parseConversationState,
  })
}

/** Exact create-only immutable confirmation store. */
export function createSdkConfirmationStore(
  credentials: RelayfileCredentials,
  options: { fetchImpl?: FetchLike; timeoutMs?: number } = {},
): ImmutableConfirmationStore {
  const client = relayfileClient(credentials, options.fetchImpl)
  const timeoutMs = options.timeoutMs ?? GUARDIAN_STATE_TIMEOUT_MS
  return {
    append: async (record) => {
      const canonical = parseConfirmationRecord(record)
      const path = guardianConfirmationPath(canonical)
      const content = `${JSON.stringify(canonical)}\n`
      assertBoundedContent(content, 'guardian confirmation')
      return withDeadline('guardian confirmation append', timeoutMs, async (signal) => {
        const existing = await readExactFile(client, credentials.workspaceId, path, signal)
        if (existing) {
          if (existing.content !== content) throw new Error('immutable guardian confirmation already differs')
          return path
        }
        await writeExactFile(client, credentials.workspaceId, path, '0', content, signal)
        const readBack = await readExactFile(client, credentials.workspaceId, path, signal)
        if (!readBack || readBack.content !== content) {
          throw new Error('guardian confirmation read-back did not match')
        }
        return path
      })
    },
  }
}

function createConversationStore(ctx: WorkforceCtx): ConversationStore {
  const credentials = ctx.credentials.tryRequire()
  if (credentials) return createSdkConversationStore(credentials.relayfile)
  if (ctx.agent.id === 'sim-agent' && ctx.deployment.id === 'sim-deployment') {
    return previewConversationStore(ctx)
  }
  throw new Error('exact Relayfile credentials are required for guardian conversations')
}

function createConfirmationStore(ctx: WorkforceCtx): ImmutableConfirmationStore {
  const credentials = ctx.credentials.tryRequire()
  if (credentials) return createSdkConfirmationStore(credentials.relayfile)
  if (ctx.agent.id === 'sim-agent' && ctx.deployment.id === 'sim-deployment') {
    return {
      append: async (record) => {
        const canonical = parseConfirmationRecord(record)
        const path = guardianConfirmationPath(canonical)
        const content = `${JSON.stringify(canonical)}\n`
        let existing: string | undefined
        try {
          existing = await ctx.files.read(path)
        } catch (error) {
          if (!String(error).includes('ENOENT')) throw error
        }
        if (existing !== undefined && existing !== content) {
          throw new Error('immutable guardian confirmation already differs')
        }
        if (existing === undefined) await ctx.files.write(path, content)
        return path
      },
    }
  }
  throw new Error('exact Relayfile credentials are required for guardian confirmations')
}

function previewConversationStore(ctx: WorkforceCtx): ConversationStore {
  const load = async (): Promise<ConversationSnapshot | null> => {
    let content: string
    try {
      content = await ctx.files.read(GUARDIAN_CONVERSATION_STATE_PATH)
    } catch (error) {
      if (String(error).includes('ENOENT')) return null
      throw error
    }
    assertBoundedContent(content, 'guardian conversation state')
    const state = parseConversationState(JSON.parse(content) as unknown)
    return { state, revision: previewRevision(state) }
  }
  return {
    load,
    save: async (state, expected) => {
      const canonical = parseConversationState(state)
      const current = await load()
      if (current?.revision !== expected?.revision) throw new GuardianStateConflictError()
      assertConversationTransition(expected?.state ?? null, canonical)
      const content = `${JSON.stringify(canonical)}\n`
      assertBoundedContent(content, 'guardian conversation state')
      await ctx.files.write(GUARDIAN_CONVERSATION_STATE_PATH, content)
      const readBack = await load()
      if (!readBack || JSON.stringify(readBack.state) !== JSON.stringify(canonical)) {
        throw new Error('guardian conversation read-back did not match')
      }
      return readBack
    },
  }
}

function exactStore(input: {
  client: RelayFileClient
  credentials: RelayfileCredentials
  path: string
  timeoutMs: number
  parse: (value: unknown) => GuardianConversationState
}): ConversationStore {
  const load = () => withDeadline('guardian conversation load', input.timeoutMs, async (signal) => {
    const file = await readExactFile(input.client, input.credentials.workspaceId, input.path, signal)
    if (!file) return null
    assertBoundedContent(file.content, 'guardian conversation state')
    return {
      state: input.parse(JSON.parse(file.content) as unknown),
      revision: file.revision,
    }
  })
  return {
    load,
    save: (state, expected) => {
      const canonical = input.parse(state)
      assertConversationTransition(expected?.state ?? null, canonical)
      const content = `${JSON.stringify(canonical)}\n`
      assertBoundedContent(content, 'guardian conversation state')
      return withDeadline('guardian conversation save', input.timeoutMs, async (signal) => {
        try {
          await writeExactFile(
            input.client,
            input.credentials.workspaceId,
            input.path,
            expected?.revision ?? '0',
            content,
            signal,
          )
        } catch (error) {
          if (
            error instanceof RevisionConflictError ||
            (error instanceof RelayFileApiError && error.status === 409)
          ) {
            throw new GuardianStateConflictError()
          }
          throw error
        }
        const readBack = await readExactFile(
          input.client,
          input.credentials.workspaceId,
          input.path,
          signal,
        )
        if (!readBack || readBack.content !== content) {
          throw new Error('guardian conversation read-back did not match')
        }
        return { state: canonical, revision: readBack.revision }
      })
    },
  }
}

export function createGithubIssueWriter(
  ctx: WorkforceCtx,
  client: GithubClient = githubClient({
    writebackTimeoutMs: GUARDIAN_WRITEBACK_TIMEOUT_MS,
    writebackPollMs: GUARDIAN_WRITEBACK_POLL_MS,
  }),
): GuardianIssueWriter {
  return {
    upsert: async (policy) => {
      const [owner, repo, extra] = policy.repository.split('/')
      if (!owner || !repo || extra) throw new Error('guardian issue repository must be owner/repo')
      const receiptPath = remediationReceiptPath(policy.dedupeKey)
      const savedReceipt = await readImmutableJson(ctx, receiptPath)
      const bodyRevision = guardianContentRevision(policy.body)
      if (savedReceipt) {
        const receipt = parseDurableIssueReceipt(savedReceipt, policy)
        if (providerText(asRecord(savedReceipt)?.bodyRevision) !== bodyRevision) {
          await updateExistingIssueEvidence(ctx, client, owner, repo, receipt.number, policy)
        }
        return receipt
      }
      const issues = await listGithubIssues(ctx, client, owner, repo)
      const match = issues
        .map(unwrapProviderRecord)
        .find((issue) => providerText(issue.body).includes(remediationMarker(policy.dedupeKey)))
      if (match) {
        const number = providerNumber(match.number) ?? providerNumber(match.id)
        const url = providerText(match.html_url) || providerText(match.url)
        if (!number || !url) throw new Error('deduplicated guardian issue is missing number/url')
        if (providerText(match.body) !== policy.body) {
          await updateExistingIssueEvidence(ctx, client, owner, repo, number, policy)
        }
        const receipt = { repository: policy.repository, number, url, dedupeKey: policy.dedupeKey }
        await appendImmutableJson(ctx, receiptPath, { ...receipt, bodyRevision })
        return receipt
      }
      const intentPath = remediationIntentPath(policy.dedupeKey)
      const existingIntent = await readImmutableJson(ctx, intentPath)
      if (existingIntent) {
        throw new Error(
          'GitHub remediation submission is pending provider correlation; refusing a duplicate issue',
        )
      }
      await appendImmutableJson(ctx, intentPath, {
        kind: 'feature-guardian:remediation-intent',
        version: 1,
        repository: policy.repository,
        dedupeKey: policy.dedupeKey,
        bodyRevision,
      })
      const created = await client.createIssue({
        owner,
        repo,
        title: policy.title,
        body: policy.body,
        labels: policy.labels,
      })
      if (created.status !== 'confirmed' || !created.url) {
        throw new Error(`GitHub remediation issue create was ${created.status}, not provider-confirmed`)
      }
      const number = issueNumberFromCreated(created.id, created.url)
      if (!number) throw new Error('GitHub remediation issue receipt is missing its issue number')
      const receipt = {
        repository: policy.repository,
        number,
        url: created.url,
        dedupeKey: policy.dedupeKey,
      }
      await appendImmutableJson(ctx, receiptPath, { ...receipt, bodyRevision })
      return receipt
    },
  }
}

export function remediationMarker(dedupeKey: string): string {
  return `<!-- feature-guardian-remediation:${dedupeKey} -->`
}

function remediationIntentPath(dedupeKey: string): string {
  return `${GUARDIAN_REMEDIATIONS_PATH}/${createHash('sha256').update(`intent:${dedupeKey}`).digest('hex')}.json`
}

function remediationReceiptPath(dedupeKey: string): string {
  return `${GUARDIAN_REMEDIATIONS_PATH}/${createHash('sha256').update(`receipt:${dedupeKey}`).digest('hex')}.json`
}

async function updateExistingIssueEvidence(
  ctx: WorkforceCtx,
  client: GithubClient,
  owner: string,
  repo: string,
  issueNumber: number,
  policy: GuardianIssuePolicy,
): Promise<void> {
  const bodyRevision = guardianContentRevision(policy.body)
  const marker = `<!-- feature-guardian-evidence:${bodyRevision} -->`
  const ledgerKey = `${policy.dedupeKey}:${bodyRevision}`
  const receiptPath = `${GUARDIAN_REMEDIATIONS_PATH}/${createHash('sha256').update(`update-receipt:${ledgerKey}`).digest('hex')}.json`
  if (await readImmutableJson(ctx, receiptPath)) return
  const comments = await client['issue-comments'].list<Record<string, unknown>>({
    owner,
    repo,
    issueNumber,
  })
  if (comments.map(unwrapProviderRecord).some((comment) => providerText(comment.body).includes(marker))) {
    await appendImmutableJson(ctx, receiptPath, { issueNumber, bodyRevision })
    return
  }
  const intentPath = `${GUARDIAN_REMEDIATIONS_PATH}/${createHash('sha256').update(`update-intent:${ledgerKey}`).digest('hex')}.json`
  if (await readImmutableJson(ctx, intentPath)) {
    throw new Error('GitHub remediation evidence update is pending; refusing a duplicate comment')
  }
  await appendImmutableJson(ctx, intentPath, { issueNumber, bodyRevision })
  const updated = await client.comment(
    { owner, repo, number: issueNumber },
    `${marker}\n${policy.body}`,
  )
  if (updated.status !== 'confirmed') {
    throw new Error(`GitHub remediation evidence update was ${updated.status}, not provider-confirmed`)
  }
  await appendImmutableJson(ctx, receiptPath, { issueNumber, bodyRevision })
}

function parseDurableIssueReceipt(
  value: unknown,
  policy: GuardianIssuePolicy,
): GuardianIssueReceipt {
  const receipt = parseIssueReceipt(value)
  if (receipt.repository !== policy.repository || receipt.dedupeKey !== policy.dedupeKey) {
    throw new Error('durable guardian issue receipt does not match its policy')
  }
  return receipt
}

async function listGithubIssues(
  ctx: WorkforceCtx,
  client: GithubClient,
  owner: string,
  repo: string,
): Promise<Record<string, unknown>[]> {
  const listed = await client.issues.list<Record<string, unknown>>({ owner, repo })
  const issues = listed.filter((value) => asRecord(value) !== undefined)
  const root = `${ctx.sandbox.cwd}/github/repos/${owner}/${repo}/issues`
  const rawIndex = await readOptionalSandboxFile(ctx, `${root}/_index.json`)
  if (!rawIndex) return issues
  const index = JSON.parse(rawIndex) as unknown
  if (!Array.isArray(index)) throw new Error('guardian GitHub issue index is malformed')
  for (const entry of index) {
    const item = asRecord(entry)
    const number = providerNumber(item?.number) ?? providerNumber(item?.id)
    const title = providerText(item?.title)
    if (!number || !title) continue
    const slug = title
      .toLowerCase()
      .replace(/[^a-z0-9]+/gu, '-')
      .replace(/^-|-$/gu, '')
    const candidates = [
      `${root}/${number}__${slug}/meta.json`,
      `${root}/${number}/meta.json`,
      `${root}/${number}.json`,
    ]
    for (const path of candidates) {
      const raw = await readOptionalSandboxFile(ctx, path)
      if (!raw) continue
      const record = asRecord(JSON.parse(raw) as unknown)
      if (record) issues.push(record)
      break
    }
  }
  return issues
}

async function readOptionalSandboxFile(
  ctx: WorkforceCtx,
  path: string,
): Promise<string | undefined> {
  try {
    return await ctx.sandbox.readFile(path)
  } catch (error) {
    const message = String(error)
    if (message.includes('ENOENT') || message.includes('not found') || message.includes('No such file')) {
      return undefined
    }
    throw error
  }
}

async function readImmutableJson(
  ctx: WorkforceCtx,
  path: string,
): Promise<unknown | null> {
  const credentials = ctx.credentials.tryRequire()
  let content: string | undefined
  if (credentials) {
    const client = relayfileClient(credentials.relayfile)
    const file = await withDeadline('guardian immutable ledger read', GUARDIAN_STATE_TIMEOUT_MS, (signal) =>
      readExactFile(client, credentials.relayfile.workspaceId, path, signal),
    )
    content = file?.content
  } else if (ctx.agent.id === 'sim-agent' && ctx.deployment.id === 'sim-deployment') {
    try {
      content = await ctx.files.read(path)
    } catch (error) {
      if (!String(error).includes('ENOENT')) throw error
    }
  } else {
    throw new Error('exact Relayfile credentials are required for guardian remediation ledger')
  }
  if (content === undefined) return null
  assertBoundedContent(content, 'guardian immutable ledger')
  return JSON.parse(content) as unknown
}

async function appendImmutableJson(
  ctx: WorkforceCtx,
  path: string,
  value: unknown,
): Promise<void> {
  const content = `${JSON.stringify(value)}\n`
  assertBoundedContent(content, 'guardian immutable ledger')
  const existing = await readImmutableJson(ctx, path)
  if (existing !== null) {
    if (`${JSON.stringify(existing)}\n` !== content) {
      throw new Error('immutable guardian ledger entry already differs')
    }
    return
  }
  const credentials = ctx.credentials.tryRequire()
  if (credentials) {
    const client = relayfileClient(credentials.relayfile)
    try {
      await withDeadline('guardian immutable ledger append', GUARDIAN_STATE_TIMEOUT_MS, (signal) =>
        writeExactFile(client, credentials.relayfile.workspaceId, path, '0', content, signal),
      )
    } catch (error) {
      if (
        !(error instanceof RevisionConflictError) &&
        !(error instanceof RelayFileApiError && error.status === 409)
      ) throw error
    }
  } else if (ctx.agent.id === 'sim-agent' && ctx.deployment.id === 'sim-deployment') {
    await ctx.files.write(path, content)
  } else {
    throw new Error('exact Relayfile credentials are required for guardian remediation ledger')
  }
  const readBack = await readImmutableJson(ctx, path)
  if (readBack === null || `${JSON.stringify(readBack)}\n` !== content) {
    throw new Error('guardian immutable ledger read-back did not match')
  }
}

export function slackThreadBacklink(channelId: string, threadTs: string): string {
  return `https://slack.com/archives/${channelId}/p${threadTs.replace('.', '')}`
}

async function postThreadReply(
  createSlackClient: typeof slackClient,
  record: GuardianConversationRecord,
  text: string,
  idempotencyKey: string,
): Promise<string> {
  const client = createSlackClient({
    writebackTimeoutMs: GUARDIAN_WRITEBACK_TIMEOUT_MS,
    writebackPollMs: GUARDIAN_WRITEBACK_POLL_MS,
  })
  const result = await client.replies.write(
    { channelId: record.channelId, messageTs: record.threadTs },
    { text, thread_ts: record.threadTs, idempotencyKey },
  )
  return requireSlackReceiptTs(result)
}

function requireSlackReceiptTs(result: WritebackResult): string {
  const receipt = asRecord(result.receipt)
  const externalId = nonEmptyString(receipt?.externalId)
  const fallbackTs = nonEmptyString(receipt?.ts)
  const ts = externalId && /^\d+\.\d+$/u.test(externalId) ? externalId : fallbackTs
  if (!ts || !/^\d+\.\d+$/u.test(ts)) {
    throw new Error('guardian Slack reply did not receive a provider timestamp')
  }
  return ts
}

function confirmationRecord(
  record: GuardianConversationRecord,
  turn: GuardianPendingTurn,
  timestamp: string,
): GuardianConfirmationRecord {
  const evidence = record.evidence
  const commands = evidence.flatMap((entry) => entry.commands ?? [])
  const tests = evidence.reduce(
    (total, entry) => ({
      passed: total.passed + (entry.tests?.passed ?? 0),
      failed: total.failed + (entry.tests?.failed ?? 0),
    }),
    { passed: 0, failed: 0 },
  )
  const automated = evidence.some(
    (entry) => entry.source === 'procedure' && entry.result === 'positive',
  )
  return {
    kind: 'feature-guardian:confirmation',
    version: 1,
    featureId: record.feature.id,
    manifestRevision: record.manifestRevision,
    manifestVersion: record.manifestVersion,
    procedureRevision: record.procedureRevision,
    generation: record.generation,
    result: 'confirmed',
    ...(turn.actorId ? { actor: { id: turn.actorId } } : {}),
    verifier: automated ? 'factory-feature-guardian:procedure-runner' : turn.actorId,
    timestamp,
    evidence,
    commands,
    tests,
    slack: {
      channelId: record.channelId,
      threadTs: record.threadTs,
      questionTs: record.threadTs,
    },
  }
}

function parseConversationState(value: unknown): GuardianConversationState {
  const root = asRecord(value)
  if (!root || root.kind !== 'feature-guardian:conversations' || root.version !== 1) {
    throw new Error('guardian conversation state kind/version is invalid')
  }
  if (!Array.isArray(root.records) || root.records.length > MAX_CONVERSATIONS) {
    throw new Error('guardian conversation state records are invalid')
  }
  const records = root.records.map(parseConversationRecord)
  if (new Set(records.map((record) => record.id)).size !== records.length) {
    throw new Error('guardian conversation state contains duplicate records')
  }
  return { kind: 'feature-guardian:conversations', version: 1, records }
}

function parseConversationRecord(value: unknown): GuardianConversationRecord {
  const record = asRecord(value)
  if (!record) throw new Error('guardian conversation record must be an object')
  const feature = parseFeatureSnapshot(record.feature)
  const status = record.status
  if (!isConversationStatus(status)) throw new Error('guardian conversation status is invalid')
  const seenEventIds = stringArray(record.seenEventIds, MAX_SEEN_EVENTS, 'seen event ids')
  const evidence = evidenceArray(record.evidence)
  const turnCount = record.turnCount
  if (!Number.isSafeInteger(turnCount) || (turnCount as number) < 0 || (turnCount as number) > MAX_TURNS) {
    throw new Error('guardian conversation turn count is invalid')
  }
  const clarificationCount = record.clarificationCount
  if (
    !Number.isSafeInteger(clarificationCount) ||
    (clarificationCount as number) < 0 ||
    (clarificationCount as number) > (turnCount as number)
  ) {
    throw new Error('guardian conversation clarification count is invalid')
  }
  const parsed: GuardianConversationRecord = {
    id: requireString(record.id, 'conversation id'),
    feature,
    manifestRevision: requireString(record.manifestRevision, 'manifest revision'),
    manifestVersion: requireString(record.manifestVersion, 'manifest version'),
    procedureRevision: requireString(record.procedureRevision, 'procedure revision'),
    generation: requirePositiveInteger(record.generation, 'generation'),
    channelId: requireString(record.channelId, 'channel id'),
    threadTs: requireSlackTs(record.threadTs, 'thread ts'),
    askedAt: requireTimestamp(record.askedAt, 'askedAt'),
    status,
    turnCount: turnCount as number,
    clarificationCount: clarificationCount as number,
    seenEventIds,
    evidence,
    updatedAt: requireTimestamp(record.updatedAt, 'updatedAt'),
  }
  if (record.pending !== undefined) parsed.pending = parsePending(record.pending)
  if (record.confirmationPath !== undefined) {
    parsed.confirmationPath = requireString(record.confirmationPath, 'confirmation path')
  }
  if (record.issue !== undefined) parsed.issue = parseIssueReceipt(record.issue)
  if (record.finalReplyTs !== undefined) {
    parsed.finalReplyTs = requireSlackTs(record.finalReplyTs, 'final reply ts')
  }
  if (record.lastProcessedEventOrder !== undefined) {
    parsed.lastProcessedEventOrder = requireEventOrder(
      record.lastProcessedEventOrder,
      'last processed event order',
    )
  }
  if (guardianConversationId(parsed) !== parsed.id) {
    throw new Error('guardian conversation record identity is invalid')
  }
  if (isTerminal(parsed.status) && parsed.pending) {
    throw new Error('terminal guardian conversation cannot retain a pending turn')
  }
  if ((parsed.status === 'asked' || parsed.status === 'discussing') && parsed.pending) {
    throw new Error(`guardian ${parsed.status} conversation cannot retain a pending turn`)
  }
  if (isTerminal(parsed.status) && !parsed.finalReplyTs) {
    throw new Error('terminal guardian conversation requires a provider-confirmed final reply')
  }
  if (
    ['verifying', 'confirmation-recorded', 'remediation-recorded', 'deferred-recorded'].includes(
      parsed.status,
    ) && !parsed.pending
  ) {
    throw new Error(`guardian ${parsed.status} conversation requires a pending turn`)
  }
  if (
    (parsed.status === 'remediation-recorded' || parsed.status === 'remediation-open') &&
    !parsed.issue
  ) {
    throw new Error(`guardian ${parsed.status} conversation requires an issue receipt`)
  }
  if (parsed.status === 'confirmed' && !parsed.confirmationPath) {
    throw new Error('confirmed guardian conversation requires a confirmation path')
  }
  if (
    parsed.confirmationPath &&
    parsed.status !== 'confirmation-recorded' &&
    parsed.status !== 'confirmed'
  ) {
    throw new Error('guardian confirmation path is invalid for the conversation status')
  }
  if (
    parsed.issue &&
    parsed.status !== 'remediation-recorded' &&
    parsed.status !== 'remediation-open'
  ) {
    throw new Error('guardian issue receipt is invalid for the conversation status')
  }
  return parsed
}

function parseConfirmationRecord(value: unknown): GuardianConfirmationRecord {
  const record = asRecord(value)
  if (!record || record.kind !== 'feature-guardian:confirmation' || record.version !== 1) {
    throw new Error('guardian confirmation kind/version is invalid')
  }
  const actor = asRecord(record.actor)
  const slack = asRecord(record.slack)
  const tests = asRecord(record.tests)
  if (record.result !== 'confirmed' || !slack || !tests) {
    throw new Error('guardian confirmation result/evidence is invalid')
  }
  return {
    kind: 'feature-guardian:confirmation',
    version: 1,
    featureId: requireString(record.featureId, 'feature id'),
    manifestRevision: requireString(record.manifestRevision, 'manifest revision'),
    manifestVersion: requireString(record.manifestVersion, 'manifest version'),
    procedureRevision: requireString(record.procedureRevision, 'procedure revision'),
    generation: requirePositiveInteger(record.generation, 'generation'),
    result: 'confirmed',
    ...(actor ? { actor: { id: requireString(actor.id, 'actor id') } } : {}),
    verifier: requireString(record.verifier, 'verifier'),
    timestamp: requireTimestamp(record.timestamp, 'timestamp'),
    evidence: evidenceArray(record.evidence),
    commands: stringArray(record.commands, 128, 'commands'),
    tests: {
      passed: requireNonNegativeInteger(tests.passed, 'passed tests'),
      failed: requireNonNegativeInteger(tests.failed, 'failed tests'),
    },
    slack: {
      channelId: requireString(slack.channelId, 'Slack channel id'),
      threadTs: requireSlackTs(slack.threadTs, 'Slack thread ts'),
      questionTs: requireSlackTs(slack.questionTs, 'Slack question ts'),
    },
  }
}

function assertConversationTransition(
  previous: GuardianConversationState | null,
  next: GuardianConversationState,
): void {
  if (!previous) return
  const nextById = new Map(next.records.map((record) => [record.id, record]))
  const removed = previous.records.filter((record) => !nextById.has(record.id))
  if (removed.some((record) => !isTerminal(record.status))) {
    throw new Error('guardian conversation records are append-only except bounded terminal pruning')
  }
  for (const prior of previous.records) {
    const current = nextById.get(prior.id)
    if (!current) continue
    if (!sameQuestion(prior, current)) {
      throw new Error('guardian conversation question identity is immutable')
    }
    if (isTerminal(prior.status) && JSON.stringify(prior) !== JSON.stringify(current)) {
      throw new Error('terminal guardian conversation records are immutable')
    }
    if (!prior.seenEventIds.every((id) => current.seenEventIds.includes(id))) {
      throw new Error('guardian conversation seen-event history cannot regress')
    }
    if (
      JSON.stringify(current.evidence.slice(0, prior.evidence.length)) !==
      JSON.stringify(prior.evidence)
    ) {
      throw new Error('guardian conversation evidence is append-only')
    }
    if (
      prior.confirmationPath !== undefined &&
      current.confirmationPath !== prior.confirmationPath
    ) {
      throw new Error('guardian conversation confirmation path is immutable')
    }
    if (prior.issue !== undefined && JSON.stringify(current.issue) !== JSON.stringify(prior.issue)) {
      throw new Error('guardian conversation issue receipt is immutable')
    }
    if (current.turnCount < prior.turnCount) {
      throw new Error('guardian conversation turn count cannot regress')
    }
    if (current.clarificationCount < prior.clarificationCount) {
      throw new Error('guardian conversation clarification count cannot regress')
    }
    if (
      prior.lastProcessedEventOrder &&
      (!current.lastProcessedEventOrder ||
        current.lastProcessedEventOrder < prior.lastProcessedEventOrder)
    ) {
      throw new Error('guardian conversation event-order watermark cannot regress')
    }
    if (!validStatusTransition(prior.status, current.status)) {
      throw new Error(`invalid guardian conversation transition ${prior.status} -> ${current.status}`)
    }
  }
}

function validStatusTransition(
  previous: GuardianConversationStatus,
  next: GuardianConversationStatus,
): boolean {
  if (previous === next) return true
  const allowed: Record<GuardianConversationStatus, GuardianConversationStatus[]> = {
    asked: ['discussing', 'verifying', 'confirmation-recorded', 'deferred-recorded'],
    discussing: ['discussing', 'verifying', 'confirmation-recorded', 'deferred-recorded'],
    verifying: ['discussing', 'confirmation-recorded', 'remediation-recorded', 'deferred-recorded'],
    'confirmation-recorded': ['confirmed'],
    'remediation-recorded': ['remediation-open'],
    'deferred-recorded': ['deferred'],
    confirmed: [],
    'remediation-open': [],
    deferred: [],
  }
  return allowed[previous].includes(next)
}

function parseFeatureSnapshot(value: unknown): GuardianFeatureSnapshot {
  const feature = asRecord(value)
  if (!feature) throw new Error('guardian feature snapshot must be an object')
  const tier = requirePositiveInteger(feature.tier, 'feature tier')
  if (tier > 6) throw new Error('guardian feature tier is invalid')
  return {
    id: requireString(feature.id, 'feature id'),
    name: requireString(feature.name, 'feature name'),
    category: requireString(feature.category, 'feature category'),
    ...(nonEmptyString(feature.cli) ? { cli: nonEmptyString(feature.cli) } : {}),
    ...(nonEmptyString(feature.api) ? { api: nonEmptyString(feature.api) } : {}),
    description: requireString(feature.description, 'feature description'),
    locations: stringArray(feature.locations, 32, 'feature locations'),
    procedure: requireString(feature.procedure, 'feature procedure'),
    tier,
    criticality: requireString(feature.criticality, 'feature criticality'),
  }
}

function parsePending(value: unknown): GuardianPendingTurn {
  const pending = asRecord(value)
  if (!pending || !isResponseKind(pending.response)) {
    throw new Error('guardian pending turn is invalid')
  }
  return {
    eventId: requireString(pending.eventId, 'event id'),
    eventOrder: requireEventOrder(pending.eventOrder, 'pending event order'),
    eventOccurredAt: requireTimestamp(pending.eventOccurredAt, 'event occurredAt'),
    actorId: requireString(pending.actorId, 'actor id'),
    response: pending.response,
    text: typeof pending.text === 'string' ? bounded(pending.text, 4_000) : '',
    ...(nonEmptyString(pending.reaction) ? { reaction: nonEmptyString(pending.reaction) } : {}),
    ...(pending.evidence !== undefined ? { evidence: parseEvidence(pending.evidence) } : {}),
    ...(pending.verification !== undefined
      ? { verification: parseProcedureResult(pending.verification) }
      : {}),
    ...(isDefectKind(pending.defectKind) ? { defectKind: pending.defectKind } : {}),
  }
}

function parseProcedureResult(value: unknown): GuardianProcedureResult {
  const result = asRecord(value)
  const evidence = parseEvidence(value)
  if (
    evidence.source !== 'procedure' ||
    !['passed', 'failed', 'skip', 'manual'].includes(String(result?.outcome))
  ) {
    throw new Error('guardian procedure result is invalid')
  }
  const outcome = result?.outcome as GuardianProcedureResultKind
  const expectedResult: Record<GuardianProcedureResultKind, GuardianEvidence['result']> = {
    passed: 'positive',
    failed: 'negative',
    skip: 'skip',
    manual: 'manual',
  }
  if (evidence.result !== expectedResult[outcome]) {
    throw new Error('guardian procedure outcome/result is inconsistent')
  }
  return {
    ...evidence,
    source: 'procedure',
    outcome,
    verifier: requireString(result?.verifier, 'procedure verifier'),
  }
}

function parseIssueReceipt(value: unknown): GuardianIssueReceipt {
  const issue = asRecord(value)
  if (!issue) throw new Error('guardian issue receipt is invalid')
  return {
    repository: requireString(issue.repository, 'issue repository'),
    number: requirePositiveInteger(issue.number, 'issue number'),
    url: requireString(issue.url, 'issue url'),
    dedupeKey: requireString(issue.dedupeKey, 'issue dedupe key'),
  }
}

function evidenceArray(value: unknown): GuardianEvidence[] {
  if (!Array.isArray(value) || value.length > MAX_EVIDENCE_ITEMS) {
    throw new Error('guardian evidence is invalid')
  }
  return value.map(parseEvidence)
}

function parseEvidence(value: unknown): GuardianEvidence {
  const evidence = asRecord(value)
  if (
    !evidence ||
    !['actor', 'procedure', 'system'].includes(String(evidence.source)) ||
    !['positive', 'negative', 'skip', 'manual', 'unknown'].includes(String(evidence.result))
  ) {
    throw new Error('guardian evidence item is invalid')
  }
  const tests = asRecord(evidence.tests)
  return {
    source: evidence.source as GuardianEvidence['source'],
    result: evidence.result as GuardianEvidence['result'],
    summary: requireString(evidence.summary, 'evidence summary'),
    ...(evidence.commands !== undefined
      ? { commands: stringArray(evidence.commands, 128, 'evidence commands') }
      : {}),
    ...(evidence.positiveAssertions !== undefined
      ? {
          positiveAssertions: stringArray(
            evidence.positiveAssertions,
            128,
            'positive assertions',
          ),
        }
      : {}),
    ...(evidence.negativeAssertions !== undefined
      ? {
          negativeAssertions: stringArray(
            evidence.negativeAssertions,
            128,
            'negative assertions',
          ),
        }
      : {}),
    ...(tests
      ? {
          tests: {
            passed: requireNonNegativeInteger(tests.passed, 'passed tests'),
            failed: requireNonNegativeInteger(tests.failed, 'failed tests'),
          },
        }
      : {}),
    ...(evidence.cleanup !== undefined
      ? { cleanup: stringArray(evidence.cleanup, 32, 'cleanup evidence') }
      : {}),
  }
}

function findCorrelatedConversation(
  state: GuardianConversationState,
  event: ParsedSlackResponse,
): GuardianConversationRecord | undefined {
  const candidates = state.records.filter(
    (record) =>
      record.channelId === event.channelId &&
      record.threadTs === event.threadTs &&
      (!event.isReaction || event.messageTs === record.threadTs),
  )
  return candidates.length === 1 ? candidates[0] : undefined
}

function findSlackPayload(root: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (!root) return undefined
  const queue: Record<string, unknown>[] = [root]
  const seen = new Set<Record<string, unknown>>()
  while (queue.length > 0) {
    const current = queue.shift()!
    if (seen.has(current)) continue
    seen.add(current)
    if (
      nonEmptyString(current.channel) ||
      nonEmptyString(asRecord(current.item)?.channel) ||
      nonEmptyString(current.thread_ts)
    ) {
      return current
    }
    for (const key of ['payload', 'event', 'message', 'data', 'resource', 'record']) {
      const nested = asRecord(current[key])
      if (nested) queue.push(nested)
    }
  }
  return undefined
}

function channelFromPath(path: string | undefined): string | undefined {
  const encoded = path?.match(/^\/slack\/channels\/([^/]+)/u)?.[1]
  return encoded?.split('__')[0]
}

function messageTsFromPath(path: string | undefined): string | undefined {
  const match = path?.match(/\/messages\/([^/]+)(?:\/replies\/([^/.]+))?/u)
  return (match?.[2] ?? match?.[1])?.replaceAll('_', '.')
}

function pendingTurn(
  parsed: ParsedSlackResponse,
  response: GuardianResponseKind,
  evidence: GuardianEvidence,
): GuardianPendingTurn {
  return {
    eventId: parsed.eventId,
    eventOrder: parsed.eventOrder,
    eventOccurredAt: parsed.occurredAt,
    actorId: parsed.actorId,
    response,
    text: parsed.text,
    ...(parsed.reaction ? { reaction: parsed.reaction } : {}),
    evidence,
  }
}

function emptyConversationState(): GuardianConversationState {
  return { kind: 'feature-guardian:conversations', version: 1, records: [] }
}

function retainConversationCapacity(
  records: GuardianConversationRecord[],
  incoming: GuardianConversationRecord,
): GuardianConversationRecord[] {
  let retained = [...records]
  for (;;) {
    const candidate = {
      kind: 'feature-guardian:conversations' as const,
      version: 1 as const,
      records: [...retained, incoming],
    }
    const fitsCount = candidate.records.length <= MAX_CONVERSATIONS
    const fitsBytes = UTF8_ENCODER.encode(`${JSON.stringify(candidate)}\n`).byteLength <= MAX_STATE_BYTES
    if (fitsCount && fitsBytes) return retained
    const removable = retained
      .filter((record) => isTerminal(record.status))
      .sort((left, right) => left.updatedAt.localeCompare(right.updatedAt))[0]
    if (!removable) {
      throw new Error('guardian conversation state reached its bounded active-record limit')
    }
    retained = retained.filter((record) => record.id !== removable.id)
  }
}

function replaceRecord(
  state: GuardianConversationState,
  id: string,
  next: GuardianConversationRecord,
): GuardianConversationState {
  return {
    ...state,
    records: state.records.map((record) => (record.id === id ? next : record)),
  }
}

function requireRecordById(
  state: GuardianConversationState,
  id: string,
): GuardianConversationRecord {
  const record = state.records.find((candidate) => candidate.id === id)
  if (!record) throw new Error('guardian conversation disappeared after checkpoint')
  return record
}

function sameQuestion(
  left: GuardianQuestion,
  right: GuardianQuestion,
): boolean {
  return JSON.stringify({
    feature: left.feature,
    manifestRevision: left.manifestRevision,
    manifestVersion: left.manifestVersion,
    procedureRevision: left.procedureRevision,
    generation: left.generation,
    channelId: left.channelId,
    threadTs: left.threadTs,
    askedAt: left.askedAt,
  }) === JSON.stringify({
    feature: right.feature,
    manifestRevision: right.manifestRevision,
    manifestVersion: right.manifestVersion,
    procedureRevision: right.procedureRevision,
    generation: right.generation,
    channelId: right.channelId,
    threadTs: right.threadTs,
    askedAt: right.askedAt,
  })
}

function appendSeen(existing: string[], eventId: string): string[] {
  if (existing.includes(eventId)) return existing
  if (existing.length >= MAX_SEEN_EVENTS) {
    throw new Error('guardian conversation seen-event history reached its bounded limit')
  }
  return [...existing, eventId]
}

function appendEvidence(
  existing: GuardianEvidence[],
  evidence: GuardianEvidence | undefined,
): GuardianEvidence[] {
  if (!evidence) return existing
  if (existing.length >= MAX_EVIDENCE_ITEMS) {
    throw new Error('guardian conversation evidence reached its bounded limit')
  }
  return [...existing, evidence]
}

function previewRevision(state: GuardianConversationState): string {
  return `preview:${guardianContentRevision(JSON.stringify(state))}`
}

function isTerminal(status: GuardianConversationStatus): boolean {
  return status === 'confirmed' || status === 'remediation-open' || status === 'deferred'
}

function isConversationStatus(value: unknown): value is GuardianConversationStatus {
  return [
    'asked',
    'discussing',
    'verifying',
    'confirmation-recorded',
    'remediation-recorded',
    'deferred-recorded',
    'confirmed',
    'remediation-open',
    'deferred',
  ].includes(String(value))
}

function isResponseKind(value: unknown): value is GuardianResponseKind {
  return ['affirmative', 'failure', 'untested', 'ambiguous', 'deferred'].includes(String(value))
}

function isDefectKind(value: unknown): value is GuardianDefectKind {
  return ['implementation', 'test', 'manifest', 'procedure', 'documentation'].includes(String(value))
}

function shortRevision(revision: string): string {
  return revision.replace(/^sha256:/u, '').slice(0, 12)
}

function resolvedInput(ctx: WorkforceCtx, name: string): string | undefined {
  const spec = ctx.persona?.inputSpecs?.[name]
  const value = process.env[spec?.env ?? name] ?? ctx.persona?.inputs?.[name] ?? spec?.default
  return nonEmptyString(value)
}

function remediationBodyHasMarker(body: string, key: string): boolean {
  return body.includes(remediationMarker(key))
}

function unwrapProviderRecord(value: Record<string, unknown>): Record<string, unknown> {
  return asRecord(value.payload) ?? asRecord(value.data) ?? value
}

function providerText(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function providerNumber(value: unknown): number | undefined {
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined
}

function issueNumberFromCreated(id: string, url: string): number | undefined {
  return providerNumber(id) ?? providerNumber(url.match(/\/issues\/(\d+)(?:$|[?#])/u)?.[1])
}

function relayfileClient(credentials: RelayfileCredentials, fetchImpl?: FetchLike): RelayFileClient {
  return new RelayFileClient({
    baseUrl: credentials.url,
    token: credentials.token,
    fetchImpl,
    readCache: false,
    retry: { maxRetries: 0 },
  })
}

async function readExactFile(
  client: RelayFileClient,
  workspaceId: string,
  path: string,
  signal: AbortSignal,
): Promise<{ content: string; revision: string } | null> {
  let file
  try {
    file = await client.readFile(workspaceId, path, `guardian-read-${randomUUID()}`, signal)
  } catch (error) {
    if (error instanceof RelayFileApiError && error.status === 404) return null
    throw error
  }
  if (!file || file.path !== path || typeof file.content !== 'string') {
    throw new Error('guardian exact read returned an invalid file')
  }
  const revision = nonEmptyString(file.revision)
  if (!revision) throw new Error('guardian exact read returned an invalid revision')
  return { content: file.content, revision }
}

async function writeExactFile(
  client: RelayFileClient,
  workspaceId: string,
  path: string,
  baseRevision: string,
  content: string,
  signal: AbortSignal,
): Promise<void> {
  const correlationId = `guardian-write-${randomUUID()}`
  const queued = await client.writeFile({
    workspaceId,
    path,
    baseRevision,
    content,
    contentType: 'application/json',
    encoding: 'utf-8',
    correlationId,
    signal,
  })
  if (!queued || !nonEmptyString(queued.opId)) {
    throw new Error('guardian exact write did not return an operation ID')
  }
  await waitForOperation(client, workspaceId, queued.opId, signal, correlationId)
}

async function waitForOperation(
  client: RelayFileClient,
  workspaceId: string,
  opId: string,
  signal: AbortSignal,
  correlationId: string,
): Promise<OperationStatusResponse> {
  for (;;) {
    const operation = await client.getOp(workspaceId, opId, correlationId, signal)
    if (operation?.status === 'succeeded') return operation
    if (['failed', 'dead_lettered', 'canceled'].includes(String(operation?.status))) {
      throw new Error(`guardian exact write ${operation?.status}`)
    }
    if (!['pending', 'running'].includes(String(operation?.status))) {
      throw new Error('guardian exact write returned an invalid operation status')
    }
    await abortableDelay(GUARDIAN_WRITEBACK_POLL_MS, signal)
  }
}

async function withDeadline<T>(
  label: string,
  timeoutMs: number,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const controller = new AbortController()
  let timeout: ReturnType<typeof setTimeout> | undefined
  const expired = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      controller.abort()
      reject(new Error(`${label} timed out after ${timeoutMs}ms`))
    }, timeoutMs)
  })
  try {
    return await Promise.race([operation(controller.signal), expired])
  } finally {
    if (timeout !== undefined) clearTimeout(timeout)
  }
}

function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timeout)
      reject(signal.reason)
    }
    const timeout = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    if (signal.aborted) onAbort()
    else signal.addEventListener('abort', onAbort, { once: true })
  })
}

function assertBoundedContent(content: string, label: string): void {
  if (UTF8_ENCODER.encode(content).byteLength > MAX_STATE_BYTES) {
    throw new Error(`${label} exceeds size limit`)
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function requireString(value: unknown, field: string): string {
  const parsed = nonEmptyString(value)
  if (!parsed || parsed.length > 8_192) throw new Error(`guardian ${field} is invalid`)
  return parsed
}

function stringArray(value: unknown, max: number, field: string): string[] {
  if (!Array.isArray(value) || value.length > max) throw new Error(`guardian ${field} is invalid`)
  const parsed = value.map((entry) => requireString(entry, field))
  if (new Set(parsed).size !== parsed.length && field === 'seen event ids') {
    throw new Error(`guardian ${field} contains duplicates`)
  }
  return parsed
}

function requirePositiveInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) {
    throw new Error(`guardian ${field} is invalid`)
  }
  return value as number
}

function requireNonNegativeInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`guardian ${field} is invalid`)
  }
  return value as number
}

function canonicalTimestamp(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const date = new Date(value)
  return Number.isFinite(date.valueOf()) ? date.toISOString() : undefined
}

function timestampFromSlackTs(value: string | undefined): string | undefined {
  if (!value || !/^\d+\.\d+$/u.test(value)) return undefined
  const seconds = Number(value)
  if (!Number.isFinite(seconds) || seconds <= 0) return undefined
  return new Date(seconds * 1_000).toISOString()
}

function requireTimestamp(value: unknown, field: string): string {
  const timestamp = canonicalTimestamp(value)
  if (!timestamp || timestamp !== value) throw new Error(`guardian ${field} is invalid`)
  return timestamp
}

function requireSlackTs(value: unknown, field: string): string {
  const ts = requireString(value, field)
  if (!/^\d+\.\d+$/u.test(ts)) throw new Error(`guardian ${field} is invalid`)
  return ts
}

function requireEventOrder(value: unknown, field: string): string {
  const order = requireString(value, field)
  // Event order is internal and compared lexicographically. Its exact shape
  // is ISO timestamp + Slack ts + stable response identity.
  const match = order.match(
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z):\d+\.\d+:slack:[a-f0-9]{64}$/u,
  )
  if (!match || canonicalTimestamp(match[1]) !== match[1]) {
    throw new Error(`guardian ${field} is invalid`)
  }
  return order
}

function bounded(value: string, max: number): string {
  return value.length <= max ? value : `${value.slice(0, max - 1)}…`
}

// Retain a named predicate for issue adapters/tests without exporting provider internals.
void remediationBodyHasMarker
