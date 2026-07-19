import { createHash, randomUUID } from 'node:crypto'

import { z } from 'zod'

export const FACTORY_CLOUD_EVENT_CONTRACT_V1 = 'factory.telemetry.v1' as const
export const FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE = 100
/** Leave headroom below Cloud's 256 KiB request-body limit. */
export const FACTORY_CLOUD_EVENT_MAX_PAYLOAD_BYTES = 240 * 1024

export const FACTORY_CLOUD_RELEASE_REASONS_V1 = [
  'completed',
  'human_review',
  'factory_stopped',
  'waiting_for_human',
  'exited',
  'offline',
  'node_offline',
  'crashed',
  'reconciled_missing',
  'delivery_failed',
  'dispatch_failed',
  'source_state_changed',
  'other',
] as const

export type FactoryCloudReleaseReasonV1 = (typeof FACTORY_CLOUD_RELEASE_REASONS_V1)[number]

export const FACTORY_CLOUD_CANCELLATION_REASONS_V1 = [
  'source_state_changed',
  'agent_spawn_failed',
  'agent_delivery_failed',
  'dispatch_failed',
] as const

export type FactoryCloudCancellationReasonV1 = (typeof FACTORY_CLOUD_CANCELLATION_REASONS_V1)[number]

export const FACTORY_CLOUD_EVENT_TYPES = [
  'instance.started',
  'instance.heartbeat',
  'instance.stopping',
  'instance.stopped',
  'run.queued',
  'run.started',
  'run.phase_changed',
  'run.waiting',
  'run.blocked',
  'run.succeeded',
  'run.failed',
  'run.cancelled',
  'agent.planned',
  'agent.spawned',
  'agent.adopted',
  'agent.resumed',
  'agent.exited',
  'agent.released',
  'clarification.requested',
  'clarification.delivered',
  'clarification.waiting',
  'clarification.answered',
  'clarification.woken',
  'clarification.escalated',
  'pull_request.discovered',
  'pull_request.published',
  'pull_request.ready',
  'pull_request.merged',
  'writeback.applied',
  'factory.failure',
  'factory.anomaly',
  'factory.snapshot',
] as const

export type FactoryCloudEventType = (typeof FACTORY_CLOUD_EVENT_TYPES)[number] | (string & {})

const opaqueString = z.string().trim().min(1).max(256)
const categoryString = z.string().trim().min(1).max(120).regex(/^[a-zA-Z0-9][a-zA-Z0-9._:/-]*$/u)
const nonNegativeInteger = z.number().int().nonnegative()
const boundedCount = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER)
const W3C_INVALID_TRACE_ID = '00000000000000000000000000000000'
const W3C_INVALID_SPAN_ID = '0000000000000000'
const FACTORY_RUN_TRACE_DOMAIN_V1 = 'agent-relay.factory.run-trace.v1\0'

export const FactoryCloudTraceIdV1Schema = z.string()
  .regex(/^[a-f0-9]{32}$/u)
  .refine((value) => value !== W3C_INVALID_TRACE_ID, 'traceId must not be all zeroes')

export const FactoryCloudSpanIdV1Schema = z.string()
  .regex(/^[a-f0-9]{16}$/u)
  .refine((value) => value !== W3C_INVALID_SPAN_ID, 'spanId must not be all zeroes')

/**
 * Deliberately narrow operational attributes. This is not a general log bag:
 * task text, prompts, issue descriptions, messages, paths, URLs, command lines,
 * stack traces, and raw error messages have no representable field.
 */
export const FactoryCloudEventAttributesV1Schema = z.object({
  backend: categoryString.optional(),
  mode: categoryString.optional(),
  component: categoryString.optional(),
  operation: categoryString.optional(),
  errorClass: categoryString.optional(),
  errorCode: categoryString.optional(),
  errorFingerprint: categoryString.optional(),
  retryable: z.boolean().optional(),
  attempt: nonNegativeInteger.optional(),
  durationMs: nonNegativeInteger.optional(),
  agentRole: categoryString.optional(),
  invocationId: opaqueString.optional(),
  nodeId: opaqueString.optional(),
  previousPhase: categoryString.optional(),
  releaseReason: z.enum(FACTORY_CLOUD_RELEASE_REASONS_V1).optional(),
  cancellationReason: z.enum(FACTORY_CLOUD_CANCELLATION_REASONS_V1).optional(),
  dryRun: z.boolean().optional(),
  count: boundedCount.optional(),
  activeRuns: boundedCount.optional(),
  queuedRuns: boundedCount.optional(),
  trackedAgents: boundedCount.optional(),
  unownedAgents: boundedCount.optional(),
  outboxDepth: boundedCount.optional(),
  outboxOldestAgeMs: nonNegativeInteger.optional(),
  droppedEvents: boundedCount.optional(),
  transport: categoryString.optional(),
  locality: categoryString.optional(),
}).strict()

export type FactoryCloudEventAttributesV1 = z.infer<typeof FactoryCloudEventAttributesV1Schema>

export const FactoryCloudInstanceV1Schema = z.object({
  id: opaqueString,
  bootId: opaqueString,
  version: z.string().trim().min(1).max(64),
  metadata: z.object({
    name: opaqueString.optional(),
    backend: categoryString.optional(),
    mode: categoryString.optional(),
    platform: categoryString.optional(),
    arch: categoryString.optional(),
    runtime: categoryString.optional(),
  }).strict().optional(),
}).strict()

export type FactoryCloudInstanceV1 = z.infer<typeof FactoryCloudInstanceV1Schema>

const factoryCloudEventShape = {
  id: opaqueString,
  occurredAt: z.string().datetime({ offset: true }),
  type: categoryString,
  level: z.enum(['debug', 'info', 'warn', 'error']).optional(),
  runId: opaqueString.optional(),
  phase: categoryString.optional(),
  status: z.enum(['queued', 'running', 'waiting', 'blocked', 'succeeded', 'failed', 'cancelled']).optional(),
  run: z.object({
    source: categoryString.optional(),
    repository: opaqueString.optional(),
    issueKey: opaqueString.optional(),
    recipe: categoryString.optional(),
  }).strict().optional(),
  attributes: FactoryCloudEventAttributesV1Schema.optional(),
  trace: z.object({
    traceId: FactoryCloudTraceIdV1Schema,
    spanId: FactoryCloudSpanIdV1Schema.optional(),
    parentSpanId: FactoryCloudSpanIdV1Schema.optional(),
  }).strict().optional(),
} as const

const requireRunId = (
  event: { type: string; runId?: string },
  context: z.RefinementCtx,
): void => {
  if (event.type.startsWith('run.') && !event.runId) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['runId'],
      message: 'runId is required for run.* events',
    })
  }
}

export const FactoryCloudEventInputV1Schema = z.object(factoryCloudEventShape).strict().superRefine(requireRunId)
export const FactoryCloudEventV1Schema = z.object({
  ...factoryCloudEventShape,
  sequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
}).strict().superRefine(requireRunId)

export type FactoryCloudEventInputV1 = z.infer<typeof FactoryCloudEventInputV1Schema>
export type FactoryCloudEventV1 = z.infer<typeof FactoryCloudEventV1Schema>

export const FactoryCloudEventBatchV1Schema = z.object({
  contract: z.literal(FACTORY_CLOUD_EVENT_CONTRACT_V1),
  instance: FactoryCloudInstanceV1Schema,
  events: z.array(FactoryCloudEventV1Schema).min(1).max(FACTORY_CLOUD_EVENT_MAX_BATCH_SIZE),
}).strict()

export type FactoryCloudEventBatchV1 = z.infer<typeof FactoryCloudEventBatchV1Schema>

export interface CreateFactoryCloudEventV1Options {
  id?: () => string
  now?: () => Date
}

/**
 * Stable correlation ID for the durable run. It has W3C trace-id shape but is
 * not evidence that an OpenTelemetry span was created or exported.
 */
export function factoryRunTraceIdV1(runId: string): string {
  const normalizedRunId = opaqueString.parse(runId)
  const traceId = createHash('sha256')
    .update(FACTORY_RUN_TRACE_DOMAIN_V1)
    .update(normalizedRunId)
    .digest('hex')
    .slice(0, 32)
  // SHA-256 producing this value is fantastically unlikely, but W3C forbids it
  // and this helper promises validity rather than probability.
  return traceId === W3C_INVALID_TRACE_ID
    ? '00000000000000000000000000000001'
    : traceId
}

export function createFactoryCloudEventV1(
  input: Omit<FactoryCloudEventInputV1, 'id' | 'occurredAt'> & Partial<Pick<FactoryCloudEventInputV1, 'id' | 'occurredAt'>>,
  options: CreateFactoryCloudEventV1Options = {},
): FactoryCloudEventInputV1 {
  const trace = input.trace ?? (input.runId ? { traceId: factoryRunTraceIdV1(input.runId) } : undefined)
  return FactoryCloudEventInputV1Schema.parse({
    ...input,
    id: input.id ?? (options.id ?? randomUUID)(),
    occurredAt: input.occurredAt ?? (options.now ?? (() => new Date()))().toISOString(),
    ...(trace ? { trace } : {}),
  })
}

/**
 * Converts provider-owned exit/release text into a closed operational category.
 * Unknown text is never forwarded because it can contain task or error details.
 */
export function factoryCloudReleaseReasonV1(value: string | undefined): FactoryCloudReleaseReasonV1 | undefined {
  if (value === undefined || !value.trim()) return undefined
  switch (value.trim().toLowerCase()) {
    case 'issue-done':
    case 'done':
    case 'completed':
      return 'completed'
    case 'issue-human-review':
      return 'human_review'
    case 'factory-stopped':
      return 'factory_stopped'
    case 'waiting-for-human':
      return 'waiting_for_human'
    case 'task_exit':
    case 'worker_exited':
    case 'exited':
      return 'exited'
    case 'offline':
      return 'offline'
    case 'node-offline':
      return 'node_offline'
    case 'crash':
    case 'crashed':
      return 'crashed'
    case 'reconciled-missing':
      return 'reconciled_missing'
    case 'dead-lettered':
    case 'dropped':
    case 'max delivery retries exceeded':
    case 'recipient unavailable':
      return 'delivery_failed'
    case 'dispatch failed':
      return 'dispatch_failed'
    case 'live dispatch state changed':
    case 'source state changed':
      return 'source_state_changed'
    default:
      return 'other'
  }
}

export function isCriticalFactoryCloudEvent(event: Pick<FactoryCloudEventInputV1, 'type' | 'level' | 'status'>): boolean {
  return event.level === 'error' ||
    event.type === 'factory.failure' ||
    event.type === 'factory.anomaly' ||
    event.type === 'instance.stopped' ||
    event.status === 'failed' ||
    event.status === 'succeeded' ||
    event.status === 'cancelled'
}
