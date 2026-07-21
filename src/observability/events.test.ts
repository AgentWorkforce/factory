import { describe, expect, it } from 'vitest'

import {
  FactoryCloudEventBatchV1Schema,
  FactoryCloudEventInputV1Schema,
  FactoryCloudEventV1Schema,
  createFactoryCloudEventV1,
  factoryCloudReleaseReasonV1,
  factoryRunTraceIdV1,
} from './events'

describe('Factory cloud event v1 contract', () => {
  it('creates a valid run event with deterministic injectable identity and time', () => {
    const event = createFactoryCloudEventV1({
      type: 'run.started',
      runId: 'run-1',
      status: 'running',
      run: { source: 'github', repository: 'AgentWorkforce/factory', issueKey: 'factory#123' },
      attributes: { backend: 'internal', dryRun: false, agentRole: 'implementer' },
    }, {
      id: () => 'event-1',
      now: () => new Date('2026-07-19T12:00:00.000Z'),
    })

    expect(event).toEqual(expect.objectContaining({
      id: 'event-1',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'run.started',
      runId: 'run-1',
      trace: { traceId: factoryRunTraceIdV1('run-1') },
    }))
    expect(FactoryCloudEventV1Schema.parse({ ...event, sequence: 1 }).sequence).toBe(1)
  })

  it('correlates every event for a durable run without inventing span ids', () => {
    const started = createFactoryCloudEventV1({ type: 'run.started', runId: 'run-1' })
    const spawned = createFactoryCloudEventV1({ type: 'agent.spawned', runId: 'run-1' })
    const otherRun = createFactoryCloudEventV1({ type: 'run.started', runId: 'run-2' })
    const instance = createFactoryCloudEventV1({ type: 'instance.heartbeat' })

    expect(started.trace).toEqual({ traceId: spawned.trace?.traceId })
    expect(started.trace?.traceId).toMatch(/^[a-f0-9]{32}$/u)
    expect(started.trace?.traceId).not.toBe('00000000000000000000000000000000')
    expect(otherRun.trace?.traceId).not.toBe(started.trace?.traceId)
    expect(started.trace).not.toHaveProperty('spanId')
    expect(instance.trace).toBeUndefined()
  })

  it('preserves explicit future OTel context and rejects W3C-invalid zero ids', () => {
    const explicitTrace = {
      traceId: '1234567890abcdef1234567890abcdef',
      spanId: '1234567890abcdef',
      parentSpanId: 'fedcba0987654321',
    }
    expect(createFactoryCloudEventV1({
      type: 'run.started',
      runId: 'run-1',
      trace: explicitTrace,
    }).trace).toEqual(explicitTrace)

    expect(() => FactoryCloudEventInputV1Schema.parse({
      id: 'event-zero-trace',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'run.started',
      runId: 'run-1',
      trace: { traceId: '00000000000000000000000000000000' },
    })).toThrow(/must not be all zeroes/u)
    expect(() => FactoryCloudEventInputV1Schema.parse({
      id: 'event-zero-span',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'run.started',
      runId: 'run-1',
      trace: {
        traceId: '1234567890abcdef1234567890abcdef',
        spanId: '0000000000000000',
      },
    })).toThrow(/must not be all zeroes/u)
  })

  it('requires run identity for run events', () => {
    expect(() => FactoryCloudEventInputV1Schema.parse({
      id: 'event-1',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'run.failed',
    })).toThrow(/runId is required/u)
  })

  it('maps raw fleet exit reasons to a closed privacy-safe category', () => {
    expect(factoryCloudReleaseReasonV1('node-offline')).toBe('node_offline')
    expect(factoryCloudReleaseReasonV1('worker_exited')).toBe('exited')
    expect(factoryCloudReleaseReasonV1('max delivery retries exceeded')).toBe('delivery_failed')
    expect(factoryCloudReleaseReasonV1('live dispatch state changed')).toBe('source_state_changed')
    expect(factoryCloudReleaseReasonV1('customer path /private/repo failed')).toBe('other')
    expect(factoryCloudReleaseReasonV1(undefined)).toBeUndefined()

    expect(() => FactoryCloudEventInputV1Schema.parse({
      id: 'event-private-reason',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'agent.exited',
      attributes: { releaseReason: 'customer-private-reason' },
    })).toThrow()
  })

  it('accepts only closed cancellation reasons', () => {
    const cancelled = createFactoryCloudEventV1({
      type: 'run.cancelled',
      runId: 'run-cancelled',
      status: 'cancelled',
      attributes: { cancellationReason: 'agent_delivery_failed' },
    })
    expect(cancelled.attributes?.cancellationReason).toBe('agent_delivery_failed')

    expect(() => FactoryCloudEventInputV1Schema.parse({
      id: 'event-private-cancellation',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'run.cancelled',
      runId: 'run-private-cancellation',
      attributes: { cancellationReason: 'recipient unavailable: /private/customer/repo' },
    })).toThrow()
  })

  it('rejects fields that could carry prompts, messages, paths, URLs, or raw errors', () => {
    const base = {
      id: 'event-private',
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'factory.failure',
    }
    for (const privateField of ['message', 'path', 'issueUrl', 'summary', 'stack', 'task', 'prompt']) {
      expect(() => FactoryCloudEventInputV1Schema.parse({ ...base, [privateField]: 'private data' }))
        .toThrow()
    }
    expect(() => FactoryCloudEventInputV1Schema.parse({
      ...base,
      attributes: { errorMessage: 'private data' },
    })).toThrow()
  })

  it('admits only the bounded run cost summary shape', () => {
    const event = createFactoryCloudEventV1({
      type: 'run.cost.v1',
      runId: 'cost-run',
      attributes: { inputTokens: 300, outputTokens: 50, usd: 0.00225 },
      cost: {
        inputTokens: 300,
        outputTokens: 50,
        usd: 0.00225,
        byRole: [{
          role: 'implementer',
          inputTokens: 300,
          outputTokens: 50,
          usd: 0.00225,
          byModel: [{
            model: 'openai/gpt-5.4',
            inputTokens: 300,
            outputTokens: 50,
            usd: 0.00225,
          }],
        }],
      },
    })
    expect(event.trace?.traceId).toBe(factoryRunTraceIdV1('cost-run'))

    expect(() => FactoryCloudEventInputV1Schema.parse({
      ...event,
      cost: { ...event.cost, task: 'inspect /private/customer/repo' },
    })).toThrow()
    expect(() => FactoryCloudEventInputV1Schema.parse({
      ...event,
      cost: {
        ...event.cost,
        byRole: event.cost!.byRole.map((role) => ({ ...role, path: '/private/customer/repo' })),
      },
    })).toThrow()
  })

  it('validates the exact authenticated ingestion batch shape', () => {
    const parsed = FactoryCloudEventBatchV1Schema.parse({
      contract: 'factory.telemetry.v1',
      instance: {
        id: 'instance-1',
        bootId: 'boot-1',
        version: '0.1.32',
        metadata: { name: '  hoopsheet  ' },
      },
      events: [{
        id: 'event-1',
        sequence: 1,
        occurredAt: '2026-07-19T12:00:00.000Z',
        type: 'instance.heartbeat',
      }],
    })
    expect(parsed.events).toHaveLength(1)
    expect(parsed.instance.metadata?.name).toBe('hoopsheet')
    expect(parsed).not.toHaveProperty('workspaceId')
  })

  it('keeps instance metadata.name optional and validates its bounded display value', () => {
    const instance = { id: 'instance-1', bootId: 'boot-1', version: '0.1.32' }
    const event = {
      id: 'event-1',
      sequence: 1,
      occurredAt: '2026-07-19T12:00:00.000Z',
      type: 'instance.heartbeat',
    }

    expect(FactoryCloudEventBatchV1Schema.parse({
      contract: 'factory.telemetry.v1',
      instance,
      events: [event],
    }).instance).toEqual(instance)
    expect(() => FactoryCloudEventBatchV1Schema.parse({
      contract: 'factory.telemetry.v1',
      instance: { ...instance, metadata: { name: '   ' } },
      events: [event],
    })).toThrow()
    expect(() => FactoryCloudEventBatchV1Schema.parse({
      contract: 'factory.telemetry.v1',
      instance: { ...instance, metadata: { name: 'x'.repeat(257) } },
      events: [event],
    })).toThrow()
  })
})
