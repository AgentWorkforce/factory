import { describe, expect, it, vi } from 'vitest'
import type { BrokerEvent, SendMessageInput, SpawnPtyInput } from '@agent-relay/harness-driver'

import {
  CostLedger,
  FactoryConfigSchema,
  createFactory,
  parseLinearIssue,
  type FactoryCloudEventInputV1,
  type FactoryEventReporter,
  type LinearIssue,
  type LinearWriteback,
  type TriageDecision,
  type TriageEngine,
} from '../../src/index'
import { InternalFleetClient, type HarnessDriverClientLike } from '../../src/fleet/internal-fleet-client'
import { FakeMountClient } from '../../src/testing'
import { InMemoryStateStore } from '../../src/state/in-memory-state-store'

const READY = 'b9bec744-b60c-4745-8022-d90d6ab59ae3'
const IMPLEMENTING = '39b9881d-1196-4c95-8b80-a20f0c7263f7'
const HUMAN_REVIEW = '24462e2d-9946-4dd1-a798-931cdd678498'
const DONE = '83ea5383-bfe9-425a-86ef-517b8190f09a'
const ISSUE_PATH = '/linear/issues/AR-185__cost-accounting.json'
const RAW_TASK = 'private task sentinel: inspect /private/customer/repository'

class UsageHarness implements HarnessDriverClientLike {
  readonly spawned: SpawnPtyInput[] = []
  readonly released: string[] = []
  readonly #agents = new Set<string>()
  readonly #listeners = new Set<(event: BrokerEvent) => void>()

  async spawnPty(input: SpawnPtyInput) {
    this.spawned.push(input)
    this.#agents.add(input.name)
    return { name: input.name, session_ref: `session-${input.name}` }
  }

  async release(name: string) {
    this.released.push(name)
    this.#agents.delete(name)
    return { name }
  }

  async listAgents() {
    return [...this.#agents].map((name) => ({ name }))
  }

  async sendMessage(input: SendMessageInput) {
    return { event_id: `event-${input.to}`, targets: [input.to] }
  }

  async sendInput() {}

  onEvent(listener: (event: BrokerEvent) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  connectEvents() {}

  emitUsage(name: string, model: string, inputTokens: number | null, outputTokens: number | null): void {
    this.#emit({
      kind: 'agent_usage',
      name,
      model,
      usage: { input_tokens: inputTokens, output_tokens: outputTokens },
      event_id: `usage-${name}`,
    })
  }

  emitExit(name: string): void {
    this.#agents.delete(name)
    this.#emit({ kind: 'agent_exit', name, reason: 'completed' })
  }

  #emit(event: Record<string, unknown>): void {
    for (const listener of this.#listeners) listener(event as BrokerEvent)
  }
}

class CostTriage implements TriageEngine {
  async triage(issue: LinearIssue): Promise<TriageDecision> {
    const ref = { uuid: issue.uuid, key: issue.key, path: issue.path }
    return {
      issue: ref,
      routes: [{ repo: 'AgentWorkforce/factory', clonePath: '/work/factory', rationale: 'cost e2e' }],
      scope: 'single',
      implementers: [{
        name: 'ar-185-impl-factory',
        role: 'implementer',
        capability: 'spawn:codex',
        model: 'openai/gpt-5.4',
        task: RAW_TASK,
        repo: 'AgentWorkforce/factory',
        clonePath: '/work/factory',
      }],
      reviewer: {
        name: 'ar-185-review-factory',
        role: 'reviewer',
        capability: 'spawn:claude',
        model: 'anthropic/claude-haiku-4.5',
        task: RAW_TASK,
        repo: 'AgentWorkforce/factory',
        clonePath: '/work/factory',
      },
      thin: false,
      confidence: 'high',
      rationale: 'cost e2e',
    }
  }
}

class RecordingReporter implements FactoryEventReporter {
  readonly events: FactoryCloudEventInputV1[] = []

  async report(event: FactoryCloudEventInputV1): Promise<void> {
    this.events.push(structuredClone(event))
  }

  async flush() {
    return { delivered: this.events.length, pending: 0, attempts: 0, stoppedReason: 'empty' as const }
  }
}

describe('run cost accounting e2e', () => {
  it('attributes fake runtime usage, persists its aggregate, and emits one bounded completion event', async () => {
    const scenario = costScenario('cost-e2e')
    const { factory, harness, stateStore, ledger, reporter, stateWrites, issueRecord } = scenario

    try {
      const issue = parseLinearIssue(ISSUE_PATH, issueRecord)
      const decision = await factory.triageIssue(issue)
      await factory.dispatch(decision)
      expect(harness.spawned.map(({ name, model }) => ({ name, model }))).toEqual([
        { name: 'ar-185-impl-factory', model: 'openai/gpt-5.4' },
        { name: 'ar-185-review', model: 'anthropic/claude-haiku-4.5' },
      ])
      expect(harness.spawned.some((spawn) => spawn.task?.includes(RAW_TASK))).toBe(true)

      const [[, running]] = await stateStore.listDispatchLifecycles('cost-e2e')
      harness.emitUsage('ar-185-impl-factory', 'openai/gpt-5.4', 1_000_000, 100_000)
      harness.emitUsage('ar-185-review', 'anthropic/claude-haiku-4.5', 500_000, 200_000)
      harness.emitUsage('ar-185-impl-factory', 'provider/not-priced', null, null)
      // Completion follows immediately: the accounting seam must drain both
      // async usage callbacks before it freezes the terminal summary.
      harness.emitExit('ar-185-impl-factory')
      await expect(factory.waitForDispatchTerminal(decision.issue)).resolves.toBeUndefined()

      await vi.waitFor(() => expect(reporter.events.filter((event) => event.type === 'cost.model.unpriced'))
        .toHaveLength(1))
      const unpricedEvent = reporter.events.find((event) => event.type === 'cost.model.unpriced')!
      expect(unpricedEvent).toMatchObject({
        runId: running.runId,
        attributes: {
          agentRole: 'implementer',
          model: 'provider/not-priced',
          inputTokens: null,
          outputTokens: null,
          usd: null,
        },
      })
      expect(Object.keys(unpricedEvent.attributes!).sort()).toEqual([
        'agentRole', 'inputTokens', 'model', 'outputTokens', 'usd',
      ])

      const total = ledger.getRunTotal(running.runId)
      expect(ledger.getRunRecords(running.runId)).toHaveLength(3)
      expect(ledger.getRunRecords(running.runId)).toContainEqual(expect.objectContaining({
        role: 'implementer',
        model: 'provider/not-priced',
        inputTokens: null,
        outputTokens: null,
        usd: null,
      }))
      expect(total).toEqual({
        runId: running.runId,
        inputTokens: 1_500_000,
        outputTokens: 300_000,
        usd: 8,
        byRole: [
          {
            role: 'implementer',
            inputTokens: 1_000_000,
            outputTokens: 100_000,
            usd: 6.5,
            byModel: [
              {
                model: 'openai/gpt-5.4',
                inputTokens: 1_000_000,
                outputTokens: 100_000,
                usd: 6.5,
              },
              {
                model: 'provider/not-priced',
                inputTokens: null,
                outputTokens: null,
                usd: null,
              },
            ],
          },
          {
            role: 'reviewer',
            inputTokens: 500_000,
            outputTokens: 200_000,
            usd: 1.5,
            byModel: [{
              model: 'anthropic/claude-haiku-4.5',
              inputTokens: 500_000,
              outputTokens: 200_000,
              usd: 1.5,
            }],
          },
        ],
      })
      expect(ledger.getRunByRole(running.runId).map(({ role }) => role)).toEqual(['implementer', 'reviewer'])

      const completed = (await stateStore.listDispatchLifecycles('cost-e2e'))
        .find(([, lifecycle]) => lifecycle.runId === running.runId)?.[1]
      expect(completed).toMatchObject({ phase: 'complete', cost: total })
      expect(stateWrites).toContain(HUMAN_REVIEW)

      const costEvents = reporter.events.filter((event) => event.type === 'run.cost.v1')
      expect(costEvents).toHaveLength(1)
      expect(costEvents[0]).toMatchObject({
        runId: running.runId,
        attributes: { inputTokens: 1_500_000, outputTokens: 300_000, usd: 8 },
        cost: { inputTokens: 1_500_000, outputTokens: 300_000, usd: 8 },
        trace: { traceId: expect.stringMatching(/^[a-f0-9]{32}$/u) },
      })
      expect(Object.keys(costEvents[0]!.attributes!).sort()).toEqual(['inputTokens', 'outputTokens', 'usd'])
      expectOnlyCostKeys(costEvents[0]!.cost)
      expect(JSON.stringify(reporter.events)).not.toContain(RAW_TASK)
      expect(JSON.stringify(reporter.events)).not.toContain('/private/customer/repository')
    } finally {
      await factory.stop()
    }
  })

  it('records null tokens when a spawned runtime reports no usage', async () => {
    const { factory, harness, stateStore, ledger, reporter, issueRecord } = costScenario('cost-e2e-missing')
    try {
      const issue = parseLinearIssue(ISSUE_PATH, issueRecord)
      const decision = await factory.triageIssue(issue)
      await factory.dispatch(decision)
      const [[, running]] = await stateStore.listDispatchLifecycles('cost-e2e-missing')

      harness.emitUsage('ar-185-impl-factory', 'openai/gpt-5.4', 100, 20)
      harness.emitExit('ar-185-impl-factory')
      await expect(factory.waitForDispatchTerminal(decision.issue)).resolves.toBeUndefined()

      expect(ledger.getRunRecords(running.runId)).toContainEqual({
        runId: running.runId,
        role: 'reviewer',
        model: 'anthropic/claude-haiku-4.5',
        inputTokens: null,
        outputTokens: null,
        usd: null,
      })
      const completed = (await stateStore.listDispatchLifecycles('cost-e2e-missing'))
        .find(([, lifecycle]) => lifecycle.runId === running.runId)?.[1]
      expect(completed?.cost?.byRole.find(({ role }) => role === 'reviewer')).toMatchObject({
        inputTokens: null,
        outputTokens: null,
        usd: null,
      })
      expect(reporter.events.filter((event) => event.type === 'run.cost.v1')).toHaveLength(1)
    } finally {
      await factory.stop()
    }
  })
})

const costScenario = (workspaceId: string) => {
  const issueRecord = linearIssueRecord()
  const mount = new FakeMountClient({ [ISSUE_PATH]: issueRecord })
  const harness = new UsageHarness()
  const stateStore = new InMemoryStateStore({ batchSize: 2 })
  const ledger = new CostLedger()
  const reporter = new RecordingReporter()
  const stateWrites: string[] = []
  const factory = createFactory(factoryConfig(workspaceId), {
    mount,
    fleet: new InternalFleetClient({
      client: harness,
      cwd: '/work/factory',
      resolveAgentRelayMcpCommand: () => undefined,
    }),
    stateStore,
    costLedger: ledger,
    reporter,
    triage: new CostTriage(),
    linear: recordingLinear(stateWrites),
    probePrResolver: async () => ({
      repo: 'AgentWorkforce/factory',
      prNumber: 185,
      headRef: 'factory/185-cost-accounting',
      headRepo: 'AgentWorkforce/factory',
      crossRepository: false,
      state: 'OPEN',
    }),
    logger: { info() {}, warn() {}, error() {} },
    processFinder: async () => ({ status: 'missing' }),
    terminationGraceMs: 0,
    readChildPids: async () => [],
  })
  return { factory, harness, stateStore, ledger, reporter, stateWrites, issueRecord }
}

const factoryConfig = (workspaceId: string) => FactoryConfigSchema.parse({
  workspaceId,
  repos: {
    byLabel: { factory: 'AgentWorkforce/factory' },
    clonePaths: { 'AgentWorkforce/factory': '/work/factory' },
    default: 'AgentWorkforce/factory',
  },
  batchSize: 2,
  models: {
    implementer: 'openai/gpt-5.4',
    reviewer: 'anthropic/claude-haiku-4.5',
  },
  stateIds: {
    readyForAgent: READY,
    agentImplementing: IMPLEMENTING,
    humanReview: HUMAN_REVIEW,
    done: DONE,
  },
  terminalState: 'human-review',
  mergePolicy: 'never',
  verification: { enabled: false },
})

const linearIssueRecord = () => ({
  provider: 'linear',
  objectType: 'issue',
  objectId: 'cost-accounting',
  payload: {
    id: 'cost-accounting',
    identifier: 'AR-185',
    title: 'Add run cost accounting',
    description: RAW_TASK,
    stateId: READY,
    url: 'https://linear.app/agent-relay/issue/AR-185/cost-accounting',
    labels: [{ name: 'factory' }],
    team: { key: 'AR', name: 'Agent Relay' },
    state: { id: READY, name: 'Ready for Agent' },
  },
})

const recordingLinear = (stateWrites: string[]): LinearWriteback => ({
  async postComment() {},
  async setState(_issue, stateId) { stateWrites.push(stateId) },
  async createIssue() { throw new Error('not used') },
  async verify() { return true },
})

const expectOnlyCostKeys = (cost: FactoryCloudEventInputV1['cost']): void => {
  expect(cost).toBeDefined()
  expect(Object.keys(cost!).sort()).toEqual(['byRole', 'inputTokens', 'outputTokens', 'usd'])
  for (const role of cost!.byRole) {
    expect(Object.keys(role).sort()).toEqual(['byModel', 'inputTokens', 'outputTokens', 'role', 'usd'])
    for (const model of role.byModel) {
      expect(Object.keys(model).sort()).toEqual(['inputTokens', 'model', 'outputTokens', 'usd'])
    }
  }
}
