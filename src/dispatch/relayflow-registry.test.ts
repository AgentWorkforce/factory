import { describe, expect, it, vi } from 'vitest'

import { linearClient, githubClient } from '@relayfile/relay-helpers'

import type { WorkflowRunnerInput, WorkflowRunnerResult } from '../node/factory-node'
import {
  createRelayflowPolicyRegistry,
  dispatchRelayflowForTrigger,
  triggerEventFromChangeEvent,
  RelayflowPolicyRegistry,
  type RelayflowPolicyEntry,
  type TriggerEvent,
  type TriggerMapperContext,
} from './relayflow-registry'
import type { ChangeEvent } from '../ports'

function makeRunner(status: WorkflowRunnerResult['status'] = 'completed', runId = 'run-001') {
  const calls: WorkflowRunnerInput[] = []
  const runner = vi.fn(async (input: WorkflowRunnerInput): Promise<WorkflowRunnerResult> => {
    calls.push(input)
    return { cwd: input.cwd, runner: '@relayflows/core', status, runId }
  })
  return { runner, calls }
}

describe('RelayflowPolicyRegistry', () => {
  it('returns undefined for an unregistered trigger', () => {
    const registry = createRelayflowPolicyRegistry()
    expect(registry.resolve('linear.issue.created')).toBeUndefined()
    expect(registry.size).toBe(0)
  })

  it('resolves a registered trigger to its policy entry', () => {
    const entry: RelayflowPolicyEntry = {
      templatePath: 'workflows/triage.yaml',
      mapInputs: (event) => ({ issueId: event.resourceId }),
    }
    const registry = createRelayflowPolicyRegistry()
    registry.register('linear.issue.created', entry)

    expect(registry.resolve('linear.issue.created')).toBe(entry)
    expect(registry.size).toBe(1)
  })

  it('supports chained register calls and multiple triggers', () => {
    const registry = createRelayflowPolicyRegistry()
    registry
      .register('linear.issue.created', {
        templatePath: 'workflows/triage.yaml',
        mapInputs: () => ({}),
      })
      .register('github.pull_request.review_submitted', {
        templatePath: 'workflows/review-gate.yaml',
        mapInputs: () => ({}),
      })

    expect(registry.size).toBe(2)
    expect([...registry.triggers()]).toEqual([
      'linear.issue.created',
      'github.pull_request.review_submitted',
    ])
  })

  it('allows overriding an existing trigger entry', () => {
    const registry = new RelayflowPolicyRegistry()
    const first: RelayflowPolicyEntry = { templatePath: 'a.yaml', mapInputs: () => ({}) }
    const second: RelayflowPolicyEntry = { templatePath: 'b.yaml', mapInputs: () => ({}) }

    registry.register('linear.issue.created', first)
    registry.register('linear.issue.created', second)

    expect(registry.resolve('linear.issue.created')).toBe(second)
    expect(registry.size).toBe(1)
  })
})

describe('dispatchRelayflowForTrigger', () => {
  it('returns null when no policy is registered for the trigger', async () => {
    const registry = createRelayflowPolicyRegistry()
    const { runner } = makeRunner()
    const result = await dispatchRelayflowForTrigger(
      { trigger: 'linear.issue.created', resourceId: 'issue-uuid-1' },
      registry,
      { cwd: '/work', workflowRunner: runner },
    )
    expect(result).toBeNull()
    expect(runner).not.toHaveBeenCalled()
  })

  it('invokes the workflow runner with the correct template path and shaped inputs', async () => {
    const { runner, calls } = makeRunner('completed', 'run-abc')
    const registry = createRelayflowPolicyRegistry()
    registry.register('linear.issue.created', {
      templatePath: 'workflows/new-issue.yaml',
      mapInputs: (event) => ({
        issueId: event.resourceId,
        source: 'linear',
      }),
    })

    const event: TriggerEvent = { trigger: 'linear.issue.created', resourceId: 'uuid-99' }
    const result = await dispatchRelayflowForTrigger(event, registry, {
      cwd: '/work/factory',
      workflowRunner: runner,
    })

    expect(result).not.toBeNull()
    expect(result!.trigger).toBe('linear.issue.created')
    expect(result!.templatePath).toBe('workflows/new-issue.yaml')
    expect(result!.runId).toBe('run-abc')
    expect(result!.status).toBe('completed')
    expect(result!.inputs).toEqual({ issueId: 'uuid-99', source: 'linear' })

    expect(calls).toHaveLength(1)
    expect(calls[0]).toMatchObject({
      workflow: 'workflows/new-issue.yaml',
      cwd: '/work/factory',
      inputs: { issueId: 'uuid-99', source: 'linear' },
    })
  })

  it('supports async input mappers (e.g. reading provider state)', async () => {
    const { runner, calls } = makeRunner()
    const registry = createRelayflowPolicyRegistry()

    registry.register('github.pull_request.review_submitted', {
      templatePath: 'workflows/review-gate.yaml',
      mapInputs: async (event) => {
        // Simulate an async provider read (e.g. relay-helpers client.getIssue)
        await Promise.resolve()
        return { prNumber: event.payload?.number, repo: event.payload?.repo }
      },
    })

    await dispatchRelayflowForTrigger(
      {
        trigger: 'github.pull_request.review_submitted',
        payload: { number: 42, repo: 'AgentWorkforce/factory' },
      },
      registry,
      { cwd: '/work', workflowRunner: runner },
    )

    expect(calls[0]!.inputs).toEqual({ prNumber: 42, repo: 'AgentWorkforce/factory' })
  })

  it('passes relay-helpers-backed provider clients to the mapper', async () => {
    const { runner } = makeRunner()
    const capturedCtx: { linear?: TriggerMapperContext['linear'] } = {}

    const registry = createRelayflowPolicyRegistry()
    registry.register('linear.issue.created', {
      templatePath: 'workflows/triage.yaml',
      mapInputs: (_event, ctx) => {
        capturedCtx.linear = ctx.linear
        return {}
      },
    })

    await dispatchRelayflowForTrigger(
      { trigger: 'linear.issue.created' },
      registry,
      { cwd: '/work', mountRoot: '/mnt/.integrations', workflowRunner: runner },
    )

    // The context must expose a relay-helpers LinearClient (not a hand-rolled object)
    // by checking that the issues path matches the writeback-path catalog resolution.
    const expected = linearClient({ mountRoot: '/mnt/.integrations' })
    expect(capturedCtx.linear!.issues.path()).toBe(expected.issues.path())
    // Catalog-backed: should be /linear/issues, not any hardcoded variation
    expect(capturedCtx.linear!.issues.path()).toBe('/linear/issues')
  })

  it('uses catalog-backed paths for GitHub via relay-helpers (not hardcoded)', async () => {
    const { runner } = makeRunner()
    const capturedCtx: { github?: TriggerMapperContext['github'] } = {}

    const registry = createRelayflowPolicyRegistry()
    registry.register('github.pull_request.review_submitted', {
      templatePath: 'workflows/review-gate.yaml',
      mapInputs: (_event, ctx) => {
        capturedCtx.github = ctx.github
        return {}
      },
    })

    await dispatchRelayflowForTrigger(
      { trigger: 'github.pull_request.review_submitted' },
      registry,
      { cwd: '/work', workflowRunner: runner },
    )

    // Verify the github client path matches the catalog (not a hardcoded string)
    const expected = githubClient()
    expect(capturedCtx.github!.issues.path({ owner: 'AgentWorkforce', repo: 'factory' }))
      .toBe(expected.issues.path({ owner: 'AgentWorkforce', repo: 'factory' }))
    expect(capturedCtx.github!.issues.path({ owner: 'AgentWorkforce', repo: 'factory' }))
      .toBe('/github/repos/AgentWorkforce/factory/issues')
  })

  it('exposes dynamic relay-helpers clients for non-named providers', async () => {
    const { runner, calls } = makeRunner()
    const registry = createRelayflowPolicyRegistry()

    registry.register('notion.page.created', {
      templatePath: 'workflows/notion-page.yaml',
      mapInputs: (_event, ctx) => ({
        relayProvider: ctx.relayClient('notion').provider,
        relayPath: ctx.relayClient('notion').path('pages', { pageId: 'page-1' }),
        providerPath: ctx.providerClient('notion').pages.path({ pageId: 'page-1' }),
      }),
    })

    await dispatchRelayflowForTrigger(
      { trigger: 'notion.page.created', resourceId: 'page-1' },
      registry,
      { cwd: '/work', mountRoot: '/mnt/.integrations', workflowRunner: runner },
    )

    expect(calls[0]!.inputs).toEqual({
      relayProvider: 'notion',
      relayPath: '/notion/pages/page-1/meta.json',
      providerPath: '/notion/pages/page-1/meta.json',
    })
  })

  it('propagates the workflow runner status and run id on needs_human outcome', async () => {
    const { runner } = makeRunner('needs_human', 'run-waiting')
    const registry = createRelayflowPolicyRegistry()
    registry.register('linear.issue.created', {
      templatePath: 'workflows/triage.yaml',
      mapInputs: () => ({}),
    })

    const result = await dispatchRelayflowForTrigger(
      { trigger: 'linear.issue.created' },
      registry,
      { cwd: '/work', workflowRunner: runner },
    )

    expect(result!.status).toBe('needs_human')
    expect(result!.runId).toBe('run-waiting')
  })
})

describe('triggerEventFromChangeEvent', () => {
  it('normalizes Relayfile change events into policy trigger names', () => {
    const change = {
      id: 'evt-1',
      workspace: 'factory-test',
      type: 'file.created',
      occurredAt: '2026-06-27T00:00:00.000Z',
      resource: {
        path: '/github/repos/AgentWorkforce/factory/pulls/42/meta.json',
        kind: 'file',
        id: '42',
        provider: 'github',
      },
      summary: {},
      expand: async () => ({ level: 'summary', path: '/github/repos/AgentWorkforce/factory/pulls/42/meta.json', summary: {} }),
    } as unknown as ChangeEvent

    expect(triggerEventFromChangeEvent(change)).toMatchObject({
      trigger: 'github.pull_request.created',
      resourceId: '42',
      payload: {
        path: '/github/repos/AgentWorkforce/factory/pulls/42/meta.json',
        provider: 'github',
        resource: 'pull_request',
      },
    })
  })
})
