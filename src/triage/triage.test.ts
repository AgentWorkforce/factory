import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema, type FactoryConfig } from '../config/schema'
import type { LinearIssue, TriageContext, TriageDecision, TriageEngine } from '../types'
import { HeuristicTriage } from './heuristic'
import { LlmTriage } from './llm'
import { TieredTriage } from './tiered'

const baseConfig = FactoryConfigSchema.parse({
  workspaceId: 'ws_123',
  repos: {
    byLabel: {
      pear: 'AgentWorkforce/pear',
      agents: 'AgentWorkforce/agents',
    },
    byProject: {
      'Pear Launch': 'AgentWorkforce/pear',
    },
    keywordRules: [
      { pattern: '\\bcloud\\b', repo: 'AgentWorkforce/cloud' },
      { pattern: '\\bdocs?\\b', repo: 'AgentWorkforce/docs' },
    ],
    clonePaths: {
      'AgentWorkforce/pear': '/work/pear',
      'AgentWorkforce/agents': '/work/agents',
      'AgentWorkforce/cloud': '/work/cloud',
      'AgentWorkforce/docs': '/work/docs',
    },
    default: 'AgentWorkforce/pear',
  },
  models: {
    implementer: 'codex-test',
    reviewer: 'claude-test',
  },
})

const ctx: TriageContext = {
  config: baseConfig,
  repoMap: [
    { repo: 'AgentWorkforce/pear', clonePath: '/repo-map/pear', source: 'label', key: 'pear' },
  ],
}

describe('HeuristicTriage routing', () => {
  it.each([
    {
      name: 'label has highest precedence',
      issue: issue({ labels: ['pear'], project: 'Other', title: 'Fix cloud deploy', description: richDescription('cloud') }),
      config: baseConfig,
      expectedRepo: 'AgentWorkforce/pear',
      expectedConfidence: 'high',
    },
    {
      name: 'project is used when labels do not route',
      issue: issue({ labels: ['unknown'], project: 'Pear Launch' }),
      config: baseConfig,
      expectedRepo: 'AgentWorkforce/pear',
      expectedConfidence: 'high',
    },
    {
      name: 'keyword rule is used after label and project miss',
      issue: issue({ labels: ['unknown'], project: 'Unknown', title: 'Fix cloud worker routing' }),
      config: baseConfig,
      expectedRepo: 'AgentWorkforce/cloud',
      expectedConfidence: 'high',
    },
    {
      name: 'default is used after route miss',
      issue: issue({ labels: ['unknown'], project: 'Unknown', title: 'Fix coordinator fallback' }),
      config: baseConfig,
      expectedRepo: 'AgentWorkforce/pear',
      expectedConfidence: 'high',
    },
    {
      name: 'unroutable issue escalates instead of guessing',
      issue: issue({ labels: ['unknown'], project: 'Unknown', title: 'Fix coordinator fallback' }),
      config: configWithoutDefault(),
      expectedRepo: undefined,
      expectedConfidence: 'low',
    },
  ])('$name', async ({ issue: linearIssue, config, expectedRepo, expectedConfidence }) => {
    const decision = await new HeuristicTriage().triage(linearIssue, { ...ctx, config })

    expect(decision.routes[0]?.repo).toBe(expectedRepo)
    expect(decision.confidence).toBe(expectedConfidence)
    expect(decision.rationale).not.toMatch(/guess/i)
  })

  it('keeps same-precedence multi-route matches and caps implementers at two', async () => {
    const decision = await new HeuristicTriage().triage(issue({
      labels: ['pear', 'agents'],
      description: richDescription('Update renderer and broker surfaces with tests in src/main/broker.ts.'),
    }), ctx)

    expect(decision.routes.map((route) => route.repo)).toEqual(['AgentWorkforce/pear', 'AgentWorkforce/agents'])
    expect(decision.scope).toBe('team')
    expect(decision.implementers).toHaveLength(2)
    expect(decision.implementers.map((agent) => agent.name)).toEqual(['ar-123-impl-pear', 'ar-123-impl-agents'])
  })

  it('fans out to one implementer per route when triage.maxImplementers is raised', async () => {
    const config = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      triage: { maxImplementers: 3 },
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
          agents: 'AgentWorkforce/agents',
          cloud: 'AgentWorkforce/cloud',
        },
        clonePaths: {
          'AgentWorkforce/pear': '/work/pear',
          'AgentWorkforce/agents': '/work/agents',
          'AgentWorkforce/cloud': '/work/cloud',
        },
      },
      models: { implementer: 'codex-test', reviewer: 'claude-test' },
    })

    const decision = await new HeuristicTriage().triage(issue({
      labels: ['pear', 'agents', 'cloud'],
      description: richDescription('Update renderer, broker, and cloud surfaces with tests in src/main/broker.ts.'),
    }), { ...ctx, config })

    expect(decision.routes.map((route) => route.repo)).toEqual([
      'AgentWorkforce/pear',
      'AgentWorkforce/agents',
      'AgentWorkforce/cloud',
    ])
    expect(decision.implementers).toHaveLength(3)
    expect(decision.implementers.map((agent) => agent.name)).toEqual([
      'ar-123-impl-pear',
      'ar-123-impl-agents',
      'ar-123-impl-cloud',
    ])
  })
})

describe('HeuristicTriage thin and scope detection', () => {
  it.each([
    { name: 'short description is thin', description: 'Fix tests in src/main/broker.ts.', thin: true },
    { name: 'long description without acceptance signal is thin', description: 'Background '.repeat(30), thin: true },
    { name: 'long description with acceptance signal is not thin', description: richDescription('Add regression tests in src/main/broker.ts and verify the error path.'), thin: false },
  ])('$name', async ({ description, thin }) => {
    const decision = await new HeuristicTriage().triage(issue({ description }), ctx)

    expect(decision.thin).toBe(thin)
  })

  it.each([
    { name: 'single surface stays single', description: richDescription('Fix renderer resizing in src/renderer/terminal.ts.'), scope: 'single' },
    { name: 'two surface buckets become team', description: richDescription('Fix renderer IPC with main broker tests in src/main/broker.ts.'), scope: 'team' },
    { name: 'two routes become team', labels: ['pear', 'agents'], description: richDescription('Fix shared task routing in src/main/broker.ts.'), scope: 'team' },
  ])('$name', async ({ labels, description, scope }) => {
    const decision = await new HeuristicTriage().triage(issue({ labels: labels ?? ['pear'], description }), ctx)

    expect(decision.scope).toBe(scope)
  })

  it('creates two scoped implementers for a same-repo team split', async () => {
    const decision = await new HeuristicTriage().triage(issue({
      description: richDescription('Fix renderer IPC with main broker tests in src/main/broker.ts.'),
    }), ctx)

    expect(decision.routes).toHaveLength(1)
    expect(decision.scope).toBe('team')
    expect(decision.implementers.map((implementer) => implementer.name)).toEqual(['ar-123-impl-ui', 'ar-123-impl-main'])
    expect(decision.implementers.every((implementer) => implementer.repo === 'AgentWorkforce/pear')).toBe(true)
  })

  it('builds AgentSpec entries from config models and clone paths', async () => {
    const decision = await new HeuristicTriage().triage(issue(), ctx)

    expect(decision.implementers[0]).toMatchObject({
      name: 'ar-123-impl',
      capability: 'spawn:codex',
      model: 'codex-test',
      repo: 'AgentWorkforce/pear',
      clonePath: '/work/pear',
      node: 'self',
    })
    expect(decision.reviewer).toMatchObject({
      name: 'ar-123-review',
      capability: 'spawn:claude',
      model: 'claude-test',
      repo: 'AgentWorkforce/pear',
      clonePath: '/work/pear',
      node: 'self',
    })
  })
})

describe('LlmTriage', () => {
  it('parses a valid JSON TriageDecision from an injected complete function', async () => {
    const expected = decisionJson()
    const prompts: string[] = []
    const triage = new LlmTriage(async (prompt) => {
      prompts.push(prompt)
      return JSON.stringify(expected)
    })

    await expect(triage.triage(issue(), ctx)).resolves.toEqual(expected)
    expect(prompts[0]).toContain('Repo routing precedence')
    expect(prompts[0]).toContain('AR-123')
  })

  it('throws on malformed JSON', async () => {
    const triage = new LlmTriage(async () => '{nope')

    await expect(triage.triage(issue(), ctx)).rejects.toThrow()
  })

  it('throws on timeout', async () => {
    const triage = new LlmTriage(() => new Promise((resolve) => {
      setTimeout(() => resolve(JSON.stringify(decisionJson())), 50)
    }), { timeoutMs: 1 })

    await expect(triage.triage(issue(), ctx)).rejects.toThrow(/timed out/i)
  })
})

describe('TieredTriage', () => {
  it('returns a confident non-thin heuristic decision without LLM', async () => {
    const heuristic = new StubTriage({ ...decisionJson(), thin: false, confidence: 'high' })
    const llm = new StubTriage(decisionJson({ routes: [{ repo: 'AgentWorkforce/cloud', clonePath: '/work/cloud', rationale: 'llm' }] }))
    const decision = await new TieredTriage(heuristic, llm).triage(issue(), ctx)

    expect(decision.routes[0]?.repo).toBe('AgentWorkforce/pear')
    expect(llm.calls).toBe(0)
  })

  it('downgrades a thin heuristic result to low confidence when LLM is absent', async () => {
    const heuristic = new StubTriage({ ...decisionJson(), thin: true, confidence: 'high' })
    const decision = await new TieredTriage(heuristic).triage(issue(), ctx)

    expect(decision.confidence).toBe('low')
    expect(decision.thin).toBe(true)
  })

  it('lets LLM fill routes for a thin or low-confidence heuristic decision', async () => {
    const heuristic = new StubTriage(decisionJson({ routes: [], implementers: [], thin: true, confidence: 'low' }))
    const llm = new StubTriage(decisionJson({
      routes: [{ repo: 'AgentWorkforce/cloud', clonePath: '/work/cloud', rationale: 'llm route' }],
      thin: false,
      confidence: 'high',
      rationale: 'llm filled route',
    }))

    const decision = await new TieredTriage(heuristic, llm).triage(issue(), ctx)

    expect(decision.confidence).toBe('high')
    expect(decision.thin).toBe(false)
    expect(decision.routes).toEqual([{ repo: 'AgentWorkforce/cloud', clonePath: '/work/cloud', rationale: 'llm route' }])
    expect(decision.implementers.map((agent) => agent.name)).toEqual(['ar-123-impl'])
  })

  it('falls back to low-confidence heuristic result when LLM fails', async () => {
    const heuristic = new StubTriage({ ...decisionJson(), thin: true, confidence: 'high' })
    const llm = new FailingTriage()

    const decision = await new TieredTriage(heuristic, llm).triage(issue(), ctx)

    expect(decision.confidence).toBe('low')
    expect(decision.routes[0]?.repo).toBe('AgentWorkforce/pear')
  })

  it('preserves naming and max-two batch invariants when merging LLM output', async () => {
    const heuristic = new StubTriage({ ...decisionJson(), thin: true, confidence: 'low' })
    const llm = new StubTriage(decisionJson({
      scope: 'team',
      routes: [
        { repo: 'AgentWorkforce/pear', clonePath: '/work/pear', rationale: 'one' },
        { repo: 'AgentWorkforce/agents', clonePath: '/work/agents', rationale: 'two' },
        { repo: 'AgentWorkforce/cloud', clonePath: '/work/cloud', rationale: 'three' },
      ],
      implementers: [
        { ...agent('bad-name', 'AgentWorkforce/pear'), name: 'bad-name' },
        { ...agent('also-bad', 'AgentWorkforce/agents'), name: 'also-bad' },
        { ...agent('third-bad', 'AgentWorkforce/cloud'), name: 'third-bad' },
      ],
    }))

    const decision = await new TieredTriage(heuristic, llm).triage(issue(), ctx)

    expect(decision.routes.map((route) => route.repo)).toEqual(['AgentWorkforce/pear', 'AgentWorkforce/agents'])
    expect(decision.implementers).toHaveLength(2)
    expect(decision.implementers.map((implementer) => implementer.name)).toEqual(['ar-123-impl-bad-name', 'ar-123-impl-also-bad'])
    expect(decision.reviewer.name).toBe('ar-123-review')
  })
})

class StubTriage implements TriageEngine {
  calls = 0

  constructor(readonly decision: TriageDecision) {}

  async triage(): Promise<TriageDecision> {
    this.calls += 1
    return this.decision
  }
}

class FailingTriage implements TriageEngine {
  async triage(): Promise<TriageDecision> {
    throw new Error('llm unavailable')
  }
}

function issue(overrides: Partial<LinearIssue> = {}): LinearIssue {
  return {
    uuid: 'uuid-123',
    key: 'AR-123',
    title: 'Fix terminal broker routing',
    description: richDescription('Fix broker routing in src/main/broker.ts and add tests for the error path.'),
    stateId: 'ready',
    state: { name: 'Ready for Agent' },
    labels: ['pear'],
    project: undefined,
    team: 'agent-relay',
    assignee: undefined,
    path: '/linear/issues/AR-123__uuid-123.json',
    raw: {},
    ...overrides,
  }
}

function richDescription(detail: string): string {
  return `${detail} `.repeat(12)
}

function configWithoutDefault(): FactoryConfig {
  const { default: _default, ...repos } = baseConfig.repos
  return {
    ...baseConfig,
    repos,
  }
}

function decisionJson(overrides: Partial<TriageDecision> = {}): TriageDecision {
  const route = { repo: 'AgentWorkforce/pear', clonePath: '/work/pear', rationale: 'matched label' }
  return {
    issue: { uuid: 'uuid-123', key: 'AR-123', path: '/linear/issues/AR-123__uuid-123.json' },
    routes: [route],
    scope: 'single',
    implementers: [agent('ar-123-impl', 'AgentWorkforce/pear')],
    reviewer: {
      ...agent('ar-123-review', 'AgentWorkforce/pear'),
      role: 'reviewer',
      capability: 'spawn:claude',
      model: 'claude-test',
    },
    thin: false,
    confidence: 'high',
    rationale: 'matched label',
    ...overrides,
  }
}

function agent(name: string, repo: string) {
  return {
    name,
    role: 'implementer' as const,
    capability: 'spawn:codex' as const,
    model: 'codex-test',
    task: `Task for ${name}`,
    repo,
    clonePath: `/work/${repo.split('/').at(-1)}`,
    node: 'self' as const,
  }
}
