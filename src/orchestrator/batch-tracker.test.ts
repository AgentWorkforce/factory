import { describe, expect, it } from 'vitest'

import type { AgentSpec } from '../ports'
import type { TriageDecision } from '../types'
import { BatchTracker } from './batch-tracker'
import { dependencyIdentity, findDependencyCycle, parseBlockedBy, resolveDependency } from './dependencies'

const dependencyDecision = (number: number): TriageDecision => ({
  issue: {
    uuid: `AgentWorkforce/pear#${number}`,
    key: String(number),
    path: `/github/repos/AgentWorkforce/pear/issues/by-id/${number}.json`,
  },
  routes: [{ repo: 'AgentWorkforce/pear', rationale: 'test' }],
  scope: 'single',
  implementers: [],
  reviewer: {
    name: `review-${number}`,
    role: 'reviewer',
    capability: 'spawn:claude',
    model: 'claude',
    task: 'review',
    repo: 'AgentWorkforce/pear',
    node: 'self',
  },
  thin: false,
  confidence: 'high',
  rationale: 'test',
})

describe('dependency-aware BatchTracker admission', () => {
  it('parks dependencies separately from capacity and queues after dependencies resolve', () => {
    const tracker = new BatchTracker(1)
    const blocker = dependencyDecision(1)
    const dependent = dependencyDecision(2)
    expect(tracker.start(blocker, false)).toBeDefined()

    expect(tracker.start(dependent, false, {
      blockers: [{
        identity: 'agentworkforce/pear#1',
        key: '1:AgentWorkforce/pear#1:/github/repos/AgentWorkforce/pear/issues/by-id/1.json',
        label: 'AgentWorkforce/pear#1',
      }],
    })).toBeUndefined()

    expect(tracker.isParked(dependent.issue)).toBe(true)
    expect(tracker.isQueued(dependent.issue)).toBe(false)
    expect(tracker.getParked(dependent.issue)).toMatchObject({ capacityBlocked: true })

    expect(tracker.start(dependent, false, { blockers: [] })).toBeUndefined()
    expect(tracker.isParked(dependent.issue)).toBe(false)
    expect(tracker.isQueued(dependent.issue)).toBe(true)
    expect(tracker.complete(blocker.issue)?.issue).toEqual(dependent.issue)
  })

  it('parses only strict Blocked by lines and keeps equal issue numbers repo-qualified', () => {
    const parsed = parseBlockedBy([
      'Blocked by: #7, AgentWorkforce/hoopsheet#7',
      'This is blocked by #8 in prose.',
      'Blocked by: #9 and #10',
    ].join('\n'))

    expect(parsed).toEqual([
      { number: 7, raw: '#7' },
      { repo: 'AgentWorkforce/hoopsheet', number: 7, raw: 'AgentWorkforce/hoopsheet#7' },
    ])
    expect(parsed.map((dependency) => resolveDependency(dependency, 'AgentWorkforce/pear')?.identity)).toEqual([
      dependencyIdentity('AgentWorkforce/pear', 7),
      dependencyIdentity('AgentWorkforce/hoopsheet', 7),
    ])
  })

  it('treats a missing or empty description as no declared dependencies', () => {
    expect(parseBlockedBy(undefined)).toEqual([])
    expect(parseBlockedBy(null)).toEqual([])
    expect(parseBlockedBy('')).toEqual([])
  })

  it('finds direct and transitive cycles reachable from an issue', () => {
    const graph = new Map<string, string[]>([
      ['agentworkforce/pear#1', ['agentworkforce/pear#2']],
      ['agentworkforce/pear#2', ['agentworkforce/pear#3']],
      ['agentworkforce/pear#3', ['agentworkforce/pear#2']],
    ])

    expect(findDependencyCycle('agentworkforce/pear#1', graph)).toEqual([
      'agentworkforce/pear#2',
      'agentworkforce/pear#3',
      'agentworkforce/pear#2',
    ])
  })
})

describe('BatchTracker implementation capacity', () => {
  it('frees a bare-repo implementation slot after the canonical PR is handed to a babysitter', () => {
    const tracker = new BatchTracker(1)
    const first = decision(1, 'pear')
    const record = tracker.start(first, false)
    expect(record).toBeDefined()
    tracker.recordPlanned(record!, babysitter(1, 'pear', 'AgentWorkforce/pear'))

    expect(tracker.canStart()).toBe(true)
    expect(tracker.start(decision(2, 'factory'), false)).toBeDefined()
  })

  it('keeps the slot occupied when a canonical PR belongs to a different owner', () => {
    const tracker = new BatchTracker(1)
    const first = decision(1, 'AgentWorkforce/pear')
    const record = tracker.start(first, false)
    expect(record).toBeDefined()
    tracker.recordPlanned(record!, babysitter(1, 'pear', 'someone-else/pear'))

    expect(tracker.canStart()).toBe(false)
  })
})

const decision = (number: number, repo: string): TriageDecision => {
  const issue = { key: String(number), uuid: `uuid-${number}`, path: `/issues/${number}.json` }
  return {
    issue,
    routes: [{ repo, rationale: 'test' }],
    scope: 'single',
    implementers: [{
      name: `ar-${number}-impl`,
      role: 'implementer',
      capability: 'spawn:codex',
      task: 'implement',
      repo,
    }],
    reviewer: {
      name: `ar-${number}-review`,
      role: 'reviewer',
      capability: 'spawn:claude',
      task: 'review',
      repo,
    },
    thin: false,
    confidence: 'high',
    rationale: 'test',
  }
}

const babysitter = (number: number, repo: string, ownedRepo: string): AgentSpec => ({
  name: `ar-${number}-babysit`,
  role: 'babysitter',
  capability: 'spawn:claude',
  task: 'babysit',
  repo,
  ownedPullRequest: { repo: ownedRepo, number },
})
