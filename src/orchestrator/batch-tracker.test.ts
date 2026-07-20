import { describe, expect, it } from 'vitest'

import type { AgentSpec } from '../ports'
import type { TriageDecision } from '../types'
import { BatchTracker } from './batch-tracker'

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
