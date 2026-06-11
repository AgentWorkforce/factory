import { describe, expect, it } from 'vitest'

import type { ChangeEvent, MountClient } from '../../ports'
import {
  filterLinearPredicateSpecs,
  hasLinearPredicates,
  linearIssueMatchesPredicates,
  linearRecordCandidates,
  linearScopePredicates,
} from '../linear-filter'

describe('Linear subscription predicates', () => {
  const issue = {
    teamId: 'team-id',
    team: { id: 'team-id', key: 'AR', name: 'Agent Relay' },
    projectId: 'project-id',
    project: { id: 'project-id', name: 'Pear Launch' },
    labelIds: ['label-id'],
    labels: [{ id: 'label-id', name: 'cloud' }, { name: 'agent' }],
    assigneeId: 'user-id',
    assignee: { id: 'user-id', name: 'KJG', email: 'kjg@example.invalid' },
  }

  it('extracts scope lists and reports whether predicates are active', () => {
    const predicates = linearScopePredicates({
      teams: ['Agent Relay'],
      labels: ['cloud'],
      projects: [],
      assignees: ['kjg@example.invalid'],
      ignored: ['x'],
    })

    expect(predicates).toEqual({
      teams: ['Agent Relay'],
      projects: [],
      labels: ['cloud'],
      assignees: ['kjg@example.invalid'],
    })
    expect(hasLinearPredicates(predicates)).toBe(true)
    expect(hasLinearPredicates({ teams: [], projects: [], labels: [], assignees: [] })).toBe(false)
  })

  it('builds candidate sets for Linear issue fields', () => {
    expect([...linearRecordCandidates(issue, 'teams')].sort()).toEqual(['agent relay', 'ar', 'team-id'])
    expect([...linearRecordCandidates(issue, 'labels')].sort()).toEqual(['agent', 'cloud', 'label-id'])
  })

  it.each([
    [{ teams: ['AR'], projects: ['Pear Launch'], labels: ['cloud'], assignees: ['KJG'] }, true],
    [{ teams: ['team-id'], projects: [], labels: ['label-id'], assignees: ['user-id'] }, true],
    [{ teams: ['Other Team'], projects: [], labels: [], assignees: [] }, false],
    [{ teams: [], projects: [], labels: ['frontend'], assignees: [] }, false],
  ])('matches issue records against predicates %#', (predicates, expected) => {
    expect(linearIssueMatchesPredicates(issue, predicates)).toBe(expected)
  })

  it('filters predicate specs by reading the Linear issue from the mount', async () => {
    const event = {
      id: 'evt-1',
      resource: { path: '/linear/issues/AR-1__uuid.json' },
    } as ChangeEvent
    const mount = {
      readFile: async () => ({ content: JSON.stringify(issue) }),
    } as Pick<MountClient, 'readFile'>
    const specs = [
      { integrationId: 'unfiltered' },
      { integrationId: 'match', linearPredicates: { teams: ['AR'], projects: [], labels: ['cloud'], assignees: [] } },
      { integrationId: 'drop', linearPredicates: { teams: ['Other'], projects: [], labels: [], assignees: [] } },
    ]

    await expect(filterLinearPredicateSpecs({ mount, event, matchedSpecs: specs }))
      .resolves.toEqual([specs[0], specs[1]])
  })
})
