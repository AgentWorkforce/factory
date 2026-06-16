import { describe, expect, it, vi } from 'vitest'

import { resolveFactoryStates, stateResolutionFromIds, type LinearStateReader } from './state-resolver'

// In-memory /linear/states reader. `index` is the rows in _index.json; `records`
// maps state id -> canonical record (name + team).
function makeReader(
  index: Array<{ id: string }>,
  records: Record<string, { name?: string; team_key?: string; team_name?: string }>,
): LinearStateReader & { reads: string[] } {
  const reads: string[] = []
  return {
    reads,
    async readFile(path: string) {
      reads.push(path)
      if (path === '/linear/states/_index.json') return { content: index }
      const id = path.replace('/linear/states/', '').replace('.json', '')
      if (records[id]) return { content: { provider: 'linear', objectType: 'state', payload: { id, ...records[id] } } }
      throw Object.assign(new Error('not found'), { code: 'ENOENT' })
    },
  }
}

const AR_STATES = [{ id: 's1' }, { id: 's2' }, { id: 's3' }, { id: 's4' }, { id: 's5' }]
const AR_RECORDS = {
  s1: { name: 'Ready for Agent', team_key: 'AR' },
  s2: { name: 'Agent Implementing', team_key: 'AR' },
  s3: { name: 'In Planning', team_key: 'AR' },
  s4: { name: 'Done', team_key: 'AR' },
  s5: { name: 'In Human Review', team_key: 'AR' },
}

describe('resolveFactoryStates', () => {
  it('resolves role names to UUIDs from /linear/states', async () => {
    const reader = makeReader(AR_STATES, AR_RECORDS)
    const states = await resolveFactoryStates(reader, {
      states: {
        readyForAgent: 'Ready for Agent',
        agentImplementing: 'Agent Implementing',
        inPlanning: 'In Planning',
        done: 'Done',
        humanReview: 'In Human Review',
      },
    })

    expect(states.idFor(undefined, 'readyForAgent')).toBe('s1')
    expect(states.idFor(undefined, 'done')).toBe('s4')
    expect(states.roleOf('s2')).toBe('agentImplementing')
    expect(states.isRole('s5', 'humanReview')).toBe(true)
    expect(states.roleOf('unknown')).toBeUndefined()
    expect(states.hasHumanReview()).toBe(true)
  })

  it('uses explicit stateIds without reading the states resource', async () => {
    const reader = makeReader(AR_STATES, AR_RECORDS)
    const states = await resolveFactoryStates(reader, {
      stateIds: { readyForAgent: 'x1', agentImplementing: 'x2', inPlanning: 'x3', done: 'x4' },
    })

    expect(states.idFor(undefined, 'readyForAgent')).toBe('x1')
    expect(states.roleOf('x4')).toBe('done')
    expect(reader.reads).toEqual([]) // never touched /linear/states
  })

  it('resolves different state names per team, each scoped to its own team', async () => {
    const reader = makeReader(
      [{ id: 'a-ready' }, { id: 'a-impl' }, { id: 'a-plan' }, { id: 'a-done' }, { id: 'e-todo' }, { id: 'e-build' }, { id: 'e-plan' }, { id: 'e-ship' }],
      {
        'a-ready': { name: 'Ready for Agent', team_key: 'AR' },
        'a-impl': { name: 'Agent Implementing', team_key: 'AR' },
        'a-plan': { name: 'In Planning', team_key: 'AR' },
        'a-done': { name: 'Done', team_key: 'AR' },
        'e-todo': { name: 'To Do', team_key: 'ENG' },
        'e-build': { name: 'Building', team_key: 'ENG' },
        'e-plan': { name: 'Backlog', team_key: 'ENG' },
        'e-ship': { name: 'Shipped', team_key: 'ENG' },
      },
    )
    const states = await resolveFactoryStates(reader, {
      // AR uses the global defaults (all those names exist in AR); ENG fully
      // overrides with its own names (all exist in ENG).
      states: { readyForAgent: 'Ready for Agent', agentImplementing: 'Agent Implementing', inPlanning: 'In Planning', done: 'Done' },
      statesByTeam: {
        ENG: { readyForAgent: 'To Do', agentImplementing: 'Building', inPlanning: 'Backlog', done: 'Shipped' },
      },
      teams: ['AR', 'ENG'],
    })

    expect(states.idFor('AR', 'readyForAgent')).toBe('a-ready')
    expect(states.idFor('AR', 'agentImplementing')).toBe('a-impl')
    expect(states.idFor('ENG', 'readyForAgent')).toBe('e-todo')
    expect(states.idFor('ENG', 'agentImplementing')).toBe('e-build')
    expect(states.idFor('ENG', 'done')).toBe('e-ship')
    // reverse map covers both teams
    expect(states.roleOf('e-todo')).toBe('readyForAgent')
    expect(states.roleOf('a-ready')).toBe('readyForAgent')
  })

  it('throws when a required role cannot be resolved', async () => {
    const reader = makeReader(AR_STATES, AR_RECORDS)
    await expect(resolveFactoryStates(reader, {
      states: { readyForAgent: 'Ready for Agent' }, // missing the other required roles
    })).rejects.toThrow(/missing \[agentImplementing, inPlanning, done\]/)
  })

  it("never resolves a team role to another team's state of the same name", async () => {
    // "Done" exists only in ENG. Resolving it for AR must fail loudly rather
    // than silently borrowing ENG's state id (cross-team mis-routing).
    const reader = makeReader(
      [{ id: 'ar-ready' }, { id: 'ar-impl' }, { id: 'ar-plan' }, { id: 'eng-done' }],
      {
        'ar-ready': { name: 'Ready for Agent', team_key: 'AR' },
        'ar-impl': { name: 'Agent Implementing', team_key: 'AR' },
        'ar-plan': { name: 'In Planning', team_key: 'AR' },
        'eng-done': { name: 'Done', team_key: 'ENG' },
      },
    )
    await expect(resolveFactoryStates(reader, {
      statesByTeam: {
        AR: { readyForAgent: 'Ready for Agent', agentImplementing: 'Agent Implementing', inPlanning: 'In Planning', done: 'Done' },
      },
      teams: ['AR'],
    })).rejects.toThrow(/No Linear workflow state named "Done" for team "AR"/)
  })

  it('matches statesByTeam keys case-insensitively against teams', async () => {
    const reader = makeReader(
      [{ id: 'e-todo' }, { id: 'e-impl' }, { id: 'e-plan' }, { id: 'e-ship' }],
      {
        'e-todo': { name: 'To Do', team_key: 'ENG' },
        'e-impl': { name: 'Building', team_key: 'ENG' },
        'e-plan': { name: 'Backlog', team_key: 'ENG' },
        'e-ship': { name: 'Shipped', team_key: 'ENG' },
      },
    )
    // statesByTeam key is "ENG" but the subscribed team token is lowercase "eng".
    const states = await resolveFactoryStates(reader, {
      statesByTeam: {
        ENG: { readyForAgent: 'To Do', agentImplementing: 'Building', inPlanning: 'Backlog', done: 'Shipped' },
      },
      teams: ['eng'],
    })

    expect(states.idFor('eng', 'readyForAgent')).toBe('e-todo')
    expect(states.idFor('ENG', 'done')).toBe('e-ship')
    expect(states.roleOf('e-todo')).toBe('readyForAgent')
  })

  it('throws a clear error when the states resource is unavailable', async () => {
    const reader: LinearStateReader = {
      async readFile() { throw new Error('no such file') },
    }
    await expect(resolveFactoryStates(reader, {
      states: { readyForAgent: 'Ready for Agent' },
    })).rejects.toThrow(/_index\.json is unavailable/)
  })

  it('errors on an ambiguous name without team scoping', async () => {
    const reader = makeReader(
      [{ id: 'a' }, { id: 'b' }],
      { a: { name: 'Done', team_key: 'AR' }, b: { name: 'Done', team_key: 'ENG' } },
    )
    await expect(resolveFactoryStates(reader, {
      states: { readyForAgent: 'Done', agentImplementing: 'Done', inPlanning: 'Done', done: 'Done' },
    })).rejects.toThrow(/ambiguous/)
  })
})

describe('stateResolutionFromIds', () => {
  it('builds forward and reverse lookups from explicit ids', () => {
    const states = stateResolutionFromIds({ readyForAgent: 'r', done: 'd' })
    expect(states.idFor('any-team', 'readyForAgent')).toBe('r')
    expect(states.roleOf('d')).toBe('done')
    expect(states.isRole('r', 'readyForAgent')).toBe(true)
    expect(() => states.idFor(undefined, 'agentImplementing')).toThrow(/No resolved Linear state/)
  })
})
