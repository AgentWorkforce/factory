import { describe, expect, it } from 'vitest'

import type { DispatchLifecycle } from '../ports/state'
import { githubLifecycleIdentity, matchingGithubLifecycleEntry } from './github-lifecycle-identity'

describe('GitHub lifecycle identity', () => {
  it('ignores malformed legacy lifecycle rows', () => {
    expect(githubLifecycleIdentity({ issue: undefined } as unknown as Pick<DispatchLifecycle, 'issue'>))
      .toBeUndefined()
    expect(githubLifecycleIdentity({ issue: { path: '' } } as unknown as Pick<DispatchLifecycle, 'issue'>))
      .toBeUndefined()
  })

  it('selects the best logical alias in one pass with deterministic legacy timestamp handling', () => {
    const queuedWithoutTimestamp = lifecycle(
      '/github/repos/AgentWorkforce__factory/issues/by-id/146.json',
      'queued',
      undefined,
    )
    const active = lifecycle(
      '/github/repos/AgentWorkforce/factory/issues/146__test-infra/meta.json',
      'running',
      1,
    )
    const unrelated = lifecycle(
      '/github/repos/AgentWorkforce/factory/issues/147__other/meta.json',
      'running',
      2,
    )

    expect(matchingGithubLifecycleEntry([
      ['queued', queuedWithoutTimestamp],
      ['unrelated', unrelated],
      ['active', active],
    ], queuedWithoutTimestamp)).toEqual(['active', active])
  })
})

const lifecycle = (
  path: string,
  phase: DispatchLifecycle['phase'],
  updatedAtMs: number | undefined,
): DispatchLifecycle => ({
  issue: { key: '146', uuid: 'AgentWorkforce/factory#146', path },
  phase,
  updatedAtMs,
} as DispatchLifecycle)
