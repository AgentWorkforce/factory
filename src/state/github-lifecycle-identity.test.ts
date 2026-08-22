import { describe, expect, it } from 'vitest'

import type { DispatchLifecycle } from '../ports/state'
import { githubLifecycleIdentity } from './github-lifecycle-identity'
import { planLifecycleMigration } from './work-unit-lifecycle-migration'

describe('GitHub lifecycle identity', () => {
  it('ignores malformed legacy lifecycle rows', () => {
    expect(githubLifecycleIdentity({ issue: undefined } as unknown as Pick<DispatchLifecycle, 'issue'>))
      .toBeUndefined()
    expect(githubLifecycleIdentity({ issue: { path: '' } } as unknown as Pick<DispatchLifecycle, 'issue'>))
      .toBeUndefined()
  })

  it('picks the liveliest alias to adopt, with deterministic legacy timestamp handling', () => {
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

    // The GitHub-only lifecycle scan this module used to own became the
    // provider-neutral rekey; the alias-ranking behaviour it guarded lives on
    // there. `active` outranks the queued row, and the unrelated issue is not
    // an alias of it at all.
    expect(planLifecycleMigration(
      [
        ['queued', queuedWithoutTimestamp],
        ['unrelated', unrelated],
        ['active', active],
      ],
      'github:agentworkforce/factory#146',
      queuedWithoutTimestamp,
      1_000,
    )).toEqual({ outcome: 'adopt', from: 'active', aliases: ['queued'] })
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
