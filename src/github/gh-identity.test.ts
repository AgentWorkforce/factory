import { describe, expect, it } from 'vitest'

import { GhCliGithubMergeGate, MountedGithubMergeGate, type GhRunner } from './merge-gate'
import { localGhMutationAllowed } from './gh-identity'
import { GhCliIssuePublisher } from '../intake/notion'
import { FactoryConfigSchema } from '../config/schema'

/**
 * The defect (#221): Factory writes to GitHub through two identities. The
 * lifecycle writeback honours `github.identity`, but the guarded merge and
 * Notion intake shell out to `gh` unconditionally, so under an explicit
 * `identity: "app"` they still attribute the write to whichever human is
 * logged in locally.
 *
 * Each case below is a must-fire / must-not-fire pair. Merge keeps its
 * historical auto/user policy, while the Notion CLI adapter is a stricter
 * explicit-user local-host capability because the production container has
 * no `gh` binary. A test that only asserted refusal would pass against a
 * change that broke every local run.
 */

const mergeInput = { repo: 'AgentWorkforce/example', number: 7, expectedHeadSha: 'a'.repeat(40) }

/**
 * Every `gh` invocation in this file goes through a fake. Nothing here may
 * reach the network: an earlier draft of this test used the real runner and
 * created a junk issue and overwrote a merged PR's body on the live repo.
 */
const fakeGh = (): { gh: (args: string[], input?: string) => Promise<string>; calls: string[][] } => {
  const calls: string[][] = []
  return {
    calls,
    gh: async (args) => {
      calls.push(args)
      return 'https://github.com/AgentWorkforce/example/issues/1'
    },
  }
}

const recordingRunner = (): { runner: GhRunner; calls: string[][] } => {
  const calls: string[][] = []
  return {
    calls,
    runner: async (args) => {
      calls.push(args)
      return { stdout: '', stderr: '' }
    },
  }
}

const configWith = (identity?: 'app' | 'user' | 'auto') =>
  FactoryConfigSchema.parse({
    repos: { org: 'AgentWorkforce', names: ['factory'] },
    ...(identity ? { github: { identity } } : {}),
  })

describe('local gh mutations under github.identity', () => {
  it('MUST NOT FIRE: auto and user still squash-merge through the local gh CLI', async () => {
    for (const identity of ['auto', 'user'] as const) {
      const { runner, calls } = recordingRunner()
      const result = await new GhCliGithubMergeGate(runner, identity).merge(mergeInput)

      expect(result.merged, `${identity} must keep merging`).toBe(true)
      expect(calls, `${identity} must still invoke gh`).toHaveLength(1)
      expect(calls[0]?.slice(0, 2)).toEqual(['pr', 'merge'])
    }
  })

  it('MUST NOT FIRE: a gate constructed without an identity keeps the historical behavior', async () => {
    const { runner, calls } = recordingRunner()
    const result = await new GhCliGithubMergeGate(runner).merge(mergeInput)

    expect(result.merged).toBe(true)
    expect(calls).toHaveLength(1)
  })

  it('MUST FIRE: identity "app" refuses the guarded merge and never spawns gh', async () => {
    const { runner, calls } = recordingRunner()
    const result = await new GhCliGithubMergeGate(runner, 'app').merge(mergeInput)

    expect(result.merged).toBe(false)
    // Refused before the process boundary, not after a merge already landed.
    expect(calls).toEqual([])
    expect(result.reason).toContain('GitHub identity "app"')
    // The refusal must name the missing server-side capability and the
    // operator's recovery path, or it is an outage with no exit.
    expect(result.reason).toContain('mergePullRequest')
    expect(result.reason).toContain('"user" or "auto"')
  })

  it('MUST NOT FIRE: identity "app" leaves the merge-gate READ working', async () => {
    const path = '/github/repos/AgentWorkforce__example/pulls/by-id/7.json'
    const gate = new MountedGithubMergeGate({
      readFile: async () => ({ content: {
        number: 7,
        mergeable: 'MERGEABLE',
        mergeStateStatus: 'CLEAN',
        headRefOid: mergeInput.expectedHeadSha,
        reviewDecision: 'APPROVED',
        statusCheckRollup: [{ conclusion: 'SUCCESS' }],
        author: 'pr-author',
        reviews: [
          { login: 'reviewer', state: 'APPROVED', commitId: mergeInput.expectedHeadSha, body: 'Looks correct, ship it.' },
        ],
      } }),
    }, new GhCliGithubMergeGate(async () => {
      throw new Error('local gh must not be used for readiness reads')
    }, 'app'))

    await expect(gate.check({ ...mergeInput, path })).resolves.toMatchObject({ verdict: 'READY', ready: true })
  })

  // The FactoryLoop selector's own must-fire/must-not-fire pair lives in
  // `src/orchestrator/factory.test.ts`. It belongs with the loop it selects
  // for, and importing the 23k-line orchestrator module from a third test
  // file measurably slowed the parallel CI workers enough to time out an
  // unrelated 5s MCP test.

  it('MUST NOT FIRE: the selector leaves auto and an absent github key merging', async () => {
    // `github.identity` is synthesised to `auto` when the key is absent, so
    // "no github block" and "identity: auto" must both stay on the old path.
    for (const config of [configWith('auto'), configWith()]) {
      expect(config.github.identity).toBe('auto')
      expect(localGhMutationAllowed(config.github.identity)).toBe(true)
    }
  })

  it('MUST FIRE: identity "app" refuses Notion intake issue create and edit, without invoking gh', async () => {
    const { gh, calls } = fakeGh()
    const publisher = new GhCliIssuePublisher('app', gh)

    await expect(publisher.createIssue({
      repo: 'AgentWorkforce/example',
      title: 'title',
      body: 'body',
      labels: [],
    })).rejects.toThrow(/GitHub identity "app"[\s\S]*createIssue/u)

    await expect(publisher.updateIssue({
      repo: 'AgentWorkforce/example',
      number: 7,
      body: 'body',
    })).rejects.toThrow(/GitHub identity "app"[\s\S]*updateIssue/u)

    // Refused before the process boundary — no write reached GitHub.
    expect(calls).toEqual([])
  })

  it('MUST NOT FIRE: Notion intake under "user" still creates and edits through gh', async () => {
    const { gh, calls } = fakeGh()
    const publisher = new GhCliIssuePublisher('user', gh)

    await expect(publisher.createIssue({
      repo: 'AgentWorkforce/example',
      title: 'title',
      body: 'body',
      labels: ['factory'],
    })).resolves.toMatchObject({ number: 1 })

    await publisher.updateIssue({ repo: 'AgentWorkforce/example', number: 7, body: 'body' })

    expect(calls.map((args) => args.slice(0, 2))).toEqual([
      ['issue', 'create'],
      ['issue', 'edit'],
    ])
  })

  it('MUST FIRE: the Notion refusal is raised by assertWritable, before any claim', () => {
    // The refusal must be reachable WITHOUT calling createIssue, because
    // publishRepoTask reserves an exactly-once delivery claim first. A
    // refusal that only lived inside createIssue would burn that claim and
    // permanently block the operator's retry under a permitted identity.
    const { gh, calls } = fakeGh()

    expect(() => new GhCliIssuePublisher('app', gh).assertWritable())
      .toThrow(/GitHub identity "app"/u)
    expect(calls).toEqual([])
  })

  it('requires exact user identity before the Notion gh adapter can write', () => {
    const user = fakeGh()
    expect(() => new GhCliIssuePublisher('user', user.gh).assertWritable()).not.toThrow()

    const automatic = fakeGh()
    expect(() => new GhCliIssuePublisher('auto', automatic.gh).assertWritable())
      .toThrow(/GitHub identity "auto".*Notion intake.*local gh.*identity.*"user"/iu)
    expect(automatic.calls).toEqual([])
  })

  it.each(['app', 'auto'] as const)('blocks Notion intake reads under %s before spawning gh', async (identity) => {
    const { gh, calls } = fakeGh()
    const publisher = new GhCliIssuePublisher(identity, gh)

    await expect(publisher.missingLabels('AgentWorkforce/example', ['factory']))
      .rejects.toThrow(new RegExp(`GitHub identity "${identity}".*Notion intake.*local gh`, 'iu'))
    expect(calls).toEqual([])
  })
})
