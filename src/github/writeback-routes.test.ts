import { describe, expect, it } from 'vitest'

import {
  GITHUB_WRITEBACK_ROUTES,
  assertRoutedGithubWritebackPath,
  githubWritebackRoute,
} from './writeback-routes'

describe('githubWritebackRoute', () => {
  it('rejects the label paths Relayfile\'s GitHub adapter has never routed', () => {
    // #431 and #411. Both were authored for months, accepted by every local
    // check, and refused server-side with `Unsupported GitHub writeback path`
    // — which is why no dispatch ever labelled an issue.
    expect(githubWritebackRoute(
      '/github/repos/AgentWorkforce/factory/labels/factory-14d9c1fe-7add-474f-bb50-c9826037d18b.json',
    )).toBeUndefined()
    expect(githubWritebackRoute(
      '/github/repos/AgentWorkforce/factory/issues/412/labels/factory-14d9c1fe-7add-474f-bb50-c9826037d18b.json',
    )).toBeUndefined()
  })

  it('routes every path shape Factory authors', () => {
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/issues/412.json')).toBe('issues')
    expect(githubWritebackRoute(
      '/github/repos/AgentWorkforce/factory/issues/412/comments/factory-abcdef012345abcdef012345.json',
    )).toBe('issue-comments')
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/refs/factory.json')).toBe('refs')
    expect(githubWritebackRoute(
      `/github/repos/AgentWorkforce/factory/refs/${encodeURIComponent('refs/heads/factory/ar-412')}.json`,
    )).toBe('refs')
    expect(githubWritebackRoute(
      '/github/repos/AgentWorkforce/factory/pull-requests/factory-ar-412-agentworkforce-factory.json',
    )).toBe('pull-requests')
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/pulls/85/close.json')).toBe('close-pull-request')
  })

  it('does not admit a near-miss of a routed shape', () => {
    // The `issues` route forbids a third segment, which is the exact reason
    // the per-label issue path did not fall through to it.
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/issues/412/assignees/x.json')).toBeUndefined()
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/milestones/1.json')).toBeUndefined()
    // `close`/`merge` are pinned to a positive PR number by the adapter.
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/pulls/0/close.json')).toBeUndefined()
    expect(githubWritebackRoute('/linear/issues/AR-412.json')).toBeUndefined()
  })

  it('normalises a trailing slash the way the adapter does', () => {
    // The adapter's `findResourceByPath` strips a trailing slash from any path
    // that does not end in `.json` before matching.
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/issues/')).toBe('issues')
    expect(githubWritebackRoute('/github/repos/AgentWorkforce/factory/issues')).toBe('issues')
  })
})

describe('assertRoutedGithubWritebackPath', () => {
  it('names the refused path and what is routable instead', () => {
    // The production failure was an error nobody could attribute to a label
    // write. A refusal that does not say which path it refused reproduces it.
    const path = '/github/repos/AgentWorkforce/factory/labels/factory-14d9c1fe-7add-474f-bb50-c9826037d18b.json'
    expect(() => assertRoutedGithubWritebackPath(path)).toThrow(path)
    expect(() => assertRoutedGithubWritebackPath(path)).toThrow(/issues, issue-comments, .*pull-requests/u)
  })

  it('passes a routed path through', () => {
    expect(() => assertRoutedGithubWritebackPath('/github/repos/AgentWorkforce/factory/issues/412.json')).not.toThrow()
  })

  it('pins the adapter route table to the eight resources it declares', () => {
    // Transcribed from relayfile-adapters `packages/github/src/resources.ts`.
    // A ninth route appearing here without a Factory path that needs it means
    // the table drifted from a copy, not from the adapter.
    expect(GITHUB_WRITEBACK_ROUTES.map((route) => route.name)).toEqual([
      'issues',
      'issue-comments',
      'reviews',
      'pull-requests',
      'refs',
      'close-pull-request',
      'merge',
      'replies',
    ])
  })
})
