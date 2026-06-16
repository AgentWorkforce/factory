import { describe, expect, it } from 'vitest'

import {
  globMatchesPath,
  globSegmentMatches,
  normalizeChangePath,
  relayfileSdkPathFiltersFor,
} from '../globs'

describe('subscription globs', () => {
  it('normalizes mount paths into comparable segments', () => {
    expect(normalizeChangePath('linear/issues/AR-1.json/')).toEqual(['linear', 'issues', 'AR-1.json'])
    expect(normalizeChangePath('/')).toEqual([])
  })

  it.each([
    ['*', 'AR-1.json', true],
    ['AR-*.json', 'AR-1.json', true],
    ['AR-*.json', 'PEAR-1.json', false],
    ['issue?.json', 'issue?.json', true],
  ])('matches glob segment %s against %s', (glob, segment, expected) => {
    expect(globSegmentMatches(glob, segment)).toBe(expected)
  })

  it.each([
    ['/linear/issues/**', '/linear/issues/AR-1.json', true],
    ['/linear/issues/**', '/linear/issues/by-state/ready/AR-1.json', true],
    ['/linear/issues/*', '/linear/issues/AR-1.json', true],
    ['/linear/issues/*', '/linear/issues/by-state/ready/AR-1.json', false],
    ['/slack/channels/D*/**', '/slack/channels/D123/messages/1/meta.json', true],
    ['/slack/channels/D*/**', '/slack/channels/C123/messages/1/meta.json', false],
    ['/github/repos/*/pulls/by-id/*.json', '/github/repos/AgentWorkforce__cloud/pulls/by-id/2086.json', true],
  ])('matches path %s -> %s', (glob, path, expected) => {
    expect(globMatchesPath(glob, path)).toBe(expected)
  })

  it('converts bridge globs into Relayfile SDK path filters', () => {
    expect(relayfileSdkPathFiltersFor([
      '/linear/issues/**',
      '/github/repos/AgentWorkforce__*/pulls/by-id/*.json',
      '/slack/**/messages',
      '/',
      '/linear/issues/**',
    ])).toEqual([
      '/',
      '/github/repos/*/pulls/by-id/*',
      '/linear/issues/**',
      '/slack/*/messages',
    ])
  })
})
