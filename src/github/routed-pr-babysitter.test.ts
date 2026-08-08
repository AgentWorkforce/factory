import { describe, expect, it } from 'vitest'

import { FactoryConfigSchema } from '../config/schema'
import type { MountClient } from '../ports'
import { discoverRoutedPullRequests, routedPrRepos } from './routed-pr-babysitter'

const config = (babysitter: Record<string, unknown> = {}) => FactoryConfigSchema.parse({
  repos: {
    org: 'AgentWorkforce',
    names: ['pear'],
    byLabel: { legacy: 'AgentWorkforce/legacy' },
  },
  babysitter: { enabled: true, mode: 'routed-open-prs', ...babysitter },
})

const pull = (overrides: Record<string, unknown> = {}) => ({
  number: 7,
  title: 'Repair routed babysitting',
  body: 'Use the PR as the definition of done.',
  state: 'OPEN',
  isDraft: false,
  merged: false,
  url: 'https://github.com/AgentWorkforce/pear/pull/7',
  headRefName: 'fix/routed-babysitting',
  headRefOid: 'abc123',
  baseRefName: 'main',
  headRepository: { nameWithOwner: 'AgentWorkforce/pear' },
  isCrossRepository: false,
  labels: [],
  ...overrides,
})

const mount = (files: Record<string, unknown>): MountClient => ({
  listTree: async (prefix: string) => Object.keys(files).filter((path) => path.startsWith(prefix)),
  readFile: async (path: string) => ({ content: files[path] }),
} as unknown as MountClient)

describe('routed PR babysitter discovery', () => {
  it('bounds widened intake strictly to repos.names', () => {
    expect(routedPrRepos(config())).toEqual(['AgentWorkforce/pear'])
  })

  it('deduplicates mount aliases and returns complete same-repository PRs', async () => {
    const files = {
      '/github/repos/AgentWorkforce/pear/pulls/7/meta.json': pull(),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull(),
      '/github/repos/AgentWorkforce__legacy/pulls/by-id/8.json': pull({ number: 8 }),
    }
    const report = await discoverRoutedPullRequests(mount(files), config())
    expect(report.scanned).toBe(2)
    expect(report.duplicates).toBe(1)
    expect(report.candidates.map(({ repo, number }) => `${repo}#${number}`)).toEqual([
      'AgentWorkforce/pear#7',
    ])
  })

  it('continues through an unreadable alias root and an unreadable first PR alias', async () => {
    const nested = '/github/repos/AgentWorkforce/pear/pulls/7/meta.json'
    const byID = '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json'
    const client = {
      listTree: async (prefix: string) => {
        if (prefix.includes('/AgentWorkforce/pear/')) throw new Error('nested alias unavailable')
        return [nested, byID]
      },
      readFile: async (path: string) => {
        if (path === nested) throw new Error('stale alias unreadable')
        return { content: pull() }
      },
    } as unknown as MountClient

    const report = await discoverRoutedPullRequests(client, config())

    expect(report.candidates.map(({ number }) => number)).toEqual([7])
    expect(report.failures.map(({ reason }) => reason)).toEqual([
      expect.stringContaining('nested alias unavailable'),
      'stale alias unreadable',
    ])
  })

  it('reads REST and established changed-file aliases', async () => {
    const rest = await discoverRoutedPullRequests(mount({
      '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull({
        files: [{ filename: 'src/rest.ts' }],
      }),
    }), config())
    expect(rest.candidates[0]?.filesChanged).toEqual(['src/rest.ts'])

    const established = await discoverRoutedPullRequests(mount({
      '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull({
        files_changed: [{ path: 'src/established.ts' }],
      }),
    }), config())
    expect(established.candidates[0]?.filesChanged).toEqual(['src/established.ts'])
  })

  it('honours label and config opt-outs before admission', async () => {
    const files = {
      '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull({
        labels: [{ name: 'factory:skip-babysitter' }],
      }),
    }
    const byLabel = await discoverRoutedPullRequests(mount(files), config())
    expect(byLabel.excluded).toBe(1)
    expect(byLabel.candidates).toEqual([])

    const byIdentity = await discoverRoutedPullRequests(
      mount({ ...files, '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull() }),
      config({ excludePullRequests: ['AgentWorkforce/pear#7'] }),
    )
    expect(byIdentity.excluded).toBe(1)
    expect(byIdentity.candidates).toEqual([])
  })

  it('fails closed on incomplete, draft, and cross-repository metadata', async () => {
    const files = {
      '/github/repos/AgentWorkforce__pear/pulls/by-id/7.json': pull({ headRefOid: undefined }),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/8.json': pull({ number: 8, isDraft: true }),
      '/github/repos/AgentWorkforce__pear/pulls/by-id/9.json': pull({
        number: 9,
        headRepository: { nameWithOwner: 'contributor/pear' },
        isCrossRepository: true,
      }),
    }
    const report = await discoverRoutedPullRequests(mount(files), config())
    expect(report.incomplete).toBe(1)
    expect(report.terminal).toBe(1)
    expect(report.crossRepository).toBe(1)
    expect(report.candidates).toEqual([])
  })
})
