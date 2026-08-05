import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  normalizeNotionManifest,
  normalizeNotionPageId,
  parseChiefSpecHeader,
  runNotionIntake,
  type GithubIssuePublisher,
  type NotionIntakeManifest,
  type WorkspaceTaskDispatcher,
} from './notion'

const pageId = '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf'
const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('Notion spec intake', () => {
  it('normalizes compact app URLs into canonical Notion page ids', () => {
    expect(normalizeNotionPageId(
      'https://app.notion.com/p/Gmail-3b36800c1c90801db1cfc8f2e1cff7cf?source=copy_link',
    )).toBe(pageId)
    expect(normalizeNotionPageId(pageId)).toBe(pageId)
  })

  it('requires an explicit, ready Chief Spec header and parses both destination kinds', () => {
    expect(() => parseChiefSpecHeader('Continue this work')).toThrow('first line must be exactly "# Chief Spec"')
    expect(() => parseChiefSpecHeader('# Chief Spec\nStatus: draft\n')).toThrow('Status must be ready')
    expect(() => parseChiefSpecHeader([
      '# Chief Spec',
      'Status: ready',
      'Title: Unsafe typo',
      'Summary: This must fail closed.',
      'Recipe: team',
      'Repository: AgentWorkforce/cloud',
    ].join('\n'))).toThrow('unknown Chief Spec field')

    expect(parseChiefSpecHeader([
      '# Chief Spec',
      'Status: ready',
      'Title: Reconcile the integration',
      'Summary: Finish the already-approved recovery procedure.',
      'Recipe: team',
      'Repos: AgentWorkforce/cloud, AgentWorkforce/relay',
      'Project-Paths: /work/benchmark',
      'Node: benchmark-host',
      '',
      'Private implementation details follow.',
    ].join('\n'))).toEqual({
      title: 'Reconcile the integration',
      summary: 'Finish the already-approved recovery procedure.',
      recipe: 'team',
      targets: [
        { repo: 'AgentWorkforce/cloud', labels: [] },
        { repo: 'AgentWorkforce/relay', labels: [] },
        { projectPath: '/work/benchmark', node: 'benchmark-host' },
      ],
    })
  })

  it('allows only an exact, principal-authorized bootstrap mapping for headerless legacy pages', async () => {
    const { root, manifest } = await fixtureManifest('headerless legacy checkpoint', {
      bootstrap: {
        authorizedPageId: pageId,
        reason: 'operator supplied the exact page and repository mapping',
        status: 'ready',
        title: 'Resume recovery',
        recipe: 'team',
        summary: 'Resume from the durable checkpoint.',
        targets: [{ repo: 'AgentWorkforce/cloud', labels: [] }],
      },
    })
    roots.push(root)

    const tasks = await normalizeNotionManifest(manifest)
    expect(tasks).toHaveLength(1)
    expect(tasks[0]).toMatchObject({
      pageId,
      bootstrap: true,
      sourceKey: `notion:${pageId}:repo:agentworkforce/cloud`,
      target: { repo: 'AgentWorkforce/cloud' },
    })

    manifest.tasks[0]!.bootstrap!.authorizedPageId = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    await expect(normalizeNotionManifest(manifest)).rejects.toThrow('does not match mounted page')
  })

  it('never publishes mounted content to a public repository without a reviewed public summary', async () => {
    const { root, manifest } = await fixtureManifest('secret body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/relay', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'public' })

    const report = await runNotionIntake({ manifest, dispatch: true, github })

    expect(report.ok).toBe(false)
    expect(report.results[0]).toMatchObject({ status: 'blocked', reason: expect.stringContaining('publicSummary') })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('publishes the privacy-safe summary with lifecycle labels and is idempotent by source marker', async () => {
    const mapping = bootstrap({
      repo: 'AgentWorkforce/relay',
      labels: ['relay'],
      publicSummary: 'Resolve the already-scoped fleet reliability follow-ups.',
    })
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', { bootstrap: mapping })
    roots.push(root)
    const github = fakeGithub({ visibility: 'public' })

    const first = await runNotionIntake({ manifest, dispatch: true, github })
    const body = vi.mocked(github.createIssue).mock.calls[0]![0].body
    expect(first.results[0]).toMatchObject({ status: 'dispatched', issue: { number: 42 } })
    expect(body).toContain('Resolve the already-scoped fleet reliability follow-ups.')
    expect(body).not.toContain('private mounted implementation detail')
    expect(vi.mocked(github.createIssue).mock.calls[0]![0].labels).toEqual([
      'factory-ready',
      'agent:team',
      'relay',
    ])

    vi.mocked(github.findBySource).mockResolvedValue({ number: 42, url: 'https://github.test/issues/42', body })
    const second = await runNotionIntake({ manifest, dispatch: true, github })
    expect(second.results[0]).toMatchObject({ status: 'already-dispatched', issue: { number: 42 } })
    expect(github.createIssue).toHaveBeenCalledTimes(1)
  })

  it('dispatches exact-path work once and persists a digest-bound receipt', async () => {
    const { root, manifest } = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(root)
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
    }

    const first = await runNotionIntake({
      manifest,
      dispatch: true,
      workspace,
      now: () => new Date('2026-08-05T22:00:00.000Z'),
    })
    expect(first.results[0]).toMatchObject({ status: 'dispatched', agent: 'benchmark-agent', node: 'kjg-laptop' })
    expect(workspace.dispatch).toHaveBeenCalledWith(expect.objectContaining({
      projectPath: '/work/benchmark',
      node: 'kjg-laptop',
      invocationId: expect.stringContaining(`factory:notion:${pageId}:workspace:/work/benchmark`),
    }))
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:workspace:/work/benchmark`]).toMatchObject({
      agent: 'benchmark-agent',
      dispatchedAt: '2026-08-05T22:00:00.000Z',
    })

    const second = await runNotionIntake({ manifest, dispatch: true, workspace })
    expect(second.results[0]).toMatchObject({ status: 'already-dispatched', agent: 'benchmark-agent' })
    expect(workspace.dispatch).toHaveBeenCalledTimes(1)
  })

  it('retains per-destination results when a later publisher fails', async () => {
    const { root, manifest } = await fixtureManifest('private body', {
      bootstrap: {
        ...bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
        targets: [
          { repo: 'AgentWorkforce/cloud', labels: [] },
          { repo: 'AgentWorkforce/chief', labels: [] },
        ],
      },
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    vi.mocked(github.findBySource)
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error('GitHub unavailable'))

    const report = await runNotionIntake({ manifest, dispatch: true, github })

    expect(report.ok).toBe(false)
    expect(report.results).toEqual([
      expect.objectContaining({ status: 'dispatched', target: { repo: 'AgentWorkforce/cloud', labels: [] } }),
      expect.objectContaining({ status: 'blocked', reason: 'GitHub unavailable' }),
    ])
  })
})

async function fixtureManifest(
  content: string,
  task: NotionIntakeManifest['tasks'][number],
): Promise<{ root: string; manifest: NotionIntakeManifest }> {
  const root = await mkdtemp(join(tmpdir(), 'factory-notion-intake-'))
  const mountRoot = join(root, 'notion')
  await mkdir(join(mountRoot, 'pages', pageId), { recursive: true })
  await writeFile(join(mountRoot, 'pages', pageId, 'content.md'), content)
  return {
    root,
    manifest: {
      version: 1,
      mountRoot,
      statePath: join(root, 'state.json'),
      tasks: [{ page: pageId, ...task }],
    },
  }
}

function bootstrap(target: NotionIntakeManifest['tasks'][number]['bootstrap'] extends infer T
  ? T extends { targets: infer Targets } ? Targets extends unknown[] ? Targets[number] : never : never
  : never,
): NonNullable<NotionIntakeManifest['tasks'][number]['bootstrap']> {
  return {
    authorizedPageId: pageId,
    reason: 'operator supplied exact page-to-project mapping',
    status: 'ready',
    title: 'Resume the checkpoint',
    recipe: 'team',
    summary: 'Resume from the verified checkpoint and preserve its gates.',
    targets: [target],
  }
}

function fakeGithub(input: { visibility: 'public' | 'private' | 'internal' }): GithubIssuePublisher {
  return {
    repositoryVisibility: vi.fn(async () => input.visibility),
    missingLabels: vi.fn(async () => []),
    findBySource: vi.fn(async () => undefined),
    createIssue: vi.fn(async () => ({ number: 42, url: 'https://github.test/issues/42' })),
  }
}
