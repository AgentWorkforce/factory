import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { runFleetCli } from '../cli/fleet'
import {
  manifestSchema,
  runNotionIntake,
  type GithubIssuePublisher,
  type NotionIntakeClaimStore,
  type WorkspaceTaskDispatcher,
} from './notion'
import {
  FACTORY_TASKS_DATA_SOURCE_ID,
  NotionApiFactoryTasksClient,
  generateFactoryTasksManifest,
  type FactoryTasksNotionClient,
  type FactoryTasksNotionPage,
} from './notion-manifest'

const repoPageId = '11111111-1111-4111-8111-111111111111'
const workspacePageId = '22222222-2222-4222-8222-222222222222'
const draftPageId = '33333333-3333-4333-8333-333333333333'
const roots: string[] = []

afterEach(async () => {
  vi.restoreAllMocks()
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('Factory Tasks Notion manifest generator', () => {
  it('maps only ready repo and workspace rows and round-trips idempotently through intake', async () => {
    const bodies = new Map([
      [repoPageId, '# Repository brief\n\nImplement the reviewed repository change.'],
      [workspacePageId, '# Workspace brief\n\nRun the reviewed local benchmark.'],
      [draftPageId, '# Draft brief\n\nThis must not dispatch.'],
    ])
    const client = fakeNotion([
      factoryTaskRow({
        id: workspacePageId,
        title: 'Run local benchmark',
        reason: 'Operator authorized the exact workspace target.',
        projectPath: '/work/benchmarks',
        node: 'benchmark-host',
      }),
      factoryTaskRow({
        id: draftPageId,
        status: 'Draft',
        title: 'Unreviewed task',
        reason: 'This row is not ready.',
        repo: 'AgentWorkforce/factory',
      }),
      factoryTaskRow({
        id: repoPageId,
        title: 'Implement repository change',
        reason: 'Operator authorized the exact repository target.',
        recipe: 'team',
        repo: 'AgentWorkforce/factory',
        publicSummary: 'Implement the reviewed repository change without exposing the private brief.',
        labels: ['intake', 'shared'],
        routes: ['implementer', 'shared'],
      }),
    ], bodies)

    const root = await mkdtemp(join(tmpdir(), 'factory-tasks-manifest-'))
    roots.push(root)
    for (const [pageId, body] of bodies) {
      const pageRoot = join(root, 'mount', 'pages', pageId)
      await mkdir(pageRoot, { recursive: true })
      await writeFile(join(pageRoot, 'content.md'), body)
    }

    const manifest = await generateFactoryTasksManifest({
      client,
      mountRoot: join(root, 'mount'),
      statePath: join(root, 'state.json'),
    })

    expect(manifestSchema.parse(manifest)).toEqual(manifest)
    expect(manifest.tasks).toEqual([
      expect.objectContaining({
        page: repoPageId,
        bootstrap: expect.objectContaining({
          authorizedPageId: repoPageId,
          status: 'ready',
          title: 'Implement repository change',
          recipe: 'team',
          summary: bodies.get(repoPageId),
          targets: [{
            repo: 'AgentWorkforce/factory',
            labels: ['intake', 'shared', 'implementer'],
            publicSummary: 'Implement the reviewed repository change without exposing the private brief.',
          }],
        }),
      }),
      expect.objectContaining({
        page: workspacePageId,
        bootstrap: expect.objectContaining({
          authorizedPageId: workspacePageId,
          title: 'Run local benchmark',
          recipe: 'single',
          summary: bodies.get(workspacePageId),
          targets: [{ projectPath: '/work/benchmarks', node: 'benchmark-host' }],
        }),
      }),
    ])
    expect(client.retrievePageMarkdown).not.toHaveBeenCalledWith(draftPageId)

    const claims = memoryClaims()
    const github = memoryGithub()
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async (task) => ({ agent: task.name, node: task.node, status: 'spawned' })),
    }

    const first = await runNotionIntake({ manifest, dispatch: true, claims, github, workspace })
    const second = await runNotionIntake({ manifest, dispatch: true, claims, github, workspace })

    expect(first).toMatchObject({
      ok: true,
      results: [
        { status: 'dispatched', target: { repo: 'AgentWorkforce/factory' } },
        { status: 'dispatched', target: { projectPath: '/work/benchmarks' } },
      ],
    })
    expect(second).toMatchObject({
      ok: true,
      results: [
        { status: 'already-dispatched', target: { repo: 'AgentWorkforce/factory' } },
        { status: 'already-dispatched', target: { projectPath: '/work/benchmarks' } },
      ],
    })
    expect(github.createIssue).toHaveBeenCalledOnce()
    expect(github.createIssue).toHaveBeenCalledWith(expect.objectContaining({
      labels: ['factory-ready', 'agent:team', 'intake', 'shared', 'implementer'],
      body: expect.stringContaining('Implement the reviewed repository change without exposing the private brief.'),
    }))
    expect(vi.mocked(github.createIssue).mock.calls[0]![0].body).not.toContain(
      'Implement the reviewed repository change.',
    )
    expect(workspace.dispatch).toHaveBeenCalledOnce()
  })

  it('does not create a second issue when a Ready page changes repository', async () => {
    const body = '# Private brief\n\nInternal implementation details.'
    const root = await mkdtemp(join(tmpdir(), 'factory-tasks-identity-'))
    roots.push(root)
    const pageRoot = join(root, 'mount', 'pages', repoPageId)
    await mkdir(pageRoot, { recursive: true })
    await writeFile(join(pageRoot, 'content.md'), body)
    const claims = memoryClaims()
    const github = memoryGithub()

    const firstManifest = await generateFactoryTasksManifest({
      client: fakeNotion([factoryTaskRow({
        id: repoPageId,
        title: 'Stable work unit',
        reason: 'Operator authorized the task.',
        repo: 'Example/one',
        publicSummary: 'Apply the reviewed public change.',
      })], new Map([[repoPageId, body]])),
      mountRoot: join(root, 'mount'),
      statePath: join(root, 'first-state.json'),
    })
    const first = await runNotionIntake({
      manifest: firstManifest,
      dispatch: true,
      claims,
      github,
    })

    const editedManifest = await generateFactoryTasksManifest({
      client: fakeNotion([factoryTaskRow({
        id: repoPageId,
        title: 'Stable work unit',
        reason: 'Operator authorized the task.',
        repo: 'Example/two',
        publicSummary: 'Apply the reviewed public change.',
      })], new Map([[repoPageId, body]])),
      mountRoot: join(root, 'mount'),
      statePath: join(root, 'independent-state.json'),
    })
    const edited = await runNotionIntake({
      manifest: editedManifest,
      dispatch: true,
      claims,
      github,
    })

    expect(first.results).toEqual([expect.objectContaining({ status: 'dispatched' })])
    expect(edited).toMatchObject({
      ok: false,
      results: [{
        status: 'blocked',
        target: { repo: 'Example/two' },
        reason: 'durable Notion claim digest does not match the mounted spec',
      }],
    })
    expect(github.createIssue).toHaveBeenCalledOnce()
    expect(claims.stored.has(`notion:${repoPageId}`)).toBe(true)
    expect(claims.stored.has(`notion:${repoPageId}:repo:example/two`)).toBe(false)
  })

  it('keeps distinct pages separate while one page fans out to distinct targets', async () => {
    const bodies = new Map([
      [repoPageId, 'Private first-page execution contract.'],
      [workspacePageId, 'Private second-page execution contract.'],
    ])
    const root = await mkdtemp(join(tmpdir(), 'factory-tasks-fanout-'))
    roots.push(root)
    for (const [pageId, body] of bodies) {
      const pageRoot = join(root, 'mount', 'pages', pageId)
      await mkdir(pageRoot, { recursive: true })
      await writeFile(join(pageRoot, 'content.md'), body)
    }
    const manifest = await generateFactoryTasksManifest({
      client: fakeNotion([
        factoryTaskRow({
          id: repoPageId,
          title: 'Fan out one page',
          reason: 'Operator authorized both public targets.',
          repo: 'Example/one',
          publicSummary: 'Apply the first reviewed public change.',
        }),
        factoryTaskRow({
          id: workspacePageId,
          title: 'Dispatch a distinct page',
          reason: 'Operator authorized the distinct public target.',
          repo: 'Example/two',
          publicSummary: 'Apply the second reviewed public change.',
        }),
      ], bodies),
      mountRoot: join(root, 'mount'),
      statePath: join(root, 'state.json'),
    })
    manifest.tasks[0]!.bootstrap!.targets.push({
      repo: 'Example/three',
      labels: [],
      publicSummary: 'Apply the additional reviewed public change.',
    })
    const claims = memoryClaims()
    const github = memoryGithub()

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.ok).toBe(true)
    expect(report.results).toHaveLength(3)
    expect(report.results.every((result) => result.status === 'dispatched')).toBe(true)
    expect(github.createIssue).toHaveBeenCalledTimes(3)
    expect([...claims.stored.keys()].filter((key) =>
      key === `notion:${repoPageId}` || key === `notion:${workspacePageId}`,
    ).sort()).toEqual([
      `notion:${repoPageId}`,
      `notion:${workspacePageId}`,
    ])
  })

  it('emits the generated manifest through the Factory CLI without constructing a fleet', async () => {
    const client = fakeNotion([
      factoryTaskRow({
        id: repoPageId,
        title: 'CLI manifest task',
        reason: 'Ready row explicitly authorized by the operator.',
        repo: 'AgentWorkforce/factory',
      }),
    ], new Map([[repoPageId, 'CLI brief']]))
    const output = buffer()

    const code = await runFleetCli([
      'intake',
      'notion',
      'generate',
      '--mount-root',
      '../notion',
      '--worker-mount-transport',
      'relay-channel',
    ], {
      notionFactoryTasks: client,
      createFleet: () => { throw new Error('manifest generation must not construct a fleet') },
      stdout: output,
      stderr: buffer(),
    })

    expect(code).toBe(0)
    expect(JSON.parse(output.text())).toMatchObject({
      version: 1,
      mountRoot: '../notion',
      workerMountTransport: { kind: 'relay-channel' },
      tasks: [{ page: repoPageId, bootstrap: { status: 'ready' } }],
    })
  })

  it('uses the live data-source schema for server filtering and paginates every result', async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(jsonResponse({
        object: 'data_source',
        properties: { Status: { id: 'status', type: 'status', status: {} } },
      }))
      .mockResolvedValueOnce(jsonResponse({
        object: 'list',
        results: [factoryTaskRow({
          id: repoPageId,
          title: 'First task',
          reason: 'First reason',
          repo: 'AgentWorkforce/factory',
        })],
        has_more: true,
        next_cursor: 'next-page',
      }))
      .mockResolvedValueOnce(jsonResponse({
        object: 'list',
        results: [factoryTaskRow({
          id: workspacePageId,
          title: 'Second task',
          reason: 'Second reason',
          projectPath: '/work/factory',
        })],
        has_more: false,
        next_cursor: null,
      }))
    const client = new NotionApiFactoryTasksClient({ token: 'test-token', fetch })

    const rows = await client.queryReadyTasks(`collection://${FACTORY_TASKS_DATA_SOURCE_ID}`)

    expect(rows.map((row) => row.id)).toEqual([repoPageId, workspacePageId])
    const firstQuery = JSON.parse(fetch.mock.calls[1]![1]!.body as string)
    const secondQuery = JSON.parse(fetch.mock.calls[2]![1]!.body as string)
    expect(firstQuery).toMatchObject({
      filter: { property: 'Status', status: { equals: 'Ready for Agent' } },
      result_type: 'page',
      page_size: 100,
    })
    expect(secondQuery.start_cursor).toBe('next-page')
    expect(String(fetch.mock.calls[1]![0])).toContain('filter_properties%5B%5D=Task')
    expect(String(fetch.mock.calls[1]![0])).toContain('filter_properties%5B%5D=Public+Summary')
    expect(fetch.mock.calls.map((call) => call[1]?.method ?? 'GET')).toEqual([
      'GET',
      'POST',
      'POST',
    ])
  })

  it('fails closed when a ready row does not select exactly one destination', async () => {
    const client = fakeNotion([
      factoryTaskRow({
        id: repoPageId,
        title: 'Ambiguous task',
        reason: 'Ambiguous target must not dispatch.',
        repo: 'AgentWorkforce/factory',
        projectPath: '/work/factory',
      }),
    ], new Map([[repoPageId, 'Ambiguous brief']]))

    await expect(generateFactoryTasksManifest({ client })).rejects.toThrow(
      'must set exactly one of Repo or Project Path',
    )
  })

  it('rejects incomplete markdown and markdown returned for another provider page', async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(jsonResponse({
        object: 'page_markdown',
        id: repoPageId,
        markdown: 'partial brief',
        truncated: true,
        unknown_block_ids: [],
      }))
      .mockResolvedValueOnce(jsonResponse({
        object: 'page_markdown',
        id: workspacePageId,
        markdown: 'wrong page brief',
        truncated: false,
        unknown_block_ids: [],
      }))
    const client = new NotionApiFactoryTasksClient({ token: 'test-token', fetch })

    await expect(client.retrievePageMarkdown(repoPageId)).rejects.toThrow('complete execution brief')
    await expect(client.retrievePageMarkdown(repoPageId)).rejects.toThrow('did not match requested page')
  })

  it('rejects duplicate provider rows before authorizing either copy', async () => {
    const row = factoryTaskRow({
      id: repoPageId,
      title: 'Duplicate provider row',
      reason: 'The provider identity must be unique.',
      repo: 'AgentWorkforce/factory',
      publicSummary: 'Apply the reviewed public change.',
    })
    const client = fakeNotion([row, structuredClone(row)], new Map([[repoPageId, 'Private brief']]))

    await expect(generateFactoryTasksManifest({ client })).rejects.toThrow(
      `Notion returned duplicate Factory Tasks row ${repoPageId}`,
    )
    expect(client.retrievePageMarkdown).toHaveBeenCalledOnce()
  })
})

function factoryTaskRow(input: {
  id: string
  status?: string
  title: string
  reason: string
  recipe?: 'single' | 'workflow' | 'team'
  repo?: string
  publicSummary?: string
  labels?: string[]
  routes?: string[]
  projectPath?: string
  node?: string
}): FactoryTasksNotionPage {
  return {
    object: 'page',
    id: input.id,
    properties: {
      Status: selectProperty('status', input.status ?? 'Ready for Agent'),
      Task: richTextProperty('title', input.title),
      Reason: richTextProperty('rich_text', input.reason),
      Recipe: selectProperty('select', input.recipe ?? 'single'),
      Repo: richTextProperty('rich_text', input.repo),
      'Public Summary': richTextProperty('rich_text', input.publicSummary),
      Labels: multiSelectProperty(input.labels ?? []),
      Route: multiSelectProperty(input.routes ?? []),
      'Project Path': richTextProperty('rich_text', input.projectPath),
      Node: richTextProperty('rich_text', input.node),
    },
  }
}

function richTextProperty(type: 'title' | 'rich_text', value?: string): Record<string, unknown> {
  return { type, [type]: value ? [{ plain_text: value }] : [] }
}

function selectProperty(type: 'select' | 'status', value: string): Record<string, unknown> {
  return { type, [type]: { name: value } }
}

function multiSelectProperty(values: string[]): Record<string, unknown> {
  return { type: 'multi_select', multi_select: values.map((name) => ({ name })) }
}

function fakeNotion(
  rows: FactoryTasksNotionPage[],
  bodies: ReadonlyMap<string, string>,
): FactoryTasksNotionClient & {
  queryReadyTasks: ReturnType<typeof vi.fn<FactoryTasksNotionClient['queryReadyTasks']>>
  retrievePageMarkdown: ReturnType<typeof vi.fn<FactoryTasksNotionClient['retrievePageMarkdown']>>
} {
  return {
    queryReadyTasks: vi.fn(async () => rows),
    retrievePageMarkdown: vi.fn(async (pageId) => {
      const body = bodies.get(pageId)
      if (body === undefined) throw new Error(`missing fake body for ${pageId}`)
      return body
    }),
  }
}

function memoryClaims(): NotionIntakeClaimStore & {
  stored: Map<string, { sourceKey: string; digest: string; claimedAt: string }>
} {
  const stored = new Map<string, { sourceKey: string; digest: string; claimedAt: string }>()
  return {
    stored,
    get: vi.fn(async (sourceKey) => stored.get(sourceKey)),
    findBySourcePrefix: vi.fn(async (sourceKeyPrefix) => [...stored.values()]
      .filter((claim) => claim.sourceKey.startsWith(sourceKeyPrefix))),
    claim: vi.fn(async (claim) => {
      const existing = stored.get(claim.sourceKey)
      if (existing) return { status: 'existing' as const, claim: existing }
      stored.set(claim.sourceKey, claim)
      return { status: 'claimed' as const, claim }
    }),
  }
}

function memoryGithub(): GithubIssuePublisher & { createIssue: ReturnType<typeof vi.fn> } {
  const issues = new Map<string, { number: number; url: string; body: string }>()
  return {
    repositoryVisibility: vi.fn(async () => 'public' as const),
    missingLabels: vi.fn(async () => []),
    findBySource: vi.fn(async (_repo, sourceKey) => issues.get(sourceKey)),
    createIssue: vi.fn(async ({ body }) => {
      const sourceKey = /<!-- factory-source:(.+) -->/u.exec(body)?.[1]
      if (!sourceKey) throw new Error('fake issue is missing its source marker')
      const issue = { number: 42, url: 'https://github.test/issues/42', body }
      issues.set(sourceKey, issue)
      return issue
    }),
    updateIssue: vi.fn(async () => undefined),
  }
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function buffer(): Pick<NodeJS.WriteStream, 'write'> & { text(): string } {
  let value = ''
  return {
    write(chunk) {
      value += String(chunk)
      return true
    },
    text: () => value,
  }
}
