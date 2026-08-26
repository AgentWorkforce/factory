import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  loadNotionIntakeManifest,
  normalizeNotionManifest,
  normalizeNotionPageId,
  parseChiefSpecHeader,
  runNotionIntake,
  type GithubIssuePublisher,
  type NotionIntakeClaimStore,
  type NotionIntakeManifest,
  type NotionIntakeTarget,
  type NotionContractPublisher,
  type WorkspaceTaskDispatcher,
} from './notion'

const pageId = '3b36800c-1c90-801d-b1cf-c8f2e1cff7cf'
const roots: string[] = []
const durableClaims = new Map<string, { sourceKey: string; digest: string; claimedAt: string }>()
const claims: NotionIntakeClaimStore = {
  get: vi.fn(async (sourceKey) => durableClaims.get(sourceKey)),
  findBySourcePrefix: vi.fn(async (sourceKeyPrefix) => [...durableClaims.values()]
    .filter((claim) => claim.sourceKey.startsWith(sourceKeyPrefix))),
  claim: vi.fn(async (claim) => {
    const existing = durableClaims.get(claim.sourceKey)
    if (existing) return { status: 'existing' as const, claim: existing }
    durableClaims.set(claim.sourceKey, claim)
    return { status: 'claimed' as const, claim }
  }),
}

afterEach(async () => {
  durableClaims.clear()
  vi.mocked(claims.get).mockClear()
  vi.mocked(claims.findBySourcePrefix).mockClear()
  vi.mocked(claims.claim).mockClear()
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

describe('Notion spec intake', () => {
  it('normalizes compact app URLs into canonical Notion page ids', () => {
    expect(normalizeNotionPageId(
      'https://app.notion.com/p/Gmail-3b36800c1c90801db1cfc8f2e1cff7cf?source=copy_link',
    )).toBe(pageId)
    expect(normalizeNotionPageId(pageId)).toBe(pageId)
    expect(() => normalizeNotionPageId(`f${pageId.replaceAll('-', '')}`)).toThrow(
      'does not contain a 32-character page id',
    )
  })

  it('defaults an omitted worker mount transport to local when loading existing manifests', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-notion-manifest-'))
    roots.push(root)
    const manifestPath = join(root, 'notion.json')
    await writeFile(manifestPath, JSON.stringify({
      version: 1,
      tasks: [{ page: pageId }],
    }))

    const manifest = await loadNotionIntakeManifest(manifestPath)

    expect(manifest.workerMountTransport).toEqual({ kind: 'local' })
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
      workUnitKey: `notion:${pageId}`,
      bootstrap: true,
      sourceKey: `notion:${pageId}:repo:agentworkforce/cloud`,
      target: { repo: 'AgentWorkforce/cloud' },
    })

    const originalDigest = tasks[0]!.digest
    manifest.tasks[0]!.bootstrap!.summary = 'Operator corrected the authorized summary.'
    const corrected = await normalizeNotionManifest(manifest)
    expect(corrected[0]!.digest).not.toBe(originalDigest)

    manifest.tasks[0]!.bootstrap!.authorizedPageId = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    await expect(normalizeNotionManifest(manifest)).rejects.toThrow('does not match mounted page')
  })

  it('never publishes mounted content to a public repository without a reviewed public summary', async () => {
    const { root, manifest } = await fixtureManifest('secret body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/relay', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'public' })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

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
    manifest.workerMountRoot = 'specs/notion'
    const github = fakeGithub({ visibility: 'public' })

    const first = await runNotionIntake({ manifest, dispatch: true, claims, github })
    const body = vi.mocked(github.createIssue).mock.calls[0]![0].body
    expect(first.results[0]).toMatchObject({ status: 'dispatched', issue: { number: 42 } })
    expect(body).toContain('Resolve the already-scoped fleet reliability follow-ups.')
    expect(body).toContain(`specs/notion/pages/${pageId}/content.md`)
    expect(body).toContain("SHA-256 hash the mounted file's UTF-8 bytes")
    expect(body).not.toContain('private mounted implementation detail')
    expect(vi.mocked(github.createIssue).mock.calls[0]![0].labels).toEqual([
      'factory-ready',
      'agent:team',
      'relay',
    ])
    expect(vi.mocked(github.createIssue).mock.calls[0]![0].title).toBe('[factory] Resume the checkpoint')
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:repo:agentworkforce/relay`]).toMatchObject({
      kind: 'github',
      issue: { number: 42, url: 'https://github.test/issues/42' },
    })

    vi.mocked(github.findBySource).mockResolvedValue({ number: 42, url: 'https://github.test/issues/42', body })
    const second = await runNotionIntake({ manifest, dispatch: true, claims, github })
    expect(second.results[0]).toMatchObject({ status: 'already-dispatched', issue: { number: 42 } })
    expect(github.createIssue).toHaveBeenCalledTimes(1)
  })

  it('refuses an app-identity intake WITHOUT consuming the exactly-once delivery claim', async () => {
    // A refusal raised from createIssue would land after claimNotionDelivery,
    // burning the claim: the operator's retry under a permitted identity would
    // then hit `durable Notion claim already exists` forever. The policy check
    // must happen before anything durable is reserved.
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)

    const refusing = fakeGithub({ visibility: 'private' })
    refusing.assertWritable = () => {
      throw new Error('GitHub identity "app" refuses creating or editing Notion intake lifecycle issues')
    }

    const blocked = await runNotionIntake({ manifest, dispatch: true, claims, github: refusing })

    expect(blocked.ok).toBe(false)
    expect(blocked.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('GitHub identity "app"'),
    })
    // Nothing durable and nothing remote was touched.
    expect(vi.mocked(claims.claim)).not.toHaveBeenCalled()
    expect(durableClaims.size).toBe(0)
    expect(refusing.createIssue).not.toHaveBeenCalled()
    // Reads carry no authorship and stay available: the refusal is raised at
    // the mutation, not at the top of the task.
    expect(refusing.repositoryVisibility).toHaveBeenCalled()

    // MUST NOT FIRE: the operator switches to a permitted identity and the
    // retry succeeds, proving the aborted run left no wedge behind.
    const permitted = fakeGithub({ visibility: 'private' })
    const retried = await runNotionIntake({ manifest, dispatch: true, claims, github: permitted })

    expect(retried.ok).toBe(true)
    expect(permitted.createIssue).toHaveBeenCalledTimes(1)
  })

  it('still reconciles an already-dispatched task under an app identity, because it writes nothing', async () => {
    // A blanket refusal at the top of publishRepoTask would break read-only
    // reconciliation for every app-configured host. Only mutations refuse.
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)

    const permitted = fakeGithub({ visibility: 'private' })
    const first = await runNotionIntake({ manifest, dispatch: true, claims, github: permitted })
    expect(first.ok).toBe(true)
    const created = await vi.mocked(permitted.createIssue).mock.results[0]!.value as { number: number; url: string }

    const refusing = fakeGithub({ visibility: 'private' })
    refusing.assertWritable = () => { throw new Error('GitHub identity "app" refuses') }
    refusing.findBySource = vi.fn(async () => ({
      ...created,
      body: vi.mocked(permitted.createIssue).mock.calls[0]![0].body,
    }))

    const reconciled = await runNotionIntake({ manifest, dispatch: true, claims, github: refusing })

    expect(reconciled.ok).toBe(true)
    expect(reconciled.results[0]).toMatchObject({ status: 'already-dispatched' })
    expect(refusing.updateIssue).not.toHaveBeenCalled()
  })

  it('preserves an explicit Factory title prefix without duplicating it', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: {
        ...bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
        title: '[factory] Resume the checkpoint',
      },
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })

    await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(vi.mocked(github.createIssue).mock.calls[0]![0].title).toBe('[factory] Resume the checkpoint')
  })

  it('delivers private mounted bytes through a portable Relay channel without copying them to GitHub', async () => {
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const github = fakeGithub({ visibility: 'private' })
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1', 'message-2'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({ status: 'dispatched' })
    expect(contracts.publish).toHaveBeenCalledWith(expect.objectContaining({
      pageId,
      content: 'private mounted implementation detail',
    }))
    const body = vi.mocked(github.createIssue).mock.calls[0]![0].body
    expect(body).toContain('workspace-private Agent Relay channel')
    expect(body).toContain('factory-notion-e1cff7cf-aabbccddee')
    expect(body).toContain('message-1,message-2')
    expect(body).toContain('chmod the file 0444')
    expect(body).not.toContain('private mounted implementation detail')
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:repo:agentworkforce/cloud`].delivery).toEqual({
      kind: 'relay-channel',
      channel: 'factory-notion-e1cff7cf-aabbccddee',
      messageIds: ['message-1', 'message-2'],
      encoding: 'base64-chunks-v1',
    })
  })

  it('migrates an untouched lifecycle issue to a portable mount without dispatching it again', async () => {
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    await runNotionIntake({ manifest, dispatch: true, claims, github })
    const originalBody = vi.mocked(github.createIssue).mock.calls[0]![0].body
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: originalBody,
    })
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({ status: 'already-dispatched', issue: { number: 42 } })
    expect(github.createIssue).toHaveBeenCalledTimes(1)
    expect(github.updateIssue).toHaveBeenCalledWith(expect.objectContaining({
      repo: 'AgentWorkforce/cloud',
      number: 42,
      body: expect.stringContaining('factory-notion-e1cff7cf-aabbccddee'),
    }))
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:repo:agentworkforce/cloud`].delivery).toEqual({
      kind: 'relay-channel',
      channel: 'factory-notion-e1cff7cf-aabbccddee',
      messageIds: ['message-1'],
      encoding: 'base64-chunks-v1',
    })
  })

  it('reconciles a portable issue marker when the receipt write was interrupted', async () => {
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    await runNotionIntake({ manifest, dispatch: true, claims, github })
    const originalBody = vi.mocked(github.createIssue).mock.calls[0]![0].body
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: originalBody,
    })
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }
    await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })
    const migratedBody = vi.mocked(github.updateIssue).mock.calls[0]![0].body
    const interruptedState = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    delete interruptedState.receipts[`notion:${pageId}:repo:agentworkforce/cloud`].delivery
    await writeFile(manifest.statePath, JSON.stringify(interruptedState))
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: migratedBody,
    })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({ status: 'already-dispatched', issue: { number: 42 } })
    expect(github.updateIssue).toHaveBeenCalledTimes(1)
    const reconciled = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(reconciled.receipts[`notion:${pageId}:repo:agentworkforce/cloud`].delivery.messageIds).toEqual(['message-1'])
  })

  it('refuses portable issue migration before its claim or contract publication while preserving metadata reconciliation', async () => {
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    await runNotionIntake({ manifest, dispatch: true, claims, github })
    const originalBody = vi.mocked(github.createIssue).mock.calls[0]![0].body
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: originalBody,
    })
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }
    durableClaims.clear()
    vi.mocked(claims.claim).mockClear()
    github.assertWritable = () => { throw new Error('GitHub identity "app" refuses') }

    const blocked = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(blocked.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('GitHub identity "app"'),
    })
    expect(claims.claim).not.toHaveBeenCalled()
    expect(durableClaims.size).toBe(0)
    expect(contracts.publish).not.toHaveBeenCalled()
    expect(github.updateIssue).not.toHaveBeenCalled()

    delete github.assertWritable
    const migrated = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })
    expect(migrated.results[0]).toMatchObject({ status: 'already-dispatched' })
    const migratedBody = vi.mocked(github.updateIssue).mock.calls[0]![0].body
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: migratedBody,
    })
    github.assertWritable = () => { throw new Error('GitHub identity "app" refuses') }
    vi.mocked(github.updateIssue).mockClear()

    const reconciled = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(reconciled.results[0]).toMatchObject({ status: 'already-dispatched' })
    expect(github.updateIssue).not.toHaveBeenCalled()
  })

  it('refuses to overwrite a manually edited lifecycle issue during portable mount migration', async () => {
    const { root, manifest } = await fixtureManifest('private mounted implementation detail', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    await runNotionIntake({ manifest, dispatch: true, claims, github })
    const originalBody = vi.mocked(github.createIssue).mock.calls[0]![0].body
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 42,
      url: 'https://github.test/issues/42',
      body: `${originalBody}\noperator note`,
    })
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('refusing to overwrite'),
    })
    expect(contracts.publish).not.toHaveBeenCalled()
    expect(github.updateIssue).not.toHaveBeenCalled()
  })

  it('blocks portable dispatch when no Relay contract publisher is configured', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const github = fakeGithub({ visibility: 'private' })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('requires an Agent Relay contract publisher'),
    })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('blocks a portable publisher response with no contract messages', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const github = fakeGithub({ visibility: 'private' })
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: [],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('at least one message id'),
    })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('blocks portable delivery identifiers that cannot round-trip through an issue marker', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const github = fakeGithub({ visibility: 'private' })
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion:unsafe',
        messageIds: ['message-1,forged'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github, contracts })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: expect.stringContaining('marker-safe ASCII alphabet'),
    })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('blocks a source marker that has neither a durable claim nor a local migration receipt', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    const [task] = await normalizeNotionManifest(manifest)
    vi.mocked(github.findBySource).mockResolvedValue({
      number: 99,
      url: 'https://github.test/issues/99',
      body: `Source digest: \`${task!.digest}\`\n<!-- factory-source:notion:${pageId}:repo:agentworkforce/cloud -->`,
    })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: 'lifecycle issue marker has neither a durable shared claim nor a local migration receipt',
    })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('requires a durable claim acknowledgement before creating a lifecycle issue', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    const unavailableClaims: NotionIntakeClaimStore = {
      get: vi.fn(async () => undefined),
      findBySourcePrefix: vi.fn(async () => []),
      claim: vi.fn(async () => { throw new Error('shared claim write failed') }),
    }

    const report = await runNotionIntake({
      manifest,
      dispatch: true,
      claims: unavailableClaims,
      github,
    })

    expect(report.results[0]).toMatchObject({ status: 'blocked', reason: 'shared claim write failed' })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('prevents two machines with independent local state from publishing the same page twice', async () => {
    const firstFixture = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(firstFixture.root)
    const secondStatePath = join(firstFixture.root, 'second-machine', 'state.json')
    const secondManifest = { ...firstFixture.manifest, statePath: secondStatePath }
    const github = fakeGithub({ visibility: 'private' })
    vi.mocked(github.createIssue).mockImplementation(async () => {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 25))
      return { number: 42, url: 'https://github.test/issues/42' }
    })

    const reports = await Promise.all([
      runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, github }),
      runNotionIntake({ manifest: secondManifest, dispatch: true, claims, github }),
    ])

    expect(github.createIssue).toHaveBeenCalledTimes(1)
    expect(reports.flatMap((report) => report.results).map((result) => result.status).sort()).toEqual([
      'blocked',
      'dispatched',
    ])
    expect(reports.flatMap((report) => report.results)).toContainEqual(expect.objectContaining({
      status: 'blocked',
      reason: expect.stringContaining('durable Notion claim already exists'),
    }))
  })

  it('migrates a legacy destination claim before refusing an edited destination', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'Example/one', labels: [] }),
    })
    roots.push(root)
    const [original] = await normalizeNotionManifest(manifest)
    durableClaims.set(original!.sourceKey, {
      sourceKey: original!.sourceKey,
      digest: original!.digest,
      claimedAt: '2026-08-06T20:00:00.000Z',
    })
    manifest.tasks[0]!.bootstrap!.targets = [{ repo: 'Example/two', labels: [] }]
    const github = fakeGithub({ visibility: 'private' })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report).toMatchObject({
      ok: false,
      results: [{
        status: 'blocked',
        target: { repo: 'Example/two' },
        reason: 'durable Notion claim digest does not match the mounted spec',
      }],
    })
    expect(durableClaims.get(`notion:${pageId}`)).toMatchObject({ digest: original!.digest })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('refuses disagreeing legacy destination claims without writing a canonical claim', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'Example/current', labels: [] }),
    })
    roots.push(root)
    const [task] = await normalizeNotionManifest(manifest)
    const workUnitKey = `notion:${pageId}`
    durableClaims.set(`${workUnitKey}:repo:example/one`, {
      sourceKey: `${workUnitKey}:repo:example/one`,
      digest: task!.digest,
      claimedAt: '2026-08-06T20:00:00.000Z',
    })
    durableClaims.set(`${workUnitKey}:repo:example/two`, {
      sourceKey: `${workUnitKey}:repo:example/two`,
      digest: 'disagreeing-digest',
      claimedAt: '2026-08-06T20:01:00.000Z',
    })
    const github = fakeGithub({ visibility: 'private' })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: 'legacy Notion claims disagree for the provider-native work unit; refusing dispatch',
    })
    expect(durableClaims.has(workUnitKey)).toBe(false)
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('continues refusing disagreeing legacy destination claims on a repeated run', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'Example/current', labels: [] }),
    })
    roots.push(root)
    const [task] = await normalizeNotionManifest(manifest)
    const workUnitKey = `notion:${pageId}`
    durableClaims.set(`${workUnitKey}:repo:example/one`, {
      sourceKey: `${workUnitKey}:repo:example/one`,
      digest: task!.digest,
      claimedAt: '2026-08-06T20:00:00.000Z',
    })
    durableClaims.set(`${workUnitKey}:repo:example/two`, {
      sourceKey: `${workUnitKey}:repo:example/two`,
      digest: 'disagreeing-digest',
      claimedAt: '2026-08-06T20:01:00.000Z',
    })
    const github = fakeGithub({ visibility: 'private' })

    const first = await runNotionIntake({ manifest, dispatch: true, claims, github })
    const second = await runNotionIntake({ manifest, dispatch: true, claims, github })

    for (const report of [first, second]) {
      expect(report.results[0]).toMatchObject({
        status: 'blocked',
        reason: 'legacy Notion claims disagree for the provider-native work unit; refusing dispatch',
      })
    }
    expect(durableClaims.has(workUnitKey)).toBe(false)
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('migrates agreeing legacy destination claims to one canonical claim', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'Example/current', labels: [] }),
    })
    roots.push(root)
    const [task] = await normalizeNotionManifest(manifest)
    const workUnitKey = `notion:${pageId}`
    for (const [repo, claimedAt] of [
      ['one', '2026-08-06T20:00:00.000Z'],
      ['two', '2026-08-06T20:01:00.000Z'],
    ] as const) {
      const sourceKey = `${workUnitKey}:repo:example/${repo}`
      durableClaims.set(sourceKey, { sourceKey, digest: task!.digest, claimedAt })
    }
    const github = fakeGithub({ visibility: 'private' })

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.results[0]).toMatchObject({ status: 'dispatched' })
    expect(durableClaims.get(workUnitKey)).toEqual({
      sourceKey: workUnitKey,
      digest: task!.digest,
      claimedAt: '2026-08-06T20:00:00.000Z',
    })
    expect(github.createIssue).toHaveBeenCalledTimes(1)
  })

  it('serializes overlapping runs and creates one lifecycle issue', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: [] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    let createdBody: string | undefined
    vi.mocked(github.findBySource).mockImplementation(async () => createdBody
      ? { number: 42, url: 'https://github.test/issues/42', body: createdBody }
      : undefined)
    vi.mocked(github.createIssue).mockImplementation(async (input) => {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 25))
      createdBody = input.body
      return { number: 42, url: 'https://github.test/issues/42' }
    })

    const reports = await Promise.all([
      runNotionIntake({ manifest, dispatch: true, claims, github }),
      runNotionIntake({ manifest, dispatch: true, claims, github }),
    ])

    expect(github.createIssue).toHaveBeenCalledTimes(1)
    expect(reports.flatMap((report) => report.results).map((result) => result.status).sort()).toEqual([
      'already-dispatched',
      'dispatched',
    ])
  })

  it('blocks lifecycle publication when required labels are missing', async () => {
    const { root, manifest } = await fixtureManifest('private mounted body', {
      bootstrap: bootstrap({ repo: 'AgentWorkforce/cloud', labels: ['reviewed'] }),
    })
    roots.push(root)
    const github = fakeGithub({ visibility: 'private' })
    vi.mocked(github.missingLabels).mockResolvedValue(['reviewed'])

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: 'missing required GitHub labels: reviewed',
    })
    expect(github.createIssue).not.toHaveBeenCalled()
  })

  it('dispatches exact-path work once and persists a digest-bound receipt', async () => {
    const { root, manifest } = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(root)
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
      redispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'respawned' })),
    }

    const first = await runNotionIntake({
      manifest,
      dispatch: true, claims,
      workspace,
      now: () => new Date('2026-08-05T22:00:00.000Z'),
    })
    expect(first.results[0]).toMatchObject({ status: 'dispatched', agent: 'benchmark-agent', node: 'kjg-laptop' })
    expect(workspace.dispatch).toHaveBeenCalledWith(expect.objectContaining({
      projectPath: '/work/benchmark',
      node: 'kjg-laptop',
      invocationId: expect.stringContaining(`factory:notion:${pageId}:workspace:/work/benchmark`),
      task: expect.stringContaining(join(root, 'notion', 'pages', pageId, 'content.md')),
    }))
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:workspace:/work/benchmark`]).toMatchObject({
      kind: 'workspace',
      agent: 'benchmark-agent',
      dispatchedAt: '2026-08-05T22:00:00.000Z',
    })

    const second = await runNotionIntake({ manifest, dispatch: true, claims, workspace })
    expect(second.results[0]).toMatchObject({ status: 'already-dispatched', agent: 'benchmark-agent' })
    expect(workspace.dispatch).toHaveBeenCalledTimes(1)

    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }
    const migrated = await runNotionIntake({
      manifest,
      dispatch: true, claims,
      workspace,
      contracts,
      now: () => new Date('2026-08-05T23:00:00.000Z'),
    })
    expect(migrated.results[0]).toMatchObject({
      status: 'already-dispatched',
      agent: 'benchmark-agent',
      node: 'kjg-laptop',
    })
    expect(workspace.redispatch).toHaveBeenCalledWith(expect.objectContaining({
      name: 'benchmark-agent',
      node: 'kjg-laptop',
      task: expect.stringContaining('factory-notion-e1cff7cf-aabbccddee'),
    }))
    expect(workspace.dispatch).toHaveBeenCalledTimes(1)
    expect(contracts.publish).toHaveBeenCalledOnce()
    const migratedState = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(migratedState.receipts[`notion:${pageId}:workspace:/work/benchmark`]).toMatchObject({
      kind: 'workspace',
      agent: 'benchmark-agent',
      dispatchedAt: '2026-08-05T23:00:00.000Z',
      delivery: {
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      },
    })

    await writeFile(
      join(manifest.mountRoot, 'pages', pageId, 'content.md'),
      'workspace body changed after dispatch',
    )
    const changed = await runNotionIntake({ manifest, dispatch: true, claims, workspace, contracts })
    expect(changed.results[0]).toMatchObject({
      status: 'blocked',
      agent: 'benchmark-agent',
      reason: 'mounted spec changed after workspace dispatch',
    })
    expect(workspace.dispatch).toHaveBeenCalledTimes(1)
    expect(workspace.redispatch).toHaveBeenCalledTimes(1)
  })

  it('writes the shared claim before spawning exact-path work and aborts when that write fails', async () => {
    const { root, manifest } = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(root)
    const events: string[] = []
    const unavailableClaims: NotionIntakeClaimStore = {
      get: vi.fn(async () => undefined),
      findBySourcePrefix: vi.fn(async () => []),
      claim: vi.fn(async () => {
        events.push('claim')
        throw new Error('durable claim unavailable')
      }),
    }
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => {
        events.push('spawn')
        return { agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' }
      }),
    }

    const report = await runNotionIntake({
      manifest,
      dispatch: true,
      claims: unavailableClaims,
      workspace,
    })

    expect(report.results[0]).toMatchObject({ status: 'blocked', reason: 'durable claim unavailable' })
    expect(events).toEqual(['claim'])
    expect(workspace.dispatch).not.toHaveBeenCalled()
  })

  it('prevents independent machines from spawning the same exact-path task twice', async () => {
    const firstFixture = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(firstFixture.root)
    const secondManifest = {
      ...firstFixture.manifest,
      statePath: join(firstFixture.root, 'second-machine', 'state.json'),
    }
    const workspace: WorkspaceTaskDispatcher = {
      find: vi.fn(async () => undefined),
      dispatch: vi.fn(async () => {
        await new Promise((resolvePromise) => setTimeout(resolvePromise, 25))
        return { agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' }
      }),
    }

    const reports = await Promise.all([
      runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, workspace }),
      runNotionIntake({ manifest: secondManifest, dispatch: true, claims, workspace }),
    ])

    expect(workspace.dispatch).toHaveBeenCalledTimes(1)
    expect(reports.flatMap((report) => report.results).map((result) => result.status).sort()).toEqual([
      'blocked',
      'dispatched',
    ])
    expect(reports.flatMap((report) => report.results)).toContainEqual(expect.objectContaining({
      status: 'blocked',
      reason: 'durable Notion claim already exists but no running workspace agent was found; refusing a second spawn',
    }))
  })

  it('reports a missing workspace lookup capability separately from a missing agent', async () => {
    const firstFixture = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(firstFixture.root)
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
    }
    await runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, workspace })

    const secondManifest = {
      ...firstFixture.manifest,
      statePath: join(firstFixture.root, 'second-machine', 'state.json'),
    }
    const report = await runNotionIntake({ manifest: secondManifest, dispatch: true, claims, workspace })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: 'durable Notion claim already exists but this workspace dispatcher cannot look up running agents; refusing a second spawn',
    })
    expect(workspace.dispatch).toHaveBeenCalledTimes(1)
  })

  it('uses a durable migration claim to prevent two old receipts from redispatching one worker', async () => {
    const firstFixture = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(firstFixture.root)
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
      redispatch: vi.fn(async () => {
        await new Promise((resolvePromise) => setTimeout(resolvePromise, 25))
        return { agent: 'benchmark-agent', node: 'kjg-laptop', status: 'respawned' }
      }),
    }
    await runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, workspace })
    const secondStatePath = join(firstFixture.root, 'second-machine', 'state.json')
    await mkdir(join(firstFixture.root, 'second-machine'), { recursive: true })
    await writeFile(secondStatePath, await readFile(firstFixture.manifest.statePath, 'utf8'))
    firstFixture.manifest.workerMountTransport = { kind: 'relay-channel' }
    const secondManifest = { ...firstFixture.manifest, statePath: secondStatePath }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const reports = await Promise.all([
      runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, workspace, contracts }),
      runNotionIntake({ manifest: secondManifest, dispatch: true, claims, workspace, contracts }),
    ])

    expect(workspace.redispatch).toHaveBeenCalledTimes(1)
    expect(contracts.publish).toHaveBeenCalledTimes(1)
    expect(reports.flatMap((report) => report.results).map((result) => result.status).sort()).toEqual([
      'already-dispatched',
      'blocked',
    ])
    expect(reports.flatMap((report) => report.results)).toContainEqual(expect.objectContaining({
      status: 'blocked',
      reason: 'durable portable-mount migration claim already exists; refusing a second workspace redispatch',
    }))
    const storedStates = await Promise.all([
      firstFixture.manifest.statePath,
      secondStatePath,
    ].map(async (statePath) => JSON.parse(await readFile(statePath, 'utf8'))))
    expect(storedStates
      .map((stored) => stored.receipts[`notion:${pageId}:workspace:/work/benchmark`]?.delivery)
      .filter(Boolean)).toEqual([{
      kind: 'relay-channel',
      channel: 'factory-notion-e1cff7cf-aabbccddee',
      messageIds: ['message-1'],
      encoding: 'base64-chunks-v1',
    }])
  })

  it('does not treat an unresolved portable migration claim as a completed receipt on another machine', async () => {
    const firstFixture = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(firstFixture.root)
    const workspace: WorkspaceTaskDispatcher = {
      find: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop' })),
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
      redispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'respawned' })),
    }
    await runNotionIntake({ manifest: firstFixture.manifest, dispatch: true, claims, workspace })
    const firstState = JSON.parse(await readFile(firstFixture.manifest.statePath, 'utf8'))
    const sourceKey = `notion:${pageId}:workspace:/work/benchmark`
    const primaryClaim = durableClaims.get(sourceKey)
    expect(primaryClaim).toBeDefined()
    durableClaims.set(`${sourceKey}:portable-mount`, {
      ...primaryClaim!,
      sourceKey: `${sourceKey}:portable-mount`,
    })

    const secondStatePath = join(firstFixture.root, 'second-machine', 'state.json')
    firstFixture.manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({
      manifest: { ...firstFixture.manifest, statePath: secondStatePath },
      dispatch: true,
      claims,
      workspace,
      contracts,
    })

    expect(firstState.receipts[sourceKey].delivery).toBeUndefined()
    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      agent: 'benchmark-agent',
      reason: 'durable portable-mount migration claim exists without a local completion receipt; refusing to assume the running worker was refreshed',
    })
    expect(contracts.publish).not.toHaveBeenCalled()
    expect(workspace.redispatch).not.toHaveBeenCalled()
    await expect(readFile(secondStatePath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' })
  })

  it('blocks an exact-path portable migration when the existing worker cannot be refreshed', async () => {
    const { root, manifest } = await fixtureManifest('workspace body', {
      bootstrap: bootstrap({ projectPath: '/work/benchmark', node: 'kjg-laptop' }),
    })
    roots.push(root)
    const workspace: WorkspaceTaskDispatcher = {
      dispatch: vi.fn(async () => ({ agent: 'benchmark-agent', node: 'kjg-laptop', status: 'spawned' })),
    }
    await runNotionIntake({ manifest, dispatch: true, claims, workspace })
    manifest.workerMountTransport = { kind: 'relay-channel' }
    const contracts: NotionContractPublisher = {
      publish: vi.fn(async () => ({
        kind: 'relay-channel',
        channel: 'factory-notion-e1cff7cf-aabbccddee',
        messageIds: ['message-1'],
        encoding: 'base64-chunks-v1',
      })),
    }

    const report = await runNotionIntake({ manifest, dispatch: true, claims, workspace, contracts })

    expect(report.results[0]).toMatchObject({
      status: 'blocked',
      reason: 'portable workspace mount migration requires a workspace redispatcher',
    })
    const stored = JSON.parse(await readFile(manifest.statePath, 'utf8'))
    expect(stored.receipts[`notion:${pageId}:workspace:/work/benchmark`].delivery).toBeUndefined()
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

    const report = await runNotionIntake({ manifest, dispatch: true, claims, github })

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
      workerMountRoot: '.integrations/notion',
      workerMountTransport: { kind: 'local' },
      statePath: join(root, 'state.json'),
      tasks: [{ page: pageId, ...task }],
    },
  }
}

function bootstrap(
  target: NotionIntakeTarget,
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
    updateIssue: vi.fn(async () => undefined),
  }
}
