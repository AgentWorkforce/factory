import { createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, isAbsolute, join, resolve } from 'node:path'

import lockfile from 'proper-lockfile'
import { z } from 'zod'

import { dispatchNotionPageIdentity } from '../dispatch/work-unit-identity'
import { assertLocalGhMutationAllowed, type GithubWriteIdentity } from '../github/gh-identity'

const INTAKE_LOCK_STALE_MS = 60_000

export const notionRecipeSchema = z.enum(['single', 'workflow', 'team'])

const repoTargetSchema = z.object({
  repo: z.string().regex(/^[^/\s]+\/[^/\s]+$/u, 'repo must be owner/name'),
  labels: z.array(z.string().trim().min(1)).default([]),
  publicSummary: z.string().trim().min(1).optional(),
}).strict()

const workspaceTargetSchema = z.object({
  projectPath: z.string().trim().min(1).refine(isAbsolute, 'projectPath must be absolute'),
  node: z.string().trim().min(1).optional(),
}).strict()

const targetSchema = z.union([repoTargetSchema, workspaceTargetSchema])

const workerMountTransportSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('local') }).strict(),
  z.object({ kind: z.literal('relay-channel') }).strict(),
]).default({ kind: 'local' })

const contractMarkerTokenSchema = z.string().trim().min(1).regex(
  /^[A-Za-z0-9._-]+$/u,
  'portable delivery identifiers must use the marker-safe ASCII alphabet',
)

const contractDeliverySchema = z.object({
  kind: z.literal('relay-channel'),
  channel: contractMarkerTokenSchema,
  messageIds: z.array(contractMarkerTokenSchema).min(1, 'portable delivery must include at least one message id'),
  encoding: z.literal('base64-chunks-v1'),
}).strict()

const bootstrapSchema = z.object({
  authorizedPageId: z.string().trim().min(1),
  reason: z.string().trim().min(1),
  status: z.literal('ready'),
  title: z.string().trim().min(1),
  recipe: notionRecipeSchema,
  summary: z.string().trim().min(1),
  targets: z.array(targetSchema).min(1),
}).strict()

export const manifestSchema = z.object({
  version: z.literal(1),
  mountRoot: z.string().trim().min(1).default('.integrations/notion'),
  workerMountRoot: z.string().trim().min(1).default('.integrations/notion'),
  workerMountTransport: workerMountTransportSchema,
  statePath: z.string().trim().min(1).default('.factory/notion-intake-state.json'),
  tasks: z.array(z.object({
    page: z.string().trim().min(1),
    bootstrap: bootstrapSchema.optional(),
  }).strict()).min(1),
}).strict()

export type NotionRecipe = z.infer<typeof notionRecipeSchema>
export type NotionIntakeTarget = z.infer<typeof targetSchema>
export type NotionIntakeManifest = z.infer<typeof manifestSchema>

export interface NormalizedNotionTask {
  pageId: string
  workUnitKey: string
  sourceKey: string
  sourcePath: string
  workerSourcePath: string
  content: string
  authorizationDigestInput?: string
  contentDigest: string
  digest: string
  title: string
  summary: string
  recipe: NotionRecipe
  target: NotionIntakeTarget
  bootstrap: boolean
}

export interface ExistingGithubIssue {
  number: number
  url: string
  body: string
}

/** Invokes the `gh` CLI with shell-free arguments and optional stdin. */
export type GhCommandRunner = (args: string[], input?: string) => Promise<string>

export interface GithubIssuePublisher {
  /**
   * Refuse now if this publisher may not perform GitHub mutations at all.
   *
   * Called before any durable claim is reserved. An identity refusal raised
   * from `createIssue` would arrive after `claimNotionDelivery` has already
   * consumed the exactly-once claim, so the operator's retry under a
   * permitted identity would then be blocked forever by its own aborted run.
   */
  assertWritable?(): void
  repositoryVisibility(repo: string): Promise<'public' | 'private' | 'internal'>
  missingLabels(repo: string, labels: readonly string[]): Promise<string[]>
  findBySource(repo: string, sourceKey: string): Promise<ExistingGithubIssue | undefined>
  createIssue(input: {
    repo: string
    title: string
    body: string
    labels: readonly string[]
  }): Promise<{ number: number; url: string }>
  updateIssue(input: {
    repo: string
    number: number
    body: string
  }): Promise<void>
}

export type NotionContractDelivery = z.infer<typeof contractDeliverySchema>

export interface NotionContractPublisher {
  publish(input: {
    pageId: string
    sourceKey: string
    content: string
    contentDigest: string
  }): Promise<NotionContractDelivery>
  dispose?(): Promise<void>
}

export interface NotionIntakeClaim {
  sourceKey: string
  digest: string
  claimedAt: string
}

export interface NotionIntakeClaimStore {
  get(sourceKey: string): Promise<NotionIntakeClaim | undefined>
  /** Enumerate legacy destination claims so they can be bound to the immutable page authority. */
  findBySourcePrefix(sourceKeyPrefix: string): Promise<NotionIntakeClaim[]>
  claim(input: NotionIntakeClaim): Promise<{
    status: 'claimed' | 'existing'
    claim: NotionIntakeClaim
  }>
  dispose?(): Promise<void>
}

export interface WorkspaceTaskDispatcher {
  find?(name: string): Promise<{ agent: string; node?: string } | undefined>
  dispatch(input: {
    name: string
    invocationId: string
    node?: string
    projectPath: string
    title: string
    task: string
  }): Promise<{ agent: string; node?: string; status?: string }>
  redispatch?(input: {
    name: string
    invocationId: string
    node?: string
    projectPath: string
    title: string
    task: string
  }): Promise<{ agent: string; node?: string; status?: string }>
}

export type NotionIntakeResult = {
  sourceKey: string
  pageId: string
  target: NotionIntakeTarget
  status: 'ready' | 'dispatched' | 'already-dispatched' | 'blocked'
  digest: string
  issue?: { number: number; url: string }
  agent?: string
  node?: string
  reason?: string
}

export interface NotionIntakeReport {
  ok: boolean
  dispatch: boolean
  results: NotionIntakeResult[]
}

type IntakeReceipt = {
  kind: 'github'
  digest: string
  issue: { number: number; url: string }
  delivery?: NotionContractDelivery
  dispatchedAt: string
} | {
  kind: 'workspace'
  digest: string
  agent: string
  node?: string
  delivery?: NotionContractDelivery
  dispatchedAt: string
}

type IntakeState = {
  version: 1
  receipts: Record<string, IntakeReceipt>
}

/** Load and validate an operator-reviewed intake manifest with paths resolved from the manifest directory. */
export async function loadNotionIntakeManifest(path: string): Promise<NotionIntakeManifest> {
  const manifestPath = resolve(path)
  const parsed = manifestSchema.parse(JSON.parse(await readFile(manifestPath, 'utf8')))
  const base = dirname(manifestPath)
  return {
    ...parsed,
    mountRoot: resolve(base, parsed.mountRoot),
    statePath: resolve(base, parsed.statePath),
  }
}

/** Plan or execute every mounted Notion destination and return one durable result per source key. */
export async function runNotionIntake(input: {
  manifest: NotionIntakeManifest
  dispatch: boolean
  github?: GithubIssuePublisher
  workspace?: WorkspaceTaskDispatcher
  contracts?: NotionContractPublisher
  claims?: NotionIntakeClaimStore
  now?: () => Date
}): Promise<NotionIntakeReport> {
  if (!input.dispatch) return await runNotionIntakeUnlocked(input)

  await mkdir(dirname(input.manifest.statePath), { recursive: true, mode: 0o700 })
  const release = await lockfile.lock(input.manifest.statePath, {
    realpath: false,
    stale: INTAKE_LOCK_STALE_MS,
    update: INTAKE_LOCK_STALE_MS / 2,
    retries: { retries: 50, factor: 1.2, minTimeout: 10, maxTimeout: 100, randomize: true },
  })
  try {
    return await runNotionIntakeUnlocked(input)
  } finally {
    await release()
  }
}

async function runNotionIntakeUnlocked(
  input: Parameters<typeof runNotionIntake>[0],
): Promise<NotionIntakeReport> {
  const tasks = await normalizeNotionManifest(input.manifest)
  const state = await readIntakeState(input.manifest.statePath)
  const results: NotionIntakeResult[] = []

  for (const task of tasks) {
    try {
      await assertMountedTaskUnchanged(task)
      const receiptBefore = state.receipts[task.sourceKey]
        ? JSON.stringify(state.receipts[task.sourceKey])
        : undefined
      let result: NotionIntakeResult
      if ('repo' in task.target) {
        result = await publishRepoTask(task, input, state)
      } else {
        result = await dispatchWorkspaceTask(task, input, state)
      }
      const receiptAfter = state.receipts[task.sourceKey]
        ? JSON.stringify(state.receipts[task.sourceKey])
        : undefined
      if (input.dispatch && receiptAfter && receiptAfter !== receiptBefore) {
        await writeIntakeState(input.manifest.statePath, state)
      }
      results.push(result)
    } catch (error) {
      results.push({
        sourceKey: task.sourceKey,
        pageId: task.pageId,
        target: task.target,
        status: 'blocked',
        digest: task.digest,
        reason: error instanceof Error ? error.message : String(error),
      })
    }
  }

  return {
    ok: results.every((result) => result.status !== 'blocked'),
    dispatch: input.dispatch,
    results,
  }
}

/** Read mounted page bodies and normalize strict headers or exact bootstrap mappings into Factory tasks. */
export async function normalizeNotionManifest(manifest: NotionIntakeManifest): Promise<NormalizedNotionTask[]> {
  const normalized: NormalizedNotionTask[] = []
  const seen = new Set<string>()

  for (const task of manifest.tasks) {
    const pageId = normalizeNotionPageId(task.page)
    const workUnitKey = dispatchNotionPageIdentity(pageId)
    const sourcePath = join(manifest.mountRoot, 'pages', pageId, 'content.md')
    const content = await readFile(sourcePath, 'utf8')
    const spec = task.bootstrap
      ? normalizedBootstrapSpec(task.bootstrap, pageId)
      : parseChiefSpecHeader(content)
    const authorizationDigestInput = task.bootstrap ? canonicalJson(spec) : undefined
    const contentDigest = createHash('sha256').update(content).digest('hex')
    const digest = contractDigest(content, authorizationDigestInput)

    for (const target of spec.targets) {
      const targetKey = 'repo' in target ? `repo:${target.repo.toLowerCase()}` : `workspace:${resolve(target.projectPath)}`
      const sourceKey = `notion:${pageId}:${targetKey}`
      if (seen.has(sourceKey)) throw new Error(`duplicate Notion intake destination: ${sourceKey}`)
      seen.add(sourceKey)
      normalized.push({
        pageId,
        workUnitKey,
        sourceKey,
        sourcePath,
        workerSourcePath: 'repo' in target || manifest.workerMountTransport.kind === 'relay-channel'
          ? join(manifest.workerMountRoot, 'pages', pageId, 'content.md')
          : sourcePath,
        content,
        ...(authorizationDigestInput ? { authorizationDigestInput } : {}),
        contentDigest,
        digest,
        title: spec.title,
        summary: spec.summary,
        recipe: spec.recipe,
        target: 'projectPath' in target
          ? { ...target, projectPath: resolve(target.projectPath) }
          : target,
        bootstrap: Boolean(task.bootstrap),
      })
    }
  }
  return normalized
}

/** Extract one boundary-delimited Notion UUID from a bare identifier or application URL. */
export function normalizeNotionPageId(value: string): string {
  const lowered = value.toLowerCase()
  const hyphenated = /(?<![0-9a-f])([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})(?![0-9a-f])/u.exec(lowered)
  const compact = hyphenated?.slice(1).join('') ?? lowered.match(/(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])/u)?.[0]
  if (!compact) throw new Error(`Notion page reference does not contain a 32-character page id: ${value}`)
  return `${compact.slice(0, 8)}-${compact.slice(8, 12)}-${compact.slice(12, 16)}-${compact.slice(16, 20)}-${compact.slice(20)}`
}

/** Parse the fail-closed Chief Spec header at the start of a mounted Notion page. */
export function parseChiefSpecHeader(content: string): {
  title: string
  summary: string
  recipe: NotionRecipe
  targets: NotionIntakeTarget[]
} {
  const lines = content.replaceAll('\r\n', '\n').split('\n')
  if (lines[0]?.trim() !== '# Chief Spec') {
    throw new Error('Notion intake refused: first line must be exactly "# Chief Spec"')
  }
  const fields = new Map<string, string>()
  const allowedFields = new Set(['status', 'title', 'summary', 'recipe', 'repos', 'project-paths', 'node', 'public-summary'])
  for (const line of lines.slice(1)) {
    if (line.trim() === '') break
    const match = /^([A-Za-z][A-Za-z-]*):\s*(.+)$/u.exec(line.trim())
    if (!match) throw new Error(`Notion intake refused malformed Chief Spec header line: ${line}`)
    const key = match[1]!.toLowerCase()
    if (!allowedFields.has(key)) throw new Error(`Notion intake refused unknown Chief Spec field: ${match[1]}`)
    if (fields.has(key)) throw new Error(`Notion intake refused duplicate Chief Spec field: ${match[1]}`)
    fields.set(key, match[2]!.trim())
  }

  if (fields.get('status')?.toLowerCase() !== 'ready') {
    throw new Error('Notion intake refused: Chief Spec Status must be ready')
  }
  const title = requiredField(fields, 'title')
  const summary = requiredField(fields, 'summary')
  const recipe = notionRecipeSchema.parse(requiredField(fields, 'recipe').toLowerCase())
  const repos = splitField(fields.get('repos')).map((repo) => repoTargetSchema.parse({
    repo,
    ...(fields.get('public-summary') ? { publicSummary: fields.get('public-summary') } : {}),
  }))
  const projectPaths = splitField(fields.get('project-paths')).map((projectPath) => workspaceTargetSchema.parse({
    projectPath,
    ...(fields.get('node') ? { node: fields.get('node') } : {}),
  }))
  const targets = [...repos, ...projectPaths]
  if (targets.length === 0) throw new Error('Notion intake refused: Chief Spec requires Repos or Project-Paths')
  return { title, summary, recipe, targets }
}

/** GitHub CLI publisher with shell-free arguments, bounded output, and source-marker reconciliation. */
export class GhCliIssuePublisher implements GithubIssuePublisher {
  readonly #identity: GithubWriteIdentity
  readonly #gh: GhCommandRunner

  /**
   * @param identity the GitHub write identity this publisher may use. Notion
   *   intake is a separate surface from the Factory lifecycle writeback and
   *   still creates and edits issues through the local `gh` CLI, so its
   *   issues are authored by the operator. That is a documented exception
   *   (see README), not a silent fallback: the caller must state the identity
   *   it is choosing, and exact `app` refuses rather than mislabelling the
   *   write, because the connected App surface exposes no issue-create
   *   operation to route it through.
   * @param gh the `gh` invoker. Injectable because every method here mutates
   *   or reads real GitHub: without a seam the only way to exercise this
   *   class is against the live API, which during development of #221
   *   created a junk issue and overwrote a merged PR's body. Tests must pass
   *   a fake; production takes the default.
   */
  constructor(identity: GithubWriteIdentity, gh: GhCommandRunner = runGh) {
    this.#identity = identity
    this.#gh = gh
  }

  assertWritable(): void {
    assertLocalGhMutationAllowed(
      this.#identity,
      'creating or editing Notion intake lifecycle issues',
      'createIssue/updateIssue',
    )
  }

  async repositoryVisibility(repo: string): Promise<'public' | 'private' | 'internal'> {
    const output = (await this.#gh(['repo', 'view', repo, '--json', 'visibility', '--jq', '.visibility'])).trim().toLowerCase()
    if (output !== 'public' && output !== 'private' && output !== 'internal') {
      throw new Error(`GitHub returned unknown visibility for ${repo}: ${output || '(empty)'}`)
    }
    return output
  }

  async missingLabels(repo: string, labels: readonly string[]): Promise<string[]> {
    const output = await this.#gh(['api', '--paginate', `repos/${repo}/labels?per_page=100`, '--jq', '.[].name'])
    const available = new Set(output.split('\n').map((label) => label.trim()).filter(Boolean))
    return labels.filter((label) => !available.has(label))
  }

  async findBySource(repo: string, sourceKey: string): Promise<ExistingGithubIssue | undefined> {
    const output = await this.#gh([
      'issue', 'list', '--repo', repo, '--state', 'all', '--limit', '100',
      '--search', `"factory-source:${sourceKey}" in:body`,
      '--json', 'number,url,body',
    ])
    const issues = z.array(z.object({ number: z.number().int().positive(), url: z.string().url(), body: z.string() })).parse(JSON.parse(output))
    const marker = sourceMarker(sourceKey)
    return issues.find((issue) => issue.body.includes(marker))
  }

  async createIssue(input: { repo: string; title: string; body: string; labels: readonly string[] }): Promise<{ number: number; url: string }> {
    assertLocalGhMutationAllowed(this.#identity, `creating a GitHub issue in ${input.repo}`, 'createIssue')
    const args = ['issue', 'create', '--repo', input.repo, '--title', input.title, '--body-file', '-']
    for (const label of input.labels) args.push('--label', label)
    const url = (await this.#gh(args, input.body)).trim()
    const number = Number(/\/issues\/(\d+)\/?$/u.exec(url)?.[1])
    if (!Number.isInteger(number) || number <= 0) throw new Error(`GitHub issue create returned an unexpected URL: ${url}`)
    return { number, url }
  }

  async updateIssue(input: { repo: string; number: number; body: string }): Promise<void> {
    assertLocalGhMutationAllowed(
      this.#identity,
      `editing the body of GitHub issue ${input.repo}#${input.number}`,
      'updateIssue',
    )
    await this.#gh(['issue', 'edit', String(input.number), '--repo', input.repo, '--body-file', '-'], input.body)
  }
}

async function publishRepoTask(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
  state: IntakeState,
): Promise<NotionIntakeResult> {
  const target = repoTargetSchema.parse(task.target)
  const base: NotionIntakeResult = {
    sourceKey: task.sourceKey,
    pageId: task.pageId,
    target,
    status: 'ready',
    digest: task.digest,
  }
  if (!input.dispatch) return base
  if (!input.github) return { ...base, status: 'blocked', reason: 'GitHub issue publisher is not configured' }

  const receipt = state.receipts[task.sourceKey]
  if (receipt && receipt.kind !== 'github') {
    return { ...base, status: 'blocked', reason: 'intake receipt kind does not match repository destination' }
  }
  if (receipt?.digest !== undefined && receipt.digest !== task.digest) {
    return { ...base, status: 'blocked', issue: receipt.issue, reason: 'mounted spec changed after the lifecycle issue was created' }
  }

  const existing = await input.github.findBySource(target.repo, task.sourceKey)
  if (existing) {
    if (receipt && (existing.number !== receipt.issue.number || existing.url !== receipt.issue.url)) {
      return { ...base, status: 'blocked', issue: existing, reason: 'lifecycle issue does not match the local receipt cache' }
    }
    const currentDigest = digestFromBody(existing.body)
    if (!currentDigest) {
      return { ...base, status: 'blocked', issue: existing, reason: 'existing lifecycle issue is missing its source digest' }
    }
    if (currentDigest !== task.digest) {
      return { ...base, status: 'blocked', issue: existing, reason: 'mounted spec changed after the lifecycle issue was created' }
    }
    await ensureNotionWorkUnitClaim(task, input)
    let claim = await observeNotionDeliveryClaim(task, input)
    if (!claim) {
      if (!receipt) {
        return {
          ...base,
          status: 'blocked',
          issue: existing,
          reason: 'lifecycle issue marker has neither a durable shared claim nor a local migration receipt',
        }
      }
      claim = (await claimNotionDelivery(task, input)).claim
    }
    const bodyDelivery = contractDeliveryFromBody(existing.body)
    if (input.manifest.workerMountTransport.kind === 'local') {
      if (receipt?.delivery || bodyDelivery) {
        return { ...base, status: 'blocked', issue: existing, reason: 'portable Notion delivery cannot be downgraded to a local worker mount' }
      }
      state.receipts[task.sourceKey] = receipt ?? {
        kind: 'github',
        digest: task.digest,
        issue: { number: existing.number, url: existing.url },
        dispatchedAt: claim.claimedAt,
      }
      return { ...base, status: 'already-dispatched', issue: existing }
    }
    const visibility = await input.github.repositoryVisibility(target.repo)
    if (visibility === 'public' && !target.publicSummary) {
      return { ...base, status: 'blocked', issue: existing, reason: 'public repository requires an explicit publicSummary; mounted content was not copied' }
    }
    const summary = visibility === 'public' ? target.publicSummary! : task.summary
    const bodyWasEdited = existing.body !== renderIssueBody(task, summary, bodyDelivery)
    if (bodyWasEdited) {
      return {
        ...base,
        status: 'blocked',
        issue: existing,
        reason: 'existing lifecycle issue body was edited; refusing to overwrite it during portable mount migration',
      }
    }
    if (receipt?.delivery && bodyDelivery && !sameContractDelivery(receipt.delivery, bodyDelivery)) {
      return { ...base, status: 'blocked', issue: existing, reason: 'lifecycle issue portable delivery does not match its local receipt cache' }
    }
    const delivery = await prepareContractDelivery(task, input, receipt?.delivery ?? bodyDelivery)
    if (delivery && !bodyDelivery) {
      // The only mutation on the reconciliation path. Everything above it is
      // a read and stays available under an app identity.
      try {
        input.github.assertWritable?.()
      } catch (error) {
        return { ...base, status: 'blocked', issue: existing, reason: error instanceof Error ? error.message : String(error) }
      }
      await assertMountedTaskUnchanged(task)
      await input.github.updateIssue({
        repo: target.repo,
        number: existing.number,
        body: renderIssueBody(task, summary, delivery),
      })
    }
    state.receipts[task.sourceKey] = {
      kind: 'github',
      digest: task.digest,
      issue: { number: existing.number, url: existing.url },
      ...(delivery ? { delivery } : {}),
      dispatchedAt: receipt?.dispatchedAt ?? claim.claimedAt,
    }
    return { ...base, status: 'already-dispatched', issue: existing }
  }
  if (receipt) {
    return { ...base, status: 'blocked', issue: receipt.issue, reason: 'local lifecycle receipt exists but its issue marker was not found' }
  }

  const visibility = await input.github.repositoryVisibility(target.repo)
  if (visibility === 'public' && !target.publicSummary) {
    return { ...base, status: 'blocked', reason: 'public repository requires an explicit publicSummary; mounted content was not copied' }
  }
  const labels = [...new Set(['factory-ready', `agent:${task.recipe}`, ...target.labels])]
  const missing = await input.github.missingLabels(target.repo, labels)
  if (missing.length > 0) {
    return { ...base, status: 'blocked', reason: `missing required GitHub labels: ${missing.join(', ')}` }
  }
  // A create is now certain, so refuse here if this publisher may not write.
  // Deliberately after the read-only checks above -- reconciliation and
  // already-dispatched tasks need no mutation and must keep working under an
  // app identity -- and deliberately before the first durable claim, so a
  // policy refusal never consumes the exactly-once claim.
  try {
    input.github.assertWritable?.()
  } catch (error) {
    return { ...base, status: 'blocked', reason: error instanceof Error ? error.message : String(error) }
  }
  await ensureNotionWorkUnitClaim(task, input)
  const delivery = await prepareContractDelivery(task, input)
  const claim = await claimNotionDelivery(task, input)
  if (claim.status === 'existing') {
    return {
      ...base,
      status: 'blocked',
      reason: 'durable Notion claim already exists; refusing lifecycle issue creation from this dispatcher',
    }
  }
  await assertMountedTaskUnchanged(task)
  const issue = await input.github.createIssue({
    repo: target.repo,
    title: factoryIssueTitle(task.title),
    labels,
    body: renderIssueBody(task, visibility === 'public' ? target.publicSummary! : task.summary, delivery),
  })
  state.receipts[task.sourceKey] = {
    kind: 'github',
    digest: task.digest,
    issue,
    ...(delivery ? { delivery } : {}),
    dispatchedAt: claim.claim.claimedAt,
  }
  return { ...base, status: 'dispatched', issue }
}

function factoryIssueTitle(title: string): string {
  return title.toLowerCase().startsWith('[factory]') ? title : `[factory] ${title}`
}

async function dispatchWorkspaceTask(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
  state: IntakeState,
): Promise<NotionIntakeResult> {
  const target = workspaceTargetSchema.parse(task.target)
  const base: NotionIntakeResult = {
    sourceKey: task.sourceKey,
    pageId: task.pageId,
    target,
    status: 'ready',
    digest: task.digest,
  }
  if (!input.dispatch) return base
  const receipt = state.receipts[task.sourceKey]
  if (receipt) {
    if (receipt.kind !== 'workspace') {
      return { ...base, status: 'blocked', reason: 'intake receipt kind does not match workspace destination' }
    }
    if (receipt.digest !== task.digest) {
      return { ...base, status: 'blocked', agent: receipt.agent, node: receipt.node, reason: 'mounted spec changed after workspace dispatch' }
    }
    await ensureNotionWorkUnitClaim(task, input)
    await claimNotionDelivery(task, input)
    const needsPortableMigration = input.manifest.workerMountTransport.kind !== 'local' && !receipt.delivery
    if (needsPortableMigration) {
      if (!input.workspace?.redispatch) {
        return {
          ...base,
          status: 'blocked',
          agent: receipt.agent,
          node: receipt.node,
          reason: 'portable workspace mount migration requires a workspace redispatcher',
        }
      }
      const migrationClaim = await claimNotionDelivery(task, input, `${task.sourceKey}:portable-mount`)
      if (migrationClaim.status === 'existing') {
        return {
          ...base,
          status: 'blocked',
          agent: receipt.agent,
          node: receipt.node,
          reason: 'durable portable-mount migration claim already exists; refusing a second workspace redispatch',
        }
      }
    }
    const delivery = await prepareContractDelivery(task, input, receipt.delivery)
    if (delivery && !sameContractDelivery(receipt.delivery, delivery)) {
      if (!input.workspace?.redispatch) {
        return {
          ...base,
          status: 'blocked',
          agent: receipt.agent,
          node: receipt.node,
          reason: 'portable workspace mount migration requires a workspace redispatcher',
        }
      }
      const refreshed = await input.workspace.redispatch({
        name: receipt.agent,
        invocationId: `factory:${task.sourceKey}:${task.digest}:portable-mount`,
        node: receipt.node ?? target.node,
        projectPath: target.projectPath,
        title: task.title,
        task: renderWorkspaceTask(task, delivery),
      })
      state.receipts[task.sourceKey] = {
        ...receipt,
        agent: refreshed.agent,
        ...(refreshed.node ? { node: refreshed.node } : {}),
        delivery,
        dispatchedAt: (input.now?.() ?? new Date()).toISOString(),
      }
    }
    const currentReceipt = state.receipts[task.sourceKey] as Extract<IntakeReceipt, { kind: 'workspace' }>
    return { ...base, status: 'already-dispatched', agent: currentReceipt.agent, node: currentReceipt.node }
  }
  if (!input.workspace) return { ...base, status: 'blocked', reason: 'workspace task dispatcher is not configured' }

  const suffix = createHash('sha256').update(task.sourceKey).digest('hex').slice(0, 8)
  const name = `notion-${task.pageId.slice(-8)}-${suffix}`
  await ensureNotionWorkUnitClaim(task, input)
  const claim = await claimNotionDelivery(task, input)
  if (claim.status === 'existing') {
    if (!input.workspace.find) {
      return {
        ...base,
        status: 'blocked',
        reason: 'durable Notion claim already exists but this workspace dispatcher cannot look up running agents; refusing a second spawn',
      }
    }
    const running = await input.workspace.find(name)
    if (!running) {
      return {
        ...base,
        status: 'blocked',
        reason: 'durable Notion claim already exists but no running workspace agent was found; refusing a second spawn',
      }
    }
    const migrationSourceKey = `${task.sourceKey}:portable-mount`
    if (input.manifest.workerMountTransport.kind !== 'local' &&
      await observeNotionDeliveryClaim(task, input, migrationSourceKey)) {
      return {
        ...base,
        status: 'blocked',
        agent: running.agent,
        node: running.node,
        reason: 'durable portable-mount migration claim exists without a local completion receipt; refusing to assume the running worker was refreshed',
      }
    }
    const delivery = await prepareContractDelivery(task, input)
    state.receipts[task.sourceKey] = {
      kind: 'workspace',
      digest: task.digest,
      agent: running.agent,
      ...(running.node ? { node: running.node } : {}),
      ...(delivery ? { delivery } : {}),
      dispatchedAt: claim.claim.claimedAt,
    }
    return { ...base, status: 'already-dispatched', agent: running.agent, node: running.node }
  }
  const delivery = await prepareContractDelivery(task, input)
  await assertMountedTaskUnchanged(task)
  const result = await input.workspace.dispatch({
    name,
    invocationId: `factory:${task.sourceKey}:${task.digest}`,
    node: target.node,
    projectPath: target.projectPath,
    title: task.title,
    task: renderWorkspaceTask(task, delivery),
  })
  state.receipts[task.sourceKey] = {
    kind: 'workspace',
    digest: task.digest,
    agent: result.agent,
    ...(result.node ? { node: result.node } : {}),
    ...(delivery ? { delivery } : {}),
    dispatchedAt: claim.claim.claimedAt,
  }
  return {
    ...base,
    status: result.status === 'already-running' ? 'already-dispatched' : 'dispatched',
    agent: result.agent,
    node: result.node,
  }
}

async function ensureNotionWorkUnitClaim(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
): Promise<NotionIntakeClaim> {
  if (!input.claims) {
    throw new Error('dispatch requires a durable Agent Relay Notion claim store')
  }

  const existing = await input.claims.get(task.workUnitKey)
  if (existing) {
    assertNotionClaim(existing, task.workUnitKey, task.digest)
    return existing
  }

  const legacyClaims = (await input.claims.findBySourcePrefix(`${task.workUnitKey}:`))
    .sort((left, right) => left.claimedAt.localeCompare(right.claimedAt) ||
      left.sourceKey.localeCompare(right.sourceKey))
  if (legacyClaims.length > 0) {
    const legacyDigests = new Set(legacyClaims.map((claim) => claim.digest))
    if (legacyDigests.size > 1) {
      throw new Error('legacy Notion claims disagree for the provider-native work unit; refusing dispatch')
    }
    const [authoritative] = legacyClaims
    const migrated = await input.claims.claim({
      sourceKey: task.workUnitKey,
      digest: authoritative!.digest,
      claimedAt: authoritative!.claimedAt,
    })
    assertNotionClaim(migrated.claim, task.workUnitKey, authoritative!.digest)
    assertNotionClaim(migrated.claim, task.workUnitKey, task.digest)
    return migrated.claim
  }

  const result = await input.claims.claim({
    sourceKey: task.workUnitKey,
    digest: task.digest,
    claimedAt: (input.now?.() ?? new Date()).toISOString(),
  })
  assertNotionClaim(result.claim, task.workUnitKey, task.digest)
  return result.claim
}

async function observeNotionDeliveryClaim(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
  sourceKey = task.sourceKey,
): Promise<NotionIntakeClaim | undefined> {
  if (!input.claims) {
    throw new Error('dispatch requires a durable Agent Relay Notion claim store')
  }
  const claim = await input.claims.get(sourceKey)
  if (claim?.sourceKey !== undefined && claim.sourceKey !== sourceKey) {
    throw new Error('durable Notion claim does not match the requested source key')
  }
  if (claim?.digest !== undefined && claim.digest !== task.digest) {
    throw new Error('durable Notion claim digest does not match the mounted spec')
  }
  return claim
}

async function claimNotionDelivery(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
  sourceKey = task.sourceKey,
): Promise<{ status: 'claimed' | 'existing'; claim: NotionIntakeClaim }> {
  if (!input.claims) {
    throw new Error('dispatch requires a durable Agent Relay Notion claim store')
  }
  const result = await input.claims.claim({
    sourceKey,
    digest: task.digest,
    claimedAt: (input.now?.() ?? new Date()).toISOString(),
  })
  if (result.claim.sourceKey !== sourceKey) {
    throw new Error('durable Notion claim does not match the requested source key')
  }
  if (result.claim.digest !== task.digest) {
    throw new Error('durable Notion claim digest does not match the mounted spec')
  }
  return result
}

function assertNotionClaim(claim: NotionIntakeClaim, sourceKey: string, digest: string): void {
  if (claim.sourceKey !== sourceKey) {
    throw new Error('durable Notion claim does not match the requested source key')
  }
  if (claim.digest !== digest) {
    throw new Error('durable Notion claim digest does not match the mounted spec')
  }
}

function normalizedBootstrapSpec(bootstrap: z.infer<typeof bootstrapSchema>, pageId: string) {
  const authorizedPageId = normalizeNotionPageId(bootstrap.authorizedPageId)
  if (authorizedPageId !== pageId) {
    throw new Error(`bootstrap authorization ${authorizedPageId} does not match mounted page ${pageId}`)
  }
  return { ...bootstrap, authorizedPageId }
}

function renderIssueBody(
  task: NormalizedNotionTask,
  summary: string,
  delivery?: NotionContractDelivery,
): string {
  return [
    '## Factory intake',
    '',
    summary,
    '',
    ...renderWorkerMountInstructions(task, delivery),
    '',
    'Treat the mounted page as the execution contract. Preserve every safety gate in it. Do not write back to Notion.',
    `Before executing, SHA-256 hash the mounted file's UTF-8 bytes and refuse the task unless it matches \`${task.contentDigest}\`.`,
    '',
    `Source identity: \`notion:${task.pageId}\``,
    `Source digest: \`${task.digest}\``,
    sourceMarker(task.sourceKey),
    ...(delivery ? [contractDeliveryMarker(delivery)] : []),
  ].join('\n')
}

function renderWorkspaceTask(task: NormalizedNotionTask, delivery?: NotionContractDelivery): string {
  return [
    task.title,
    '',
    task.summary,
    '',
    ...renderWorkerMountInstructions(task, delivery),
    `Before executing, SHA-256 hash that file's UTF-8 bytes and refuse the task unless it matches ${task.contentDigest}.`,
    'Preserve every safety gate in that page. Do not write back to Notion.',
    `Factory source: ${task.sourceKey}`,
    `Source digest: ${task.digest}`,
  ].join('\n')
}

async function prepareContractDelivery(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
  existing?: NotionContractDelivery,
): Promise<NotionContractDelivery | undefined> {
  if (input.manifest.workerMountTransport.kind === 'local') return undefined
  if (!input.contracts) {
    throw new Error('relay-channel worker mount transport requires an Agent Relay contract publisher')
  }
  const published = await input.contracts.publish({
    pageId: task.pageId,
    sourceKey: task.sourceKey,
    content: task.content,
    contentDigest: task.contentDigest,
  })
  const parsed = contractDeliverySchema.safeParse(published)
  if (!parsed.success) {
    const details = [...new Set(parsed.error.issues.map((issue) => issue.message))].join('; ')
    throw new Error(`portable Notion contract publisher returned invalid delivery: ${details}`)
  }
  const delivery = parsed.data
  if (existing && !sameContractDelivery(existing, delivery)) {
    throw new Error('portable Notion contract delivery changed after dispatch')
  }
  return delivery
}

function renderWorkerMountInstructions(
  task: NormalizedNotionTask,
  delivery?: NotionContractDelivery,
): string[] {
  if (!delivery) {
    return [
      'The complete authorized spec is available to workers through the read-only Relayfile mount:',
      `\`${task.workerSourcePath}\``,
    ]
  }
  return [
    'The complete authorized spec was snapshotted from the read-only Notion mount into a workspace-private Agent Relay channel:',
    `- channel: \`${delivery.channel}\``,
    `- ordered message ids: \`${delivery.messageIds.join(',')}\``,
    `- encoding: \`${delivery.encoding}\``,
    '',
    `Join that channel, concatenate the base64 payload from those exact messages in order, decode it to \`${task.workerSourcePath}\`, chmod the file 0444, and then apply the SHA-256 gate below. Never copy the contract to GitHub or another public surface.`,
  ]
}

function sameContractDelivery(
  left: NotionContractDelivery | undefined,
  right: NotionContractDelivery,
): boolean {
  return Boolean(left && left.kind === right.kind && left.channel === right.channel &&
    left.encoding === right.encoding && left.messageIds.join('\0') === right.messageIds.join('\0'))
}

function contractDeliveryMarker(delivery: NotionContractDelivery): string {
  return `<!-- factory-notion-contract:${delivery.kind}:${delivery.channel}:${delivery.messageIds.join(',')} -->`
}

function contractDeliveryFromBody(body: string): NotionContractDelivery | undefined {
  const match = /<!-- factory-notion-contract:relay-channel:([A-Za-z0-9._-]+):([A-Za-z0-9._-]+(?:,[A-Za-z0-9._-]+)*) -->/u.exec(body)
  if (!match) return undefined
  const parsed = contractDeliverySchema.safeParse({
    kind: 'relay-channel',
    channel: match[1],
    messageIds: match[2]?.split(','),
    encoding: 'base64-chunks-v1',
  })
  return parsed.success ? parsed.data : undefined
}

function sourceMarker(sourceKey: string): string {
  return `<!-- factory-source:${sourceKey} -->`
}

function digestFromBody(body: string): string | undefined {
  return /Source digest: `([0-9a-f]{64})`/u.exec(body)?.[1]
}

function requiredField(fields: ReadonlyMap<string, string>, name: string): string {
  const value = fields.get(name)
  if (!value) throw new Error(`Notion intake refused: Chief Spec requires ${name}`)
  return value
}

function splitField(value: string | undefined): string[] {
  return value?.split(',').map((entry) => entry.trim()).filter(Boolean) ?? []
}

async function readIntakeState(path: string): Promise<IntakeState> {
  try {
    const common = {
      digest: z.string().regex(/^[0-9a-f]{64}$/u),
      dispatchedAt: z.string().datetime(),
      delivery: contractDeliverySchema.optional(),
    }
    return z.object({
      version: z.literal(1),
      receipts: z.record(z.string(), z.discriminatedUnion('kind', [
        z.object({
          kind: z.literal('github'),
          ...common,
          issue: z.object({ number: z.number().int().positive(), url: z.string().url() }),
        }).strict(),
        z.object({
          kind: z.literal('workspace'),
          ...common,
          agent: z.string().min(1),
          node: z.string().min(1).optional(),
        }).strict(),
      ])),
    }).parse(JSON.parse(await readFile(path, 'utf8')))
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === 'ENOENT') return { version: 1, receipts: {} }
    throw error
  }
}

async function assertMountedTaskUnchanged(task: NormalizedNotionTask): Promise<void> {
  const content = await readFile(task.sourcePath, 'utf8')
  if (contractDigest(content, task.authorizationDigestInput) !== task.digest) {
    throw new Error('mounted spec changed while intake was planning dispatch')
  }
}

function contractDigest(content: string, authorizationDigestInput?: string): string {
  const digest = createHash('sha256').update(content)
  if (authorizationDigestInput) digest.update('\0').update(authorizationDigestInput)
  return digest.digest('hex')
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value && typeof value === 'object') {
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
      .join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

async function writeIntakeState(path: string, state: IntakeState): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const temporaryPath = `${path}.${process.pid}.${Date.now()}.tmp`
  await writeFile(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 })
  await rename(temporaryPath, path)
}

async function runGh(args: string[], input?: string): Promise<string> {
  return await new Promise((resolvePromise, reject) => {
    const child = spawn('gh', args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 30_000,
      killSignal: 'SIGKILL',
    })
    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    let stdoutSize = 0
    let stderrSize = 0
    let settled = false
    const fail = (error: Error) => {
      if (settled) return
      settled = true
      reject(error)
    }
    child.stdout.on('data', (chunk: Buffer) => {
      stdoutSize += chunk.length
      if (stdoutSize <= 1024 * 1024) stdout.push(chunk)
    })
    child.stderr.on('data', (chunk: Buffer) => {
      stderrSize += chunk.length
      if (stderrSize <= 1024 * 1024) stderr.push(chunk)
    })
    child.once('error', fail)
    child.stdin.once('error', (error: NodeJS.ErrnoException) => {
      if (error.code !== 'EPIPE') fail(error)
    })
    child.once('close', (code) => {
      if (settled) return
      if (code !== 0) {
        fail(new Error(`gh ${args.slice(0, 2).join(' ')} failed (${code ?? 'signal'}): ${Buffer.concat(stderr).toString('utf8').trim()}`))
        return
      }
      if (stdoutSize > 1024 * 1024 || stderrSize > 1024 * 1024) {
        fail(new Error(`gh ${stdoutSize > 1024 * 1024 ? 'stdout' : 'stderr'} exceeded 1 MiB`))
        return
      }
      settled = true
      resolvePromise(Buffer.concat(stdout).toString('utf8'))
    })
    child.stdin.end(input)
  })
}
