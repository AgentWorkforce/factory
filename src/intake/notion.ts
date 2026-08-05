import { createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, isAbsolute, join, resolve } from 'node:path'

import { z } from 'zod'

const recipeSchema = z.enum(['single', 'workflow', 'team'])

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

const bootstrapSchema = z.object({
  authorizedPageId: z.string().trim().min(1),
  reason: z.string().trim().min(1),
  status: z.literal('ready'),
  title: z.string().trim().min(1),
  recipe: recipeSchema,
  summary: z.string().trim().min(1),
  targets: z.array(targetSchema).min(1),
}).strict()

const manifestSchema = z.object({
  version: z.literal(1),
  mountRoot: z.string().trim().min(1).default('.integrations/notion'),
  statePath: z.string().trim().min(1).default('.factory/notion-intake-state.json'),
  tasks: z.array(z.object({
    page: z.string().trim().min(1),
    bootstrap: bootstrapSchema.optional(),
  }).strict()).min(1),
}).strict()

export type NotionRecipe = z.infer<typeof recipeSchema>
export type NotionIntakeTarget = z.infer<typeof targetSchema>
export type NotionIntakeManifest = z.infer<typeof manifestSchema>

export interface NormalizedNotionTask {
  pageId: string
  sourceKey: string
  sourcePath: string
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

export interface GithubIssuePublisher {
  repositoryVisibility(repo: string): Promise<'public' | 'private' | 'internal'>
  missingLabels(repo: string, labels: readonly string[]): Promise<string[]>
  findBySource(repo: string, sourceKey: string): Promise<ExistingGithubIssue | undefined>
  createIssue(input: {
    repo: string
    title: string
    body: string
    labels: readonly string[]
  }): Promise<{ number: number; url: string }>
}

export interface WorkspaceTaskDispatcher {
  dispatch(input: {
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

type IntakeState = {
  version: 1
  receipts: Record<string, { digest: string; agent: string; node?: string; dispatchedAt: string }>
}

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

export async function runNotionIntake(input: {
  manifest: NotionIntakeManifest
  dispatch: boolean
  github?: GithubIssuePublisher
  workspace?: WorkspaceTaskDispatcher
  now?: () => Date
}): Promise<NotionIntakeReport> {
  const tasks = await normalizeNotionManifest(input.manifest)
  const state = await readIntakeState(input.manifest.statePath)
  const results: NotionIntakeResult[] = []

  for (const task of tasks) {
    try {
      if ('repo' in task.target) {
        results.push(await publishRepoTask(task, input))
        continue
      }
      const result = await dispatchWorkspaceTask(task, input, state)
      results.push(result)
      if (input.dispatch && result.status === 'dispatched') {
        await writeIntakeState(input.manifest.statePath, state)
      }
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

  if (input.dispatch) {
    await writeIntakeState(input.manifest.statePath, state)
  }
  return {
    ok: results.every((result) => result.status !== 'blocked'),
    dispatch: input.dispatch,
    results,
  }
}

export async function normalizeNotionManifest(manifest: NotionIntakeManifest): Promise<NormalizedNotionTask[]> {
  const normalized: NormalizedNotionTask[] = []
  const seen = new Set<string>()

  for (const task of manifest.tasks) {
    const pageId = normalizeNotionPageId(task.page)
    const sourcePath = join(manifest.mountRoot, 'pages', pageId, 'content.md')
    const content = await readFile(sourcePath, 'utf8')
    const spec = task.bootstrap
      ? normalizedBootstrapSpec(task.bootstrap, pageId)
      : parseChiefSpecHeader(content)
    const digest = createHash('sha256').update(content).digest('hex')

    for (const target of spec.targets) {
      const targetKey = 'repo' in target ? `repo:${target.repo.toLowerCase()}` : `workspace:${resolve(target.projectPath)}`
      const sourceKey = `notion:${pageId}:${targetKey}`
      if (seen.has(sourceKey)) throw new Error(`duplicate Notion intake destination: ${sourceKey}`)
      seen.add(sourceKey)
      normalized.push({
        pageId,
        sourceKey,
        sourcePath,
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

export function normalizeNotionPageId(value: string): string {
  const lowered = value.toLowerCase()
  const hyphenated = /([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})(?![0-9a-f])/u.exec(lowered)
  const compact = hyphenated?.slice(1).join('') ?? lowered.match(/[0-9a-f]{32}(?![0-9a-f])/u)?.[0]
  if (!compact) throw new Error(`Notion page reference does not contain a 32-character page id: ${value}`)
  return `${compact.slice(0, 8)}-${compact.slice(8, 12)}-${compact.slice(12, 16)}-${compact.slice(16, 20)}-${compact.slice(20)}`
}

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
  const recipe = recipeSchema.parse(requiredField(fields, 'recipe').toLowerCase())
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

export class GhCliIssuePublisher implements GithubIssuePublisher {
  async repositoryVisibility(repo: string): Promise<'public' | 'private' | 'internal'> {
    const output = (await runGh(['repo', 'view', repo, '--json', 'visibility', '--jq', '.visibility'])).trim().toLowerCase()
    if (output !== 'public' && output !== 'private' && output !== 'internal') {
      throw new Error(`GitHub returned unknown visibility for ${repo}: ${output || '(empty)'}`)
    }
    return output
  }

  async missingLabels(repo: string, labels: readonly string[]): Promise<string[]> {
    const output = await runGh(['label', 'list', '--repo', repo, '--limit', '1000', '--json', 'name', '--jq', '.[].name'])
    const available = new Set(output.split('\n').map((label) => label.trim()).filter(Boolean))
    return labels.filter((label) => !available.has(label))
  }

  async findBySource(repo: string, sourceKey: string): Promise<ExistingGithubIssue | undefined> {
    const output = await runGh([
      'issue', 'list', '--repo', repo, '--state', 'all', '--limit', '1000',
      '--json', 'number,url,body',
    ])
    const issues = z.array(z.object({ number: z.number().int().positive(), url: z.string().url(), body: z.string() })).parse(JSON.parse(output))
    const marker = sourceMarker(sourceKey)
    return issues.find((issue) => issue.body.includes(marker))
  }

  async createIssue(input: { repo: string; title: string; body: string; labels: readonly string[] }): Promise<{ number: number; url: string }> {
    const args = ['issue', 'create', '--repo', input.repo, '--title', input.title, '--body-file', '-']
    for (const label of input.labels) args.push('--label', label)
    const url = (await runGh(args, input.body)).trim()
    const number = Number(/\/issues\/(\d+)\/?$/u.exec(url)?.[1])
    if (!Number.isInteger(number) || number <= 0) throw new Error(`GitHub issue create returned an unexpected URL: ${url}`)
    return { number, url }
  }
}

async function publishRepoTask(
  task: NormalizedNotionTask,
  input: Parameters<typeof runNotionIntake>[0],
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

  const existing = await input.github.findBySource(target.repo, task.sourceKey)
  if (existing) {
    const currentDigest = digestFromBody(existing.body)
    if (!currentDigest) {
      return { ...base, status: 'blocked', issue: existing, reason: 'existing lifecycle issue is missing its source digest' }
    }
    if (currentDigest !== task.digest) {
      return { ...base, status: 'blocked', issue: existing, reason: 'mounted spec changed after the lifecycle issue was created' }
    }
    return { ...base, status: 'already-dispatched', issue: existing }
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
  const issue = await input.github.createIssue({
    repo: target.repo,
    title: task.title,
    labels,
    body: renderIssueBody(task, visibility === 'public' ? target.publicSummary! : task.summary),
  })
  return { ...base, status: 'dispatched', issue }
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
    if (receipt.digest !== task.digest) {
      return { ...base, status: 'blocked', agent: receipt.agent, node: receipt.node, reason: 'mounted spec changed after workspace dispatch' }
    }
    return { ...base, status: 'already-dispatched', agent: receipt.agent, node: receipt.node }
  }
  if (!input.workspace) return { ...base, status: 'blocked', reason: 'workspace task dispatcher is not configured' }

  const suffix = createHash('sha256').update(task.sourceKey).digest('hex').slice(0, 8)
  const name = `notion-${task.pageId.slice(-8)}-${suffix}`
  const result = await input.workspace.dispatch({
    name,
    invocationId: `factory:${task.sourceKey}:${task.digest}`,
    node: target.node,
    projectPath: target.projectPath,
    title: task.title,
    task: renderWorkspaceTask(task),
  })
  state.receipts[task.sourceKey] = {
    digest: task.digest,
    agent: result.agent,
    ...(result.node ? { node: result.node } : {}),
    dispatchedAt: (input.now?.() ?? new Date()).toISOString(),
  }
  return { ...base, status: 'dispatched', agent: result.agent, node: result.node }
}

function normalizedBootstrapSpec(bootstrap: z.infer<typeof bootstrapSchema>, pageId: string) {
  const authorizedPageId = normalizeNotionPageId(bootstrap.authorizedPageId)
  if (authorizedPageId !== pageId) {
    throw new Error(`bootstrap authorization ${authorizedPageId} does not match mounted page ${pageId}`)
  }
  return bootstrap
}

function renderIssueBody(task: NormalizedNotionTask, summary: string): string {
  const mountedPath = `.integrations/notion/pages/${task.pageId}/content.md`
  return [
    '## Factory intake',
    '',
    summary,
    '',
    'The complete authorized spec is available to workers through the read-only Relayfile mount:',
    `\`${mountedPath}\``,
    '',
    'Treat the mounted page as the execution contract. Preserve every safety gate in it. Do not write back to Notion.',
    '',
    `Source identity: \`notion:${task.pageId}\``,
    `Source digest: \`${task.digest}\``,
    sourceMarker(task.sourceKey),
  ].join('\n')
}

function renderWorkspaceTask(task: NormalizedNotionTask): string {
  return [
    task.title,
    '',
    task.summary,
    '',
    `Read the full execution contract from the authorized read-only Notion mount at .integrations/notion/pages/${task.pageId}/content.md.`,
    'Preserve every safety gate in that page. Do not write back to Notion.',
    `Factory source: ${task.sourceKey}`,
    `Source digest: ${task.digest}`,
  ].join('\n')
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
    return z.object({
      version: z.literal(1),
      receipts: z.record(z.string(), z.object({
        digest: z.string().regex(/^[0-9a-f]{64}$/u),
        agent: z.string().min(1),
        node: z.string().min(1).optional(),
        dispatchedAt: z.string().datetime(),
      })),
    }).parse(JSON.parse(await readFile(path, 'utf8')))
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === 'ENOENT') return { version: 1, receipts: {} }
    throw error
  }
}

async function writeIntakeState(path: string, state: IntakeState): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const temporaryPath = `${path}.${process.pid}.${Date.now()}.tmp`
  await writeFile(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 })
  await rename(temporaryPath, path)
}

async function runGh(args: string[], input?: string): Promise<string> {
  return await new Promise((resolvePromise, reject) => {
    const child = spawn('gh', args, { stdio: ['pipe', 'pipe', 'pipe'] })
    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    let size = 0
    let settled = false
    const fail = (error: Error) => {
      if (settled) return
      settled = true
      reject(error)
    }
    child.stdout.on('data', (chunk: Buffer) => {
      size += chunk.length
      if (size <= 1024 * 1024) stdout.push(chunk)
    })
    child.stderr.on('data', (chunk: Buffer) => {
      size += chunk.length
      if (size <= 1024 * 1024) stderr.push(chunk)
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
      if (size > 1024 * 1024) {
        fail(new Error('gh output exceeded 1 MiB'))
        return
      }
      settled = true
      resolvePromise(Buffer.concat(stdout).toString('utf8'))
    })
    child.stdin.end(input)
  })
}
