import { z } from 'zod'

import {
  manifestSchema,
  normalizeNotionPageId,
  notionRecipeSchema,
  type NotionIntakeManifest,
  type NotionIntakeTarget,
} from './notion'

export const FACTORY_TASKS_DATA_SOURCE_ID = 'a7fb83ad-c667-4003-a1dc-132c6826aac1'
export const NOTION_API_VERSION = '2026-03-11'
export const READY_FOR_AGENT_STATUS = 'Ready for Agent'

const FACTORY_TASK_PROPERTIES = [
  'Status',
  'Task',
  'Recipe',
  'Reason',
  'Repo',
  'Public Summary',
  'Labels',
  'Route',
  'Project Path',
  'Node',
] as const

const notionPageSchema = z.object({
  object: z.literal('page'),
  id: z.string().min(1),
  properties: z.record(z.string(), z.unknown()),
}).passthrough()

const notionQuerySchema = z.object({
  object: z.literal('list'),
  results: z.array(notionPageSchema),
  has_more: z.boolean(),
  next_cursor: z.string().min(1).nullable(),
}).passthrough()

const notionDataSourceSchema = z.object({
  object: z.literal('data_source'),
  properties: z.record(z.string(), z.object({ type: z.string().min(1) }).passthrough()),
}).passthrough()

const notionMarkdownSchema = z.object({
  object: z.literal('page_markdown'),
  id: z.string().min(1),
  markdown: z.string(),
  truncated: z.boolean(),
  unknown_block_ids: z.array(z.string()),
}).passthrough()

export type FactoryTasksNotionPage = z.infer<typeof notionPageSchema>

/** The minimal read-only Notion surface needed by the manifest generator. */
export interface FactoryTasksNotionClient {
  queryReadyTasks(dataSourceId: string): Promise<FactoryTasksNotionPage[]>
  retrievePageMarkdown(pageId: string): Promise<string>
}

export interface NotionApiFactoryTasksClientOptions {
  token: string
  apiBaseUrl?: string
  fetch?: typeof globalThis.fetch
}

/** Queries Factory Tasks through Notion's versioned, read-only data APIs. */
export class NotionApiFactoryTasksClient implements FactoryTasksNotionClient {
  readonly #token: string
  readonly #apiBaseUrl: string
  readonly #fetch: typeof globalThis.fetch

  constructor(options: NotionApiFactoryTasksClientOptions) {
    const token = options.token.trim()
    if (!token) throw new Error('Notion manifest generation requires NOTION_API_KEY')
    this.#token = token
    this.#apiBaseUrl = (options.apiBaseUrl ?? 'https://api.notion.com').replace(/\/$/u, '')
    this.#fetch = options.fetch ?? globalThis.fetch
  }

  async queryReadyTasks(dataSourceId: string): Promise<FactoryTasksNotionPage[]> {
    const id = normalizeDataSourceId(dataSourceId)
    const descriptor = notionDataSourceSchema.parse(
      await this.#request(`/v1/data_sources/${encodeURIComponent(id)}`),
    )
    const statusType = descriptor.properties.Status?.type
    if (statusType !== 'status' && statusType !== 'select') {
      throw new Error(
        `Factory Tasks property Status must be a Notion status or select, received ${statusType ?? 'missing'}`,
      )
    }

    const query = new URLSearchParams()
    for (const property of FACTORY_TASK_PROPERTIES) query.append('filter_properties[]', property)
    const path = `/v1/data_sources/${encodeURIComponent(id)}/query?${query.toString()}`
    const pages: FactoryTasksNotionPage[] = []
    const seenCursors = new Set<string>()
    let startCursor: string | undefined

    do {
      if (startCursor) {
        if (seenCursors.has(startCursor)) {
          throw new Error('Notion Factory Tasks pagination repeated its next cursor')
        }
        seenCursors.add(startCursor)
      }
      const response = notionQuerySchema.parse(await this.#request(path, {
        method: 'POST',
        body: JSON.stringify({
          filter: {
            property: 'Status',
            [statusType]: { equals: READY_FOR_AGENT_STATUS },
          },
          sorts: [{ timestamp: 'created_time', direction: 'ascending' }],
          result_type: 'page',
          page_size: 100,
          ...(startCursor ? { start_cursor: startCursor } : {}),
        }),
      }))
      pages.push(...response.results)
      startCursor = response.has_more ? response.next_cursor ?? undefined : undefined
      if (response.has_more && !startCursor) {
        throw new Error('Notion Factory Tasks pagination did not return its next cursor')
      }
    } while (startCursor)

    return pages
  }

  async retrievePageMarkdown(pageId: string): Promise<string> {
    const id = normalizeNotionPageId(pageId)
    const response = notionMarkdownSchema.parse(
      await this.#request(`/v1/pages/${encodeURIComponent(id)}/markdown`),
    )
    if (normalizeNotionPageId(response.id) !== id) {
      throw new Error(`Notion markdown response did not match requested page ${id}`)
    }
    if (response.truncated || response.unknown_block_ids.length > 0) {
      throw new Error(`Notion page ${id} could not be read as a complete execution brief`)
    }
    return response.markdown
  }

  async #request(path: string, init: RequestInit = {}): Promise<unknown> {
    const response = await this.#fetch(`${this.#apiBaseUrl}${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${this.#token}`,
        'Notion-Version': NOTION_API_VERSION,
        ...(init.body ? { 'Content-Type': 'application/json' } : {}),
      },
      signal: AbortSignal.timeout(30_000),
    })
    if (!response.ok) {
      const details = (await response.text()).trim().slice(0, 2_000)
      throw new Error(
        `Notion API ${init.method ?? 'GET'} ${path.split('?')[0]} failed (${response.status})${details ? `: ${details}` : ''}`,
      )
    }
    return await response.json()
  }
}

export interface GenerateFactoryTasksManifestOptions {
  client: FactoryTasksNotionClient
  dataSourceId?: string
  mountRoot?: string
  workerMountRoot?: string
  workerMountTransport?: 'local' | 'relay-channel'
  statePath?: string
}

/** Convert every currently-ready Factory Tasks row into an intake bootstrap authorization. */
export async function generateFactoryTasksManifest(
  options: GenerateFactoryTasksManifestOptions,
): Promise<NotionIntakeManifest> {
  const rows = await options.client.queryReadyTasks(
    options.dataSourceId ?? FACTORY_TASKS_DATA_SOURCE_ID,
  )
  const readyRows = rows
    .filter((row) => propertyText(row, 'Status', false) === READY_FOR_AGENT_STATUS)
    .map((row) => ({ row, pageId: normalizeNotionPageId(row.id) }))
    .sort((left, right) => left.pageId.localeCompare(right.pageId))

  const seen = new Set<string>()
  const tasks: NotionIntakeManifest['tasks'] = []
  for (const { row, pageId } of readyRows) {
    if (seen.has(pageId)) throw new Error(`Notion returned duplicate Factory Tasks row ${pageId}`)
    seen.add(pageId)

    const repo = propertyText(row, 'Repo', false)
    const projectPath = propertyText(row, 'Project Path', false)
    if (Boolean(repo) === Boolean(projectPath)) {
      throw new Error(
        `Factory Tasks row ${pageId} must set exactly one of Repo or Project Path`,
      )
    }

    let target: NotionIntakeTarget
    if (repo) {
      const publicSummary = propertyText(row, 'Public Summary', false)
      const labels = unique([
        ...propertyList(row, 'Labels'),
        ...propertyList(row, 'Route'),
      ])
      target = { repo, labels, ...(publicSummary ? { publicSummary } : {}) }
    } else {
      const node = propertyText(row, 'Node', false)
      target = { projectPath: projectPath!, ...(node ? { node } : {}) }
    }

    const summary = (await options.client.retrievePageMarkdown(pageId)).trim()
    tasks.push({
      page: pageId,
      bootstrap: {
        authorizedPageId: pageId,
        reason: propertyText(row, 'Reason'),
        status: 'ready',
        title: propertyText(row, 'Task'),
        recipe: notionRecipeSchema.parse(propertyText(row, 'Recipe').toLowerCase()),
        summary,
        targets: [target],
      },
    })
  }

  if (tasks.length === 0) {
    throw new Error(`Factory Tasks has no rows with Status = ${READY_FOR_AGENT_STATUS}`)
  }

  return manifestSchema.parse({
    version: 1,
    ...(options.mountRoot ? { mountRoot: options.mountRoot } : {}),
    ...(options.workerMountRoot ? { workerMountRoot: options.workerMountRoot } : {}),
    ...(options.workerMountTransport
      ? { workerMountTransport: { kind: options.workerMountTransport } }
      : {}),
    ...(options.statePath ? { statePath: options.statePath } : {}),
    tasks,
  })
}

function normalizeDataSourceId(value: string): string {
  const id = value.trim().replace(/^collection:\/\//u, '')
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(id)) {
    throw new Error(`invalid Notion data source id: ${value}`)
  }
  return id.toLowerCase()
}

function propertyText(row: FactoryTasksNotionPage, name: string): string
function propertyText(row: FactoryTasksNotionPage, name: string, required: true): string
function propertyText(row: FactoryTasksNotionPage, name: string, required: false): string | undefined
function propertyText(
  row: FactoryTasksNotionPage,
  name: string,
  required = true,
): string | undefined {
  const property = record(row.properties[name])
  let value: string | undefined
  switch (property?.type) {
    case 'title':
      value = richText(property.title)
      break
    case 'rich_text':
      value = richText(property.rich_text)
      break
    case 'select':
      value = record(property.select)?.name as string | undefined
      break
    case 'status':
      value = record(property.status)?.name as string | undefined
      break
    case 'url':
      value = typeof property.url === 'string' ? property.url : undefined
      break
    case undefined:
      break
    default:
      throw new Error(
        `Factory Tasks row ${normalizeNotionPageId(row.id)} property ${name} has unsupported type ${String(property?.type)}`,
      )
  }
  value = typeof value === 'string' ? value.trim() : undefined
  if (!value && required) {
    throw new Error(`Factory Tasks row ${normalizeNotionPageId(row.id)} requires property ${name}`)
  }
  return value || undefined
}

function propertyList(row: FactoryTasksNotionPage, name: string): string[] {
  const property = record(row.properties[name])
  if (!property) return []
  if (property.type === 'multi_select') {
    if (!Array.isArray(property.multi_select)) {
      throw new Error(`Factory Tasks row ${normalizeNotionPageId(row.id)} property ${name} is malformed`)
    }
    return property.multi_select.map((entry) => {
      const value = record(entry)?.name
      if (typeof value !== 'string' || !value.trim()) {
        throw new Error(`Factory Tasks row ${normalizeNotionPageId(row.id)} property ${name} is malformed`)
      }
      return value.trim()
    })
  }
  const scalar = propertyText(row, name, false)
  return scalar?.split(',').map((entry) => entry.trim()).filter(Boolean) ?? []
}

function richText(value: unknown): string | undefined {
  if (!Array.isArray(value)) return undefined
  const text = value.map((entry) => {
    const item = record(entry)
    if (typeof item?.plain_text === 'string') return item.plain_text
    const content = record(item?.text)?.content
    return typeof content === 'string' ? content : ''
  }).join('')
  return text || undefined
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values)]
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}
