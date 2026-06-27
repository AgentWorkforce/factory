import {
  githubClient,
  linearClient,
  providerClient,
  relayClient,
  slackClient,
  type GithubClient,
  type IntegrationClientOptions,
  type LinearClient,
  type ProviderClient,
  type RelayClient,
  type SlackClient,
} from '@relayfile/relay-helpers'

import { runRelayflowsWorkflow, type WorkflowRunner, type WorkflowRunnerResult } from '../node/factory-node.js'
import type { ChangeEvent } from '../ports/mount.js'

// Normalized trigger string: "{provider}.{resource}.{action}"
// Examples: "linear.issue.created", "github.pull_request.review_submitted"
export type IntegrationTrigger = string

export interface TriggerEvent {
  trigger: IntegrationTrigger
  // Provider-specific resource identifier (e.g. issue UUID, PR number as string)
  resourceId?: string
  // Raw event payload forwarded from the integration webhook
  payload?: Record<string, unknown>
}

// Relay-helpers-backed provider clients injected into every input mapper.
// Paths come from the writeback-path catalog so they never drift from the
// adapters that materialize the draft — no hardcoded provider path strings.
export interface TriggerMapperContext {
  linear: LinearClient
  github: GithubClient
  slack: SlackClient
  relayClient: typeof relayClient
  providerClient: typeof providerClient
}

// Maps a trigger event to a structured input record for the relayflow template.
// May be async to allow reading provider state via relay-helpers before shaping inputs.
export type TriggerInputMapper = (
  event: TriggerEvent,
  ctx: TriggerMapperContext,
) => Promise<Record<string, unknown>> | Record<string, unknown>

export interface RelayflowPolicyEntry {
  // Path to the relayflow template (.yaml, .yml, .ts, or .py).
  // Relative paths are resolved against the cwd passed to dispatchRelayflowForTrigger.
  templatePath: string
  mapInputs: TriggerInputMapper
}

export interface RelayflowDispatchResult {
  trigger: string
  templatePath: string
  runId?: string
  status: WorkflowRunnerResult['status']
  inputs: Record<string, unknown>
}

/**
 * Registry mapping normalized integration triggers to relayflow template paths
 * and input mappers. Configurable at construction time — register policies via
 * `register()` and extend or override entries freely; no hardcoded switch inside
 * any node handler.
 */
export class RelayflowPolicyRegistry {
  readonly #entries = new Map<IntegrationTrigger, RelayflowPolicyEntry>()

  register(trigger: IntegrationTrigger, entry: RelayflowPolicyEntry): this {
    this.#entries.set(trigger, entry)
    return this
  }

  resolve(trigger: IntegrationTrigger): RelayflowPolicyEntry | undefined {
    return this.#entries.get(trigger)
  }

  get size(): number {
    return this.#entries.size
  }

  triggers(): IterableIterator<IntegrationTrigger> {
    return this.#entries.keys()
  }
}

export function createRelayflowPolicyRegistry(): RelayflowPolicyRegistry {
  return new RelayflowPolicyRegistry()
}

export interface DispatchRelayflowOptions {
  // Working directory passed to the workflow runner (resolves relative templatePaths).
  cwd: string
  // Mount root for the relay-helpers VFS clients (provider read/write operations).
  // Defaults to the process ambient mount root when omitted.
  mountRoot?: string
  // Override the workflow runner (defaults to runRelayflowsWorkflow).
  workflowRunner?: WorkflowRunner
}

export type RelayflowDynamicClient = RelayClient<string & Parameters<typeof relayClient>[0]>
export type RelayflowDynamicProviderClient = ProviderClient<string & Parameters<typeof providerClient>[0]>

export function triggerEventFromChangeEvent(event: ChangeEvent): TriggerEvent | null {
  const path = event.resource?.path
  const provider = event.resource?.provider ?? providerFromPath(path)
  const resource = resourceFromPath(provider, path)
  if (!provider || !resource) return null

  return {
    trigger: `${provider}.${resource}.${actionFromChangeEvent(event)}`,
    resourceId: event.resource?.id ?? path,
    payload: {
      event,
      path,
      provider,
      resource,
    },
  }
}

export async function dispatchRelayflowForChangeEvent(
  changeEvent: ChangeEvent,
  registry: RelayflowPolicyRegistry,
  opts: DispatchRelayflowOptions,
): Promise<RelayflowDispatchResult | null> {
  const event = triggerEventFromChangeEvent(changeEvent)
  if (!event) return null
  return dispatchRelayflowForTrigger(event, registry, opts)
}

/**
 * Look up `event.trigger` in `registry`, build relay-helpers-backed provider
 * clients, call the entry's `mapInputs`, and invoke the relayflows SDK.
 *
 * Returns null when no policy is registered for the trigger (no dispatch, no
 * error). Returns a {@link RelayflowDispatchResult} with the run id and status
 * on success.
 */
export async function dispatchRelayflowForTrigger(
  event: TriggerEvent,
  registry: RelayflowPolicyRegistry,
  opts: DispatchRelayflowOptions,
): Promise<RelayflowDispatchResult | null> {
  const entry = registry.resolve(event.trigger)
  if (!entry) return null

  const clientOpts: IntegrationClientOptions = opts.mountRoot ? { mountRoot: opts.mountRoot } : {}
  const ctx: TriggerMapperContext = {
    linear: linearClient(clientOpts),
    github: githubClient(clientOpts),
    slack: slackClient(clientOpts),
    relayClient: (provider) => relayClient(provider, clientOpts),
    providerClient: (provider) => providerClient(provider, clientOpts),
  }

  const inputs = await entry.mapInputs(event, ctx)
  const runner = opts.workflowRunner ?? runRelayflowsWorkflow

  const result = await runner({
    workflow: entry.templatePath,
    cwd: opts.cwd,
    inputs,
  })

  return {
    trigger: event.trigger,
    templatePath: entry.templatePath,
    runId: result.runId,
    status: result.status,
    inputs,
  }
}

function providerFromPath(path: string | undefined): string | undefined {
  return path?.split('/').filter(Boolean)[0]
}

function resourceFromPath(provider: string | undefined, path: string | undefined): string | undefined {
  if (!path) return undefined
  const segments = path.split('/').filter(Boolean)
  if (segments.length < 2) return undefined

  if (provider === 'github') {
    const reposIndex = segments.indexOf('repos')
    const collection = reposIndex >= 0 ? segments[reposIndex + 3] : undefined
    return normalizeResourceName(collection ?? segments[1])
  }

  if (provider === 'slack') {
    const messageIndex = segments.findIndex((segment) => segment === 'messages' || segment === 'replies')
    if (messageIndex >= 0) return normalizeResourceName(segments[messageIndex])
  }

  return normalizeResourceName(segments[1])
}

function normalizeResourceName(value: string | undefined): string | undefined {
  if (!value) return undefined
  if (value === 'pulls' || value === 'pull-requests' || value === 'pull_requests') return 'pull_request'
  if (value.endsWith('ies')) return `${value.slice(0, -3)}y`
  if (value.endsWith('s')) return value.slice(0, -1)
  return value
}

function actionFromChangeEvent(event: ChangeEvent): string {
  const filesystemEventType = (event as ChangeEvent & { filesystemEventType?: string }).filesystemEventType
  return actionFromChangeEventType(filesystemEventType ?? event.type)
}

function actionFromChangeEventType(type: ChangeEvent['type'] | string): string {
  const eventType = String(type)
  if (eventType === 'file.created') return 'created'
  if (eventType === 'file.updated') return 'updated'
  if (eventType === 'file.deleted') return 'deleted'
  const parts = eventType.split('.').filter(Boolean)
  return parts.at(-1) ?? 'changed'
}
