/**
 * Consumer-facing contract for Relayfile's durable resource-subscription API.
 *
 * Relayfile owns matching and delivery-claim persistence. A runtime owns the
 * meaning of its opaque IDs and must only accept a claim after it has made the
 * corresponding wake durable on its side.
 */
export type ResourceSubscriptionInput = {
  provider: string
  resourceRef: string
  eventTypes: string[]
  terminalEventTypes?: string[]
  subscriberId: string
  intent?: string
  ttlSeconds: number
}

export type ResourceSubscription = {
  subscriptionId: string
  /** Derived by Relayfile from the authenticated workspace bearer token. */
  ownerId: string
  provider: string
  resourceRef: string
  eventTypes: string[]
  terminalEventTypes?: string[]
  subscriberId: string
  intent?: string
  expiresAt: string
}

export type ResourceDeliveryClaim = {
  deliveryId: string
  /** Opaque, short-lived lease credential required to accept this claim. */
  claimToken: string
  subscriptionId: string
  resourceRef: string
  eventType: string
  subscriberId: string
  ownerId: string
  provider: string
  terminal: boolean
}

export type AcceptedResourceDelivery = {
  deliveryId: string
  subscriptionId: string
  terminal: boolean
}

export interface ResourceSubscriptionsClient {
  createOrRenew(workspaceId: string, input: ResourceSubscriptionInput): Promise<ResourceSubscription>
  /** Atomically lease pending claims for the authenticated owner. */
  claimDeliveryClaims(workspaceId: string, input?: { limit?: number }): Promise<ResourceDeliveryClaim[]>
  acceptDelivery(workspaceId: string, input: { deliveryId: string; claimToken: string }): Promise<AcceptedResourceDelivery>
  cancel(workspaceId: string, input: { subscriptionId: string }): Promise<void>
}

/** The service is absent or too old; callers may retain their legacy route. */
export class ResourceSubscriptionsUnavailableError extends Error {
  constructor(message = 'Relayfile durable resource subscriptions are unavailable') {
    super(message)
    this.name = 'ResourceSubscriptionsUnavailableError'
  }
}

export class ResourceSubscriptionsHttpError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ResourceSubscriptionsHttpError'
    this.status = status
  }
}

export const isResourceSubscriptionsUnavailable = (error: unknown): boolean => {
  if (error instanceof ResourceSubscriptionsUnavailableError) return true
  if (!error || typeof error !== 'object') return false
  const record = error as Record<string, unknown>
  const status = record.status ?? record.statusCode ?? record.httpStatus
  return status === 404
}

export type ResourceSubscriptionsHttpClientOptions = {
  baseUrl: string
  tokenProvider: () => string | undefined | Promise<string | undefined>
  fetch?: typeof fetch
  /** Bounds an individual Relayfile API call; defaults to 15 seconds. */
  requestTimeoutMs?: number
  /** Optional lifecycle cancellation shared by all requests from this client. */
  signal?: AbortSignal
}

const DEFAULT_RESOURCE_SUBSCRIPTION_REQUEST_TIMEOUT_MS = 15_000

/**
 * Minimal REST adapter for Relayfile Cloud's public subscription contract.
 * The SDK can adopt these endpoints later without changing Factory's port.
 */
export function createResourceSubscriptionsHttpClient(
  options: ResourceSubscriptionsHttpClientOptions,
): ResourceSubscriptionsClient {
  const requestTimeoutMs = options.requestTimeoutMs === undefined
    ? DEFAULT_RESOURCE_SUBSCRIPTION_REQUEST_TIMEOUT_MS
    : Math.max(1, Math.trunc(options.requestTimeoutMs))

  const request = async (workspaceId: string, path: string, init: RequestInit = {}): Promise<unknown> => {
    const token = await options.tokenProvider()
    if (!token) throw new ResourceSubscriptionsHttpError(401, 'Relayfile workspace bearer token is unavailable')
    const url = new URL(`/v1/workspaces/${encodeURIComponent(workspaceId)}${path}`, options.baseUrl)
    const abort = new AbortController()
    const forwardedSignals = [options.signal, init.signal].filter((signal): signal is AbortSignal => Boolean(signal))
    const removeAbortListeners: Array<() => void> = []
    for (const signal of forwardedSignals) {
      const forwardAbort = () => abort.abort(signal.reason)
      if (signal.aborted) forwardAbort()
      else {
        signal.addEventListener('abort', forwardAbort, { once: true })
        removeAbortListeners.push(() => signal.removeEventListener('abort', forwardAbort))
      }
    }
    let timedOut = false
    const timeout = setTimeout(() => {
      timedOut = true
      abort.abort(new Error('Relayfile resource-subscription request timed out'))
    }, requestTimeoutMs)
    let response: Response
    try {
      response = await (options.fetch ?? fetch)(url, {
        ...init,
        signal: abort.signal,
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${token}`,
          ...(init.body ? { 'content-type': 'application/json' } : {}),
          ...init.headers,
        },
      })
    } catch (error) {
      if (timedOut) {
        throw new ResourceSubscriptionsHttpError(504, `Relayfile resource-subscription request timed out after ${requestTimeoutMs}ms`)
      }
      throw error
    } finally {
      clearTimeout(timeout)
      for (const remove of removeAbortListeners) remove()
    }
    if (!response.ok) {
      if (response.status === 404) throw new ResourceSubscriptionsUnavailableError()
      throw new ResourceSubscriptionsHttpError(response.status, `Relayfile resource-subscription request failed (${response.status})`)
    }
    if (response.status === 204) return undefined
    try {
      return await response.json()
    } catch {
      throw new ResourceSubscriptionsHttpError(response.status, 'Relayfile resource-subscription response was not valid JSON')
    }
  }

  return {
    async createOrRenew(workspaceId, input) {
      const payload = await request(workspaceId, '/subscriptions', {
        method: 'POST',
        body: JSON.stringify(input),
      })
      return parseSubscription(payload)
    },
    async claimDeliveryClaims(workspaceId, input) {
      const payload = await request(workspaceId, '/subscriptions/deliveries/claim', {
        method: 'POST',
        body: JSON.stringify(input ?? {}),
      })
      const envelope = record(payload)
      const deliveries = Array.isArray(payload)
        ? payload
        : Array.isArray(envelope?.deliveries)
          ? envelope.deliveries
          : undefined
      if (!deliveries) {
        throw new ResourceSubscriptionsHttpError(502, 'Relayfile delivery claim response was invalid')
      }
      return deliveries.map(parseDeliveryClaim)
    },
    async acceptDelivery(workspaceId, input) {
      const payload = await request(workspaceId, `/subscriptions/deliveries/${encodeURIComponent(input.deliveryId)}/accept`, {
        method: 'POST',
        body: JSON.stringify({ claimToken: input.claimToken }),
      })
      return parseAcceptedDelivery(payload)
    },
    async cancel(workspaceId, input) {
      await request(workspaceId, `/subscriptions/${encodeURIComponent(input.subscriptionId)}`, { method: 'DELETE' })
    },
  }
}

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const string = (value: unknown, field: string): string => {
  if (typeof value !== 'string' || !value) throw new ResourceSubscriptionsHttpError(502, `Relayfile resource-subscription response omitted ${field}`)
  return value
}

const stringArray = (value: unknown, field: string): string[] => {
  if (!Array.isArray(value) || !value.every((item) => typeof item === 'string')) {
    throw new ResourceSubscriptionsHttpError(502, `Relayfile resource-subscription response omitted ${field}`)
  }
  return value
}

const boolean = (value: unknown, field: string): boolean => {
  if (typeof value !== 'boolean') throw new ResourceSubscriptionsHttpError(502, `Relayfile resource-subscription response omitted ${field}`)
  return value
}

const parseSubscription = (value: unknown): ResourceSubscription => {
  const source = record(record(value)?.subscription) ?? record(value)
  if (!source) throw new ResourceSubscriptionsHttpError(502, 'Relayfile resource-subscription response was invalid')
  return {
    subscriptionId: string(source.id ?? source.subscriptionId, 'subscription id'),
    ownerId: string(source.ownerId, 'ownerId'),
    subscriberId: string(source.subscriberId, 'subscriberId'),
    provider: string(source.provider, 'provider'),
    resourceRef: string(source.resourceRef, 'resourceRef'),
    eventTypes: stringArray(source.eventTypes, 'eventTypes'),
    ...(source.terminalEventTypes === undefined ? {} : { terminalEventTypes: stringArray(source.terminalEventTypes, 'terminalEventTypes') }),
    ...(typeof source.intent === 'string' && source.intent ? { intent: source.intent } : {}),
    expiresAt: string(source.expiresAt, 'expiresAt'),
  }
}

const parseDeliveryClaim = (value: unknown): ResourceDeliveryClaim => {
  const source = record(value)
  if (!source) throw new ResourceSubscriptionsHttpError(502, 'Relayfile delivery claim response was invalid')
  const event = record(source.event)
  return {
    deliveryId: string(source.deliveryId ?? source.id, 'deliveryId'),
    claimToken: string(source.claimToken, 'claimToken'),
    subscriptionId: string(source.subscriptionId, 'subscriptionId'),
    ownerId: string(source.ownerId, 'ownerId'),
    subscriberId: string(source.subscriberId, 'subscriberId'),
    provider: string(source.provider, 'provider'),
    resourceRef: string(source.resourceRef, 'resourceRef'),
    eventType: typeof source.eventType === 'string'
      ? source.eventType
      : string(event?.type, 'event.type'),
    terminal: boolean(source.terminal, 'terminal'),
  }
}

const parseAcceptedDelivery = (value: unknown): AcceptedResourceDelivery => {
  const source = record(record(value)?.delivery) ?? record(value)
  if (!source) throw new ResourceSubscriptionsHttpError(502, 'Relayfile delivery acceptance response was invalid')
  return {
    deliveryId: string(source.deliveryId ?? source.id, 'deliveryId'),
    subscriptionId: string(source.subscriptionId, 'subscriptionId'),
    terminal: boolean(source.terminal, 'terminal'),
  }
}
