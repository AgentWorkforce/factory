import type {
  AcceptDurableSubscriptionDeliveryInput,
  ClaimDurableSubscriptionDeliveriesInput,
  CreateOrRenewDurableResourceSubscriptionInput,
  DurableResourceSubscription,
  DurableSubscriptionDelivery,
  DurableSubscriptionDeliveryListResponse,
  DurableSubscriptionDeliveryResponse,
} from '@relayfile/sdk'

/**
 * Consumer-facing contract for Relayfile's durable resource-subscription API.
 *
 * Relayfile owns matching and delivery-claim persistence. A runtime owns the
 * meaning of its opaque IDs and must only accept a claim after it has made the
 * corresponding wake durable on its side.
 */
export type ResourceSubscriptionInput = Omit<
  CreateOrRenewDurableResourceSubscriptionInput,
  'workspaceId' | 'correlationId' | 'signal'
>

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

/** The canonical Relayfile SDK surface used by Factory. */
export interface ResourceSubscriptionsSdk {
  createOrRenewDurableResourceSubscription(
    input: CreateOrRenewDurableResourceSubscriptionInput,
  ): Promise<DurableResourceSubscription>
  claimDurableSubscriptionDeliveries(
    input: ClaimDurableSubscriptionDeliveriesInput,
  ): Promise<DurableSubscriptionDeliveryListResponse>
  acceptDurableSubscriptionDelivery(
    input: AcceptDurableSubscriptionDeliveryInput,
  ): Promise<DurableSubscriptionDeliveryResponse>
  cancelDurableResourceSubscription(
    workspaceId: string,
    subscriptionId: string,
    options?: { signal?: AbortSignal },
  ): Promise<void>
}

export type ResourceSubscriptionsSdkClientOptions = {
  /** Optional lifecycle cancellation forwarded through the SDK on every call. */
  signal?: AbortSignal
}

/** The service is absent or too old; callers may retain their legacy route. */
export class ResourceSubscriptionsUnavailableError extends Error {
  constructor(message = 'Relayfile durable resource subscriptions are unavailable') {
    super(message)
    this.name = 'ResourceSubscriptionsUnavailableError'
  }
}

export const isResourceSubscriptionsUnavailable = (error: unknown): boolean => {
  if (error instanceof ResourceSubscriptionsUnavailableError) return true
  if (!error || typeof error !== 'object') return false
  const record = error as Record<string, unknown>
  const status = record.status ?? record.statusCode ?? record.httpStatus
  return status === 404
}

/**
 * Adapts Factory's narrow orchestration port to Relayfile's public SDK.
 * Authentication, request timeouts, retries, URL construction, and response
 * transport remain owned by RelayFileClient.
 */
export function createResourceSubscriptionsSdkClient(
  sdk: ResourceSubscriptionsSdk,
  options: ResourceSubscriptionsSdkClientOptions = {},
): ResourceSubscriptionsClient {
  return {
    async createOrRenew(workspaceId, input) {
      const subscription = await sdk.createOrRenewDurableResourceSubscription({
        workspaceId,
        ...input,
        signal: options.signal,
      })
      return toResourceSubscription(subscription)
    },

    async claimDeliveryClaims(workspaceId, input) {
      const { deliveries } = await sdk.claimDurableSubscriptionDeliveries({
        workspaceId,
        ...(input?.limit === undefined ? {} : { limit: input.limit }),
        signal: options.signal,
      })
      if (!Array.isArray(deliveries)) {
        throw new Error('Relayfile SDK returned an invalid durable-delivery envelope')
      }
      return deliveries.map(toResourceDeliveryClaim)
    },

    async acceptDelivery(workspaceId, input) {
      const { delivery } = await sdk.acceptDurableSubscriptionDelivery({
        workspaceId,
        deliveryId: input.deliveryId,
        claimToken: input.claimToken,
        signal: options.signal,
      })
      if (delivery.status !== 'accepted') {
        throw new Error(`Relayfile SDK returned delivery ${delivery.id} with non-accepted status ${delivery.status}`)
      }
      return {
        deliveryId: delivery.id,
        subscriptionId: delivery.subscriptionId,
        terminal: delivery.terminal,
      }
    },

    async cancel(workspaceId, input) {
      await sdk.cancelDurableResourceSubscription(
        workspaceId,
        input.subscriptionId,
        { signal: options.signal },
      )
    },
  }
}

const toResourceSubscription = (
  subscription: DurableResourceSubscription,
): ResourceSubscription => ({
  subscriptionId: subscription.id,
  ownerId: subscription.ownerId,
  subscriberId: subscription.subscriberId,
  provider: subscription.provider,
  resourceRef: subscription.resourceRef,
  eventTypes: subscription.eventTypes,
  terminalEventTypes: subscription.terminalEventTypes,
  ...(subscription.intent ? { intent: subscription.intent } : {}),
  expiresAt: subscription.expiresAt,
})

const toResourceDeliveryClaim = (
  delivery: DurableSubscriptionDelivery,
): ResourceDeliveryClaim => {
  if (delivery.status !== 'claimed' || !delivery.claimToken) {
    throw new Error(`Relayfile SDK returned delivery ${delivery.id} without a live claim`)
  }
  return {
    deliveryId: delivery.id,
    claimToken: delivery.claimToken,
    subscriptionId: delivery.subscriptionId,
    ownerId: delivery.ownerId,
    subscriberId: delivery.subscriberId,
    provider: delivery.provider,
    resourceRef: delivery.resourceRef,
    eventType: delivery.event.type,
    terminal: delivery.terminal,
  }
}
