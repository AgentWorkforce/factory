import { dedupeStrings, stringList } from './common'
import { hasLinearPredicates, linearScopePredicates, type LinearScopePredicates } from './linear-filter'

const SLACK_DM_EVENT_GLOBS = [
  '/slack/channels/D*/**',
  '/slack/users/*/messages/**',
]

export type WatchRegistration = {
  glob: string
  coalesceMs: number
}

export type DeliveryTargets = {
  agents: string[]
  channels: string[]
}

export type LocalMountRoot = {
  localRoot: string
  remoteRoot: string
}

export type ConnectedIntegrationLike = {
  provider: string
  integrationId: string
  scope: Record<string, unknown>
  mountPaths: string[]
  connectedAt?: string
  notifyAgent?: boolean
  subscribeAgent?: boolean
  downloadHistoricalData?: boolean
  visibleInProject?: boolean
  localMountPaths?: string[]
  lastSyncAt?: string
  lastError?: string
}

export type SubscriptionSpec = {
  integrationId: string
  provider: string
  mountPaths: string[]
  localMountRoots: LocalMountRoot[]
  eventPathGlobs: string[]
  watches: WatchRegistration[]
  targets: DeliveryTargets
  allowHistoricalReplay: boolean
  linearPredicates?: LinearScopePredicates
}

function toRelayfileProvider(provider: string): string {
  const normalized = provider.trim().toLowerCase()
  return normalized === 'gmail' ? 'google-mail' : normalized
}

function isSlackProvider(provider: string): boolean {
  const normalized = toRelayfileProvider(provider)
  return normalized === 'slack' || normalized.startsWith('slack-')
}

function isLinearProvider(provider: string): boolean {
  const normalized = toRelayfileProvider(provider)
  return normalized === 'linear' || normalized.startsWith('linear-')
}

function scopeBooleanDefault(scope: Record<string, unknown>, keys: string[], defaultValue: boolean): boolean {
  for (const key of keys) {
    const value = scope[key]
    if (typeof value === 'boolean') return value
  }
  return defaultValue
}

export function slackListenDms(integration: ConnectedIntegrationLike): boolean {
  if (!isSlackProvider(integration.provider)) return false
  return scopeBooleanDefault(integration.scope, ['listenDms', 'listenDirectMessages', 'directMessages'], false)
}

// ported from src/main/integration-event-bridge.ts @canonicalMountPaths
export function canonicalMountPaths(integration: ConnectedIntegrationLike): string[] {
  const provider = toRelayfileProvider(integration.provider)
  const mountPaths = integration.mountPaths.flatMap((path) => {
    const discovery = path.match(/^\/discovery(?:\/.*)?$/)
    if (discovery) return [path]
    const prefixed = path.match(/^\/integrations\/[^/]+(\/.*)?$/)
    if (prefixed) {
      const normalized = `/${provider}${prefixed[1] ?? ''}`
      return [normalized, ...slackCanonicalChannelAliases(provider, normalized)]
    }
    const rootLevel = path.match(/^\/[^/]+(\/.*)?$/)
    if (rootLevel) {
      const normalized = `/${provider}${rootLevel[1] ?? ''}`
      return [normalized, ...slackCanonicalChannelAliases(provider, normalized)]
    }
    return [path, ...slackCanonicalChannelAliases(provider, path)]
  })
  return dedupeStrings(mountPaths)
}

function slackCanonicalChannelAliases(provider: string, path: string): string[] {
  if (!isSlackProvider(provider)) return []
  const match = path.match(/^\/slack\/channels\/([^/]+)__(?:[^/]+)(\/.*)?$/u)
  const channelId = match?.[1]?.trim()
  if (!channelId) return []
  return [`/slack/channels/${channelId}${match?.[2] ?? ''}`]
}

function watchGlobForPath(path: string): string {
  const root = path.trim().replace(/\/+$/u, '')
  return root.endsWith('/**') ? root : `${root || '/'}/**`
}

// ported from src/main/integration-event-bridge.ts @eventPathGlobsForIntegration
export function eventPathGlobsForIntegration(integration: ConnectedIntegrationLike): string[] {
  return dedupeStrings([
    ...canonicalMountPaths(integration).map(watchGlobForPath),
    ...(slackListenDms(integration) ? SLACK_DM_EVENT_GLOBS : []),
  ])
}

function normalizeAgentTarget(value: string): string | null {
  const trimmed = value.trim()
  if (!trimmed || trimmed.startsWith('#')) return null
  return trimmed.startsWith('@') ? trimmed.slice(1).trim() || null : trimmed
}

function normalizeChannelTarget(value: string): string | null {
  const trimmed = value.trim()
  if (!trimmed) return null
  return trimmed.startsWith('#') ? trimmed : `#${trimmed}`
}

// ported from src/main/integration-event-bridge.ts @deliveryTargetsFor
export function deliveryTargetsFor(integrations: ConnectedIntegrationLike[]): DeliveryTargets {
  const agents: string[] = []
  const channels: string[] = []
  for (const integration of integrations) {
    const scope = integration.scope
    agents.push(
      ...[
        ...stringList(scope.notifyAgents),
        ...stringList(scope.notificationAgents),
        ...stringList(scope.listenerAgents),
        ...stringList(scope.agentListeners),
      ].map(normalizeAgentTarget).filter((entry): entry is string => entry !== null),
    )
    channels.push(
      ...[
        ...stringList(scope.notifyChannels),
        ...stringList(scope.notificationChannels),
        ...stringList(scope.listenerChannels),
        ...stringList(scope.channelListeners),
        ...stringList(scope.relayChannels),
      ].map(normalizeChannelTarget).filter((entry): entry is string => entry !== null),
    )
  }
  return {
    agents: dedupeStrings(agents),
    channels: dedupeStrings(channels),
  }
}

function pathSegments(path: string): string[] {
  return path.split(/[\\/]+/u).filter(Boolean)
}

function remoteRootForLocalMountPath(workspaceId: string, localPath: string): string | null {
  const segments = pathSegments(localPath)
  const workspaceIndex = segments.findIndex((segment) => segment === workspaceId)
  if (workspaceIndex < 0) return null
  const remoteSegments = segments.slice(workspaceIndex + 1)
  return remoteSegments.length > 0 ? `/${remoteSegments.join('/')}` : null
}

function normalizeRemotePath(path: string): string | null {
  const segments = path
    .trim()
    .split(/[\\/]+/)
    .filter((segment) => segment.length > 0)
  if (segments.some((segment) => segment === '.' || segment === '..')) return null
  return segments.length > 0 ? `/${segments.join('/')}` : '/'
}

function slackWritebackCommandMountPathFor(provider: string, mountPath: string): string | null {
  if (!isSlackProvider(provider)) return null
  const normalized = normalizeRemotePath(mountPath)
  if (!normalized) return null
  const segments = normalized.split('/').filter(Boolean)
  if (segments[0] !== 'slack') return null

  const collection = segments[1]
  if (!['channels', 'dms', 'users'].includes(collection || '')) return null

  if (segments.length === 3) return `${normalized}/messages`
  if (segments.length === 4 && segments[3] === 'messages') return normalized
  if (segments.length === 5 && segments[3] === 'threads') return `${normalized}/replies`
  if (segments.length === 6 && segments[3] === 'threads' && segments[5] === 'replies') return normalized
  return null
}

function isSlackWritebackCommandRoot(remotePath: string): boolean {
  const normalized = normalizeRemotePath(remotePath)
  return normalized !== null && slackWritebackCommandMountPathFor('slack', remotePath) === normalized
}

function allowsLocalMountWatching(integration: ConnectedIntegrationLike): boolean {
  return integration.downloadHistoricalData === true &&
    canonicalMountPaths(integration).some((mountPath) => isSlackWritebackCommandRoot(mountPath))
}

function concreteLocalMountRootsForIntegration(
  workspaceId: string,
  integration: ConnectedIntegrationLike,
  _mountPaths: string[],
): LocalMountRoot[] {
  if (!allowsLocalMountWatching(integration)) return []

  return (integration.localMountPaths || [])
    .map((localRoot) => {
      const remoteRoot = remoteRootForLocalMountPath(workspaceId, localRoot)
      return remoteRoot ? { localRoot, remoteRoot } : null
    })
    .filter((entry): entry is LocalMountRoot =>
      entry !== null && isSlackWritebackCommandRoot(entry.remoteRoot)
    )
}

// ported from src/main/integration-event-bridge.ts @subscriptionSpecsFor
export function subscriptionSpecsFor(
  integrations: ConnectedIntegrationLike[],
  localMountWorkspaceId?: string,
): SubscriptionSpec[] {
  return integrations.map((integration) => {
    const mountPaths = canonicalMountPaths(integration)
    const eventPathGlobs = eventPathGlobsForIntegration(integration)
    return {
      integrationId: integration.integrationId,
      provider: integration.provider,
      mountPaths,
      localMountRoots: localMountWorkspaceId
        ? concreteLocalMountRootsForIntegration(localMountWorkspaceId, integration, mountPaths)
        : [],
      eventPathGlobs,
      watches: eventPathGlobs.map((glob) => ({
        glob,
        coalesceMs: 750,
      })),
      targets: deliveryTargetsFor([integration]),
      allowHistoricalReplay: integration.downloadHistoricalData === true,
      ...(isLinearProvider(integration.provider) && hasLinearPredicates(linearScopePredicates(integration.scope))
        ? { linearPredicates: linearScopePredicates(integration.scope) }
        : {}),
    }
  }).filter((spec) => spec.watches.length > 0)
}
