import { slackMessagePath, slackReplyPath } from '../constants/slack'
import type { MountClient } from '../ports'
import { safePathSegment, stableHash, trimToLines } from './shared'

export interface MountSlackWritebackConfig {
  channel?: string
  channelDir?: string
  clientIdPrefix?: string
}

interface ThreadRef {
  channelDir: string
  channelId: string
  threadTs: string
}

const channelIdFromDir = (channelDir: string): string => channelDir.split('__')[0] ?? channelDir

const channelSegment = (value: string): string => {
  const trimmed = value.trim().replace(/^#/u, '')
  const match = trimmed.match(/(?:^|\/)slack\/channels\/([^/]+)/u)
  return match?.[1] ?? trimmed.split('/')[0] ?? trimmed
}

const channelAliases = (value?: string): Set<string> => {
  const segment = typeof value === 'string' ? channelSegment(value) : ''
  if (!segment) return new Set()

  const aliases = new Set<string>()
  const add = (candidate: string) => {
    const normalized = candidate.trim().replace(/^#/u, '').toLowerCase()
    if (normalized) aliases.add(normalized)
  }

  add(segment)
  const [id, name] = segment.split('__')
  if (id) add(id)
  if (name) add(name)

  return aliases
}

const assertSlackChannelAllowed = (
  configuredChannel: string | undefined,
  targetChannel: string | undefined,
  context: string,
): void => {
  const configured = channelAliases(configuredChannel)
  const target = channelAliases(targetChannel)
  if (configured.size === 0) {
    throw new Error(`Refusing Slack writeback for ${context}: configured factory-e2e channel is required`)
  }
  if (target.size === 0 || ![...target].some((alias) => configured.has(alias))) {
    throw new Error(`Refusing Slack writeback for ${context}: target channel must match configured factory-e2e channel`)
  }
}

const pathTs = (threadTs: string): string => threadTs.replace(/\./g, '_')

const payloadTs = (threadId: string): string => threadId.replace(/_/g, '.')

const rootClientId = (prefix: string, channelDir: string, text: string): string =>
  `${safePathSegment(prefix)}-${safePathSegment(channelIdFromDir(channelDir))}-${stableHash(text)}`

const replyClientId = (prefix: string, threadId: string, text: string): string =>
  `${safePathSegment(prefix)}-reply-${safePathSegment(threadId)}-${stableHash(text)}`

const confirmPath = async (mount: MountClient, path: string): Promise<void> => {
  const confirmation = await mount.confirmWrite(path, { timeoutMs: 90_000 })
  if (confirmation !== 'acked') {
    throw new Error(`Writeback not acked for ${path}: ${confirmation}`)
  }

  await mount.readFile(path)
}

export const MountSlackWriteback = (
  mount: MountClient,
  slackCfg: MountSlackWritebackConfig = {},
) => {
  const threads = new Map<string, ThreadRef>()
  const prefix = slackCfg.clientIdPrefix ?? 'factory'

  return {
    async postThread(root: { channel: string; text: string }): Promise<{ threadId: string }> {
      const channelDir = slackCfg.channelDir ?? root.channel ?? slackCfg.channel
      assertSlackChannelAllowed(slackCfg.channel, root.channel, 'postThread')
      assertSlackChannelAllowed(slackCfg.channel, channelDir, 'postThread path')

      const text = trimToLines(root.text, 3)
      const channelId = channelIdFromDir(channelDir)
      const clientId = rootClientId(prefix, channelDir, text)
      const path = slackMessagePath(channelDir, clientId)

      await mount.writeFile(path, { channelId, text }, { guarded: true })
      await confirmPath(mount, path)

      threads.set(clientId, { channelDir, channelId, threadTs: clientId })
      return { threadId: clientId }
    },

    async reply(threadId: string, text: string): Promise<void> {
      const fallbackChannelDir = slackCfg.channelDir ?? slackCfg.channel
      assertSlackChannelAllowed(slackCfg.channel, fallbackChannelDir, 'reply')
      const ref = threads.get(threadId) ?? (
        fallbackChannelDir
          ? { channelDir: fallbackChannelDir, channelId: channelIdFromDir(fallbackChannelDir), threadTs: payloadTs(threadId) }
          : undefined
      )
      if (!ref) {
        throw new Error(`Unknown Slack thread ${threadId}; provide channelDir in slack config`)
      }
      assertSlackChannelAllowed(slackCfg.channel, ref.channelDir, 'reply path')

      const body = trimToLines(text, 3)
      const clientId = replyClientId(prefix, threadId, body)
      const path = slackReplyPath(ref.channelDir, pathTs(ref.threadTs), clientId)

      await mount.writeFile(path, {
        channelId: ref.channelId,
        thread_ts: ref.threadTs,
        text: body,
      }, { guarded: true })
      await confirmPath(mount, path)
    },
  }
}
