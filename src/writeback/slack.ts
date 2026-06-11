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
      if (!channelDir) {
        throw new Error('Slack channel is required for postThread')
      }

      const text = trimToLines(root.text, 3)
      const channelId = channelIdFromDir(channelDir)
      const clientId = rootClientId(prefix, channelDir, text)
      const path = slackMessagePath(channelDir, clientId)

      await mount.writeFile(path, { channelId, text })
      await confirmPath(mount, path)

      threads.set(clientId, { channelDir, channelId, threadTs: clientId })
      return { threadId: clientId }
    },

    async reply(threadId: string, text: string): Promise<void> {
      const fallbackChannelDir = slackCfg.channelDir ?? slackCfg.channel
      const ref = threads.get(threadId) ?? (
        fallbackChannelDir
          ? { channelDir: fallbackChannelDir, channelId: channelIdFromDir(fallbackChannelDir), threadTs: payloadTs(threadId) }
          : undefined
      )
      if (!ref) {
        throw new Error(`Unknown Slack thread ${threadId}; provide channelDir in slack config`)
      }

      const body = trimToLines(text, 3)
      const clientId = replyClientId(prefix, threadId, body)
      const path = slackReplyPath(ref.channelDir, pathTs(ref.threadTs), clientId)

      await mount.writeFile(path, {
        channelId: ref.channelId,
        thread_ts: ref.threadTs,
        text: body,
      })
      await confirmPath(mount, path)
    },
  }
}
