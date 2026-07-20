import type { ChangeEvent, MountClient } from '../ports'
import { asRecord, parseJsonContent, wrappedPayload } from '../writeback/shared'

export type SlackThreadReply = {
  channelDir: string
  threadTs: string
  messageTs: string
  text: string
  author?: string
  isThreadReply: boolean
  isBot: boolean
  raw: Record<string, unknown>
}

export type SlackThreadScopePredicates = {
  channelDirs: string[]
  threadIds?: string[]
  botUserId?: string
}

export type SlackThreadPredicateSubscriptionSpec = {
  slackThreadPredicates?: SlackThreadScopePredicates
}

export const slackThreadReplyGlob = (channelDir: string): string =>
  `/slack/channels/${channelDir}/messages/**`

export const isSlackMessageEventPath = (path: string): boolean =>
  /^\/slack\/channels\/[^/]+\/messages\/.+/u.test(path)

export const parseSlackThreadReply = (
  path: string,
  content: unknown,
  botUserId = 'U0B2596R7EZ',
): SlackThreadReply | undefined => {
  const raw = asRecord(parseJsonContent(content)) ?? {}
  const payload = wrappedPayload(raw)
  const channelDir = path.match(/^\/slack\/channels\/([^/]+)\//u)?.[1] ?? ''
  const pathMatch = path.match(/^\/slack\/channels\/[^/]+\/messages\/([^/]+)(?:\/replies\/([^/]+))?/u)
  const parentFromPath = pathMatch?.[2] ? slackPayloadTs(pathMatch[1]) : undefined
  const messageFromPath = slackPayloadTs(pathMatch?.[2] ?? pathMatch?.[1] ?? '')
  const messageTs = stringValue(payload.ts) ?? messageFromPath
  const threadTs = stringValue(payload.thread_ts) ?? parentFromPath
  if (!channelDir || !threadTs || !messageTs) return undefined

  return {
    channelDir,
    threadTs,
    messageTs,
    text: stringValue(payload.text) ?? '',
    author: stringValue(payload.user_name) ?? stringValue(payload.username) ?? stringValue(payload.user),
    isThreadReply: Boolean(parentFromPath) || threadTs !== messageTs,
    isBot: isSlackBotPayload(payload, botUserId),
    raw,
  }
}

/**
 * Provider-specific filtering for subscription consumers that want arbitrary
 * replies, but only inside explicitly allowed Slack channels/threads.
 */
export async function filterSlackThreadReplySpecs<TSpec extends SlackThreadPredicateSubscriptionSpec>(
  input: {
    mount: Pick<MountClient, 'readFile'>
    event: ChangeEvent
    matchedSpecs: TSpec[]
  },
): Promise<TSpec[]> {
  const path = input.event.resource?.path
  if (!path || !isSlackMessageEventPath(path)) return input.matchedSpecs
  const predicateSpecs = input.matchedSpecs.filter((spec) => spec.slackThreadPredicates)
  if (predicateSpecs.length === 0) return input.matchedSpecs

  let content: unknown
  try {
    content = (await input.mount.readFile(path)).content
  } catch {
    return input.matchedSpecs.filter((spec) => !spec.slackThreadPredicates)
  }

  return input.matchedSpecs.filter((spec) => {
    const predicates = spec.slackThreadPredicates
    if (!predicates) return true
    const reply = parseSlackThreadReply(path, content, predicates.botUserId)
    if (!reply?.isThreadReply || reply.isBot) return false
    if (!predicates.channelDirs.includes(reply.channelDir)) return false
    return !predicates.threadIds || predicates.threadIds.includes(reply.threadTs)
  })
}

const slackPayloadTs = (value: string): string => value.replace(/_/g, '.')
const stringValue = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined
const isSlackBotPayload = (payload: Record<string, unknown>, botUserId: string): boolean =>
  payload.user_is_bot === true ||
  stringValue(payload.user) === botUserId ||
  stringValue(payload.subtype) === 'bot_message' ||
  Boolean(stringValue(payload.bot_id))
