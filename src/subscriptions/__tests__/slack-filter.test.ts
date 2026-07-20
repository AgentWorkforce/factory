import { describe, expect, it } from 'vitest'

import type { ChangeEvent, MountClient } from '../../ports'
import { filterSlackThreadReplySpecs, parseSlackThreadReply } from '../slack-filter'

describe('Slack thread reply predicates', () => {
  const path = '/slack/channels/C123__factory/messages/1780751612_176219/replies/1780751613_000001.json'
  const content = {
    payload: {
      thread_ts: '1780751612.176219',
      ts: '1780751613.000001',
      text: 'Continue with the shared helper.',
      user: 'U123',
      user_name: 'human',
    },
  }

  it('parses arbitrary nested replies with provider and path identities', () => {
    expect(parseSlackThreadReply(path, content)).toMatchObject({
      channelDir: 'C123__factory',
      threadTs: '1780751612.176219',
      messageTs: '1780751613.000001',
      text: 'Continue with the shared helper.',
      author: 'human',
      isThreadReply: true,
      isBot: false,
    })
  })

  it('keeps only specs whose explicit channel and optional thread match', async () => {
    const event = { id: 'evt-1', resource: { path } } as ChangeEvent
    const mount = { readFile: async () => ({ content }) } as Pick<MountClient, 'readFile'>
    const specs = [
      { integrationId: 'unfiltered' },
      { integrationId: 'channel', slackThreadPredicates: { channelDirs: ['C123__factory'] } },
      { integrationId: 'thread', slackThreadPredicates: { channelDirs: ['C123__factory'], threadIds: ['1780751612.176219'] } },
      { integrationId: 'other', slackThreadPredicates: { channelDirs: ['C999'] } },
    ]

    await expect(filterSlackThreadReplySpecs({ mount, event, matchedSpecs: specs }))
      .resolves.toEqual(specs.slice(0, 3))
  })

  it('filters bot-authored replies so agent writeback cannot create a conversation loop', async () => {
    const event = { id: 'evt-bot', resource: { path } } as ChangeEvent
    const mount = {
      readFile: async () => ({ content: { payload: { ...content.payload, subtype: 'bot_message', bot_id: 'B123' } } }),
    } as Pick<MountClient, 'readFile'>
    const specs = [{ integrationId: 'channel', slackThreadPredicates: { channelDirs: ['C123__factory'] } }]

    await expect(filterSlackThreadReplySpecs({ mount, event, matchedSpecs: specs }))
      .resolves.toEqual([])
  })
})
