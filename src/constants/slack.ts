export const slackMessagePath = (channelDir: string, clientId: string) =>
  `/slack/channels/${channelDir}/messages/${clientId}.json`

export const slackReplyPath = (channelDir: string, parentTs: string, clientId: string) =>
  `/slack/channels/${channelDir}/messages/${parentTs}/replies/${clientId}.json`
