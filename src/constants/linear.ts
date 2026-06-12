export const LINEAR_STATE_IDS = {
  readyForAgent: 'b9bec744-b60c-4745-8022-d90d6ab59ae3',
  agentImplementing: '39b9881d-1196-4c95-8b80-a20f0c7263f7',
  done: '83ea5383-bfe9-425a-86ef-517b8190f09a',
  inPlanning: '3de351f2-90e6-4731-aa6b-4a55b77f481e',
} as const

export const linearIssuePath = (key: string, uuid: string) => `/linear/issues/${key}__${uuid}.json`

export const linearByStatePath = (slug: string) => `/linear/issues/by-state/${slug}/`

export const linearCommentPath = (_issuePath: string, name: string) =>
  `/linear/comments/${name}.json`
