import { linearCommentPath, linearIssuePath } from '../constants/linear'
import type { MountClient } from '../ports'
import { assertInFactoryScope, isInFactoryScope } from '../safety/factory-scope'
import type { LinearIssue } from '../types'
import { asRecord, safePathSegment, stableHash, wrappedPayload } from './shared'

export interface LinearStateIds {
  [name: string]: string
}

export interface LinearCommentPayload {
  body: string
  botActor: ''
  isArtificialAgentSessionRoot: false
  issue: {
    id: string
    identifier: string
    title: string
    url: string
  }
  issue_id: string
  issueId: string
}

export interface MountLinearWritebackConfig {
  stateIds?: LinearStateIds
  safety?: {
    requireTitlePrefix?: string
    requireTeamKey?: string
  }
}

export interface LinearCreateIssuePayload extends Record<string, unknown> {
  id?: string
  identifier?: string
  title?: string
  team?: unknown
}

const issuePath = (issue: LinearIssue): string =>
  issue.path || linearIssuePath(issue.key, issue.uuid)

export const linearCommentName = (issue: LinearIssue, body: string): string =>
  `factory-${safePathSegment(issue.key)}-${stableHash(body)}`

const issueUrl = (issue: LinearIssue): string => {
  const rawUrl = asRecord(issue.raw)?.url
  const payloadUrl = asRecord(asRecord(issue.raw)?.payload)?.url
  return typeof rawUrl === 'string'
    ? rawUrl
    : typeof payloadUrl === 'string'
      ? payloadUrl
      : ''
}

const linearCommentPayload = (issue: LinearIssue, body: string): LinearCommentPayload => ({
  body,
  botActor: '',
  isArtificialAgentSessionRoot: false,
  issue: {
    id: issue.uuid,
    identifier: issue.key,
    title: issue.title,
    url: issueUrl(issue),
  },
  issue_id: issue.uuid,
  issueId: issue.uuid,
})

const safetyFromConfig = (configOrStateIds?: LinearStateIds | MountLinearWritebackConfig) => {
  const safety = asRecord(asRecord(configOrStateIds)?.safety)
  if (safety) {
    return {
      requireTitlePrefix: typeof safety.requireTitlePrefix === 'string' && safety.requireTitlePrefix
        ? safety.requireTitlePrefix
        : '[factory-e2e]',
      requireTeamKey: typeof safety.requireTeamKey === 'string' && safety.requireTeamKey
        ? safety.requireTeamKey
        : 'AR',
    }
  }
  return { requireTitlePrefix: '[factory-e2e]', requireTeamKey: 'AR' }
}

const payloadInFactoryScope = (
  payload: Record<string, unknown>,
  safety: ReturnType<typeof safetyFromConfig>,
): boolean => {
  return isInFactoryScope(scopeIssueFromPayload(payload, 'createIssue payload'), safety)
}

const readIssuePayloadForGuard = async (
  mount: MountClient,
  issue: LinearIssue,
): Promise<Record<string, unknown>> => {
  const path = issuePath(issue)
  try {
    return wrappedPayload((await mount.readFile(path)).content)
  } catch {
    throw new Error(`Refusing Linear writeback for ${issue.key}: unable to read guard fields from ${path}`)
  }
}

const createIssuePath = (payload: LinearCreateIssuePayload): string => {
  const identifier = typeof payload.identifier === 'string' && payload.identifier ? payload.identifier : undefined
  const id = typeof payload.id === 'string' && payload.id ? payload.id : undefined
  if (identifier && id) return linearIssuePath(identifier, id)
  if (identifier) return `/linear/issues/${safePathSegment(identifier)}.json`
  if (id) return `/linear/issues/${safePathSegment(id)}.json`
  throw new Error('Linear createIssue payload must include id or identifier')
}

const confirmWriteback = async (
  mount: MountClient,
  path: string,
  verify: () => Promise<boolean>,
): Promise<void> => {
  const confirmation = await mount.confirmWrite(path, { timeoutMs: 90_000 })
  if (confirmation !== 'acked') {
    throw new Error(`Writeback not acked for ${path}: ${confirmation}`)
  }

  if (!await verify()) {
    throw new Error(`Writeback read-back verification failed for ${path}`)
  }
}

export const MountLinearWriteback = (
  mount: MountClient,
  configOrStateIds?: LinearStateIds | MountLinearWritebackConfig,
) => {
  const safety = safetyFromConfig(configOrStateIds)
  const adapter = {
    async setState(issue: LinearIssue, stateId: string): Promise<void> {
      assertInFactoryScope(scopeIssueFromPayload(await readIssuePayloadForGuard(mount, issue), issue.key), safety)
      const path = issuePath(issue)
      await mount.writeFile(path, { stateId })
      await confirmWriteback(mount, path, () => adapter.verify(issue, { stateId }))
    },

    async postComment(issue: LinearIssue, body: string): Promise<void> {
      assertInFactoryScope(scopeIssueFromPayload(await readIssuePayloadForGuard(mount, issue), issue.key), safety)
      const name = linearCommentName(issue, body)
      const path = linearCommentPath(issuePath(issue), name)
      await mount.writeFile(path, linearCommentPayload(issue, body))
      await confirmWriteback(mount, path, () => adapter.verify(issue, { commentName: name }))
    },

    async createIssue(payload: LinearCreateIssuePayload): Promise<{ path: string }> {
      assertInFactoryScope(scopeIssueFromPayload(payload, 'createIssue payload'), safety, 'createIssue payload')
      const path = createIssuePath(payload)
      await mount.writeFile(path, payload)
      await confirmWriteback(mount, path, async () => {
        try {
          const written = wrappedPayload((await mount.readFile(path)).content)
          return payloadInFactoryScope(written, safety) &&
            (typeof payload.id !== 'string' || written.id === payload.id) &&
            (typeof payload.identifier !== 'string' || written.identifier === payload.identifier)
        } catch {
          return false
        }
      })
      return { path }
    },

    async verify(
      issue: LinearIssue,
      expect: { stateId?: string; commentName?: string },
    ): Promise<boolean> {
      try {
        if (expect.stateId) {
          const { content } = await mount.readFile(issuePath(issue))
          const payload = wrappedPayload(content)
          return payload.stateId === expect.stateId
        }

        if (expect.commentName) {
          const { content } = await mount.readFile(linearCommentPath(issuePath(issue), expect.commentName))
          const payload = wrappedPayload(content)
          return payload.issue_id === issue.uuid && payload.issueId === issue.uuid
        }

        return false
      } catch {
        return false
      }
    },
  }

  return adapter
}

const scopeIssueFromPayload = (payload: Record<string, unknown>, key: string) => ({
  key,
  title: typeof payload.title === 'string' ? payload.title : '',
  team: typeof asRecord(payload.team)?.key === 'string' ? asRecord(payload.team)?.key as string : undefined,
  raw: { payload },
})
