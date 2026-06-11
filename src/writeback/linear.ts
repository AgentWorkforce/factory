import { linearCommentPath, linearIssuePath } from '../constants/linear'
import type { MountClient } from '../ports'
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

const safetyFromConfig = (
  configOrStateIds?: LinearStateIds | MountLinearWritebackConfig,
): { titlePrefix: string; teamKey: string } => {
  const safety = asRecord(asRecord(configOrStateIds)?.safety)
  if (safety) {
    return {
      titlePrefix: typeof safety.requireTitlePrefix === 'string' && safety.requireTitlePrefix
        ? safety.requireTitlePrefix
        : '[factory-e2e]',
      teamKey: typeof safety.requireTeamKey === 'string' && safety.requireTeamKey
        ? safety.requireTeamKey
        : 'AR',
    }
  }
  return { titlePrefix: '[factory-e2e]', teamKey: 'AR' }
}

const payloadTitle = (payload: Record<string, unknown>): string | null =>
  typeof payload.title === 'string' && payload.title ? payload.title : null

const titleHasFactoryMarker = (title: string, marker: string): boolean =>
  title === marker || title.startsWith(`${marker} `)

const payloadTeamKey = (payload: Record<string, unknown>): string | null => {
  const team = asRecord(payload.team)
  return typeof team?.key === 'string' && team.key ? team.key : null
}

const assertPayloadInFactoryScope = (
  payload: Record<string, unknown>,
  safety: { titlePrefix: string; teamKey: string },
  context: string,
): void => {
  const title = payloadTitle(payload)
  if (!title || !titleHasFactoryMarker(title, safety.titlePrefix)) {
    throw new Error(`Refusing Linear writeback for ${context}: title must start with ${safety.titlePrefix} boundary`)
  }

  const team = asRecord(payload.team)
  const teamKey = payloadTeamKey(payload)
  if (team && teamKey !== safety.teamKey) {
    throw new Error(`Refusing Linear writeback for ${context}: team key must be ${safety.teamKey}`)
  }
}

const payloadInFactoryScope = (
  payload: Record<string, unknown>,
  safety: { titlePrefix: string; teamKey: string },
): boolean => {
  const title = payloadTitle(payload)
  if (!title || !titleHasFactoryMarker(title, safety.titlePrefix)) return false
  const team = asRecord(payload.team)
  return !team || payloadTeamKey(payload) === safety.teamKey
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
      assertPayloadInFactoryScope(await readIssuePayloadForGuard(mount, issue), safety, issue.key)
      const path = issuePath(issue)
      await mount.writeFile(path, { stateId })
      await confirmWriteback(mount, path, () => adapter.verify(issue, { stateId }))
    },

    async postComment(issue: LinearIssue, body: string): Promise<void> {
      assertPayloadInFactoryScope(await readIssuePayloadForGuard(mount, issue), safety, issue.key)
      const name = linearCommentName(issue, body)
      const path = linearCommentPath(issuePath(issue), name)
      await mount.writeFile(path, linearCommentPayload(issue, body))
      await confirmWriteback(mount, path, () => adapter.verify(issue, { commentName: name }))
    },

    async createIssue(payload: LinearCreateIssuePayload): Promise<{ path: string }> {
      assertPayloadInFactoryScope(payload, safety, 'createIssue payload')
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
