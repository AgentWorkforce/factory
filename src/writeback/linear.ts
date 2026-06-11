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
  _stateIds?: LinearStateIds,
) => {
  const adapter = {
    async setState(issue: LinearIssue, stateId: string): Promise<void> {
      const path = issuePath(issue)
      await mount.writeFile(path, { stateId })
      await confirmWriteback(mount, path, () => adapter.verify(issue, { stateId }))
    },

    async postComment(issue: LinearIssue, body: string): Promise<void> {
      const name = linearCommentName(issue, body)
      const path = linearCommentPath(issuePath(issue), name)
      await mount.writeFile(path, linearCommentPayload(issue, body))
      await confirmWriteback(mount, path, () => adapter.verify(issue, { commentName: name }))
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
