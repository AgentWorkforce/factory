import { linearCommentPath, linearIssuePath } from '../constants/linear'
import type { MountClient } from '../ports'
import type { Logger } from '../ports/system'
import { assertInFactoryScope, isInFactoryScope } from '../safety/factory-scope'
import type { LinearIssue } from '../types'
import { asRecord, safePathSegment, stableHash, wrappedPayload } from './shared'

export interface LinearStateIds {
  [name: string]: string
}

export interface LinearCommentPayload {
  body: string
  issueId: string
}

export interface MountLinearWritebackConfig {
  stateIds?: LinearStateIds
  safety?: {
    requireTitlePrefix?: string
    requireLabel?: string
    requireTeamKey?: string
  }
  logger?: Pick<Logger, 'warn'>
}

export interface LinearCreateIssuePayload extends Record<string, unknown> {
  id?: string
  identifier?: string
  title?: string
  teamId?: string
  team?: unknown
  labels?: unknown
  source?: unknown
}

const issuePath = (issue: LinearIssue): string =>
  issue.path || linearIssuePath(issue.key, issue.uuid)

export const linearCommentName = (issue: LinearIssue, body: string): string =>
  `${issue.key}__factory-${stableHash(body)}`

const linearCommentPayload = (issue: LinearIssue, body: string): LinearCommentPayload => ({
  body,
  issueId: issue.uuid,
})

const safetyFromConfig = (configOrStateIds?: LinearStateIds | MountLinearWritebackConfig) => {
  const safety = asRecord(asRecord(configOrStateIds)?.safety)
  if (safety) {
    return {
      requireTitlePrefix: typeof safety.requireTitlePrefix === 'string' && safety.requireTitlePrefix
        ? safety.requireTitlePrefix
        : '[factory-e2e]',
      requireLabel: typeof safety.requireLabel === 'string'
        ? safety.requireLabel
        : 'factory',
      requireTeamKey: typeof safety.requireTeamKey === 'string' && safety.requireTeamKey
        ? safety.requireTeamKey
        : 'AR',
    }
  }
  return { requireTitlePrefix: '[factory-e2e]', requireLabel: 'factory', requireTeamKey: 'AR' }
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

interface CachedIssuePayload {
  payload: Record<string, unknown>
  writable: Record<string, unknown>
}

const createIssuePath = (payload: LinearCreateIssuePayload): string => {
  const id = typeof payload.id === 'string' && payload.id ? payload.id : undefined
  if (id) return `/linear/issues/factory-create-${safePathSegment(id)}.json`
  const identifier = typeof payload.identifier === 'string' && payload.identifier ? payload.identifier : undefined
  if (identifier && !looksLikeProviderIssueIdentifier(identifier)) {
    return `/linear/issues/${safePathSegment(identifier)}.json`
  }
  throw new Error('Linear createIssue payload must include a non-provider id/clientId')
}

const looksLikeProviderIssueIdentifier = (value: string): boolean =>
  /^[A-Z][A-Z0-9]*-/u.test(value)

const confirmWriteback = async (
  mount: MountClient,
  path: string,
  verify: () => Promise<boolean>,
  logger: Pick<Logger, 'warn'>,
): Promise<void> => {
  await assertWritebackAcked(mount, path)
  try {
    if (!await verify()) {
      logger.warn?.(`[factory-sdk] Linear writeback read-back verification failed for ${path}; treating getOp ack as success`)
    }
  } catch (error) {
    logger.warn?.(`[factory-sdk] Linear writeback read-back verification errored for ${path}; treating getOp ack as success`, error)
  }
}

const assertWritebackAcked = async (
  mount: MountClient,
  path: string,
): Promise<void> => {
  const confirmation = await mount.confirmWrite(path, { timeoutMs: 90_000 })
  if (confirmation !== 'acked') {
    throw new Error(`Writeback not acked for ${path}: ${confirmation}`)
  }
}

export const MountLinearWriteback = (
  mount: MountClient,
  configOrStateIds?: LinearStateIds | MountLinearWritebackConfig,
) => {
  const safety = safetyFromConfig(configOrStateIds)
  const logger = (asRecord(configOrStateIds)?.logger as Pick<Logger, 'warn'> | undefined) ?? console
  const canonicalByPath = new Map<string, CachedIssuePayload>()

  const seedCanonical = (
    path: string,
    payload: Record<string, unknown>,
  ): CachedIssuePayload | undefined => {
    if (!payloadInFactoryScope(payload, safety)) return undefined
    const canonical = {
      payload: { ...payload },
      writable: createIssueWritePayload(payload),
    }
    canonicalByPath.set(path, canonical)
    return canonical
  }

  const rawIssuePayload = (issue: LinearIssue): Record<string, unknown> =>
    wrappedPayload(issue.raw)

  const canonicalForIssue = async (issue: LinearIssue): Promise<CachedIssuePayload> => {
    const path = issuePath(issue)
    const rawPayload = rawIssuePayload(issue)
    let canonical = canonicalByPath.get(path)
    if (payloadInFactoryScope(rawPayload, safety)) {
      canonical = seedCanonical(path, rawPayload)
    } else if (!canonical) {
      throw new Error(`Refusing Linear writeback for ${issue.key}: missing title-bearing canonical issue payload`)
    }
    if (!canonical) {
      throw new Error(`Refusing Linear writeback for ${issue.key}: missing title-bearing canonical issue payload`)
    }

    const livePayload = await readIssuePayloadForGuard(mount, issue)
    if (payloadInFactoryScope(livePayload, safety)) {
      seedCanonical(path, livePayload)
      return canonical
    }

    if (isStateOnlyDraft(livePayload)) {
      const cached = canonicalByPath.get(path)
      if (cached) return cached
      throw new Error(`Refusing Linear writeback for ${issue.key}: missing title-bearing canonical issue payload`)
    }

    assertInFactoryScope(scopeIssueFromPayload(livePayload, issue.key), safety)
    throw new Error(`Refusing Linear writeback for ${issue.key}: missing title-bearing canonical issue payload`)
  }

  const updateCanonicalState = (
    path: string,
    issue: LinearIssue,
    canonical: CachedIssuePayload,
    stateId: string,
  ): void => {
    const payload = { ...canonical.payload, stateId }
    canonicalByPath.set(path, {
      payload,
      writable: { ...canonical.writable, stateId },
    })
    issue.stateId = stateId
    const rawPayload = asRecord(issue.raw.payload)
    issue.raw = rawPayload
      ? { ...issue.raw, payload: { ...rawPayload, stateId } }
      : { ...issue.raw, stateId }
  }

  const adapter = {
    async setState(issue: LinearIssue, stateId: string): Promise<void> {
      const path = issuePath(issue)
      const canonical = await canonicalForIssue(issue)
      assertInFactoryScope(scopeIssueFromPayload(canonical.payload, issue.key), safety)
      await mount.writeFile(path, {
        ...canonical.writable,
        stateId,
      }, { guarded: true })
      updateCanonicalState(path, issue, canonical, stateId)
      await confirmWriteback(mount, path, () => verifyStateReadback(mount, issue, stateId), logger)
    },

    async postComment(issue: LinearIssue, body: string): Promise<void> {
      const canonical = await canonicalForIssue(issue)
      assertInFactoryScope(scopeIssueFromPayload(canonical.payload, issue.key), safety)
      const name = linearCommentName(issue, body)
      const path = linearCommentPath(issuePath(issue), name)
      await mount.writeFile(path, linearCommentPayload(issue, body), { guarded: true })
      await confirmWriteback(mount, path, () => verifyCommentReadback(mount, issue, name), logger)
    },

    async createIssue(payload: LinearCreateIssuePayload): Promise<{ path: string }> {
      assertInFactoryScope(scopeIssueFromPayload(payload, 'createIssue payload'), safety, 'createIssue payload')
      const path = createIssuePath(payload)
      seedCanonical(path, payload)
      await mount.writeFile(path, createIssueDraftPayload(payload), { guarded: true })
      await confirmWriteback(mount, path, async () => {
        try {
          const written = wrappedPayload((await mount.readFile(path)).content)
          return payloadInFactoryScope(written, safety)
        } catch {
          return false
        }
      }, logger)
      return { path }
    },

    async verify(
      issue: LinearIssue,
      expect: { stateId?: string; commentName?: string },
    ): Promise<boolean> {
      if (expect.stateId) {
        const path = issuePath(issue)
        await assertWritebackAcked(mount, path)
        return verifyStateReadback(mount, issue, expect.stateId)
      }

      if (expect.commentName) {
        const path = linearCommentPath(issuePath(issue), expect.commentName)
        await assertWritebackAcked(mount, path)
        return verifyCommentReadback(mount, issue, expect.commentName)
      }

      return false
    },
  }

  return adapter
}

const isStateOnlyDraft = (payload: Record<string, unknown>): boolean => {
  const keys = Object.keys(payload)
  return keys.length === 1 && keys[0] === 'stateId' && typeof payload.stateId === 'string'
}

const verifyStateReadback = async (
  mount: MountClient,
  issue: LinearIssue,
  stateId: string,
): Promise<boolean> => {
  try {
    const { content } = await mount.readFile(issuePath(issue))
    const payload = wrappedPayload(content)
    return payload.stateId === stateId
  } catch {
    return false
  }
}

const verifyCommentReadback = async (
  mount: MountClient,
  issue: LinearIssue,
  commentName: string,
): Promise<boolean> => {
  try {
    const { content } = await mount.readFile(linearCommentPath(issuePath(issue), commentName))
    const payload = wrappedPayload(content)
    return payload.issueId === issue.uuid || payload.issue_id === issue.uuid
  } catch {
    return false
  }
}

const createIssueWritePayload = (payload: LinearCreateIssuePayload): Record<string, unknown> => {
  const writable: Record<string, unknown> = {}
  for (const key of [
    'title',
    'teamId',
    'stateId',
    'description',
    'priority',
    'assigneeId',
    'labelIds',
    'labels',
    'source',
    'parentId',
    'projectId',
    'estimate',
  ]) {
    const value = payload[key]
    if (value !== undefined) writable[key] = value
  }

  const teamId = typeof payload.teamId === 'string'
    ? payload.teamId
    : typeof asRecord(payload.team)?.id === 'string'
      ? asRecord(payload.team)?.id
      : undefined
  if (teamId) writable.teamId = teamId

  return writable
}

// createIssue drafts (e.g. GitHub mirrors) only carry a team key, no teamId. The
// shared writable strips that, leaving the draft teamless. Re-attach the team
// object for creates only — existing issues already have their team assigned, so
// setState/postComment must not re-send it.
const createIssueDraftPayload = (payload: LinearCreateIssuePayload): Record<string, unknown> => {
  const writable = createIssueWritePayload(payload)
  if (writable.teamId === undefined && asRecord(payload.team)) {
    writable.team = payload.team
  }
  return writable
}

const scopeIssueFromPayload = (payload: Record<string, unknown>, key: string) => ({
  key,
  title: typeof payload.title === 'string' ? payload.title : '',
  team: typeof asRecord(payload.team)?.key === 'string' ? asRecord(payload.team)?.key as string : undefined,
  raw: { payload },
})
