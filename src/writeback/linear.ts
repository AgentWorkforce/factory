import { linearByIdPath, linearByUuidPath, linearCommentPath, linearIssuePath } from '../constants/linear'
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
  readbackConfirmAttempts?: number
  readbackConfirmDelayMs?: number
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

const hasGuardFields = (payload: Record<string, unknown>): boolean =>
  typeof payload.title === 'string' || Array.isArray(payload.labels) || asRecord(payload.team) !== undefined

const readIssuePayloadForGuard = async (
  mount: MountClient,
  issue: LinearIssue,
): Promise<Record<string, unknown>> => {
  // The primary <key>__<uuid>.json may be a change-event STUB (no title/labels/
  // team — the sparse-sync case); fall back to the canonical by-id/by-uuid
  // records so the factory-scope guard sees the real fields and doesn't refuse a
  // legitimately-[factory] issue.
  const candidates = [
    issuePath(issue),
    ...(issue.key ? [linearByIdPath(issue.key)] : []),
    ...(issue.uuid ? [linearByUuidPath(issue.uuid)] : []),
  ]
  let primaryPayload: Record<string, unknown> | undefined
  let lastError: unknown
  for (const path of candidates) {
    try {
      const payload = wrappedPayload((await mount.readFile(path)).content)
      if (primaryPayload === undefined) {
        primaryPayload = payload
      }
      if (hasGuardFields(payload)) {
        return payload
      }
    } catch (error) {
      lastError = error
    }
  }
  // No record carried guard fields. Preserve prior behavior: return the primary
  // payload (the scope guard then decides) rather than failing the read outright.
  if (primaryPayload !== undefined) {
    return primaryPayload
  }
  throw new Error(
    `Refusing Linear writeback for ${issue.key}: unable to read guard fields` +
    (lastError instanceof Error ? ` (${lastError.message})` : ''),
  )
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

const READBACK_CONFIRM_ATTEMPTS = 30
const READBACK_CONFIRM_DELAY_MS = 1000
const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const confirmWriteback = async (
  mount: MountClient,
  path: string,
  verify: () => Promise<boolean>,
  logger: Pick<Logger, 'warn'>,
  options: { attempts: number; delayMs: number },
): Promise<void> => {
  await assertWritebackAcked(mount, path)
  // getOp can return a FAKED success on a busy/wedged mount ("workspace write
  // path is busy"), so the read-back is the source of truth. Retry to absorb
  // eventual-consistency lag; if it never confirms, the write did NOT land —
  // throw instead of silently faking success (which previously left issues
  // un-advanced while the factory believed they had advanced).
  for (let attempt = 0; attempt < options.attempts; attempt += 1) {
    let confirmed = false
    try {
      confirmed = await verify()
    } catch {
      confirmed = false
    }
    if (confirmed) {
      return
    }
    if (attempt < options.attempts - 1) {
      logger.warn?.(`[factory-sdk] Linear writeback read-back for ${path} not yet confirmed (attempt ${attempt + 1}/${options.attempts}); retrying`)
      await delay(options.delayMs)
    }
  }
  throw new Error(`[factory-sdk] Linear writeback for ${path} acked but the read-back never confirmed it landed; the write did not propagate`)
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

function positiveInteger(value: unknown, fallback: number): number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 ? value : fallback
}

export const MountLinearWriteback = (
  mount: MountClient,
  configOrStateIds?: LinearStateIds | MountLinearWritebackConfig,
) => {
  const safety = safetyFromConfig(configOrStateIds)
  const logger = (asRecord(configOrStateIds)?.logger as Pick<Logger, 'warn'> | undefined) ?? console
  const readbackConfirm = {
    attempts: positiveInteger(asRecord(configOrStateIds)?.readbackConfirmAttempts, READBACK_CONFIRM_ATTEMPTS),
    delayMs: positiveInteger(asRecord(configOrStateIds)?.readbackConfirmDelayMs, READBACK_CONFIRM_DELAY_MS),
  }
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
    async setState(issue: LinearIssue, stateId: string): Promise<{ claimToken: string } | void> {
      const path = issuePath(issue)
      const canonical = await canonicalForIssue(issue)
      assertInFactoryScope(scopeIssueFromPayload(canonical.payload, issue.key), safety)
      const receipt = await mount.writeFile(path, {
        ...canonical.writable,
        stateId,
      }, { guarded: true })
      updateCanonicalState(path, issue, canonical, stateId)
      await confirmWriteback(mount, path, () => verifyStateReadback(mount, issue, stateId), logger, readbackConfirm)
      return receipt?.targetRevision
        ? { claimToken: receipt.targetRevision }
        : undefined
    },

    async compareAndSetState(
      issue: LinearIssue,
      expectedStateId: string,
      claimToken: string,
      stateId: string,
    ): Promise<'applied' | 'superseded' | 'unproven'> {
      const path = issuePath(issue)
      const canonical = await canonicalForIssue(issue)
      assertInFactoryScope(scopeIssueFromPayload(canonical.payload, issue.key), safety)
      const current = await mount.readFile(path)
      const currentPayload = wrappedPayload(current.content)
      if (currentPayload.stateId !== expectedStateId) return 'superseded'
      if (current.revision === undefined) return 'unproven'
      // Matching only the effective state is insufficient: another actor may
      // have restored the same state after this dispatch claim. The exact
      // target revision returned by the original write identifies ownership.
      if (current.revision !== claimToken) return 'unproven'
      // A revision only protects the exact mounted resource. If that resource
      // is a sparse/state-only projection, Factory cannot prove that a full
      // rewrite would preserve concurrent provider fields, so fail closed.
      if (!payloadInFactoryScope(currentPayload, safety)) return 'unproven'
      const currentCanonical: CachedIssuePayload = {
        payload: { ...currentPayload },
        writable: createIssueWritePayload(currentPayload),
      }
      try {
        await mount.writeFile(path, {
          ...currentCanonical.writable,
          stateId,
        }, { guarded: true, baseRevision: claimToken })
      } catch (error) {
        if (isRevisionConflict(error)) {
          // A conflict invalidates the immutable claim token. Re-read only to
          // distinguish a real state transition (`superseded`) from a same-
          // state edit whose ownership is now ambiguous (`unproven`). Never
          // retry with the newer revision: it could belong to another claim
          // with the same effective state.
          try {
            const latest = wrappedPayload((await mount.readFile(path)).content)
            return latest.stateId === expectedStateId ? 'unproven' : 'superseded'
          } catch {
            return 'unproven'
          }
        }
        throw error
      }
      updateCanonicalState(path, issue, currentCanonical, stateId)
      await confirmWriteback(mount, path, () => verifyStateReadback(mount, issue, stateId), logger, readbackConfirm)
      return 'applied'
    },

    async postComment(issue: LinearIssue, body: string): Promise<void> {
      const canonical = await canonicalForIssue(issue)
      assertInFactoryScope(scopeIssueFromPayload(canonical.payload, issue.key), safety)
      const name = linearCommentName(issue, body)
      const path = linearCommentPath(issuePath(issue), name)
      await mount.writeFile(path, linearCommentPayload(issue, body), { guarded: true })
      await confirmWriteback(mount, path, () => verifyCommentReadback(mount, issue, name), logger, readbackConfirm)
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
      }, logger, readbackConfirm)
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

const isRevisionConflict = (error: unknown): boolean =>
  Boolean(error && typeof error === 'object' && 'status' in error && error.status === 409)

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
