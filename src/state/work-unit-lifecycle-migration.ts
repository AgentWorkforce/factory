import { dispatchIssueIdentity } from '../dispatch/work-unit-identity'
import type { DispatchLifecycle } from '../ports/state'
import type { IssueRef } from '../types'

/**
 * Dispatch claims used to be keyed on `${key}:${uuid}:${path}` — the Relayfile
 * sense path, which is a property of the surface an issue arrived through and
 * not of the work itself. Claims are now keyed on the work-unit identity, so
 * every store has to recognise a row written under an older key before it
 * decides that no claim exists and creates a second one.
 *
 * The rekey is provider-neutral and replaces the GitHub-only lifecycle scan.
 * It runs inside whatever serialized mutation the store already holds.
 */

/** The pre-#211 composite key. Retained as a legacy alias, never as authority. */
export const legacyCompositeLifecycleKey = (issue: IssueRef): string =>
  `${issue.key}:${issue.uuid}:${issue.path}`

/**
 * The identity the surface would have produced for itself, ignoring any
 * declared origin — the key a row got if it was written by a build that
 * already keyed on identity but before mirrors retained their origin.
 */
export const surfaceLifecycleKey = (issue: IssueRef): string =>
  dispatchIssueIdentity({ uuid: issue.uuid, key: issue.key, path: issue.path })

/**
 * The surface key, or undefined when the surface ALONE cannot produce an
 * identity — a mirror that resolves only through its origin has nothing left
 * once the origin is stripped. Without this guard such a seed throws here and
 * rejects a claim whose canonical key resolved perfectly well.
 */
const surfaceLifecycleKeyOrUndefined = (issue: IssueRef): string | undefined => {
  try {
    return surfaceLifecycleKey(issue)
  } catch {
    return undefined
  }
}

export type LifecycleMigration =
  /** Nothing to move: either the canonical row exists already, or there is no row at all. */
  | { outcome: 'canonical'; aliases: string[] }
  /** Exactly one legacy row wins and must be moved to the canonical key. */
  | { outcome: 'adopt'; from: string; aliases: string[] }
  /**
   * More than one alias holds a live, unexpired lease. Two live leases mean two
   * dispatchers may each believe they own this work; choosing between them
   * could abandon in-flight work. Refuse and let an operator reconcile.
   */
  | { outcome: 'conflict'; keys: string[] }

const isTerminal = (lifecycle: DispatchLifecycle): boolean =>
  lifecycle.phase === 'complete' || lifecycle.phase === 'abandoned'

/** A lease nobody has let expire, on a row that has not finished. */
const holdsLiveLease = (lifecycle: DispatchLifecycle, nowMs: number): boolean =>
  !isTerminal(lifecycle) && Boolean(lifecycle.lease && lifecycle.lease.leaseUntilMs > nowMs)

export const isMigrationAlias = (lifecycle: DispatchLifecycle): boolean =>
  typeof lifecycle.migrationAliasOf === 'string'

/**
 * Decides how a claim for `canonicalKey` should treat rows persisted under
 * older keys. Pure: the caller performs the moves so it can do so inside its
 * own lock and persistence.
 */
export const planLifecycleMigration = (
  entries: Iterable<[string, DispatchLifecycle]>,
  canonicalKey: string,
  seed: Pick<DispatchLifecycle, 'issue'>,
  nowMs: number,
): LifecycleMigration => {
  const surfaceKey = surfaceLifecycleKeyOrUndefined(seed.issue)
  const legacyKeys = new Set([
    legacyCompositeLifecycleKey(seed.issue),
    ...(surfaceKey ? [surfaceKey] : []),
  ])
  legacyKeys.delete(canonicalKey)

  let canonical: DispatchLifecycle | undefined
  const candidates: Array<[string, DispatchLifecycle]> = []
  for (const [key, lifecycle] of entries) {
    if (key === canonicalKey) {
      canonical = lifecycle
      continue
    }
    // A row already carrying the canonical identity on its own issue covers the
    // GitHub by-id/slugged path aliases the old GitHub-only scan existed for,
    // and does it for every provider.
    const matches = legacyKeys.has(key) || identityOf(lifecycle) === canonicalKey
    if (matches && !isMigrationAlias(lifecycle)) candidates.push([key, lifecycle])
  }

  if (candidates.length === 0) return { outcome: 'canonical', aliases: [] }

  const live = [
    ...(canonical && holdsLiveLease(canonical, nowMs) ? [[canonicalKey, canonical] as const] : []),
    ...candidates.filter(([, lifecycle]) => holdsLiveLease(lifecycle, nowMs)),
  ]
  if (live.length > 1) return { outcome: 'conflict', keys: live.map(([key]) => key).sort() }

  // A canonical row already exists and the ONLY live lease is on a legacy row.
  // Demoting that row would strip a lease its owner may still be renewing, and
  // moving it onto the canonical key would overwrite the row already there.
  // Neither is safe, so refuse rather than pick — a rolling upgrade can produce
  // exactly this state.
  if (canonical && live.length === 1 && live[0]![0] !== canonicalKey) {
    return { outcome: 'conflict', keys: [canonicalKey, live[0]![0]].sort() }
  }

  if (canonical) return { outcome: 'canonical', aliases: candidates.map(([key]) => key) }

  const ranked = [...candidates].sort(compareCandidates(nowMs))
  const [winnerKey] = ranked[0]!
  return {
    outcome: 'adopt',
    from: winnerKey,
    aliases: ranked.slice(1).map(([key]) => key),
  }
}

/** A row's own work-unit identity, or undefined when it cannot produce one. */
const identityOf = (lifecycle: DispatchLifecycle): string | undefined => {
  try {
    return dispatchIssueIdentity(lifecycle.issue)
  } catch {
    // A row with an empty provider identity cannot alias anything; it is not a
    // reason to refuse a claim for a different, well-formed work unit.
    return undefined
  }
}

/**
 * The row holding an unexpired lease wins outright — it is the one a dispatcher
 * may still be working. Otherwise live work outranks queued, queued outranks
 * finished, and the most recently touched row wins within a rank.
 */
const compareCandidates = (nowMs: number) => (
  [leftKey, left]: [string, DispatchLifecycle],
  [rightKey, right]: [string, DispatchLifecycle],
): number =>
  Number(holdsLiveLease(right, nowMs)) - Number(holdsLiveLease(left, nowMs)) ||
  rank(left) - rank(right) ||
  (right.updatedAtMs ?? 0) - (left.updatedAtMs ?? 0) ||
  leftKey.localeCompare(rightKey)

const rank = (lifecycle: DispatchLifecycle): number => {
  if (isTerminal(lifecycle)) return 2
  if (lifecycle.phase === 'queued') return 1
  return 0
}

/**
 * Retention for migration aliases. They are audit evidence, not lookup
 * authority — every caller derives the canonical identity, so dropping one can
 * never reopen dispatch. The state document is about to be Durable-Object
 * backed, where a value caps at 128 KiB and every mutation rewrites it, so the
 * bound is hard rather than advisory.
 */
export const MIGRATION_ALIAS_RETENTION = {
  perCanonical: 2,
  perWorkspace: 32,
  maxAgeMs: 7 * 24 * 60 * 60 * 1_000,
} as const

/**
 * The alias keys to delete, oldest first. A row holding an unexpired lease is
 * never returned, whatever the caps say.
 */
export const prunableMigrationAliases = (
  entries: Iterable<[string, DispatchLifecycle]>,
  nowMs: number,
): string[] => {
  const all = [...entries]
  const byKey = new Map(all)
  // A canonical row that finished — or that was cleared away entirely — has no
  // further use for its audit trail.
  const canonicalIsDone = (canonicalKey: string): boolean => {
    const canonical = byKey.get(canonicalKey)
    return !canonical || isTerminal(canonical)
  }

  const oldestFirst = all
    .filter(([, lifecycle]) => isMigrationAlias(lifecycle))
    .sort(([leftKey, left], [rightKey, right]) =>
      (left.updatedAtMs ?? 0) - (right.updatedAtMs ?? 0) || leftKey.localeCompare(rightKey))

  const doomed = new Set<string>()
  const keptPerCanonical = new Map<string, number>()

  // Newest first, so the per-canonical cap keeps the most recent evidence and
  // the rows it drops are the oldest.
  for (const [key, lifecycle] of [...oldestFirst].reverse()) {
    // Never prunable, whatever the caps say.
    if (lifecycle.lease && lifecycle.lease.leaseUntilMs > nowMs) continue
    const canonicalKey = lifecycle.migrationAliasOf!
    if (
      canonicalIsDone(canonicalKey) ||
      nowMs - (lifecycle.updatedAtMs ?? 0) > MIGRATION_ALIAS_RETENTION.maxAgeMs ||
      (keptPerCanonical.get(canonicalKey) ?? 0) >= MIGRATION_ALIAS_RETENTION.perCanonical
    ) {
      doomed.add(key)
      continue
    }
    keptPerCanonical.set(canonicalKey, (keptPerCanonical.get(canonicalKey) ?? 0) + 1)
  }

  // Then oldest-first until the per-workspace cap is met, skipping leased rows.
  const survivors = oldestFirst.filter(([key]) => !doomed.has(key))
  let kept = survivors.length
  for (const [key, lifecycle] of survivors) {
    if (kept <= MIGRATION_ALIAS_RETENTION.perWorkspace) break
    if (lifecycle.lease && lifecycle.lease.leaseUntilMs > nowMs) continue
    doomed.add(key)
    kept -= 1
  }

  return [...doomed]
}

/** Strips the lease and marks a losing row as audit-only evidence of the rekey. */
export const asMigrationAlias = (
  lifecycle: DispatchLifecycle,
  canonicalKey: string,
  nowMs?: number,
): DispatchLifecycle => {
  const alias: DispatchLifecycle = {
    ...lifecycle,
    migrationAliasOf: canonicalKey,
    updatedAtMs: nowMs ?? lifecycle.updatedAtMs,
  }
  delete alias.lease
  // An alias must not occupy a dispatch slot or look adoptable.
  delete alias.slotHeldSinceAtMs
  return alias
}

/**
 * Rekeys every lifecycle in one workspace onto its work-unit identity.
 *
 * The per-claim plan only fires for the unit being claimed, but startup
 * adoption walks `listDispatchLifecycles` and uses the key it is handed. A row
 * left under a legacy key would be adopted under that key and then missed by
 * every canonical read that follows, so the document is migrated as it loads.
 *
 * This runs where no clock is available, so it never judges lease EXPIRY: a
 * unit with more than one leased row is left exactly as it is. That is strictly
 * more conservative than the claim path, which has a real `nowMs` and refuses
 * precisely. Unrelated reads keep working either way, and the refusal happens
 * where it belongs — when someone tries to claim that unit.
 *
 * Returns whether anything changed.
 */
export const migrateDispatchLifecycleKeys = (
  read: () => Array<[string, DispatchLifecycle]>,
  move: (from: string, to: string) => void,
  demote: (key: string, canonicalKey: string) => void,
): boolean => {
  const groups = new Map<string, Array<[string, DispatchLifecycle]>>()
  for (const [key, lifecycle] of read()) {
    if (isMigrationAlias(lifecycle)) continue
    const canonicalKey = identityOf(lifecycle)
    if (!canonicalKey) continue
    groups.set(canonicalKey, [...(groups.get(canonicalKey) ?? []), [key, lifecycle]])
  }

  let changed = false
  for (const [canonicalKey, rows] of groups) {
    if (rows.length === 1 && rows[0]![0] === canonicalKey) continue
    // Cannot tell an expired lease from a live one here, so any two leased rows
    // are left for the claim path to refuse or resolve.
    const leased = rows.filter(([, lifecycle]) => lifecycle.lease)
    if (leased.length > 1) continue

    const existing = rows.find(([key]) => key === canonicalKey)
    // Never demote a leased row in favour of the canonical one: the lease may
    // still be live and its owner still working. Leave it for the claim path.
    if (leased.length === 1 && existing && leased[0]![0] !== canonicalKey) continue

    const winner = existing ?? [...rows].sort(compareLoadCandidates)[0]!
    if (!existing) {
      move(winner[0], canonicalKey)
      changed = true
    }
    for (const [key] of rows) {
      if (key === winner[0] || key === canonicalKey) continue
      demote(key, canonicalKey)
      changed = true
    }
  }
  return changed
}

/** Leased first, then the same ranking the claim path uses. */
const compareLoadCandidates = (
  [leftKey, left]: [string, DispatchLifecycle],
  [rightKey, right]: [string, DispatchLifecycle],
): number =>
  Number(Boolean(right.lease)) - Number(Boolean(left.lease)) ||
  rank(left) - rank(right) ||
  (right.updatedAtMs ?? 0) - (left.updatedAtMs ?? 0) ||
  leftKey.localeCompare(rightKey)
