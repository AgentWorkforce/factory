import { FACTORY_STATE_ROLES, type FactoryStateRole } from '../config/schema'

// Roles the factory cannot operate without; humanReview is optional (the
// terminalState falls back to `done` when it is unset).
const REQUIRED_ROLES: readonly FactoryStateRole[] = [
  'readyForAgent',
  'agentImplementing',
  'inPlanning',
  'done',
]

const STATES_INDEX_PATH = '/linear/states/_index.json'
const stateCanonicalPath = (id: string): string => `/linear/states/${id}.json`

// Cap concurrent canonical-record reads so a large states catalog can't flood a
// remote/cloud mount (rate limits, timeouts, fd exhaustion).
const CATALOG_READ_CONCURRENCY = 10
const ISSUE_FALLBACK_SCAN_LIMIT = 150

// Minimal read surface so the resolver is decoupled from the full MountClient
// and trivially testable with an in-memory map.
export interface LinearStateReader {
  readFile(path: string): Promise<{ content: unknown }>
  listTree?(prefix: string): Promise<string[]>
}

type RoleNames = Partial<Record<FactoryStateRole, string>>
type RoleIds = Partial<Record<FactoryStateRole, string>>

export interface ResolveFactoryStatesInput {
  // role -> workflow-state name, workspace-wide default
  states?: RoleNames
  // teamKey -> (role -> workflow-state name), per-team overrides
  statesByTeam?: Record<string, RoleNames>
  // role -> explicit UUID, lowest-precedence fallback
  stateIds?: RoleIds
  // teams the factory subscribes to; resolved up front so required-role gaps
  // fail loudly at startup rather than mid-run.
  teams?: readonly string[]
}

export interface FactoryStateResolution {
  // Forward: the UUID a role maps to for an issue's team (throws if a required
  // role is unresolved). Falls back to the global mapping when the team has no
  // override.
  idFor(teamToken: string | undefined, role: FactoryStateRole): string
  optionalIdFor(teamToken: string | undefined, role: FactoryStateRole): string | undefined
  // Reverse: which role (if any) a state UUID fills. UUIDs are globally unique,
  // so reads/filters need no team context.
  roleOf(stateId: string | undefined): FactoryStateRole | undefined
  isRole(stateId: string | undefined, role: FactoryStateRole): boolean
  // Resolve a workflow-state NAME to its UUID. Lets read paths backfill the
  // state UUID for issues whose synced payload carries only `state.name` (no id)
  // — see relayfile-adapters#205. Returns undefined when the name is unknown.
  idForName(stateName: string | undefined, teamToken?: string): string | undefined
  // True when any team resolves a humanReview state (gates terminalState).
  hasHumanReview(teamToken?: string): boolean
}

// Build a resolution from explicit, already-known UUIDs (no mount reads). Used
// as the back-compat / test path when names aren't configured: the global ids
// apply to every team. Lenient — unresolved roles only throw when used.
export function stateResolutionFromIds(stateIds: RoleIds, stateNames: RoleNames = {}): FactoryStateResolution {
  const roleById = new Map<string, FactoryStateRole>()
  const idByName = new Map<string, string>()
  for (const role of FACTORY_STATE_ROLES) {
    const id = stateIds[role]
    if (id) roleById.set(id, role)
    const name = stateNames[role]
    if (id && name) idByName.set(norm(name), id)
  }
  return {
    idFor(_teamToken, role) {
      const id = stateIds[role]
      if (!id) throw new Error(`No resolved Linear state for role "${role}".`)
      return id
    },
    optionalIdFor(_teamToken, role) {
      return stateIds[role]
    },
    roleOf(stateId) {
      return stateId ? roleById.get(stateId) : undefined
    },
    isRole(stateId, role) {
      return Boolean(stateId) && roleById.get(stateId as string) === role
    },
    idForName(stateName) {
      return stateName ? idByName.get(norm(stateName)) : undefined
    },
    hasHumanReview() {
      return Boolean(stateIds.humanReview)
    },
  }
}

interface StateRecord {
  id: string
  name?: string
  teamKey?: string
  teamName?: string
}

interface TargetStateLookup {
  name: string
  teamToken?: string
}

const norm = (value: string | undefined): string => (value ?? '').trim().toLowerCase()

const asArray = (value: unknown): unknown[] => {
  if (Array.isArray(value)) return value
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    for (const key of ['entries', 'rows', 'items', 'payload']) {
      if (Array.isArray(record[key])) return record[key] as unknown[]
    }
  }
  return []
}

const unwrap = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== 'object') return {}
  const record = value as Record<string, unknown>
  return record.payload && typeof record.payload === 'object'
    ? (record.payload as Record<string, unknown>)
    : record
}

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

const str = (value: unknown): string | undefined => (typeof value === 'string' && value ? value : undefined)

// Lazily load and cache the /linear/states catalog. Only invoked when at least
// one role is configured by name, so explicit-UUID setups never read it.
// The states catalog could not be read at all (e.g. /linear/states unreadable
// under the mount token). Distinct from a *resolution* failure (ambiguous name,
// cross-team, no match) so optional roles can tolerate an absent catalog without
// also swallowing genuine misconfiguration.
class CatalogUnavailableError extends Error {}

const isCatalogUnavailable = (error: unknown): boolean => error instanceof CatalogUnavailableError

class StateCatalog {
  #records: StateRecord[] | undefined
  #isFallback = false

  constructor(
    private readonly reader: LinearStateReader,
    private readonly targetLookups: readonly TargetStateLookup[],
  ) {}

  get isFallback(): boolean {
    return this.#isFallback
  }

  async records(): Promise<StateRecord[]> {
    if (this.#records) return this.#records
    let index: unknown
    try {
      index = (await this.reader.readFile(STATES_INDEX_PATH)).content
    } catch (error) {
      const issueRecords = await this.recordsFromIssues()
      if (issueRecords.length > 0) {
        this.#isFallback = true
        this.#records = issueRecords
        return issueRecords
      }
      throw new CatalogUnavailableError(
        `Cannot resolve Linear state names: ${STATES_INDEX_PATH} is unavailable ` +
        `(${error instanceof Error ? error.message : String(error)}). ` +
        `Deploy the workflow-states resource or pin stateIds (UUIDs) in config.`,
      )
    }
    const ids = asArray(index)
      .map((row) => str((row as Record<string, unknown>)?.id))
      .filter((id): id is string => Boolean(id))
    // Read canonical records in bounded batches — a workspace can have many
    // states, and an unbounded Promise.all would flood a remote/cloud mount.
    const records: StateRecord[] = []
    for (let i = 0; i < ids.length; i += CATALOG_READ_CONCURRENCY) {
      const batch = await Promise.all(
        ids.slice(i, i + CATALOG_READ_CONCURRENCY).map(async (id): Promise<StateRecord> => {
          try {
            const rec = unwrap((await this.reader.readFile(stateCanonicalPath(id))).content)
            return { id, name: str(rec.name), teamKey: str(rec.team_key), teamName: str(rec.team_name) }
          } catch {
            return { id }
          }
        }),
      )
      records.push(...batch)
    }
    this.#records = records
    return records
  }

  async recordsFromIssues(): Promise<StateRecord[]> {
    if (!this.reader.listTree) return []
    let paths: string[]
    try {
      paths = await this.reader.listTree('/linear/issues')
    } catch {
      return []
    }

    const byKey = new Map<string, StateRecord>()
    const foundTargets = new Set<string>()
    const issuePaths = paths.filter((path) =>
      path.endsWith('.json') &&
      !path.includes('/comments/') &&
      !path.startsWith('/linear/issues/by-uuid/'),
    )
    const max = Math.min(issuePaths.length, ISSUE_FALLBACK_SCAN_LIMIT)
    for (let i = 0; i < max; i += CATALOG_READ_CONCURRENCY) {
      if (this.targetLookups.length > 0 && foundTargets.size >= this.targetLookups.length) break
      const batch = await Promise.all(
        issuePaths.slice(i, Math.min(i + CATALOG_READ_CONCURRENCY, max)).map(async (path): Promise<StateRecord | undefined> => {
          try {
            const payload = unwrap((await this.reader.readFile(path)).content)
            const state = asRecord(payload.state)
            const id = str(payload.stateId) ?? str(state?.id)
            const name = str(state?.name)
            if (!id || !name) return undefined
            return {
              id,
              name,
              teamKey: str(asRecord(payload.team)?.key) ?? str(payload.teamKey),
              teamName: str(asRecord(payload.team)?.name) ?? str(payload.teamName),
            }
          } catch {
            return undefined
          }
        }),
      )
      for (const record of batch) {
        if (!record) continue
        byKey.set(`${norm(record.teamKey) || norm(record.teamName)}:${norm(record.name)}:${record.id}`, record)
        for (const target of this.targetLookups) {
          if (targetMatchesRecord(target, record)) foundTargets.add(targetKey(target))
        }
      }
    }
    return [...byKey.values()]
  }

  // Resolve a state NAME to a UUID, optionally scoped to a team token (matched
  // against team_key or team_name). Throws on no match or ambiguity.
  async resolve(name: string, teamToken: string | undefined): Promise<string> {
    const records = await this.records()
    let matches = records.filter((rec) => norm(rec.name) === norm(name))
    if (teamToken) {
      // Restrict to states that belong to this team (or are team-less). Never
      // fall back to another team's state of the same name — that would silently
      // mis-route issues across teams. If nothing remains, it's a hard no-match.
      const teamless = (rec: StateRecord) => !rec.teamKey && !rec.teamName
      const inTeam = (rec: StateRecord) => norm(rec.teamKey) === norm(teamToken) || norm(rec.teamName) === norm(teamToken)
      matches = matches.filter((rec) => inTeam(rec) || teamless(rec))
      const scoped = matches.filter(inTeam)
      if (scoped.length > 0) matches = scoped
    }
    if (matches.length === 0) {
      throw new Error(`No Linear workflow state named "${name}"${teamToken ? ` for team "${teamToken}"` : ''}.`)
    }
    if (matches.length > 1) {
      throw new Error(
        `Linear workflow state "${name}" is ambiguous (${matches.length} matches)` +
        `${teamToken ? ` for team "${teamToken}"` : ''}; scope it with linear.statesByTeam.`,
      )
    }
    return matches[0].id
  }
}

const targetKey = (target: TargetStateLookup): string =>
  `${norm(target.teamToken)}:${norm(target.name)}`

const targetMatchesRecord = (target: TargetStateLookup, record: StateRecord): boolean => {
  if (norm(record.name) !== norm(target.name)) return false
  if (!target.teamToken) return true
  const teamless = !record.teamKey && !record.teamName
  return teamless || norm(record.teamKey) === norm(target.teamToken) || norm(record.teamName) === norm(target.teamToken)
}

// Resolve every configured role for every known team to concrete UUIDs, building
// the forward (team,role)->id map and the reverse id->role map. Throws if a
// required role cannot be resolved for any team.
export async function resolveFactoryStates(
  reader: LinearStateReader,
  input: ResolveFactoryStatesInput,
): Promise<FactoryStateResolution> {
  const globalNames = input.states ?? {}
  const explicitIds = input.stateIds ?? {}
  // Normalize statesByTeam keys up front so per-team lookups are case-insensitive
  // and consistent with how byTeam/teamTokens are keyed (avoids a lowercase
  // team token silently missing an uppercase override, then overwriting it).
  const byTeamNames: Record<string, RoleNames> = {}
  for (const [team, names] of Object.entries(input.statesByTeam ?? {})) {
    byTeamNames[norm(team)] = names
  }

  // Dedupe team tokens by normalized form while keeping the first original
  // casing for resolution + error messages.
  const teamTokenByNorm = new Map<string, string>()
  for (const team of [...Object.keys(input.statesByTeam ?? {}), ...(input.teams ?? [])]) {
    const key = norm(team)
    if (key && !teamTokenByNorm.has(key)) teamTokenByNorm.set(key, team)
  }
  const catalog = new StateCatalog(reader, targetStateLookups(globalNames, byTeamNames, [...teamTokenByNorm.values()]))

  // Resolve one role for one team token, applying precedence:
  // per-team name > global name > explicit UUID. When a name is configured but
  // the states catalog is unavailable (e.g. /linear/states unreadable under the
  // mount token), fall back to the pinned UUID rather than failing startup — the
  // name still feeds the reverse name->id map below for read-side backfill.
  // `tolerant` is for the team-less default pass when subscribed teams exist:
  // each team is resolved authoritatively below, so a team-less ambiguity must
  // not abort startup (addresses the global-default-forces-teamless-lookup P1).
  const resolveRole = async (
    teamToken: string | undefined,
    role: FactoryStateRole,
    tolerant: boolean,
  ): Promise<string | undefined> => {
    const perTeamName = teamToken ? byTeamNames[norm(teamToken)]?.[role] : undefined
    const name = perTeamName ?? globalNames[role]
    if (name) {
      try {
        return await catalog.resolve(name, teamToken)
      } catch (error) {
        // Fall back to the pinned UUID when the catalog can't resolve the name.
        if (explicitIds[role]) return explicitIds[role]
        // Best-effort team-less default pass: another team will fill this role.
        if (tolerant) return undefined
        // An OPTIONAL role (e.g. humanReview) tolerates an absent catalog, or an
        // incomplete issue-derived fallback catalog. A healthy /linear/states
        // catalog still fails on missing/typoed optional names.
        if (!REQUIRED_ROLES.includes(role)) {
          if (isCatalogUnavailable(error)) return undefined
          if (catalog.isFallback && !isAmbiguousStateError(error)) return undefined
        }
        throw error
      }
    }
    return explicitIds[role]
  }

  const byTeam = new Map<string, RoleIds>()

  const resolveAllRoles = async (
    teamToken: string | undefined,
    enforce: boolean,
    tolerant = false,
  ): Promise<RoleIds> => {
    const resolved: RoleIds = {}
    for (const role of FACTORY_STATE_ROLES) {
      const id = await resolveRole(teamToken, role, tolerant)
      if (id) resolved[role] = id
    }
    const missing = REQUIRED_ROLES.filter((role) => !resolved[role])
    if (enforce && missing.length > 0) {
      throw new Error(
        `Linear state resolution incomplete${teamToken ? ` for team "${teamToken}"` : ''}: ` +
        `missing [${missing.join(', ')}]. Set linear.states / linear.statesByTeam (by name) or stateIds (UUIDs).`,
      )
    }
    return resolved
  }

  // The team-less default only needs to satisfy required roles when there's
  // global config meant to fill it; a per-team-only setup leaves it best-effort
  // (each subscribed team is enforced below, and idFor() throws at use if an
  // unconfigured team's issue ever appears).
  //
  // When teams ARE configured, the team-less default pass is tolerant: the
  // factory's default `linear.states` names are global, so resolving them
  // team-lessly would be ambiguous in a multi-team workspace and wrongly fail
  // startup — even though each subscribed team resolves cleanly. Per-team passes
  // (below) are authoritative; the team-less default is only a fallback.
  const hasGlobalConfig = Object.keys(globalNames).length > 0 || Object.keys(explicitIds).length > 0
  const hasTeamScopes = teamTokenByNorm.size > 0
  const defaultIds = await resolveAllRoles(undefined, hasGlobalConfig && !hasTeamScopes, hasTeamScopes)
  for (const [key, team] of teamTokenByNorm) {
    byTeam.set(key, await resolveAllRoles(team, true))
  }

  const idsFor = (teamToken: string | undefined): RoleIds =>
    (teamToken ? byTeam.get(norm(teamToken)) : undefined) ?? defaultIds

  // Reverse map across the default + every team mapping.
  const roleById = new Map<string, FactoryStateRole>()
  for (const ids of [defaultIds, ...byTeam.values()]) {
    for (const role of FACTORY_STATE_ROLES) {
      const id = ids[role]
      if (id) roleById.set(id, role)
    }
  }

  // Reverse name->id map: lets a read path backfill the state UUID for issues
  // whose synced payload only carries `state.name` (relayfile-adapters#205).
  // Built from configured names paired with their resolved UUID (global + team).
  const idByName = new Map<string, string>()
  const recordNames = (names: RoleNames, ids: RoleIds): void => {
    for (const role of FACTORY_STATE_ROLES) {
      const name = names[role]
      const id = ids[role]
      if (name && id) idByName.set(norm(name), id)
    }
  }
  recordNames(globalNames, defaultIds)
  for (const [key, team] of teamTokenByNorm) {
    recordNames(byTeamNames[key] ?? {}, byTeam.get(key) ?? {})
  }

  return {
    idFor(teamToken, role) {
      const id = idsFor(teamToken)[role]
      if (!id) throw new Error(`No resolved Linear state for role "${role}"${teamToken ? ` (team "${teamToken}")` : ''}.`)
      return id
    },
    optionalIdFor(teamToken, role) {
      return idsFor(teamToken)[role]
    },
    roleOf(stateId) {
      return stateId ? roleById.get(stateId) : undefined
    },
    isRole(stateId, role) {
      return Boolean(stateId) && roleById.get(stateId as string) === role
    },
    idForName(stateName) {
      return stateName ? idByName.get(norm(stateName)) : undefined
    },
    hasHumanReview(teamToken) {
      return Boolean(idsFor(teamToken).humanReview)
    },
  }
}

const isAmbiguousStateError = (error: unknown): boolean =>
  /ambiguous/u.test(error instanceof Error ? error.message : String(error))

const targetStateLookups = (
  globalNames: RoleNames,
  byTeamNames: Record<string, RoleNames>,
  teamTokens: readonly string[],
): TargetStateLookup[] => {
  const byKey = new Map<string, TargetStateLookup>()
  const record = (teamToken: string | undefined, names: RoleNames): void => {
    for (const role of FACTORY_STATE_ROLES) {
      const name = names[role]
      if (!name) continue
      const target = { name, teamToken }
      byKey.set(targetKey(target), target)
    }
  }

  if (teamTokens.length === 0) {
    record(undefined, globalNames)
    return [...byKey.values()]
  }

  for (const teamToken of teamTokens) {
    const teamNames = byTeamNames[norm(teamToken)] ?? {}
    for (const role of FACTORY_STATE_ROLES) {
      const name = teamNames[role] ?? globalNames[role]
      if (!name) continue
      const target = { name, teamToken }
      byKey.set(targetKey(target), target)
    }
  }
  return [...byKey.values()]
}
