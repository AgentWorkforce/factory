import { estimateCostUsd, hasModelPricing, roundUsd } from './pricing'

export const COST_LEDGER_ROLES = [
  'implementer',
  'reviewer',
  'babysitter',
  'triage',
  'workflow',
] as const

export type CostLedgerRole = (typeof COST_LEDGER_ROLES)[number]

/** Matches the closed cloud-event bound for one role's model breakdown. */
export const MAX_RUN_COST_MODELS_PER_ROLE = 32
export const RUN_COST_OTHER_MODELS_ID = 'factory/other-models'

export interface CostLedgerRecord {
  runId: string
  role: CostLedgerRole
  model: string
  inputTokens: number | null
  outputTokens: number | null
  usd: number | null
}

export type CostLedgerRecordInput = Omit<CostLedgerRecord, 'usd'>

export interface CostModelBreakdown {
  model: string
  inputTokens: number | null
  outputTokens: number | null
  usd: number | null
}

export interface CostRoleBreakdown {
  role: CostLedgerRole
  inputTokens: number | null
  outputTokens: number | null
  usd: number | null
  byModel: CostModelBreakdown[]
}

export interface RunCostTotal {
  runId: string
  inputTokens: number | null
  outputTokens: number | null
  usd: number | null
  byRole: CostRoleBreakdown[]
}

export interface UnpricedModelCostRecord extends CostLedgerRecord {
  usd: null
}

export type UnpricedModelListener = (record: UnpricedModelCostRecord) => void

export interface CostLedgerRecordOptions {
  /**
   * Deterministic logical identity for cumulative runtime updates. Factory uses
   * run id + agent name, so a later usage report replaces rather than doubles
   * the same spawned agent.
   */
  entryId?: string
}

/**
 * In-memory accounting seam for one Factory process. Durable run records retain
 * the completion aggregate; this ledger keeps the detailed live attribution.
 */
export class CostLedger {
  readonly #records = new Map<string, CostLedgerRecord>()
  readonly #unpricedListeners = new Set<UnpricedModelListener>()
  readonly #reportedUnpricedModels = new Set<string>()
  #nextEntry = 0

  record(input: CostLedgerRecordInput, options: CostLedgerRecordOptions = {}): CostLedgerRecord {
    const normalized = normalizeRecordInput(input)
    const unpriced = !hasModelPricing(normalized.model)
    const usd = normalized.inputTokens === null || normalized.outputTokens === null
      ? null
      : estimateCostUsd(normalized.model, normalized.inputTokens, normalized.outputTokens)
    const record: CostLedgerRecord = { ...normalized, usd }
    const entryId = options.entryId ?? `entry:${this.#nextEntry++}`
    this.#records.set(entryId, record)

    if (unpriced) this.#emitUnpriced(record as UnpricedModelCostRecord)
    return structuredClone(record)
  }

  hasEntry(entryId: string): boolean {
    return this.#records.has(entryId)
  }

  getRunRecords(runId: string): CostLedgerRecord[] {
    return [...this.#records.values()]
      .filter((record) => record.runId === runId)
      .map((record) => structuredClone(record))
  }

  getRunTotal(runId: string): RunCostTotal {
    const byRole = this.getRunByRole(runId)
    return {
      runId,
      inputTokens: nullableSum(byRole.map((entry) => entry.inputTokens)),
      outputTokens: nullableSum(byRole.map((entry) => entry.outputTokens)),
      usd: nullableUsdSum(byRole.map((entry) => entry.usd)),
      byRole,
    }
  }

  getRunByRole(runId: string): CostRoleBreakdown[] {
    const records = this.getRunRecords(runId)
    const roles = new Map<CostLedgerRole, CostLedgerRecord[]>()
    for (const record of records) {
      const current = roles.get(record.role) ?? []
      current.push(record)
      roles.set(record.role, current)
    }

    return [...roles].map(([role, roleRecords]) => ({
      role,
      inputTokens: nullableSum(roleRecords.map((record) => record.inputTokens)),
      outputTokens: nullableSum(roleRecords.map((record) => record.outputTokens)),
      usd: nullableUsdSum(roleRecords.map((record) => record.usd)),
      byModel: modelBreakdowns(roleRecords),
    })).sort((left, right) => roleIndex(left.role) - roleIndex(right.role))
  }

  onUnpricedModel(listener: UnpricedModelListener): () => void {
    this.#unpricedListeners.add(listener)
    return () => this.#unpricedListeners.delete(listener)
  }

  #emitUnpriced(record: UnpricedModelCostRecord): void {
    const key = `${record.runId}\0${record.model}`
    if (this.#reportedUnpricedModels.has(key)) return
    this.#reportedUnpricedModels.add(key)
    for (const listener of this.#unpricedListeners) {
      try {
        listener(structuredClone(record))
      } catch {
        // Cost accounting is an observability aid, never a run dependency.
      }
    }
  }
}

const modelBreakdowns = (records: CostLedgerRecord[]): CostModelBreakdown[] => {
  const models = new Map<string, CostLedgerRecord[]>()
  for (const record of records) {
    const current = models.get(record.model) ?? []
    current.push(record)
    models.set(record.model, current)
  }
  return [...models].map(([model, modelRecords]) => ({
    model,
    inputTokens: nullableSum(modelRecords.map((record) => record.inputTokens)),
    outputTokens: nullableSum(modelRecords.map((record) => record.outputTokens)),
    usd: nullableUsdSum(modelRecords.map((record) => record.usd)),
  })).sort((left, right) => compareModelIds(left.model, right.model))
}

/**
 * Produces the deterministic, privacy-safe shape persisted and emitted when a
 * run completes. The detailed ledger remains available through
 * getRunRecords(); excess model buckets are folded into one aggregate instead
 * of making the bounded observability event invalid.
 */
export const boundedRunCostTotal = (total: RunCostTotal): RunCostTotal => ({
  ...structuredClone(total),
  byRole: total.byRole
    .map((role) => ({ ...structuredClone(role), byModel: boundedModels(role.byModel) }))
    .sort((left, right) => roleIndex(left.role) - roleIndex(right.role)),
})

const boundedModels = (models: CostModelBreakdown[]): CostModelBreakdown[] => {
  const sorted = structuredClone(models).sort((left, right) => compareModelIds(left.model, right.model))
  if (sorted.length <= MAX_RUN_COST_MODELS_PER_ROLE) return sorted

  const candidates = sorted.filter((entry) => entry.model !== RUN_COST_OTHER_MODELS_ID)
  const overflow = [
    ...sorted.filter((entry) => entry.model === RUN_COST_OTHER_MODELS_ID),
    ...candidates.slice(MAX_RUN_COST_MODELS_PER_ROLE - 1),
  ]
  return [
    ...candidates.slice(0, MAX_RUN_COST_MODELS_PER_ROLE - 1),
    {
      model: RUN_COST_OTHER_MODELS_ID,
      inputTokens: nullableSum(overflow.map((entry) => entry.inputTokens)),
      outputTokens: nullableSum(overflow.map((entry) => entry.outputTokens)),
      usd: nullableUsdSum(overflow.map((entry) => entry.usd)),
    },
  ]
}

const roleIndex = (role: CostLedgerRole): number => COST_LEDGER_ROLES.indexOf(role)

const compareModelIds = (left: string, right: string): number => left < right ? -1 : left > right ? 1 : 0

const normalizeRecordInput = (input: CostLedgerRecordInput): CostLedgerRecordInput => ({
  runId: boundedIdentifier(input.runId, 'unknown-run'),
  role: COST_LEDGER_ROLES.includes(input.role) ? input.role : 'triage',
  model: boundedModelId(input.model),
  inputTokens: tokenCount(input.inputTokens),
  outputTokens: tokenCount(input.outputTokens),
})

const boundedIdentifier = (value: string, fallback: string): string => {
  const normalized = typeof value === 'string' ? value.trim() : ''
  return (normalized || fallback).slice(0, 256)
}

const boundedModelId = (value: string): string => {
  const normalized = boundedIdentifier(value, 'unknown')
  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,119}$/u.test(normalized)
    ? normalized
    : 'unknown'
}

const tokenCount = (value: number | null): number | null =>
  value !== null && Number.isSafeInteger(value) && value >= 0 ? value : null

const nullableSum = (values: Array<number | null>): number | null => {
  const known = values.filter((value): value is number => value !== null)
  if (known.length === 0) return null
  return known.reduce((sum, value) =>
    sum >= Number.MAX_SAFE_INTEGER - value ? Number.MAX_SAFE_INTEGER : sum + value, 0)
}

const nullableUsdSum = (values: Array<number | null>): number | null => {
  const total = nullableSum(values)
  return total === null ? null : roundUsd(total)
}
