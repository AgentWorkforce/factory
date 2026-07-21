import { estimateCostUsd, roundUsd } from './pricing'

export const COST_LEDGER_ROLES = [
  'implementer',
  'reviewer',
  'babysitter',
  'triage',
  'workflow',
] as const

export type CostLedgerRole = (typeof COST_LEDGER_ROLES)[number]

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
    let unpriced = false
    const usd = normalized.inputTokens === null || normalized.outputTokens === null
      ? null
      : estimateCostUsd(normalized.model, normalized.inputTokens, normalized.outputTokens, {
          onUnpricedModel: () => { unpriced = true },
        })
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
    }))
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
  }))
}

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
  return known.length > 0 ? known.reduce((sum, value) => sum + value, 0) : null
}

const nullableUsdSum = (values: Array<number | null>): number | null => {
  const total = nullableSum(values)
  return total === null ? null : roundUsd(total)
}
