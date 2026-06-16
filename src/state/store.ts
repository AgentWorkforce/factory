export interface InFlightRecord {
  issueId: string
  [key: string]: unknown
}

export interface ClarificationRecord {
  threadId: string
  issueId: string
  [key: string]: unknown
}

export interface StateStore {
  getInFlight(issueId: string): Promise<InFlightRecord | null>
  putInFlight(record: InFlightRecord): Promise<void>
  deleteInFlight(issueId: string): Promise<void>
  listInFlight(): Promise<InFlightRecord[]>
  getWaitingClarification(threadId: string): Promise<ClarificationRecord | null>
  putWaitingClarification(record: ClarificationRecord): Promise<void>
  deleteWaitingClarification(threadId: string): Promise<void>
}

export class InMemoryStateStore implements StateStore {
  readonly #inFlight = new Map<string, InFlightRecord>()
  readonly #clarifications = new Map<string, ClarificationRecord>()

  async getInFlight(issueId: string): Promise<InFlightRecord | null> {
    return this.#inFlight.get(issueId) ?? null
  }

  async putInFlight(record: InFlightRecord): Promise<void> {
    this.#inFlight.set(record.issueId, record)
  }

  async deleteInFlight(issueId: string): Promise<void> {
    this.#inFlight.delete(issueId)
  }

  async listInFlight(): Promise<InFlightRecord[]> {
    return [...this.#inFlight.values()]
  }

  async getWaitingClarification(threadId: string): Promise<ClarificationRecord | null> {
    return this.#clarifications.get(threadId) ?? null
  }

  async putWaitingClarification(record: ClarificationRecord): Promise<void> {
    this.#clarifications.set(record.threadId, record)
  }

  async deleteWaitingClarification(threadId: string): Promise<void> {
    this.#clarifications.delete(threadId)
  }
}
