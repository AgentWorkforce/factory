import type { FactoryCloudEventInputV1 } from '../observability/events'

export interface FactoryEventReportResult {
  delivered: number
  pending: number
  attempts: number
  stoppedReason?: 'empty' | 'deadline' | 'retry-exhausted' | 'rejected' | 'unavailable'
}

export interface FactoryEventReporter {
  /** Persist an event for asynchronous delivery. Implementations must not reject. */
  report(event: FactoryCloudEventInputV1): Promise<void>
  /** Best-effort drain. Implementations must resolve with status rather than reject. */
  flush(options?: { deadlineMs?: number }): Promise<FactoryEventReportResult>
  close?(options?: { deadlineMs?: number }): Promise<FactoryEventReportResult>
}
