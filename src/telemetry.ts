/**
 * Worker-safe telemetry contract surface.
 *
 * Keep this entrypoint narrow: the observability barrel also exports the
 * filesystem-backed outbox and instance identity implementation.
 */
export * from './observability/events.js'
export type {
  FactoryEventReporter,
  FactoryEventReportResult,
} from './ports/observability.js'
