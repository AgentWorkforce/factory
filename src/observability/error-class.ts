/**
 * The one allowlist for error identifiers that cross a telemetry, operator or
 * public boundary.
 *
 * An error's `message` is dependency-controlled free text: it routinely
 * carries provider prose, filesystem paths, URLs and query strings that may
 * embed credentials. Its *class name* is code-controlled and carries none of
 * that — but only if the reader refuses anything that does not look like a
 * class name, because `name` is a writable property and a persisted record can
 * be written by an older or hostile producer.
 *
 * Anything that fails the pattern collapses to `Error`, which still tells a
 * reader "this failed" without letting the failure choose what gets published.
 */
export const TELEMETRY_ERROR_CLASS_PATTERN = /^[A-Za-z][A-Za-z0-9]{0,63}(?:Error|Exception)$/u

/** What an unrecognised class name collapses to. */
export const TELEMETRY_ERROR_CLASS_FALLBACK = 'Error'

/** True when `value` is a class name the allowlist admits verbatim. */
export function isTelemetryErrorClassName(value: unknown): value is string {
  return typeof value === 'string' && TELEMETRY_ERROR_CLASS_PATTERN.test(value)
}

/** The allowlisted class name of a thrown value. */
export function telemetryErrorClass(error: unknown): string {
  const name = error instanceof Error ? error.name : ''
  return isTelemetryErrorClassName(name) ? name : TELEMETRY_ERROR_CLASS_FALLBACK
}

/**
 * The allowlisted class name of an already-persisted record's class field.
 *
 * Used when re-publishing a stored status: the producer wrote a string, and
 * this side has to decide whether that string may cross the boundary.
 */
export function telemetryErrorClassName(value: unknown): string {
  return isTelemetryErrorClassName(value) ? value : TELEMETRY_ERROR_CLASS_FALLBACK
}
