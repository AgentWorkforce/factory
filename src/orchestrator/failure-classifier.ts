export type FailureRecord = {
  errorKind: string
  message: string
  occurredAtMs: number
}

export type FailureClassification = 'transient' | 'structural'

/**
 * Classifies repeated failures conservatively: structural failures require at
 * least two records with one unanimous error kind. Any conflicting error kind
 * keeps the classification transient.
 */
export function classifyFailure(attempts: readonly FailureRecord[]): FailureClassification {
  if (attempts.length < 2) return 'transient'

  const errorKind = attempts[0].errorKind
  return attempts.every((attempt) => attempt.errorKind === errorKind)
    ? 'structural'
    : 'transient'
}
