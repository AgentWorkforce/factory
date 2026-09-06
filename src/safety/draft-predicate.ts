import type { AllowedDraftPredicateDiagnostics } from '../ports/mount'

export type { AllowedDraftPredicateDiagnostics } from '../ports/mount'

/** Record a fail-closed predicate exit while preserving the boolean contract. */
export const rejectDraft = (
  diagnostics: AllowedDraftPredicateDiagnostics | undefined,
  branch: string,
  detail?: string,
): false => diagnostics?.reject(branch, detail) ?? false
