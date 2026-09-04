/**
 * Canonical Software Garden naming for issue lifecycle labels, readiness
 * labels, and title prefixes, plus the legacy Factory names that stay
 * readable during the product rename transition.
 *
 * The product was renamed from Factory to Software Garden. New writes and
 * documented defaults use the garden names below. Read/discovery paths accept
 * the legacy factory names as aliases so existing configs and in-flight
 * issues carrying `factory-ready`, `factory:in-progress`, and
 * `factory:human-review` remain discoverable and recoverable while operators
 * migrate. The alias surface is deliberately limited to these paired names;
 * custom configured labels are never given aliases.
 *
 * Intentionally NOT renamed (breaking API or storage migrations): the npm
 * package and `factory` CLI binary, `FACTORY_*` environment variables,
 * `factory/` implementation branch prefixes, `factory-notion-*` claim channel
 * and source-marker strings, `factory:dispatch:v1:` agent identity stamps,
 * `.factory/` state paths, `AgentWorkforce/factory` repository coordinates,
 * and telemetry/event keys.
 */

export const GARDEN_AUTOMATION_LABEL = 'garden'
export const LEGACY_FACTORY_AUTOMATION_LABEL = 'factory'

export const GARDEN_READY_LABEL = 'garden-ready'
export const LEGACY_FACTORY_READY_LABEL = 'factory-ready'

export const GARDEN_IN_PROGRESS_LABEL = 'garden:in-progress'
export const LEGACY_FACTORY_IN_PROGRESS_LABEL = 'factory:in-progress'

export const GARDEN_HUMAN_REVIEW_LABEL = 'garden:human-review'
export const LEGACY_FACTORY_HUMAN_REVIEW_LABEL = 'factory:human-review'

export const GARDEN_TITLE_PREFIX = '[garden]'
export const LEGACY_FACTORY_TITLE_PREFIX = '[factory]'

export const GARDEN_E2E_TITLE_PREFIX = '[garden-e2e]'
export const LEGACY_FACTORY_E2E_TITLE_PREFIX = '[factory-e2e]'

export const GARDEN_SKIP_BABYSITTER_LABEL = 'garden:skip-babysitter'
export const LEGACY_FACTORY_SKIP_BABYSITTER_LABEL = 'factory:skip-babysitter'

/** Every lifecycle status label name (canonical and legacy), lowercased. */
export const GARDEN_LIFECYCLE_LABEL_NAMES: readonly string[] = [
  GARDEN_IN_PROGRESS_LABEL,
  LEGACY_FACTORY_IN_PROGRESS_LABEL,
  GARDEN_HUMAN_REVIEW_LABEL,
  LEGACY_FACTORY_HUMAN_REVIEW_LABEL,
]

const normalizeLabel = (label: string): string => label.trim().toLowerCase()

/**
 * The canonical label and its legacy alias (or just the label itself when it
 * is not one of the renamed pairs). Symmetric: a legacy-configured value also
 * accepts its garden successor, so operators can adopt garden labels before
 * or after flipping the config.
 */
export const gardenLabelAliases = (label: string): readonly string[] => {
  const normalized = normalizeLabel(label)
  if (!normalized) return []
  if (normalized === GARDEN_AUTOMATION_LABEL || normalized === LEGACY_FACTORY_AUTOMATION_LABEL) {
    return [GARDEN_AUTOMATION_LABEL, LEGACY_FACTORY_AUTOMATION_LABEL]
  }
  if (normalized === GARDEN_READY_LABEL || normalized === LEGACY_FACTORY_READY_LABEL) {
    return [GARDEN_READY_LABEL, LEGACY_FACTORY_READY_LABEL]
  }
  if (normalized === GARDEN_IN_PROGRESS_LABEL || normalized === LEGACY_FACTORY_IN_PROGRESS_LABEL) {
    return [GARDEN_IN_PROGRESS_LABEL, LEGACY_FACTORY_IN_PROGRESS_LABEL]
  }
  if (normalized === GARDEN_HUMAN_REVIEW_LABEL || normalized === LEGACY_FACTORY_HUMAN_REVIEW_LABEL) {
    return [GARDEN_HUMAN_REVIEW_LABEL, LEGACY_FACTORY_HUMAN_REVIEW_LABEL]
  }
  if (normalized === GARDEN_SKIP_BABYSITTER_LABEL || normalized === LEGACY_FACTORY_SKIP_BABYSITTER_LABEL) {
    return [GARDEN_SKIP_BABYSITTER_LABEL, LEGACY_FACTORY_SKIP_BABYSITTER_LABEL]
  }
  return [normalized]
}

/**
 * Whether any label in `labels` is the configured label or its rename alias.
 *
 * Every entry is normalized on the way in, whatever collection it arrives in.
 * A `Set` used to be probed with `has` directly, so a provider-cased label such
 * as `Factory` matched through the array form and missed through the set form —
 * dual-read behavior must not depend on the shape of the collection.
 */
export const matchesGardenLabelAlias = (
  labels: ReadonlySet<string> | readonly string[],
  label: string,
): boolean => {
  const aliases = gardenLabelAliases(label)
  if (aliases.length === 0) return false
  for (const entry of labels) {
    if (aliases.includes(normalizeLabel(entry))) return true
  }
  return false
}

/** Whether `labels` carries the lifecycle status label, under either name. */
export const hasGardenLifecycleLabel = (
  labels: ReadonlySet<string>,
  status: 'in-progress' | 'human-review',
): boolean =>
  status === 'human-review'
    ? labels.has(GARDEN_HUMAN_REVIEW_LABEL) || labels.has(LEGACY_FACTORY_HUMAN_REVIEW_LABEL)
    : labels.has(GARDEN_IN_PROGRESS_LABEL) || labels.has(LEGACY_FACTORY_IN_PROGRESS_LABEL)

/**
 * The lifecycle status a label set expresses, under either naming.
 * `human-review` wins when both statuses are present, matching the
 * single-name precedence it replaces.
 */
export const gardenLifecycleStatusFromLabels = (
  labels: ReadonlySet<string>,
): 'in-progress' | 'human-review' | undefined => {
  if (hasGardenLifecycleLabel(labels, 'human-review')) return 'human-review'
  if (hasGardenLifecycleLabel(labels, 'in-progress')) return 'in-progress'
  return undefined
}

/** Whether `name` is any lifecycle status label name, canonical or legacy. */
export const isGardenLifecycleLabelName = (name: string): boolean =>
  GARDEN_LIFECYCLE_LABEL_NAMES.includes(normalizeLabel(name))

const titlePrefixAliases = (prefix: string): readonly string[] => {
  if (prefix === GARDEN_TITLE_PREFIX) return [GARDEN_TITLE_PREFIX, LEGACY_FACTORY_TITLE_PREFIX]
  if (prefix === LEGACY_FACTORY_TITLE_PREFIX) return [LEGACY_FACTORY_TITLE_PREFIX, GARDEN_TITLE_PREFIX]
  if (prefix === GARDEN_E2E_TITLE_PREFIX) return [GARDEN_E2E_TITLE_PREFIX, LEGACY_FACTORY_E2E_TITLE_PREFIX]
  if (prefix === LEGACY_FACTORY_E2E_TITLE_PREFIX) return [LEGACY_FACTORY_E2E_TITLE_PREFIX, GARDEN_E2E_TITLE_PREFIX]
  return [prefix]
}

/** Whether `title` is exactly the prefix or starts with `prefix ` (alias-aware). */
export const hasGardenTitlePrefix = (title: string, prefix: string): boolean =>
  titlePrefixAliases(prefix).some(
    (candidate) => title === candidate || title.startsWith(`${candidate} `),
  )
