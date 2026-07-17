/**
 * factory-feature-guardian handler.
 *
 * Hourly cron tick:
 *   1. Read .agentworkforce/features/manifest.yaml
 *   2. Load cycle progress from workspace memory
 *   3. Pick the next unchecked feature (criticality, tier, then name)
 *   4. Generate a concise check via ctx.llm
 *   5. Post it to Slack with optional team mentions
 *   6. Persist progress, resetting after a complete manifest cycle
 */
import { input } from '@agentworkforce/delivery'
import { defineAgent, type WorkforceCtx } from '@agentworkforce/runtime'
import { slackClient } from '@relayfile/relay-helpers'

type Criticality = 'critical' | 'hot' | 'standard'

interface Feature {
  id: string
  name: string
  cli?: string
  api?: string
  desc: string
  location: string
  tier: number
  criticality: Criticality
}

interface MutableFeature {
  id?: string
  name?: string
  cli?: string
  api?: string
  description?: string
  location?: string
  verifyTier?: number
  criticality?: Criticality
}

interface ProgressState {
  kind: 'factory-feature-guardian:progress'
  version: 1
  checkedIds: string[]
  cycleStartedAt: string
  totalFeatures: number
}

const MANIFEST_RELPATH = '.agentworkforce/features/manifest.yaml'
const PROGRESS_TAG = 'factory-feature-guardian:progress'

async function loadFeatures(ctx: WorkforceCtx): Promise<Feature[]> {
  const absPath = `${ctx.sandbox.cwd}/${MANIFEST_RELPATH}`
  return parseManifestFeatures(await ctx.sandbox.readFile(absPath))
}

/**
 * Parse only the checked-in manifest schema. Keeping this parser local avoids
 * making the published Factory package depend on a YAML library solely for an
 * Agent Workforce deployment artifact. The parser rejects incomplete entries
 * and duplicate IDs instead of silently scheduling malformed checks.
 */
export function parseManifestFeatures(raw: string): Feature[] {
  const features: Feature[] = []
  const seenIds = new Set<string>()
  let categoryCriticality: Criticality | undefined
  let current: MutableFeature | undefined

  const finishCurrent = (): void => {
    if (!current) return
    const feature = validateFeature(current)
    if (seenIds.has(feature.id)) {
      throw new Error(`Duplicate feature id in manifest: ${feature.id}`)
    }
    seenIds.add(feature.id)
    features.push(feature)
    current = undefined
  }

  for (const line of raw.split(/\r?\n/u)) {
    const categoryMatch = /^  [a-z][a-z0-9-]+:\s*$/u.exec(line)
    if (categoryMatch) {
      finishCurrent()
      categoryCriticality = undefined
      continue
    }

    const criticalityMatch = /^    criticality:\s*(critical|hot|standard)\s*$/u.exec(line)
    if (criticalityMatch) {
      finishCurrent()
      categoryCriticality = criticalityMatch[1] as Criticality
      continue
    }

    const idMatch = /^      - id:\s*(.+?)\s*$/u.exec(line)
    if (idMatch) {
      finishCurrent()
      if (!categoryCriticality) {
        throw new Error(`Feature ${decodeScalar(idMatch[1])} appears before category criticality`)
      }
      current = {
        id: decodeScalar(idMatch[1]),
        criticality: categoryCriticality,
      }
      continue
    }

    if (!current) continue
    const fieldMatch = /^        (name|cli|api|description|location|verify_tier):\s*(.*?)\s*$/u.exec(line)
    if (!fieldMatch) continue
    const [, key, rawValue] = fieldMatch
    const value = decodeScalar(rawValue)
    if (key === 'name') current.name = value
    else if (key === 'cli') current.cli = value
    else if (key === 'api') current.api = value
    else if (key === 'description') current.description = value
    else if (key === 'location') current.location = value
    else if (key === 'verify_tier') current.verifyTier = Number(value)
  }

  finishCurrent()
  if (features.length === 0) throw new Error('Manifest parsed but contained no features')
  validateCatalogSummary(raw, features)
  return features
}

function validateCatalogSummary(raw: string, features: Feature[]): void {
  const categoriesSection = /^categories:\s*$/mu.exec(raw)
  if (!categoriesSection) throw new Error('Manifest is missing categories')
  const categoryCount = [
    ...raw.slice(categoriesSection.index + categoriesSection[0].length)
      .matchAll(/^  [a-z][a-z0-9-]+:\s*$/gmu),
  ].length
  const declaredCategories = manifestInteger(raw, /^  category_count:\s*(\d+)\s*$/mu, 'category_count')
  const declaredFeatures = manifestInteger(raw, /^  feature_count:\s*(\d+)\s*$/mu, 'feature_count')
  if (declaredCategories !== categoryCount || declaredFeatures !== features.length) {
    throw new Error(
      `Manifest catalog mismatch: declared ${declaredCategories} categories/${declaredFeatures} features, parsed ${categoryCount}/${features.length}`,
    )
  }

  for (let tier = 1; tier <= 6; tier += 1) {
    const expression = new RegExp(`^    ${tier}:\\s*(\\d+)\\s*$`, 'mu')
    const declared = manifestInteger(raw, expression, `tier ${tier}`)
    const actual = features.filter((feature) => feature.tier === tier).length
    if (declared !== actual) {
      throw new Error(`Manifest catalog tier ${tier} mismatch: declared ${declared}, parsed ${actual}`)
    }
  }
}

function manifestInteger(raw: string, expression: RegExp, label: string): number {
  const match = expression.exec(raw)
  const value = Number(match?.[1])
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Manifest catalog is missing a valid ${label}`)
  }
  return value
}

function decodeScalar(value: string | undefined): string {
  const trimmed = value?.trim() ?? ''
  if (trimmed.length < 2) return trimmed
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replaceAll("''", "'")
  }
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    try {
      const parsed = JSON.parse(trimmed) as unknown
      return typeof parsed === 'string' ? parsed : trimmed
    } catch {
      return trimmed.slice(1, -1)
    }
  }
  return trimmed
}

function validateFeature(inputFeature: MutableFeature): Feature {
  if (!inputFeature.id || !inputFeature.name || !inputFeature.description || !inputFeature.location) {
    throw new Error(`Incomplete manifest feature: ${JSON.stringify(inputFeature)}`)
  }
  if (!inputFeature.cli && !inputFeature.api) {
    throw new Error(`Manifest feature ${inputFeature.id} has neither cli nor api`)
  }
  const verifyTier = inputFeature.verifyTier
  if (typeof verifyTier !== 'number' || !Number.isInteger(verifyTier) || verifyTier < 1 || verifyTier > 6) {
    throw new Error(`Manifest feature ${inputFeature.id} has invalid verify_tier`)
  }
  if (!inputFeature.criticality) {
    throw new Error(`Manifest feature ${inputFeature.id} has no category criticality`)
  }
  return {
    id: inputFeature.id,
    name: inputFeature.name,
    cli: inputFeature.cli,
    api: inputFeature.api,
    desc: inputFeature.description,
    location: inputFeature.location,
    tier: verifyTier,
    criticality: inputFeature.criticality,
  }
}

async function loadProgress(ctx: WorkforceCtx): Promise<ProgressState | null> {
  const items = await ctx.memory.recall('factory feature guardian cycle progress', {
    tags: [PROGRESS_TAG],
    scope: 'workspace',
    limit: 5,
  })
  const newestFirst = [...items].sort((left, right) =>
    (right.createdAt ?? '').localeCompare(left.createdAt ?? ''))
  for (const item of newestFirst) {
    try {
      const parsed = JSON.parse(item.content) as unknown
      if (isProgressState(parsed)) return parsed
    } catch {
      // Ignore malformed or stale memory rows.
    }
  }
  return null
}

function isProgressState(value: unknown): value is ProgressState {
  if (!isRecord(value)) return false
  return value.kind === PROGRESS_TAG &&
    value.version === 1 &&
    Array.isArray(value.checkedIds) &&
    value.checkedIds.every((entry) => typeof entry === 'string') &&
    typeof value.cycleStartedAt === 'string' &&
    typeof value.totalFeatures === 'number'
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

async function saveProgress(ctx: WorkforceCtx, state: ProgressState): Promise<void> {
  await ctx.memory.save(JSON.stringify(state), {
    tags: [PROGRESS_TAG],
    scope: 'workspace',
    ttlSeconds: 60 * 60 * 24 * 14,
  })
}

function pickNextFeature(features: Feature[], checkedIds: Set<string>): Feature | null {
  const criticalityOrder: Record<Criticality, number> = {
    critical: 0,
    hot: 1,
    standard: 2,
  }
  const ordered = [...features].sort((left, right) => {
    const criticalityDiff = criticalityOrder[left.criticality] - criticalityOrder[right.criticality]
    if (criticalityDiff !== 0) return criticalityDiff
    const tierDiff = left.tier - right.tier
    if (tierDiff !== 0) return tierDiff
    return left.name.localeCompare(right.name)
  })
  return ordered.find((feature) => !checkedIds.has(feature.id)) ?? null
}

function tierLabel(tier: number): string {
  if (tier === 1) return 'installed package/source checkout only'
  if (tier === 2) return 'valid factory.config.json; fixture mode is acceptable'
  if (tier === 3) return 'reachable Linear or GitHub ticket provider'
  if (tier === 4) return 'internal broker or hosted Relay fleet available'
  if (tier === 5) return 'cloud auth and writable Relayfile mount'
  return 'manual check with a live issue or pull request'
}

function featureSurface(feature: Feature): string {
  return [
    feature.cli ? `CLI: \`${feature.cli}\`` : undefined,
    feature.api ? `API/config: \`${feature.api}\`` : undefined,
  ].filter((value): value is string => Boolean(value)).join('\n')
}

async function generateCheckMessage(ctx: WorkforceCtx, feature: Feature): Promise<string> {
  const prompt = [
    'You are the Factory Feature Guardian, a proactive Slack bot for the Agent Workforce team.',
    'Write a brief conversational Slack message (3-5 sentences, no markdown header) asking the team to confirm whether one Factory feature still works as intended.',
    'Name the feature, include its CLI/API/config surface, describe the expected behavior and source path, mention its verification prerequisite, and ask whether implementation, tests, procedures, or docs have drifted.',
    'Preserve stated safety and topology boundaries exactly. In particular, never imply that cross-host active/active control-plane ownership is supported.',
    'End exactly with: "React ✅ if working as expected, 🔧 if something is off, or ❓ if untested."',
    'Keep it casual, precise, and direct. Do not claim that you ran the check.',
    '',
    `Feature: ${feature.name}`,
    featureSurface(feature),
    `Expected behavior: ${feature.desc}`,
    `Source: ${feature.location}`,
    `Verify tier: ${feature.tier} (${tierLabel(feature.tier)})`,
    `Criticality: ${feature.criticality}`,
  ].join('\n')

  try {
    return (await ctx.llm.complete(prompt, { maxTokens: 320 })).trim()
  } catch {
    return fallbackCheckMessage(feature)
  }
}

export function fallbackCheckMessage(feature: Feature): string {
  return [
    `🔍 *Factory Feature Check: ${feature.name}*`,
    '',
    featureSurface(feature),
    `Source: ${feature.location}`,
    '',
    `This should: ${feature.desc}`,
    `Verification prerequisite: tier ${feature.tier} — ${tierLabel(feature.tier)}.`,
    '',
    'Is this working as expected right now? React ✅ if working as expected, 🔧 if something is off, or ❓ if untested.',
  ].join('\n')
}

export default defineAgent({
  schedules: [{ name: 'hourly-check', cron: '0 * * * *', tz: 'America/New_York' }],
  handler: async (ctx, _event) => {
    const channel = input(ctx, 'SLACK_CHANNEL')
    if (!channel) {
      ctx.log('warn', 'factory-feature-guardian.no-channel', {
        reason: 'SLACK_CHANNEL not configured',
      })
      return
    }

    let features: Feature[]
    try {
      features = await loadFeatures(ctx)
    } catch (error) {
      const absPath = `${ctx.sandbox.cwd}/${MANIFEST_RELPATH}`
      ctx.log('error', 'factory-feature-guardian.manifest-load-failed', {
        path: absPath,
        error: String(error),
      })
      await slackClient().post(
        channel,
        `⚠️ *factory-feature-guardian* could not load \`${MANIFEST_RELPATH}\`: \`${String(error)}\``,
      ).catch(() => undefined)
      return
    }

    const progress = await loadProgress(ctx)
    const checkedIds = new Set(
      (progress?.totalFeatures === features.length ? progress.checkedIds : [])
        .filter((id) => features.some((feature) => feature.id === id)),
    )
    const totalFeatures = features.length

    let feature = pickNextFeature(features, checkedIds)
    let cycleStartedAt = progress?.cycleStartedAt ?? new Date().toISOString()
    if (!feature) {
      ctx.log('info', 'factory-feature-guardian.cycle-complete', { total: totalFeatures })
      checkedIds.clear()
      cycleStartedAt = new Date().toISOString()
      feature = pickNextFeature(features, checkedIds)
    }
    if (!feature) return

    const userWill = input(ctx, 'SLACK_USER_WILL')
    const userKhaliq = input(ctx, 'SLACK_USER_KHALIQ')
    const mentions = [
      userWill ? `<@${userWill}>` : undefined,
      userKhaliq ? `<@${userKhaliq}>` : undefined,
    ].filter((value): value is string => Boolean(value)).join(' ')
    const mentionPrefix = mentions ? `${mentions} — ` : ''

    const checkBody = await generateCheckMessage(ctx, feature)
    const ordinal = checkedIds.size + 1
    const remaining = totalFeatures - ordinal
    const progressNote = `_[factory feature check · ${ordinal}/${totalFeatures} · ${remaining} remaining in cycle]_`
    const result = await slackClient().post(
      channel,
      [mentionPrefix + checkBody, '', progressNote].join('\n'),
    )
    if (!result.ts) {
      ctx.log('error', 'factory-feature-guardian.post-failed', {
        channel,
        feature: feature.id,
      })
      return
    }

    ctx.log('info', 'factory-feature-guardian.posted', {
      channel,
      feature: feature.id,
      ts: result.ts,
    })
    checkedIds.add(feature.id)
    await saveProgress(ctx, {
      kind: PROGRESS_TAG,
      version: 1,
      checkedIds: [...checkedIds],
      cycleStartedAt,
      totalFeatures,
    })
  },
})
