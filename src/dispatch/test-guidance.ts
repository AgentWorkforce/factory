import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import type { TemplateIssue, TemplateRoute } from './templates'

const FEATURE_MANIFEST_PATH = '.agentworkforce/features/manifest.yaml'
const VERIFY_PROCEDURES_PATH = '.agentworkforce/features/verify/procedures.md'

type VerifyTier = 1 | 2 | 3 | 4 | 5 | 6

interface ManifestFeature {
  id: string
  name: string
  location: string
  verifyTier: VerifyTier
  requiresRunningInstance: boolean
}

interface MutableManifestFeature {
  id?: string
  name?: string
  location?: string
  verifyTier?: number
  requiresRunningInstance?: boolean
}

export interface ResolveTestGuidanceInput {
  /** Local checkout containing the repository-specific feature manifest. */
  repoPath?: string
  issue: Pick<TemplateIssue, 'title' | 'description'>
  route?: Pick<TemplateRoute, 'rationale'>
  /** Provider-reported PR paths, when a diff already exists. */
  changedFiles?: readonly string[]
  /** Future-compatible hook for AgentSpec.previewUrl from issue #129. */
  previewUrl?: string
}

/**
 * Read a target repository's feature map and turn only the overlapping slice
 * into task-ready verification instructions. A missing manifest, an unreadable
 * checkout, or no matching feature is intentionally a no-op: dispatch itself
 * must remain available to repositories that have not adopted the map yet.
 */
export async function resolveTestGuidance(
  input: ResolveTestGuidanceInput,
): Promise<string | undefined> {
  if (!input.repoPath) return undefined

  let rawManifest: string
  try {
    rawManifest = readFileSync(join(input.repoPath, FEATURE_MANIFEST_PATH), 'utf8')
  } catch {
    return undefined
  }

  const features = parseManifestFeatures(rawManifest)
  if (features.length === 0) return undefined

  let rawProcedures: string | undefined
  try {
    rawProcedures = readFileSync(join(input.repoPath, VERIFY_PROCEDURES_PATH), 'utf8')
  } catch {
    // Older manifests can still use the documented fallback tier mapping.
  }

  const referencedPaths = issueReferencedPaths(input.issue, input.route)
  const changedFiles = (input.changedFiles ?? []).map(normalizeRepoPath).filter(Boolean)
  const matching = features.filter((feature) => {
    const locations = feature.location.split(',').map(normalizeRepoPath).filter(Boolean)
    return locations.some((location) =>
      changedFiles.some((path) => pathsOverlap(location, path)) ||
      referencedPaths.some((path) => pathsOverlap(location, path)),
    )
  })
  if (matching.length === 0) return undefined

  return renderGuidance(matching, input.previewUrl, rawProcedures)
}

function parseManifestFeatures(raw: string): ManifestFeature[] {
  const features: ManifestFeature[] = []
  let current: MutableManifestFeature | undefined

  const finishCurrent = (): void => {
    if (!current) return
    const verifyTier = current.verifyTier
    if (
      current.id &&
      current.name &&
      current.location &&
      Number.isInteger(verifyTier) &&
      verifyTier !== undefined &&
      verifyTier >= 1 &&
      verifyTier <= 6
    ) {
      features.push({
        id: current.id,
        name: current.name,
        location: current.location,
        verifyTier: verifyTier as VerifyTier,
        requiresRunningInstance: current.requiresRunningInstance ?? false,
      })
    }
    current = undefined
  }

  for (const line of raw.split(/\r?\n/u)) {
    const idMatch = /^      - id:\s*(.+?)\s*$/u.exec(line)
    if (idMatch) {
      finishCurrent()
      current = { id: decodeYamlScalar(idMatch[1]) }
      continue
    }
    if (!current) continue

    const fieldMatch = /^        (name|location|verify_tier|requires_preview|requires_running_instance|running_instance):\s*(.*?)\s*$/u.exec(line)
    if (!fieldMatch) continue
    const [, key, rawValue] = fieldMatch
    const value = decodeYamlScalar(rawValue)
    if (key === 'name') current.name = value
    else if (key === 'location') current.location = value
    else if (key === 'verify_tier') current.verifyTier = Number(value)
    else current.requiresRunningInstance = value.toLowerCase() === 'true'
  }
  finishCurrent()
  return features
}

function decodeYamlScalar(raw: string | undefined): string {
  const value = raw?.trim() ?? ''
  if (value.startsWith("'") && value.endsWith("'")) {
    return value.slice(1, -1).replaceAll("''", "'")
  }
  if (value.startsWith('"') && value.endsWith('"')) {
    try {
      const parsed = JSON.parse(value) as unknown
      if (typeof parsed === 'string') return parsed
    } catch {
      return value.slice(1, -1)
    }
  }
  return value
}

function issueReferencedPaths(
  issue: Pick<TemplateIssue, 'title' | 'description'>,
  route: Pick<TemplateRoute, 'rationale'> | undefined,
): string[] {
  const text = [issue.title, issue.description, route?.rationale].filter(Boolean).join('\n')
  const matches = text.matchAll(/(?:^|[\s('"`])((?:\.{0,2}\/)?(?:[A-Za-z0-9_@.*-]+\/)+[A-Za-z0-9_@.*-]+)(?=$|[\s)'"`,:;])/gmu)
  return [...matches].map((match) => normalizeRepoPath(match[1] ?? '')).filter(Boolean)
}

function normalizeRepoPath(value: string): string {
  return value
    .trim()
    .replace(/[,:;.]+$/gu, '')
    .replace(/^\.\//u, '')
    .replace(/^\/+|\/+$/gu, '')
    .replaceAll('\\', '/')
}

function pathsOverlap(left: string, right: string): boolean {
  return left === right || left.startsWith(`${right}/`) || right.startsWith(`${left}/`)
}

function renderGuidance(
  features: ManifestFeature[],
  previewUrl: string | undefined,
  rawProcedures: string | undefined,
): string {
  const byTier = new Map<VerifyTier, ManifestFeature[]>()
  for (const feature of features) {
    const tierFeatures = byTier.get(feature.verifyTier) ?? []
    tierFeatures.push(feature)
    byTier.set(feature.verifyTier, tierFeatures)
  }

  const lines = [
    'Feature-specific verification guidance:',
    `Source: \`${FEATURE_MANIFEST_PATH}\` and \`${VERIFY_PROCEDURES_PATH}\`.`,
    'The manifest entries overlapping the issue or current PR diff are:',
    ...features.map((feature) =>
      `- ${feature.name} (\`${feature.id}\`) — \`${feature.location}\`, verify tier ${feature.verifyTier}`,
    ),
  ]

  for (const [tier, tierFeatures] of [...byTier.entries()].sort(([left], [right]) => left - right)) {
    lines.push('', `Verify tier ${tier}: ${tierPrerequisite(tier)}`)
    const previewFeatures = tierFeatures.filter((feature) => feature.requiresRunningInstance)
    const commandFeatures = tierFeatures.filter((feature) => !feature.requiresRunningInstance)
    if (commandFeatures.length > 0) {
      const command = tierProcedureSnippet(tier, rawProcedures)
      if (tier <= 2) {
        lines.push('Run the corresponding repository procedure:', '```bash', command, '```')
      } else {
        lines.push(
          'The corresponding repository procedure is shown for reference; do not run it without the prerequisite:',
          '```bash',
          command,
          '```',
          unverifiedTierLine(tier, `this dispatch does not provide ${tierPrerequisite(tier)}`),
        )
      }
    }
    if (previewFeatures.length > 0) {
      if (previewUrl) {
        lines.push(
          `Use your computer-use/browser tool against ${previewUrl} and exercise the covered feature in the running instance. Record what you checked and the observed result.`,
        )
      } else {
        lines.push(unverifiedTierLine(tier, 'a running preview URL is not available'))
      }
    }
  }

  return lines.join('\n')
}

function tierPrerequisite(tier: VerifyTier): string {
  if (tier === 1) return 'source checkout with installed dependencies'
  if (tier === 2) return 'valid repository config (a checked-in fixture is acceptable)'
  if (tier === 3) return 'reachable ticket-provider credentials and a disposable issue'
  if (tier === 4) return 'a reachable fleet backend and capable node'
  if (tier === 5) return 'cloud auth and a writable Relayfile mount'
  return 'manual verification with a live disposable issue or pull request'
}

function tierProcedureSnippet(tier: VerifyTier, rawProcedures: string | undefined): string {
  if (rawProcedures) {
    const heading = new RegExp(`^## Tier ${tier}\\b[^\\n]*\\n([\\s\\S]*?)(?=^## Tier \\d\\b|(?![\\s\\S]))`, 'mu')
    const section = heading.exec(rawProcedures)?.[1]
    const fenced = section ? /```(?:bash|sh|shell|text)?\s*\n([\s\S]*?)```/u.exec(section)?.[1]?.trim() : undefined
    if (fenced) return fenced
  }
  if (tier === 1) return 'npm run build\nnpm test'
  if (tier === 2) {
    return 'npx vitest run src/config/schema.test.ts src/config/local-clone-paths.test.ts src/cli/fleet.test.ts src/safety/factory-scope.test.ts src/triage/triage.test.ts src/dispatch/templates.test.ts src/dispatch/relayflow-registry.test.ts src/linear/state-resolver.test.ts src/orchestrator/factory.test.ts src/orchestrator/reaper.test.ts src/state/file-state-store.test.ts src/node/factory-node.test.ts'
  }
  if (tier === 3) return 'factory canary <DISPOSABLE_ISSUE> --config ./factory.config.json'
  if (tier === 4) return 'factory fleet roster --backend <internal|relay>'
  if (tier === 5) return 'factory run-once --config ./factory.config.json --dry-run'
  return '.agentworkforce/features/critical-paths.md'
}

function unverifiedTierLine(tier: VerifyTier, reason: string): string {
  return `This tier is not runnable in this environment (${reason}). Do not claim it passed; note in your final report that verify tier ${tier} is unverified.`
}
