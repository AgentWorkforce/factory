import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

type Feature = { id: string; cli?: string; api?: string; location?: string }
type Category = { features?: Feature[] }
type Manifest = {
  version: string
  verification: { document: string; categories: Record<string, string> }
  categories: Record<string, Category>
}

const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url))
const manifest = parse(
  readFileSync(new URL('../../features/manifest.yaml', import.meta.url), 'utf8'),
) as Manifest
const procedures = readFileSync(
  new URL('../../features/verify/procedures.md', import.meta.url),
  'utf8',
)
const packageJson = JSON.parse(
  readFileSync(new URL('../../../package.json', import.meta.url), 'utf8'),
) as { exports: Record<string, unknown> }
const features = Object.values(manifest.categories).flatMap(
  (category) => category.features ?? [],
)

describe('Factory feature manifest contract', () => {
  it('maps every and only every category to a detailed verification procedure', () => {
    expect(manifest.version).toBe('1.1')
    expect(manifest.verification.document).toBe(
      '.agentworkforce/features/verify/procedures.md',
    )
    expect(Object.keys(manifest.verification.categories).sort()).toEqual(
      Object.keys(manifest.categories).sort(),
    )

    for (const category of Object.keys(manifest.categories)) {
      const procedure = manifest.verification.categories[category]
      expect(procedure, `${category} needs a verification procedure`).toMatch(
        /^[a-z0-9-]+$/,
      )
      expect(procedures).toContain(`## ${procedure}`)
    }
  })

  it('has unique feature ids and a concrete public or implementation surface', () => {
    const ids = features.map((feature) => feature.id)
    expect(new Set(ids).size).toBe(ids.length)
    for (const feature of features) {
      expect(feature.id).toMatch(/^[a-z0-9-]+$/)
      expect(Boolean(feature.cli || feature.api || feature.location)).toBe(true)
    }
  })

  it('covers every public Factory CLI leaf', () => {
    const commands = features.flatMap((feature) =>
      feature.cli ? [feature.cli.replaceAll('`', '')] : [],
    )
    const expected = [
      'factory --help',
      'factory --version',
      'factory featuremap check',
      'factory run-once',
      'factory start',
      'factory status',
      'factory loop',
      'factory loop-status',
      'factory kill-loop',
      'factory reap-orphans',
      'factory canary',
      'factory triage',
      'factory dispatch',
      'factory babysit',
      'factory close-probe',
      'factory fleet spawn',
      'factory fleet roster',
      'factory fleet release',
    ]

    for (const command of expected) {
      expect(
        commands.some((documented) => documented.startsWith(command)),
        `${command} is missing`,
      ).toBe(true)
    }
  })

  it('covers every public configuration field', () => {
    const api = features.flatMap((feature) => (feature.api ? [feature.api] : []))
    const expected = [
      'factory.config.json#workspaceId',
      'factory.config.json#issueSource',
      'factory.config.json#batchSize',
      'factory.config.json#subscription.teams',
      'factory.config.json#subscription.projects',
      'factory.config.json#subscription.labels',
      'factory.config.json#subscription.assignees',
      'factory.config.json#liveSubscription.transport',
      'factory.config.json#liveSubscription.pollIntervalMs',
      'factory.config.json#liveSubscription.eventLimit',
      'factory.config.json#liveSubscription.replaySkewMarginMs',
      'factory.config.json#dispatch.errorCooldownMs',
      'factory.config.json#dispatch.maxAttempts',
      'factory.config.json#triage.maxImplementers',
      'factory.config.json#reporting.enabled',
      'factory.config.json#reporting.instanceName',
      'factory.config.json#reporting.outboxPath',
      'factory.config.json#reporting.batchSize',
      'factory.config.json#reporting.requestTimeoutMs',
      'factory.config.json#repos.org',
      'factory.config.json#repos.names',
      'factory.config.json#repos.overrides',
      'factory.config.json#repos.byLabel',
      'factory.config.json#repos.byProject',
      'factory.config.json#repos.keywordRules[].pattern',
      'factory.config.json#repos.keywordRules[].repo',
      'factory.config.json#repos.default',
      'factory.config.json#repos.cloneRoot',
      'factory.config.json#repos.clonePaths',
      'factory.config.json#cloneRoot',
      'factory.config.json#clonePaths',
      'factory.config.json#loop.maxIterations',
      'factory.config.json#loop.maxConsecutiveFailures',
      'factory.config.json#loop.heartbeatPath',
      'factory.config.json#loop.registryPath',
      'factory.config.json#loop.heartbeatStaleMs',
      'factory.config.json#factoryLoopHeartbeatPath',
      'factory.config.json#factoryLoopRegistryPath',
      'factory.config.json#models.implementer',
      'factory.config.json#models.reviewer',
      'factory.config.json#models.triage',
      'factory.config.json#models.babysitter',
      'factory.config.json#agentCapabilities.implementer',
      'factory.config.json#agentCapabilities.reviewer',
      'factory.config.json#agentCapabilities.babysitter',
      'factory.config.json#babysitter.enabled',
      'factory.config.json#mergePolicy',
      'factory.config.json#terminalState',
      'factory.config.json#slack.channel',
      'factory.config.json#slack.style',
      'factory.config.json#slack.botUserId',
      'factory.config.json#slack.stakeholderUserIds',
      'factory.config.json#slack.staleAfterMs',
      'factory.config.json#slack.conversationCoalesceMs',
      'factory.config.json#github.identity',
      'factory.config.json#environments.kubernetes',
      'factory.config.json#linear.states.readyForAgent',
      'factory.config.json#linear.states.agentImplementing',
      'factory.config.json#linear.states.inPlanning',
      'factory.config.json#linear.states.done',
      'factory.config.json#linear.states.humanReview',
      'factory.config.json#linear.statesByTeam',
      'factory.config.json#linear.teamIds',
      'factory.config.json#stateIds.readyForAgent',
      'factory.config.json#stateIds.agentImplementing',
      'factory.config.json#stateIds.inPlanning',
      'factory.config.json#stateIds.done',
      'factory.config.json#stateIds.humanReview',
      'factory.config.json#safety.requireTitlePrefix',
      'factory.config.json#safety.requireLabel',
      'factory.config.json#safety.requireTeamKey',
      'factory.node.json#capabilities',
      'factory.node.json#dryRun',
      'factory.config.json#capabilities',
      'factory.config.json#dryRun',
      'factory.config.json#factoryConfig',
      'factory.config.json#workspaceConfig',
      'factory.config.json#nodeConfig',
      'factory.config.json#fixtureFiles',
    ]

    for (const field of expected) {
      expect(
        api.some((documented) => documented.includes(field)),
        `${field} is missing`,
      ).toBe(true)
    }
  })

  it('covers every published package export', () => {
    const api = features.flatMap((feature) => (feature.api ? [feature.api] : []))
    const expected: Record<string, string> = {
      '.': 'createFactory()',
      './observability': '@agent-relay/factory/observability',
      './telemetry': '@agent-relay/factory/telemetry',
      './testing': '@agent-relay/factory/testing',
      './writeback': 'LinearWriteback / GithubWriteback / SlackWriteback',
      './node': 'createFactoryNodeDefinition()',
      './featuremap': '@agent-relay/factory/featuremap',
      './hosted': '@agent-relay/factory/hosted',
      './verification-stack.schema.json': '@agent-relay/factory/verification-stack.schema.json',
      './kubernetes-environment-stack.schema.json': '@agent-relay/factory/kubernetes-environment-stack.schema.json',
      './environments': '@agent-relay/factory/environments',
    }
    expect(Object.keys(packageJson.exports).sort()).toEqual(Object.keys(expected).sort())
    for (const surface of Object.values(expected)) {
      expect(
        api.some((documented) => documented.includes(surface)),
        `${surface} is missing`,
      ).toBe(true)
    }
  })

  it('covers release-sensitive implementation areas and checked-in automation', () => {
    const locations = features.flatMap((feature) =>
      feature.location ? feature.location.split(',').map((value) => value.trim()) : [],
    )
    const expectedAreas = [
      'src/hosted/',
      'src/environments/',
      'src/observability/',
      'src/orchestrator/dependencies.ts',
      'src/git/agent-worktree.ts',
      'src/mount/relayfile-integration-preflight.ts',
      'src/state/github-lifecycle-identity.ts',
      '.agentworkforce/agents/factory-feature-guardian/',
      '.agentworkforce/agents/factory-maintainability/',
      '.claude/skills/verify-features.md',
      'workflows/verify-features.ts',
      'scripts/verify-packed-e2e.mjs',
    ]
    for (const area of expectedAreas) {
      expect(
        locations.some((location) => location.startsWith(area) || area.startsWith(location)),
        `${area} is missing`,
      ).toBe(true)
      expect(existsSync(`${repositoryRoot}/${area}`), `${area} does not exist`).toBe(true)
    }
  })
})
