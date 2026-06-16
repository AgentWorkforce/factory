import { z } from 'zod'

// The five workflow-state roles the factory drives an issue through. Each is
// configured either by name (config.linear.states.<role>, resolved to a
// workspace UUID at startup) or by explicit UUID (config.stateIds.<role>).
export const FACTORY_STATE_ROLES = [
  'readyForAgent',
  'agentImplementing',
  'inPlanning',
  'done',
  'humanReview',
] as const
export type FactoryStateRole = (typeof FACTORY_STATE_ROLES)[number]

// A mapping from factory roles to a team's workflow-state NAMES (e.g.
// readyForAgent -> "Ready for Agent", or "To Do" for a team that names it
// differently). All optional: unset roles fall back to global/explicit config.
const linearRoleNamesSchema = z.object({
  readyForAgent: z.string().optional(),
  agentImplementing: z.string().optional(),
  inPlanning: z.string().optional(),
  done: z.string().optional(),
  humanReview: z.string().optional(),
}).default({})

const subscriptionSchema = z.object({
  teams: z.array(z.string()).default([]),
  projects: z.array(z.string()).default([]),
  labels: z.array(z.string()).default([]),
  assignees: z.array(z.string()).default([]),
}).default({})

const liveSubscriptionSchema = z.object({
  transport: z.enum(['subscribe-and-poll', 'subscribe', 'poll']).default('subscribe-and-poll'),
  pollIntervalMs: z.number().int().min(50).default(5_000),
  eventLimit: z.number().int().min(1).max(1_000).default(1_000),
  replaySkewMarginMs: z.number().int().min(0).default(60_000),
}).default({})

const dispatchSchema = z.object({
  errorCooldownMs: z.number().int().min(0).default(60_000),
  maxAttempts: z.number().int().min(1).max(5).default(2),
}).default({})

const loopSchema = z.object({
  maxIterations: z.number().int().min(1).max(5).default(3),
  maxConsecutiveFailures: z.number().int().min(1).max(5).default(3),
  heartbeatPath: z.string().min(1).default('/tmp/factory-run/factory-loop-heartbeat.json'),
  registryPath: z.string().min(1).default('/tmp/factory-run/factory-loop-registry.json'),
  heartbeatStaleMs: z.number().int().min(1_000).default(60_000),
}).default({})

const triageSchema = z.object({
  maxImplementers: z.number().int().min(1).max(6).default(2),
}).default({})

const workspaceReposSchema = z.object({
  // Compact, single-source repo config. Most setups only need these: `names`
  // is the label/repo list, and byLabel + clonePaths + subscription.labels are
  // derived from them at parse time (see the transform below).
  //   byLabel[name]   = overrides[name] ?? `${org}/${name}`
  //   clonePaths[repo] = `${cloneRoot}/${repoName}`
  org: z.string().optional(),
  names: z.array(z.string()).optional(),
  overrides: z.record(z.string(), z.string()).default({}),
  // Explicit forms remain supported as an escape hatch and are merged over the
  // derived maps (explicit entries win). byLabel is optional now that it can be
  // derived from `names`.
  byLabel: z.record(z.string(), z.string()).default({}),
  byProject: z.record(z.string(), z.string()).default({}),
  keywordRules: z.array(z.object({ pattern: z.string(), repo: z.string() })).default([]),
  default: z.string().optional(),
  // Legacy/node-local repo checkout inputs accepted by the composed schema.
  // WorkspaceConfig strips them; FactoryConfig uses them to preserve #369.
  cloneRoot: z.string().optional(),
  clonePaths: z.record(z.string(), z.string()).default({}),
})

const modelsSchema = z.object({
  implementer: z.string().optional(),
  reviewer: z.string().optional(),
  triage: z.string().optional(),
  // The PR babysitter defaults to sonnet — it shepherds an already-open PR
  // (CI/conflicts/comments) rather than authoring from scratch, so the
  // mid-tier model is the deliberate default rather than the implementer/
  // reviewer's unset (inherit) behavior.
  babysitter: z.string().default('sonnet'),
}).default({})

const slackSchema = z.object({
  channel: z.string(),
  style: z.literal('threaded-summarized').default('threaded-summarized'),
  botUserId: z.string().default('U0B2596R7EZ'),
  staleAfterMs: z.number().int().min(1_000).default(10 * 60_000),
}).optional()

const babysitterSchema = z.object({
  enabled: z.boolean().default(false),
}).default({})

const linearSchema = z.object({
  states: linearRoleNamesSchema,
  statesByTeam: z.record(z.string(), linearRoleNamesSchema).default({}),
}).default({})

const stateIdsSchema = z.object({
  readyForAgent: z.string().optional(),
  agentImplementing: z.string().optional(),
  done: z.string().optional(),
  inPlanning: z.string().optional(),
  humanReview: z.string().optional(),
}).default({})

const safetySchema = z.object({
  requireTitlePrefix: z.string().min(1).default('[factory-e2e]'),
  requireLabel: z.string().default('factory'),
  requireTeamKey: z.string().min(1).default('AR'),
}).default({})

const WorkspaceConfigObjectSchema = z.object({
  // Optional. When omitted, the CLI derives the workspace from the cloud session
  // via `resolveActiveWorkspace()` (returns the active `relayfileWorkspaceId`),
  // falling back to the SDK's built-in default. Set it only to pin a non-active
  // workspace. See resolveFactoryWorkspace() in relayfile-cloud-mount-client.ts.
  workspaceId: z.string().optional(),
  subscription: subscriptionSchema,
  liveSubscription: liveSubscriptionSchema,
  dispatch: dispatchSchema,
  loop: loopSchema,
  triage: triageSchema,
  repos: workspaceReposSchema,
  batchSize: z.number().int().min(1).max(5).default(5),
  models: modelsSchema,
  slack: slackSchema,
  mergePolicy: z.enum(['never', 'on-green-with-review']).default('never'),
  // Opt-in PR babysitter. When enabled, a sonnet agent is spawned once the
  // implementer's PR opens (webhook-driven, see the orchestrator) and shepherds
  // it — addressing review comments, resolving conflicts, and fixing CI — until
  // it is green, then transitions the issue to the `human-review` terminal state
  // instead of jumping straight to `done`. Default off preserves the legacy
  // PR-open -> done behavior.
  babysitter: babysitterSchema,
  // Which Linear state an issue lands in once the agents finish and the PR is
  // open. `human-review` parks it for operator review (Done is reserved for the
  // actual merge); `done` is the legacy behavior. Only honored when the
  // `humanReview` role resolves to a state — otherwise it falls back to `done`.
  terminalState: z.enum(['done', 'human-review']).default('human-review'),
  // Dynamic, workspace-agnostic Linear configuration. Nothing about state names
  // or UUIDs is hardcoded — customers map the factory's semantic roles to
  // whatever their teams call those states, and the names are resolved to UUIDs
  // at startup against /linear/states (see resolveFactoryStates).
  //
  // `states` is the workspace-wide default mapping; `statesByTeam.<TEAM>`
  // overrides individual roles for teams that name their states differently
  // (resolution per issue uses the issue's team, falling back to `states`).
  linear: linearSchema,
  // Explicit workflow-state UUIDs. Lowest-precedence fallback / single-team
  // escape hatch for setups that prefer pinning ids over name resolution; any
  // role resolved by name (per-team or global) takes precedence. Populated in
  // place once resolution runs, so the orchestrator always sees concrete UUIDs.
  stateIds: stateIdsSchema,
  safety: safetySchema,
})

const NodeConfigObjectSchema = z.object({
  workspaceId: z.string().optional(),
  capabilities: z.array(z.string()).default([]),
  cloneRoot: z.string().optional(),
  clonePaths: z.record(z.string(), z.string()).default({}),
  dryRun: z.boolean().default(false),
  factoryLoopHeartbeatPath: z.string().min(1).optional(),
  factoryLoopRegistryPath: z.string().min(1).optional(),
})

const FactoryConfigObjectSchema = WorkspaceConfigObjectSchema.merge(NodeConfigObjectSchema)

export const WorkspaceConfigSchema = WorkspaceConfigObjectSchema.transform((cfg) => normalizeWorkspaceConfig(cfg))
export const NodeConfigSchema = NodeConfigObjectSchema.transform((cfg) => normalizeNodeConfig(cfg))
export const FactoryConfigSchema = FactoryConfigObjectSchema.transform((cfg) => normalizeFactoryConfig(cfg))

function normalizeWorkspaceConfig(cfg: z.infer<typeof WorkspaceConfigObjectSchema>) {
  const resolved = resolveRepos(cfg.repos, cfg.repos.cloneRoot)
  const labels = resolveSubscriptionLabels(cfg.subscription.labels, cfg.repos.names ?? [])

  const {
    cloneRoot: _legacyCloneRoot,
    clonePaths: _legacyClonePaths,
    ...repos
  } = cfg.repos

  return {
    ...cfg,
    subscription: { ...cfg.subscription, labels },
    repos: {
      ...repos,
      byLabel: resolved.byLabel,
      byProject: resolved.byProject,
      keywordRules: resolved.keywordRules,
      ...(resolved.defaultRepo !== undefined ? { default: resolved.defaultRepo } : {}),
    },
  }
}

function normalizeNodeConfig(cfg: z.infer<typeof NodeConfigObjectSchema>) {
  return {
    ...cfg,
    factoryLoopHeartbeatPath: cfg.factoryLoopHeartbeatPath,
    factoryLoopRegistryPath: cfg.factoryLoopRegistryPath,
  }
}

function normalizeFactoryConfig(cfg: z.infer<typeof FactoryConfigObjectSchema>) {
  const cloneRoot = cfg.cloneRoot ?? cfg.repos.cloneRoot
  const explicitClonePaths = {
    ...cfg.repos.clonePaths,
    ...cfg.clonePaths,
  }
  const resolved = resolveRepos(cfg.repos, cloneRoot, explicitClonePaths)
  const labels = resolveSubscriptionLabels(cfg.subscription.labels, cfg.repos.names ?? [])
  const heartbeatPath = cfg.factoryLoopHeartbeatPath ?? cfg.loop.heartbeatPath
  const registryPath = cfg.factoryLoopRegistryPath ?? cfg.loop.registryPath

  return {
    ...cfg,
    cloneRoot,
    clonePaths: resolved.clonePaths,
    factoryLoopHeartbeatPath: heartbeatPath,
    factoryLoopRegistryPath: registryPath,
    subscription: { ...cfg.subscription, labels },
    loop: {
      ...cfg.loop,
      heartbeatPath,
      registryPath,
    },
    repos: {
      byLabel: resolved.byLabel,
      byProject: resolved.byProject,
      keywordRules: resolved.keywordRules,
      clonePaths: resolved.clonePaths,
      ...(resolved.defaultRepo !== undefined ? { default: resolved.defaultRepo } : {}),
    },
  }
}

function resolveRepos(
  repos: z.infer<typeof workspaceReposSchema>,
  cloneRoot?: string,
  explicitClonePaths: Record<string, string> = repos.clonePaths,
) {
  const { org, names, overrides, byLabel, byProject, keywordRules, default: defaultRepo } = repos
  const repoNames = names ?? []

  // Derive byLabel from `names` (label === repo name): overrides[name] wins,
  // else `${org}/${name}` when an org is set, else the bare name. Explicit
  // byLabel entries are merged last so they always win.
  const derivedByLabel: Record<string, string> = {}
  for (const name of repoNames) {
    derivedByLabel[name] = overrides[name] ?? (org ? `${org}/${name}` : name)
  }
  const resolvedByLabel = { ...derivedByLabel, ...byLabel }

  // Derive clonePaths as `${cloneRoot}/${repoName}` for every routed repo.
  // Explicit clonePaths entries win.
  const derivedClonePaths: Record<string, string> = {}
  if (cloneRoot) {
    const root = cloneRoot.replace(/\/+$/u, '')
    for (const repo of Object.values(resolvedByLabel)) {
      const repoName = repo.includes('/') ? repo.slice(repo.lastIndexOf('/') + 1) : repo
      derivedClonePaths[repo] = `${root}/${repoName}`
    }
  }
  const resolvedClonePaths = { ...derivedClonePaths, ...explicitClonePaths }

  return {
    byLabel: resolvedByLabel,
    byProject,
    keywordRules,
    clonePaths: resolvedClonePaths,
    defaultRepo,
  }
}

function resolveSubscriptionLabels(labels: string[], repoNames: string[]): string[] {
  return labels.length > 0 ? labels : repoNames
}

export interface LoadedFactoryConfig {
  workspaceConfig: WorkspaceConfig
  nodeConfig: NodeConfig
  factoryConfig: FactoryConfig
}

export function loadFactoryConfig(input: unknown): LoadedFactoryConfig {
  const record = asConfigRecord(input)
  const hasSplit = Object.prototype.hasOwnProperty.call(record, 'workspaceConfig') ||
    Object.prototype.hasOwnProperty.call(record, 'nodeConfig')

  if (hasSplit) {
    if (!Object.prototype.hasOwnProperty.call(record, 'workspaceConfig')) {
      throw new Error('split factory config requires workspaceConfig')
    }
    if (!Object.prototype.hasOwnProperty.call(record, 'nodeConfig')) {
      throw new Error('split factory config requires nodeConfig')
    }
    return normalizeLoadedConfig(combineSplitConfigInput(record.workspaceConfig, record.nodeConfig))
  }

  return normalizeLoadedConfig(record.factoryConfig ?? input)
}

function normalizeLoadedConfig(input: unknown): LoadedFactoryConfig {
  const factoryConfig = FactoryConfigSchema.parse(input)
  const workspaceConfig = WorkspaceConfigSchema.parse(factoryConfig)
  const nodeConfig = NodeConfigSchema.parse({
    workspaceId: factoryConfig.workspaceId,
    capabilities: factoryConfig.capabilities,
    cloneRoot: factoryConfig.cloneRoot,
    clonePaths: factoryConfig.clonePaths,
    dryRun: factoryConfig.dryRun,
    factoryLoopHeartbeatPath: factoryConfig.loop.heartbeatPath,
    factoryLoopRegistryPath: factoryConfig.loop.registryPath,
  })

  return { workspaceConfig, nodeConfig, factoryConfig }
}

function combineSplitConfigInput(workspaceInput: unknown, nodeInput: unknown): Record<string, unknown> {
  const workspace = asConfigRecord(workspaceInput)
  const node = asConfigRecord(nodeInput)
  const workspaceRepos = asOptionalConfigRecord(workspace.repos)
  assertCompatibleWorkspaceIds(workspace.workspaceId, node.workspaceId)

  return {
    ...workspace,
    ...node,
    repos: {
      ...workspaceRepos,
      cloneRoot: node.cloneRoot ?? workspaceRepos.cloneRoot,
      clonePaths: node.clonePaths ?? workspaceRepos.clonePaths,
    },
  }
}

function assertCompatibleWorkspaceIds(workspaceId: unknown, nodeWorkspaceId: unknown): void {
  if (
    typeof workspaceId === 'string' &&
    typeof nodeWorkspaceId === 'string' &&
    workspaceId !== nodeWorkspaceId
  ) {
    throw new Error(`split factory config workspaceId mismatch: workspaceConfig=${workspaceId} nodeConfig=${nodeWorkspaceId}`)
  }
}

function asConfigRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>
  throw new Error('factory config must be a JSON object')
}

function asOptionalConfigRecord(value: unknown): Record<string, unknown> {
  if (value === undefined) return {}
  return asConfigRecord(value)
}

export type WorkspaceConfig = z.infer<typeof WorkspaceConfigSchema>
export type NodeConfig = z.infer<typeof NodeConfigSchema>
export type FactoryConfig = z.infer<typeof FactoryConfigSchema>
