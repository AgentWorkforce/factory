import { homedir } from 'node:os'
import { isAbsolute, join } from 'node:path'

import { z } from 'zod'
import {
  DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD,
  DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS,
  DEFAULT_FLEET_ROSTER_TIMEOUT_MS,
} from '../fleet/control-plane-circuit'

import { KubernetesEnvironmentConfigSchema } from '../environments/connection-registry.js'

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

/**
 * Deadline for one readiness reconcile sweep (#296).
 *
 * This is a wedge backstop, not a latency target. An unbounded sweep stops the
 * reconcile loop permanently and silently, because the timer re-arms only when
 * the sweep settles — so the deadline exists to guarantee that it settles.
 *
 * DO NOT lower this to a small multiple of `reconcileIntervalMs`. Container
 * disk is ephemeral, so the Relayfile mirror rehydrates on every boot, and #36
 * measured a real cold-mirror reconcile at 3,665,173 ms (61 minutes) in
 * production. A deadline under realistic worst-case hydration converts a slow
 * boot into a crash loop, which is worse than the hang it would be preventing.
 * 90 minutes leaves roughly 47% headroom over that measurement.
 *
 * A stall is *reported* far sooner than it is killed — see
 * `READINESS_RECONCILE_STALL_INTERVALS` — so operators do not wait 90 minutes
 * to learn that a pass is stuck.
 */
export const DEFAULT_READINESS_RECONCILE_TIMEOUT_MS = 90 * 60_000

const liveSubscriptionSchema = z.object({
  transport: z.enum(['subscribe-and-poll', 'subscribe', 'poll']).default('subscribe-and-poll'),
  pollIntervalMs: z.number().int().min(50).default(5_000),
  eventLimit: z.number().int().min(1).max(1_000).default(1_000),
  replaySkewMarginMs: z.number().int().min(0).default(60_000),
  /** Independent source-of-truth sweep; live event watermarks remain a latency optimization. */
  reconcileIntervalMs: z.number().int().min(50).default(60_000),
  /** Bounds one sweep so a hung dependency call cannot stop the loop forever. */
  reconcileTimeoutMs: z.number().int().min(50).max(6 * 60 * 60_000)
    .default(DEFAULT_READINESS_RECONCILE_TIMEOUT_MS),
}).superRefine((value, ctx) => {
  // A deadline below the interval kills every pass that takes longer than one
  // tick, which is most of them on a cold mirror.
  if (value.reconcileTimeoutMs < value.reconcileIntervalMs) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['reconcileTimeoutMs'],
      message: `reconcileTimeoutMs (${value.reconcileTimeoutMs}) must be at least reconcileIntervalMs (${value.reconcileIntervalMs})`,
    })
  }
}).default({})

export const DEFAULT_AGENT_HOLD_TIMEOUT_MS = 4 * 60 * 60_000

const dispatchSchema = z.object({
  errorCooldownMs: z.number().int().min(0).default(60_000),
  maxAttempts: z.number().int().min(1).max(5).default(2),
  // Terminal delivery is not guaranteed: a daemon crash, stalled reviewer, or
  // dropped writeback can otherwise leave an already-idle team allocated
  // forever. This bounds the wall-clock interval from the first successful
  // agent placement until terminal cleanup.
  agentHoldTimeoutMs: z.number().int().min(1).max(7 * 24 * 60 * 60_000)
    .default(DEFAULT_AGENT_HOLD_TIMEOUT_MS),
}).default({})

const fleetHealthSchema = z.object({
  // Roster is read-only, so Factory can safely bound it locally. Mutating
  // spawn/resume calls are never abandoned behind a local timeout.
  rosterTimeoutMs: z.number().int().min(100).max(60_000).default(DEFAULT_FLEET_ROSTER_TIMEOUT_MS),
  failureThreshold: z.number().int().min(1).max(10).default(DEFAULT_FLEET_CONTROL_FAILURE_THRESHOLD),
  resetTimeoutMs: z.number().int().min(1_000).max(15 * 60_000).default(DEFAULT_FLEET_CONTROL_RESET_TIMEOUT_MS),
  // Production launchers can require an explicit, non-project broker state
  // directory so Factory never silently shares an interactive broker.
  requireDedicatedBroker: z.boolean().default(false),
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

// Which agent CLI backs each spawn role. `models.*` picks the model; this picks
// the CLI. Restricted to the two spawn capabilities actually wired into
// capabilityCli (internal-fleet-client.ts) — spawn:opencode / spawn:gemini would
// resolve to an undefined CLI and are intentionally excluded until mapped.
// Defaults preserve today's behavior (codex implements, claude reviews/babysits)
// so existing configs are unaffected unless a role is set explicitly. The map is
// named agentCapabilities rather than `capabilities` because the node config
// already owns a top-level `capabilities` (advertised node capability list).
const spawnCapabilitySchema = z.enum(['spawn:codex', 'spawn:claude'])
const agentCapabilitiesSchema = z.object({
  implementer: spawnCapabilitySchema.default('spawn:codex'),
  reviewer: spawnCapabilitySchema.default('spawn:claude'),
  babysitter: spawnCapabilitySchema.default('spawn:claude'),
}).default({})

const slackSchema = z.object({
  channel: z.string(),
  style: z.literal('threaded-summarized').default('threaded-summarized'),
  botUserId: z.string().default('U0B2596R7EZ'),
  // Slack user IDs to mention when an agent needs a human decision. Keeping
  // this explicit avoids guessing that a Linear assignee name is also a Slack
  // identity, while still making parked questions immediately actionable.
  stakeholderUserIds: z.array(z.string().min(1)).default([]),
  staleAfterMs: z.number().int().min(1_000).default(10 * 60_000),
  conversationCoalesceMs: z.number().int().min(0).max(60_000).default(750),
}).optional()

const babysitterSchema = z.object({
  enabled: z.boolean().default(false),
  // Select the declarative intake/discovery surface. Routed activation is
  // deliberately disabled in src/github/routed-pr-babysitter.ts until the
  // lifecycle design lands, so this value cannot spawn a routed worker yet.
  mode: z.enum(['factory-created', 'routed-open-prs']).default('factory-created'),
  // Discovery excludes candidates carrying an author-controlled stop label.
  excludeLabels: z.array(z.string().trim().min(1)).default(['factory:skip-babysitter']),
  excludePullRequests: z.array(z.string().regex(
    /^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,99})\/[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,99})#[1-9]\d*$/u,
    'expected owner/repo#number',
  )).default([]),
  // Reserved for the activation design; discovery itself never notifies.
  notifyHumans: z.boolean().default(false),
}).default({})

const reportingSchema = z.object({
  enabled: z.boolean().default(true),
  instanceName: z.string().trim().min(1).max(256).optional(),
  outboxPath: z.string().min(1).optional(),
  batchSize: z.number().int().min(1).max(100).default(100),
  requestTimeoutMs: z.number().int().min(100).max(60_000).default(15_000),
}).default({})

const ticketDispatchNotificationSchema = z.discriminatedUnion('surface', [
  z.object({
    surface: z.literal('relay'),
    channel: z.string().trim().min(1),
  }).strict(),
  z.object({
    surface: z.literal('slack'),
    channel: z.string().trim().min(1).optional(),
    dm: z.string().trim().min(1).optional(),
  }).strict(),
  z.object({
    surface: z.literal('telegram'),
    chatId: z.string().trim().min(1),
  }).strict(),
  z.object({
    surface: z.literal('linear'),
    commentOnIssue: z.literal(true),
  }).strict(),
]).superRefine((target, ctx) => {
  if (target.surface === 'slack' && !target.channel && !target.dm) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['channel'],
      message: 'Slack ticket-dispatch notifications require channel and/or dm',
    })
  }
})

const hooksSchema = z.object({
  onTicketDispatch: z.object({
    notify: z.array(ticketDispatchNotificationSchema).min(1),
  }).strict().optional(),
}).strict().optional()

const previewServiceSchema = z.object({
  /** Local HTTP port the repository's development server listens on. */
  port: z.number().int().min(1).max(65_535),
  /** Consecutive node-local ports Factory may allocate for concurrent issues. */
  portSpan: z.number().int().min(1).max(1_000).optional(),
  /** Optional stable tailnet HTTPS port; otherwise Factory allocates one. */
  httpsPort: z.number().int().min(1).max(65_535).optional(),
  /** Foreground command Factory supervises for the issue-lifetime preview. */
  startCommand: z.string().trim().min(1),
}).superRefine((service, ctx) => {
  if (service.port + (service.portSpan ?? 100) - 1 > 65_535) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['portSpan'],
      message: 'preview service port range must end at or below 65535',
    })
  }
})

// Tailscale Serve is deliberately the only initial provider. Unlike Funnel,
// Serve is tailnet-only and keeps the tailnet's grants/ACLs in the request path.
// Making the access mode a literal prevents a config typo from silently
// publishing an unauthenticated URL to Slack or GitHub.
const previewSchema = z.object({
  provider: z.literal('tailscale-serve').default('tailscale-serve'),
  access: z.literal('tailnet').default('tailnet'),
  services: z.record(z.string(), previewServiceSchema).default({}),
  tailscaleBinary: z.string().trim().min(1).default('tailscale'),
  registryPath: z.string().min(1).default('~/.factory/tailscale-previews.json'),
  httpsPortRange: z.tuple([
    z.number().int().min(1).max(65_535),
    z.number().int().min(1).max(65_535),
  ]).default([10_000, 10_999]),
}).superRefine((preview, ctx) => {
  if (preview.httpsPortRange[0] > preview.httpsPortRange[1]) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['httpsPortRange'],
      message: 'preview.httpsPortRange start must be less than or equal to end',
    })
  }
}).optional()

const githubSchema = z.object({
  // Controls the credential identity used for GitHub writes. Exact `app`
  // selects the connected App for both PR publication and issue lifecycle
  // writes. `auto` preserves compatibility: PRs prefer the App, while issue
  // lifecycle writes retain the operator's local `gh` authentication.
  identity: z.enum(['app', 'user', 'auto']).default('auto'),
}).default({})

const verificationSchema = z.object({
  /** Default-on: an auto-merge must have a green live-stack verification verdict. */
  enabled: z.boolean().default(true),
  descriptorPath: z.string().trim().min(1).default('.factory/verification-stack.yaml'),
  maxConcurrentEnvironments: z.number().int().min(1).max(20).default(2),
  maxRunTimeoutMs: z.number().int().min(1_000).max(2 * 60 * 60_000).default(30 * 60_000),
  maxEnvironmentTtlMs: z.number().int().min(1_000).max(24 * 60 * 60_000).default(60 * 60_000),
  maxTeardownTimeoutMs: z.number().int().min(1_000).max(30 * 60_000).default(5 * 60_000),
}).strict().default({})

// The factory owns its workflow-state NAME conventions; consumers (e.g. pear)
// don't hand-configure them. These names let the factory resolve a role from a
// synced record that carries state.name but no state.id (sparse-sync fallback).
// A workspace that names states differently can override via config.
const DEFAULT_LINEAR_STATE_NAMES = {
  readyForAgent: 'Ready for Agent',
  agentImplementing: 'Agent Implementing',
  done: 'Done',
  inPlanning: 'In Planning',
  humanReview: 'In Human Review',
}

const linearSchema = z.object({
  states: linearRoleNamesSchema.default(DEFAULT_LINEAR_STATE_NAMES),
  statesByTeam: z.record(z.string(), linearRoleNamesSchema).default({}),
  teamIds: z.record(z.string(), z.string()).default({}),
}).default({ states: DEFAULT_LINEAR_STATE_NAMES, statesByTeam: {}, teamIds: {} })

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

const environmentsSchema = z.object({
  kubernetes: KubernetesEnvironmentConfigSchema.optional(),
}).strict().default({})

const WorkspaceConfigObjectSchema = z.object({
  // Optional. When omitted, the CLI derives the workspace from the cloud session
  // via `resolveActiveWorkspace()` (returns the active `relayfileWorkspaceId`),
  // falling back to the SDK's built-in default. Set it only to pin a non-active
  // workspace. See resolveFactoryWorkspace() in relayfile-cloud-mount-client.ts.
  workspaceId: z.string().optional(),
  // Optional exact root of this workspace's single Relayfile mirror. When
  // omitted Factory reads Relayfile's existing registration. This is a
  // workspace-scoped escape hatch, never a request to re-home per checkout.
  localMountRoot: z.string()
    .trim()
    .min(1)
    .refine(isAbsolute, 'localMountRoot must be an absolute path')
    .optional(),
  subscription: subscriptionSchema,
  liveSubscription: liveSubscriptionSchema,
  dispatch: dispatchSchema,
  fleetHealth: fleetHealthSchema,
  loop: loopSchema,
  triage: triageSchema,
  repos: workspaceReposSchema,
  batchSize: z.number().int().min(1).max(5).default(1),
  models: modelsSchema,
  agentCapabilities: agentCapabilitiesSchema,
  slack: slackSchema,
  // This selects the discovery + lifecycle-writeback adapter, not every intake
  // surface. Notion deliberately remains a separate intake command that
  // normalizes repository work into GitHub lifecycle issues. Linear remains
  // the default whenever its issue sub-root is connected. When omitted, the
  // orchestrator probes that sub-root once and falls back to GitHub-native
  // issue lifecycle handling only when Linear is absent.
  issueSource: z.enum(['linear', 'github']).optional(),
  mergePolicy: z.enum(['never', 'on-green-with-review']).default('never'),
  // Opt-in PR babysitter. When enabled, a sonnet agent is spawned once the
  // implementer's PR opens (webhook-driven, see the orchestrator) and shepherds
  // it — addressing review comments, resolving conflicts, and fixing CI — until
  // it is green, then transitions the issue to the `human-review` terminal state
  // instead of jumping straight to `done`. Default off preserves the legacy
  // PR-open -> done behavior.
  babysitter: babysitterSchema,
  // Authenticated run progress is a Cloud product feature, not anonymous
  // analytics. It defaults on for real CLI sessions and remains no-op when no
  // Cloud account is available; delivery failure never changes orchestration.
  reporting: reportingSchema,
  // Optional fan-out for the point an agent team has been successfully
  // dispatched. Every configured surface is attempted independently.
  hooks: hooksSchema,
  preview: previewSchema,
  github: githubSchema,
  verification: verificationSchema,
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
  environments: environmentsSchema,
})

const NodeConfigObjectSchema = z.object({
  workspaceId: z.string().optional(),
  capabilities: z.array(z.string()).default([]),
  cloneRoot: z.string().optional(),
  clonePaths: z.record(z.string(), z.string()).default({}),
  dryRun: z.boolean().default(false),
  factoryLoopHeartbeatPath: z.string().min(1).optional(),
  factoryLoopRegistryPath: z.string().min(1).optional(),
  preview: previewSchema,
})

const FactoryConfigObjectSchema = WorkspaceConfigObjectSchema.merge(NodeConfigObjectSchema)

// Shared with routedPrRepos (src/github/routed-pr-babysitter.ts): the last
// step in resolving a repos.names entry to a routable owner/repo slug. A
// resolved value that already has a slash is used as-is; one that doesn't
// gets `org` prefixed as a rescue, regardless of whether it came from an
// explicit byLabel entry, overrides, or the bare name itself -- that rescue
// used to live only in routedPrRepos, so a validator that ran before
// normalizeFactoryConfig's transform (and so never saw org get applied)
// could reject a config routedPrRepos would resolve fine at runtime.
export const resolveRoutedRepo = (configured: string, org: string | undefined): string | undefined => {
  const repo = configured.includes('/') ? configured : (org ? `${org}/${configured}` : undefined)
  return repo && /^[^/]+\/[^/]+$/u.test(repo) ? repo : undefined
}

const requireRoutedBabysitterRepos = (
  cfg: z.infer<typeof WorkspaceConfigObjectSchema>,
  ctx: z.RefinementCtx,
): void => {
  if (cfg.babysitter.mode === 'routed-open-prs' && (cfg.repos.names?.length ?? 0) === 0) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['repos', 'names'],
      message: 'repos.names must contain at least one repository when babysitter.mode is routed-open-prs',
    })
    return
  }
  if (cfg.babysitter.mode === 'routed-open-prs') {
    // Mirror resolveRepos' own derivation (overrides win over a bare
    // org/name, explicit byLabel wins over that) instead of a second,
    // hand-rolled copy of that fallback chain that can drift from it.
    const { byLabel } = resolveRepos(cfg.repos, cfg.repos.cloneRoot)
    const routedRepos = (cfg.repos.names ?? [])
      .map((name) => resolveRoutedRepo(byLabel[name] ?? name, cfg.repos.org))
    if (!routedRepos.some(Boolean)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['repos', 'names'],
        message: 'repos.names must resolve at least one owner/repository route when babysitter.mode is routed-open-prs',
      })
    }
  }
}

export const WorkspaceConfigSchema = WorkspaceConfigObjectSchema
  .superRefine(requireRoutedBabysitterRepos)
  .transform((cfg) => normalizeWorkspaceConfig(cfg))
export const NodeConfigSchema = NodeConfigObjectSchema.transform((cfg) => normalizeNodeConfig(cfg))
export const FactoryConfigSchema = FactoryConfigObjectSchema
  .superRefine(requireRoutedBabysitterRepos)
  .transform((cfg) => normalizeFactoryConfig(cfg))

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
    preview: normalizePreviewConfig(cfg.preview),
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
  const cloneRoot = cfg.cloneRoot === undefined
    ? undefined
    : expandLeadingTilde(cfg.cloneRoot, 'cloneRoot')

  return {
    ...cfg,
    cloneRoot,
    clonePaths: expandClonePaths(cfg.clonePaths),
    factoryLoopHeartbeatPath: cfg.factoryLoopHeartbeatPath,
    factoryLoopRegistryPath: cfg.factoryLoopRegistryPath,
    preview: normalizePreviewConfig(cfg.preview),
  }
}

function normalizeFactoryConfig(cfg: z.infer<typeof FactoryConfigObjectSchema>) {
  const topLevelCloneRoot = cfg.cloneRoot === undefined
    ? undefined
    : expandLeadingTilde(cfg.cloneRoot, 'cloneRoot')
  const legacyCloneRoot = cfg.repos.cloneRoot === undefined
    ? undefined
    : expandLeadingTilde(cfg.repos.cloneRoot, 'repos.cloneRoot')
  const cloneRoot = topLevelCloneRoot ?? legacyCloneRoot
  const explicitClonePaths = {
    ...expandClonePaths(cfg.repos.clonePaths, 'repos.clonePaths'),
    ...expandClonePaths(cfg.clonePaths),
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
    preview: normalizePreviewConfig(cfg.preview),
    repos: {
      ...(cfg.repos.org !== undefined ? { org: cfg.repos.org } : {}),
      ...(cfg.repos.names !== undefined ? { names: cfg.repos.names } : {}),
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
    const root = expandLeadingTilde(cloneRoot, 'cloneRoot').replace(/\/+$/u, '')
    for (const repo of Object.values(resolvedByLabel)) {
      const repoName = repo.includes('/') ? repo.slice(repo.lastIndexOf('/') + 1) : repo
      derivedClonePaths[repo] = `${root}/${repoName}`
    }
  }
  const resolvedClonePaths = {
    ...derivedClonePaths,
    ...expandClonePaths(explicitClonePaths),
  }

  return {
    byLabel: resolvedByLabel,
    byProject,
    keywordRules,
    clonePaths: resolvedClonePaths,
    defaultRepo,
  }
}

function expandClonePaths(
  clonePaths: Record<string, string>,
  field = 'clonePaths',
): Record<string, string> {
  return Object.fromEntries(Object.entries(clonePaths).map(([repo, clonePath]) => [
    repo,
    expandLeadingTilde(clonePath, `${field}[${JSON.stringify(repo)}]`),
  ]))
}

/** Expand the shell-like home shorthand that Node's filesystem APIs do not. */
export function expandLeadingTilde(value: string, field = 'path'): string {
  if (value === '~') return homedir()
  if (value.startsWith('~/')) return join(homedir(), value.slice(2))
  if (value.startsWith('~')) {
    throw new Error(`${field} does not support ~user expansion; use ~ or ~/ instead`)
  }
  return value
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
    preview: factoryConfig.preview,
  })

  return { workspaceConfig, nodeConfig, factoryConfig }
}

function combineSplitConfigInput(workspaceInput: unknown, nodeInput: unknown): Record<string, unknown> {
  const workspace = asConfigRecord(workspaceInput)
  const node = asConfigRecord(nodeInput)
  const workspaceRepos = asOptionalConfigRecord(workspace.repos)
  assertCompatibleWorkspaceIds(workspace.workspaceId, node.workspaceId)

  // Validate tilde syntax in both split halves before node-local values take
  // precedence, so an overridden ~user path is never silently accepted.
  validateClonePathSyntax(workspaceRepos, 'workspaceConfig.repos')
  validateClonePathSyntax(node, 'nodeConfig')

  const workspacePreview = asOptionalConfigRecord(workspace.preview)
  const nodePreview = asOptionalConfigRecord(node.preview)
  const hasPreview = workspace.preview !== undefined || node.preview !== undefined

  return {
    ...workspace,
    ...node,
    ...(hasPreview ? {
      preview: {
        ...workspacePreview,
        ...nodePreview,
        services: {
          ...asOptionalConfigRecord(workspacePreview.services),
          ...asOptionalConfigRecord(nodePreview.services),
        },
      },
    } : {}),
    repos: {
      ...workspaceRepos,
      cloneRoot: node.cloneRoot ?? workspaceRepos.cloneRoot,
      clonePaths: node.clonePaths ?? workspaceRepos.clonePaths,
    },
  }
}

function normalizePreviewConfig<T extends z.infer<typeof previewSchema>>(preview: T): T {
  if (!preview) return preview
  return {
    ...preview,
    registryPath: expandLeadingTilde(preview.registryPath, 'preview.registryPath'),
  } as T
}

function validateClonePathSyntax(input: Record<string, unknown>, field: string): void {
  if (typeof input.cloneRoot === 'string') expandLeadingTilde(input.cloneRoot, `${field}.cloneRoot`)
  const clonePaths = asOptionalConfigRecord(input.clonePaths)
  for (const [repo, clonePath] of Object.entries(clonePaths)) {
    if (typeof clonePath === 'string') {
      expandLeadingTilde(clonePath, `${field}.clonePaths[${JSON.stringify(repo)}]`)
    }
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
export type PreviewConfig = NonNullable<FactoryConfig['preview']>
export type PreviewServiceConfig = PreviewConfig['services'][string]
