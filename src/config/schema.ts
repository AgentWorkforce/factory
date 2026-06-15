import { z } from 'zod'

import { LINEAR_STATE_IDS } from '../constants/linear'

const DEFAULT_STATE_IDS = {
  readyForAgent: LINEAR_STATE_IDS.readyForAgent,
  agentImplementing: LINEAR_STATE_IDS.agentImplementing,
  done: LINEAR_STATE_IDS.done,
  inPlanning: LINEAR_STATE_IDS.inPlanning,
}

export const FactoryConfigSchema = z.object({
  workspaceId: z.string(),
  subscription: z.object({
    teams: z.array(z.string()).default([]),
    projects: z.array(z.string()).default([]),
    labels: z.array(z.string()).default([]),
    assignees: z.array(z.string()).default([]),
  }).default({}),
  liveSubscription: z.object({
    transport: z.enum(['subscribe-and-poll', 'subscribe', 'poll']).default('subscribe-and-poll'),
    pollIntervalMs: z.number().int().min(50).default(5_000),
    eventLimit: z.number().int().min(1).max(1_000).default(1_000),
    replaySkewMarginMs: z.number().int().min(0).default(60_000),
  }).default({}),
  dispatch: z.object({
    errorCooldownMs: z.number().int().min(0).default(60_000),
    maxAttempts: z.number().int().min(1).max(5).default(2),
  }).default({}),
  loop: z.object({
    maxIterations: z.number().int().min(1).max(5).default(3),
    maxConsecutiveFailures: z.number().int().min(1).max(5).default(3),
    heartbeatPath: z.string().min(1).default('/tmp/factory-run/factory-loop-heartbeat.json'),
    registryPath: z.string().min(1).default('/tmp/factory-run/factory-loop-registry.json'),
    heartbeatStaleMs: z.number().int().min(1_000).default(60_000),
  }).default({}),
  triage: z.object({
    maxImplementers: z.number().int().min(1).max(6).default(2),
  }).default({}),
  repos: z.object({
    byLabel: z.record(z.string(), z.string()),
    byProject: z.record(z.string(), z.string()).default({}),
    keywordRules: z.array(z.object({ pattern: z.string(), repo: z.string() })).default([]),
    clonePaths: z.record(z.string(), z.string()).default({}),
    default: z.string().optional(),
  }),
  batchSize: z.number().int().min(1).max(5).default(5),
  models: z.object({
    implementer: z.string().optional(),
    reviewer: z.string().optional(),
    triage: z.string().optional(),
    // The PR babysitter defaults to sonnet — it shepherds an already-open PR
    // (CI/conflicts/comments) rather than authoring from scratch, so the
    // mid-tier model is the deliberate default rather than the implementer/
    // reviewer's unset (inherit) behavior.
    babysitter: z.string().default('sonnet'),
  }).default({}),
  slack: z.object({
    channel: z.string(),
    style: z.literal('threaded-summarized').default('threaded-summarized'),
    botUserId: z.string().default('U0B2596R7EZ'),
    staleAfterMs: z.number().int().min(1_000).default(10 * 60_000),
  }).optional(),
  mergePolicy: z.enum(['never', 'on-green-with-review']).default('never'),
  // Opt-in PR babysitter. When enabled, a sonnet agent is spawned once the
  // implementer's PR opens (webhook-driven, see the orchestrator) and shepherds
  // it — addressing review comments, resolving conflicts, and fixing CI — until
  // it is green, then transitions the issue to the `human-review` terminal state
  // instead of jumping straight to `done`. Default off preserves the legacy
  // PR-open -> done behavior.
  babysitter: z.object({
    enabled: z.boolean().default(false),
  }).default({}),
  // Which Linear state an issue lands in once the agents finish and the PR is
  // open. `human-review` parks it for operator review (Done is reserved for the
  // actual merge); `done` is the legacy behavior. Only honored when
  // `stateIds.humanReview` is configured — otherwise it falls back to `done`.
  terminalState: z.enum(['done', 'human-review']).default('human-review'),
  stateIds: z.object({
    readyForAgent: z.string(),
    agentImplementing: z.string(),
    done: z.string(),
    inPlanning: z.string(),
    // The "In Human Review" workflow-state UUID. Optional and not part of the
    // default stateIds because it is workspace-specific. When unset, the factory
    // falls back to `done` even if terminalState is `human-review`.
    humanReview: z.string().optional(),
  }).default(DEFAULT_STATE_IDS),
  safety: z.object({
    requireTitlePrefix: z.string().min(1).default('[factory-e2e]'),
    requireLabel: z.string().default('factory'),
    requireTeamKey: z.string().min(1).default('AR'),
  }).default({}),
  dryRun: z.boolean().default(false),
})

export type FactoryConfig = z.infer<typeof FactoryConfigSchema>
