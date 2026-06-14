import { z } from 'zod'

import { LINEAR_STATE_IDS } from '../constants/linear'

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
  }).default({}),
  slack: z.object({
    channel: z.string(),
    style: z.literal('threaded-summarized').default('threaded-summarized'),
    botUserId: z.string().default('U0B2596R7EZ'),
    staleAfterMs: z.number().int().min(1_000).default(10 * 60_000),
  }).optional(),
  mergePolicy: z.enum(['never', 'on-green-with-review']).default('never'),
  stateIds: z.object({
    readyForAgent: z.string(),
    agentImplementing: z.string(),
    done: z.string(),
    inPlanning: z.string(),
  }).default(LINEAR_STATE_IDS),
  safety: z.object({
    requireTitlePrefix: z.string().min(1).default('[factory-e2e]'),
    requireLabel: z.string().default('factory'),
    requireTeamKey: z.string().min(1).default('AR'),
  }).default({}),
  dryRun: z.boolean().default(false),
})

export type FactoryConfig = z.infer<typeof FactoryConfigSchema>
